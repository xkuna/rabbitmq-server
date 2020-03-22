%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% Copyright (c) 2012-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_queue).

-behaviour(rabbit_queue_type).

-export([is_enabled/0,
         declare/2,
         delete/4,
         purge/1,
         policy_changed/1,
         recover/2,
         is_recoverable/1,
         consume/3,
         cancel/5,
         handle_event/2,
         deliver/2,
         settle/3,
         reject/4,
         credit/4,
         dequeue/4,
         info/2,
         init/1,
         update/2,
         state_info/1,
         stat/1]).

-export([set_retention_policy/3]).
-export([add_replica/3,
         delete_replica/3]).
-export([format_osiris_event/3]).

-include("rabbit.hrl").
-include("amqqueue.hrl").

-type appender_seq() :: non_neg_integer().

-record(stream_client, {name :: term(),
                        %% TODO why it's initialised to a `ra` thing?
                        leader = ra:server_id(),
                        next_seq = 1 :: non_neg_integer(),
                        correlation = #{} :: #{appender_seq() => term()},
                        soft_limit :: non_neg_integer(),
                        slow = false :: boolean()
                       }).

-import(rabbit_queue_type_util, [args_policy_lookup/3,
                                 qname_to_internal_name/1]).

-spec is_enabled() -> boolean().
is_enabled() ->
    rabbit_feature_flags:is_enabled(stream_queue).

-spec declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing', amqqueue:amqqueue()} |
    rabbit_types:channel_exit().
declare(Q0, Node) when ?amqqueue_is_stream(Q0) ->
    Arguments = amqqueue:get_arguments(Q0),
    QName = amqqueue:get_name(Q0),
    check_invalid_arguments(QName, Arguments),
    rabbit_queue_type_util:check_auto_delete(Q0),
    rabbit_queue_type_util:check_exclusive(Q0),
    rabbit_queue_type_util:check_non_durable(Q0),
    Opts = amqqueue:get_options(Q0),
    ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
    Conf0 = make_stream_conf(Node, Q0),
    case osiris:start_cluster(Conf0) of
        {ok, #{leader_pid := LeaderPid} = Conf} ->
            Q1 = amqqueue:set_type_state(amqqueue:set_pid(Q0, LeaderPid), Conf),
            case rabbit_amqqueue:internal_declare(Q1, false) of
                {created, Q} ->
                    rabbit_event:notify(queue_created,
                                        [{name, QName},
                                         {durable, true},
                                         {auto_delete, false},
                                         {arguments, Arguments},
                                         {user_who_performed_action,
                                          ActingUser}]),
                    {new, Q};
                {error, Error} ->
                    _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
                    rabbit_misc:protocol_error(
                      internal_error,
                      "Cannot declare a queue '~s' on node '~s': ~255p",
                      [rabbit_misc:rs(QName), node(), Error]);
                {existing, _} = Ex ->
                    Ex
            end;
        {error, {already_started, _}} ->
            rabbit_misc:protocol_error(precondition_failed,
                                       "safe queue name already in use '~s'",
                                       [Node])
    end.

-spec delete(amqqueue:amqqueue(), boolean(),
             boolean(), rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty).
delete(Q, _IfUnused, _IfEmpty, ActingUser) ->
    osiris:delete_cluster(amqqueue:get_type_state(Q)),
    _ = rabbit_amqqueue:internal_delete(amqqueue:get_name(Q), ActingUser),
    %% TODO return number of ready messages
    {ok, 0}.

-spec purge(amqqueue:amqqueue()) ->
    {'ok', non_neg_integer()}.
purge(_Q) ->
    {ok, 0}.

-spec policy_changed(amqqueue:amqqueue()) -> 'ok'.
policy_changed(_Q) ->
    ok.

stat(_) ->
    {ok, 0, 0}.

consume(_, _, _) ->
    ok.

cancel(_, _, _, _, _) ->
    ok.

credit(_, _, _, _) ->
    ok.

deliver(QSs, #delivery{confirm = Confirm} = Delivery) ->
    lists:foldl(
      fun({_Q, stateless}, {Qs, Actions}) ->
              %% TODO what do we do with stateless?
              %% QRef = amqqueue:get_pid(Q),
              %% ok = rabbit_fifo_client:untracked_enqueue(
              %%        [QRef], Delivery#delivery.message),
              {Qs, Actions};
         ({Q, S0}, {Qs, Actions}) ->
              S = deliver(Confirm, Delivery, S0),
              {[{Q, S} | Qs], Actions}
      end, {[], []}, QSs).

deliver(_Confirm, #delivery{message = Msg, msg_seq_no = MsgId},
       #stream_client{name = Name,
                      leader = LeaderPid,
                      next_seq = Seq,
                      correlation = Correlation0,
                      soft_limit = SftLmt,
                      slow = Slow0} = State) ->
    ok = osiris:write(LeaderPid, Seq, term_to_binary(Msg)),
    Correlation = case MsgId of
                      undefined ->
                          Correlation0;
                      _ when is_number(MsgId) ->
                          Correlation0#{Seq => MsgId}
                  end,
    Slow = case maps:size(Correlation) >= SftLmt of
               true when not Slow0 ->
                   credit_flow:block(Name),
                   true;
               Bool ->
                   Bool
           end,
    State#stream_client{next_seq = Seq + 1,
                        correlation = Correlation,
                        slow = Slow}.

dequeue(_, _, _, _) ->
    ok.

handle_event({From, Corrs}, State = #stream_client{correlation = Correlation0,
                                                   soft_limit = SftLmt,
                                                   slow = Slow0,
                                                   name = Name}) ->
    MsgIds = maps:values(maps:with(Corrs, Correlation0)),
    Correlation = maps:without(Corrs, Correlation0),
    Slow = case maps:size(Correlation) < SftLmt of
               true when Slow0 ->
                   credit_flow:unblock(Name),
                   false;
               _ ->
                   Slow0
           end,
    {ok, State#stream_client{correlation = Correlation,
                             slow = Slow}, [{settled, From, MsgIds}]}.

is_recoverable(Q) ->
    Node = node(),
    #{replica_nodes := Nodes,
      leader_node := Leader} = amqqueue:get_type_state(Q),
    lists:member(Node, Nodes ++ [Leader]).

recover(_VHost, Queues) ->
    lists:foldl(
      fun (Q0, {R0, F0}) ->
              case recover(Q0) of
                  {ok, Q} ->
                      {[Q | R0], F0};
                  {error, Q} ->
                      {R0, [Q | F0]}
              end
      end, {[], []}, Queues).

reject(_, _, _, _) ->
    ok.

settle(_, _, _) ->
    ok.

info(Q, Items) ->
    lists:foldr(fun(Item, Acc) ->
                        [{Item, i(Item, Q)} | Acc]
                end, [], Items).

i(name,        Q) when ?is_amqqueue(Q) -> amqqueue:get_name(Q);
i(durable,     Q) when ?is_amqqueue(Q) -> amqqueue:is_durable(Q);
i(auto_delete, Q) when ?is_amqqueue(Q) -> amqqueue:is_auto_delete(Q);
i(arguments,   Q) when ?is_amqqueue(Q) -> amqqueue:get_arguments(Q);
i(leader, Q) when ?is_amqqueue(Q) ->
    #{leader_node := Leader} = amqqueue:get_type_state(Q),
    Leader;
i(members, Q) when ?is_amqqueue(Q) ->
    #{replica_nodes := Nodes} = amqqueue:get_type_state(Q),
    Nodes;
i(state, Q) when ?is_amqqueue(Q) ->
    %% TODO the coordinator should answer this, I guess??
    running;
i(messages, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, _, _, M, _}] ->
            M;
        [] ->
            0
    end;
i(messages_ready, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, MR, _, _, _}] ->
            MR;
        [] ->
            0
    end;
i(messages_unacknowledged, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, _, MU, _, _}] ->
            MU;
        [] ->
            0
    end.

init(Q) when ?is_amqqueue(Q) ->
    Leader = amqqueue:get_pid(Q),
    {ok, SoftLimit} = application:get_env(rabbit, stream_messages_soft_limit),
    #stream_client{name = amqqueue:get_name(Q),
                   leader = Leader,
                   soft_limit = SoftLimit}.

update(_, State) ->
    State.

state_info(_) ->
    #{}.

set_retention_policy(Name, VHost, Policy) ->
    case rabbit_amqqueue:check_max_age(Policy) of
        {error, _} = E ->
            E;
        MaxAge ->
            QName = rabbit_misc:r(VHost, queue, Name),
            Fun = fun(Q) ->
                          Conf = amqqueue:get_type_state(Q),
                          amqqueue:set_type_state(Q, Conf#{max_age => MaxAge})
                  end,
            case rabbit_misc:execute_mnesia_transaction(
                   fun() -> rabbit_amqqueue:update(QName, Fun) end) of
                not_found ->
                    {error, not_found};
                _ ->
                    ok
            end
    end.

add_replica(VHost, Name, Node) ->
    QName = rabbit_misc:r(VHost, queue, Name),
    Fun = fun(Q) ->
                  Conf = amqqueue:get_type_state(Q),
                  Replicas = maps:get(replica_nodes, Conf),
                  case lists:member(Node, Replicas) of
                      true ->
                          Q;
                      false ->
                          {ok, Pid} = osiris_replica:start(Node, Conf),
                          ReplicaPids = maps:get(replica_pids, Conf),
                          Conf1 = maps:put(replica_pids, [Pid | ReplicaPids],
                                           maps:put(replica_nodes, [Node | Replicas], Conf)),
                          amqqueue:set_type_state(Q, Conf1)
                  end
          end,
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            {error, quorum_queue_not_supported};
        {ok, Q} when ?amqqueue_is_stream(Q) ->
            case lists:member(Node, rabbit_mnesia:cluster_nodes(running)) of
                false ->
                    {error, node_not_running};
                true ->
                    case rabbit_misc:execute_mnesia_transaction(
                           fun() -> rabbit_amqqueue:update(QName, Fun) end) of
                        not_found ->
                            {error, not_found};
                        _ ->
                            ok
                    end
            end;
        E ->
            E
    end.

delete_replica(VHost, Name, Node) ->
    QName = rabbit_misc:r(VHost, queue, Name),
    Fun = fun(Q) ->
                  Conf = amqqueue:get_type_state(Q),
                  Replicas = maps:get(replica_nodes, Conf),
                  case lists:member(Node, Replicas) of
                      true ->
                          %% What if delete directory crashes?
                          ok = osiris_replica:delete(Node, Conf),
                          ReplicaPids0 = maps:get(replica_pids, Conf),
                          ReplicaPids = lists:filter(fun(Pid) ->
                                                             node(Pid) =/= Node
                                                     end, ReplicaPids0),
                          Conf1 = maps:put(replica_pids, ReplicaPids,
                                           maps:put(replica_nodes, lists:delete(Node, Replicas), Conf)),
                          amqqueue:set_type_state(Q, Conf1);
                      false ->
                          Q
                  end
          end,
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            {error, quorum_queue_not_supported};
        {ok, Q} when ?amqqueue_is_stream(Q) ->
            case lists:member(Node, rabbit_mnesia:cluster_nodes(running)) of
                false ->
                    {error, node_not_running};
                true ->
                    case rabbit_misc:execute_mnesia_transaction(
                           fun() -> rabbit_amqqueue:update(QName, Fun) end) of
                        not_found ->
                            {error, not_found};
                        _ ->
                            ok
                    end
            end;
        E ->
            E
    end.

make_stream_conf(Node, Q) ->
    QName = amqqueue:get_name(Q),
    Name = queue_name(QName),
    MaxLength = args_policy_lookup(<<"max-length">>, fun min/2, Q),
    MaxBytes = args_policy_lookup(<<"max-length-bytes">>, fun min/2, Q),
    %% TODO max age units
    MaxAge = args_policy_lookup(<<"max-age">>, fun max_age/2, Q),
    Replicas = rabbit_mnesia:cluster_nodes(all) -- [Node],
    Formatter = {?MODULE, format_osiris_event, [QName]},
    #{reference => QName,
      name => Name,
      max_length => MaxLength,
      max_bytes => MaxBytes,
      max_age => MaxAge,
      leader_node => Node,
      replica_nodes => Replicas,
      event_formatter => Formatter,
      epoch => 1}.

format_osiris_event(ServerId, Evt, QRef) ->
    {'$gen_cast', {queue_event, QRef, {ServerId, Evt}}}.

max_age(Age1, Age2) ->
    min(rabbit_amqqueue:check_max_age(Age1), rabbit_amqqueue:check_max_age(Age2)).

check_invalid_arguments(QueueName, Args) ->
    Keys = [<<"x-expires">>, <<"x-message-ttl">>,
            <<"x-max-priority">>, <<"x-queue-mode">>, <<"x-overflow">>,
            <<"x-max-in-memory-length">>, <<"x-max-in-memory-bytes">>,
            <<"x-quorum-initial-group-size">>, <<"x-cancel-on-ha-failover">>],
    rabbit_queue_type_util:check_invalid_arguments(QueueName, Args, Keys).

queue_name(#resource{virtual_host = VHost, name = Name}) ->
    Timestamp = erlang:integer_to_binary(erlang:system_time()),
    osiris_util:to_base64uri(erlang:binary_to_list(<<VHost/binary, "_", Name/binary, "_",
                                                     Timestamp/binary>>)).

recover(Q0) ->
    Node = node(),
    Conf0 = amqqueue:get_type_state(Q0),
    QName = amqqueue:get_name(Q0),
    Pid = amqqueue:get_pid(Q0),
    case node(Pid) of
        Node ->
            Conf1 = maps:put(replica_pids, [], Conf0),
            {ok, LeaderPid} = osiris:restart_server(Conf1),
            Conf2 = maps:put(leader_pid, LeaderPid, Conf1),
            Q1 = amqqueue:set_pid(amqqueue:set_type_state(Q0, Conf2), LeaderPid),
            {ok, _} = rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Q1),
            ReplicaPids = restart_replicas_on_nodes(Conf2),
            {ok, update_replicas(QName, ReplicaPids)};
        _ ->
            restart_replica(Q0, Node, Conf0)
    end.

restart_replica(Q, Node, Conf) ->
    case rabbit_misc:is_process_alive(maps:get(leader_pid, Conf)) of
        true ->
            case osiris:restart_replica(Node, Conf) of
                {ok, ReplicaPid} ->
                    {ok, update_replicas(amqqueue:get_name(Q), [ReplicaPid])};
                {error, already_present} ->
                    {ok, Q};
                {error, {already_started, _}} ->
                    {ok, Q};
                Error ->
                    rabbit_log:warning("Error starting stream ~p replica: ~p",
                                       [maps:get(name, Conf), Error]),
                    {error, Q}
            end;
        false ->
            rabbit_log:debug("Stream ~p writer is down, the replica on this node"
                             " will be started by the writer", [maps:get(name, Conf)]),
            {error, Q}
    end.

restart_replicas_on_nodes(Conf) ->
    lists:foldl(
      fun(Replica, Pids) ->
              try
                  case osiris:restart_replica(Replica, Conf) of
                      {ok, Pid} ->
                          [Pid | Pids];
                      {error, already_present} ->
                          Pids;
                      {error, {already_started, _}} ->
                          Pids;
                      Error ->
                          rabbit_log:warning("Error starting stream ~p replica on node ~p: ~p",
                                             [maps:get(name, Conf), Replica, Error]),
                          Pids
                  end
              catch
                  _:_ ->
                      %% Node is not yet up, this is normal
                      Pids
              end
      end, [], maps:get(replicas, Conf)).

update_replicas(QName, ReplicaPids0) ->
    Fun = fun (Q) ->
                  Conf = amqqueue:get_type_state(Q),
                  ReplicaPids = filter_alive(maps:get(replicas, Conf)) ++ ReplicaPids0,
                  amqqueue:set_type_state(Q, maps:put(replicas, ReplicaPids, Conf))
          end,
    rabbit_misc:execute_mnesia_transaction(
      fun() -> rabbit_amqqueue:update(QName, Fun) end).

filter_alive(Pids) ->
    lists:filter(fun(Pid) when node(Pid) == node() ->
                         rabbit_misc:is_process_alive(Pid);
                    (_) ->
                         true
                 end, Pids).
