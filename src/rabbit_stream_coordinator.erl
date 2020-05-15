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
-module(rabbit_stream_coordinator).

-behaviour(ra_machine).

-export([start/0]).
-export([format_ra_event/2]).

-export([init/1,
         apply/3,
         state_enter/2,
         init_aux/1,
         handle_aux/6]).

-export([start_cluster/1,
         delete_cluster/2,
         add_replica/2,
         delete_replica/2]).

-export([phase_repair_mnesia/2,
         phase_repair_mnesia/3,
         phase_start_cluster/2,
         phase_delete_cluster/3,
         phase_check_quorum/2,
         phase_start_new_leader/1,
         phase_stop_replicas/1,
         phase_start_replica/3,
         phase_delete_replica/3]).

-export([log_overview/1]).

-define(STREAM_COORDINATOR_STARTUP, {stream_coordinator_startup, self()}).

-record(?MODULE, {streams, monitors}).

start() ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    ServerId = {?MODULE, node()},
    case ra:restart_server(ServerId) of
        {error, Reason} when Reason == not_started orelse
                             Reason == name_not_registered ->
            case ra:start_server(make_ra_conf(node(), Nodes)) of
                ok ->
                    global:set_lock(?STREAM_COORDINATOR_STARTUP),
                    case find_members(Nodes) of
                        [] ->
                            %% We're the first (and maybe only) one
                            ra:trigger_election(ServerId);
                        Members ->
                            %% What to do if we get a timeout?
                            {ok, _, _} = ra:add_member(Members, ServerId, 30000)
                    end,
                    global:del_lock(?STREAM_COORDINATOR_STARTUP),
                    _ = ra:members(ServerId),
                    ok;
                Error ->
                    exit(Error)
            end;
        ok ->
            ok;
        Error ->
            exit(Error)
    end.

find_members([]) ->
    [];
find_members([Node | Nodes]) ->
    case ra:members({?MODULE, Node}) of
        {_, Members, _} ->
            Members;
        {error, noproc} ->
            find_members(Nodes);
        {timeout, _} ->
            %% not sure what to do here
            find_members(Nodes)
    end.

start_cluster(Q) ->
    Server = {?MODULE, node()},
    ra:process_command(Server, {start_cluster, Q}).

delete_cluster(Q, ActingUser) ->
    Server = {?MODULE, node()},
    ra:process_command(Server, {delete_cluster, Q, ActingUser}).

add_replica(Name, Node) ->
    Server = {?MODULE, node()},
    ra:process_command(Server, {start_replica, Name, Node}).

delete_replica(Name, Node) ->
    Server = {?MODULE, node()},
    ra:process_command(Server, {delete_replica, Name, Node}).

init(_Conf) ->
    #?MODULE{streams = #{},
             monitors = #{}}.

apply(#{from := From}, {start_cluster, Q}, #?MODULE{streams = Streams} = State) ->
    #{name := StreamId} = Conf = amqqueue:get_type_state(Q),
    case maps:is_key(StreamId, Streams) of
        true ->
            {State, '$ra_no_reply', [{reply, From, {error, already_started}}]};
        false ->
            {State#?MODULE{streams = maps:put(StreamId, Q, Streams)}, '$ra_no_reply', 
             [{aux, {phase, StreamId, phase_start_cluster, [From, Conf]}}]}
    end;
apply(_Meta, {start_cluster_reply, StreamId, From, {ok, #{leader_pid := LeaderPid,
                                                          replica_pids := ReplicaPids} = Conf}},
      #?MODULE{streams = Streams,
               monitors = Monitors0} = State) ->
    Q0 = maps:get(StreamId, Streams),
    Q = amqqueue:set_type_state(amqqueue:set_pid(Q0, LeaderPid), Conf),
    Monitors = lists:foldl(fun(Pid, M) ->
                                   maps:put(Pid, {StreamId, follower}, M)
                           end, Monitors0, ReplicaPids),
    {State#?MODULE{streams = maps:put(StreamId, Conf, Streams),
                   monitors = maps:put(LeaderPid, {StreamId, leader}, Monitors)}, ok,
     [{monitor, process, LeaderPid}] ++ [{monitor, process, Pid} || Pid <- ReplicaPids] ++
     [{aux, {phase, StreamId, phase_repair_mnesia, [From, new, Q]}}]};
apply(_Meta, {start_cluster_reply, _StreamId, From, {error, {{already_started, _}, _}}}, State) ->
    {State, ok, [{reply, From, {error, already_started}}]};
apply(#{from := From}, {start_replica, StreamId, Node}, #?MODULE{streams = Streams} = State) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            {State, '$ra_no_reply', [{reply, From, {error, not_found}}]};
        Conf ->
            {State, '$ra_no_reply', [{aux, {phase, StreamId, phase_start_replica,
                                            [From, Node, Conf]}}]}
    end;
apply(#{from := From}, {delete_replica, StreamId, Node},
      #?MODULE{streams = Streams,
               monitors = Monitors} = State) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            {State, '$ra_no_reply', [{reply, From, {error, not_found}}]};
        Conf0 ->
            Replicas0 = maps:get(replica_nodes, Conf0),
            ReplicaPids0 = maps:get(replica_pids, Conf0),
            case lists:member(Node, Replicas0) of
                false ->
                    {State, '$ra_no_reply', [{reply, From, ok}]};
                true ->
                    Pid = lists:filter(fun(P) -> node(P) == Node end, ReplicaPids0),
                    ReplicaPids = lists:delete(Pid, ReplicaPids0),
                    Replicas = lists:delete(Node, Replicas0),
                    Conf = maps:put(replica_pids, ReplicaPids,
                                    maps:put(replica_nodes, Replicas, Conf0)),
                    {State#?MODULE{monitors = maps:remove(Pid, Monitors),
                                   streams = maps:put(StreamId, Conf, Streams)},
                     '$ra_no_reply',
                     [{aux, {phase, StreamId, phase_delete_replica, [From, Node, Conf]}}]}
            end
    end;
apply(#{from := From}, {delete_cluster, StreamId, ActingUser},
      #?MODULE{streams = Streams,
               monitors = Monitors0} = State) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            {State, '$ra_no_reply', [{reply, From, {ok, 0}}]};
        Conf ->
            ReplicaPids = maps:get(replica_pids, Conf),
            LeaderPid = maps:get(leader_pid, Conf),
            Monitors = lists:foldl(fun(Pid, M) ->
                                           maps:remove(Pid, M)
                                   end, Monitors0, ReplicaPids ++ [LeaderPid]),
            {State#?MODULE{monitors = Monitors,
                           streams = maps:remove(StreamId, Streams)}, '$ra_no_reply',
             [{aux, {phase, StreamId, phase_delete_cluster, [From, Conf, ActingUser]}}]}
    end;
apply(_Meta, {delete_cluster_reply, _StreamId, From}, State) ->
    %% TODO return number of ready messages
    {State, ok, [{reply, From, {ok, 0}}]};
apply(_Meta, {down, Pid, _Reason}, #?MODULE{streams = Streams,
                                            monitors = Monitors} = State) ->
    case maps:get(Pid, Monitors, undefined) of
        {StreamId, leader} ->
            Conf = maps:get(StreamId, Streams),
            {State#?MODULE{monitors = maps:remove(Pid, Monitors)},
             ok, [{aux, {phase, StreamId, phase_check_quorum, [StreamId, Conf]}}]};
        {StreamId, follower} ->
            Conf0 = maps:get(StreamId, Streams),
            ReplicaPids = maps:get(replica_pids, Conf0),
            Conf = maps:put(replica_pids, lists:delete(Pid, ReplicaPids), Conf0),
            Node = node(Pid),
            {State#?MODULE{monitors = maps:remove(Pid, Monitors)}, ok,
             [{aux, {phase, StreamId, phase_start_replica, [undefined, Node, Conf]}}]};
        {_, stopping} ->
            {State#?MODULE{monitors = maps:remove(Pid, Monitors)}, ok, []};
        undefined ->
            {State, ok, []}
    end;
apply(_Meta, {start_leader_election, StreamId, NewEpoch, Offsets},
      #?MODULE{streams = Streams} = State) ->
    #{leader_node := Leader,
      replica_nodes := Replicas} = Conf0 = maps:get(StreamId, Streams),
    Conf = maps:put(epoch, NewEpoch, Conf0),
    NewLeader = find_max_offset(Offsets),
    NewConf = maps:put(replica_nodes, lists:delete(NewLeader, Replicas ++ [Leader]),
                       maps:put(leader_node, NewLeader, Conf)),
    {State#?MODULE{streams = maps:put(StreamId, Conf, Streams)}, ok,
     [{aux, {phase, StreamId, phase_start_new_leader, [NewConf]}}]};
apply(_Meta, {restart_replicas, #{name := StreamId,
                                 leader_pid := LeaderPid} = Conf},
      #?MODULE{streams = Streams,
               monitors = Monitors} = State) ->
    {State#?MODULE{streams = maps:put(StreamId, Conf, Streams),
                   monitors = maps:put(LeaderPid, {StreamId, leader}, Monitors)}, ok,
     [{monitor, process, LeaderPid}, {aux, {phase, StreamId, phase_stop_replicas, [Conf]}}]};
apply(_Meta, {stream_updated, From, #{name := StreamId} = Conf}, #?MODULE{streams = Streams} = State) ->
    MaybeReply = case From of
                     undefined -> [];
                     _ -> [{reply, From, ok}]
                 end,
    {State#?MODULE{streams = maps:put(StreamId, Conf, Streams)}, ok,
     [{aux, {phase, StreamId, phase_repair_mnesia, [update, Conf]}}] ++ MaybeReply};
apply(_Meta, {stream_updated, From, {Pid, Type}, #{name := StreamId} = Conf},
      #?MODULE{streams = Streams,
               monitors = Monitors} = State) ->
    MaybeReply = case From of
                     undefined -> [];
                     _ -> [{reply, From, ok}]
                 end,
    {State#?MODULE{streams = maps:put(StreamId, Conf, Streams),
                   monitors = maps:put(Pid, {StreamId, Type}, Monitors)}, ok,
     [{monitor, process, Pid},
      {aux, {phase, StreamId, phase_repair_mnesia, [update, Conf]}}] ++ MaybeReply}.

state_enter(leader, #?MODULE{streams = Streams}) ->
    maps:fold(fun(_, Conf, Acc) ->
                      [{monitor, process, maps:get(leader_pid, Conf)}] ++
                          [{monitor, process, Pid} || Pid <- maps:get(replica_pids, Conf)] ++
                          Acc
              end, [], Streams);
state_enter(_, _) ->
    [].

init_aux(_Name) ->
    {#{}, #{}, #{}}.

%% TODO ensure the dead writer is restarted as a replica at some point in time, increasing timeout?
handle_aux(leader, _, {phase, StreamId, Fun, Args} = Cmd, {Monitors, Streams, Pending}, LogState, _) ->
    case maps:get(StreamId, Streams, undefined) of
        {_, _} when Fun == phase_start_cluster; Fun == phase_start_replica; Fun == phase_delete_replica;
                    Fun == phase_delete_cluster ->
            UpdateFun = fun(Cmds) -> Cmds ++ [Cmd] end,
            {no_reply, {Monitors, Streams, maps:update_with(StreamId, UpdateFun, [Cmd], Pending)},
             LogState};
        _ ->
            Pid = erlang:apply(?MODULE, Fun, Args),
            {no_reply,
             {maps:put(Pid, StreamId, Monitors), maps:put(StreamId, {Fun, Args}, Streams), Pending},
             LogState, [{monitor, process, aux, Pid}]}
    end;
handle_aux(leader, _, {down, Pid, normal}, {Monitors0, Streams0, Pending}, LogState, _) ->
    StreamId = maps:get(Pid, Monitors0),
    Monitors = maps:remove(Pid, Monitors0),
    case maps:get(StreamId, Streams0) of
        {Phase, _} when Phase == phase_delete_cluster;
                        Phase == phase_repair_mnesia ->
            Streams = maps:remove(StreamId, Streams0),
            case maps:get(StreamId, Pending, []) of
                [] ->
                    {no_reply, {Monitors, Streams, Pending}, LogState};
                [{phase, StreamId, Fun, Args} | Cmds] ->
                    NewPid = erlang:apply(?MODULE, Fun, Args),
                    {no_reply,
                     {maps:put(NewPid, StreamId, Monitors), maps:put(StreamId, {Fun, Args}, Streams),
                      maps:put(StreamId, Cmds, Pending)}, LogState, [{monitor, process, aux, NewPid}]}
            end;
        _ ->
            {no_reply, {Monitors, Streams0, Pending}, LogState}
    end;
handle_aux(leader, _, {down, Pid, _}, {Monitors0, Streams, Pending}, LogState, _) ->
    %% The phase has failed, let's retry it
    StreamId = maps:get(Pid, Monitors0),
    {Fun, Args} = maps:get(StreamId, Streams),
    rabbit_log:warning("Error while executing coordinator phase ~p for stream queue ~p",
                       [Fun, StreamId]),
    NewPid = erlang:apply(?MODULE, Fun, Args),
    Monitors = maps:put(NewPid, StreamId, maps:remove(Pid, Monitors0)),
    {no_reply, {Monitors, Streams, Pending}, LogState};
handle_aux(_, _, _, AuxState, LogState, _) ->
    {no_reply, AuxState, LogState}.

phase_start_replica(From, Node, #{replica_nodes := Replicas0,
                                 replica_pids := ReplicaPids0} = Conf0) ->
    spawn(
      fun() ->
              %% TODO start replica could fail and this enter an infinity loop.
              %% We should retry but not block
              %% If a new leader hasn't yet been elected, this will fail with a badmatch
              %% as get_reader_context returns a no proc. An unhandled failure will
              %% crash this monitored process and restart it later.
              %% TODO However, do we want that crash in the log? We might need to try/catch
              %% to provide a log message instead as it's 'expected'. We could try to
              %% verify first that the leader is alive, but there would still be potential
              %% for a race condition in here.
              case osiris_replica:start(Node, Conf0) of
                  {ok, Pid} ->
                      ReplicaPids = [Pid | ReplicaPids0],
                      Replicas = [Node | Replicas0],
                      Conf = maps:put(replica_pids, ReplicaPids,
                                      maps:put(replica_nodes, Replicas, Conf0)),
                      ra:pipeline_command({?MODULE, node()},
                                          {stream_updated, From, {Pid, follower}, Conf});
                  {error, already_present} ->
                      gen_statem:reply(From, ok);
                  {error, {already_started, _}} ->
                      gen_statem:reply(From, ok);
                  {error, Reason} ->
                      %% TODO what to do?
                      rabbit_log:warning("Error while starting replica for ~p : ~p",
                                         [maps:get(name, Conf0), Reason]),
                      ok
              end
      end).

phase_delete_replica(From, Node, Conf) ->
    spawn(
      fun() ->
              ok = osiris_replica:delete(Node, Conf),
              ra:pipeline_command({?MODULE, node()}, {stream_updated, From, Conf})
      end).

phase_stop_replicas(#{replica_nodes := Replicas} = Conf) ->
    spawn(
      fun() ->
              [try
                   osiris_replica:stop(Node, Conf)
               catch _:{{nodedown, _}, _} ->
                       %% It could be the old leader that is still down, it's normal.
                       ok
               end  || Node <- Replicas],
              ra:pipeline_command({?MODULE, node()}, {stream_updated, undefined, Conf})
      end).

phase_start_new_leader(#{leader_node := Node} = Conf) ->
    spawn(fun() ->
                  osiris_replica:stop(Node, Conf),
                  %% If the start fails, the monitor will capture the crash and restart it
                  case osiris_writer:start(Conf) of
                      {ok, Pid} ->
                          ra:pipeline_command({?MODULE, node()},
                                              {restart_replicas, maps:put(leader_pid, Pid, Conf)});
                      {error, already_present} ->
                          ra:pipeline_command({?MODULE, node()},
                                              {restart_replicas, Conf});
                      {error, {already_started, Pid}} ->
                          ra:pipeline_command({?MODULE, node()},
                                              {restart_replicas, maps:put(leader_pid, Pid, Conf)})
                  end
          end).

phase_check_quorum(StreamId, #{epoch := Epoch,
                               replica_nodes := Nodes} = Conf) ->
    spawn(fun() ->
                  Offsets = find_replica_offsets(Conf),
                  case is_quorum(length(Nodes), length(Offsets)) of
                      true ->
                          ra:pipeline_command({?MODULE, node()},
                                              {start_leader_election, StreamId, Epoch + 1, Offsets});
                      false ->
                          %% Let's crash this process so the monitor will restart it
                          exit({not_enough_quorum, StreamId})
                  end
          end).

find_replica_offsets(#{replica_nodes := Nodes,
                       leader_node := Leader} = Conf) ->
    lists:foldl(
      fun(Node, Acc) ->
              try
                  %% osiris_log:overview/1 needs the directory - last item of the list
                  %% TODO highest offset and epoch - ask JV
                  %% Node availability is what we need, not the reader process!
                  %% TODO ensure we dont' match the rpc:call response with {error, sthing}
                  {_Range, Offsets} = rpc:call(Node, ?MODULE, log_overview, [Conf]),
                  [{Node, select_highest_offset(Offsets)} | Acc]
              catch
                  _:_ ->
                      Acc
              end
      end, [], Nodes ++ [Leader]).

select_highest_offset([]) ->
    empty;
select_highest_offset(Offsets) ->
    lists:last(Offsets).

log_overview(Config) ->
    Dir = osiris_log:directory(Config),
    osiris_log:overview(Dir).

find_max_offset(Offsets) ->
    [{Node, _} | _] = lists:sort(fun({_, {Ao, E}}, {_, {Bo, E}}) ->
                                         Ao >= Bo;
                                    ({_, {_, Ae}}, {_, {_, Be}}) ->
                                         Ae >= Be;
                                    ({_, empty}, _) ->
                                         false;
                                    (_, {_, empty}) ->
                                         true
                                 end, Offsets),
    Node.

is_quorum(1, 1) ->
    true;
is_quorum(NumReplicas, NumAlive) ->
    NumAlive >= ((NumReplicas div 2) + 1).

phase_repair_mnesia(From, new, Q) ->
    spawn(fun() ->
                  Reply = rabbit_amqqueue:internal_declare(Q, false),
                  gen_statem:reply(From, Reply)
          end).

phase_repair_mnesia(update, #{reference := QName,
                              leader_pid := LeaderPid} = Conf) ->
    Fun = fun (Q) ->
                  amqqueue:set_type_state(amqqueue:set_pid(Q, LeaderPid), Conf)
          end,
    spawn(fun() ->
                  case rabbit_misc:execute_mnesia_transaction(
                         fun() ->
                                 rabbit_amqqueue:update(QName, Fun)
                         end) of
                      not_found ->
                          %% This can happen during recovery
                          [Q] = mnesia:dirty_read(rabbit_durable_queue, QName),
                          {ok, _} = rabbit_amqqueue:ensure_rabbit_queue_record_is_initialized(Fun(Q));
                      V ->
                          V
                  end
          end).

phase_start_cluster(From, #{name := StreamId} = Conf) ->
    spawn(
      fun() ->
              Reply = osiris:start_cluster(Conf),
              ra:pipeline_command({?MODULE, node()}, {start_cluster_reply, StreamId, From, Reply})
      end).

phase_delete_cluster(From, #{name := StreamId,
                             reference := QName} = Conf, ActingUser) ->
    spawn(
      fun() ->
              ok = osiris:delete_cluster(Conf),
              _ = rabbit_amqqueue:internal_delete(QName, ActingUser),
              ra:pipeline_command({?MODULE, node()}, {delete_cluster_reply, StreamId, From})
      end).

format_ra_event(ServerId, Evt) ->
    {stream_coordinator_event, ServerId, Evt}.

make_ra_conf(Node, Nodes) ->
    UId = ra:new_uid(ra_lib:to_binary(?MODULE)),
    Formatter = {?MODULE, format_ra_event, []},
    Members = [{?MODULE, N} || N <- Nodes],
    #{cluster_name => ?MODULE,
      id => {?MODULE, Node},
      uid => UId,
      friendly_name => atom_to_list(?MODULE),
      metrics_key => ?MODULE,
      initial_members => Members,
      log_init_args => #{uid => UId},
      tick_timeout => 5000,
      machine => {module, ?MODULE, #{}},
      ra_event_formatter => Formatter}.
