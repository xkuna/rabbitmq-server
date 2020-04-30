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
         add_replica/2]).

-export([phase_repair_mnesia/2,
         phase_repair_mnesia/3,
         phase_start_cluster/2,
         phase_delete_cluster/3,
         phase_check_quorum/2,
         phase_start_new_leader/1,
         phase_restart_replicas/1,
         phase_start_replica/3]).

-define(STREAM_COORDINATOR_STARTUP, {stream_coordinator_startup, self()}).

-record(?MODULE, {streams}).

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

init(_Conf) ->
    #?MODULE{streams = #{}}.

apply(#{from := From}, {start_cluster, Q}, #?MODULE{streams = Streams} = State) ->
    #{name := StreamId} = Conf = amqqueue:get_type_state(Q),
    case maps:is_key(StreamId, Streams) of
        true ->
            {State, '$ra_no_reply', [{reply, From, {error, already_started}}]};
        false ->
            {State#?MODULE{streams = maps:put(StreamId, Q, Streams)}, '$ra_no_reply', 
             [{aux, {phase, StreamId, phase_start_cluster, [From, Conf]}}]}
    end;
apply(_Meta, {start_cluster_reply, StreamId, From, {ok, #{leader_pid := LeaderPid} = Conf}},
      #?MODULE{streams = Streams} = State) ->
    Q0 = maps:get(StreamId, Streams),
    Q = amqqueue:set_type_state(amqqueue:set_pid(Q0, LeaderPid), Conf),
    {State#?MODULE{streams = maps:put(StreamId, Conf, Streams)}, ok,
     [{monitor, process, LeaderPid},
      {aux, {phase, StreamId, phase_repair_mnesia, [From, new, Q]}}]};
apply(_Meta, {start_cluster_reply, StreamId, From, {error, {already_started, _}}},
      #?MODULE{streams = Streams} = State) ->
    {State#?MODULE{streams = maps:remove(StreamId, Streams)}, ok,
     [{reply, From, {error, already_started}}]};
apply(#{from := From}, {start_replica, StreamId, Node}, #?MODULE{streams = Streams} = State) ->
    Conf = maps:get(StreamId, Streams),
    Replicas = maps:get(replica_nodes, Conf),
    case lists:member(Node, Replicas) of
        true ->
            {State, '$ra_no_reply', [{reply, From, ok}]};
        false ->
            {State, '$ra_no_reply', [{aux, {phase, StreamId, phase_start_replica, [From, Node, Conf]}}]}
    end;
apply(#{from := From}, {delete_cluster, StreamId, ActingUser}, #?MODULE{streams = Streams} = State) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            {State, '$ra_no_reply', [{reply, From, {ok, 0}}]};
        Conf ->
            {State, '$ra_no_reply', [{aux, {phase, StreamId, phase_delete_cluster,
                                            [From, Conf, ActingUser]}}]}
    end;
apply(_Meta, {delete_cluster_reply, StreamId, From}, #?MODULE{streams = Streams} = State) ->
    %% TODO return number of ready messages
    {State#?MODULE{streams = maps:remove(StreamId, Streams)}, ok, [{reply, From, {ok, 0}}]};
apply(_Meta, {down, Pid, _Reason}, #?MODULE{streams = Streams} = State) ->
    {StreamId, Conf} = find_stream(Pid, Streams),
    {State, ok, [{aux, {phase, StreamId, phase_check_quorum, [StreamId, Conf]}}]};
apply(_Meta, {start_leader_election, StreamId, NewEpoch, Offsets},
      #?MODULE{streams = Streams} = State) ->
    #{leader_node := Leader,
      replica_nodes := Replicas} = Conf0 = maps:get(StreamId, Streams),
    Conf = maps:put(epoch, NewEpoch, Conf0),
    NewLeader = find_max_offset(Offsets),
    NewConf = maps:put(replica_nodes, [Leader] ++ lists:delete(NewLeader, Replicas),
                       maps:put(leader_node, NewLeader, Conf)),
    {State#?MODULE{streams = maps:put(StreamId, Conf, Streams)}, ok,
     [{aux, {phase, StreamId, phase_start_new_leader, [NewConf]}}]};
apply(_Meta, {restart_replicas, #{name := StreamId} = Conf}, #?MODULE{streams = Streams} = State) ->
    {State#?MODULE{streams = maps:put(StreamId, Conf, Streams)}, ok,
     [{aux, {phase, StreamId, phase_restart_replicas, [Conf]}}]};
apply(_Meta, {stream_updated, From, #{name := StreamId} = Conf},
      #?MODULE{streams = Streams} = State) ->
    MaybeReply = case From of
                     undefined -> [];
                     _ -> [{reply, From, ok}]
                 end,
    {State#?MODULE{streams = maps:put(StreamId, Conf, Streams)}, ok,
     [{aux, {phase, StreamId, phase_repair_mnesia, [update, Conf]}}] ++ MaybeReply}.

state_enter(leader, #?MODULE{streams = Streams}) ->
    maps:fold(fun(_, Conf, Acc) ->
                      [{monitor, process, maps:get(leader_pid, Conf)} | Acc]
              end, [], Streams);
state_enter(_, _) ->
    [].

init_aux(_Name) ->
    {#{}, #{}}.

%% TODO ensure the dead writer is restarted as a replica at some point in time, increasing timeout?
handle_aux(leader, _, {phase, StreamId, Fun, Args}, {Monitors, Streams}, LogState, _) ->
    case maps:get(StreamId, Streams, undefined) of
        {Fun, _} ->
            {no_reply, {Monitors, Streams}, LogState};
        _Other ->
            %% TODO should we check order of phases?
            Pid = erlang:apply(?MODULE, Fun, Args),
            {no_reply, {maps:put(Pid, StreamId, Monitors), maps:put(StreamId, {Fun, Args}, Streams)},
             LogState, [{monitor, process, aux, Pid}]}
    end;
handle_aux(_, _, {down, _Pid, normal}, AuxState, LogState, _) ->
    {no_reply, AuxState, LogState};
handle_aux(_, _, {down, Pid, _}, {Monitors0, Streams}, LogState, _) ->
    %% The phase has failed, let's retry it
    StreamId = maps:get(Pid, Monitors0),
    {Fun, Args} = maps:get(StreamId, Streams),
    NewPid = erlang:apply(?MODULE, Fun, Args),
    Monitors = maps:put(NewPid, StreamId, maps:remove(Pid, Monitors0)),
    {no_reply, {Monitors, Streams}, LogState};
handle_aux(_, _, _, AuxState, LogState, _) ->
    {no_reply, AuxState, LogState}.

find_stream(Pid, Streams) ->
    Iterator = maps:iterator(Streams),
    find_stream0(Pid, Iterator).

find_stream0(Pid, Iterator0) ->
    case maps:next(Iterator0) of
        {StreamId, #{leader_pid := Pid} = Conf, _} ->
            {StreamId, Conf};
        {_, _, Iterator} ->
            find_stream0(Pid, Iterator);
        none ->
            %% TODO this shouldn't happen
            exit(stream_not_found)
    end.

phase_start_replica(From, Node, #{replica_nodes := Replicas} = Conf0) ->
    spawn(
      fun() ->
              {ok, Pid} = osiris:start_replica(Node, Conf0),
              ReplicaPids = maps:get(replica_pids, Conf0),
              Conf = maps:put(replica_pids, [Pid | ReplicaPids],
                             maps:put(replica_nodes, [Node | Replicas], Conf0)),
              ra:pipeline_command({?MODULE, node()},
                                  {stream_updated, From, maps:put(replica_pids, ReplicaPids, Conf)})
      end).

phase_restart_replicas(#{replica_nodes := Replicas} = Conf) ->
    spawn(
      fun() ->
              ReplicaPids = lists:foldl(fun(Node, Acc) ->
                                                try
                                                    ok = osiris_replica:stop(Node, Conf),
                                                    {ok, Pid} = osiris_replica:start(Node, Conf),
                                                    [Pid | Acc]
                                                catch
                                                    _:_ ->
                                                        %% TODO log here
                                                        %% Node down as the leader that just went
                                                        %% down is now in the replica list
                                                        Acc
                                                end
                                        end, [], Replicas),
              ra:pipeline_command({?MODULE, node()},
                                  {stream_updated, undefined, maps:put(replica_pids, ReplicaPids, Conf)})
      end).

phase_start_new_leader(#{leader_node := Node} = Conf) ->
    spawn(fun() ->
                  osiris_replica:stop(Node, Conf),
                  %% If the start fails, the monitor will capture the crash and restart it
                  {ok, Pid} = osiris_writer:start(Conf),
                  ra:pipeline_command({?MODULE, node()},
                                      {restart_replicas, maps:put(leader_pid, Pid, Conf)})
          end).

phase_check_quorum(StreamId, #{replica_pids := Pids,
                               epoch := Epoch}) ->
    spawn(fun() ->
                  Offsets = find_replica_offsets(Pids),
                  case is_quorum(length(Pids), length(Offsets)) of
                      true ->
                          ra:pipeline_command({?MODULE, node()},
                                              {start_leader_election, StreamId, Epoch + 1, Offsets});
                      false ->
                          %% Let's crash this process so the monitor will restart it
                          exit({not_enough_quorum, StreamId})
                  end
          end).

find_replica_offsets(Pids) ->
    lists:foldl(
      fun(Pid, Acc) ->
              try
                  %% osiris_log:overview/1 needs the directory - last item of the list
                  %% TODO highest offset and epoch - ask JV
                  %% Node availability is what we need, not the reader process!
                  {ok, Ctx} = gen:call(Pid, '$gen_call', get_reader_context, 5000),
                  [{Pid, maps:get(committed_offset, Ctx)} | Acc]
              catch
                  _:_ ->
                      Acc
              end
      end, [], Pids).

find_max_offset(Offsets) ->
    [{Pid, _} | _] = lists:sort(fun({_, A}, {_, B}) ->
                                        A >= B
                                end, Offsets),
    node(Pid).

is_quorum(NumReplicas, NumAlive) ->
    NumAlive >= ((NumReplicas div 2) + 1).

phase_repair_mnesia(From, new, Q) ->
    spawn(fun() ->
                  Reply = rabbit_amqqueue:internal_declare(Q, false),
                  gen_statem:reply(From, Reply)
          end).

phase_repair_mnesia(update, #{reference := QName} = Conf) ->
    Fun = fun (Q) ->
                  amqqueue:set_type_state(Q, Conf)
          end,
    spawn(fun() ->
                  rabbit_misc:execute_mnesia_transaction(
                    fun() -> rabbit_amqqueue:update(QName, Fun) end)
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
      friendly_name => ?MODULE,
      metrics_key => ?MODULE,
      initial_members => Members,
      log_init_args => #{uid => UId},
      tick_timeout => 5000,
      machine => {module, ?MODULE, #{}},
      ra_event_formatter => Formatter}.
