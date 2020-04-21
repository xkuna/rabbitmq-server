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
         apply/3]).

-export([start_cluster/1,
         delete_cluster/2]).

-export([phase_repair_mnesia/3,
         phase_start_cluster/2,
         phase_delete_cluster/3]).

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

init(_Conf) ->
    #?MODULE{streams = #{}}.

apply(#{from := From}, {start_cluster, Q}, #?MODULE{streams = Streams} = State) ->
    #{name := StreamId} = Conf = amqqueue:get_type_state(Q),
    case maps:is_key(StreamId, Streams) of
        true ->
            {State, {error, already_started}, []};
        false ->
            {State#?MODULE{streams = maps:put(StreamId, Q, Streams)}, '$ra_no_reply', 
             [{mod_call, ?MODULE, phase_start_cluster, [From, Conf]}]}
    end;
apply(_Meta, {start_cluster_reply, StreamId, From, {ok, #{leader_pid := LeaderPid} = Conf}},
      #?MODULE{streams = Streams} = State) ->
    Q0 = maps:get(StreamId, Streams),
    Q = amqqueue:set_type_state(amqqueue:set_pid(Q0, LeaderPid), Conf),
    {State#?MODULE{streams = maps:put(StreamId, Conf, Streams)}, ok,
     [{mod_call, ?MODULE, phase_repair_mnesia, [From, new, Q]}]};
apply(_Meta, {start_cluster_reply, StreamId, From, {error, {already_started, _}}},
      #?MODULE{streams = Streams} = State) ->
    {State#?MODULE{streams = maps:remove(StreamId, Streams)}, ok,
     [{reply, From, {error, already_started}}]};
apply(#{from := From}, {delete_cluster, StreamId, ActingUser}, #?MODULE{streams = Streams} = State) ->
    case maps:get(StreamId, Streams, undefined) of
        undefined ->
            {State, {ok, 0}, []};
        Conf ->
            {State, '$ra_no_reply', [{mod_call, ?MODULE, phase_delete_cluster,
                                      [From, Conf, ActingUser]}]}
    end;
apply(_Meta, {delete_cluster_reply, StreamId, From}, #?MODULE{streams = Streams} = State) ->
    %% TODO return number of ready messages
    {State#?MODULE{streams = maps:remove(StreamId, Streams)}, ok, [{reply, From, {ok, 0}}]}.

phase_repair_mnesia(From, new, Q) ->
    spawn(fun() ->
                  Reply = rabbit_amqqueue:internal_declare(Q, false),
                  gen_statem:reply(From, Reply)
          end).

phase_start_cluster(From, #{name := StreamId} = Conf) ->
    %% TODO monitor and handle
    spawn(
      fun() ->
              Reply = osiris:start_cluster(Conf),
              ra:pipeline_command({?MODULE, node()}, {start_cluster_reply, StreamId, From, Reply})
      end).

phase_delete_cluster(From, #{name := StreamId,
                             reference := QName} = Conf, ActingUser) ->
    %% TODO NEDS TO BE LINKED AND HANDLED
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
    Members = [{?MODULE, Node} || Node <- Nodes],
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
