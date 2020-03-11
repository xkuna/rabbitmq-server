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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018-2020 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_stream_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
      {group, single_node},
      {group, unclustered},
      {group, clustered}
    ].

groups() ->
    [
     {single_node, [], all_tests()},
     {clustered, [], [
                      {cluster_size_2, [], all_tests()},
                      {cluster_size_3, [], all_tests() ++ [delete_replica,
                                                           delete_down_replica,
                                                           delete_classic_replica,
                                                           delete_quorum_replica]},
                      {cluster_size_5, [], all_tests()}
                     ]},
     {unclustered, [], [
                        {unclustered_size_3, [], [add_replica,
                                                  add_classic_replica,
                                                  add_quorum_replica]}
                       ]}
    ].

all_tests() ->
    [
     declare_args,
     declare_max_age,
     declare_invalid_args,
     declare_invalid_properties,
     declare_queue,
     delete_queue
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config = rabbit_ct_helpers:merge_app_env(
               Config0, {rabbit, []}),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun rabbit_ct_broker_helpers:enable_dist_proxy_manager/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(clustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(unclustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]);
init_per_group(Group, Config) ->
    ClusterSize = case Group of
                      single_node -> 1;
                      cluster_size_2 -> 2;
                      cluster_size_3 -> 3;
                      unclustered_size_3 -> 3;
                      cluster_size_5 -> 5
                  end,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, ClusterSize},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    Config1b = rabbit_ct_helpers:set_config(Config1, [{net_ticktime, 10}]),
    Ret = rabbit_ct_helpers:run_steps(Config1b,
                                      [fun merge_app_env/1 ] ++
                                      rabbit_ct_broker_helpers:setup_steps()),
    case Ret of
        {skip, _} ->
            Ret;
        Config2 ->
            EnableFF = rabbit_ct_broker_helpers:enable_feature_flag(
                         Config2, stream_queue),
            case EnableFF of
                ok ->
                    ok = rabbit_ct_broker_helpers:rpc(
                           Config2, 0, application, set_env,
                           [rabbit, channel_tick_interval, 100]),
                    %% HACK: the larger cluster sizes benefit for a bit
                    %% more time after clustering before running the
                    %% tests.
                    case Group of
                        cluster_size_5 ->
                            timer:sleep(5000),
                            Config2;
                        _ ->
                            Config2
                    end;
                Skip ->
                    end_per_group(Group, Config2),
                    Skip
            end
    end.

end_per_group(clustered, Config) ->
    Config;
end_per_group(unclustered, Config) ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

merge_app_env(Config) ->
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

declare_args(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-max-length">>, long, 2000}])),
    assert_queue_type(Server, Q, rabbit_stream_queue).

declare_max_age(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server), Q,
               [{<<"x-queue-type">>, longstr, <<"stream">>},
                {<<"x-max-age">>, longstr, <<"1A">>}])),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-max-age">>, longstr, <<"1Y">>}])),
    assert_queue_type(Server, Q, rabbit_stream_queue).

declare_invalid_properties(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       amqp_channel:call(
         rabbit_ct_client_helpers:open_channel(Config, Server),
         #'queue.declare'{queue     = Q,
                          auto_delete = true,
                          durable   = true,
                          arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]})),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       amqp_channel:call(
         rabbit_ct_client_helpers:open_channel(Config, Server),
         #'queue.declare'{queue     = Q,
                          exclusive = true,
                          durable   = true,
                          arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]})),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       amqp_channel:call(
         rabbit_ct_client_helpers:open_channel(Config, Server),
         #'queue.declare'{queue     = Q,
                          durable   = false,
                          arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]})).

declare_invalid_args(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                    {<<"x-expires">>, long, 2000}])),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                    {<<"x-message-ttl">>, long, 2000}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                    {<<"x-max-priority">>, long, 2000}])),

    [?assertExit(
        {{shutdown, {server_initiated_close, 406, _}}, _},
        declare(rabbit_ct_client_helpers:open_channel(Config, Server),
                Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                     {<<"x-overflow">>, longstr, XOverflow}]))
     || XOverflow <- [<<"reject-publish">>, <<"reject-publish-dlx">>]],

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                    {<<"x-queue-mode">>, longstr, <<"lazy">>}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                    {<<"x-quorum-initial-group-size">>, longstr, <<"hop">>}])).

declare_queue(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    %% Test declare an existing queue
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ?assertMatch([_], rpc:call(Server, supervisor, which_children,
                               [osiris_server_sup])),

    %% Test declare an existing queue with different arguments
    ?assertExit(_, declare(Ch, Q, [])).

delete_queue(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})).

add_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    %% Not a member of the cluster, what would happen?
    ?assertEqual({error, node_not_running},
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server1])),
    ok = rabbit_control_helper:command(stop_app, Server1),
    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    rabbit_control_helper:command(start_app, Server1),
    timer:sleep(1000),
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server1])),
    %% replicas must be recorded on the state, and if we publish messages then they must
    %% be stored on disk
    [Info0] = rabbit_ct_broker_helpers:rpc(Config, 0,
                                          rabbit_amqqueue, info_all, [<<"/">>, [leader, replicas]]),
    ?assertEqual(Server0, proplists:get_value(leader, Info0)),
    ?assertEqual([Server1], proplists:get_value(replicas, Info0)),
    %% And if we try again? Idempotent
    ?assertEqual(ok, rpc:call(Server0, rabbit_stream_queue, add_replica,
                              [<<"/">>, Q, Server1])),
    %% Add another node
    ok = rabbit_control_helper:command(stop_app, Server2),
    ok = rabbit_control_helper:command(join_cluster, Server2, [atom_to_list(Server0)], []),
    rabbit_control_helper:command(start_app, Server2),
    ?assertEqual(ok, rpc:call(Server0, rabbit_stream_queue, add_replica,
                              [<<"/">>, Q, Server2])),
    [Info] = rabbit_ct_broker_helpers:rpc(Config, 0,
                                          rabbit_amqqueue, info_all, [<<"/">>, [leader, replicas]]),
    ?assertEqual(Server0, proplists:get_value(leader, Info)),
    Replicas = lists:sort([Server1, Server2]),
    ?assertEqual(Replicas, lists:sort(proplists:get_value(replicas, Info))).

add_classic_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"classic">>}])),
    %% Not a member of the cluster, what would happen?
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server1])),
    ok = rabbit_control_helper:command(stop_app, Server1),
    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    rabbit_control_helper:command(start_app, Server1),
    timer:sleep(1000),
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server1])).

add_quorum_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    %% Not a member of the cluster, what would happen?
    ?assertEqual({error, quorum_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server1])),
    ok = rabbit_control_helper:command(stop_app, Server1),
    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    rabbit_control_helper:command(start_app, Server1),
    timer:sleep(1000),
    ?assertEqual({error, quorum_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server1])).

delete_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    check_leader_and_replicas(Config, Server0, lists:sort([Server1, Server2])),
    %% Not a member of the cluster, what would happen?
    ?assertEqual({error, node_not_running},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, 'zen@rabbit'])),
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server1])),
    %% check it's gone
    check_leader_and_replicas(Config, Server0, [Server2]),
    %% And if we try again? Idempotent
    ?assertEqual(ok, rpc:call(Server0, rabbit_stream_queue, delete_replica,
                              [<<"/">>, Q, Server1])),
    %% Delete the last replica
    ?assertEqual(ok, rpc:call(Server0, rabbit_stream_queue, delete_replica,
                              [<<"/">>, Q, Server2])),
    check_leader_and_replicas(Config, Server0, []).

delete_classic_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"classic">>}])),
    %% Not a member of the cluster, what would happen?
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, 'zen@rabbit'])),
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server1])).

delete_quorum_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    %% Not a member of the cluster, what would happen?
    ?assertEqual({error, quorum_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, 'zen@rabbit'])),
    ?assertEqual({error, quorum_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server1])).

delete_down_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    check_leader_and_replicas(Config, Server0, lists:sort([Server1, Server2])),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    ?assertEqual({error, node_not_running},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server1])),
    %% check it isn't gone
    check_leader_and_replicas(Config, Server0, lists:sort([Server1, Server2])),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1).

%%----------------------------------------------------------------------------

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).
assert_queue_type(Server, Q, Expected) ->
    Actual = get_queue_type(Server, Q),
    Expected = Actual.

get_queue_type(Server, Q0) ->
    QNameRes = rabbit_misc:r(<<"/">>, queue, Q0),
    {ok, Q1} = rpc:call(Server, rabbit_amqqueue, lookup, [QNameRes]),
    amqqueue:get_type(Q1).

check_leader_and_replicas(Config, Leader, Replicas) ->
    [Info] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue,
                                           info_all, [<<"/">>, [leader, replicas]]),
    ?assertEqual(Leader, proplists:get_value(leader, Info)),
    ?assertEqual(Replicas, lists:sort(proplists:get_value(replicas, Info))).
