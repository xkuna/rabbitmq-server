-module(rabbit_msg_record_SUITE).

-compile(export_all).

-export([
         ]).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     ampq091_roundtrip
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

ampq091_roundtrip(_Config) ->
    % meck:expect(rabbit_mnesia, dir, fun () -> ?config(priv_dir, Config) end),
    %% we need this process to generate the first guid for each process
    % _GuidPid = rabbit_guid:start_link(),
    Props = #'P_basic'{content_type = <<"text/plain">>,
                       content_encoding = <<"gzip">>,
                       headers = [{<<"x-stream-offset">>, long, 99},
                                  {<<"x-string">>, longstr, <<"a string">>},
                                  {<<"x-bool">>, bool, false},
                                  {<<"x-unsignedbyte">>, unsignedbyte, 1},
                                  {<<"x-unsignedshort">>, unsignedshort, 1},
                                  {<<"x-unsignedint">>, unsignedint, 1},
                                  {<<"x-signedint">>, signedint, 1},
                                  {<<"x-timestamp">>, timestamp, 1},
                                  {<<"x-double">>, double, 1.0},
                                  {<<"x-float">>, float, 1.0},
                                  {<<"x-binary">>, binary, <<"data">>}
                                 ],
                       delivery_mode = 2,
                       priority = 99,
                       correlation_id = <<"corr">> ,
                       reply_to = <<"reply-to">>,
                       expiration = <<"1">>,
                       message_id = <<"msg-id">>,
                       timestamp = 99,
                       type = <<"45">>,
                       user_id = <<"banana">>,
                       app_id = <<"rmq">>
                       % cluster_id = <<"adf">>
                      },
    % Content = #content{class_id = 40, %% basic publish
    %                    properties = Props,
    %                    properties_bin = none,
    %                    %% protocol = ???,
    %                    payload_fragments_rev = },
    % XName = #resource{kind = exchange,
    %                   virtual_host = <<"vhost">>,
    %                   name = <<"name">>},
    Payload = [<<"data">>],
    test_amqp091_roundtrip(Props, Payload),
    test_amqp091_roundtrip(#'P_basic'{}, Payload),
    % {ok, BasicMsg} = rabbit_basic:message(XName, <<"routing-key">>, Content),
    % AddMsgAnns = #{<<"x-routing-key">> => <<"routing-key">>,
    %                <<"x-virtual-host">> => <<"vhost">>,
    %                <<"x-exchange">> => <<"exchange">>},
    % MsgRecord0 = rabbit_msg_record:from_amqp091(Props, Payload),
    % MsgRecord = rabbit_msg_record:init(
    %               iolist_to_binary(rabbit_msg_record:to_iodata(MsgRecord0))),
    % % meck:unload(),
    % {PropsOut, PayloadOut} = rabbit_msg_record:to_amqp091(MsgRecord),
    % ?assertEqual(Props, PropsOut),
    % ?assertEqual(iolist_to_binary(Payload),
    %              iolist_to_binary(PayloadOut)),
    %% basic message requires additional stuff
    ok.

%% Utility

test_amqp091_roundtrip(Props, Payload) ->
    MsgRecord0 = rabbit_msg_record:from_amqp091(Props, Payload),
    MsgRecord = rabbit_msg_record:init(
                  iolist_to_binary(rabbit_msg_record:to_iodata(MsgRecord0))),
    % meck:unload(),
    {PropsOut, PayloadOut} = rabbit_msg_record:to_amqp091(MsgRecord),
    ?assertEqual(Props, PropsOut),
    ?assertEqual(iolist_to_binary(Payload),
                 iolist_to_binary(PayloadOut)),
    ok.
