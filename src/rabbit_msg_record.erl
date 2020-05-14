-module(rabbit_msg_record).

-export([
         init/1,
         to_iodata/1,
         from_amqp091/2,
         to_amqp091/1
         ]).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-type maybe(T) :: T | undefined.
-type amqp10_data() :: [#'v1_0.data'{}] |
                       [#'v1_0.amqp_sequence'{}] |
                       #'v1_0.amqp_value'{}.
-record(msg,
        {
         % header :: maybe(#'v1_0.header'{}),
         % delivery_annotations :: maybe(#'v1_0.delivery_annotations'{}),
         message_annotations :: maybe(#'v1_0.message_annotations'{}),
         properties :: maybe(#'v1_0.properties'{}),
         application_properties :: maybe(#'v1_0.application_properties'{}),
         data :: maybe(amqp10_data())
         % footer :: maybe(#'v1_0.footer'{})
         }).

%% holds static or rarely changing fields
-record(cfg, {}).
-record(?MODULE, {cfg :: #cfg{},
                  msg :: #msg{},
                  %% holds a list of modifications to various sections
                  changes :: list()}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

%% this module acts as a wrapper / converter for the internal binar storage format
%% (AMQP 1.0) and any format it needs to be converted to / from.
%% Efficiency is key. No unnecessary allocations or work should be done until it
%% is absolutely needed

%% init from an AMQP 1.0 encoded binary
-spec init(binary()) -> state().
init(Bin) when is_binary(Bin) ->
    %% TODO: delay parsing until needed
    % [
    %  #'v1_0.message_annotations'{} = MA,
    %  #'v1_0.properties'{} = P,
    %  #'v1_0.application_properties'{} = AP,
    %  #'v1_0.data'{} = Data
    % ] = amqp10_framing:decode_bin(Bin),
    {MA, P, AP, D} = decode(amqp10_framing:decode_bin(Bin),
                            {undefined, undefined, undefined, undefined}),
    #?MODULE{msg = #msg{properties = P,
                        application_properties = AP,
                        message_annotations = MA,
                        data = D}}.

decode([], Acc) ->
    Acc;
decode([#'v1_0.message_annotations'{} = MA | Rem], {_, P, AP, D}) ->
    decode(Rem, {MA, P, AP, D});
decode([#'v1_0.properties'{} = P | Rem], {MA, _, AP, D}) ->
    decode(Rem, {MA, P, AP, D});
decode([#'v1_0.application_properties'{} = AP | Rem], {MA, P, _, D}) ->
    decode(Rem, {MA, P, AP, D});
decode([#'v1_0.data'{} = D | Rem], {MA, P, AP, _}) ->
    decode(Rem, {MA, P, AP, D}).

amqp10_properties_empty(#'v1_0.properties'{message_id = undefined,
                                           user_id = undefined,
                                           to = undefined,
                                           % subject = wrap(utf8, RKey),
                                           reply_to = undefined,
                                           correlation_id = undefined,
                                           content_type = undefined,
                                           content_encoding = undefined,
                                           creation_time = undefined}) ->
    true;
amqp10_properties_empty(_) ->
    false.

%% to realise the final binary data representation
-spec to_iodata(state()) -> iodata().
to_iodata(#?MODULE{msg = #msg{properties = P,
                              application_properties = AP,
                              message_annotations = MA,
                              data = Data}}) ->
    [
     case MA of
         #'v1_0.message_annotations'{content = []} ->
             <<>>;
         _ ->
             amqp10_framing:encode_bin(MA)
     end,
     case amqp10_properties_empty(P) of
         true -> <<>>;
         false ->
             amqp10_framing:encode_bin(P)
     end,
     case AP of
         #'v1_0.application_properties'{content = []} ->
             <<>>;
         _ ->
             amqp10_framing:encode_bin(AP)
     end,
     amqp10_framing:encode_bin(Data)
    ].


%% take a binary AMQP 1.0 input function,
%% parses it and returns the current parse state
%% this is the input function from storage and from, e.g. socket input
-spec from_amqp091(#'P_basic'{}, iodata()) -> state().
from_amqp091(#'P_basic'{message_id = MsgId,
                        expiration = Expiration,
                        delivery_mode = DelMode,
                        headers = Headers,
                        user_id = UserId,
                        reply_to = ReplyTo,
                        type = Type,
                        priority = Priority,
                        app_id = AppId,
                        correlation_id = CorrId,
                        content_type = ContentType,
                        content_encoding = ContentEncoding,
                        timestamp = Timestamp
                       }, Data) ->
    %% TODO: support parsing properties bin directly?
    P = #'v1_0.properties'{message_id = wrap(utf8, MsgId),
                           user_id = wrap(utf8, UserId),
                           to = undefined,
                           % subject = wrap(utf8, RKey),
                           reply_to = case ReplyTo of
                                          undefined ->
                                              undefined;
                                          _ ->
                                              wrap(utf8,
                                                   <<"/queue/", ReplyTo/binary>>)
                                      end,
                           correlation_id = wrap(utf8, CorrId),
                           content_type = wrap(symbol, ContentType),
                           content_encoding = wrap(symbol, ContentEncoding),
                           creation_time = wrap(timestamp, Timestamp)},

    APC0 = [{wrap(utf8, K), from_091(T, V)} || {K, T, V}
                                               <- case Headers of
                                                      undefined -> [];
                                                      _ -> Headers
                                                  end],
    %% properties that do not map directly to AMQP 1.0 properties are stored
    %% in application properties
    APC = map_add(<<"x-basic-type">>, utf8, Type,
                  map_add(<<"x-basic-app-id">>, utf8, AppId, APC0)),

    MAC = map_add(<<"x-basic-priority">>, ubyte, Priority,
                  map_add(<<"x-basic-delivery-mode">>, ubyte, DelMode,
                          map_add(<<"x-basic-expiration">>, utf8, Expiration, []))),

    AP = #'v1_0.application_properties'{content = APC},
    MA = #'v1_0.message_annotations'{content = MAC},
    #?MODULE{msg = #msg{properties = P,
                        application_properties = AP,
                        message_annotations = MA,
                        data = #'v1_0.data'{content = Data}}}.

map_add(_Key, _Type, undefined, Acc) ->
    Acc;
map_add(Key, Type, Value, Acc) ->
    [{wrap(utf8, Key), wrap(Type, Value)} | Acc].

-spec to_amqp091(state()) -> {#'P_basic'{}, iodata()}.
to_amqp091(#?MODULE{msg = #msg{properties = P,
                               application_properties = APR,
                               message_annotations = MAR,
                               data = #'v1_0.data'{content = Payload}}}) ->
    #'v1_0.properties'{message_id = MsgId,
                       user_id = UserId,
                       reply_to = ReplyTo0,
                       correlation_id = CorrId,
                       content_type = ContentType,
                       content_encoding = ContentEncoding,
                       creation_time = Timestamp} = case P of
                                                        undefined ->
                                                            #'v1_0.properties'{};
                                                        _ ->
                                                            P
                                                    end,

    AP0 = case APR of
              #'v1_0.application_properties'{content = AC} -> AC;
              _ -> []
          end,
    MA0 = case MAR of
              #'v1_0.message_annotations'{content = MC} -> MC;
              _ -> []
          end,

    {Type, AP1} = get_application_property(<<"x-basic-type">>, AP0),
    {AppId, AP} = get_application_property(<<"x-basic-app-id">>, AP1),

    {Priority, MA1} = get_application_property(<<"x-basic-priority">>, MA0),
    {DelMode, MA2} = get_application_property(<<"x-basic-delivery-mode">>, MA1),
    {Expiration, _MA} = get_application_property(<<"x-basic-expiration">>, MA2),

    Headers = [to_091(unwrap(K), V) || {K, V} <- AP],

    BP = #'P_basic'{message_id = unwrap(MsgId),
                    delivery_mode = DelMode,
                    expiration = Expiration,
                    user_id = unwrap(UserId),
                    headers = case Headers of
                                  [] -> undefined;
                                  _ -> Headers
                              end,
                    reply_to = case ReplyTo0 of
                                   undefined ->
                                       undefined;
                                   {utf8, <<"/queue/", ReplyTo/binary>>} ->
                                       ReplyTo
                               end,
                    type = Type,
                    app_id = AppId,
                    priority = Priority,
                    correlation_id = unwrap(CorrId),
                    content_type = unwrap(ContentType),
                    content_encoding = unwrap(ContentEncoding),
                    timestamp = unwrap(Timestamp)
                   },
    {BP, Payload}.

%%% Internal

get_application_property(K, AP0) ->
    case lists:keytake(wrap(utf8, K), 1, AP0) of
        false ->
            {undefined, AP0};
        {value, {_, V}, AP}  ->
            {unwrap(V), AP}
    end.

wrap(_Type, undefined) ->
    undefined;
wrap(Type, Val) ->
    {Type, Val}.

unwrap(undefined) ->
    undefined;
unwrap({_Type, V}) ->
    V.

% symbol_for(#'v1_0.properties'{}) ->
%     {symbol, <<"amqp:properties:list">>};

% number_for(#'v1_0.properties'{}) ->
%     {ulong, 115};
% encode(Frame = #'v1_0.properties'{}) ->
%     amqp10_framing:encode_described(list, 115, Frame);

% encode_described(list, CodeNumber, Frame) ->
%     {described, {ulong, CodeNumber},
%      {list, lists:map(fun encode/1, tl(tuple_to_list(Frame)))}};

% -spec generate(amqp10_type()) -> iolist().
% generate({described, Descriptor, Value}) ->
%     DescBin = generate(Descriptor),
%     ValueBin = generate(Value),
%     [ ?DESCRIBED_BIN, DescBin, ValueBin ].

to_091(Key, {utf8, V}) when is_binary(V) -> {Key, longstr, V};
to_091(Key, {long, V}) -> {Key, long, V};
to_091(Key, {ubyte, V}) -> {Key, unsignedbyte, V};
to_091(Key, {short, V}) -> {Key, short, V};
to_091(Key, {ushort, V}) -> {Key, unsignedshort, V};
to_091(Key, {uint, V}) -> {Key, unsignedint, V};
to_091(Key, {int, V}) -> {Key, signedint, V};
to_091(Key, {double, V}) -> {Key, double, V};
to_091(Key, {float, V}) -> {Key, float, V};
%% NB: header values can never be shortstr!
to_091(Key, {timestamp, V}) -> {Key, timestamp, V};
to_091(Key, {binary, V}) -> {Key, binary, V};
to_091(Key, {boolean, V}) -> {Key, bool, V}.

from_091(longstr, V) when is_binary(V) -> {utf8, V};
from_091(long, V) -> {long, V};
from_091(unsignedbyte, V) -> {ubyte, V};
from_091(short, V) -> {short, V};
from_091(unsignedshort, V) -> {ushort, V};
from_091(unsignedint, V) -> {uint, V};
from_091(signedint, V) -> {int, V};
from_091(double, V) -> {double, V};
from_091(float, V) -> {float, V};
from_091(bool, V) -> {boolean, V};
from_091(binary, V) -> {binary, V};
from_091(timestamp, V) -> {timestamp, V}.

% convert_header(signedint, V) -> [$I, <<V:32/signed>>];
% convert_header(decimal, V) -> {Before, After} = V,
%                               [$D, Before, <<After:32>>];
% convert_header(timestamp, V) -> [$T, <<V:64>>];
% % convert_header(table, V) -> [$F | table_to_binary(V)];
% % convert_header(array, V) -> [$A | array_to_binary(V)];
% convert_header(byte, V) -> [$b, <<V:8/signed>>];
% convert_header(double, V) -> [$d, <<V:64/float>>];
% convert_header(float, V) -> [$f, <<V:32/float>>];
% convert_header(short, V) -> [$s, <<V:16/signed>>];
% convert_header(binary, V) -> [$x | long_string_to_binary(V)];
% convert_header(unsignedbyte, V) -> [$B, <<V:8/unsigned>>];
% convert_header(unsignedshort, V) -> [$u, <<V:16/unsigned>>];
% convert_header(unsignedint, V) -> [$i, <<V:32/unsigned>>];
% convert_header(void, _V) -> [$V].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
