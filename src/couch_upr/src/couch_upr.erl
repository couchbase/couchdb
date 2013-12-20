% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_upr).
-behaviour(gen_server).

% Public API
-export([start/1]).
-export([enum_docs_since/7, get_failover_log/2]).
-export([get_sequence_number/2]).

% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

-include_lib("couch_upr/include/couch_upr.hrl").


-record(state, {
    socket = nil,
    timeout = 5000,
    request_id = 0,
    callbacks = dict:new(),
    % Data that was received but not parsed yet
    data = <<>>
}).

-record(mutation, {
    seq = 0        :: non_neg_integer(),
    rev_seq = 0    :: non_neg_integer(),
    flags = 0      :: non_neg_integer(),
    expiration = 0 :: non_neg_integer(),
    locktime = 0   :: non_neg_integer(),
    key = <<>>     :: binary(),
    value = <<>>   :: binary()
}).

% #doc{} is from couch_db.hrl. They are copy & pasted here
% as they will go away once the proper UPR is in place.
-record(doc,
    {
    id = <<>>,
    rev = {0, <<>>},

    % the binary body
    body = <<"{}">>,
    content_meta = 0, % should be 0-255 only.

    deleted = false,

    % key/value tuple of meta information, provided when using special options:
    % couch_db:open_doc(Db, Id, Options).
    meta = []
    }).

% Public API

start(Name) ->
    gen_server:start_link(?MODULE, [Name], []).


enum_docs_since(_, _, [], _, _, _, _) ->
    % No matching partition version found. Recreate the index from scratch
    {rollback, 0};
enum_docs_since(Pid, PartId, [PartVersion|PartVersions], StartSeq, EndSeq,
        InFun, InAcc) ->
    RequestId = gen_server:call(Pid, get_request_id),
    {Socket, Timeout} = gen_server:call(Pid, get_socket_and_timeout),
    StreamRequest = encode_stream_request(
        PartId, RequestId, 0, StartSeq, EndSeq, PartVersion),
    ok = gen_tcp:send(Socket, StreamRequest),
    Result = receive_single_snapshot(Socket, Timeout, InFun, {nil, InAcc}),
    case Result of
    {ok, {FailoverLog, Mutations}} ->
        {ok, Mutations, FailoverLog};
    % The failover log doesn't match. Try a previous partition version. The
    % last partition in the list will work as it requests with a partition
    % version sequence number of 0, which means requesting from the beginning.
    {error, wrong_partition_version} ->
        enum_docs_since(
            Pid, PartId, PartVersions, StartSeq, EndSeq, InFun, InAcc);
    {rollback, RollbackSeq} ->
        {rollback, RollbackSeq}
    end.


get_sequence_number(Pid, PartId) ->
    RequestId = gen_server:call(Pid, get_request_id),
    {Socket, Timeout} = gen_server:call(Pid, get_socket_and_timeout),
    SeqStatRequest = encode_seq_stat_request(PartId, RequestId),
    ok = gen_tcp:send(Socket, SeqStatRequest),
    {ok, Stats} = receive_stats(Socket, Timeout, []),
    % The stats return the sequence number as well as the partition UUID, but
    % we care only about the sequence number
    [{_, SeqBin} | _] = Stats,
    {ok, list_to_integer(binary_to_list(SeqBin))}.


% The failover log is a list of 2-tuples with the partition UUID and the
% sequence number when it was created
get_failover_log(Pid, PartId) ->
    RequestId = gen_server:call(Pid, get_request_id),
    {Socket, Timeout} = gen_server:call(Pid, get_socket_and_timeout),
    FailoverLogRequest = encode_failover_log_request(PartId, RequestId),
    ok = gen_tcp:send(Socket, FailoverLogRequest),
    case gen_tcp:recv(Socket, ?UPR_HEADER_LEN, Timeout) of
    {ok, Header} ->
        case parse_header(Header) of
        {failover_log, ?UPR_STATUS_OK, RequestId, BodyLength} ->
            receive_failover_log(Socket, Timeout, BodyLength);
        {failover_log, Status, RequestId, 0} ->
            {error, Status}
        end
    end.


% gen_server callbacks

init([Name]) ->
    UprTimeout = list_to_integer(
        couch_config:get("upr", "connection_timeout")),
    UprPort = list_to_integer(couch_config:get("upr", "port")),
    {ok, Socket} = gen_tcp:connect("localhost", UprPort,
        [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    RequestId = 0,
    OpenConnection = encode_open_connection(Name, RequestId),
    ok = gen_tcp:send(Socket, OpenConnection),
    case gen_tcp:recv(Socket, ?UPR_HEADER_LEN, UprTimeout) of
    {ok, Header} ->
        {open_connection, RequestId} = parse_header(Header)
    end,
    {ok, #state{
        socket = Socket,
        timeout = UprTimeout,
        request_id = RequestId + 1
    }}.


handle_call(get_request_id, _From, State) ->
    RequestId = case State#state.request_id of
    RequestId0 when RequestId0 < 1 bsl (?UPR_SIZES_OPAQUE + 1) ->
        RequestId0;
    _ ->
        0
    end,
    {reply, RequestId, State#state{request_id=RequestId + 1}};

handle_call(get_socket_and_timeout, _From, State) ->
    {reply, {State#state.socket, State#state.timeout}, State}.


handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Internal functions

receive_single_snapshot(Socket, Timeout, MutationFun, Acc) ->
    case gen_tcp:recv(Socket, ?UPR_HEADER_LEN, Timeout) of
    {ok, Header} ->
        case parse_header(Header) of
        {stream_request, Status, _RequestId, BodyLength} ->
            case Status of
            ?UPR_STATUS_OK ->
                 {ok, FailoverLog} = receive_failover_log(
                     Socket, Timeout, BodyLength),
                 {_, MutationAcc} = Acc,
                 Acc2 = {FailoverLog, MutationAcc},
                 receive_single_snapshot(Socket, Timeout, MutationFun, Acc2);
            ?UPR_STATUS_ROLLBACK ->
                case gen_tcp:recv(Socket, BodyLength, Timeout) of
                {ok, <<RollbackSeq:?UPR_SIZES_BY_SEQ>>} ->
                    {rollback, RollbackSeq};
                {error, closed} ->
                    io:format("vmx: closed5~n", [])
                end;
            ?UPR_STATUS_KEY_NOT_FOUND ->
                {error, wrong_partition_version}
            end;
        {snapshot_marker, _PartId, _RequestId} ->
            receive_single_snapshot(Socket, Timeout, MutationFun, Acc);
        {snapshot_mutation, PartId, _RequestId, KeyLength, BodyLength} ->
            Mutation = receive_snapshot_mutation(
                Socket, Timeout, PartId, KeyLength, BodyLength),
            {FailoverLog, MutationAcc} = Acc,
            MutationAcc2 = MutationFun(Mutation, MutationAcc),
            Acc2 = {FailoverLog, MutationAcc2},
            receive_single_snapshot(Socket, Timeout, MutationFun, Acc2);
        {snapshot_deletion, PartId, _RequestId, KeyLength, BodyLength} ->
            Deletion = receive_snapshot_deletion(
                Socket, Timeout, PartId, KeyLength, BodyLength),
            {FailoverLog, MutationAcc} = Acc,
            MutationAcc2 = MutationFun(Deletion, MutationAcc),
            Acc2 = {FailoverLog, MutationAcc2},
            receive_single_snapshot(Socket, Timeout, MutationFun, Acc2);
        {stream_end, _PartId, _RequestId, BodyLength} ->
            _Flag = receive_stream_end(Socket, Timeout, BodyLength),
            {ok, Acc}
        end;
    {error, closed} ->
        io:format("vmx: closed~n", []),
        closed
    end.


receive_snapshot_mutation(Socket, Timeout, PartId, KeyLength, BodyLength) ->
    case gen_tcp:recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
         {snapshot_mutation, Mutation} = parse_snapshot_mutation(KeyLength, Body),
         % XXX vmx 2013-08-23: For now, queue in items in the way the current
         %     updater expects them. This can be changed later to a simpler
         %     format.
         #mutation{
             seq = Seq,
             rev_seq = RevSeq,
             flags = Flags,
             expiration = Expiration,
             key = Key,
             value = Value
         } = Mutation,
         % XXX vmx 2013-08-23: Use correct CAS value
         Cas = 0,
         Doc = #doc{
             id = Key,
             rev = {RevSeq, <<Cas:64, Expiration:32, Flags:32>>},
             body = Value
         },
         {Seq, Doc, PartId};
    {error, closed} ->
        io:format("vmx: closed2~n", [])
    end.

receive_snapshot_deletion(Socket, Timeout, PartId, KeyLength, BodyLength) ->
    case gen_tcp:recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
         {snapshot_deletion, Deletion} = parse_snapshot_deletion(KeyLength, Body),
         % XXX vmx 2013-08-23: For now, queue in items in the way the current
         %     updater expects them. This can be changed later to a simpler
         %     format.
         {Seq, RevSeq, Key} = Deletion,
         % XXX vmx 2013-08-23: Use correct CAS value
         Cas = 0,
         Doc = #doc{
             id = Key,
             rev = {RevSeq, <<Cas:64, 0:32, 0:32>>},
             deleted = true
         },
         {Seq, Doc, PartId};
    {error, closed} ->
        io:format("vmx: closed3~n", [])
    end.

receive_stream_end(Socket, Timeout, BodyLength) ->
    case gen_tcp:recv(Socket, BodyLength, Timeout) of
    {ok, Flag} ->
        Flag;
    {error, closed} ->
        io:format("vmx: closed4~n", [])
    end.


% Returns the failover log as a list 2-tuple pairs with
% partition UUID and sequence number
receive_failover_log(_Socket, _Timeout, 0) ->
    {error, no_failover_log_found};
receive_failover_log(Socket, Timeout, BodyLength) ->
    case gen_tcp:recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
        parse_failover_log(Body);
    {error, closed} ->
        io:format("vmx: closed6~n", [])
    end.


receive_stats(Socket, Timeout, Acc) ->
    case gen_tcp:recv(Socket, ?UPR_HEADER_LEN, Timeout) of
    {ok, Header} ->
        case parse_header(Header) of
        {stats, ?UPR_STATUS_OK, _RequestId, BodyLength, KeyLength} when
                BodyLength > 0 andalso KeyLength > 0 ->
            {ok, Stat} = receive_stat(Socket, Timeout, BodyLength, KeyLength),
            receive_stats(Socket, Timeout, [Stat|Acc]);
        {stats, ?UPR_STATUS_OK, _RequestId, 0, 0} ->
            {ok, lists:reverse(Acc)};
        {stats, Status, _RequestId, BodyLength, 0} ->
            case gen_tcp:recv(Socket, BodyLength, Timeout) of
            {ok, Msg} ->
                {error, {Status, Msg}};
            {error, closed} ->
                {error, closed}
            end
        end;
    {error, closed} ->
        io:format("vmx: closed7~n", []),
        {error, closed}
    end.


receive_stat(Socket, Timeout, BodyLength, KeyLength) ->
    case gen_tcp:recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
        parse_stat(Body, KeyLength, BodyLength - KeyLength);
    {error, closed} ->
        io:format("vmx: closed8~n", []),
        {error, closed}
    end.


% TODO vmx 2013-08-22: Bad match error handling
parse_header(<<?UPR_MAGIC_RESPONSE,
               Opcode,
               KeyLength:?UPR_SIZES_KEY_LENGTH,
               _ExtraLength,
               0,
               Status:?UPR_SIZES_STATUS,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               _Cas:?UPR_SIZES_CAS>>) ->
    case Opcode of
    ?UPR_OPCODE_STREAM_REQUEST ->
        {stream_request, Status, RequestId, BodyLength};
    ?UPR_OPCODE_OPEN_CONNECTION ->
        {open_connection, RequestId};
    ?UPR_OPCODE_FAILOVER_LOG_REQUEST ->
        {failover_log, Status, RequestId, BodyLength};
    ?UPR_OPCODE_STATS ->
        {stats, Status, RequestId, BodyLength, KeyLength}
    end;
parse_header(<<?UPR_MAGIC_REQUEST,
               Opcode,
               KeyLength:?UPR_SIZES_KEY_LENGTH,
               _ExtraLength,
               _DataType,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               _Cas:?UPR_SIZES_CAS>>) ->
    case Opcode of
    ?UPR_OPCODE_STREAM_END ->
        {stream_end, PartId, RequestId, BodyLength};
    ?UPR_OPCODE_SNAPSHOT_MARKER ->
        {snapshot_marker, PartId, RequestId};
    ?UPR_OPCODE_MUTATION ->
        {snapshot_mutation, PartId, RequestId, KeyLength, BodyLength};
    ?UPR_OPCODE_DELETION ->
        {snapshot_deletion, PartId, RequestId, KeyLength, BodyLength}
    end.

parse_snapshot_mutation(KeyLength, Body) ->
    <<Seq:?UPR_SIZES_BY_SEQ,
      RevSeq:?UPR_SIZES_REV_SEQ,
      Flags:?UPR_SIZES_FLAGS,
      Expiration:?UPR_SIZES_EXPIRATION,
      LockTime:?UPR_SIZES_LOCK,
      Key:KeyLength/binary,
      Value/binary>> = Body,
    {snapshot_mutation, #mutation{
        seq = Seq,
        rev_seq = RevSeq,
        flags = Flags,
        expiration = Expiration,
        locktime = LockTime,
        key = Key,
        value = Value
    }}.

parse_snapshot_deletion(KeyLength, Body) ->
    <<Seq:?UPR_SIZES_BY_SEQ,
      RevSeq:?UPR_SIZES_REV_SEQ,
      Key:KeyLength/binary>> = Body,
    {snapshot_deletion, {Seq, RevSeq, Key}}.


parse_failover_log(Body) ->
    parse_failover_log(Body, []).
parse_failover_log(<<>>, Acc) ->
    {ok, lists:reverse(Acc)};
parse_failover_log(<<PartUuid:(?UPR_SIZES_PARTITION_UUID div 8)/binary,
                     PartSeq:?UPR_SIZES_BY_SEQ,
                     Rest/binary>>,
                   Acc) ->
    parse_failover_log(Rest, [{PartUuid, PartSeq}|Acc]).


parse_stat(Body, KeyLength, ValueLength) ->
    <<Key:KeyLength/binary, Value:ValueLength/binary>> = Body,
    {ok, {Key, Value}}.


%UPR_OPEN command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x50
%Key length   (2,3)  : 0x0018
%Extra length (4)    : 0x08
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000020
%Opaque       (12-15): 0x00000001
%CAS          (16-23): 0x0000000000000000
%  seqno      (24-27): 0x00000000
%  flags      (28-31): 0x00000000 (consumer)
%Key          (32-55): bucketstream vb[100-105]
encode_open_connection(Name, RequestId) ->
    Body = <<0:?UPR_SIZES_SEQNO,
             ?UPR_FLAG_PRODUCER:?UPR_SIZES_FLAGS,
             Name/binary>>,

    KeyLength = byte_size(Name),
    BodyLength = byte_size(Body),
    ExtraLength = BodyLength - KeyLength,

    Header = <<?UPR_MAGIC_REQUEST,
               ?UPR_OPCODE_OPEN_CONNECTION,
               KeyLength:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               0:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               0:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%UPR_STREAM_REQ command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x53
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x28
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000028
%Opaque       (12-15): 0x00001000
%CAS          (16-23): 0x0000000000000000
%  flags      (24-27): 0x00000000
%  reserved   (28-31): 0x00000000
%  start seqno(32-39): 0x0000000000ffeedd
%  end seqno  (40-47): 0xffffffffffffffff
%  vb UUID    (48-55): 0x00000000feeddeca
%  high seqno (56-63): 0x0000000000000000
encode_stream_request(PartId, RequestId, Flags, StartSeq, EndSeq,
        {PartUuid, PartHighSeq}) ->
    Body = <<Flags:?UPR_SIZES_FLAGS,
             0:?UPR_SIZES_RESERVED,
             StartSeq:?UPR_SIZES_BY_SEQ,
             EndSeq:?UPR_SIZES_BY_SEQ,
             PartUuid:(?UPR_SIZES_PARTITION_UUID div 8)/binary,
             PartHighSeq:?UPR_SIZES_BY_SEQ>>,

    BodyLength = byte_size(Body),
    ExtraLength = BodyLength,

    Header = <<?UPR_MAGIC_REQUEST,
               ?UPR_OPCODE_STREAM_REQUEST,
               0:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               0:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.


%UPR_GET_FAILOVER_LOG command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x54
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0xdeadbeef
%CAS          (16-23): 0x0000000000000000
encode_failover_log_request(PartId, RequestId) ->
    Header = <<?UPR_MAGIC_REQUEST,
               ?UPR_OPCODE_FAILOVER_LOG_REQUEST,
               0:?UPR_SIZES_KEY_LENGTH,
               0,
               0,
               PartId:?UPR_SIZES_PARTITION,
               0:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               0:?UPR_SIZES_CAS>>,
    <<Header/binary>>.


%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x10
%Key length   (2,3)  : 0x000e
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%VBucket      (6,7)  : 0x0001
%Total body   (8-11) : 0x0000000e
%Opaque       (12-15): 0x00000000
%CAS          (16-23): 0x0000000000000000
%Key                 : vbucket-seqno 1
encode_seq_stat_request(PartId, RequestId) ->
    Body = <<"vbucket-seqno ",
        (list_to_binary(integer_to_list(PartId)))/binary>>,

    KeyLength = BodyLength = byte_size(Body),
    ExtraLength = 0,

    Header = <<?UPR_MAGIC_REQUEST,
               ?UPR_OPCODE_STATS,
               KeyLength:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               0:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.
