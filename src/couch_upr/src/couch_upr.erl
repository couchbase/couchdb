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
-export([start/0]).
-export([enum_docs_since/6]).
-export([get_sequence_number/1]).

% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

-include_lib("couch_upr/include/couch_upr.hrl").


-define(UPR_SERVER_PORT, 12345).


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

start() ->
    gen_server:start_link(?MODULE, [], []).


% XXX vmx 2013-09-04: Use the partition version instead of StartSeq
enum_docs_since(Pid, PartId, StartSeq, EndSeq, InFun, InAcc) ->
    RequestId = gen_server:call(Pid, get_request_id),
    {Socket, Timeout} = gen_server:call(Pid, get_socket_and_timeout),
    StreamRequest = encode_stream_request(PartId, RequestId, StartSeq, EndSeq, 5678, 0,
        "test_consumer"),
    ok = gen_tcp:send(Socket, StreamRequest),
    receive_single_snapshot(Socket, Timeout, InFun, InAcc).


get_sequence_number(PartId) ->
    % NOTE vmx 2013-09-06: In the future this will be a stat from ep-engine
    Seq = couch_upr_fake_server:get_sequence_number(PartId),
    {ok, Seq}.


% gen_server callbacks

init([]) ->
    UprTimeout = list_to_integer(
        couch_config:get("upr", "connection_timeout")),
    {ok, Socket} = gen_tcp:connect("localhost", ?UPR_SERVER_PORT,
        [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    {ok, #state{
        socket = Socket,
        timeout = UprTimeout
    }}.



handle_call(get_request_id, _From, State) ->
    RequestId = case State#state.request_id of
    RequestId0 when RequestId0 < 1 bsl (?UPR_WIRE_SIZES_REQUEST_ID * 8) ->
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
    case gen_tcp:recv(Socket, ?UPR_WIRE_HEADER_LEN, Timeout) of
    {ok, Header} ->
        case parse_header(Header) of
        {stream_ok, _PartId, _RequestId} ->
            receive_single_snapshot(Socket, Timeout, MutationFun, Acc);
        {snapshot_start, _PartId, _RequestId} ->
            receive_single_snapshot(Socket, Timeout, MutationFun, Acc);
        {snapshot_end, _PartId, _RequestId} ->
            Acc;
        {snapshot_mutation, PartId, _RequestId, KeyLength, BodyLength} ->
            Mutation = receive_snapshot_mutation(
                Socket, Timeout, PartId, KeyLength, BodyLength),
            Acc2 = MutationFun(Mutation, Acc),
            receive_single_snapshot(Socket, Timeout, MutationFun, Acc2);
        {snapshot_deletion, PartId, _RequestId, KeyLength, BodyLength} ->
            Deletion = receive_snapshot_deletion(
                Socket, Timeout, PartId, KeyLength, BodyLength),
            Acc2 = MutationFun(Deletion, Acc),
            receive_single_snapshot(Socket, Timeout, MutationFun, Acc2);
        _ ->
            {error, {"Can't parse header", Header}}
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
         {Seq, RevSeq, Flags, Expiration, _LockTime, Key} = Deletion,
         % XXX vmx 2013-08-23: Use correct CAS value
         Cas = 0,
         Doc = #doc{
             id = Key,
             rev = {RevSeq, <<Cas:64, Expiration:32, Flags:32>>},
             deleted = true
         },
         {Seq, Doc, PartId};
    {error, closed} ->
        io:format("vmx: closed3~n", [])
    end.


% TODO vmx 2013-08-22: Bad match error handling
parse_header(<<?UPR_WIRE_MAGIC_RESPONSE,
               ?UPR_WIRE_OPCODE_RESPONSE,
               0:(?UPR_WIRE_SIZES_KEY_LENGTH*8),
               0,
               ?UPR_WIRE_REQUEST_TYPE_OK,
               PartId:(?UPR_WIRE_SIZES_PARTITION*8),
               0:(?UPR_WIRE_SIZES_BODY*8),
               RequestId:(?UPR_WIRE_SIZES_REQUEST_ID*8),
               0:(?UPR_WIRE_SIZES_CAS*8)>>) ->
    {stream_ok, PartId, RequestId};
parse_header(<<?UPR_WIRE_MAGIC_RESPONSE,
                 ?UPR_WIRE_OPCODE_STREAM_MESSAGE,
                 0:(?UPR_WIRE_SIZES_KEY_LENGTH*8),
                 0,
                 ?UPR_WIRE_REQUEST_TYPE_SNAPSHOT_START,
                 PartId:(?UPR_WIRE_SIZES_PARTITION*8),
                 0:(?UPR_WIRE_SIZES_BODY*8),
                 RequestId:(?UPR_WIRE_SIZES_REQUEST_ID*8),
                 0:(?UPR_WIRE_SIZES_CAS*8)>>) ->
    {snapshot_start, PartId, RequestId};
parse_header(<<?UPR_WIRE_MAGIC_RESPONSE,
                 ?UPR_WIRE_OPCODE_STREAM_MESSAGE,
                 0:(?UPR_WIRE_SIZES_KEY_LENGTH*8),
                 0,
                 ?UPR_WIRE_REQUEST_TYPE_SNAPSHOT_END,
                 PartId:(?UPR_WIRE_SIZES_PARTITION*8),
                 0:(?UPR_WIRE_SIZES_BODY*8),
                 RequestId:(?UPR_WIRE_SIZES_REQUEST_ID*8),
                 0:(?UPR_WIRE_SIZES_CAS*8)>>) ->
    {snapshot_end, PartId, RequestId};
parse_header(<<?UPR_WIRE_MAGIC_RESPONSE,
                 ?UPR_WIRE_OPCODE_STREAM_MESSAGE,
                 KeyLength:(?UPR_WIRE_SIZES_KEY_LENGTH*8),
                 _ExtraLength,
                 ?UPR_WIRE_REQUEST_TYPE_MUTATION,
                 PartId:(?UPR_WIRE_SIZES_PARTITION*8),
                 BodyLength:(?UPR_WIRE_SIZES_BODY*8),
                 RequestId:(?UPR_WIRE_SIZES_REQUEST_ID*8),
                 _Cas:(?UPR_WIRE_SIZES_CAS*8)>>) ->
    {snapshot_mutation, PartId, RequestId, KeyLength, BodyLength};
parse_header(<<?UPR_WIRE_MAGIC_RESPONSE,
                 ?UPR_WIRE_OPCODE_STREAM_MESSAGE,
                 KeyLength:(?UPR_WIRE_SIZES_KEY_LENGTH*8),
                 _ExtraLength,
                 ?UPR_WIRE_REQUEST_TYPE_DELETION,
                 PartId:(?UPR_WIRE_SIZES_PARTITION*8),
                 BodyLength:(?UPR_WIRE_SIZES_BODY*8),
                 RequestId:(?UPR_WIRE_SIZES_REQUEST_ID*8),
                 _Cas:(?UPR_WIRE_SIZES_CAS*8)>>) ->
    {snapshot_deletion, PartId, RequestId, KeyLength, BodyLength}.

parse_snapshot_mutation(KeyLength, Body) ->
    <<Seq:(?UPR_WIRE_SIZES_BY_SEQ*8),
      RevSeq:(?UPR_WIRE_SIZES_REV_SEQ*8),
      Flags:(?UPR_WIRE_SIZES_FLAGS*8),
      Expiration:(?UPR_WIRE_SIZES_EXPIRY*8),
      LockTime:(?UPR_WIRE_SIZES_LOCK*8),
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
    <<Seq:(?UPR_WIRE_SIZES_BY_SEQ*8),
      RevSeq:(?UPR_WIRE_SIZES_REV_SEQ*8),
      Flags:(?UPR_WIRE_SIZES_FLAGS*8),
      Expiration:(?UPR_WIRE_SIZES_EXPIRY*8),
      LockTime:(?UPR_WIRE_SIZES_LOCK*8),
      Key:KeyLength/binary>> = Body,
    {snapshot_deletion,
        {Seq, RevSeq, Flags, Expiration, LockTime, Key}}.


%Field          (offset)  (value)
%Magic          (0)     : 0x80                 (Request)
%Opcode         (1)     : 0x01                 (UPR Request)
%Key Length     (2-3)   : 0x000E               (14)
%Extra Length   (4)     : 0x28                 (40)
%Request Type   (5)     : 0x01                 (Stream Request)
%VBucket        (6-7)   : 0x000C               (12)
%Total Body     (8-11)  : 0x00000036           (56)
%Request ID     (12-15) : 0x0000002D           (45)
%Cas            (16-23) : 0x0000000000000000
%Start By Seqno (24-31) : 0x0000000000000000   (0)
%End By Seqno   (32-39) : 0xFFFFFFFFFFFFFFFF   (2^64)
%VBucket UUID   (40-47) : 0x000000000003C58A   (247178)
%High Seqno     (48-55) : 0x0000000000000A78   (2680)
%Group ID       (56-69) : "replication_12"
encode_stream_request(PartId, RequestId, StartSeq, EndSeq, PartUuid, HighSeq,
                      GroupId) ->
    Body = <<0:(?UPR_WIRE_SIZES_CAS*8),
             StartSeq:(?UPR_WIRE_SIZES_BY_SEQ*8),
             EndSeq:(?UPR_WIRE_SIZES_BY_SEQ*8),
             PartUuid:(?UPR_WIRE_SIZES_PARTITION_UUID*8),
             HighSeq:(?UPR_WIRE_SIZES_BY_SEQ*8),
             (list_to_binary(GroupId))/binary>>,

    BodyLength = byte_size(Body),
    KeyLength = length(GroupId),

    % XXX vmx 2013-08-19: Still only 80% sure that ExtraLength has the correct
    %    value
    ExtraLength = BodyLength - KeyLength,

    Header = <<?UPR_WIRE_MAGIC_REQUEST,
               ?UPR_WIRE_OPCODE_REQUEST,
               KeyLength:(?UPR_WIRE_SIZES_KEY_LENGTH*8),
               ExtraLength,
               ?UPR_WIRE_REQUEST_TYPE_STREAM,
               PartId:(?UPR_WIRE_SIZES_PARTITION*8),
               BodyLength:(?UPR_WIRE_SIZES_BODY*8),
               RequestId:(?UPR_WIRE_SIZES_REQUEST_ID*8)>>,
    <<Header/binary, Body/binary>>.
