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

% This module is for parsing and encoding all the DCP commands that are needed
% by the couch_fake_dcp_server.
-module(couch_dcp_producer).

-export([parse_header/1]).
-export([encode_open_connection/1, encode_snapshot_marker/5,
    encode_snapshot_mutation/10, encode_snapshot_deletion/6,
    encode_stream_request_ok/2, encode_stream_request_error/2,
    encode_stream_request_rollback/2, encode_stream_end/2,
    encode_failover_log/2, encode_stat/3, encode_stat_error/3,
    encode_sasl_auth/1, encode_stream_close_response/2,
    encode_select_bucket_response/2, encode_control_flow_ok/1,
    encode_buffer_ack_ok/1, encode_noop_request/1,
    encode_seqs/2]).

-include_lib("couch_dcp/include/couch_dcp.hrl").
-include_lib("couch_dcp/include/couch_dcp_typespecs.hrl").


-spec parse_header(<<_:192>>) ->
                          {atom(), request_id(), partition_id()} |
                          {atom(), size(), request_id()} |
                          {atom(), size(), request_id(), partition_id()} |
                          {atom(), request_id()}.
parse_header(<<?DCP_MAGIC_REQUEST,
               Opcode,
               _KeyLength:?DCP_SIZES_KEY_LENGTH,
               _ExtraLength,
               _DataType,
               PartId:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               _Cas:?DCP_SIZES_CAS>>) ->
    case Opcode of
    ?DCP_OPCODE_OPEN_CONNECTION ->
        {open_connection, BodyLength, RequestId};
    ?DCP_OPCODE_STREAM_REQUEST ->
        {stream_request, BodyLength, RequestId, PartId};
    ?DCP_OPCODE_FAILOVER_LOG_REQUEST ->
        {failover_log, RequestId, PartId};
    ?DCP_OPCODE_STATS ->
        {stats, BodyLength, RequestId, PartId};
    ?DCP_OPCODE_SASL_AUTH ->
        {sasl_auth, BodyLength, RequestId};
    ?DCP_OPCODE_STREAM_CLOSE ->
        {stream_close, RequestId, PartId};
    ?DCP_OPCODE_SELECT_BUCKET ->
        {select_bucket, BodyLength, RequestId};
    ?DCP_OPCODE_DCP_BUFFER ->
        {buffer_ack, BodyLength, RequestId};
    ?DCP_OPCODE_DCP_CONTROL ->
        {control_request, BodyLength, RequestId};
    ?DCP_OPCODE_SEQS ->
        {all_seqs, RequestId}
    end;

parse_header(<<?DCP_MAGIC_RESPONSE,
               Opcode,
               _KeyLength:?DCP_SIZES_KEY_LENGTH,
               _ExtraLength,
               0,
               Status:?DCP_SIZES_STATUS,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               _Cas:?DCP_SIZES_CAS>>) ->
    case Opcode of
    ?DCP_OPCODE_DCP_NOOP ->
        {noop_response, Status, RequestId, BodyLength}
    end.

%DCP_OPEN response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x50
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x0000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0x00000001
%CAS          (16-23): 0x0000000000000000
-spec encode_open_connection(request_id()) -> <<_:192>>.
encode_open_connection(RequestId) ->
    <<?DCP_MAGIC_RESPONSE,
      ?DCP_OPCODE_OPEN_CONNECTION,
      0:?DCP_SIZES_KEY_LENGTH,
      0,
      0,
      0:?DCP_SIZES_STATUS,
      0:?DCP_SIZES_BODY,
      RequestId:?DCP_SIZES_OPAQUE,
      0:?DCP_SIZES_CAS>>.

%DCP_SNAPSHOT_MARKER command
%Field           (offset) (value)
%Magic           (0)    : 0x80
%Opcode          (1)    : 0x56
%Key length      (2,3)  : 0x0000
%Extra length    (4)    : 0x14
%Data type       (5)    : 0x00
%Vbucket         (6,7)  : 0x0000
%Total body      (8-11) : 0x00000014
%Opaque          (12-15): 0xdeadbeef
%CAS             (16-23): 0x0000000000000000
%  Start Seqno   (24-31): 0x0000000000000000
%  End Seqno     (32-39): 0x0000000000000008
%  Snapshot Type (40-43): 0x00000001 (disk)
-spec encode_snapshot_marker(partition_id(), request_id(), update_seq(),
                             update_seq(), non_neg_integer()) -> binary().
encode_snapshot_marker(PartId, RequestId, StartSeq, EndSeq, Type) ->
    Body = <<StartSeq:?DCP_SIZES_BY_SEQ,
             EndSeq:?DCP_SIZES_BY_SEQ,
             Type:?DCP_SIZES_SNAPSHOT_TYPE>>,
    BodyLength = byte_size(Body),
    ExtraLength = BodyLength,
    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_SNAPSHOT_MARKER,
               0:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%DCP_MUTATION command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x57
%Key length   (2,3)  : 0x0005
%Extra length (4)    : 0x1f
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0210
%Total body   (8-11) : 0x00000029
%Opaque       (12-15): 0x00001210
%CAS          (16-23): 0x0000000000000000
%  by seqno   (24-31): 0x0000000000000004
%  rev seqno  (32-39): 0x0000000000000001
%  flags      (40-43): 0x00000000
%  expiration (44-47): 0x00000000
%  lock time  (48-51): 0x00000000
%  nmeta      (52-53): 0x0000
%  nru        (54)   : 0x00
%Key          (55-59): hello
%Value        (60-64): world
-spec encode_snapshot_mutation(partition_id(), request_id(), non_neg_integer(),
                               non_neg_integer(), non_neg_integer(),
                               non_neg_integer(), non_neg_integer(),
                               non_neg_integer(), binary(), binary()) ->
                                      binary().
encode_snapshot_mutation(PartId, RequestId, Cas, Seq, RevSeq, Flags,
                         Expiration, LockTime, Key, Value) ->
    % XXX vmx 2014-01-08: No metadata support for now
    MetadataLength = 0,
    % NRU is set intentionally to some strange value, to simulate
    % that it could be anything and should be ignored.
    Nru = 87,
    Body = <<Seq:?DCP_SIZES_BY_SEQ,
             RevSeq:?DCP_SIZES_REV_SEQ,
             Flags:?DCP_SIZES_FLAGS,
             Expiration:?DCP_SIZES_EXPIRATION,
             LockTime:?DCP_SIZES_LOCK,
             MetadataLength:?DCP_SIZES_METADATA_LENGTH,
             Nru:?DCP_SIZES_NRU_LENGTH,
             Key/binary,
             Value/binary>>,

    KeyLength = byte_size(Key),
    ValueLength = byte_size(Value),
    BodyLength = byte_size(Body),
    ExtraLength = BodyLength - KeyLength - ValueLength - MetadataLength,

    DataType = case ejson:validate(Value) of
    ok ->
        ?DCP_DATA_TYPE_JSON;
    _ ->
        ?DCP_DATA_TYPE_RAW
    end,
    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_MUTATION,
               KeyLength:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               DataType,
               PartId:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               Cas:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%DCP_DELETION command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x58
%Key length   (2,3)  : 0x0005
%Extra length (4)    : 0x12
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0210
%Total body   (8-11) : 0x00000017
%Opaque       (12-15): 0x00001210
%CAS          (16-23): 0x0000000000000000
%  by seqno   (24-31): 0x0000000000000005
%  rev seqno  (32-39): 0x0000000000000001
%  nmeta      (40-41): 0x0000
%Key          (42-46): hello
-spec encode_snapshot_deletion(partition_id(), request_id(), non_neg_integer(),
                               non_neg_integer(), non_neg_integer(),
                               binary()) -> binary().
encode_snapshot_deletion(PartId, RequestId, Cas, Seq, RevSeq, Key) ->
    % XXX vmx 2014-01-08: No metadata support for now
    MetadataLength = 0,
    Body = <<Seq:?DCP_SIZES_BY_SEQ,
             RevSeq:?DCP_SIZES_REV_SEQ,
             MetadataLength:?DCP_SIZES_METADATA_LENGTH,
             Key/binary>>,

    KeyLength = byte_size(Key),
    BodyLength = byte_size(Body),
    ExtraLength = BodyLength - KeyLength - MetadataLength,

    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_DELETION,
               KeyLength:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               Cas:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%DCP_STREAM_REQ response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x53
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x0000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0x00001000
%CAS          (16-23): 0x0000000000000000
-spec encode_stream_request_ok(request_id(), partition_version()) -> binary().
encode_stream_request_ok(RequestId, FailoverLog) ->
    {BodyLength, Value} = failover_log_to_bin(FailoverLog),
    ExtraLength = 0,
    Header= <<?DCP_MAGIC_RESPONSE,
              ?DCP_OPCODE_STREAM_REQUEST,
              0:?DCP_SIZES_KEY_LENGTH,
              ExtraLength,
              0,
              ?DCP_STATUS_OK:?DCP_SIZES_STATUS,
              BodyLength:?DCP_SIZES_BODY,
              RequestId:?DCP_SIZES_OPAQUE,
              0:?DCP_SIZES_CAS>>,
    <<Header/binary, Value/binary>>.

-spec encode_stream_request_error(request_id(), dcp_status()) -> binary().
encode_stream_request_error(RequestId, Status) ->
    Message = case Status of
    ?DCP_STATUS_KEY_NOT_FOUND ->
        <<"Not found">>;
    ?DCP_STATUS_ERANGE ->
        <<"Outside range">>;
    _ ->
        <<>>
    end,
    ExtraLength = 0,
    BodyLength = byte_size(Message),
    <<?DCP_MAGIC_RESPONSE,
      ?DCP_OPCODE_STREAM_REQUEST,
      0:?DCP_SIZES_KEY_LENGTH,
      ExtraLength,
      0,
      Status:?DCP_SIZES_STATUS,
      BodyLength:?DCP_SIZES_BODY,
      RequestId:?DCP_SIZES_OPAQUE,
      0:?DCP_SIZES_CAS,
      Message/binary>>.

%DCP_STREAM_REQ response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x53
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x08
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x0023 (Rollback)
%Total body   (8-11) : 0x00000008
%Opaque       (12-15): 0x00001000
%CAS          (16-23): 0x0000000000000000
%  rollback # (24-31): 0x0000000000000000
-spec encode_stream_request_rollback(request_id(), update_seq()) -> <<_:256>>.
encode_stream_request_rollback(RequestId, Seq) ->
    <<?DCP_MAGIC_RESPONSE,
      ?DCP_OPCODE_STREAM_REQUEST,
      0:?DCP_SIZES_KEY_LENGTH,
      (?DCP_SIZES_BY_SEQ div 8),
      0,
      ?DCP_STATUS_ROLLBACK:?DCP_SIZES_STATUS,
      (?DCP_SIZES_BY_SEQ div 8):?DCP_SIZES_BODY,
      RequestId:?DCP_SIZES_OPAQUE,
      0:?DCP_SIZES_CAS,
      Seq:?DCP_SIZES_BY_SEQ>>.

%DCP_STREAM_END command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x55
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x04
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000004
%Opaque       (12-15): 0xdeadbeef
%CAS          (16-23): 0x0000000000000000
%  flag       (24-27): 0x00000000 (OK)
-spec encode_stream_end(partition_id(), request_id()) -> binary().
encode_stream_end(PartId, RequestId) ->
    % XXX vmx 2013-09-11: For now we return only success
    Body = <<?DCP_FLAG_OK:?DCP_SIZES_FLAGS>>,
    BodyLength = byte_size(Body),
    % XXX vmx 2013-08-19: Still only 80% sure that ExtraLength has the correct
    %    value
    ExtraLength = BodyLength,
    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_STREAM_END,
               0:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%DCP_GET_FAILOVER_LOG response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x54
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x0000
%Total body   (8-11) : 0x00000040
%Opaque       (12-15): 0xdeadbeef
%CAS          (16-23): 0x0000000000000000
%  vb UUID    (24-31): 0x00000000feeddeca
%  vb seqno   (32-39): 0x0000000000005432
%  vb UUID    (40-47): 0x0000000000decafe
%  vb seqno   (48-55): 0x0000000001343214
%  vb UUID    (56-63): 0x00000000feedface
%  vb seqno   (64-71): 0x0000000000000004
%  vb UUID    (72-79): 0x00000000deadbeef
%  vb seqno   (80-87): 0x0000000000006524
-spec encode_failover_log(request_id(), partition_version()) -> binary().
encode_failover_log(RequestId, FailoverLog) ->
    {BodyLength, Value} = failover_log_to_bin(FailoverLog),
    ExtraLength = 0,
    Header = <<?DCP_MAGIC_RESPONSE,
               ?DCP_OPCODE_FAILOVER_LOG_REQUEST,
               0:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               ?DCP_STATUS_OK:?DCP_SIZES_STATUS,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Value/binary>>.

-spec failover_log_to_bin(partition_version()) ->
                                 {non_neg_integer(), binary()}.
failover_log_to_bin(FailoverLog) ->
    FailoverLogBin = [[<<Uuid:64/integer>>, <<Seq:64>>] ||
                         {Uuid, Seq} <- FailoverLog],
    Value = list_to_binary(FailoverLogBin),
    {byte_size(Value), Value}.


%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x10
%Key length   (2,3)  : 0x0003
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x0000
%Total body   (8-11) : 0x00000007
%Opaque       (12-15): 0x00000000
%CAS          (16-23): 0x0000000000000000
%Key                 : The textual string "pid"
%Value               : The textual string "3078"
-spec encode_stat(request_id(), binary(), binary()) -> binary().
encode_stat(RequestId, Key, Value) ->
    Body = <<Key/binary, Value/binary>>,
    KeyLength = byte_size(Key),
    BodyLength = byte_size(Body),
    ExtraLength = 0,
    Header = <<?DCP_MAGIC_RESPONSE,
               ?DCP_OPCODE_STATS,
               KeyLength:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               ?DCP_STATUS_OK:?DCP_SIZES_STATUS,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

-spec encode_stat_error(request_id(), dcp_status(), binary()) -> binary().
encode_stat_error(RequestId, Status, Message) ->
    ExtraLength = 0,
    BodyLength = byte_size(Message),
    <<?DCP_MAGIC_RESPONSE,
      ?DCP_OPCODE_STATS,
      0:?DCP_SIZES_KEY_LENGTH,
      ExtraLength,
      0,
      Status:?DCP_SIZES_STATUS,
      BodyLength:?DCP_SIZES_BODY,
      RequestId:?DCP_SIZES_OPAQUE,
      0:?DCP_SIZES_CAS,
      Message/binary>>.

-spec encode_sasl_auth(request_id()) -> binary().
encode_sasl_auth(RequestId) ->
    Body = <<"Authenticated">>,
    BodyLength = byte_size(Body),
    ExtraLength = 0,
    Header = <<?DCP_MAGIC_RESPONSE,
               ?DCP_OPCODE_SASL_AUTH,
               0:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               0:?DCP_SIZES_STATUS,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.


%DCP_STREAM_CLOSE response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x53
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x10000000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0x00001000
%CAS          (16-23): 0x0000000000000000

-spec encode_stream_close_response(request_id(), dcp_status()) -> binary().
encode_stream_close_response(RequestId, Status) ->
    Message = case Status of
    ?DCP_STATUS_KEY_NOT_FOUND ->
        <<"Not found">>;
    ?DCP_STATUS_ERANGE ->
        <<"Outside range">>;
    _ ->
        <<>>
    end,
    ExtraLength = 0,
    BodyLength = byte_size(Message),
    <<?DCP_MAGIC_RESPONSE,
      ?DCP_OPCODE_STREAM_CLOSE,
      0:?DCP_SIZES_KEY_LENGTH,
      ExtraLength,
      0,
      Status:?DCP_SIZES_STATUS,
      BodyLength:?DCP_SIZES_BODY,
      RequestId:?DCP_SIZES_OPAQUE,
      0:?DCP_SIZES_CAS,
      Message/binary>>.


%DCP_SELECT_BUCKET response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x53
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x10000000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0x00001000
%CAS          (16-23): 0x0000000000000000

-spec encode_select_bucket_response(request_id(), dcp_status()) -> binary().
encode_select_bucket_response(RequestId, Status) ->
    ExtraLength = 0,
    BodyLength = 0,
    <<?DCP_MAGIC_RESPONSE,
      ?DCP_OPCODE_SELECT_BUCKET,
      0:?DCP_SIZES_KEY_LENGTH,
      ExtraLength,
      0,
      Status:?DCP_SIZES_STATUS,
      BodyLength:?DCP_SIZES_BODY,
      RequestId:?DCP_SIZES_OPAQUE,
      0:?DCP_SIZES_CAS>>.

%DCP_CONTROL_BINARY_CMD response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x5E
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x00000000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0x00001000
%CAS          (16-23): 0x0000000000000000

-spec encode_control_flow_ok(request_id()) -> binary().
encode_control_flow_ok(RequestId) ->
    ExtraLength = 0,
    BodyLength = 0,
    <<?DCP_MAGIC_RESPONSE,
      ?DCP_OPCODE_DCP_CONTROL,
      0:?DCP_SIZES_KEY_LENGTH,
      ExtraLength,
      0,
      ?DCP_STATUS_OK:?DCP_SIZES_STATUS,
      BodyLength:?DCP_SIZES_BODY,
      RequestId:?DCP_SIZES_OPAQUE,
      0:?DCP_SIZES_CAS>>.

%DCP_BUFFER_ACK_CMD response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x5D
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x00000000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0x00000000
%CAS          (16-23): 0x0000000000000000

-spec encode_buffer_ack_ok(request_id()) -> binary().
encode_buffer_ack_ok(RequestId) ->
    ExtraLength = 0,
    BodyLength = 0,
    <<?DCP_MAGIC_RESPONSE,
      ?DCP_OPCODE_DCP_BUFFER,
      0:?DCP_SIZES_KEY_LENGTH,
      ExtraLength,
      0,
      0:?DCP_SIZES_STATUS,
      BodyLength:?DCP_SIZES_BODY,
      RequestId:?DCP_SIZES_OPAQUE,
      0:?DCP_SIZES_CAS>>.

%DCP_NOOP command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x5C
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%VBucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0x00000005
%CAS          (16-23): 0x0000000000000000

-spec encode_noop_request(request_id()) -> binary().
encode_noop_request(RequestId) ->
    <<?DCP_MAGIC_REQUEST,
      ?DCP_OPCODE_DCP_NOOP,
      0:?DCP_SIZES_KEY_LENGTH,
      0,
      0,
      0:?DCP_SIZES_PARTITION,
      0:?DCP_SIZES_BODY,
      RequestId:?DCP_SIZES_OPAQUE,
      0:?DCP_SIZES_CAS>>.

%GET_ALL_VB_SEQNOS response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x48
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x0000
%Total body   (8-11) : 0x00000014
%Opaque       (12-15): 0x00000000
%CAS          (16-23): 0x0000000000000000
%  vb         (24-25): 0x0000
%  vb seqno   (26-33): 0x0000000000005432
%  vb         (34-35): 0x0001
%  vb seqno   (36-43): 0x0000000000001111
-spec encode_seqs(request_id(), [tuple()]) -> binary().
encode_seqs(RequestId, PartIdSeqs) ->
    Body = lists:foldl(
        fun({PartId, Seq}, Acc) ->
            <<Acc/binary, PartId:?DCP_SIZES_PARTITION, Seq:?DCP_SIZES_BY_SEQ>>
        end, <<>>, PartIdSeqs),
    BodyLength = byte_size(Body),
    Header = <<?DCP_MAGIC_RESPONSE,
               ?DCP_OPCODE_SEQS,
               0:?DCP_SIZES_KEY_LENGTH,
               0,
               0,
               ?DCP_STATUS_OK:?DCP_SIZES_STATUS,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.
