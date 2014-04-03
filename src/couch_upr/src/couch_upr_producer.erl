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

% This module is for parsing and encoding all the UPR commands that are needed
% by the couch_fake_upr_server.
-module(couch_upr_producer).

-export([parse_header/1]).
-export([encode_open_connection/1, encode_snapshot_marker/5,
    encode_snapshot_mutation/10, encode_snapshot_deletion/6,
    encode_stream_request_ok/2, encode_stream_request_error/2,
    encode_stream_request_rollback/2, encode_stream_end/2,
    encode_failover_log/2, encode_stat/3, encode_stat_error/3,
    encode_sasl_auth/1, encode_stream_close_response/2,
    encode_select_bucket_response/2]).

-include_lib("couch_upr/include/couch_upr.hrl").
-include_lib("couch_upr/include/couch_upr_typespecs.hrl").


-spec parse_header(<<_:192>>) ->
                          {atom(), request_id(), partition_id()} |
                          {atom(), size(), request_id()} |
                          {atom(), size(), request_id(), partition_id()}.
parse_header(<<?UPR_MAGIC_REQUEST,
               Opcode,
               _KeyLength:?UPR_SIZES_KEY_LENGTH,
               _ExtraLength,
               _DataType,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               _Cas:?UPR_SIZES_CAS>>) ->
    case Opcode of
    ?UPR_OPCODE_OPEN_CONNECTION ->
        {open_connection, BodyLength, RequestId};
    ?UPR_OPCODE_STREAM_REQUEST ->
        {stream_request, BodyLength, RequestId, PartId};
    ?UPR_OPCODE_FAILOVER_LOG_REQUEST ->
        {failover_log, RequestId, PartId};
    ?UPR_OPCODE_STATS ->
        {stats, BodyLength, RequestId, PartId};
    ?UPR_OPCODE_SASL_AUTH ->
        {sasl_auth, BodyLength, RequestId};
    ?UPR_OPCODE_STREAM_CLOSE ->
        {stream_close, RequestId, PartId};
    ?UPR_OPCODE_SELECT_BUCKET ->
        {select_bucket, BodyLength, RequestId}
    end.


%UPR_OPEN response
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
    <<?UPR_MAGIC_RESPONSE,
      ?UPR_OPCODE_OPEN_CONNECTION,
      0:?UPR_SIZES_KEY_LENGTH,
      0,
      0,
      0:?UPR_SIZES_STATUS,
      0:?UPR_SIZES_BODY,
      RequestId:?UPR_SIZES_OPAQUE,
      0:?UPR_SIZES_CAS>>.

%UPR_SNAPSHOT_MARKER command
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
    Body = <<StartSeq:?UPR_SIZES_BY_SEQ,
             EndSeq:?UPR_SIZES_BY_SEQ,
             Type:?UPR_SIZES_SNAPSHOT_TYPE>>,
    BodyLength = byte_size(Body),
    ExtraLength = BodyLength,
    Header = <<?UPR_MAGIC_REQUEST,
               ?UPR_OPCODE_SNAPSHOT_MARKER,
               0:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               0:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%UPR_MUTATION command
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
    Body = <<Seq:?UPR_SIZES_BY_SEQ,
             RevSeq:?UPR_SIZES_REV_SEQ,
             Flags:?UPR_SIZES_FLAGS,
             Expiration:?UPR_SIZES_EXPIRATION,
             LockTime:?UPR_SIZES_LOCK,
             MetadataLength:?UPR_SIZES_METADATA_LENGTH,
             Nru:?UPR_SIZES_NRU_LENGTH,
             Key/binary,
             Value/binary>>,

    KeyLength = byte_size(Key),
    ValueLength = byte_size(Value),
    BodyLength = byte_size(Body),
    ExtraLength = BodyLength - KeyLength - ValueLength - MetadataLength,

    Header = <<?UPR_MAGIC_REQUEST,
               ?UPR_OPCODE_MUTATION,
               KeyLength:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               Cas:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%UPR_DELETION command
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
    Body = <<Seq:?UPR_SIZES_BY_SEQ,
             RevSeq:?UPR_SIZES_REV_SEQ,
             MetadataLength:?UPR_SIZES_METADATA_LENGTH,
             Key/binary>>,

    KeyLength = byte_size(Key),
    BodyLength = byte_size(Body),
    ExtraLength = BodyLength - KeyLength - MetadataLength,

    Header = <<?UPR_MAGIC_REQUEST,
               ?UPR_OPCODE_DELETION,
               KeyLength:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               Cas:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%UPR_STREAM_REQ response
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
    Header= <<?UPR_MAGIC_RESPONSE,
              ?UPR_OPCODE_STREAM_REQUEST,
              0:?UPR_SIZES_KEY_LENGTH,
              ExtraLength,
              0,
              ?UPR_STATUS_OK:?UPR_SIZES_STATUS,
              BodyLength:?UPR_SIZES_BODY,
              RequestId:?UPR_SIZES_OPAQUE,
              0:?UPR_SIZES_CAS>>,
    <<Header/binary, Value/binary>>.

-spec encode_stream_request_error(request_id(), upr_status()) -> binary().
encode_stream_request_error(RequestId, Status) ->
    Message = case Status of
    ?UPR_STATUS_KEY_NOT_FOUND ->
        <<"Not found">>;
    ?UPR_STATUS_ERANGE ->
        <<"Outside range">>;
    _ ->
        <<>>
    end,
    ExtraLength = 0,
    BodyLength = byte_size(Message),
    <<?UPR_MAGIC_RESPONSE,
      ?UPR_OPCODE_STREAM_REQUEST,
      0:?UPR_SIZES_KEY_LENGTH,
      ExtraLength,
      0,
      Status:?UPR_SIZES_STATUS,
      BodyLength:?UPR_SIZES_BODY,
      RequestId:?UPR_SIZES_OPAQUE,
      0:?UPR_SIZES_CAS,
      Message/binary>>.

%UPR_STREAM_REQ response
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
    <<?UPR_MAGIC_RESPONSE,
      ?UPR_OPCODE_STREAM_REQUEST,
      0:?UPR_SIZES_KEY_LENGTH,
      (?UPR_SIZES_BY_SEQ div 8),
      0,
      ?UPR_STATUS_ROLLBACK:?UPR_SIZES_STATUS,
      (?UPR_SIZES_BY_SEQ div 8):?UPR_SIZES_BODY,
      RequestId:?UPR_SIZES_OPAQUE,
      0:?UPR_SIZES_CAS,
      Seq:?UPR_SIZES_BY_SEQ>>.

%UPR_STREAM_END command
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
    Body = <<?UPR_FLAG_OK:?UPR_SIZES_FLAGS>>,
    BodyLength = byte_size(Body),
    % XXX vmx 2013-08-19: Still only 80% sure that ExtraLength has the correct
    %    value
    ExtraLength = BodyLength,
    Header = <<?UPR_MAGIC_REQUEST,
               ?UPR_OPCODE_STREAM_END,
               0:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               0:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%UPR_GET_FAILOVER_LOG response
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
    Header = <<?UPR_MAGIC_RESPONSE,
               ?UPR_OPCODE_FAILOVER_LOG_REQUEST,
               0:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               ?UPR_STATUS_OK:?UPR_SIZES_STATUS,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               0:?UPR_SIZES_CAS>>,
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
    Header = <<?UPR_MAGIC_RESPONSE,
               ?UPR_OPCODE_STATS,
               KeyLength:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               ?UPR_STATUS_OK:?UPR_SIZES_STATUS,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               0:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

-spec encode_stat_error(request_id(), upr_status(), binary()) -> binary().
encode_stat_error(RequestId, Status, Message) ->
    ExtraLength = 0,
    BodyLength = byte_size(Message),
    <<?UPR_MAGIC_RESPONSE,
      ?UPR_OPCODE_STATS,
      0:?UPR_SIZES_KEY_LENGTH,
      ExtraLength,
      0,
      Status:?UPR_SIZES_STATUS,
      BodyLength:?UPR_SIZES_BODY,
      RequestId:?UPR_SIZES_OPAQUE,
      0:?UPR_SIZES_CAS,
      Message/binary>>.

-spec encode_sasl_auth(request_id()) -> binary().
encode_sasl_auth(RequestId) ->
    Body = <<"Authenticated">>,
    BodyLength = byte_size(Body),
    ExtraLength = 0,
    Header = <<?UPR_MAGIC_RESPONSE,
               ?UPR_OPCODE_SASL_AUTH,
               0:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               0:?UPR_SIZES_STATUS,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               0:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.


%UPR_STREAM_CLOSE response
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

-spec encode_stream_close_response(request_id(), upr_status()) -> binary().
encode_stream_close_response(RequestId, Status) ->
    Message = case Status of
    ?UPR_STATUS_KEY_NOT_FOUND ->
        <<"Not found">>;
    ?UPR_STATUS_ERANGE ->
        <<"Outside range">>;
    _ ->
        <<>>
    end,
    ExtraLength = 0,
    BodyLength = byte_size(Message),
    <<?UPR_MAGIC_RESPONSE,
      ?UPR_OPCODE_STREAM_CLOSE,
      0:?UPR_SIZES_KEY_LENGTH,
      ExtraLength,
      0,
      Status:?UPR_SIZES_STATUS,
      BodyLength:?UPR_SIZES_BODY,
      RequestId:?UPR_SIZES_OPAQUE,
      0:?UPR_SIZES_CAS,
      Message/binary>>.


%UPR_SELECT_BUCKET response
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

-spec encode_select_bucket_response(request_id(), upr_status()) -> binary().
encode_select_bucket_response(RequestId, Status) ->
    ExtraLength = 0,
    BodyLength = 0,
    <<?UPR_MAGIC_RESPONSE,
      ?UPR_OPCODE_SELECT_BUCKET,
      0:?UPR_SIZES_KEY_LENGTH,
      ExtraLength,
      0,
      Status:?UPR_SIZES_STATUS,
      BodyLength:?UPR_SIZES_BODY,
      RequestId:?UPR_SIZES_OPAQUE,
      0:?UPR_SIZES_CAS>>.
