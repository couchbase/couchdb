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
% by the indexer.
-module(couch_dcp_consumer).

-export([parse_header/1, parse_snapshot_marker/1, parse_snapshot_mutation/4,
    parse_snapshot_deletion/2, parse_failover_log/1, parse_stat/4]).
-export([encode_sasl_auth/3, encode_open_connection/2, encode_stream_request/8,
    encode_failover_log_request/2, encode_stat_request/3, encode_stream_close/2,
    encode_select_bucket/2]).
-export([encode_noop_response/1, encode_buffer_request/2,
    encode_control_request/3, parse_all_seqs/3, encode_all_seqs_request/1]).

-include_lib("couch_dcp/include/couch_dcp.hrl").
-include_lib("couch_dcp/include/couch_dcp_typespecs.hrl").


% TODO vmx 2013-08-22: Bad match error handling
-spec parse_header(<<_:192>>) ->
                          {atom(), size()} |
                          {atom(), dcp_status(), request_id(), size()} |
                          {atom(), dcp_status(), request_id()} |
                          {atom(), dcp_status(), request_id(), size(),
                           size()} |
                          {atom(), partition_id(), request_id(), size()} |
                          {atom(), partition_id(), request_id(), size(),
                           size()} |
                          {atom(), partition_id(), request_id(), size(),
                           size(), uint64()} |
                          {atom(), partition_id(), request_id(), size(),
                           size(), size(), uint64()}.
parse_header(<<?DCP_MAGIC_RESPONSE,
               Opcode,
               KeyLength:?DCP_SIZES_KEY_LENGTH,
               _ExtraLength,
               0,
               Status:?DCP_SIZES_STATUS,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               _Cas:?DCP_SIZES_CAS>>) ->
    case Opcode of
    ?DCP_OPCODE_STREAM_REQUEST ->
        {stream_request, Status, RequestId, BodyLength};
    ?DCP_OPCODE_OPEN_CONNECTION ->
        {open_connection, RequestId};
    ?DCP_OPCODE_FAILOVER_LOG_REQUEST ->
        {failover_log, Status, RequestId, BodyLength};
    ?DCP_OPCODE_STATS ->
        {stats, Status, RequestId, BodyLength, KeyLength};
    ?DCP_OPCODE_SASL_AUTH ->
        {sasl_auth, Status, RequestId, BodyLength};
    ?DCP_OPCODE_STREAM_CLOSE ->
        {stream_close, Status, RequestId, BodyLength};
    ?DCP_OPCODE_SELECT_BUCKET ->
        {select_bucket, Status, RequestId, BodyLength};
    ?DCP_OPCODE_DCP_BUFFER ->
        {buffer_ack, Status, RequestId};
    ?DCP_OPCODE_DCP_CONTROL ->
        {control_request, Status, RequestId};
    ?DCP_OPCODE_SEQS ->
        {all_seqs, Status, RequestId, BodyLength}
    end;
parse_header(<<?DCP_MAGIC_REQUEST,
               Opcode,
               KeyLength:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               DataType,
               PartId:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               Cas:?DCP_SIZES_CAS>>) ->
    case Opcode of
    ?DCP_OPCODE_STREAM_END ->
        {stream_end, PartId, RequestId, BodyLength};
    ?DCP_OPCODE_SNAPSHOT_MARKER ->
        {snapshot_marker, PartId, RequestId, BodyLength};
    ?DCP_OPCODE_MUTATION ->
        {snapshot_mutation, PartId, RequestId, KeyLength, BodyLength,
            ExtraLength, Cas, DataType};
    ?DCP_OPCODE_DELETION ->
        {snapshot_deletion, PartId, RequestId, KeyLength, BodyLength, Cas,
            DataType};
    ?DCP_OPCODE_EXPIRATION ->
        {snapshot_expiration, PartId, RequestId, KeyLength, BodyLength, Cas,
            DataType};
    ?DCP_OPCODE_DCP_NOOP ->
        {noop_request, RequestId}
    end.

-spec parse_snapshot_marker(<<_:160>>) ->
                                   {snapshot_marker, update_seq(),
                                    update_seq(), non_neg_integer()}.
parse_snapshot_marker(Body) ->
    <<StartSeq:?DCP_SIZES_BY_SEQ,
      EndSeq:?DCP_SIZES_BY_SEQ,
      Type:?DCP_SIZES_SNAPSHOT_TYPE>> = Body,
    {snapshot_marker, StartSeq, EndSeq, Type}.


-spec parse_snapshot_mutation(size(), binary(), size(), size()) ->
                                     {snapshot_mutation, #mutation{}}.
parse_snapshot_mutation(KeyLength, Body, BodyLength, ExtraLength) ->
    <<Seq:?DCP_SIZES_BY_SEQ,
      RevSeq:?DCP_SIZES_REV_SEQ,
      Flags:?DCP_SIZES_FLAGS,
      Expiration:?DCP_SIZES_EXPIRATION,
      LockTime:?DCP_SIZES_LOCK,
      MetadataLength:?DCP_SIZES_METADATA_LENGTH,
      _Nru:?DCP_SIZES_NRU_LENGTH,
      Key:KeyLength/binary,
      Rest/binary>> = Body,
    ValueLength = BodyLength - ExtraLength - KeyLength - MetadataLength,
    <<Value:ValueLength/binary,
      Metadata:MetadataLength/binary>> = Rest,
    {snapshot_mutation, #mutation{
        seq = Seq,
        rev_seq = RevSeq,
        flags = Flags,
        expiration = Expiration,
        locktime = LockTime,
        key = Key,
        value = Value,
        metadata = Metadata
    }}.

-spec parse_snapshot_deletion(size(), binary()) ->
                                     {snapshot_deletion,
                                      {update_seq(), non_neg_integer(),
                                       binary(), binary()}}.
parse_snapshot_deletion(KeyLength, Body) ->
    % XXX vmx 2014-01-07: No metadata support for now. Make it so it breaks
    % once it's there.
    MetadataLength = 0,
    <<Seq:?DCP_SIZES_BY_SEQ,
      RevSeq:?DCP_SIZES_REV_SEQ,
      MetadataLength:?DCP_SIZES_METADATA_LENGTH,
      Key:KeyLength/binary,
      Metadata:MetadataLength/binary>> = Body,
    {snapshot_deletion, {Seq, RevSeq, Key, Metadata}}.


-spec parse_failover_log(binary(), partition_version()) ->
                                {ok, partition_version()}.
parse_failover_log(Body) ->
    parse_failover_log(Body, []).
parse_failover_log(<<>>, Acc) ->
    {ok, lists:reverse(Acc)};
parse_failover_log(<<PartUuid:?DCP_SIZES_PARTITION_UUID,
                     PartSeq:?DCP_SIZES_BY_SEQ,
                     Rest/binary>>,
                   Acc) ->
    parse_failover_log(Rest, [{PartUuid, PartSeq}|Acc]).


-spec parse_stat(binary(), dcp_status(), size(), size()) ->
                        {ok, {binary(), binary()}} |
                        {error, {dcp_status(), binary()}}.
parse_stat(Body, Status, 0, _ValueLength) ->
    {error, {Status, Body}};
parse_stat(Body, ?DCP_STATUS_OK, KeyLength, ValueLength) ->
    <<Key:KeyLength/binary, Value:ValueLength/binary>> = Body,
    {ok, {Key, Value}}.

-spec parse_all_seqs(dcp_status(), binary(), list()) ->
                        {ok, list()} |
                        {error, {dcp_status(), binary()}}.
parse_all_seqs(?DCP_STATUS_OK, <<Key:?DCP_SIZES_PARTITION, Value:
        ?DCP_SIZES_BY_SEQ, Rest/binary>>, Acc) ->
    Acc2 = [{Key, Value} | Acc],
    parse_all_seqs(?DCP_STATUS_OK, Rest, Acc2);
parse_all_seqs(?DCP_STATUS_OK, <<>>, Acc) ->
    {ok, lists:reverse(Acc)};
parse_all_seqs(Status, Body, _Acc) ->
    {error, {Status, Body}}.

-spec encode_sasl_auth(binary(), binary(), request_id()) -> binary().
encode_sasl_auth(User, Passwd, RequestId) ->
    AuthType = <<"PLAIN">>,
    Body = <<AuthType/binary, $\0,
             User/binary, $\0,
             Passwd/binary, $\0>>,

    KeyLength = byte_size(AuthType),
    BodyLength = byte_size(Body),
    ExtraLength = 0,

    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_SASL_AUTH,
               KeyLength:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               0:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.


%DCP_SELECT_BUCKET command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x50
%Key length   (2,3)  : 0x0006
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000006
%Opaque       (12-15): 0x00000001
%CAS          (16-23): 0x0000000000000000
%Key          (24-29): bucket

encode_select_bucket(Bucket, RequestId) ->
    KeyLength = byte_size(Bucket),
    ExtraLength = 0,
    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_SELECT_BUCKET,
               KeyLength:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               0:?DCP_SIZES_PARTITION,
               KeyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Bucket/binary>>.

%DCP_OPEN command
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
-spec encode_open_connection(binary(), request_id()) -> binary().
encode_open_connection(Name, RequestId) ->
    Body = <<0:?DCP_SIZES_SEQNO,
             ?DCP_FLAG_PRODUCER:?DCP_SIZES_FLAGS,
             Name/binary>>,

    KeyLength = byte_size(Name),
    BodyLength = byte_size(Body),
    ExtraLength = BodyLength - KeyLength,

    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_OPEN_CONNECTION,
               KeyLength:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               0:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%DCP_STREAM_REQ command
%Field                  (offset) (value)
%Magic                  (0)    : 0x80
%Opcode                 (1)    : 0x53
%Key length             (2,3)  : 0x0000
%Extra length           (4)    : 0x30
%Data type              (5)    : 0x00
%Vbucket                (6,7)  : 0x0000
%Total body             (8-11) : 0x00000030
%Opaque                 (12-15): 0x00001000
%CAS                    (16-23): 0x0000000000000000
%  flags                (24-27): 0x00000000
%  reserved             (28-31): 0x00000000
%  start seqno          (32-39): 0x0000000000ffeedd
%  end seqno            (40-47): 0xffffffffffffffff
%  vb UUID              (48-55): 0x00000000feeddeca
%  snapshot start seqno (56-63): 0x0000000000000000
%  snapshot end seqno   (64-71): 0x0000000000ffeeff
-spec encode_stream_request(partition_id(), request_id(), non_neg_integer(),
                            update_seq(), update_seq(),
                            uuid(), update_seq(), update_seq()) -> binary().
encode_stream_request(PartId, RequestId, Flags, StartSeq, EndSeq, PartUuid,
        SnapshotStart, SnapshotEnd) ->
    Body = <<Flags:?DCP_SIZES_FLAGS,
             0:?DCP_SIZES_RESERVED,
             StartSeq:?DCP_SIZES_BY_SEQ,
             EndSeq:?DCP_SIZES_BY_SEQ,
             PartUuid:?DCP_SIZES_PARTITION_UUID,
             SnapshotStart:?DCP_SIZES_BY_SEQ,
             SnapshotEnd:?DCP_SIZES_BY_SEQ>>,

    BodyLength = byte_size(Body),
    ExtraLength = BodyLength,

    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_STREAM_REQUEST,
               0:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.


%DCP_CLOSE_STREAM command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x52
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0005
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0xdeadbeef
%CAS          (16-23): 0x0000000000000000
encode_stream_close(PartId, RequestId) ->
    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_STREAM_CLOSE,
               0:?DCP_SIZES_KEY_LENGTH,
               0,
               0,
               PartId:?DCP_SIZES_PARTITION,
               0:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    Header.

%DCP_GET_FAILOVER_LOG command
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
-spec encode_failover_log_request(partition_id(), request_id()) -> binary().
encode_failover_log_request(PartId, RequestId) ->
    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_FAILOVER_LOG_REQUEST,
               0:?DCP_SIZES_KEY_LENGTH,
               0,
               0,
               PartId:?DCP_SIZES_PARTITION,
               0:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
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
-spec encode_stat_request(binary(), partition_id() | nil, request_id()) -> binary().
encode_stat_request(Stat, PartId, RequestId) ->
    case PartId of
    nil ->
        PartId2 = 0,
        Body = Stat;
    PartId ->
        PartId2 = PartId,
        Body = <<Stat/binary, " ",
            (list_to_binary(integer_to_list(PartId)))/binary>>
    end,
    KeyLength = BodyLength = byte_size(Body),
    ExtraLength = 0,

    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_STATS,
               KeyLength:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId2:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%GET_ALL_VB_SEQNOS command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x48
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%VBucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0x00000000
%CAS          (16-23): 0x0000000000000000
encode_all_seqs_request(RequestId) ->
    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_SEQS,
               0:?DCP_SIZES_KEY_LENGTH,
               0,
               0,
               0:?DCP_SIZES_PARTITION,
               0:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary>>.

%DCP_CONTROL_BINARY_REQUEST command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x5E
%Key length   (2,3)  : 0x0016
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%VBucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x0000001a
%Opaque       (12-15): 0x00000005
%CAS          (16-23): 0x0000000000000000
%Key                 : connection_buffer_size
%Value               : 0x31303234
-spec encode_control_request(request_id(), connection | stream, integer())
                                                                -> binary().
encode_control_request(RequestId, Type, BufferSize) ->
    Key = case Type of
    connection ->
        <<"connection_buffer_size">>
    end,
    BufferSize2 = list_to_binary(integer_to_list(BufferSize)),
    Body = <<Key/binary, BufferSize2/binary>>,

    KeyLength =  byte_size(Key),
    BodyLength = byte_size(Body),
    ExtraLength = 0,

    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_DCP_CONTROL,
               KeyLength:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               0:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%DCP_BUFFER_ACK_REQUEST command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x5D
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x04
%Data type    (5)    : 0x00
%VBucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000004
%Opaque       (12-15): 0x00000000
%CAS          (16-23): 0x0000000000000000
%BufferSize   (24-27): 0x00001000
-spec encode_buffer_request(request_id(), size()) -> binary().
encode_buffer_request(RequestId, BufferSize) ->
    Extra = <<BufferSize:?DCP_SIZES_BUFFER_SIZE>>,
    BodyLength = ExtraLength = byte_size(Extra),

    Header = <<?DCP_MAGIC_REQUEST,
               ?DCP_OPCODE_DCP_BUFFER,
               0:?DCP_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               0:?DCP_SIZES_PARTITION,
               BodyLength:?DCP_SIZES_BODY,
               RequestId:?DCP_SIZES_OPAQUE,
               0:?DCP_SIZES_CAS>>,
    <<Header/binary, Extra/binary>>.

%DCP_NOOP response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x5C
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%VBucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0x00000005
%CAS          (16-23): 0x0000000000000000

-spec encode_noop_response(request_id()) -> binary().
encode_noop_response(RequestId) ->
    <<?DCP_MAGIC_RESPONSE,
      ?DCP_OPCODE_DCP_NOOP,
      0:?DCP_SIZES_KEY_LENGTH,
      0,
      0,
      0:?DCP_SIZES_PARTITION,
      0:?DCP_SIZES_BODY,
      RequestId:?DCP_SIZES_OPAQUE,
      0:?DCP_SIZES_CAS>>.
