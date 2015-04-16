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

-define(DCP_HEADER_LEN, 24).

-define(DCP_MAGIC_REQUEST, 16#80).
-define(DCP_MAGIC_RESPONSE, 16#81).
-define(DCP_OPCODE_OPEN_CONNECTION, 16#50).
-define(DCP_OPCODE_STREAM_REQUEST, 16#53).
-define(DCP_OPCODE_STREAM_CLOSE, 16#52).
-define(DCP_OPCODE_FAILOVER_LOG_REQUEST, 16#54).
-define(DCP_OPCODE_STREAM_END, 16#55).
-define(DCP_OPCODE_SNAPSHOT_MARKER, 16#56).
-define(DCP_OPCODE_MUTATION, 16#57).
-define(DCP_OPCODE_DELETION, 16#58).
-define(DCP_OPCODE_EXPIRATION, 16#59).
-define(DCP_OPCODE_STATS, 16#10).
-define(DCP_OPCODE_SEQS, 16#48).
-define(DCP_OPCODE_SASL_AUTH, 16#21).
-define(DCP_OPCODE_SELECT_BUCKET, 16#89).
-define(DCP_OPCODE_DCP_CONTROL, 16#5E).
-define(DCP_OPCODE_DCP_BUFFER, 16#5D).
-define(DCP_OPCODE_DCP_NOOP, 16#5C).
-define(DCP_FLAG_OK, 16#00).
-define(DCP_FLAG_STATE_CHANGED, 16#01).
-define(DCP_FLAG_CONSUMER, 16#00).
-define(DCP_FLAG_PRODUCER, 16#01).
-define(DCP_FLAG_NOFLAG, 16#00).
-define(DCP_FLAG_DISKONLY, 16#02).
-define(DCP_FLAG_USELATEST_ENDSEQNO, 16#04).
-define(DCP_REQUEST_TYPE_MUTATION, 16#03).
-define(DCP_REQUEST_TYPE_DELETION, 16#04).
-define(DCP_STATUS_OK, 16#00).
-define(DCP_STATUS_KEY_NOT_FOUND, 16#01).
-define(DCP_STATUS_KEY_EEXISTS, 16#02).
-define(DCP_STATUS_ROLLBACK, 16#23).
-define(DCP_STATUS_NOT_MY_VBUCKET, 16#07).
-define(DCP_STATUS_ERANGE, 16#22).
-define(DCP_STATUS_SASL_AUTH_FAILED, 16#20).
-define(DCP_STATUS_NOT_SUPPORTED, 16#83).
-define(DCP_STATUS_TMP_FAIL, 16#86).
-define(DCP_SNAPSHOT_TYPE_MEMORY, 16#1).
-define(DCP_SNAPSHOT_TYPE_DISK, 16#2).
-define(DCP_SNAPSHOT_TYPE_MASK, 16#3).
-define(DCP_DATA_TYPE_RAW, 16#0).
-define(DCP_DATA_TYPE_JSON, 16#1).
-define(DCP_DATA_TYPE_RAW_COMPRESSED, 16#2).
-define(DCP_DATA_TYPE_JSON_COMPRESSED, 16#3).
% The sizes are in bits
-define(DCP_SIZES_KEY_LENGTH, 16).
-define(DCP_SIZES_PARTITION, 16).
-define(DCP_SIZES_BODY, 32).
-define(DCP_SIZES_OPAQUE, 32).
-define(DCP_SIZES_CAS, 64).
-define(DCP_SIZES_BY_SEQ, 64).
-define(DCP_SIZES_REV_SEQ, 64).
-define(DCP_SIZES_FLAGS, 32).
-define(DCP_SIZES_EXPIRATION, 32).
-define(DCP_SIZES_LOCK, 32).
-define(DCP_SIZES_KEY, 40).
-define(DCP_SIZES_VALUE, 56).
-define(DCP_SIZES_PARTITION_UUID, 64).
-define(DCP_SIZES_RESERVED, 32).
-define(DCP_SIZES_STATUS, 16).
-define(DCP_SIZES_SEQNO, 32).
-define(DCP_SIZES_METADATA_LENGTH, 16).
-define(DCP_SIZES_NRU_LENGTH, 8).
-define(DCP_SIZES_SNAPSHOT_TYPE, 32).
-define(DCP_SIZES_BUFFER_SIZE, 32).
-define(DCP_BUFFER_ACK_THRESHOLD, 0.2).

% NOTE vmx 2014-01-16: In ep-engine the maximum size is currently 25
-define(DCP_MAX_FAILOVER_LOG_SIZE, 25).


-record(mutation, {
    seq = 0         :: update_seq(),
    rev_seq = 0     :: non_neg_integer(),
    flags = 0       :: non_neg_integer(),
    expiration = 0  :: non_neg_integer(),
    locktime = 0    :: non_neg_integer(),
    key = <<>>      :: binary(),
    value = <<>>    :: binary(),
    metadata = <<>> :: binary()
}).

-record(dcp_doc, {
    id = <<>>       :: binary(),
    body = <<>>     :: binary(),
    % data_type corresponds to content_meta in #doc{} from couch_db.hrl
    data_type = 0   :: 0..255,
    partition = 0   :: partition_id(),
    cas = 0         :: uint64(),
    rev_seq = 0     :: uint64(),
    seq = 0         :: update_seq(),
    flags = 0       :: non_neg_integer(),
    expiration = 0  :: non_neg_integer(),
    deleted = false :: boolean()
}).
