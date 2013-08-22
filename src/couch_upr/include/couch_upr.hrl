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

-define(UPR_WIRE_HEADER_LEN, 24).

-define(UPR_WIRE_MAGIC_REQUEST, 16#80).
-define(UPR_WIRE_MAGIC_RESPONSE, 16#81).
-define(UPR_WIRE_OPCODE_STREAM_MESSAGE, 16#04).
-define(UPR_WIRE_OPCODE_REQUEST, 16#01).
-define(UPR_WIRE_OPCODE_RESPONSE, 16#02).
-define(UPR_WIRE_REQUEST_TYPE_SNAPSHOT_START, 16#01).
-define(UPR_WIRE_REQUEST_TYPE_SNAPSHOT_END, 16#02).
-define(UPR_WIRE_REQUEST_TYPE_MUTATION, 16#03).
-define(UPR_WIRE_REQUEST_TYPE_DELETION, 16#04).
-define(UPR_WIRE_REQUEST_TYPE_STREAM, 16#01).
-define(UPR_WIRE_REQUEST_TYPE_OK, 16#01).
% The sizes are in byte as the protocol is byte-aligned
-define(UPR_WIRE_SIZES_KEY_LENGTH, 2).
-define(UPR_WIRE_SIZES_PARTITION, 2).
-define(UPR_WIRE_SIZES_BODY, 4).
-define(UPR_WIRE_SIZES_REQUEST_ID, 4).
-define(UPR_WIRE_SIZES_CAS, 8).
-define(UPR_WIRE_SIZES_BY_SEQ, 8).
-define(UPR_WIRE_SIZES_REV_SEQ, 8).
-define(UPR_WIRE_SIZES_FLAGS, 4).
-define(UPR_WIRE_SIZES_EXPIRY, 4).
-define(UPR_WIRE_SIZES_LOCK, 4).
-define(UPR_WIRE_SIZES_KEY, 5).
-define(UPR_WIRE_SIZES_VALUE, 7).
-define(UPR_WIRE_SIZES_PARTITION_UUID, 8).
