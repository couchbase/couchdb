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

-type dcp_status()    :: non_neg_integer().
-type dcp_data_type() :: 0..255.
-type request_id()    :: non_neg_integer().
-type size()          :: non_neg_integer().
-type socket()        :: port().

% Those types are duplicates from couch_set_view.hrl
-type uint64()                   :: 0..18446744073709551615.
-type partition_id()             :: non_neg_integer().
-type update_seq()               :: non_neg_integer().
-type uuid()                     :: uint64().
-type partition_seq()            :: {partition_id(), update_seq()}.
% Manipulate via ordsets or orddict, keep it ordered by partition id.
-type partition_seqs()           :: ordsets:ordset(partition_seq()).
-type partition_version()        :: [{uuid(), update_seq()}].
% Manipulate via ordsets or orddict, keep it ordered by partition id.
-type partition_versions()       :: ordsets:ordset({partition_id(), partition_version()}).
