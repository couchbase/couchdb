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

-define(ON_ERROR_DEFAULT, continue).

-record(simple_index_spec, {
    database,
    ddoc_database = nil, % Couchbase specific
    ddoc_id,
    index_name
}).

% It's always remote.
-record(merged_index_spec, {
    url,
    ejson_spec
}).

-record(index_merge, {
   indexes = [],   % [ #simple_index_spec{} | #merged_index_spec{} ]
   callback,
   user_acc,
   % parameters that matter only when there are remote views to merge
   conn_timeout = 60000,             % milliseconds
   on_error = ?ON_ERROR_DEFAULT,     % 'continue' | 'stop'
   ddoc_revision = nil,              % nil | auto | Revision
   http_params = nil,
   user_ctx = nil,
   % extra is for index implementation specific properties
   extra = nil
}).

-record(merge_params, {
    index_name,
    queue,
    collector,
    skip = 0,
    limit = 10000000000, % Huge number to simplify logic (from couch_db.hrl)
    row_acc = [],
    % Indexer specific record
    extra = nil
}).

-record(httpdb, {
   url,
   timeout,
   headers = [{"Accept", "application/json"}],
   ibrowse_options = []
}).
