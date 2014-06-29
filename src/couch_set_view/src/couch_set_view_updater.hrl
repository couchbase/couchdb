% -*- Mode: Erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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


-include_lib("couch_set_view/include/couch_set_view.hrl").


-record(writer_acc, {
    parent,
    owner,
    group,
    last_seqs = orddict:new(),
    part_versions = orddict:new(),
    compactor_running,
    write_queue,
    initial_build,
    view_empty_kvs,
    kvs = [],
    kvs_size = 0,
    state = updating_active,
    final_batch = false,
    max_seqs,
    stats = #set_view_updater_stats{},
    tmp_dir = nil,
    initial_seqs,
    max_insert_batch_size,
    tmp_files = dict:new(),
    throttle = 0,
    force_flush = false
}).
