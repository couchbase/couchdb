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


-define(dbname(SetName, PartId),
      <<SetName/binary, $/, (?l2b(integer_to_list(PartId)))/binary>>).

-define(master_dbname(SetName), <<SetName/binary, "/master">>).

-define(set_num_partitions(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.num_partitions).

-define(set_abitmask(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.abitmask).

-define(set_pbitmask(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.pbitmask).

-define(set_cbitmask(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.cbitmask).

-define(set_seqs(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.seqs).

-define(set_purge_seqs(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.purge_seqs).


% Used to configure a new set view.
-record(set_view_params, {
    max_partitions = 0,
    % list of initial active partitions (list of integers in the range 0 .. N - 1)
    active_partitions = [],
    % list of initial passive partitions (list of integers in the range 0 .. N - 1)
    passive_partitions = []
}).

-record(set_view_index_header, {
    % maximum number of partitions this set view supports
    num_partitions = nil,  % nil means not yet defined
    % active partitions bitmap
    abitmask = 0,
    % passive partitions bitmap
    pbitmask = 0,
    % cleanup partitions bitmap
    cbitmask = 0,
    % update seq numbers from each partition, format: [ {PartitionId, Seq} ]
    seqs = [],
    % purge seq numbers from each partition, format: [ {PartitionId, Seq} ]
    purge_seqs = [],
    id_btree_state = nil,
    view_states = nil
}).

-record(set_view_group, {
    sig = nil,
    fd = nil,
    set_name,
    name,
    def_lang,
    design_options = [],
    views,
    lib,
    id_btree = nil,
    query_server = nil,
    waiting_delayed_commit = nil,
    ref_counter = nil,
    index_header = nil,
    db_set = nil
}).

-record(set_view, {
    id_num,
    % update seq numbers from each partition, format: [ {PartitionId, Seq} ]
    update_seqs = [],
    % purge seq numbers from each partition, format: [ {PartitionId, Seq} ]
    purge_seqs = [],
    map_names = [],
    def,
    btree = nil,
    reduce_funs = [],
    options = []
}).
