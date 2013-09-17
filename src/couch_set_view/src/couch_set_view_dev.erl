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

% This module is mainly a proxy to couch_set_view. It's specifically
% for development views.
-module(couch_set_view_dev).

-export([define_group/4, set_active_partition/4]).


-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").


-spec define_group(atom(), binary(), binary(), partition_id()) -> 'ok'.
define_group(Mod, SetName, DDocId, PartitionId) ->
    Params = #set_view_params{
        max_partitions = ?MAX_NUM_PARTITIONS,
        active_partitions = [PartitionId],
        passive_partitions = [],
        use_replica_index = false
    },
    try
        GroupPid = couch_set_view:get_group_pid(Mod, SetName, DDocId, dev),
        case couch_set_view_group:define_view(GroupPid, Params) of
        ok ->
            ok;
        Error ->
            throw(Error)
        end
    catch throw:{error, empty_group} ->
        ok
    end.


-spec set_active_partition(atom(), binary(), binary(), partition_id()) -> 'ok'.
set_active_partition(Mod, SetName, DDocId, ActivePartition) ->
    try
        GroupPid = couch_set_view:get_group_pid(Mod, SetName, DDocId, dev),
        CleanupPartitions =
            lists:seq(0, ?MAX_NUM_PARTITIONS - 1) -- [ActivePartition],
        case couch_set_view_group:set_state(
                GroupPid, [ActivePartition], [], CleanupPartitions) of
        ok ->
            ok;
        Error ->
            throw(Error)
        end
    catch throw:{error, empty_group} ->
        ok
    end.
