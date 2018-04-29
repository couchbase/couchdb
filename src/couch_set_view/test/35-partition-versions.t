#!/usr/bin/env escript
%% -*- Mode: Erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
%%! -smp enable

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

-include("../../couchdb/couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").


test_set_name() -> <<"couch_test_partition_versions">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.

main(_) ->
    test_util:init_code_path(),

    etap:plan(1),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.

test() ->
    etap:diag("Testing partition versions de-duplication (MB-19245)"),
    couch_set_view_test_util:start_server(test_set_name()),
    create_set(),

    Group1 = get_group(),
    Header1 = Group1#set_view_group.index_header,
    PartitionVersions1 = Header1#set_view_index_header.partition_versions,

    % Create a header with duplicated partition versions
    Header2 = Header1#set_view_index_header{
        partition_versions = PartitionVersions1 ++ PartitionVersions1
    },
    Group2 = Group1#set_view_group{
        index_header = Header2
    },

    Group2Dedup = couch_set_view_group:remove_duplicate_partitions(Group2),
    etap:is(Group2Dedup#set_view_group.index_header, Header1,
            "Partition versions are correct, rest of header kept unchanged"),

    shutdown_group(),
    couch_set_view_test_util:stop_server(),
    ok.

create_set() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(),
        num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(),
        num_set_partitions()),
    couch_set_view:cleanup_index_files(mapreduce_view, test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>,
                        <<"function(doc, meta) {emit(meta.id, doc.value);}">>}
                ]}}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag(io_lib:format(
        "Configuring set view with partitions [0 .. ~p] as active",
        [num_set_partitions() div 2 - 1])),
    etap:diag(io_lib:format(
        "Configuring set view with partitions [~p .. ~p] as passive",
        [num_set_partitions() div 2, num_set_partitions() - 1])),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, num_set_partitions() div 2 - 1),
        passive_partitions = lists:seq(num_set_partitions() div 2,
            num_set_partitions() - 1),
        use_replica_index = false
    },
    ok = couch_set_view:define_group(
        mapreduce_view, test_set_name(), ddoc_id(), Params).

get_group_pid() ->
    couch_set_view:get_group_pid(
      mapreduce_view, test_set_name(), ddoc_id(), prod).

get_group() ->
    GroupPid = get_group_pid(),
    {ok, Group, 0} = gen_server:call(
        GroupPid, #set_view_group_req{stale = false, debug = false}, infinity),
    Group.

shutdown_group() ->
    couch_dcp_fake_server:reset(),
    GroupPid = get_group_pid(),
    couch_set_view_test_util:delete_set_dbs(test_set_name(),
        num_set_partitions()),
    MonRef = erlang:monitor(process, GroupPid),
    receive
    {'DOWN', MonRef, _, _, _} ->
        ok
    after 10000 ->
        etap:bail("Timeout waiting for group shutdown")
    end.
