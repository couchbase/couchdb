#!/usr/bin/env escript
%% -*- erlang -*-
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

-define(MAX_WAIT_TIME, 600 * 1000).

% from couch_set_view.hrl
-record(set_view_params, {
    max_partitions = 0,
    active_partitions = [],
    passive_partitions = [],
    use_replica_index = false
}).

test_set_name() -> <<"couch_test_set_index_replica_compact">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 123789.


main(_) ->
    test_util:init_code_path(),

    etap:plan(16),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    couch_set_view_test_util:start_server(),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    populate_set(),

    etap:diag("Marking partitions [ 8 .. 63 ] as replicas"),
    ok = couch_set_view:add_replica_partitions(test_set_name(), ddoc_id(), lists:seq(8, 63)),

    verify_group_info_before_replica_removal(),
    wait_for_replica_full_update(get_replica_group_info()),
    verify_group_info_before_replica_removal(),

    etap:diag("Removing partitions [ 8 .. 63 ] from replica set"),
    ok = couch_set_view:remove_replica_partitions(test_set_name(), ddoc_id(), lists:seq(8, 63)),
    verify_group_info_after_replica_removal(),

    DiskSizeBefore = replica_index_disk_size(),

    etap:diag("Trigerring replica group compaction"),
    {ok, CompactPid} = couch_set_view_compactor:start_compact(test_set_name(), ddoc_id(), replica),
    etap:diag("Waiting for replica group compaction to finish"),
    Ref = erlang:monitor(process, CompactPid),
    receive
    {'DOWN', Ref, process, CompactPid, normal} ->
        ok;
    {'DOWN', Ref, process, CompactPid, Reason} ->
        etap:bail("Failure compacting replica group: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for replica group compaction to finish")
    end,

    RepGroupInfo = get_replica_group_info(),
    {Stats} = couch_util:get_value(stats, RepGroupInfo),
    etap:is(couch_util:get_value(compactions, Stats), 1, "Replica had 1 full compaction in stats"),
    etap:is(couch_util:get_value(cleanups, Stats), 1, "Replica had 1 full cleanup in stats"),
    verify_group_info_after_replica_compact(),

    DiskSizeAfter = replica_index_disk_size(),
    etap:is(DiskSizeAfter < DiskSizeBefore, true, "Index file size is smaller after compaction"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


verify_group_info_before_replica_removal() ->
    etap:diag("Verifying replica group info before removing replica partitions"),
    RepGroupInfo = get_replica_group_info(),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo),
        lists:seq(8, 63),
        "Replica group has [ 8 .. 63 ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as cleanup partitions").


verify_group_info_after_replica_removal() ->
    etap:diag("Verifying replica group info after removing replica partitions"),
    RepGroupInfo = get_replica_group_info(),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as passive partitions"),
    CleanupParts = couch_util:get_value(cleanup_partitions, RepGroupInfo),
    {Stats} = couch_util:get_value(stats, RepGroupInfo),
    CleanupHist = couch_util:get_value(cleanup_history, Stats),
    case length(CleanupHist) > 0 of
    true ->
        etap:is(
            length(CleanupParts),
            0,
            "Replica group has a right value for cleanup partitions");
    false ->
        etap:is(
            length(CleanupParts) > 0,
            true,
           "Replica group has a right value for cleanup partitions")
    end,
    etap:is(
        ordsets:intersection(CleanupParts, lists:seq(0, 7)),
        [],
        "Replica group doesn't have any cleanup partition with ID in [ 0 .. 7 ]").


verify_group_info_after_replica_compact() ->
    etap:diag("Verifying replica group info after compaction"),
    RepGroupInfo = get_replica_group_info(),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as cleanup partitions").


wait_for_replica_full_update(RepGroupInfo) ->
    etap:diag("Waiting for a full replica group update"),
    Pid = spawn(fun() ->
        wait_replica_update_loop(get_replica_updates_count(RepGroupInfo))
    end),
    Ref = erlang:monitor(process, Pid),
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Failure waiting for full replica group update: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for replica group update")
    end.


wait_replica_update_loop(Updates) ->
    case get_replica_updates_count() > Updates of
    true ->
        ok;
    false ->
        ok = timer:sleep(500),
        wait_replica_update_loop(Updates)
    end.


get_replica_updates_count() ->
    get_replica_updates_count(get_replica_group_info()).


get_replica_updates_count(RepGroupInfo) ->
    {Stats} = couch_util:get_value(stats, RepGroupInfo),
    Updates = couch_util:get_value(full_updates, Stats),
    true = is_integer(Updates),
    Updates.


get_replica_group_info() ->
    {ok, MainInfo} = couch_set_view:get_group_info(test_set_name(), ddoc_id()),
    {RepInfo} = couch_util:get_value(replica_group_info, MainInfo),
    RepInfo.


replica_index_disk_size() ->
    Info = get_replica_group_info(),
    Size = couch_util:get_value(disk_size, Info),
    true = is_integer(Size),
    true = (Size >= 0),
    Size.


populate_set() ->
    couch_set_view:cleanup_index_files(test_set_name()),
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    DDoc = {[
        {<<"_id">>, ddoc_id()},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, null); }">>},
                {<<"reduce">>, <<"_count">>}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    DocList = lists:map(
        fun(I) ->
            {[
                {<<"_id">>, iolist_to_binary(["doc", integer_to_list(I)])},
                {<<"value">>, I}
            ]}
        end,
        lists:seq(1, num_docs())),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList),
    etap:diag("Configuring set view with partitions [0 .. 7] as active"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, 7),
        passive_partitions = [],
        use_replica_index = true
    },
    ok = couch_set_view:define_group(test_set_name(), ddoc_id(), Params).
