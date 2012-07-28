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


-include_lib("couch_set_view/include/couch_set_view.hrl").

test_set_name() -> <<"couch_test_set_index_shutdown">>.
num_set_partitions() -> 8.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 8000.


main(_) ->
    test_util:init_code_path(),

    etap:plan(62),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    couch_set_view_test_util:start_server(test_set_name()),
    ok = couch_config:set("set_views", "update_interval", "0", false),

    test_partition_deletes_when_group_is_alive(),

    couch_set_view_test_util:stop_server(),
    ok.


test_partition_deletes_when_group_is_alive() ->
    create_set([0, 1, 2, 3], [4]),
    ValueGenFun1 = fun(I) -> I end,
    update_documents(0, num_docs(), ValueGenFun1),

    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    ok = gen_server:call(GroupPid, {set_auto_cleanup, false}, infinity),
    RepGroupPid = get_replica_pid(GroupPid),
    ok = gen_server:call(RepGroupPid, {set_auto_cleanup, false}, infinity),

    ok = couch_set_view:add_replica_partitions(test_set_name(), ddoc_id(), [5]),

    IndexFile = group_index_file(main),
    RepIndexFile = group_index_file(replica),
    etap:is(filelib:is_file(IndexFile), true, "Main index file exists"),
    etap:is(filelib:is_file(RepIndexFile), true, "Replica index file exists"),

    query_view(4000, []),

    etap:is(is_process_alive(GroupPid), true, "Group alive after query"),
    etap:is(is_process_alive(RepGroupPid), true, "Replica group alive after query"),

    etap:is(filelib:is_file(IndexFile), true, "Main index file exists"),
    etap:is(filelib:is_file(RepIndexFile), true, "Replica index file exists"),

    {ok, Db6} = couch_set_view_test_util:open_set_db(test_set_name(), 6),
    {ok, Db7} = couch_set_view_test_util:open_set_db(test_set_name(), 7),
    ok = couch_db:close(Db6),
    ok = couch_db:close(Db7),
    Db6Pid = couch_set_view_test_util:get_db_main_pid(Db6),
    Db7Pid = couch_set_view_test_util:get_db_main_pid(Db7),
    etap:is(is_process_alive(Db6Pid), true, "Partition 6 database is alive"),
    etap:is(is_process_alive(Db7Pid), true, "Partition 7 database is alive"),

    etap:diag("Deleting databases of partitions 6 and 7"),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 6),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 7),
    ok = timer:sleep(1000),
    etap:is(is_process_alive(Db6Pid), false, "Partition 6 database is not alive"),
    etap:is(is_process_alive(Db7Pid), false, "Partition 7 database is not alive"),

    etap:is(is_process_alive(GroupPid), true, "Group is still alive"),
    etap:is(is_process_alive(RepGroupPid), true, "Replica group is still alive"),
    etap:is(filelib:is_file(IndexFile), true, "Main index file exists"),
    etap:is(filelib:is_file(RepIndexFile), true, "Replica index file exists"),

    MainSnapshot1 = get_main_snapshot(GroupPid),
    etap:is(?set_abitmask(MainSnapshot1),
            couch_set_view_util:build_bitmask([0, 1, 2, 3]),
            "Main group's active bitmask is correct"),
    etap:is(?set_pbitmask(MainSnapshot1),
            couch_set_view_util:build_bitmask([4]),
            "Main group's passive bitmask is correct"),
    etap:is(?set_cbitmask(MainSnapshot1),
            0,
            "Main group's cleanup bitmask is correct"),

    RepSnapshot1 = get_replica_snapshot(RepGroupPid),
    etap:is(?set_abitmask(RepSnapshot1),
            0,
            "Replica group's active bitmask is correct"),
    etap:is(?set_pbitmask(RepSnapshot1),
            couch_set_view_util:build_bitmask([5]),
            "Replica group's passive bitmask is correct"),
    etap:is(?set_cbitmask(RepSnapshot1),
            0,
            "Replica group's cleanup bitmask is correct"),

    etap:diag("Marking set view partition 3 for cleanup"),
    ok = couch_set_view_group:set_state(GroupPid, [], [], [3]),
    query_view(3000, []),

    {ok, Db3} = couch_set_view_test_util:open_set_db(test_set_name(), 3),
    ok = couch_db:close(Db3),
    Db3Pid = couch_set_view_test_util:get_db_main_pid(Db3),
    etap:is(is_process_alive(Db3Pid), true, "Partition 3 database is alive"),

    etap:diag("Deleting database of partition 3"),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 3),
    ok = timer:sleep(1000),
    etap:is(is_process_alive(Db3Pid), false, "Partition 3 database is not alive"),

    etap:is(is_process_alive(GroupPid), true, "Group is still alive"),
    etap:is(is_process_alive(RepGroupPid), true, "Replica group is still alive"),
    etap:is(filelib:is_file(IndexFile), true, "Main index file exists"),
    etap:is(filelib:is_file(RepIndexFile), true, "Replica index file exists"),

    MainSnapshot2 = get_main_snapshot(GroupPid),
    etap:is(?set_abitmask(MainSnapshot2),
            couch_set_view_util:build_bitmask([0, 1, 2]),
            "Main group's active bitmask is correct"),
    etap:is(?set_pbitmask(MainSnapshot2),
            couch_set_view_util:build_bitmask([4]),
            "Main group's passive bitmask is correct"),
    etap:is(?set_cbitmask(MainSnapshot2),
            couch_set_view_util:build_bitmask([3]),
            "Main group's cleanup bitmask is correct"),

    RepSnapshot2 = get_replica_snapshot(RepGroupPid),
    etap:is(?set_abitmask(RepSnapshot2),
            0,
            "Replica group's active bitmask is correct"),
    etap:is(?set_pbitmask(RepSnapshot2),
            couch_set_view_util:build_bitmask([5]),
            "Replica group's passive bitmask is correct"),
    etap:is(?set_cbitmask(RepSnapshot2),
            0,
            "Replica group's cleanup bitmask is correct"),

    query_view(3000, []),

    {ok, Db1} = couch_set_view_test_util:open_set_db(test_set_name(), 1),
    ok = couch_db:close(Db1),
    Db1Pid = couch_set_view_test_util:get_db_main_pid(Db1),
    etap:is(is_process_alive(Db1Pid), true, "Partition 1 database is alive"),

    etap:diag("Deleting database of partition 1"),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 1),
    ok = timer:sleep(1000),
    etap:is(is_process_alive(Db1Pid), false, "Partition 1 database is not alive"),

    etap:is(is_process_alive(GroupPid), true, "Group is still alive"),
    etap:is(is_process_alive(RepGroupPid), true, "Replica group is still alive"),
    etap:is(filelib:is_file(IndexFile), true, "Main index file still exists"),
    etap:is(filelib:is_file(RepIndexFile), true, "Replica index file still exists"),

    MainSnapshot3 = get_main_snapshot(GroupPid),
    etap:is(?set_abitmask(MainSnapshot3),
            couch_set_view_util:build_bitmask([0, 2]),
            "Main group's active bitmask is correct"),
    etap:is(?set_pbitmask(MainSnapshot3),
            couch_set_view_util:build_bitmask([4]),
            "Main group's passive bitmask is correct"),
    etap:is(?set_cbitmask(MainSnapshot3),
            couch_set_view_util:build_bitmask([1, 3]),
            "Main group's cleanup bitmask is correct"),

    RepSnapshot3 = get_replica_snapshot(RepGroupPid),
    etap:is(?set_abitmask(RepSnapshot3),
            0,
            "Replica group's active bitmask is correct"),
    etap:is(?set_pbitmask(RepSnapshot3),
            couch_set_view_util:build_bitmask([5]),
            "Replica group's passive bitmask is correct"),
    etap:is(?set_cbitmask(RepSnapshot3),
            0,
            "Replica group's cleanup bitmask is correct"),

    {ok, Db5} = couch_set_view_test_util:open_set_db(test_set_name(), 5),
    ok = couch_db:close(Db5),
    Db5Pid = couch_set_view_test_util:get_db_main_pid(Db5),
    etap:is(is_process_alive(Db5Pid), true, "Partition 5 database is alive"),

    etap:diag("Deleting database of partition 5 (currently marked as replica)"),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 5),
    ok = timer:sleep(1000),
    etap:is(is_process_alive(Db5Pid), false, "Partition 5 database is not alive"),

    etap:is(is_process_alive(GroupPid), true, "Group is still alive"),
    etap:is(is_process_alive(RepGroupPid), true, "Replica group is still alive"),
    etap:is(filelib:is_file(IndexFile), true, "Main index file still exists"),
    etap:is(filelib:is_file(RepIndexFile), true, "Replica index file still exists"),

    MainSnapshot4 = get_main_snapshot(GroupPid),
    etap:is(?set_abitmask(MainSnapshot4),
            couch_set_view_util:build_bitmask([0, 2]),
            "Main group's active bitmask is correct"),
    etap:is(?set_pbitmask(MainSnapshot4),
            couch_set_view_util:build_bitmask([4]),
            "Main group's passive bitmask is correct"),
    etap:is(?set_cbitmask(MainSnapshot4),
            couch_set_view_util:build_bitmask([1, 3]),
            "Main group's cleanup bitmask is correct"),

    RepSnapshot4 = get_replica_snapshot(RepGroupPid),
    etap:is(?set_abitmask(RepSnapshot4),
            0,
            "Replica group's active bitmask is correct"),
    etap:is(?set_pbitmask(RepSnapshot4),
            0,
            "Replica group's passive bitmask is correct"),
    etap:is(?set_cbitmask(RepSnapshot4),
            couch_set_view_util:build_bitmask([5]),
            "Replica group's cleanup bitmask is correct"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


query_view(ExpectedRowCount, QueryString) ->
    {ok, {ViewResults}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, QueryString),
    etap:is(
        length(couch_util:get_value(<<"rows">>, ViewResults)),
        ExpectedRowCount,
        "Got " ++ integer_to_list(ExpectedRowCount) ++ " view rows"),
    SortedKeys =  couch_set_view_test_util:are_view_keys_sorted(
        {ViewResults}, fun(A, B) -> A < B end),
    etap:is(SortedKeys, true, "View result keys are sorted").


create_set(ActiveParts, PassiveParts) ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view:cleanup_index_files(test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    DDoc = {[
        {<<"_id">>, ddoc_id()},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc.value, doc._id); }">>}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag("Configuring set view with partitions [0 .. 31]"
              " as active and [32 .. 47] as passive"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = ActiveParts,
        passive_partitions = PassiveParts,
        use_replica_index = true
    },
    ok = couch_set_view:define_group(test_set_name(), ddoc_id(), Params).


update_documents(StartId, Count, ValueGenFun) ->
    etap:diag("Updating " ++ integer_to_list(Count) ++ " new documents"),
    DocList0 = lists:map(
        fun(I) ->
            {I rem num_set_partitions(), {[
                {<<"_id">>, doc_id(I)},
                {<<"value">>, ValueGenFun(I)}
            ]}}
        end,
        lists:seq(StartId, StartId + Count - 1)),
    DocList = [Doc || {_, Doc} <- lists:keysort(1, DocList0)],
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


doc_id(I) ->
    iolist_to_binary(io_lib:format("doc_~8..0b", [I])).


group_index_file(Type) ->
    {ok, Info} = couch_set_view:get_group_info(test_set_name(), ddoc_id()),
    binary_to_list(iolist_to_binary(
        [
            filename:join([
                couch_set_view:set_index_dir(couch_config:get("couchdb", "view_index_dir"), test_set_name()),
                atom_to_list(Type) ++ "_" ++ binary_to_list(couch_util:get_value(signature, Info))
            ]),
            ".view.1"
        ])).


get_replica_pid(GroupPid) ->
    {ok, Pid} = gen_server:call(GroupPid, replica_pid, infinity),
    Pid.


get_main_snapshot(GroupPid) ->
    {ok, Group} = gen_server:call(GroupPid, request_group, infinity),
    Group.


get_replica_snapshot(RepGroupPid) ->
    {ok, RepGroup} = gen_server:call(RepGroupPid, request_group, infinity),
    RepGroup.
