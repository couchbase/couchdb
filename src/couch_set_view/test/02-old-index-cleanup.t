#!/usr/bin/env escript
%% -*- erlang -*-

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


test_set_name() -> <<"couch_test_set_index_cleanup">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 1000.


main(_) ->
    test_util:init_code_path(),

    etap:plan(18),
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

    {ok, DDocRev} = populate_set(),

    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    query_view(num_docs(), []),
    etap:is(is_process_alive(GroupPid), true, "Group alive after query"),
    GroupSig = get_group_sig(),
    IndexFile = binary_to_list(GroupSig) ++ ".view",

    etap:is(all_index_files(), [IndexFile], "Index file found"),

    {ok, _} = update_ddoc(DDocRev),
    ok = timer:sleep(1000),
    NewGroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    etap:isnt(NewGroupPid, GroupPid, "Got new group after ddoc update"),

    OldGroupMon = erlang:monitor(process, GroupPid),
    receive
    {'DOWN', OldGroupMon, _, _, _} ->
        etap:diag("Old group shutdown after ddoc update")
    after 30000 ->
        etap:bail("Old group didn't shutdown after ddoc update")
    end,

    etap:is(is_process_alive(NewGroupPid), true, "New group alive before query"),
    NewGroupSig = get_group_sig(),
    etap:isnt(NewGroupSig, GroupSig, "New group has a different signature"),

    NewIndexFile = binary_to_list(NewGroupSig) ++ ".view",
    AllIndexFiles = all_index_files(),
    etap:is(lists:member(NewIndexFile, AllIndexFiles), true,
        "New index file found"),
    etap:is(lists:member(IndexFile, AllIndexFiles), true,
        "Old index file not deleted before cleanup"),

    etap:diag("Performing view cleanup"),
    couch_set_view:cleanup_index_files(test_set_name()),
    NewAllIndexFiles = all_index_files(),
    etap:is(lists:member(NewIndexFile, NewAllIndexFiles), true,
        "New index file found after cleanup"),
    etap:is(lists:member(IndexFile, NewAllIndexFiles), false,
        "Old index file deleted after cleanup"),

    query_view(0, "stale=ok"),
    etap:is(is_process_alive(NewGroupPid), true,
        "New group alive after query with ?stale=ok"),

    query_view(num_docs(), []),
    etap:is(is_process_alive(NewGroupPid), true,
        "New group alive after query without ?stale=ok"),

    ok = couch_set_view_test_util:delete_ddoc(test_set_name(), ddoc_id()),

    GroupMon = erlang:monitor(process, NewGroupPid),
    receive
    {'DOWN', GroupMon, _, _, _} ->
        etap:diag("New group shutdown after ddoc deleted")
    after 30000 ->
        etap:bail("New group didn't shutdown after ddoc was deleted")
    end,

    etap:diag("Performing view cleanup"),
    couch_set_view:cleanup_index_files(test_set_name()),
    etap:is(all_index_files(), [], "0 index files after ddoc deleted and cleanup"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),

    couch_set_view_test_util:stop_server(),
    ok.


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


populate_set() ->
    DDoc = {[
        {<<"_id">>, ddoc_id()},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc.value, doc._id); }">>}
            ]}}
        ]}}
    ]},
    {ok, DDocRev} = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    DocList = lists:map(
        fun(I) ->
            {[
                {<<"_id">>, iolist_to_binary(["doc", integer_to_list(I)])},
                {<<"value">>, I}
            ]}
        end,
        lists:seq(1, num_docs())),
    ok = couch_set_view_test_util:populate_set_alternated(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList),
    ok = couch_set_view_test_util:define_set_view(
        test_set_name(),
        ddoc_id(),
        num_set_partitions(),
        lists:seq(0, num_set_partitions() - 1),
        []),
    {ok, DDocRev}.


update_ddoc(DDocRev) ->
    NewDDoc = {[
        {<<"_id">>, ddoc_id()},
        {<<"_rev">>, DDocRev},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc.value, null); }">>}
            ]}}
        ]}}
    ]},
    {ok, NewRev} = couch_set_view_test_util:update_ddoc(test_set_name(), NewDDoc),
    ok = couch_set_view_test_util:define_set_view(
        test_set_name(),
        ddoc_id(),
        num_set_partitions(),
        lists:seq(0, num_set_partitions() - 1),
        []),
    {ok, NewRev}.


get_group_sig() ->
    {ok, Info} = couch_set_view:get_group_info(test_set_name(), ddoc_id()),
    couch_util:get_value(signature, Info).


all_index_files() ->
    IndexDir = couch_config:get("couchdb", "view_index_dir") ++
        "/set_view_" ++ binary_to_list(test_set_name()) ++ "_design",
    filelib:fold_files(
        IndexDir, ".*\\.view$", false,
        fun(N, A) -> [filename:basename(N) | A] end, []).
