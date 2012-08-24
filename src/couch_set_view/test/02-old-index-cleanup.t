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


test_set_name() -> <<"couch_test_set_index_cleanup">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.
ddoc_id_copy() -> <<"_design/test_copy">>.
num_docs() -> 1000.


main(_) ->
    test_util:init_code_path(),

    etap:plan(69),
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

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    ok = populate_set(),

    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    query_view(ddoc_id(), num_docs(), []),
    etap:is(is_process_alive(GroupPid), true, "Group alive after query"),
    GroupSig = get_group_sig(),
    IndexFile = "main_" ++ binary_to_list(GroupSig) ++ ".view.1",

    etap:is(all_index_files(), [IndexFile], "Index file found"),

    RawGroupSig = get_raw_sig(ddoc_id()),
    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [{test_set_name(), {ddoc_id(), RawGroupSig}}],
            "Correct group entry in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [{{test_set_name(), RawGroupSig}, GroupPid}],
            "Correct group entry in couch_sig_to_setview_pid ets table"),

    ok = update_ddoc(ddoc_id()),
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

    RawNewGroupSig = get_raw_sig(ddoc_id()),
    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [{test_set_name(), {ddoc_id(), RawNewGroupSig}}],
            "Correct group entry in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawNewGroupSig}),
            [{{test_set_name(), RawNewGroupSig}, NewGroupPid}],
            "Correct group entry in couch_sig_to_setview_pid ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [],
            "Old group entry not in couch_sig_to_setview_pid ets table anymore"),

    NewIndexFile = "main_" ++ binary_to_list(NewGroupSig) ++ ".view.1",
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

    query_view(ddoc_id(), 0, "stale=ok"),
    etap:is(is_process_alive(NewGroupPid), true,
        "New group alive after query with ?stale=ok"),

    query_view(ddoc_id(), num_docs(), []),
    etap:is(is_process_alive(NewGroupPid), true,
        "New group alive after query without ?stale=ok"),

    etap:diag("Creating ddoc copy with different _id"),
    ok = create_ddoc_copy(ddoc_id_copy()),
    ok = timer:sleep(1000),

    RawNewGroupCopySig = get_raw_sig(ddoc_id_copy()),
    etap:is(RawNewGroupCopySig, RawNewGroupSig, "Group copy has same signature"),
    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [{test_set_name(), {ddoc_id(), RawNewGroupSig}},
             {test_set_name(), {ddoc_id_copy(), RawNewGroupCopySig}}],
            "Correct group entries in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawNewGroupSig}),
            [{{test_set_name(), RawNewGroupSig}, NewGroupPid}],
            "Correct group entry in couch_sig_to_setview_pid ets table"),

    etap:diag("Deleting original ddoc"),
    ok = couch_set_view_test_util:delete_ddoc(test_set_name(), ddoc_id()),

    GroupMon = erlang:monitor(process, NewGroupPid),
    receive
    {'DOWN', GroupMon, _, _, _} ->
        etap:diag("New group shutdown after ddoc deleted")
    after 30000 ->
        etap:bail("New group didn't shutdown after ddoc was deleted")
    end,
    % Let couch_set_view have some time to process the group's down message
    ok = timer:sleep(1500),

    AllIndexFiles2 = all_index_files(),
    etap:is(lists:member(NewIndexFile, AllIndexFiles2), true,
        "Index file found after deleting original ddoc (because copy is not deleted)"),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [],
            "No group entry in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawNewGroupSig}),
            [],
            "No group entry in couch_sig_to_setview_pid ets table"),

    NewGroupPid2 = couch_set_view:get_group_pid(test_set_name(), ddoc_id_copy()),
    etap:isnt(NewGroupPid2, NewGroupPid, "New group pid after deleting original ddoc"),
    query_view(ddoc_id_copy(), num_docs(), "stale=ok"),
    etap:diag("Got same query results after deleting original ddoc and querying "
              "ddoc copy with ?stale=ok"),
    etap:is(is_process_alive(NewGroupPid2), true,
        "New group copy alive after query with ?stale=ok"),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [{test_set_name(), {ddoc_id_copy(), RawNewGroupSig}}],
            "New group entry in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawNewGroupSig}),
            [{{test_set_name(), RawNewGroupSig}, NewGroupPid2}],
            "New group entry in couch_sig_to_setview_pid ets table"),

    etap:diag("Deleting ddoc copy"),
    ok = couch_set_view_test_util:delete_ddoc(test_set_name(), ddoc_id_copy()),

    etap:diag("Performing view cleanup"),
    couch_set_view:cleanup_index_files(test_set_name()),
    etap:is(all_index_files(), [], "0 index files after ddoc deleted and cleanup"),

    % Let couch_set_view have some time to process the group's down message
    ok = timer:sleep(1500),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [],
            "No group entry in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawNewGroupSig}),
            [],
            "No group entry in couch_sig_to_setview_pid ets table"),

    test_recreate_ddoc_with_copy(),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),

    couch_set_view_test_util:stop_server(),
    ok.


test_recreate_ddoc_with_copy() ->
    etap:diag("Recreating design doc with a copy"),

    update_ddoc(ddoc_id()),
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    RawGroupSig = get_raw_sig(ddoc_id()),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [{test_set_name(), {ddoc_id(), RawGroupSig}}],
            "Correct group entry in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [{{test_set_name(), RawGroupSig}, GroupPid}],
            "Correct group entry in couch_sig_to_setview_pid ets table"),

    query_view(ddoc_id(), num_docs(), []),

    etap:diag("Creating ddoc copy"),
    ok = create_ddoc_copy(ddoc_id_copy()),

    GroupPidCopy = couch_set_view:get_group_pid(test_set_name(), ddoc_id_copy()),
    etap:is(GroupPidCopy, GroupPid, "DDoc copy has same group pid"),
    RawGroupSigCopy = get_raw_sig(ddoc_id_copy()),
    etap:is(RawGroupSigCopy, RawGroupSig, "DDoc copy has same signature"),

    query_view(ddoc_id_copy(), num_docs(), "stale=ok"),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [{test_set_name(), {ddoc_id(), RawGroupSig}},
             {test_set_name(), {ddoc_id_copy(), RawGroupSig}}],
            "Correct group entries in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [{{test_set_name(), RawGroupSig}, GroupPid}],
            "Correct group entry in couch_sig_to_setview_pid ets table"),

    etap:diag("Deleting ddoc copy"),
    ok = couch_set_view_test_util:delete_ddoc(test_set_name(), ddoc_id_copy()),
    ok = timer:sleep(1000),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [],
            "No group entry in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [],
            "No group entry in couch_sig_to_setview_pid ets table"),

    query_view(ddoc_id(), num_docs(), "stale=ok"),

    etap:diag("Deleting original ddoc"),
    ok = couch_set_view_test_util:delete_ddoc(test_set_name(), ddoc_id()),
    % Let couch_set_view have some time to process the group's down message
    ok = timer:sleep(1500),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [],
            "No group entry in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [],
            "No group entry in couch_sig_to_setview_pid ets table"),

    couch_set_view:cleanup_index_files(test_set_name()),

    etap:diag("Creating original ddoc again"),
    update_ddoc(ddoc_id()),
    GroupPid2 = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    query_view(ddoc_id(), num_docs(), []),

    etap:diag("Creating ddoc copy again"),
    ok = create_ddoc_copy(ddoc_id_copy()),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [{test_set_name(), {ddoc_id(), RawGroupSig}},
             {test_set_name(), {ddoc_id_copy(), RawGroupSig}}],
            "Correct group entries in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [{{test_set_name(), RawGroupSig}, GroupPid2}],
            "Correct group entry in couch_sig_to_setview_pid ets table"),

    etap:diag("Killing view group process"),
    couch_util:shutdown_sync(GroupPid2),
    ok = timer:sleep(1000),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [],
            "couch_setview_name_to_sig ets table is empty"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [],
            "couch_sig_to_setview_pid ets table is empty"),

    etap:diag("Starting again view group process"),
    GroupPid3 = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    etap:isnt(GroupPid3, GroupPid2, "Got a different view group pid"),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [{test_set_name(), {ddoc_id(), RawGroupSig}},
             {test_set_name(), {ddoc_id_copy(), RawGroupSig}}],
            "Correct group entries in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [{{test_set_name(), RawGroupSig}, GroupPid3}],
            "Correct group entry in couch_sig_to_setview_pid ets table"),

    etap:diag("Deleting master database"),
    couch_set_view_test_util:delete_set_db(test_set_name(), master),
    ok = timer:sleep(1000),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [],
            "couch_setview_name_to_sig ets table is empty"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [],
            "couch_sig_to_setview_pid ets table is empty"),

    etap:diag("Recreating database set"),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    etap:diag("Adding design document again, but not opening its view group"),
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), ddoc(ddoc_id())),

    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [{test_set_name(), {ddoc_id(), RawGroupSig}}],
            "Correct alias in couch_setview_name_to_sig ets table"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [],
            "couch_sig_to_setview_pid ets table is empty"),

    ViewManagerPid = whereis(couch_set_view),

    etap:diag("Deleting master database"),
    couch_set_view_test_util:delete_set_db(test_set_name(), master),
    ok = timer:sleep(1000),

    etap:is(is_process_alive(ViewManagerPid), true, "View manager didn't die"),
    etap:is(ets:lookup(couch_setview_name_to_sig, test_set_name()),
            [],
            "couch_setview_name_to_sig ets table is empty"),
    etap:is(ets:lookup(couch_sig_to_setview_pid, {test_set_name(), RawGroupSig}),
            [],
            "couch_sig_to_setview_pid ets table is empty"),
    ok.


query_view(DDocId, ExpectedRowCount, QueryString) ->
    {ok, {ViewResults}} = couch_set_view_test_util:query_view(
        test_set_name(), DDocId, <<"test">>, QueryString),
    etap:is(
        length(couch_util:get_value(<<"rows">>, ViewResults)),
        ExpectedRowCount,
        "Got " ++ integer_to_list(ExpectedRowCount) ++ " view rows"),
    SortedKeys =  couch_set_view_test_util:are_view_keys_sorted(
        {ViewResults}, fun(A, B) -> A < B end),
    etap:is(SortedKeys, true, "View result keys are sorted").


populate_set() ->
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"language">>, <<"javascript">>},
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(doc.value, meta.id); }">>}
                ]}}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    DocList = lists:map(
        fun(I) ->
            {[
                {<<"meta">>, {[{<<"id">>, iolist_to_binary(["doc", integer_to_list(I)])}]}},
                {<<"json">>, {[
                    {<<"value">>, I}
                    ]}}
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
    ok.


ddoc(Id) ->
    {[
        {<<"meta">>, {[{<<"id">>, Id}]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(doc.value, null); }">>}
                ]}}
            ]}}
        ]}}
    ]}.


update_ddoc(DDocId) ->
    NewDDoc = ddoc(DDocId),
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), NewDDoc),
    ok = couch_set_view_test_util:define_set_view(
        test_set_name(),
        ddoc_id(),
        num_set_partitions(),
        lists:seq(0, num_set_partitions() - 1),
        []),
    ok.


create_ddoc_copy(CopyId) ->
    DDoc = ddoc(CopyId),
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc).


get_group_sig() ->
    {ok, Info} = couch_set_view:get_group_info(test_set_name(), ddoc_id()),
    couch_util:get_value(signature, Info).


get_raw_sig(DDocId) ->
    Pid = couch_set_view:get_group_pid(test_set_name(), DDocId),
    {ok, Sig} = gen_server:call(Pid, get_sig, infinity),
    Sig.


all_index_files() ->
    IndexDir = couch_set_view:set_index_dir(
        couch_config:get("couchdb", "view_index_dir"), test_set_name()),
    filelib:fold_files(
        IndexDir, ".*\\.view\\.[0-9]+$", false,
        fun(N, A) -> [filename:basename(N) | A] end, []).
