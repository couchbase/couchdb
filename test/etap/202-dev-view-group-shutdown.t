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

% Test shutdown of development view groups - old couchdb view groups where
% the database that has the design document is different from the database
% that has the documents to index (design documents live in a "master"
% database while documents live in "vbucket" databases).

data_db_name() -> <<"couch_test_dev_view_group_shutdown/0">>.
master_db_name() -> <<"couch_test_dev_view_group_shutdown/master">>.
ddoc_name() -> <<"dev_foo">>.

-record(user_ctx, {
    name = null,
    roles = [],
    handler
}).


main(_) ->
    test_util:init_code_path(),

    etap:plan(7),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            io:format(standard_error, "Test died abnormally: ~p", [Other]),
            etap:bail(Other)
    end,
    ok.


test() ->
    couch_server_sup:start_link(test_util:config_files()),

    create_db(data_db_name()),
    create_db(master_db_name()),
    create_docs(),
    create_design_doc(),

    ViewResults1 = query_view(),
    etap:is(ViewResults1,
            [{<<"doc1">>, 1}, {<<"doc2">>, 2}, {<<"doc3">>, 3}],
            "Correct map view query results"),

    GroupPid1 = get_group_pid(),
    MonRef1 = erlang:monitor(process, GroupPid1),

    GroupFileName1 = get_group_index_file_name(),
    etap:is(lists:member(GroupFileName1, list_index_files()),
            true,
            "Found view group index file"),

    etap:diag("Updating design document in master database"),
    update_design_doc(),

    receive
    {'DOWN', MonRef1, process, GroupPid1, normal} ->
         etap:diag("View group shutdown after ddoc update");
    {'DOWN', MonRef1, process, GroupPid1, _Reason} ->
         etap:bail("View group shutdown after ddoc update with unexpected reason")
    after 10000 ->
         etap:bail("Timeout waiting for view group shutdown")
    end,

    ViewResults2 = query_view(),
    etap:is(ViewResults2,
            [{<<"doc1">>, 3}, {<<"doc2">>, 6}, {<<"doc3">>, 9}],
            "Correct map view query results after ddoc update"),

    cleanup_index_files(),

    GroupFileName2 = get_group_index_file_name(),
    etap:is(lists:member(GroupFileName2, list_index_files()),
            true,
            "Found new view group index file"),
    etap:is(lists:member(GroupFileName1, list_index_files()),
            false,
            "Old view group index file not found after cleanup"),

    GroupPid2 = get_group_pid(),
    MonRef2 = erlang:monitor(process, GroupPid2),

    etap:diag("Deleting design document from master database"),
    delete_design_doc(),

    receive
    {'DOWN', MonRef2, process, GroupPid2, normal} ->
         etap:diag("View group shutdown after ddoc deleted");
    {'DOWN', MonRef2, process, GroupPid2, _Reason2} ->
         etap:bail("View group shutdown after ddoc deleted with unexpected reason")
    after 10000 ->
         etap:bail("Timeout waiting for view group shutdown")
    end,

    etap:diag("Creating design document again"),
    create_design_doc(),

    ViewResults3 = query_view(),
    etap:is(ViewResults3,
            [{<<"doc1">>, 1}, {<<"doc2">>, 2}, {<<"doc3">>, 3}],
            "Correct map view query results after ddoc recreated"),

    GroupPid3 = get_group_pid(),
    MonRef3 = erlang:monitor(process, GroupPid3),

    etap:diag("Deleting data database"),
    delete_db(data_db_name()),

    receive
    {'DOWN', MonRef3, process, GroupPid3, shutdown} ->
         etap:diag("View group shutdown after data database deleted");
    {'DOWN', MonRef3, process, GroupPid3, _Reason3} ->
         etap:bail("View group shutdown after data database  deleted with unexpected reason")
    after 10000 ->
         etap:bail("Timeout waiting for view group shutdown")
    end,

    etap:is(list_index_files(),
            [],
            "No index files after data database deleted"),

    delete_db(master_db_name()),
    couch_server_sup:stop(),
    ok.


create_db(DbName) ->
    delete_db(DbName),
    {ok, Db} = couch_db:create(
        DbName, [{user_ctx, #user_ctx{roles = [<<"_admin">>]}}]),
    ok = couch_db:close(Db).


delete_db(DbName) ->
    couch_server:delete(DbName, [{user_ctx, #user_ctx{roles = [<<"_admin">>]}}]).


create_docs() ->
    {ok, Db} = couch_db:open_int(
        data_db_name(), [{user_ctx, #user_ctx{roles = [<<"_admin">>]}}]),
    Doc1 = couch_doc:from_json_obj({[
        {<<"meta">>, {[
            {<<"id">>, <<"doc1">>}
        ]}},
        {<<"json">>, {[
            {<<"value">>, 1}
        ]}}
    ]}),
    Doc2 = couch_doc:from_json_obj({[
        {<<"meta">>, {[
            {<<"id">>, <<"doc2">>}
        ]}},
        {<<"json">>, {[
            {<<"value">>, 2}
        ]}}
    ]}),
    Doc3 = couch_doc:from_json_obj({[
        {<<"meta">>, {[
            {<<"id">>, <<"doc3">>}
        ]}},
        {<<"json">>, {[
            {<<"value">>, 3}
        ]}}
    ]}),
    ok = couch_db:update_docs(Db, [Doc1, Doc2, Doc3], [sort_docs]),
    {ok, _} = couch_db:ensure_full_commit(Db),
    ok = couch_db:close(Db).


create_design_doc() ->
    DDoc = couch_doc:from_json_obj({[
        {<<"meta">>, {[
            {<<"id">>, <<"_design/", (ddoc_name())/binary>>}
        ]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"bar">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>}
                ]}}
            ]}}
        ]}}
    ]}),
    save_ddoc(DDoc).


update_design_doc() ->
    DDoc = couch_doc:from_json_obj({[
        {<<"meta">>, {[
            {<<"id">>, <<"_design/", (ddoc_name())/binary>>}
        ]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"bar">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value * 3); }">>}
                ]}}
            ]}}
        ]}}
    ]}),
    save_ddoc(DDoc).


delete_design_doc() ->
    DDoc = couch_doc:from_json_obj({[
        {<<"meta">>, {[
            {<<"id">>, <<"_design/", (ddoc_name())/binary>>},
            {<<"deleted">>, true}
        ]}}
    ]}),
    save_ddoc(DDoc).


save_ddoc(DDoc) ->
    {ok, Db} = couch_db:open_int(
        master_db_name(), [{user_ctx, #user_ctx{roles = [<<"_admin">>]}}]),
    ok = couch_db:update_doc(Db, DDoc, []),
    {ok, _} = couch_db:ensure_full_commit(Db),
    ok = couch_db:close(Db).


query_view() ->
    {ok, DataDb} = couch_db:open_int(data_db_name(), []),
    {ok, MasterDb} = couch_db:open_int(master_db_name(), []),
    DDocId = <<"_design/", (ddoc_name())/binary>>,
    {ok, View, _Group} = couch_view:get_map_view(
        DataDb, {MasterDb, DDocId}, <<"bar">>, false),
    FoldFun = fun({{Key, _DocId}, Value}, _OffsetReds, Acc) ->
        {ok, [{Key, Value} | Acc]}
    end,
    {ok, _, Results} = couch_view:fold(View, FoldFun, [], []),
    ok = couch_db:close(DataDb),
    ok = couch_db:close(MasterDb),
    lists:reverse(Results).


get_group_pid() ->
    couch_view:get_group_server({data_db_name(), master_db_name()}, ddoc_name()).


get_group_index_file_name() ->
    {ok, Info} = couch_view:get_group_info({data_db_name(), master_db_name()}, ddoc_name()),
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    BaseName = binary_to_list(couch_util:get_value(signature, Info)) ++ ".view",
    RootDir ++ "/." ++ binary_to_list(master_db_name()) ++ "_design" ++ "/" ++ BaseName.


list_index_files() ->
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    filelib:wildcard(
        RootDir ++ "/." ++ binary_to_list(master_db_name()) ++ "_design" ++ "/*.view").


cleanup_index_files() ->
    {ok, MasterDb} = couch_db:open_int(master_db_name(), []),
    couch_view:cleanup_index_files(MasterDb),
    ok = couch_db:close(MasterDb).
