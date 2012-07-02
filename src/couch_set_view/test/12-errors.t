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

% from couch_db.hrl
-define(MIN_STR, <<>>).
-define(MAX_STR, <<255>>).

-record(view_query_args, {
    start_key,
    end_key,
    start_docid = ?MIN_STR,
    end_docid = ?MAX_STR,
    direction = fwd,
    inclusive_end = true,
    limit = 10000000000,
    skip = 0,
    group_level = 0,
    view_type = nil,
    include_docs = false,
    conflicts = false,
    stale = false,
    multi_get = false,
    callback = nil,
    list = nil,
    run_reduce = true,
    keys = nil,
    view_name = nil,
    debug = false
}).

-define(etap_match(Got, Expected, Desc),
        etap:fun_is(fun(XXXXXX) ->
            case XXXXXX of Expected -> true; _ -> false end
        end, Got, Desc)).

test_set_name() -> <<"couch_test_set_index_errors">>.
num_set_partitions() -> 4.
num_docs() -> 1000.


main(_) ->
    test_util:init_code_path(),

    etap:plan(12),
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
    ok = couch_config:set("set_views", "update_interval", "0", false),

    test_partition_not_found_when_group_is_configured(),
    test_partition_not_found_when_group_starts(),

    etap:diag("Testing map function with a runtime error"),
    test_map_runtime_error(),
    etap:diag("Testing map function with invalid syntax"),
    test_map_syntax_error(),

    etap:diag("Testing builtin reduce function with a runtime error"),
    test_builtin_reduce_runtime_error(),
    etap:diag("Testing with an invalid builtin reduce function"),
    test_invalid_builtin_reduce_error(),
    etap:diag("Testing reduce function with a runtime error"),
    test_reduce_runtime_error(),
    etap:diag("Testing reduce function with invalid syntax"),
    test_reduce_syntax_error(),

    couch_set_view_test_util:stop_server(),
    ok.


test_partition_not_found_when_group_is_configured() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"_id">>, DDocId},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, 1); }">>}
            ]}}
        ]}}
    ]},
    populate_set(DDoc),

    etap:diag("Deleting database of partition 1 before configuring view group"),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 0),

    ConfigError = configure_view_group(DDocId, [0, 1, 2, 3], []),
    ?etap_match(
        ConfigError,
        {error, {db_open_error, _DbName, {not_found, no_db_file}, _Text}},
        "Got an error when configuring view group"),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_partition_not_found_when_group_starts() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"_id">>, DDocId},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, 1); }">>}
            ]}}
        ]}}
    ]},
    populate_set(DDoc),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    lists:foreach(
        fun(PartId) ->
            {ok, Db} = couch_set_view_test_util:open_set_db(test_set_name(), PartId),
            {ok, _} = couch_db:ensure_full_commit(Db),
            ok = couch_db:close(Db)
        end,
        [master, 0, 1, 2, 3]),
    GroupPid1 = couch_set_view:get_group_pid(test_set_name(), DDocId),
    couch_util:shutdown_sync(GroupPid1),
    couch_util:shutdown_sync(whereis(couch_server)),

    etap:diag("Deleting database of active partition 1 after view group shutdown"),
    DbFile = iolist_to_binary([test_set_name(), "/0.couch.1"]),
    DbDir = couch_config:get("couchdb", "database_dir"),
    ok = file:delete(filename:join([DbDir, DbFile])),
    ok = timer:sleep(1000),

    SetViewServerBefore = whereis(couch_set_view),
    MonRef = erlang:monitor(process, SetViewServerBefore),

    try
        couch_set_view:get_group_pid(test_set_name(), DDocId),
        etap:bail("No failure opening view group after deleting an active partition database")
    catch _:Error ->
        DbName = iolist_to_binary([test_set_name(), "/0"]),
        ?etap_match(
            Error,
            {error, {db_open_error, DbName, {not_found, no_db_file}, _Text}},
            "Got an error when opening view group")
    end,

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("set_view server died")
    after 5000 ->
        ok
    end,

    SetViewServerAfter = whereis(couch_set_view),
    etap:is(SetViewServerAfter, SetViewServerBefore, "couch_set_view server didn't die"),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_map_runtime_error() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"_id">>, DDocId},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc.value.foo.bar, 1); }">>}
            ]}}
        ]}}
    ]},
    populate_set(DDoc),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(test_set_name(), DDocId),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult = (catch query_map_view(DDocId, <<"test">>, false)),
    etap:is(QueryResult, {ok, []}, "Map view query returned 0 rows"),

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("view group died")
    after 5000 ->
        etap:is(is_process_alive(GroupPid), true, "View group is still alive")
    end,
    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_map_syntax_error() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"_id">>, DDocId},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, 1); ">>}
            ]}}
        ]}}
    ]},
    Result = try
        couch_set_view_test_util:update_ddoc(test_set_name(), DDoc)
    catch throw:Error ->
        Error
    end,
    ?etap_match(Result, {invalid_design_doc, _}, "Design document creation got rejected"),
    {invalid_design_doc, Reason} = Result,
    etap:diag("Design document creation error reason: " ++ binary_to_list(Reason)),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_builtin_reduce_runtime_error() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"_id">>, DDocId},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, 'foobar'); }">>},
                {<<"reduce">>, <<"_sum">>}
            ]}}
        ]}}
    ]},
    populate_set(DDoc),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(test_set_name(), DDocId),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult = try
        query_reduce_view(DDocId, <<"test">>, false)
    catch _:Error ->
        Error
    end,
    ?etap_match(QueryResult, {error, _}, "Received error response"),

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("view group died")
    after 5000 ->
        etap:is(is_process_alive(GroupPid), true, "View group is still alive")
    end,
    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_invalid_builtin_reduce_error() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"_id">>, DDocId},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, 1); }">>},
                {<<"reduce">>, <<"_foobar">>}
            ]}}
        ]}}
    ]},
    Result = try
        couch_set_view_test_util:update_ddoc(test_set_name(), DDoc)
    catch throw:Error ->
        Error
    end,
    ?etap_match(Result, {invalid_design_doc, _}, "Design document creation got rejected"),
    {invalid_design_doc, Reason} = Result,
    etap:diag("Design document creation error reason: " ++ binary_to_list(Reason)),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_reduce_runtime_error() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"_id">>, DDocId},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, 1); }">>},
                {<<"reduce">>, <<"function(key, values, rereduce) { return values[0].foo.bar; }">>}
            ]}}
        ]}}
    ]},
    populate_set(DDoc),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(test_set_name(), DDocId),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult = try
        query_reduce_view(DDocId, <<"test">>, false)
    catch _:Error ->
        Error
    end,
    ?etap_match(QueryResult, {error, _}, "Received error response"),

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("view group died")
    after 5000 ->
        etap:is(is_process_alive(GroupPid), true, "View group is still alive")
    end,
    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_reduce_syntax_error() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"_id">>, DDocId},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, 'foobar'); }">>},
                {<<"reduce">>, <<"function(key, values, rereduce) { return sum(values);">>}
            ]}}
        ]}}
    ]},

    Result = try
        couch_set_view_test_util:update_ddoc(test_set_name(), DDoc)
    catch throw:Error ->
        Error
    end,
    ?etap_match(Result, {invalid_design_doc, _}, "Design document creation got rejected"),
    {invalid_design_doc, Reason} = Result,
    etap:diag("Design document creation error reason: " ++ binary_to_list(Reason)),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


query_map_view(DDocId, ViewName, Stale) ->
    etap:diag("Querying map view " ++ binary_to_list(DDocId) ++ "/" ++
        binary_to_list(ViewName)),
    {ok, View, Group, _} = couch_set_view:get_map_view(
        test_set_name(), DDocId, ViewName, #set_view_group_req{stale = Stale}),
    FoldFun = fun({{Key, DocId}, {_PartId, Value}}, _, Acc) ->
        {ok, [{{Key, DocId}, Value} | Acc]}
    end,
    ViewArgs = #view_query_args{
        run_reduce = true,
        view_name = <<"test">>
    },
    {ok, _, Rows} = couch_set_view:fold(Group, View, FoldFun, [], ViewArgs),
    couch_set_view:release_group(Group),
    {ok, Rows}.


query_reduce_view(DDocId, ViewName, Stale) ->
    etap:diag("Querying reduce view " ++ binary_to_list(DDocId) ++ "/" ++
        binary_to_list(ViewName) ++ "with ?group=true"),
    {ok, View, Group, _} = couch_set_view:get_reduce_view(
        test_set_name(), DDocId, ViewName, #set_view_group_req{stale = Stale}),
    KeyGroupFun = fun({_Key1, _}, {_Key2, _}) -> true end,
    FoldFun = fun(Key, Red, Acc) -> {ok, [{Key, Red} | Acc]} end,
    ViewArgs = #view_query_args{
        run_reduce = true,
        view_name = <<"test">>
    },
    {ok, Rows} = couch_set_view:fold_reduce(Group, View, FoldFun, [], KeyGroupFun, ViewArgs),
    couch_set_view:release_group(Group),
    case Rows of
    [{_Key, RedValue}] ->
        {ok, RedValue};
    [] ->
        empty
    end.


populate_set(DDoc) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
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
        DocList).


configure_view_group(DDocId, Active, Passive) ->
    etap:diag("Configuring view group"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = Active,
        passive_partitions = Passive
    },
    try
        couch_set_view:define_group(test_set_name(), DDocId, Params)
    catch _:Error ->
        Error
    end.
