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

-define(etap_match(Got, Expected, Desc),
        etap:fun_is(fun(XXXXXX) ->
            case XXXXXX of Expected -> true; _ -> false end
        end, Got, Desc)).

test_set_name() -> <<"couch_test_set_index_errors">>.
num_set_partitions() -> 4.
num_docs() -> 1000.


main(_) ->
    test_util:init_code_path(),

    etap:plan(31),
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

    etap:diag("Testing map function with a runtime error"),
    test_map_runtime_error(),
    etap:diag("Testing map function with a runtime error for a group of a views"),
    test_map_runtime_error_multiple_views(),
    etap:diag("Testing map function with invalid syntax"),
    test_map_syntax_error(),

    etap:diag("Testing case where map function emits a key that is too long"),
    test_too_long_map_key(),

    etap:diag("Testing case where map function emits a value that is too long"),
    test_too_long_map_value(),

    etap:diag("Testing case where too many KV pairs are emitted for a single document"),
    test_too_many_keys_per_doc(),

    etap:diag("Testing builtin reduce _sum function with a runtime error (initial build)"),
    test_builtin_reduce_sum_runtime_error(),
    etap:diag("Testing builtin reduce _sum function with a runtime error (incr update)"),
    test_builtin_reduce_sum_runtime_error2(),
    etap:diag("Testing builtin reduce _stats function with a runtime error (initial build)"),
    test_builtin_reduce_stats_runtime_error(),
    etap:diag("Testing builtin reduce _stats function with a runtime error (incr update)"),
    test_builtin_reduce_stats_runtime_error2(),

    etap:diag("Testing with an invalid builtin reduce function"),
    test_invalid_builtin_reduce_error(),
    etap:diag("Testing reduce function with a runtime error"),
    test_reduce_runtime_error(),
    etap:diag("Testing reduce function with invalid syntax"),
    test_reduce_syntax_error(),

    etap:diag("Testing reduce function producing a too large reduction"),
    test_reduce_too_large_reduction(),
    etap:diag("Testing reduce function producing a too large re-reduction"),
    test_reduce_too_large_rereduction(),

    couch_set_view_test_util:stop_server(),
    ok.


test_map_runtime_error() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(doc.value.foo.bar, 1); }">>}
            ]}}
        ]}}
        ]}}
    ]},
    populate_set(DDoc),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), DDocId, prod),
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


test_map_runtime_error_multiple_views() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"views">>, {[
            {<<"test1">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(doc.value, 1); }">>}
            ]}},
            {<<"test2">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(doc.value.foo.bar, 2); }">>}
            ]}},
            {<<"test3">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(doc.value, 3); }">>}
            ]}}
        ]}}
        ]}}
    ]},
    populate_set(DDoc, 4),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), DDocId, prod),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult2 = (catch query_map_view(DDocId, <<"test2">>, false)),
    etap:is(QueryResult2, {ok, []}, "Map view test2 query returned 0 rows"),

    QueryResult1 = (catch query_map_view(DDocId, <<"test1">>, false)),
    ExpectedRows1 = [
        {{{json, <<"1">>}, <<"doc1">>}, {json, <<"1">>}},
        {{{json, <<"2">>}, <<"doc2">>}, {json, <<"1">>}},
        {{{json, <<"3">>}, <<"doc3">>}, {json, <<"1">>}},
        {{{json, <<"4">>}, <<"doc4">>}, {json, <<"1">>}}
    ],
    etap:is(QueryResult1, {ok, ExpectedRows1}, "Map view test1 query returned 4 rows"),

    QueryResult3 = (catch query_map_view(DDocId, <<"test3">>, false)),
    ExpectedRows3 = [
        {{{json, <<"1">>}, <<"doc1">>}, {json, <<"3">>}},
        {{{json, <<"2">>}, <<"doc2">>}, {json, <<"3">>}},
        {{{json, <<"3">>}, <<"doc3">>}, {json, <<"3">>}},
        {{{json, <<"4">>}, <<"doc4">>}, {json, <<"3">>}}
    ],
    etap:is(QueryResult3, {ok, ExpectedRows3}, "Map view test3 query returned 4 rows"),

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
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, 1); ">>}
            ]}}
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


test_too_long_map_key() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) {\n"
                              "var key = meta.id;\n"
                              "while (key.length < 4096) {\n"
                              "    key = key.concat(key);\n"
                              "}\n"
                              "emit(key, null);\n"
                              "}">>}
            ]}}
        ]}}
        ]}}
    ]},
    populate_set(DDoc, 1),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), DDocId, prod),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult = (catch query_map_view(DDocId, <<"test">>, false)),
    ExpectedResult = {ok, []},
    etap:is(QueryResult, ExpectedResult, "No key emitted when a key is too long"),

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("view group died")
    after 5000 ->
        etap:is(is_process_alive(GroupPid), true, "View group is still alive")
    end,
    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_too_long_map_value() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    ok = mapreduce:set_max_kv_size_per_doc(0),
    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) {\n"
                              "var val = meta.id;\n"
                              "while (val.length < (16 * 1024 * 1024)) {\n"
                              "    val = val.concat(val);\n"
                              "}\n"
                              "emit(meta.id, val);\n"
                              "}">>}
            ]}}
        ]}}
        ]}}
    ]},
    populate_set(DDoc, 1),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), DDocId, prod),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult = (catch query_map_view(DDocId, <<"test">>, false)),
    ExpectedResult = {error, <<"value emitted for key `<ud>\"doc1\"</ud>`, document "
                               "`<ud>doc1</ud>`, is too big (16777218 bytes)">>},
    etap:is(QueryResult, ExpectedResult, "Got an error when a value is too long"),

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("view group died")
    after 5000 ->
        etap:is(is_process_alive(GroupPid), true, "View group is still alive")
    end,
    ok = mapreduce:set_max_kv_size_per_doc(1 * 1024 * 1024),
    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_too_many_keys_per_doc() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) {\n"
                              "for (var i = 0; i < 70000; i++) {\n"
                              "    emit(i, meta.id);\n"
                              "}\n"
                              "}">>}
            ]}}
        ]}}
        ]}}
    ]},
    populate_set(DDoc, 1),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), DDocId, prod),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult = (catch query_map_view(DDocId, <<"test">>, false)),
    ExpectedResult = {error, <<"Too many (70000) keys emitted for document"
                               " `<ud>doc1</ud>` (maximum allowed is 65535">>},
    etap:is(QueryResult, ExpectedResult,
            "Got an error when too many keys are emitted per document"),

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("view group died")
    after 5000 ->
        etap:is(is_process_alive(GroupPid), true, "View group is still alive")
    end,
    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_builtin_reduce_sum_runtime_error() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, 'foobar'); }">>},
                {<<"reduce">>, <<"_sum">>}
            ]}}
        ]}}
        ]}}
    ]},
    populate_set(DDoc),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), DDocId, prod),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult = try
        query_reduce_view(DDocId, <<"test">>, false)
    catch _:Error ->
        Error
    end,

    etap:is(QueryResult,
            {error, <<"Reducer: Error building index for view `test`, reason: Value is not a number (key \"doc1\")">>},
            "Received error response"),

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("view group died")
    after 5000 ->
        etap:is(is_process_alive(GroupPid), true, "View group is still alive")
    end,
    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_builtin_reduce_sum_runtime_error2() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
            {<<"language">>, <<"javascript">>},
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                    {<<"reduce">>, <<"_sum">>}
                ]}}
            ]}}
        ]}}
    ]},

    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    add_documents(0, num_docs(), true),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), DDocId, prod),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult1 = try
        query_reduce_view(DDocId, <<"test">>, false),
        ok
    catch _:Error1 ->
        Error1
    end,
    etap:is(QueryResult1,
            ok, <<"Query triggering initial index build did not throw any exception">>),


    add_documents(num_docs(), 2 * num_docs(), false),

    QueryResult2 = try
        query_reduce_view(DDocId, <<"test">>, false)
    catch _:Error2 ->
        Error2
    end,
    DocId = doc_id(num_docs()),
    etap:is(QueryResult2,
            {error, <<"Reducer: Error updating index for view `test`, reason: Value is not a number (key \"", DocId/binary, "\")">>},
            "Received error response"),

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("view group died")
    after 5000 ->
        etap:is(is_process_alive(GroupPid), true, "View group is still alive")
    end,
    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_builtin_reduce_stats_runtime_error() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
            {<<"language">>, <<"javascript">>},
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, 'foobar'); }">>},
                    {<<"reduce">>, <<"_stats">>}
                ]}}
            ]}}
        ]}}
    ]},
    populate_set(DDoc),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), DDocId, prod),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult = try
        query_reduce_view(DDocId, <<"test">>, false)
    catch _:Error ->
        Error
    end,

    etap:is(QueryResult,
            {error, <<"Reducer: Error building index for view `test`, reason: Value is not a number (key \"doc1\")">>},
            "Received error response"),

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("view group died")
    after 5000 ->
        etap:is(is_process_alive(GroupPid), true, "View group is still alive")
    end,
    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_builtin_reduce_stats_runtime_error2() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
            {<<"language">>, <<"javascript">>},
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                    {<<"reduce">>, <<"_stats">>}
                ]}}
            ]}}
        ]}}
    ]},

    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    add_documents(0, num_docs(), true),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), DDocId, prod),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult1 = try
        query_reduce_view(DDocId, <<"test">>, false),
        ok
    catch _:Error1 ->
        Error1
    end,
    etap:is(QueryResult1,
            ok, <<"Query triggering initial index build did not throw any exception">>),


    add_documents(num_docs(), 2 * num_docs(), false),

    QueryResult2 = try
        query_reduce_view(DDocId, <<"test">>, false)
    catch _:Error2 ->
        Error2
    end,
    DocId = doc_id(num_docs()),
    etap:is(QueryResult2,
            {error, <<"Reducer: Error updating index for view `test`, reason: Value is not a number (key \"", DocId/binary, "\")">>},
            "Received error response"),

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
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, 1); }">>},
                {<<"reduce">>, <<"_foobar">>}
            ]}}
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
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, 1); }">>},
                {<<"reduce">>, <<"function(key, values, rereduce) { return values[0].foo.bar; }">>}
            ]}}
        ]}}
        ]}}
    ]},
    populate_set(DDoc),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), DDocId, prod),
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
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, 'foobar'); }">>},
                {<<"reduce">>, <<"function(key, values, rereduce) { return sum(values);">>}
            ]}}
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


test_reduce_too_large_reduction() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, 'foobar'); }">>},
                {<<"reduce">>, <<"function(key, values, rereduce) {"
                                 "  if (rereduce) return 'foo';"
                                 "  var r = 'qwerty';"
                                 "  while (r.length < 65536) {"
                                 "    r = r.concat(r);"
                                 "  }"
                                 "  return r;"
                                 "}">>}
            ]}}
        ]}}
        ]}}
    ]},
    populate_set(DDoc),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(mapreduce_view, test_set_name(), DDocId, prod),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult = try
        query_reduce_view(DDocId, <<"test">>, false)
    catch _:Error ->
        Error
    end,

    etap:is(QueryResult,
            {error, <<"reduction too large">>},
            "Received error response with too large reduce value"),

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("view group died")
    after 5000 ->
        etap:is(is_process_alive(GroupPid), true, "View group is still alive")
    end,
    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_reduce_too_large_rereduction() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDocId = <<"_design/test">>,
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, DDocId}]}},
        {<<"json">>, {[
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, 'foobar'); }">>},
                {<<"reduce">>, <<"function(key, values, rereduce) {"
                                 "  if (!rereduce) return 'foo';"
                                 "  var r = 'qwerty';"
                                 "  while (r.length < 65536) {"
                                 "    r = r.concat(r);"
                                 "  }"
                                 "  return r;"
                                 "}">>}
            ]}}
        ]}}
        ]}}
    ]},
    populate_set(DDoc),

    ok = configure_view_group(DDocId, [0, 1, 2, 3], []),
    GroupPid = couch_set_view:get_group_pid(mapreduce_view, test_set_name(), DDocId, prod),
    MonRef = erlang:monitor(process, GroupPid),

    QueryResult = try
        query_reduce_view(DDocId, <<"test">>, false)
    catch _:Error ->
        Error
    end,

    etap:is(QueryResult,
            {error, <<"reduction too large">>},
            "Received error response with too large rereduce value"),

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("view group died")
    after 5000 ->
        etap:is(is_process_alive(GroupPid), true, "View group is still alive")
    end,
    couch_util:shutdown_sync(GroupPid),
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
    {ok, lists:reverse(Rows)}.


query_reduce_view(DDocId, ViewName, Stale) ->
    etap:diag("Querying reduce view " ++ binary_to_list(DDocId) ++ "/" ++
        binary_to_list(ViewName) ++ "with ?group=true"),
    {ok, View, Group, _} = couch_set_view:get_reduce_view(
        test_set_name(), DDocId, ViewName, #set_view_group_req{stale = Stale}),
    FoldFun = fun(Key, Red, Acc) -> {ok, [{Key, Red} | Acc]} end,
    ViewArgs = #view_query_args{
        run_reduce = true,
        view_name = <<"test">>
    },
    {ok, Rows} = couch_set_view:fold_reduce(Group, View, FoldFun, [], ViewArgs),
    couch_set_view:release_group(Group),
    case Rows of
    [{_Key, {json, RedValue}}] ->
        {ok, RedValue};
    [] ->
        empty
    end.


populate_set(DDoc) ->
    populate_set(DDoc, num_docs()).

populate_set(DDoc, NumDocs) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
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
        lists:seq(1, NumDocs)),
    ok = couch_set_view_test_util:populate_set_alternated(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


doc_id(K) ->
    iolist_to_binary(io_lib:format("doc_~8..0b", [K])).


add_documents(StartId, Count, IsNumber) ->
    DocValFun = case IsNumber of
    true ->
        fun(K) -> K end;
    _ ->
        fun(K) -> doc_id(K) end
    end,
    etap:diag("Adding " ++ integer_to_list(Count) ++ " new documents"),
    DocList0 = lists:map(
        fun(I) ->
            {I rem num_set_partitions(), {[
                {<<"meta">>, {[{<<"id">>, doc_id(I)}]}},
                {<<"json">>, {[
                    {<<"value">>, DocValFun(I)}
                ]}}
            ]}}
        end,
        lists:seq(StartId, StartId + Count - 1)),

    DocList = [Doc || {_, Doc} <- lists:keysort(1, DocList0)],
    ok = couch_set_view_test_util:populate_set_sequentially(
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
        couch_set_view:define_group(
            mapreduce_view, test_set_name(), DDocId, Params)
    catch _:Error ->
        Error
    end.
