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

test_set_name() -> <<"couch_test_set_index_dev_view">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/dev_test">>.
num_docs() -> 1024.  % keep it a multiple of num_set_partitions()
docs_per_partition() -> num_docs() div num_set_partitions().


main(_) ->
    test_util:init_code_path(),

    etap:plan(20),
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

    etap:diag("Testing development views"),

    test_map_query(0),
    test_map_query(1),
    test_map_query(2),
    test_map_query(3),

    test_reduce_query(0),
    test_reduce_query(1),
    test_reduce_query(2),
    test_reduce_query(3),

    test_set_active_partition(),
    test_map_query_updated(0),
    test_cleanup(0),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:stop_server(),
    ok.


test_map_query(PartitionId) ->
    setup_test(),
    ok = configure_view_group(ddoc_id(), PartitionId),

    {ok, Rows} = (catch query_map_view(<<"test">>)),
    etap:is(length(Rows), docs_per_partition(),
        "Got " ++ integer_to_list(docs_per_partition()) ++ " view rows"),
    verify_rows(Rows, PartitionId),

    shutdown_group().


test_reduce_query(PartitionId) ->
    setup_test(),
    ok = configure_view_group(ddoc_id(), PartitionId),

    {ok, Reduce} = (catch query_reduce_view(<<"testred">>)),
    Offset = (PartitionId * docs_per_partition()),
    ExpectedReduce = lists:sum(lists:seq(
        1 + Offset, Offset + docs_per_partition())),
    etap:is(Reduce, ExpectedReduce, "Reduce value is correct"),

    shutdown_group().


test_set_active_partition() ->
    setup_test(),
    ok = configure_view_group(ddoc_id(), 1),

    {ok, Rows} = (catch query_map_view(<<"test">>)),
    etap:is(length(Rows), docs_per_partition(),
        "Got " ++ integer_to_list(docs_per_partition()) ++ " view rows"),
    verify_rows(Rows, 1),

    % Set a different partition as active
    couch_set_view_dev:set_active_partition(
        mapreduce_view, test_set_name(), ddoc_id(), 2),
    {ok, Rows2} = (catch query_map_view(<<"test">>)),
    etap:is(length(Rows2), docs_per_partition(),
        "Got " ++ integer_to_list(docs_per_partition()) ++ " view rows"),
    verify_rows(Rows2, 2),

    shutdown_group().


test_map_query_updated(PartitionId) ->
    setup_test(),
    update_design_doc(),
    ok = configure_view_group(ddoc_id(), PartitionId),

    {ok, Rows} = (catch query_map_view(<<"test">>)),
    etap:is(length(Rows), docs_per_partition(),
        "Got " ++ integer_to_list(docs_per_partition()) ++ " view rows"),
    verify_rows_updated(Rows, PartitionId),

    shutdown_group().


test_cleanup(PartitionId) ->
    setup_test(),
    ok = configure_view_group(ddoc_id(), PartitionId),
    {ok, _Rows} = (catch query_map_view(<<"test">>)),

    GroupSig = get_group_sig(),
    IndexFile = "main_" ++ binary_to_list(GroupSig) ++ ".view.1",
    etap:is(all_index_files(), [IndexFile], "Index file found"),

    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), dev),
    MonRef = erlang:monitor(process, GroupPid),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),

    receive
    {'DOWN', MonRef, _, _, shutdown} ->
        ok
    after 10000 ->
        etap:bail("Timeout waiting for group to shutdown after dbs deleted")
    end,

    etap:is(all_index_files(), [], "Index file got correctly deleted").


% As the partitions are populated sequentially we can easily very them
verify_rows(Rows, PartitionId) ->
    Offset = (PartitionId * docs_per_partition()),
    DocList = lists:map(fun(Doc) ->
        {[{<<"meta">>, {[{<<"id">>, DocId}]}},
          {<<"json">>, {[{<<"value">>, Value}]}}]} = Doc,
        {<<"\"", DocId/binary, "\"">>, DocId,
            list_to_binary(integer_to_list(Value))}
    end, create_docs(1 + Offset, Offset + docs_per_partition())),
    etap:is(Rows, lists:sort(DocList), "Returned correct rows").


verify_rows_updated(Rows, PartitionId) ->
    Offset = (PartitionId * docs_per_partition()),
    DocList = lists:map(fun(Doc) ->
        {[{<<"meta">>, {[{<<"id">>, DocId}]}},
          {<<"json">>, {[{<<"value">>, Value}]}}]} = Doc,
        % the `*3` is the difference to `verify_rows/2`
        {<<"\"", DocId/binary, "\"">>, DocId,
            list_to_binary(integer_to_list(Value*3))}
    end, create_docs(1 + Offset, Offset + docs_per_partition())),
    etap:is(Rows, lists:sort(DocList), "Returned correct rows").


query_map_view(ViewName) ->
    etap:diag("Querying map view " ++ binary_to_list(ddoc_id()) ++ "/" ++
        binary_to_list(ViewName)),
    Req = #set_view_group_req{
        stale = false,
        category = dev
    },
    {ok, View, Group, _} = couch_set_view:get_map_view(
        test_set_name(), ddoc_id(), ViewName, Req),

    FoldFun = fun({{{json, Key}, DocId}, {_PartId, {json, Value}}}, _, Acc) ->
        {ok, [{Key, DocId, Value} | Acc]}
    end,
    ViewArgs = #view_query_args{
        run_reduce = false,
        view_name = ViewName
    },
    {ok, _, Rows} = couch_set_view:fold(Group, View, FoldFun, [], ViewArgs),
    couch_set_view:release_group(Group),
    {ok, lists:reverse(Rows)}.


query_reduce_view(ViewName) ->
    etap:diag("Querying reduce view " ++ binary_to_list(ddoc_id()) ++ "/" ++
        binary_to_list(ViewName)),
    Req = #set_view_group_req{
        stale = false,
        category = dev
    },
    {ok, View, Group, _} = couch_set_view:get_reduce_view(
        test_set_name(), ddoc_id(), ViewName, Req),

    FoldFun = fun(Key, {json, Red}, Acc) ->
        {ok, [{Key, ejson:decode(Red)} | Acc]}
    end,
    ViewArgs = #view_query_args{
        run_reduce = true,
        view_name = ViewName
    },
    {ok, Rows} = couch_set_view:fold_reduce(Group, View, FoldFun, [], ViewArgs),
    couch_set_view:release_group(Group),
    case Rows of
    [{_Key, RedValue}] ->
        {ok, RedValue};
    [] ->
        empty
    end.


setup_test() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>}
                ]}},
                {<<"testred">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                    {<<"reduce">>, <<"_sum">>}
                ]}}
            ]}}
        ]}}
    ]},
    populate_set(DDoc).


update_design_doc() ->
    etap:diag("Updating design document in master database"),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value*3); }">>}
                ]}}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc).


create_docs(From, To) ->
    lists:map(
        fun(I) ->
            {[
                {<<"meta">>, {[{<<"id">>, iolist_to_binary(["doc", integer_to_list(I)])}]}},
                {<<"json">>, {[{<<"value">>, I}]}}
            ]}
        end,
        lists:seq(From, To)).


populate_set(DDoc) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    DocList = create_docs(1, num_docs()),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


configure_view_group(DDocId, PartitionId) ->
    etap:diag("Configuring view group"),
    try
        ok = couch_set_view_dev:define_group(
            mapreduce_view, test_set_name(), DDocId, PartitionId)
    catch _:Error ->
        Error
    end.


shutdown_group() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), dev),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    MonRef = erlang:monitor(process, GroupPid),
    receive
    {'DOWN', MonRef, _, _, _} ->
        ok
    after 10000 ->
        etap:bail("Timeout waiting for group shutdown")
    end.


get_group_sig() ->
    {ok, Info} = couch_set_view:get_group_info(
       mapreduce_view, test_set_name(), ddoc_id(), dev),
    couch_util:get_value(signature, Info).


all_index_files() ->
    IndexDir = couch_set_view:set_index_dir(
        couch_config:get("couchdb", "view_index_dir"), test_set_name(), dev),
    filelib:fold_files(
        IndexDir, ".*\\.view\\.[0-9]+$", false,
        fun(N, A) -> [filename:basename(N) | A] end, []).
