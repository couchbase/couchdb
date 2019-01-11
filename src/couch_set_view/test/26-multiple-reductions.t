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

-define(MAX_WAIT_TIME, 900 * 1000).

-define(etap_match(Got, Expected, Desc),
        etap:fun_is(fun(XXXXXX) ->
            case XXXXXX of Expected -> true; _ -> false end
        end, Got, Desc)).

test_set_name() -> <<"couch_test_set_index_multiple_reductions">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
initial_num_docs() -> 70400.  % must be multiple of num_set_partitions()

main(_) ->
    test_util:init_code_path(),

    etap:plan(34),
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

    ValueGenFun1 = fun(I) -> I end,
    create_set(),
    update_documents(0, initial_num_docs(), ValueGenFun1),
    trigger_update(),

    QueryResult1 = query_reduce_view(<<"test2">>, doc_id(1000), doc_id(5000)),
    etap:is(QueryResult1, <<"4001">>, "Query result is \"4001\""),

    QueryResult2 = query_reduce_view(<<"test5">>, [doc_id(1000),1], [doc_id(5000),1]),
    etap:is(QueryResult2, 4001, "Query result is 4001"),

    QueryResult3 = query_reduce_view(<<"test2">>, doc_id(5000), doc_id(15000)),
    etap:is(QueryResult3, <<"10001">>, "Query result is \"10001\""),

    QueryResult4 = query_reduce_view(<<"test4">>, [doc_id(2000),1], [doc_id(3500),1]),
    etap:is(QueryResult4, -1501, "Query result is -1501"),

    verify_btrees_1(ValueGenFun1),
    compact_view_group(),
    verify_btrees_1(ValueGenFun1),

    QueryResult11 = query_reduce_view(<<"test2">>, doc_id(1000), doc_id(5000)),
    etap:is(QueryResult11, <<"4001">>, "Query result is \"4001\""),

    QueryResult22 = query_reduce_view(<<"test5">>, [doc_id(1000),1], [doc_id(5000),1]),
    etap:is(QueryResult22, 4001, "Query result is 4001"),

    QueryResult33 = query_reduce_view(<<"test2">>, doc_id(5000), doc_id(15000)),
    etap:is(QueryResult33, <<"10001">>, "Query result is \"10001\""),

    QueryResult44 = query_reduce_view(<<"test4">>, [doc_id(2000),1], [doc_id(3500),1]),
    etap:is(QueryResult44, -1501, "Query result is -1501"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:stop_server(),
    ok.


create_set() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view:cleanup_index_files(mapreduce_view, test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                {<<"reduce">>, <<"_count">>}
            ]}},
            {<<"test2">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                {<<"reduce">>, <<"function(key, values, rereduce) {"
                                 "  if (rereduce) {"
                                 "     var s = 0;"
                                 "     for (var i = 0; i < values.length; ++i) {"
                                 "         s += Number(values[i]);"
                                 "     }"
                                 "     return String(s);"
                                 "  }"
                                 "  return String(values.length);"
                                 "}">>}
            ]}},
            {<<"test3">>, {[
                {<<"map">>, <<"function(doc, meta) { emit([meta.id,1], doc.value); }">>},
                {<<"reduce">>, <<"_sum">>}
            ]}},
            {<<"test4">>, {[
                {<<"map">>, <<"function(doc, meta) { emit([meta.id,1], doc.value); }">>},
                {<<"reduce">>, <<"function(key, values, rereduce) {"
                                 "  if (rereduce) return sum(values);"
                                 "  return -values.length;"
                                 "}">>}
            ]}},
            {<<"test5">>, {[
                {<<"map">>, <<"function(doc, meta) { emit([meta.id,1], doc.value); }">>},
                {<<"reduce">>, <<"_count">>}
            ]}}
        ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag("Configuring set view with partitions [0 .. 63] as active"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, 63),
        passive_partitions = [],
        use_replica_index = false
    },
    ok = couch_set_view:define_group(
        mapreduce_view, test_set_name(), ddoc_id(), Params).


update_documents(StartId, Count, ValueGenFun) ->
    etap:diag("Updating " ++ integer_to_list(Count) ++ " documents"),
    DocList0 = lists:map(
        fun(I) ->
            {I rem num_set_partitions(), {[
                {<<"meta">>, {[{<<"id">>, doc_id(I)}]}},
                {<<"json">>, {[
                    {<<"value">>, ValueGenFun(I)}
                ]}}
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


compact_view_group() ->
    {ok, CompactPid} = couch_set_view_compactor:start_compact(
        mapreduce_view, test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, CompactPid),
    etap:diag("Waiting for view group compaction to finish"),
    receive
    {'DOWN', Ref, process, CompactPid, normal} ->
        ok;
    {'DOWN', Ref, process, CompactPid, noproc} ->
        ok;
    {'DOWN', Ref, process, CompactPid, Reason} ->
        etap:bail("Failure compacting main group: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for main group compaction to finish")
    end.


trigger_update() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, UpdaterPid} = gen_server:call(GroupPid, {start_updater, []}, infinity),
    Ref = erlang:monitor(process, UpdaterPid),
    etap:diag("Waiting for updater to finish"),
    receive
    {'DOWN', Ref, _, _, {updater_finished, _}} ->
        etap:diag("Updater finished");
    {'DOWN', Ref, _, _, Reason} ->
        etap:bail("Updater finished with unexpected reason: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for updater to finish")
    end.


query_reduce_view(ViewName, StartKey, EndKey) ->
    etap:diag("Querying reduce view " ++ binary_to_list(ViewName)),
    {ok, View, Group, _} = couch_set_view:get_reduce_view(
        test_set_name(), ddoc_id(), ViewName,
        #set_view_group_req{stale = false, debug = true}),
    FoldFun = fun(Key, Red, Acc) -> {ok, [{Key, Red} | Acc]} end,
    ViewArgs = #view_query_args{
        run_reduce = true,
        view_name = ViewName,
        start_key = StartKey,
        end_key = EndKey
    },
    {ok, Rows} = couch_set_view:fold_reduce(Group, View, FoldFun, [], ViewArgs),
    couch_set_view:release_group(Group),
    case Rows of
    [{_Key, {json, RedValue}}] ->
        ejson:decode(RedValue);
    [] ->
        empty
    end.


verify_btrees_1(ValueGenFun) ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, Group} = gen_server:call(GroupPid, request_group, infinity),
    #set_view_group{
        id_btree = IdBtree,
        views = Views,
        index_header = #set_view_index_header{
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = Group,
    etap:is(length(Views), 2, "2 view record in view group"),
    [View1, View2] = Views,
    ?etap_match((View1#set_view.indexer)#mapreduce_view.reduce_funs,
                [{<<"test5">>, _}, {<<"test4">>, _}, {<<"test3">>, _}],
                "First view record has views 5 downto 3"),
    ?etap_match((View2#set_view.indexer)#mapreduce_view.reduce_funs,
                [{<<"test2">>, _}, {<<"test">>, _}],
                "Second view record has views 2 downto 1"),
    #set_view{
        id_num = 0,
        indexer = #mapreduce_view{
            btree = View1Btree
        }
    } = View1,
    #set_view{
        id_num = 1,
        indexer = #mapreduce_view{
            btree = View2Btree
        }
    } = View2,

    etap:diag("Verifying view group btrees"),
    ExpectedBitmask = couch_set_view_util:build_bitmask(lists:seq(0, 63)),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), lists:seq(0, 63)),
    ExpectedReductionViewBtree1 = [
        initial_num_docs(),
        -initial_num_docs(),
        lists:sum([ValueGenFun(I) || I <- lists:seq(0, initial_num_docs() - 1)])
    ],
    ExpectedReductionViewBtree2 = [
        list_to_binary(integer_to_list(initial_num_docs())),
        initial_num_docs()
    ],

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {initial_num_docs(), ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {initial_num_docs(), ExpectedReductionViewBtree1, ExpectedBitmask}},
        "View1 Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View2Btree),
        {ok, {initial_num_docs(), ExpectedReductionViewBtree2, ExpectedBitmask}},
        "View2 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    MaxPerPart = initial_num_docs() div num_set_partitions(),
    {ok, _, {_, _, _, IdBtreeFoldResult}} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, {P0, I0, C0, It}) ->
            case C0 >= MaxPerPart of
            true ->
                P = P0 + 1,
                I = P,
                C = 1;
            false ->
                P = P0,
                I = I0,
                C = C0 + 1
            end,
            true = (P < num_set_partitions()),
            ExpectedKv = {
                <<P:16, (doc_id(I))/binary>>,
                {P, [{0, [doc_id(I), 1]}, {1, doc_id(I)}]}
            },
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(It))
            end,
            {ok, {P, I + num_set_partitions(), C, It + 1}}
        end,
        {0, 0, 0, 0}, []),
    etap:is(IdBtreeFoldResult, initial_num_docs(),
        "Id Btree has " ++ integer_to_list(initial_num_docs()) ++ " entries"),
    etap:diag("Verifying the View1 Btree"),
    {ok, _, View1BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, Acc) ->
            PartId = Acc rem 64,
            ExpectedKv = {{[doc_id(Acc), 1], doc_id(Acc)}, {PartId, ValueGenFun(Acc)}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(Acc))
            end,
            {ok, Acc + 1}
        end,
        0, []),
    etap:is(View1BtreeFoldResult, initial_num_docs(),
        "View1 Btree has " ++ integer_to_list(initial_num_docs()) ++ " entries"),
    etap:diag("Verifying the View2 Btree"),
    {ok, _, View2BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View2Btree,
        fun(Kv, _, Acc) ->
            PartId = Acc rem 64,
            ExpectedKv = {{doc_id(Acc), doc_id(Acc)}, {PartId, ValueGenFun(Acc)}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View2 Btree has an unexpected KV at iteration " ++ integer_to_list(Acc))
            end,
            {ok, Acc + 1}
        end,
        0, []),
    etap:is(View2BtreeFoldResult, initial_num_docs(),
        "View2 Btree has " ++ integer_to_list(initial_num_docs()) ++ " entries").
