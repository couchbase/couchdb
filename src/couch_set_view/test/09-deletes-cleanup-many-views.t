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

% Like 08-deletes-cleanup.t but with several views in the design document.
% This triggers a different codepath in the indexer.

-define(JSON_ENCODE(V), ejson:encode(V)). % couch_db.hrl
-define(MAX_WAIT_TIME, 900 * 1000).

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
    debug = false,
    filter = true,
    type = main
}).


test_set_name() -> <<"couch_test_set_index_deletes_cleanup_many">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
initial_num_docs() -> 156288.  % must be multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(154),
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

    create_set(),
    add_documents(0, initial_num_docs()),

    ExpectedView2Value1 = lists:sum(lists:seq(0, initial_num_docs() - 1)),
    ExpectedView3Value1 = lists:sum(lists:seq(0, initial_num_docs() - 1)) * 4,

    {QueryResult1View1, Group1} = query_reduce_view(<<"view_1">>, false),
    etap:is(
        QueryResult1View1,
        initial_num_docs(),
        "Reduce value of view 1 is " ++ couch_util:to_list(initial_num_docs())),
    {QueryResult1View2, Group2} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        QueryResult1View2,
        ExpectedView2Value1,
        "Reduce value of view 2 is " ++ couch_util:to_list(ExpectedView2Value1)),
    {QueryResult1View3, Group3} = query_reduce_view(<<"view_3">>, false),
    etap:is(
        QueryResult1View3,
        ExpectedView3Value1,
        "Reduce value of view 3 is " ++ couch_util:to_list(ExpectedView3Value1)),

    verify_btrees_1(Group1),
    compare_groups(Group1, Group2),
    compare_groups(Group1, Group3),

    compact_view_group(),

    {QueryResult2View1, Group4} = query_reduce_view(<<"view_1">>, false),
    etap:is(
        QueryResult2View1,
        initial_num_docs(),
        "Reduce value of view 1 is " ++ couch_util:to_list(initial_num_docs())),
    {QueryResult2View2, Group5} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        QueryResult2View2,
        ExpectedView2Value1,
        "Reduce value of view 2 is " ++ couch_util:to_list(ExpectedView2Value1)),
    {QueryResult2View3, Group6} = query_reduce_view(<<"view_3">>, false),
    etap:is(
        QueryResult2View3,
        ExpectedView3Value1,
        "Reduce value of view 3 is " ++ couch_util:to_list(ExpectedView3Value1)),

    verify_btrees_1(Group4),
    compare_groups(Group4, Group5),
    compare_groups(Group4, Group6),

    etap:diag("Deleting all documents"),
    delete_docs(0, initial_num_docs()),
    etap:is(
        couch_set_view_test_util:doc_count(test_set_name(), lists:seq(0, 63)),
        0,
        "All docs were deleted"),

    etap:diag("Marking partitions [ 32 .. 63 ] for cleanup"),
    ok = lists:foreach(
        fun(I) ->
            ok = couch_set_view:set_partition_states(test_set_name(), ddoc_id(), [], [], [I])
        end,
        lists:seq(32, 63)),

    etap:diag("Waiting for cleanup of partitions [ 32 .. 63 ]"),
    MainGroupInfo = get_group_info(),
    wait_for_cleanup(MainGroupInfo),
    etap:diag("Cleanup finished"),

    {QueryResult3View1, Group7} = query_reduce_view(<<"view_1">>, false),
    etap:is(
        QueryResult3View1,
        empty,
        "Reduce value of view 1 is empty"),
    {QueryResult3View2, Group8} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        QueryResult3View2,
        empty,
        "Reduce value of view 2 is empty"),
    {QueryResult3View3, Group9} = query_reduce_view(<<"view_3">>, false),
    etap:is(
        QueryResult3View3,
        empty,
        "Reduce value of view 3 is empty"),

    verify_btrees_2(Group7),
    compare_groups(Group7, Group8),
    compare_groups(Group7, Group9),

    compact_view_group(),

    {QueryResult4View1, Group10} = query_reduce_view(<<"view_1">>, false),
    etap:is(
        QueryResult4View1,
        empty,
        "Reduce value of view 1 is empty"),
    {QueryResult4View2, Group11} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        QueryResult4View2,
        empty,
        "Reduce value of view 2 is empty"),
    {QueryResult4View3, Group12} = query_reduce_view(<<"view_3">>, false),
    etap:is(
        QueryResult4View3,
        empty,
        "Reduce value of view 3 is empty"),

    verify_btrees_2(Group10),
    compare_groups(Group10, Group11),
    compare_groups(Group10, Group12),

    etap:diag("Marking partitions [ 32 .. 63 ] as active"),
    ok = lists:foreach(
        fun(I) ->
            ok = couch_set_view:set_partition_states(test_set_name(), ddoc_id(), [I], [], [])
        end,
        lists:seq(32, 63)),

    {QueryResult5View1, Group13} = query_reduce_view(<<"view_1">>, false),
    etap:is(
        QueryResult5View1,
        empty,
        "Reduce value of view 1 is empty"),
    {QueryResult5View2, Group14} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        QueryResult5View2,
        empty,
        "Reduce value of view 2 is empty"),
    {QueryResult5View3, Group15} = query_reduce_view(<<"view_3">>, false),
    etap:is(
        QueryResult5View3,
        empty,
        "Reduce value of view 3 is empty"),

    verify_btrees_3(Group13),
    compare_groups(Group13, Group14),
    compare_groups(Group13, Group15),

    compact_view_group(),

    {QueryResult6View1, Group16} = query_reduce_view(<<"view_1">>, false),
    etap:is(
        QueryResult6View1,
        empty,
        "Reduce value of view 1 is empty"),
    {QueryResult6View2, Group17} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        QueryResult6View2,
        empty,
        "Reduce value of view 2 is empty"),
    {QueryResult6View3, Group18} = query_reduce_view(<<"view_3">>, false),
    etap:is(
        QueryResult6View3,
        empty,
        "Reduce value of view 3 is empty"),

    verify_btrees_3(Group16),
    compare_groups(Group16, Group17),
    compare_groups(Group16, Group18),

    etap:diag("Creating the same documents again"),
    add_documents(0, initial_num_docs()),

    {QueryResult7View1, Group19} = query_reduce_view(<<"view_1">>, false),
    etap:is(
        QueryResult7View1,
        initial_num_docs(),
        "Reduce value of view 1 is " ++ couch_util:to_list(initial_num_docs())),
    {QueryResult7View2, Group20} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        QueryResult7View2,
        ExpectedView2Value1,
        "Reduce value of view 2 is " ++ couch_util:to_list(ExpectedView2Value1)),
    {QueryResult7View3, Group21} = query_reduce_view(<<"view_3">>, false),
    etap:is(
        QueryResult7View3,
        ExpectedView3Value1,
        "Reduce value of view 3 is " ++ couch_util:to_list(ExpectedView3Value1)),

    verify_btrees_1(Group19),
    compare_groups(Group19, Group20),
    compare_groups(Group19, Group21),

    compact_view_group(),

    {QueryResult8View1, Group22} = query_reduce_view(<<"view_1">>, false),
    etap:is(
        QueryResult8View1,
        initial_num_docs(),
        "Reduce value of view 1 is " ++ couch_util:to_list(initial_num_docs())),
    {QueryResult8View2, Group23} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        QueryResult8View2,
        ExpectedView2Value1,
        "Reduce value of view 2 is " ++ couch_util:to_list(ExpectedView2Value1)),
    {QueryResult8View3, Group24} = query_reduce_view(<<"view_3">>, false),
    etap:is(
        QueryResult8View3,
        ExpectedView3Value1,
        "Reduce value of view 3 is " ++ couch_util:to_list(ExpectedView3Value1)),

    verify_btrees_1(Group22),
    compare_groups(Group22, Group23),
    compare_groups(Group22, Group24),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


query_reduce_view(ViewName, Stale) ->
    etap:diag("Querying reduce view " ++ binary_to_list(ViewName) ++ " with ?group=true"),
    {ok, View, Group, _} = couch_set_view:get_reduce_view(
        test_set_name(), ddoc_id(), ViewName,
        #set_view_group_req{stale = Stale, debug = true}),
    FoldFun = fun(Key, Red, Acc) -> {ok, [{Key, Red} | Acc]} end,
    ViewArgs = #view_query_args{
        run_reduce = true,
        view_name = ViewName
    },
    {ok, Rows} = couch_set_view:fold_reduce(Group, View, FoldFun, [], ViewArgs),
    couch_set_view:release_group(Group),
    case Rows of
    [{_Key, {json, RedValue}}] ->
        {ejson:decode(RedValue), Group};
    [] ->
        {empty, Group}
    end.


wait_for_cleanup(GroupInfo) ->
    etap:diag("Waiting for main index cleanup to finish"),
    Pid = spawn(fun() ->
        wait_for_cleanup_loop(GroupInfo)
    end),
    Ref = erlang:monitor(process, Pid),
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, noproc} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Failure waiting for main index cleanup: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for main index cleanup")
    end.


wait_for_cleanup_loop(GroupInfo) ->
    case couch_util:get_value(cleanup_partitions, GroupInfo) of
    [] ->
        {Stats} = couch_util:get_value(stats, GroupInfo),
        Cleanups = couch_util:get_value(cleanups, Stats),
        etap:is(
            (is_integer(Cleanups) andalso (Cleanups > 0)),
            true,
            "Main group stats has at least 1 full cleanup");
    _ ->
        ok = timer:sleep(1000),
        wait_for_cleanup_loop(get_group_info())
    end.


get_group_info() ->
    {ok, Info} = couch_set_view:get_group_info(test_set_name(), ddoc_id()),
    Info.


delete_docs(StartId, NumDocs) ->
    Dbs = dict:from_list(lists:map(
        fun(I) ->
            {ok, Db} = couch_set_view_test_util:open_set_db(test_set_name(), I),
            {I, Db}
        end,
        lists:seq(0, 63))),
    Docs = lists:foldl(
        fun(I, Acc) ->
            Doc = couch_doc:from_json_obj({[
                {<<"meta">>, {[{<<"deleted">>, true}, {<<"id">>, doc_id(I)}]}},
                {<<"json">>, {[]}}
            ]}),
            DocList = case orddict:find(I rem 64, Acc) of
            {ok, L} ->
                L;
            error ->
                []
            end,
            orddict:store(I rem 64, [Doc | DocList], Acc)
        end,
        orddict:new(), lists:seq(StartId, StartId + NumDocs - 1)),
    [] = orddict:fold(
        fun(I, DocList, Acc) ->
            Db = dict:fetch(I, Dbs),
            etap:diag("Deleting " ++ integer_to_list(length(DocList)) ++
                " documents from partition " ++ integer_to_list(I)),
            ok = couch_db:update_docs(Db, DocList, [sort_docs]),
            Acc
        end,
        [], Docs),
    ok = lists:foreach(fun({_, Db}) -> ok = couch_db:close(Db) end, dict:to_list(Dbs)).


create_set() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view:cleanup_index_files(test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"view_1">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                {<<"reduce">>, <<"_count">>}
            ]}},
            {<<"view_2">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                {<<"reduce">>, <<"_sum">>}
            ]}},
            {<<"view_3">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value * 2); }">>},
                {<<"reduce">>, <<"function(key, values, rereduce) {"
                                 "if (rereduce) {"
                                 "    return sum(values);"
                                 "} else {"
                                 "    var result = 0;"
                                 "    for (var i = 0; i < values.length; i++) {"
                                 "        result += (values[i] * 2);"
                                 "    }"
                                 "    return result;"
                                 "}"
                                 "}">>}
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
    ok = couch_set_view:define_group(test_set_name(), ddoc_id(), Params).


add_documents(StartId, Count) ->
    etap:diag("Adding " ++ integer_to_list(Count) ++ " new documents"),
    DocList0 = lists:map(
        fun(I) ->
            {I rem num_set_partitions(), {[
            {<<"meta">>, {[{<<"id">>, doc_id(I)}]}},
            {<<"json">>, {[
                {<<"value">>, I}
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
    {ok, CompactPid} = couch_set_view_compactor:start_compact(test_set_name(), ddoc_id(), main),
    etap:diag("Waiting for view group compaction to finish"),
    Ref = erlang:monitor(process, CompactPid),
    receive
    {'DOWN', Ref, process, CompactPid, normal} ->
        ok;
    {'DOWN', Ref, process, CompactPid, Reason} ->
        etap:bail("Failure compacting main group: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for main group compaction to finish")
    end.


get_view(_ViewName, []) ->
    undefined;
get_view(ViewName, [#set_view{reduce_funs = RedFuns} = View | Rest]) ->
    case couch_util:get_value(ViewName, RedFuns) of
    undefined ->
        get_view(ViewName, Rest);
    _ ->
        View
    end.


verify_btrees_1(Group) ->
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
    etap:is(2, length(Views), "2 view btrees in the group"),
    View0 = get_view(<<"view_1">>, Views),
    View1 = get_view(<<"view_2">>, Views),
    View2 = get_view(<<"view_3">>, Views),
    etap:is(View1, View0, "Views 1 and 2 share the same btree"),
    #set_view{
        btree = View0Btree
    } = View0,
    #set_view{
        btree = View2Btree
    } = View2,
    etap:diag("Verifying view group btrees"),
    ExpectedBitmask = couch_set_view_util:build_bitmask(lists:seq(0, 63)),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), lists:seq(0, 63)),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {initial_num_docs(), ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    ExpectedView0Reds = [lists:sum(lists:seq(0, initial_num_docs() - 1)), initial_num_docs()],
    couch_set_view_mapreduce:start_reduce_context(View0),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View0Btree),
        {ok, {initial_num_docs(), ExpectedView0Reds, ExpectedBitmask}},
        "View0 Btree has the right reduce value"),
    couch_set_view_mapreduce:end_reduce_context(View0),
    ExpectedView2Reds = [lists:sum(lists:seq(0, initial_num_docs() - 1)) * 4],
    couch_set_view_mapreduce:start_reduce_context(View2),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View2Btree),
        {ok, {initial_num_docs(), ExpectedView2Reds, ExpectedBitmask}},
        "View2 Btree has the right reduce value"),
    couch_set_view_mapreduce:end_reduce_context(View2),

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
            Value = [{View2#set_view.id_num, doc_id(I)}, {View0#set_view.id_num, doc_id(I)}],
            ExpectedKv = {<<P:16, (doc_id(I))/binary>>, {P, Value}},
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
    etap:diag("Verifying the View0 Btree"),
    {ok, _, View0BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View0Btree,
        fun(Kv, _, Acc) ->
            ExpectedKv = {{doc_id(Acc), doc_id(Acc)}, {Acc rem 64, Acc}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View0 Btree has an unexpected KV at iteration " ++ integer_to_list(Acc))
            end,
            {ok, Acc + 1}
        end,
        0, []),
    etap:is(View0BtreeFoldResult, initial_num_docs(),
        "View0 Btree has " ++ integer_to_list(initial_num_docs()) ++ " entries"),
    etap:diag("Verifying the View2 Btree"),
    {ok, _, View2BtreeFoldResult} =  couch_set_view_test_util:fold_view_btree(
        Group,
        View2Btree,
        fun(Kv, _, Acc) ->
            ExpectedKv = {{doc_id(Acc), doc_id(Acc)}, {Acc rem 64, Acc * 2}},
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


verify_btrees_2(Group) ->
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
    etap:is(2, length(Views), "2 view btrees in the group"),
    View0 = get_view(<<"view_1">>, Views),
    View1 = get_view(<<"view_2">>, Views),
    View2 = get_view(<<"view_3">>, Views),
    etap:is(View1, View0, "Views 1 and 2 share the same btree"),
    #set_view{
        btree = View0Btree
    } = View0,
    #set_view{
        btree = View2Btree
    } = View2,
    etap:diag("Verifying view group btrees"),
    ExpectedABitmask = couch_set_view_util:build_bitmask(lists:seq(0, 31)),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), lists:seq(0, 31)),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {0, 0}},
        "Id Btree has the right reduce value"),
    couch_set_view_mapreduce:start_reduce_context(View0),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View0Btree),
        {ok, {0, [0, 0], 0}},
        "View0 Btree has the right reduce value"),
    couch_set_view_mapreduce:end_reduce_context(View0),
    couch_set_view_mapreduce:start_reduce_context(View2),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View2Btree),
        {ok, {0, [0], 0}},
        "View2 Btree has the right reduce value"),
    couch_set_view_mapreduce:end_reduce_context(View2),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedABitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(_Kv, _, Acc) ->
            {ok, Acc + 1}
        end,
        0, []),
    etap:is(IdBtreeFoldResult, 0, "Id Btree is empty"),
    etap:diag("Verifying the View0 Btree"),
    {ok, _, View0BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View0Btree,
        fun(_Kv, _, Acc) ->
            {ok, Acc + 1}
        end,
        0, []),
    etap:is(View0BtreeFoldResult, 0, "View0 Btree is empty"),
    etap:diag("Verifying the View2 Btree"),
    {ok, _, View2BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View2Btree,
        fun(_Kv, _, Acc) ->
            {ok, Acc + 1}
        end,
        0, []),
    etap:is(View2BtreeFoldResult, 0, "View2 Btree is empty").


verify_btrees_3(Group) ->
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
    etap:is(2, length(Views), "2 view btrees in the group"),
    View0 = get_view(<<"view_1">>, Views),
    View1 = get_view(<<"view_2">>, Views),
    View2 = get_view(<<"view_3">>, Views),
    etap:is(View1, View0, "Views 1 and 2 share the same btree"),
    #set_view{
        btree = View0Btree
    } = View0,
    #set_view{
        btree = View2Btree
    } = View2,
    etap:diag("Verifying view group btrees"),
    ExpectedABitmask = couch_set_view_util:build_bitmask(lists:seq(0, 63)),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), lists:seq(0, 63)),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {0, 0}},
        "Id Btree has the right reduce value"),
    couch_set_view_mapreduce:start_reduce_context(View0),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View0Btree),
        {ok, {0, [0, 0], 0}},
        "View0 Btree has the right reduce value"),
    couch_set_view_mapreduce:end_reduce_context(View0),
    couch_set_view_mapreduce:start_reduce_context(View2),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View2Btree),
        {ok, {0, [0], 0}},
        "View2 Btree has the right reduce value"),
    couch_set_view_mapreduce:end_reduce_context(View2),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedABitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(_Kv, _, Acc) ->
            {ok, Acc + 1}
        end,
        0, []),
    etap:is(IdBtreeFoldResult, 0, "Id Btree is empty"),
    etap:diag("Verifying the View0 Btree"),
    {ok, _, View0BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View0Btree,
        fun(_Kv, _, Acc) ->
            {ok, Acc + 1}
        end,
        0, []),
    etap:is(View0BtreeFoldResult, 0, "View0 Btree is empty"),
    etap:diag("Verifying the View2 Btree"),
    {ok, _, View2BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View0Btree,
        fun(_Kv, _, Acc) ->
            {ok, Acc + 1}
        end,
        0, []),
    etap:is(View2BtreeFoldResult, 0, "View2 Btree is empty").


compare_groups(Group1, Group2) ->
    etap:is(
        Group2#set_view_group.views,
        Group1#set_view_group.views,
        "View states are equal"),
    etap:is(
        Group2#set_view_group.index_header,
        Group1#set_view_group.index_header,
        "Index headers are equal").
