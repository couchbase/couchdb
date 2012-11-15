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

-define(JSON_ENCODE(V), ejson:encode(V)). % couch_db.hrl
-define(MAX_WAIT_TIME, 600 * 1000).

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

test_set_name() -> <<"couch_test_set_index_passive_parts">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 16448.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(561),
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
    add_documents(0, num_docs()),

    % build index
    _ = get_group_snapshot(),

    ActivePartitions1 = lists:seq(0, num_set_partitions() - 1),

    verify_btrees(ActivePartitions1, fun(I) -> I end),

    FoldFun = fun(PartId, ActivePartsAcc) ->
        ActivePartsAcc2 = ordsets:del_element(PartId, ActivePartsAcc),
        ok = couch_set_view:set_partition_states(
            test_set_name(), ddoc_id(), [], [PartId], []),
        fold_view(ActivePartsAcc2, fun(I) -> I end),
        ActivePartsAcc2
    end,

    ActivePartitions2 = lists:foldl(
        FoldFun,
        ActivePartitions1,
        lists:reverse(lists:seq(1, 31, 2))),

    update_documents(0, num_docs(), fun(I) -> I * 5 end),
    fold_view(ActivePartitions2, fun(I) -> I * 5 end),

    wait_updater_finishes(),
    update_documents(0, num_docs(), fun(I) -> I end),
    fold_view(ActivePartitions2, fun(I) -> I end),

    wait_updater_finishes(),
    verify_btrees(ActivePartitions2, fun(I) -> I end),
    compact_view_group(),
    verify_btrees(ActivePartitions2, fun(I) -> I end),

    ActivePartitions3 = lists:foldl(
        FoldFun,
        ActivePartitions2,
        lists:seq(0, 31, 2)),

    update_documents(0, num_docs(), fun(I) -> I * 5 end),
    fold_view(ActivePartitions3, fun(I) -> I * 5 end),

    wait_updater_finishes(),
    update_documents(0, num_docs(), fun(I) -> I end),
    fold_view(ActivePartitions3, fun(I) -> I end),

    wait_updater_finishes(),
    verify_btrees(ActivePartitions3, fun(I) -> I end),
    compact_view_group(),
    verify_btrees(ActivePartitions3, fun(I) -> I end),

    ActivePartitions4 = lists:foldl(
        FoldFun,
        ActivePartitions3,
        lists:seq(32, 63, 2)),

    update_documents(0, num_docs(), fun(I) -> I * I + 3 end),
    fold_view(ActivePartitions4, fun(I) -> I * I + 3 end),

    wait_updater_finishes(),
    update_documents(0, num_docs(), fun(I) -> I end),
    fold_view(ActivePartitions4, fun(I) -> I end),

    wait_updater_finishes(),
    verify_btrees(ActivePartitions4, fun(I) -> I end),
    compact_view_group(),
    verify_btrees(ActivePartitions4, fun(I) -> I end),

    ActivePartitions5 = lists:foldl(
        FoldFun,
        ActivePartitions4,
        lists:reverse(lists:seq(33, 63, 2))),

    update_documents(0, num_docs(), fun(I) -> I * -3 end),
    fold_view(ActivePartitions5, fun(I) -> I * -3 end),

    wait_updater_finishes(),
    update_documents(0, num_docs(), fun(I) -> I end),
    fold_view(ActivePartitions5, fun(I) -> I end),

    wait_updater_finishes(),
    verify_btrees(ActivePartitions5, fun(I) -> I end),
    compact_view_group(),
    verify_btrees(ActivePartitions5, fun(I) -> I end),

    etap:is(
        ActivePartitions5,
        [],
        "Final list of active partitions is empty"),

    FinalActivePartitions = lists:seq(0, num_set_partitions() - 1),

    lists:foreach(
        fun(PartId) ->
            ok = couch_set_view:set_partition_states(
                test_set_name(), ddoc_id(), [PartId], [], [])
        end, FinalActivePartitions),

    fold_view(FinalActivePartitions, fun(I) -> I end),
    wait_updater_finishes(),
    verify_btrees(FinalActivePartitions, fun(I) -> I end),

    compact_view_group(),

    fold_view(FinalActivePartitions, fun(I) -> I end),
    wait_updater_finishes(),
    verify_btrees(FinalActivePartitions, fun(I) -> I end),

    update_documents(0, num_docs(), fun(I) -> I * 10 + 1 end),
    fold_view(FinalActivePartitions, fun(I) -> I * 10 + 1 end),
    wait_updater_finishes(),
    verify_btrees(FinalActivePartitions, fun(I) -> I * 10 + 1 end),

    compact_view_group(),
    verify_btrees(FinalActivePartitions, fun(I) -> I * 10 + 1 end),
    fold_view(FinalActivePartitions, fun(I) -> I * 10 + 1 end),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


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
                {<<"reduce">>, <<"_sum">>}
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
    add_documents(StartId, Count, fun(I) -> I end).

add_documents(StartId, Count, ValueGenFun) ->
    etap:diag("Adding " ++ integer_to_list(Count) ++ " new documents"),
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


update_documents(StartId, NumDocs, ValueGenFun) ->
    etap:diag("About to update " ++ integer_to_list(NumDocs) ++ " documents"),
    Dbs = dict:from_list(lists:map(
        fun(I) ->
            {ok, Db} = couch_set_view_test_util:open_set_db(test_set_name(), I),
            {I, Db}
        end,
        lists:seq(0, num_set_partitions() - 1))),
    Docs = lists:foldl(
        fun(I, Acc) ->
            Doc = couch_doc:from_json_obj({[
                {<<"meta">>, {[{<<"id">>, doc_id(I)}]}},
                {<<"json">>, {[
                    {<<"value">>, ValueGenFun(I)}
                ]}}
            ]}),
            DocList = case orddict:find(I rem num_set_partitions(), Acc) of
            {ok, L} ->
                L;
            error ->
                []
            end,
            orddict:store(I rem num_set_partitions(), [Doc | DocList], Acc)
        end,
        orddict:new(), lists:seq(StartId, StartId + NumDocs - 1)),
    [] = orddict:fold(
        fun(I, DocList, Acc) ->
            Db = dict:fetch(I, Dbs),
            ok = couch_db:update_docs(Db, DocList, [sort_docs]),
            Acc
        end,
        [], Docs),
    etap:diag("Updated " ++ integer_to_list(NumDocs) ++ " documents"),
    ok = lists:foreach(fun({_, Db}) -> ok = couch_db:close(Db) end, dict:to_list(Dbs)).


doc_id(I) ->
    iolist_to_binary(io_lib:format("doc_~8..0b", [I])).


fold_view(ActiveParts, ValueGenFun) ->
    {ok, FoldView, Group, _} = couch_set_view:get_reduce_view(
        test_set_name(), ddoc_id(), <<"view_1">>,
        #set_view_group_req{stale = false, debug = true}),

    etap:diag("Verifying some btree metadata"),
    #set_view_group{
        views = [#set_view{}],
        index_header = #set_view_index_header{
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = Group,

    ExpectedABitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedPBitmask = couch_set_view_util:build_bitmask(
        ordsets:subtract(lists:seq(0, num_set_partitions() - 1), ActiveParts)),
    DbSeqsActive = couch_set_view_test_util:get_db_seqs(test_set_name(), ActiveParts),
    ExpectedViewReduction = lists:sum(
        [ValueGenFun(I) || I <- lists:seq(0, num_docs() - 1),
            ordsets:is_element(I rem num_set_partitions(), ActiveParts)]),

    etap:is(
        [{P, S} || {P, S} <- HeaderUpdateSeqs, ordsets:is_element(P, ActiveParts)],
        DbSeqsActive,
        "Header has right active update seqs list"),
    etap:is(Abitmask, ExpectedABitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, ExpectedPBitmask, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    ViewArgs = #view_query_args{
        run_reduce = true,
        view_name = <<"view_1">>
    },

    etap:diag("Folding the view with ?group=false"),

    FullRedFoldFun = fun(_Key, {json, Red}, Acc) ->
        {ok, [Red | Acc]}
    end,
    {ok, FullRedResult} = couch_set_view:fold_reduce(
        Group, FoldView, FullRedFoldFun, [], ViewArgs),
    case ActiveParts of
    [] ->
        etap:is(
            FullRedResult,
            [],
           "Got correct fold reduce value with ?group=false");
    _ ->
        etap:is(
            ejson:decode(FullRedResult),
            ExpectedViewReduction,
            "Got correct fold reduce value with ?group=false")
    end,

    etap:diag("Folding the view with ?group=true"),

    PerKeyRedFoldFun = fun(Key, {json, Red}, {NextVal, I}) ->
        ExpectedKey = {json, ejson:encode(doc_id(NextVal))},
        ExpectedRed = ValueGenFun(NextVal),
        case {Key, ejson:decode(Red)} of
        {ExpectedKey, ExpectedRed} ->
            ok;
        _ ->
            etap:bail("Unexpected KV at view fold iteration " ++ integer_to_list(I))
        end,
        {ok, {next_val(NextVal, ActiveParts), I + 1}}
    end,
    {ok, {_, PerKeyRedResult}} = couch_set_view:fold_reduce(
        Group, FoldView, PerKeyRedFoldFun,
        {case ActiveParts of [] -> nil; _ -> hd(ActiveParts) end, 0},
        ViewArgs#view_query_args{group_level = exact}),
    etap:is(
        PerKeyRedResult,
        length(ActiveParts) * (num_docs() div num_set_partitions()),
        "Got correct fold reduce value with ?group=true"),
    ok.


next_val(I, ActiveParts) ->
    case ordsets:is_element((I + 1) rem num_set_partitions(), ActiveParts) of
    true ->
        I + 1;
    false ->
        next_val(I + 1, ActiveParts)
    end.


verify_btrees(ActiveParts, ValueGenFun) ->
    Group = get_group_snapshot(),
    etap:diag("Verifying btrees"),

    #set_view_group{
        id_btree = IdBtree,
        views = [View1],
        index_header = #set_view_index_header{
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = Group,
    #set_view{
        btree = View1Btree
    } = View1,
    ExpectedBitmask = couch_set_view_util:build_bitmask(
        lists:seq(0, num_set_partitions() - 1)),
    ExpectedABitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedPBitmask = couch_set_view_util:build_bitmask(
        ordsets:subtract(lists:seq(0, num_set_partitions() - 1), ActiveParts)),
    DbSeqs = couch_set_view_test_util:get_db_seqs(
        test_set_name(), lists:seq(0, num_set_partitions() - 1)),
    ExpectedKVCount = num_docs(),
    ExpectedBtreeViewReduction = lists:sum(
        [ValueGenFun(I) || I <- lists:seq(0, num_docs() - 1)]),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {ExpectedKVCount, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {ExpectedKVCount, [ExpectedBtreeViewReduction], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedABitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, ExpectedPBitmask, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    MaxPerPart = num_docs() div num_set_partitions(),
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
            DocId = doc_id(I),
            Value = [{View1#set_view.id_num, DocId}],
            ExpectedKv = {<<P:16, DocId/binary>>, {P, Value}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(It))
            end,
            {ok, {P, I + num_set_partitions(), C, It + 1}}
        end,
        {0, 0, 0, 0}, []),
    etap:is(IdBtreeFoldResult, ExpectedKVCount,
        "Id Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, View1BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            DocId = doc_id(I),
            ExpectedKv = {{DocId, DocId}, {PartId, ValueGenFun(I)}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            {ok, I + 1}
        end,
        0, []),
    etap:is(View1BtreeFoldResult, ExpectedKVCount,
        "View1 Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),
    ok.


get_group_snapshot() ->
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    {ok, Group, 0} = gen_server:call(
        GroupPid, #set_view_group_req{stale = false, debug = true}, infinity),
    Group.


wait_updater_finishes() ->
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    {ok, UpPid} = gen_server:call(GroupPid, updater_pid, infinity),
    case UpPid of
    nil ->
        ok;
    _ when is_pid(UpPid) ->
        Ref = erlang:monitor(process, UpPid),
        receive
        {'DOWN', Ref, process, UpPid, {updater_finished, _}} ->
            ok;
        {'DOWN', Ref, process, UpPid, noproc} ->
            ok;
        {'DOWN', Ref, process, UpPid, Reason} ->
            etap:bail("Failure updating main group: " ++ couch_util:to_list(Reason))
        after ?MAX_WAIT_TIME ->
            etap:bail("Timeout waiting for main group update")
        end
    end.


compact_view_group() ->
    {ok, CompactPid} = couch_set_view_compactor:start_compact(test_set_name(), ddoc_id(), main),
    etap:diag("Waiting for main view group compaction to finish"),
    Ref = erlang:monitor(process, CompactPid),
    receive
    {'DOWN', Ref, process, CompactPid, normal} ->
        ok;
    {'DOWN', Ref, process, CompactPid, Reason} ->
        etap:bail("Failure compacting main view group: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for main view group compaction to finish")
    end.
