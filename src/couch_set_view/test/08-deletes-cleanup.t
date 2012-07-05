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

% Test motivated by MB-4518.

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


test_set_name() -> <<"couch_test_set_index_deletes_cleanup">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
initial_num_docs() -> 35520.  % must be multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(74),
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

    create_set(),
    add_documents(0, initial_num_docs()),

    {QueryResult1, Group1} = query_reduce_view(false),
    etap:is(
        QueryResult1,
        initial_num_docs(),
        "Reduce view has value " ++ couch_util:to_list(initial_num_docs())),
    verify_btrees_1(Group1),

    compact_view_group(),

    {QueryResult2, Group2} = query_reduce_view(false),
    etap:is(
        QueryResult2,
        initial_num_docs(),
        "Reduce view has value " ++ couch_util:to_list(initial_num_docs())),
    verify_btrees_1(Group2),

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

    {QueryResult3, Group3} = query_reduce_view(false),
    etap:is(
        QueryResult3,
        empty,
        "Reduce view returned 0 rows"),
    verify_btrees_2(Group3),

    compact_view_group(),

    {QueryResult4, Group4} = query_reduce_view(false),
    etap:is(
        QueryResult4,
        empty,
        "Reduce view returned 0 rows"),
    verify_btrees_2(Group4),

    etap:diag("Marking partitions [ 32 .. 63 ] as active"),
    ok = lists:foreach(
        fun(I) ->
            ok = couch_set_view:set_partition_states(test_set_name(), ddoc_id(), [I], [], [])
        end,
        lists:seq(32, 63)),

    {QueryResult5, Group5} = query_reduce_view(false),
    etap:is(
        QueryResult5,
        empty,
        "Reduce view returned 0 rows"),
    verify_btrees_3(Group5),

    compact_view_group(),

    {QueryResult6, Group6} = query_reduce_view(false),
    etap:is(
        QueryResult6,
        empty,
        "Reduce view returned 0 rows"),
    verify_btrees_3(Group6),

    etap:diag("Creating the same documents again"),
    add_documents(0, initial_num_docs()),

    {QueryResult7, Group7} = query_reduce_view(false),
    etap:is(
        QueryResult7,
        initial_num_docs(),
        "Reduce view has value " ++ couch_util:to_list(initial_num_docs())),
    verify_btrees_1(Group7),

    compact_view_group(),

    {QueryResult8, Group8} = query_reduce_view(false),
    etap:is(
        QueryResult8,
        initial_num_docs(),
        "Reduce view has value " ++ couch_util:to_list(initial_num_docs())),
    verify_btrees_1(Group8),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


query_reduce_view(Stale) ->
    etap:diag("Querying reduce view with ?group=true"),
    {ok, View, Group, _} = couch_set_view:get_reduce_view(
        test_set_name(), ddoc_id(), <<"test">>,
        #set_view_group_req{stale = Stale, debug = true}),
    FoldFun = fun(Key, Red, Acc) -> {ok, [{Key, Red} | Acc]} end,
    ViewArgs = #view_query_args{
        run_reduce = true,
        view_name = <<"test">>
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
                {<<"_id">>, doc_id(I)},
                {<<"_deleted">>, true}
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
        {<<"_id">>, ddoc_id()},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, doc.value); }">>},
                {<<"reduce">>, <<"_count">>}
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
                {<<"_id">>, doc_id(I)},
                {<<"value">>, I}
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


verify_btrees_1(Group) ->
    #set_view_group{
        id_btree = IdBtree,
        views = [View0],
        index_header = #set_view_index_header{
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = Group,
    #set_view{
        id_num = 0,
        btree = View0Btree
    } = View0,
    etap:diag("Verifying view group btrees"),
    ExpectedBitmask = couch_set_view_util:build_bitmask(lists:seq(0, 63)),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), lists:seq(0, 63)),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {initial_num_docs(), ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View0Btree),
        {ok, {initial_num_docs(), [initial_num_docs()], ExpectedBitmask}},
        "View0 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_btree:fold(
        IdBtree,
        fun(Kv, _, Acc) ->
            V = ?JSON_ENCODE(doc_id(Acc)),
            [ExpectedKv] = couch_set_view_updater:convert_back_index_kvs_to_binary(
                [{doc_id(Acc), {Acc rem 64, [{0, [V]}]}}], []),
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(Acc))
            end,
            {ok, Acc + 1}
        end,
        0, []),
    etap:is(IdBtreeFoldResult, initial_num_docs(),
        "Id Btree has " ++ integer_to_list(initial_num_docs()) ++ " entries"),
    etap:diag("Verifying the View0 Btree"),
    {ok, _, View0BtreeFoldResult} = couch_btree:fold(
        View0Btree,
        fun(Kv, _, Acc) ->
            V = ?JSON_ENCODE(Acc),
            ExpectedKeyDocId = couch_set_view_util:encode_key_docid(
                ?JSON_ENCODE(doc_id(Acc)), doc_id(Acc)),
            ExpectedKv = {ExpectedKeyDocId, <<(Acc rem 64):16, (size(V)):24, V/binary>>},
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
        "View0 Btree has " ++ integer_to_list(initial_num_docs()) ++ " entries").


verify_btrees_2(Group) ->
    #set_view_group{
        id_btree = IdBtree,
        views = [View0],
        index_header = #set_view_index_header{
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = Group,
    #set_view{
        id_num = 0,
        btree = View0Btree
    } = View0,
    etap:diag("Verifying view group btrees"),
    ExpectedABitmask = couch_set_view_util:build_bitmask(lists:seq(0, 31)),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), lists:seq(0, 31)),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {0, 0}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View0Btree),
        {ok, {0, [0], 0}},
        "View0 Btree has the right reduce value"),

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
    etap:is(View0BtreeFoldResult, 0, "View0 Btree is empty").


verify_btrees_3(Group) ->
    #set_view_group{
        id_btree = IdBtree,
        views = [View0],
        index_header = #set_view_index_header{
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = Group,
    #set_view{
        id_num = 0,
        btree = View0Btree
    } = View0,
    etap:diag("Verifying view group btrees"),
    ExpectedABitmask = couch_set_view_util:build_bitmask(lists:seq(0, 63)),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), lists:seq(0, 63)),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {0, 0}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View0Btree),
        {ok, {0, [0], 0}},
        "View0 Btree has the right reduce value"),

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
    etap:is(View0BtreeFoldResult, 0, "View0 Btree is empty").
