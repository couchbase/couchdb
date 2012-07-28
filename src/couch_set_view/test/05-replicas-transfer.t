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

-define(JSON_ENCODE(V), ejson:encode(V)). % couch_db.hrl
-define(JSON_DECODE(V), ejson:decode(V)). % couch_db.hrl
-define(MAX_WAIT_TIME, 600 * 1000).

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
    debug = false,
    filter = true,
    type = main
}).


test_set_name() -> <<"couch_test_set_index_replicas_transfer">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 70848.  % keep it a multiple of num_set_partitions()


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
    ok = couch_config:set("set_views", "update_interval", "0", false),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    create_set(),
    add_documents(0, num_docs()),

    MainGroupInfo1 = get_group_info(),
    {RepGroupInfo1} = couch_util:get_value(replica_group_info, MainGroupInfo1),

    ExpectedView1Result1 = num_docs() div 2,
    ExpectedView2Result1 = lists:sum(
        [I * 2 || I <- lists:seq(0, num_docs() - 1), (I rem 64) < 32]),

    {View1QueryResult1, Group1} = query_reduce_view(<<"view_1">>, false),
    {View2QueryResult1, Group2} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        View1QueryResult1,
        ExpectedView1Result1,
        "Reduce view 1 has value " ++ couch_util:to_list(ExpectedView1Result1)),
    etap:is(
        View2QueryResult1,
        ExpectedView2Result1,
        "Reduce view 2 has value " ++ couch_util:to_list(ExpectedView2Result1)),

    verify_main_group_btrees_1(Group1),
    verify_replica_group_btrees_1(Group1),
    compare_groups(Group1, Group2),

    etap:diag("Verifying main and replica group infos"),
    etap:is(
        couch_util:get_value(active_partitions, MainGroupInfo1),
        lists:seq(0, 31),
        "Main group has [ 0 .. 31 ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, MainGroupInfo1),
        [],
        "Main group has [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, MainGroupInfo1),
        [],
        "Main group has [ ] as cleanup partitions"),
    etap:is(
        couch_util:get_value(replica_partitions, MainGroupInfo1),
        [],
        "Main group has [ ] as replica partitions"),
    etap:is(
        couch_util:get_value(replicas_on_transfer, MainGroupInfo1),
        [],
        "Main group has [ ] as replicas on transfer"),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo1),
        [],
        "Replica group has [ ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo1),
        [],
        "Replica group has [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, RepGroupInfo1),
        [],
        "Replica group has [ ] as cleanup partitions"),

    etap:diag("Marking partitions [ 32 .. 63 ] as replicas"),
    ok = couch_set_view:add_replica_partitions(test_set_name(), ddoc_id(), lists:seq(32, 63)),

    MainGroupInfo2 = get_group_info(),
    {RepGroupInfo2} = couch_util:get_value(replica_group_info, MainGroupInfo2),

    etap:diag("Verifying main and replica group infos again"),
    etap:is(
        couch_util:get_value(active_partitions, MainGroupInfo2),
        lists:seq(0, 31),
        "Main group has [ 0 .. 31 ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, MainGroupInfo2),
        [],
        "Main group has [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, MainGroupInfo2),
        [],
        "Main group has [ ] as cleanup partitions"),
    etap:is(
        couch_util:get_value(replica_partitions, MainGroupInfo2),
        lists:seq(32, 63),
        "Main group has [ 32 .. 63] as replica partitions"),
    etap:is(
        couch_util:get_value(replicas_on_transfer, MainGroupInfo2),
        [],
        "Main group has [ ] as replicas on transfer"),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo2),
        [],
        "Replica group has [ ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo2),
        lists:seq(32, 63),
        "Replica group has [ 32 .. 63 ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, RepGroupInfo2),
        [],
        "Replica group has [ ] as cleanup partitions"),

    {View1QueryResult2, _Group3} = query_reduce_view(<<"view_1">>, false),
    {View2QueryResult2, _Group4} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        View1QueryResult2,
        ExpectedView1Result1,
        "Reduce view 1 has value " ++ couch_util:to_list(ExpectedView1Result1)),
    etap:is(
        View2QueryResult2,
        ExpectedView2Result1,
        "Reduce view 2 has value " ++ couch_util:to_list(ExpectedView2Result1)),

    wait_for_replica_full_update(RepGroupInfo2),

    {View1QueryResult3, Group5} = query_reduce_view(<<"view_1">>, false),
    {View2QueryResult3, Group6} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        View1QueryResult3,
        ExpectedView1Result1,
        "Reduce view 1 has value " ++ couch_util:to_list(ExpectedView1Result1)),
    etap:is(
        View2QueryResult3,
        ExpectedView2Result1,
        "Reduce view 2 has value " ++ couch_util:to_list(ExpectedView2Result1)),

    verify_main_group_btrees_2(Group5),
    verify_replica_group_btrees_2(Group5),
    compare_groups(Group5, Group6),

    ExpectedView1Result2 = num_docs(),
    ExpectedView2Result2 = lists:sum([I * 2 || I <- lists:seq(0, num_docs() - 1)]),

    etap:diag("Marking partitions [ 32 .. 63 ] as active"),
    lists:foreach(
        fun(I) ->
            ok = couch_set_view:set_partition_states(test_set_name(), ddoc_id(), [I], [], [])
        end,
        lists:seq(32, 63)),

    MainGroupInfo3 = get_group_info(),
    {RepGroupInfo3} = couch_util:get_value(replica_group_info, MainGroupInfo3),

    {View1QueryResult4, _Group7} = query_reduce_view(<<"view_1">>, false),
    {View2QueryResult4, _Group8} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        View1QueryResult4,
        ExpectedView1Result2,
        "Reduce view 1 has value " ++ couch_util:to_list(ExpectedView1Result2)),
    etap:is(
        View2QueryResult4,
        ExpectedView2Result2,
        "Reduce view 2 has value " ++ couch_util:to_list(ExpectedView2Result2)),

    etap:diag("Waiting for transfer of replica partitions [ 32 .. 63 ] to main group"),
    wait_for_main_full_update(MainGroupInfo2, ExpectedView1Result2, ExpectedView2Result2),
    etap:diag("Replicas transferred to main group"),

    verify_group_info_during_replicas_transfer(MainGroupInfo3, RepGroupInfo3),

    wait_for_replica_cleanup(),

    MainGroupInfo4 = get_group_info(),
    {RepGroupInfo4} = couch_util:get_value(replica_group_info, MainGroupInfo4),
    verify_group_info_after_replicas_transfer(MainGroupInfo4, RepGroupInfo4),

    {View1QueryResult5, Group9}  = query_reduce_view(<<"view_1">>, false),
    {View2QueryResult5, Group10} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        View1QueryResult5,
        ExpectedView1Result2,
        "Reduce view has value " ++ couch_util:to_list(ExpectedView1Result2)),
    etap:is(
        View2QueryResult5,
        ExpectedView2Result2,
        "Reduce view 2 has value " ++ couch_util:to_list(ExpectedView2Result2)),

    verify_main_group_btrees_3(Group9),
    verify_replica_group_btrees_3(Group9),
    compare_groups(Group9, Group10),

    compact_main_view_group(),
    compact_replica_view_group(),

    {View1QueryResult6, Group11}  = query_reduce_view(<<"view_1">>, false),
    {View2QueryResult6, Group12} = query_reduce_view(<<"view_2">>, false),
    etap:is(
        View1QueryResult6,
        ExpectedView1Result2,
        "Reduce view has value " ++ couch_util:to_list(ExpectedView1Result2)),
    etap:is(
        View2QueryResult6,
        ExpectedView2Result2,
        "Reduce view 2 has value " ++ couch_util:to_list(ExpectedView2Result2)),

    verify_main_group_btrees_3(Group11),
    verify_replica_group_btrees_3(Group11),
    compare_groups(Group11, Group12),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


query_reduce_view(ViewName, Stale) ->
    query_reduce_view(ViewName, Stale, []).

query_reduce_view(ViewName, Stale, Partitions) ->
    etap:diag("Querying reduce view " ++ binary_to_list(ViewName) ++ " with ?group=true"),
    GroupReq = #set_view_group_req{
        stale = Stale,
        wanted_partitions = Partitions,
        debug = true
    },
    {ok, View, Group, []} = couch_set_view:get_reduce_view(
        test_set_name(), ddoc_id(), ViewName, GroupReq),
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


verify_group_info_during_replicas_transfer(MainGroupInfo, RepGroupInfo) ->
    etap:diag("Verifying main and replica group infos obtained "
        "right after activating the replica partitions"),
    MainActive = couch_util:get_value(active_partitions, MainGroupInfo),
    Diff = ordsets:subtract(MainActive, lists:seq(0, 31)),
    etap:is(
        ordsets:intersection(MainActive, lists:seq(0, 31)),
        lists:seq(0, 31),
        "Main group had partitions [ 0 .. 31 ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, MainGroupInfo),
        ordsets:subtract(lists:seq(32, 63), Diff),
        "Main group had [ 32 .. 63 ] - Diff as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, MainGroupInfo),
        [],
        "Main group had [ ] as cleanup partitions"),
    etap:is(
        couch_util:get_value(replica_partitions, MainGroupInfo),
        ordsets:subtract(lists:seq(32, 63), Diff),
        "Main group had [ 32 .. 63 ] - Diff as replica partitions"),
    etap:is(
        couch_util:get_value(replicas_on_transfer, MainGroupInfo),
        ordsets:subtract(lists:seq(32, 63), Diff),
        "Main group had [ 32 .. 63 ] - Diff as replicas on transfer"),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo),
        ordsets:subtract(lists:seq(32, 63), Diff),
        "Replica group had [ 32 .. 63 ] - Diff as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo),
        [],
        "Replica group had [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, RepGroupInfo),
        [],
        "Replica group had [ ] as cleanup partitions").


verify_group_info_after_replicas_transfer(MainGroupInfo, RepGroupInfo) ->
    etap:diag("Verifying main and replica group infos obtained "
        "after the replica partitions were transferred"),
    etap:is(
        couch_util:get_value(active_partitions, MainGroupInfo),
        lists:seq(0, 63),
        "Main group had partitions [ 0 .. 63 ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, MainGroupInfo),
        [],
        "Main group has [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, MainGroupInfo),
        [],
        "Main group has [ ] as cleanup partitions"),
    etap:is(
        couch_util:get_value(replica_partitions, MainGroupInfo),
        [],
        "Main group has [ ] as replica partitions"),
    etap:is(
        couch_util:get_value(replicas_on_transfer, MainGroupInfo),
        [],
        "Main group has [ ] as replicas on transfer"),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as cleanup partitions").


wait_for_replica_full_update(RepGroupInfo) ->
    etap:diag("Waiting for a full replica group update"),
    {Stats} = couch_util:get_value(stats, RepGroupInfo),
    Updates = couch_util:get_value(full_updates, Stats),
    Pid = spawn(fun() ->
        wait_replica_update_loop(Updates)
    end),
    Ref = erlang:monitor(process, Pid),
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, noproc} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Failure waiting for full replica group update: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for replica group update")
    end.


wait_for_replica_cleanup() ->
    etap:diag("Waiting for replica index cleanup to finish"),
    MainGroupInfo = get_group_info(),
    {RepGroupInfo} = couch_util:get_value(replica_group_info, MainGroupInfo),
    Pid = spawn(fun() ->
        wait_replica_cleanup_loop(RepGroupInfo)
    end),
    Ref = erlang:monitor(process, Pid),
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, noproc} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Failure waiting for replica index cleanup: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for replica index cleanup")
    end.


wait_replica_cleanup_loop(GroupInfo) ->
    case couch_util:get_value(cleanup_partitions, GroupInfo) of
    [] ->
        {Stats} = couch_util:get_value(stats, GroupInfo),
        Cleanups = couch_util:get_value(cleanups, Stats),
        etap:is(
            (is_integer(Cleanups) andalso (Cleanups > 0)),
            true,
            "Replica group stats has at least 1 full cleanup");
    _ ->
        ok = timer:sleep(500),
        MainGroupInfo = get_group_info(),
        {NewRepGroupInfo} = couch_util:get_value(replica_group_info, MainGroupInfo),
        wait_replica_cleanup_loop(NewRepGroupInfo)
    end.


wait_replica_update_loop(Updates) ->
    MainGroupInfo = get_group_info(),
    {RepGroupInfo} = couch_util:get_value(replica_group_info, MainGroupInfo),
    {Stats} = couch_util:get_value(stats, RepGroupInfo),
    case couch_util:get_value(full_updates, Stats) > Updates of
    true ->
        ok;
    false ->
        ok = timer:sleep(1000),
        wait_replica_update_loop(Updates)
    end.


wait_for_main_full_update(GroupInfo, ExpectedReduceValue1, ExpectedReduceValue2) ->
    etap:diag("Waiting for a full main group update"),
    {Stats} = couch_util:get_value(stats, GroupInfo),
    Updates = couch_util:get_value(full_updates, Stats),
    Pid = spawn(fun() ->
        NumQueries = wait_main_update_loop(
            Updates, ExpectedReduceValue1, ExpectedReduceValue2, lists:seq(0, 63), 0),
        % This assertion works as an alarm. Normally NumQueries varies
        % per test run but it's always strictly greater than 0.
        % On 2 different machines/hardware, it's normally greater than 100,
        % which is most than enough for this test's purpose.
        etap:is(NumQueries > 0, true,
            "At least one query was done while the replica partitions data" ++
            " was being transferred"),
        etap:diag("Performed " ++ integer_to_list(NumQueries) ++
            " queries while the replica partitions were being" ++
            " transferred from the replica group to main group.")
    end),
    Ref = erlang:monitor(process, Pid),
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Failure waiting for full main group update: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for main group update")
    end.


wait_main_update_loop(Updates, ExpectedReduceValue1, ExpectedReduceValue2, ExpectedPartitions, NumQueriesDone) ->
    MainGroupInfo = get_group_info(),
    {Stats} = couch_util:get_value(stats, MainGroupInfo),
    case couch_util:get_value(full_updates, Stats) > Updates of
    true ->
        NumQueriesDone;
    false ->
        {RedValue1, _} = query_reduce_view(<<"view_1">>, false, ExpectedPartitions),
        {RedValue2, _} = query_reduce_view(<<"view_2">>, false, ExpectedPartitions),
        case RedValue1 =:= ExpectedReduceValue1 of
        true ->
            etap:diag("Reduce view 1 returned expected value " ++
                couch_util:to_list(ExpectedReduceValue1));
        false ->
            etap:bail("Reduce view 1 did not return expected value " ++
                couch_util:to_list(ExpectedReduceValue1) ++
                ", got " ++ couch_util:to_list(RedValue1)),
            exit(bad_reduce_value)
        end,
        case RedValue2 =:= ExpectedReduceValue2 of
        true ->
            etap:diag("Reduce view 2 returned expected value " ++
                couch_util:to_list(ExpectedReduceValue2));
        false ->
            etap:bail("Reduce view 2 did not return expected value " ++
                couch_util:to_list(ExpectedReduceValue2) ++
                ", got " ++ couch_util:to_list(RedValue2)),
            exit(bad_reduce_value)
        end,
        wait_main_update_loop(
            Updates, ExpectedReduceValue1, ExpectedReduceValue2,
            ExpectedPartitions, NumQueriesDone + 2)
    end.


get_group_info() ->
    {ok, Info} = couch_set_view:get_group_info(test_set_name(), ddoc_id()),
    Info.


doc_id(I) ->
    iolist_to_binary(io_lib:format("doc_~8..0b", [I])).


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


create_set() ->
    couch_set_view:cleanup_index_files(test_set_name()),
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    DDoc = {[
        {<<"_id">>, ddoc_id()},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"view_1">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, doc.value); }">>},
                {<<"reduce">>, <<"_count">>}
            ]}},
            {<<"view_2">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, doc.value * 2); }">>},
                {<<"reduce">>, <<"_sum">>}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag("Configuring set view with partitions [0 .. 31] as active"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, 31),
        passive_partitions = [],
        use_replica_index = true
    },
    ok = couch_set_view:define_group(test_set_name(), ddoc_id(), Params).


compact_main_view_group() ->
    compact_view_group(main).

compact_replica_view_group() ->
    compact_view_group(replica).

compact_view_group(Type) ->
    {ok, CompactPid} = couch_set_view_compactor:start_compact(test_set_name(), ddoc_id(), Type),
    etap:diag("Waiting for " ++ atom_to_list(Type) ++ " view group compaction to finish"),
    Ref = erlang:monitor(process, CompactPid),
    receive
    {'DOWN', Ref, process, CompactPid, normal} ->
        ok;
    {'DOWN', Ref, process, CompactPid, Reason} ->
        etap:bail("Failure compacting " ++ atom_to_list(Type) ++ " group: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for " ++ atom_to_list(Type) ++ " group compaction to finish")
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


verify_main_group_btrees_1(Group) ->
    etap:diag("Verifying main view group"),
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
    View1 = get_view(<<"view_1">>, Views),
    View2 = get_view(<<"view_2">>, Views),
    etap:isnt(View1, View2, "Views 1 and 2 have different btrees"),
    #set_view{
        btree = View1Btree
    } = View1,
    #set_view{
        btree = View2Btree
    } = View2,
    ExpectedBitmask = couch_set_view_util:build_bitmask(lists:seq(0, 31)),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), lists:seq(0, 31)),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {num_docs() div 2, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {num_docs() div 2, [num_docs() div 2], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),
    ExpectedView2Reduction = [lists:sum(
        [I * 2 || I <- lists:seq(0, num_docs() - 1), (I rem 64) < 32])],
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View2Btree),
        {ok, {num_docs() div 2, ExpectedView2Reduction, ExpectedBitmask}},
        "View2 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, {_, IdBtreeFoldResult}} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, {NextId, I}) ->
            PartId = NextId rem 64,
            Value = [
                 {View2#set_view.id_num, doc_id(NextId)},
                 {View1#set_view.id_num, doc_id(NextId)}
            ],
            ExpectedKv = {doc_id(NextId), {PartId, Value}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            case PartId =:= 31 of
            true ->
                {ok, {NextId + 33, I + 1}};
            false ->
                {ok, {NextId + 1, I + 1}}
            end
        end,
        {0, 0}, []),
    etap:is(IdBtreeFoldResult, (num_docs() div 2),
        "Id Btree has " ++ integer_to_list(num_docs() div 2) ++ " entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, {_, View1BtreeFoldResult}} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, {NextId, I}) ->
            PartId = NextId rem 64,
            ExpectedKv = {
                {doc_id(NextId), doc_id(NextId)},
                {PartId, NextId}
            },
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            case PartId =:= 31 of
            true ->
                {ok, {NextId + 33, I + 1}};
            false ->
                {ok, {NextId + 1, I + 1}}
            end
        end,
        {0, 0}, []),
    etap:is(View1BtreeFoldResult, (num_docs() div 2),
        "View1 Btree has " ++ integer_to_list(num_docs() div 2) ++ " entries"),

    etap:diag("Verifying the View2 Btree"),
    {ok, _, {_, View2BtreeFoldResult}} = couch_set_view_test_util:fold_view_btree(
        Group,
        View2Btree,
        fun(Kv, _, {NextId, I}) ->
            PartId = NextId rem 64,
            ExpectedKv = {
                {doc_id(NextId), doc_id(NextId)},
                {PartId, NextId * 2}
            },
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View2 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            case PartId =:= 31 of
            true ->
                {ok, {NextId + 33, I + 1}};
            false ->
                {ok, {NextId + 1, I + 1}}
            end
        end,
        {0, 0}, []),
    etap:is(View2BtreeFoldResult, (num_docs() div 2),
        "View2 Btree has " ++ integer_to_list(num_docs() div 2) ++ " entries").


verify_replica_group_btrees_1(MainGroup) ->
    etap:diag("Verifying replica view group"),
    etap:is(
        MainGroup#set_view_group.replica_group,
        nil,
        "Main group points to a nil replica group"),
    {ok, RepGroup, 0} = gen_server:call(
        MainGroup#set_view_group.replica_pid,
        #set_view_group_req{stale = ok, debug = true}),
    #set_view_group{
        id_btree = IdBtree,
        views = Views,
        index_header = #set_view_index_header{
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = RepGroup,
    etap:is(2, length(Views), "2 view btrees in the group"),
    View1 = get_view(<<"view_1">>, Views),
    View2 = get_view(<<"view_2">>, Views),
    etap:isnt(View1, View2, "Views 1 and 2 have different btrees"),
    #set_view{
        btree = View1Btree
    } = View1,
    #set_view{
        btree = View2Btree
    } = View2,

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(MainGroup, IdBtree),
        {ok, {0, 0}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(MainGroup, View1Btree),
        {ok, {0, [0], 0}},
        "View1 Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(MainGroup, View2Btree),
        {ok, {0, [0], 0}},
        "View2 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, [], "Header has right update seqs list"),
    etap:is(Abitmask, 0, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_btree:fold(
        IdBtree,
        fun(_Kv, _, I) ->
            {ok, I + 1}
        end,
        0, []),
    etap:is(IdBtreeFoldResult, 0, "Id Btree is empty"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, View1BtreeFoldResult} = couch_btree:fold(
        View1Btree,
        fun(_Kv, _, I) ->
            {ok, I + 1}
        end,
        0, []),
    etap:is(View1BtreeFoldResult, 0, "View1 Btree is empty"),

    etap:diag("Verifying the View2 Btree"),
    {ok, _, View2BtreeFoldResult} = couch_btree:fold(
        View2Btree,
        fun(_Kv, _, I) ->
            {ok, I + 1}
        end,
        0, []),
    etap:is(View2BtreeFoldResult, 0, "View2 Btree is empty").


verify_main_group_btrees_2(Group) ->
    verify_main_group_btrees_1(Group).


verify_replica_group_btrees_2(MainGroup) ->
    etap:diag("Verifying replica view group"),
    etap:is(
        MainGroup#set_view_group.replica_group,
        nil,
        "Main group points to a nil replica group"),
    {ok, RepGroup, 0} = gen_server:call(
        MainGroup#set_view_group.replica_pid,
        #set_view_group_req{stale = ok, debug = true}),
    #set_view_group{
        id_btree = IdBtree,
        views = Views,
        index_header = #set_view_index_header{
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = RepGroup,
    etap:is(2, length(Views), "2 view btrees in the group"),
    View1 = get_view(<<"view_1">>, Views),
    View2 = get_view(<<"view_2">>, Views),
    etap:isnt(View1, View2, "Views 1 and 2 have different btrees"),
    #set_view{
        btree = View1Btree
    } = View1,
    #set_view{
        btree = View2Btree
    } = View2,
    ExpectedBitmask = couch_set_view_util:build_bitmask(lists:seq(32, 63)),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), lists:seq(32, 63)),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(MainGroup, IdBtree),
        {ok, {num_docs() div 2, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(MainGroup, View1Btree),
        {ok, {num_docs() div 2, [num_docs() div 2], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),
    ExpectedView2Reduction = [lists:sum(
        [I * 2 || I <- lists:seq(0, num_docs() - 1), (I rem 64) > 31])],
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(MainGroup, View2Btree),
        {ok, {num_docs() div 2, ExpectedView2Reduction, ExpectedBitmask}},
        "View2 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, 0, "Header has right active bitmask"),
    etap:is(Pbitmask, ExpectedBitmask, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, {_, IdBtreeFoldResult}} = couch_set_view_test_util:fold_id_btree(
        MainGroup,
        IdBtree,
        fun(Kv, _, {NextId, I}) ->
            PartId = NextId rem 64,
            Value = [
                 {View2#set_view.id_num, doc_id(NextId)},
                 {View1#set_view.id_num, doc_id(NextId)}
            ],
            ExpectedKv = {doc_id(NextId), {PartId, Value}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            case PartId =:= 63 of
            true ->
                {ok, {NextId + 33, I + 1}};
            false ->
                {ok, {NextId + 1, I + 1}}
            end
        end,
        {32, 0}, []),
    etap:is(IdBtreeFoldResult, (num_docs() div 2),
        "Id Btree has " ++ integer_to_list(num_docs() div 2) ++ " entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, {_, View1BtreeFoldResult}} = couch_set_view_test_util:fold_view_btree(
        MainGroup,
        View1Btree,
        fun(Kv, _, {NextId, I}) ->
            PartId = NextId rem 64,
            ExpectedKv = {
                {doc_id(NextId), doc_id(NextId)},
                {PartId, NextId}
            },
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            case PartId =:= 63 of
            true ->
                {ok, {NextId + 33, I + 1}};
            false ->
                {ok, {NextId + 1, I + 1}}
            end
        end,
        {32, 0}, []),
    etap:is(View1BtreeFoldResult, (num_docs() div 2),
        "View1 Btree has " ++ integer_to_list(num_docs() div 2) ++ " entries"),

    etap:diag("Verifying the View2 Btree"),
    {ok, _, {_, View2BtreeFoldResult}} = couch_set_view_test_util:fold_view_btree(
        MainGroup,
        View2Btree,
        fun(Kv, _, {NextId, I}) ->
            PartId = NextId rem 64,
            ExpectedKv = {
                {doc_id(NextId), doc_id(NextId)},
                {PartId, NextId * 2}
            },
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View2 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            case PartId =:= 63 of
            true ->
                {ok, {NextId + 33, I + 1}};
            false ->
                {ok, {NextId + 1, I + 1}}
            end
        end,
        {32, 0}, []),
    etap:is(View2BtreeFoldResult, (num_docs() div 2),
        "View2 Btree has " ++ integer_to_list(num_docs() div 2) ++ " entries").


verify_main_group_btrees_3(Group) ->
    etap:diag("Verifying main view group"),
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
    View1 = get_view(<<"view_1">>, Views),
    View2 = get_view(<<"view_2">>, Views),
    etap:isnt(View1, View2, "Views 1 and 2 have different btrees"),
    #set_view{
        btree = View1Btree
    } = View1,
    #set_view{
        btree = View2Btree
    } = View2,
    ExpectedBitmask = couch_set_view_util:build_bitmask(lists:seq(0, 63)),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), lists:seq(0, 63)),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {num_docs(), ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {num_docs(), [num_docs()], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),
    ExpectedView2Reduction = [lists:sum([I * 2 || I <- lists:seq(0, num_docs() - 1)])],
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View2Btree),
        {ok, {num_docs(), ExpectedView2Reduction, ExpectedBitmask}},
        "View2 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, I) ->
            PartId = I rem 64,
            Value = [
                 {View2#set_view.id_num, doc_id(I)},
                 {View1#set_view.id_num, doc_id(I)}
            ],
            ExpectedKv = {doc_id(I), {PartId, Value}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            {ok, I + 1}
        end,
        0, []),
    etap:is(IdBtreeFoldResult, num_docs(),
        "Id Btree has " ++ integer_to_list(num_docs()) ++ " entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, View1BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, I) ->
            PartId = I rem 64,
            ExpectedKv = {{doc_id(I), doc_id(I)}, {PartId, I}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            {ok, I + 1}
        end,
        0, []),
    etap:is(View1BtreeFoldResult, num_docs(),
        "View1 Btree has " ++ integer_to_list(num_docs()) ++ " entries"),

    etap:diag("Verifying the View2 Btree"),
    {ok, _, View2BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View2Btree,
        fun(Kv, _, I) ->
            PartId = I rem 64,
            ExpectedKv = {{doc_id(I), doc_id(I)}, {PartId, I * 2}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View2 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            {ok, I + 1}
        end,
        0, []),
    etap:is(View2BtreeFoldResult, num_docs(),
        "View2 Btree has " ++ integer_to_list(num_docs()) ++ " entries").


verify_replica_group_btrees_3(MainGroup) ->
    verify_replica_group_btrees_1(MainGroup).


compare_groups(Group1, Group2) ->
    etap:is(
        Group2#set_view_group.views,
        Group1#set_view_group.views,
        "View states are equal"),
    etap:is(
        Group2#set_view_group.index_header,
        Group1#set_view_group.index_header,
        "Index headers are equal").
