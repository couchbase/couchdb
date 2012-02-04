#!/usr/bin/env escript
%% -*- erlang -*-

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

-define(MAX_UPDATE_WAIT_TIME, 600 * 1000).

% from couch_set_view.hrl
-record(set_view_params, {
    max_partitions = 0,
    active_partitions = [],
    passive_partitions = [],
    use_replica_index = false
}).

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


test_set_name() -> <<"couch_test_set_index_replicas_transfer">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 241107.


main(_) ->
    test_util:init_code_path(),

    etap:plan(38),
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

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    populate_set(),

    MainGroupInfo1 = get_group_info(),
    {RepGroupInfo1} = couch_util:get_value(replica_group_info, MainGroupInfo1),

    DocCount1 = couch_set_view_test_util:doc_count(test_set_name(), lists:seq(0, 31)),
    etap:is(
        query_reduce_view(false),
        DocCount1,
       "Reduce view has value "  ++ couch_util:to_list(DocCount1)),

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

    etap:is(
        query_reduce_view(false),
        DocCount1,
       "Reduce view has value "  ++ couch_util:to_list(DocCount1)),
    wait_for_replica_full_update(RepGroupInfo2),
    etap:is(
        query_reduce_view(false),
        DocCount1,
       "Reduce view has value " ++ couch_util:to_list(DocCount1)),

    DocCount2 = couch_set_view_test_util:doc_count(test_set_name(), lists:seq(0, 63)),
    etap:diag("Marking partitions [ 32 .. 63 ] as active"),
    ok = couch_set_view:set_partition_states(test_set_name(), ddoc_id(), lists:seq(32, 63), [], []),

    MainGroupInfo3 = get_group_info(),
    {RepGroupInfo3} = couch_util:get_value(replica_group_info, MainGroupInfo3),

    etap:is(
        query_reduce_view(false),
        DocCount2,
       "Reduce view has value " ++ couch_util:to_list(DocCount2)),

    etap:diag("Waiting for transfer of replica partitions [ 32 .. 63 ] to main group"),
    wait_for_main_full_update(MainGroupInfo2, DocCount2),
    etap:diag("Replicas transferred to main group"),

    verify_group_info_during_replicas_transfer(MainGroupInfo3, RepGroupInfo3),

    wait_for_replica_cleanup(),

    MainGroupInfo4 = get_group_info(),
    {RepGroupInfo4} = couch_util:get_value(replica_group_info, MainGroupInfo4),
    verify_group_info_after_replicas_transfer(MainGroupInfo4, RepGroupInfo4),

    etap:is(
        query_reduce_view(false),
        DocCount2,
       "Reduce view has value " ++ couch_util:to_list(DocCount2)),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


query_reduce_view(Stale) ->
    query_reduce_view(Stale, []).

query_reduce_view(Stale, Partitions) ->
    etap:diag("Querying reduce view with ?group=true"),
    {ok, View, Group, []} = couch_set_view:get_reduce_view(
        test_set_name(), ddoc_id(), <<"test">>, Stale, Partitions),
    KeyGroupFun = fun({_Key1, _}, {_Key2, _}) -> true end,
    FoldFun = fun(Key, Red, Acc) -> {ok, [{Key, Red} | Acc]} end,
    ViewArgs = #view_query_args{
        run_reduce = true,
        view_name = <<"test">>
    },
    {ok, Rows} = couch_set_view:fold_reduce(Group, View, FoldFun, [], KeyGroupFun, ViewArgs),
    couch_set_view:release_group(Group),
    [{_Key, RedValue}] = Rows,
    RedValue.


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
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Failure waiting for full replica group update: " ++ couch_util:to_list(Reason))
    after ?MAX_UPDATE_WAIT_TIME ->
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
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Failure waiting for replica index cleanup: " ++ couch_util:to_list(Reason))
    after ?MAX_UPDATE_WAIT_TIME ->
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


wait_for_main_full_update(GroupInfo, ExpectedReduceValue) ->
    etap:diag("Waiting for a full main group update"),
    {Stats} = couch_util:get_value(stats, GroupInfo),
    Updates = couch_util:get_value(full_updates, Stats),
    Pid = spawn(fun() ->
        wait_main_update_loop(Updates, ExpectedReduceValue, lists:seq(0, 63))
    end),
    Ref = erlang:monitor(process, Pid),
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Failure waiting for full main group update: " ++ couch_util:to_list(Reason))
    after ?MAX_UPDATE_WAIT_TIME ->
        etap:bail("Timeout waiting for main group update")
    end.


wait_main_update_loop(Updates, ExpectedReduceValue, ExpectedPartitions) ->
    MainGroupInfo = get_group_info(),
    {Stats} = couch_util:get_value(stats, MainGroupInfo),
    case couch_util:get_value(full_updates, Stats) > Updates of
    true ->
        ok;
    false ->
        RedValue = query_reduce_view(false, ExpectedPartitions),
        case RedValue =:= ExpectedReduceValue of
        true ->
            etap:diag("Reduce view returned expected value " ++
                couch_util:to_list(ExpectedReduceValue));
        false ->
            etap:bail("Reduce view did not return expected value " ++
                couch_util:to_list(ExpectedReduceValue) ++
                ", got " ++ couch_util:to_list(RedValue)),
            exit(bad_reduce_value)
        end,
        wait_main_update_loop(Updates, ExpectedReduceValue, ExpectedPartitions)
    end.


get_group_info() ->
    {ok, Info} = couch_set_view:get_group_info(test_set_name(), ddoc_id()),
    Info.


populate_set() ->
    couch_set_view:cleanup_index_files(test_set_name()),
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    DDoc = {[
        {<<"_id">>, ddoc_id()},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, null); }">>},
                {<<"reduce">>, <<"_count">>}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    DocList = lists:map(
        fun(I) ->
            {[
                {<<"_id">>, iolist_to_binary(["doc", integer_to_list(I)])},
                {<<"value">>, I}
            ]}
        end,
        lists:seq(1, num_docs())),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList),
    etap:diag("Configuring set view with partitions [0 .. 31] as active"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, 31),
        passive_partitions = [],
        use_replica_index = true
    },
    ok = couch_set_view:define_group(test_set_name(), ddoc_id(), Params).
