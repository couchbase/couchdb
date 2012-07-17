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
-define(MAX_WAIT_TIME, 900 * 1000).

-define(etap_match(Got, Expected, Desc),
        etap:fun_is(fun(XXXXXX) ->
            case XXXXXX of Expected -> true; _ -> false end
        end, Got, Desc)).

test_set_name() -> <<"couch_test_set_unindexable_partitions">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 25856.  % keep it a multiple of num_set_partitions()


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
    ActiveParts = lists:seq(0, num_set_partitions() - 1),
    ValueGenFun1 = fun(I) -> I end,
    update_documents(0, num_docs(), ValueGenFun1),

    trigger_update_and_wait(),

    ExpectedSeqs1 = couch_set_view_test_util:get_db_seqs(test_set_name(), ActiveParts),
    ExpectedUnindexableSeqs1 = [],
    verify_btrees_1(ExpectedSeqs1, ExpectedUnindexableSeqs1, ValueGenFun1),

    Unindexable = lists:seq(0, num_set_partitions() - 1, 2),
    etap:diag("Marking the following partitions as unindexable: ~w", [Unindexable]),

    etap:is(
        couch_set_view:mark_partitions_unindexable(test_set_name(), ddoc_id(), Unindexable),
        ok,
        "Marked unindexable partitions"),

    ExpectedSeqs2 = [
        {P, S} ||
        {P, S} <- couch_set_view_test_util:get_db_seqs(test_set_name(), ActiveParts),
        (not ordsets:is_element(P, Unindexable))
    ],
    ExpectedUnindexableSeqs2 = [
        {P, S} ||
        {P, S} <- couch_set_view_test_util:get_db_seqs(test_set_name(), ActiveParts),
        ordsets:is_element(P, Unindexable)
    ],
    verify_btrees_1(ExpectedSeqs2, ExpectedUnindexableSeqs2, ValueGenFun1),

    compact_view_group(),
    verify_btrees_1(ExpectedSeqs2, ExpectedUnindexableSeqs2, ValueGenFun1),

    MarkResult1 = try
        couch_set_view:set_partition_states(
            test_set_name(), ddoc_id(), [], [lists:last(Unindexable)], [])
    catch throw:Error ->
        Error
    end,
    ?etap_match(MarkResult1,
                {error, _},
                "Error setting unindexable partition to passive state"),

    MarkResult2 = try
        couch_set_view:set_partition_states(
            test_set_name(), ddoc_id(), [], [], [lists:last(Unindexable)])
    catch throw:Error2 ->
        Error2
    end,
    ?etap_match(MarkResult2,
                {error, _},
                "Error setting unindexable partition to cleanup state"),

    ValueGenFun2 = fun(I) -> I * 3 end,
    update_documents(0, num_docs(), ValueGenFun2),
    % stale=false request should block caller until all active partitions are
    % marked as indexable again (updates to them happened after being marked as
    % unindexable).
    {ClientPid, ClientMonRef} = spawn_monitor(fun() ->
        Snapshot = get_group_snapshot(false),
        exit({ok, Snapshot})
    end),
    receive
    {'DOWN', ClientMonRef, process, ClientPid, Reason2} ->
        etap:bail("Client was not blocked, exit reason: " ++ couch_util:to_list(Reason2))
    after 10000 ->
        etap:diag("Client is blocked")
    end,

    ExpectedSeqs3 = [
        {P, S} ||
        {P, S} <- couch_set_view_test_util:get_db_seqs(test_set_name(), ActiveParts),
        (not ordsets:is_element(P, Unindexable))
    ],
    ExpectedUnindexableSeqs3 = ExpectedUnindexableSeqs2,
    verify_btrees_2(ExpectedSeqs3, ExpectedUnindexableSeqs3, ValueGenFun1, ValueGenFun2),

    compact_view_group(),
    verify_btrees_2(ExpectedSeqs3, ExpectedUnindexableSeqs3, ValueGenFun1, ValueGenFun2),

    etap:diag("Marking the following partitions as indexable (again): ~w", [Unindexable]),

    etap:is(
        couch_set_view:mark_partitions_indexable(test_set_name(), ddoc_id(), Unindexable),
        ok,
        "Marked indexable partitions"),

    trigger_update_and_wait(),

    % Client should have been unblocked already or is just about to be unblocked.
    receive
    {'DOWN', ClientMonRef, process, ClientPid, {ok, #set_view_group{}}} ->
        etap:diag("Client was unblocked");
    {'DOWN', ClientMonRef, process, ClientPid, Reason} ->
        etap:bail("Client was not blocked, exit reason: " ++ couch_util:to_list(Reason))
    after 10000 ->
        etap:bail("Client is still blocked")
    end,

    ExpectedSeqs4 = couch_set_view_test_util:get_db_seqs(test_set_name(), ActiveParts),
    ExpectedUnindexableSeqs4 = [],
    verify_btrees_1(ExpectedSeqs4, ExpectedUnindexableSeqs4, ValueGenFun2),

    compact_view_group(),
    verify_btrees_1(ExpectedSeqs4, ExpectedUnindexableSeqs4, ValueGenFun2),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


get_group_snapshot(Staleness) ->
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    {ok, Group, 0} = gen_server:call(
        GroupPid, #set_view_group_req{stale = Staleness}, infinity),
    Group.


trigger_update_and_wait() ->
    etap:diag("Trigerring index update"),
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    {ok, UpPid} = gen_server:call(GroupPid, start_updater, infinity),
    case is_pid(UpPid) of
    true ->
        ok;
    false ->
        etap:bail("Updater was not triggered~n")
    end,
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
    end.


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
            {<<"view_1">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, doc.value); }">>},
                {<<"reduce">>, <<"_sum">>}
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


update_documents(StartId, Count, ValueGenFun) ->
    etap:diag("Updating " ++ integer_to_list(Count) ++ " new documents"),
    DocList0 = lists:map(
        fun(I) ->
            {I rem num_set_partitions(), {[
                {<<"_id">>, doc_id(I)},
                {<<"value">>, ValueGenFun(I)}
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


verify_btrees_1(ExpectedSeqs, ExpectedUnindexableSeqs, ValueGenFun) ->
    Group = get_group_snapshot(ok),
    #set_view_group{
        id_btree = IdBtree,
        views = Views,
        index_header = #set_view_index_header{
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask,
            unindexable_seqs = UnindexableSeqs
        }
    } = Group,
    etap:is(1, length(Views), "1 view btree in the group"),
    [View1] = Views,
    #set_view{
        btree = View1Btree
    } = View1,
    ActiveParts = lists:seq(0, num_set_partitions() - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedKVCount = (num_docs() div num_set_partitions()) * length(ActiveParts),
    ExpectedView1Reduction = lists:sum([
        ValueGenFun(I) || I <- lists:seq(0, num_docs() - 1)
    ]),

    etap:is(
        couch_btree:full_reduce(IdBtree),
        {ok, {ExpectedKVCount, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_btree:full_reduce(View1Btree),
        {ok, {ExpectedKVCount, [ExpectedView1Reduction], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, ExpectedSeqs, "Header has right update seqs list"),
    etap:is(UnindexableSeqs, ExpectedUnindexableSeqs, "Header has right unindexable seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_btree:fold(
        IdBtree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            UserKey = DocId = doc_id(I),
            Value = [{View1#set_view.id_num, UserKey}],
            ExpectedKv = {DocId, {PartId, Value}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            {ok, I + 1}
        end,
        0, []),
    etap:is(IdBtreeFoldResult, ExpectedKVCount,
        "Id Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, View1BtreeFoldResult} = couch_btree:fold(
        View1Btree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            Key = DocId = doc_id(I),
            Value = ValueGenFun(I),
            ExpectedKv = {{Key, DocId}, {PartId, {json, ?JSON_ENCODE(Value)}}},
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


verify_btrees_2(ExpectedSeqs, ExpectedUnindexableSeqs, ValueGenFun1, ValueGenFun2) ->
    Group = get_group_snapshot(ok),
    #set_view_group{
        id_btree = IdBtree,
        views = Views,
        index_header = #set_view_index_header{
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask,
            unindexable_seqs = UnindexableSeqs
        }
    } = Group,
    etap:is(1, length(Views), "1 view btree in the group"),
    [View1] = Views,
    #set_view{
        btree = View1Btree
    } = View1,
    ActiveParts = lists:seq(0, num_set_partitions() - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedKVCount = (num_docs() div num_set_partitions()) * length(ActiveParts),
    ExpectedView1Reduction = lists:sum([
        ValueGenFun1(I) || I <- lists:seq(0, num_docs() - 1),
        orddict:is_key(I rem num_set_partitions(), ExpectedUnindexableSeqs)
    ]) + lists:sum([
        ValueGenFun2(I) || I <- lists:seq(0, num_docs() - 1),
        (not orddict:is_key(I rem num_set_partitions(), ExpectedUnindexableSeqs))
    ]),

    etap:is(
        couch_btree:full_reduce(IdBtree),
        {ok, {ExpectedKVCount, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_btree:full_reduce(View1Btree),
        {ok, {ExpectedKVCount, [ExpectedView1Reduction], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, ExpectedSeqs, "Header has right update seqs list"),
    etap:is(UnindexableSeqs, ExpectedUnindexableSeqs, "Header has right unindexable seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_btree:fold(
        IdBtree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            UserKey = DocId = doc_id(I),
            Value = [{View1#set_view.id_num, UserKey}],
            ExpectedKv = {DocId, {PartId, Value}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            {ok, I + 1}
        end,
        0, []),
    etap:is(IdBtreeFoldResult, ExpectedKVCount,
        "Id Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, View1BtreeFoldResult} = couch_btree:fold(
        View1Btree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            case orddict:is_key(PartId, ExpectedUnindexableSeqs) of
            true ->
                Value = ValueGenFun1(I);
            false ->
                Value = ValueGenFun2(I)
            end,
            Key = DocId = doc_id(I),
            ExpectedKv = {{Key, DocId}, {PartId, {json, ?JSON_ENCODE(Value)}}},
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
