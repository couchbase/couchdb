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

test_set_name() -> <<"couch_test_set_unindexable_partitions">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 16448.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(151),
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
    ReplicaParts = lists:seq(num_set_partitions() div 2, num_set_partitions() - 1),
    couch_set_view:add_replica_partitions(
        mapreduce_view, test_set_name(), ddoc_id(), ReplicaParts),

    ActiveParts = lists:seq(0, (num_set_partitions() div 2) - 1),
    ValueGenFun1 = fun(I) -> I end,
    update_documents(0, num_docs(), ValueGenFun1),

    main_trigger_wait_for_initial_build(),
    replica_trigger_wait_for_initial_build(),

    ExpectedSeqs1 = couch_set_view_test_util:get_db_seqs(test_set_name(), ActiveParts),
    ExpectedUnindexableSeqs1 = [],
    verify_btrees_1(ActiveParts, [], ExpectedSeqs1, ExpectedUnindexableSeqs1, ValueGenFun1),

    Unindexable = lists:seq(0, (num_set_partitions() div 2) - 1, 2),
    etap:diag("Marking the following partitions as unindexable: ~w", [Unindexable]),

    etap:is(
        couch_set_view:mark_partitions_unindexable(
            mapreduce_view, test_set_name(), ddoc_id(), Unindexable),
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
    verify_btrees_1(ActiveParts, [], ExpectedSeqs2, ExpectedUnindexableSeqs2, ValueGenFun1),

    compact_view_group(),
    verify_btrees_1(ActiveParts, [], ExpectedSeqs2, ExpectedUnindexableSeqs2, ValueGenFun1),

    MarkResult1 = try
        couch_set_view:set_partition_states(
            mapreduce_view, test_set_name(), ddoc_id(), [],
            [lists:last(Unindexable)], [])
    catch throw:Error ->
        Error
    end,
    ?etap_match(MarkResult1,
                {error, _},
                "Error setting unindexable partition to passive state"),

    MarkResult2 = try
        couch_set_view:set_partition_states(
            mapreduce_view, test_set_name(), ddoc_id(), [], [],
            [lists:last(Unindexable)])
    catch throw:Error2 ->
        Error2
    end,
    ?etap_match(MarkResult2,
                {error, _},
                "Error setting unindexable partition to cleanup state"),

    MarkResult3 = try
        couch_set_view:mark_partitions_unindexable(
            mapreduce_view, test_set_name(), ddoc_id(),
            [lists:last(ReplicaParts)])
    catch throw:Error3 ->
        Error3
    end,
    ?etap_match(MarkResult3,
                {error, _},
                "Error marking replica partition as unindexable"),

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
        couch_set_view:mark_partitions_indexable(
            mapreduce_view, test_set_name(), ddoc_id(), Unindexable),
        ok,
        "Marked indexable partitions"),

    trigger_main_update_and_wait(),

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
    verify_btrees_1(ActiveParts, [], ExpectedSeqs4, ExpectedUnindexableSeqs4, ValueGenFun2),

    compact_view_group(),
    verify_btrees_1(ActiveParts, [], ExpectedSeqs4, ExpectedUnindexableSeqs4, ValueGenFun2),

    % Mark first replica partition as active. Verify that after this it's possible
    % to mark it as unindexable and then back to indexable once again.
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    ok = gen_server:call(GroupPid, {set_auto_transfer_replicas, false}, infinity),

    ActivateReplicaResult = couch_set_view:set_partition_states(
        mapreduce_view, test_set_name(), ddoc_id(),
        [hd(ReplicaParts)], [], []),
    ?etap_match(ActivateReplicaResult,
                ok,
                "Activated replica partition " ++ integer_to_list(hd(ReplicaParts))),

    Unindexable2 = [hd(ReplicaParts)],
    etap:is(
        couch_set_view:mark_partitions_unindexable(
            mapreduce_view, test_set_name(), ddoc_id(), Unindexable2),
        ok,
        "Marked replica partition on transfer as unindexable"),

    PassiveParts = [hd(ReplicaParts)],
    ExpectedSeqs5 = ExpectedSeqs4,
    ExpectedUnindexableSeqs5 = [{hd(ReplicaParts), 0}],
    verify_btrees_1(ActiveParts, PassiveParts, ExpectedSeqs5, ExpectedUnindexableSeqs5, ValueGenFun2),
    compact_view_group(),
    verify_btrees_1(ActiveParts, PassiveParts, ExpectedSeqs5, ExpectedUnindexableSeqs5, ValueGenFun2),

    etap:is(
        couch_set_view:mark_partitions_indexable(
            mapreduce_view, test_set_name(), ddoc_id(), Unindexable2),
        ok,
        "Marked replica partition on transfer as indexable again"),

    ExpectedSeqs6 = ExpectedSeqs5 ++ [{hd(ReplicaParts), 0}],
    ExpectedUnindexableSeqs6 = [],
    verify_btrees_1(ActiveParts, PassiveParts, ExpectedSeqs6, ExpectedUnindexableSeqs6, ValueGenFun2),
    compact_view_group(),
    verify_btrees_1(ActiveParts, PassiveParts, ExpectedSeqs6, ExpectedUnindexableSeqs6, ValueGenFun2),

    % Test for MB-8677
    etap:is(
        couch_set_view:mark_partitions_unindexable(mapreduce_view, test_set_name(), ddoc_id(), Unindexable2),
        ok,
        "Marked replica partition on transfer as unindexable again"),
    etap:is(
        couch_set_view:set_partition_states(mapreduce_view, test_set_name(), ddoc_id(), Unindexable2, [], []),
        ok,
        "Request to mark replicas on transfer to active didn't raise an error"),
    % Trigger the automatic transfer
    ok = gen_server:call(GroupPid, {set_auto_transfer_replicas, true}, infinity),
    trigger_main_update_and_wait(),
    ExpectedSeqs7 = ExpectedSeqs5 ++ [{hd(ReplicaParts), 514}],
    verify_btrees_1(ActiveParts ++ Unindexable2,
                    [],
                    ExpectedSeqs7, ExpectedUnindexableSeqs5, ValueGenFun2),

    % Mark it as indexable again, so that it can be cleanup
    etap:is(
        couch_set_view:mark_partitions_indexable(mapreduce_view, test_set_name(), ddoc_id(), Unindexable2),
        ok,
        "Marked replica partition on transfer as indexable again"),

    ok = couch_set_view:set_partition_states(
        mapreduce_view, test_set_name(), ddoc_id(), [], [], [hd(ReplicaParts)]),
    wait_for_main_cleanup(),

    verify_btrees_1(ActiveParts, [], ExpectedSeqs4, ExpectedUnindexableSeqs4, ValueGenFun2),
    compact_view_group(),
    verify_btrees_1(ActiveParts, [], ExpectedSeqs4, ExpectedUnindexableSeqs4, ValueGenFun2),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:stop_server(),
    ok.


get_group_snapshot(Staleness) ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, Group, _} = gen_server:call(
        GroupPid, #set_view_group_req{stale = Staleness}, infinity),
    Group.


trigger_main_update_and_wait() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    trigger_update_and_wait(GroupPid).

trigger_replica_update_and_wait() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, ReplicaGroupPid} = gen_server:call(GroupPid, replica_pid, infinity),
    trigger_update_and_wait(ReplicaGroupPid).

trigger_update_and_wait(GroupPid) ->
    etap:diag("Trigerring index update"),
    {ok, UpPid} = gen_server:call(GroupPid, {start_updater, []}, infinity),
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


wait_for_main_cleanup() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, CleanerPid} = gen_server:call(GroupPid, start_cleaner, infinity),
    CleanerRef = erlang:monitor(process, CleanerPid),
    receive
    {'DOWN', CleanerRef, _, _, _} ->
        ok
    after 60000 ->
        etap:bail("Timeout waiting for cleaner to finish")
    end.


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
                {<<"view_1">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                    {<<"reduce">>, <<"_sum">>}
                ]}}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag("Configuring set view with partitions [0 .. 31] as active"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, (num_set_partitions() div 2) - 1),
        passive_partitions = [],
        use_replica_index = true
    },
    ok = couch_set_view:define_group(
        mapreduce_view, test_set_name(), ddoc_id(), Params).


update_documents(StartId, Count, ValueGenFun) ->
    etap:diag("Updating " ++ integer_to_list(Count) ++ " new documents"),
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


verify_btrees_1(ActiveParts, PassiveParts, ExpectedSeqs,
                ExpectedUnindexableSeqs, ValueGenFun) ->
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
        indexer = #mapreduce_view{
            btree = View1Btree
        }
    } = View1,
    AllSeqs = HeaderUpdateSeqs ++ UnindexableSeqs,
    ExpectedActiveBitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedPassiveBitmask = couch_set_view_util:build_bitmask(PassiveParts),
    ExpectedIndexedBitmask = couch_set_view_util:build_bitmask(
        [P || P <- (ActiveParts ++ PassiveParts), couch_util:get_value(P, AllSeqs, -1) > 0]),
    ExpectedKVCount = (num_docs() div num_set_partitions()) *
        length([P || P <- ActiveParts, couch_util:get_value(P, AllSeqs, -1) > 0]),
    ExpectedView1Reduction = lists:sum([
        ValueGenFun(I) || I <- lists:seq(0, num_docs() - 1),
            lists:member(I rem num_set_partitions(), ActiveParts),
            couch_util:get_value(I rem num_set_partitions(), AllSeqs, -1) > 0
    ]),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {ExpectedKVCount, ExpectedIndexedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {ExpectedKVCount, [ExpectedView1Reduction], ExpectedIndexedBitmask}},
        "View1 Btree has the right reduce value 1"),

    etap:is(HeaderUpdateSeqs, ExpectedSeqs, "Header has right update seqs list"),
    etap:is(UnindexableSeqs, ExpectedUnindexableSeqs, "Header has right unindexable seqs list"),
    etap:is(Abitmask, ExpectedActiveBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, ExpectedPassiveBitmask, "Header has right passive bitmask"),
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
            UserKey = DocId = doc_id(I),
            Value = [{View1#set_view.id_num, UserKey}],
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
    {ok, _, {_, View1BtreeFoldResult}} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, {NextId, I}) ->
            PartId = NextId rem num_set_partitions(),
            Key = DocId = doc_id(NextId),
            Value = ValueGenFun(NextId),
            ExpectedKv = {{Key, DocId}, {PartId, Value}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,

            HighestActivePartId = length(ActiveParts) - 1,
            case PartId =:= HighestActivePartId of
            true ->
                NumNonActiveParts = num_set_partitions() - HighestActivePartId,
                {ok, {NextId + NumNonActiveParts, I + 1}};
            false ->
                {ok, {NextId + 1, I + 1}}
            end
        end,
        {0, 0}, []),
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
        indexer = #mapreduce_view{
            btree = View1Btree
        }
    } = View1,
    ActiveParts = lists:seq(0, (num_set_partitions() div 2) - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedKVCount = (num_docs() div num_set_partitions()) * length(ActiveParts),
    ExpectedView1Reduction = lists:sum([
        ValueGenFun1(I) || I <- lists:seq(0, num_docs() - 1),
        orddict:is_key(I rem num_set_partitions(), ExpectedUnindexableSeqs),
        lists:member(I rem num_set_partitions(), ActiveParts)
    ]) + lists:sum([
        ValueGenFun2(I) || I <- lists:seq(0, num_docs() - 1),
        (not orddict:is_key(I rem num_set_partitions(), ExpectedUnindexableSeqs)),
        lists:member(I rem num_set_partitions(), ActiveParts)
    ]),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {ExpectedKVCount, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {ExpectedKVCount, [ExpectedView1Reduction], ExpectedBitmask}},
        "View1 Btree has the right reduce value 2"),

    etap:is(HeaderUpdateSeqs, ExpectedSeqs, "Header has right update seqs list"),
    etap:is(UnindexableSeqs, ExpectedUnindexableSeqs, "Header has right unindexable seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
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
            UserKey = DocId = doc_id(I),
            Value = [{View1#set_view.id_num, UserKey}],
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
    {ok, _, {_, View1BtreeFoldResult}} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, {NextId, I}) ->
            PartId = NextId rem num_set_partitions(),
            case orddict:is_key(PartId, ExpectedUnindexableSeqs) of
            true ->
                Value = ValueGenFun1(NextId);
            false ->
                Value = ValueGenFun2(NextId)
            end,
            Key = DocId = doc_id(NextId),
            ExpectedKv = {{Key, DocId}, {PartId, Value}},
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
    etap:is(View1BtreeFoldResult, ExpectedKVCount,
        "View1 Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),
    ok.


main_trigger_wait_for_initial_build() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, _, _} = gen_server:call(
        GroupPid, #set_view_group_req{stale = false, debug = true}, ?MAX_WAIT_TIME).


replica_trigger_wait_for_initial_build() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, ReplicaGroupPid} = gen_server:call(GroupPid, replica_pid, infinity),
    {ok, _, _} = gen_server:call(
        ReplicaGroupPid, #set_view_group_req{stale = false, debug = true}, ?MAX_WAIT_TIME).
