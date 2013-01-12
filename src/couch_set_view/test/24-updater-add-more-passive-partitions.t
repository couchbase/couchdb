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
-define(MAX_WAIT_TIME, 900 * 1000).

-include_lib("couch_set_view/include/couch_set_view.hrl").

test_set_name() -> <<"couch_test_set_index_updater_add_more_passive_partitions">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs_0() -> 78144.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(18),
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

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    create_set(),
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),

    ValueGenFun1 = fun(I) -> I end,
    update_documents(0, num_docs_0(), ValueGenFun1),

    NewPassiveParts = lists:seq(num_set_partitions() div 2, num_set_partitions() - 1),

    % Trigger index update
    {ok, UpdaterPid} = gen_server:call(GroupPid, {start_updater, [pause]}, infinity),
    Ref = erlang:monitor(process, UpdaterPid),

    lists:foreach(
        fun(PartId) ->
            ok = couch_set_view:set_partition_states(
                test_set_name(), ddoc_id(), [], [PartId], [])
        end,
        NewPassiveParts),

    etap:diag("Added more passive partitions while updater is running"),

    ActiveParts0 = lists:seq(1, num_set_partitions() - 1, 2),
    PassiveParts0 = lists:seq(0, num_set_partitions() - 1, 2),

    etap:diag("Changing the state of some partitions from passive to active " ++
                  "while the updater is running"),
    lists:foreach(
        fun(PartId) ->
            ok = couch_set_view:set_partition_states(
                test_set_name(), ddoc_id(), [PartId], [], [])
        end,
        ActiveParts0),

    PassiveParts = ordsets:union(PassiveParts0, lists:seq(1, num_set_partitions() - 1, 4)),
    ActiveParts = ordsets:subtract(ActiveParts0, PassiveParts),

    etap:diag("Changing the state of some partitions from active back to passive " ++
                  "while the updater is running"),
    lists:foreach(
        fun(PartId) ->
            ok = couch_set_view:set_partition_states(
                test_set_name(), ddoc_id(), [], [PartId], [])
        end,
        PassiveParts),

    UpdaterPid ! continue,
    etap:diag("Waiting for updater to finish"),
    receive
    {'DOWN', Ref, _, _, {updater_finished, _}} ->
        etap:diag("Updater finished");
    {'DOWN', Ref, _, _, Reason} ->
        etap:bail("Updater finished with unexpected reason: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for updater to finish")
    end,

    verify_btrees(ValueGenFun1, num_docs_0(), ActiveParts, PassiveParts),

    etap:diag("Shutting down group pid, and verifying last written header is good"),
    couch_util:shutdown_sync(GroupPid),

    verify_btrees(ValueGenFun1, num_docs_0(), ActiveParts, PassiveParts),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


get_group_snapshot() ->
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    {ok, Group} = gen_server:call(GroupPid, request_group, infinity),
    Group.


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
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, meta.id); }">>},
                {<<"reduce">>, <<"_count">>}
            ]}}
        ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag("Configuring set view with partitions [0 .. 31] as passive"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = [],
        passive_partitions = lists:seq(0, (num_set_partitions() div 2) - 1),
        use_replica_index = false
    },
    ok = couch_set_view:define_group(test_set_name(), ddoc_id(), Params).


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


verify_btrees(ValueGenFun, NumDocs, ActiveParts, PassiveParts) ->
    Group = get_group_snapshot(),
    #set_view_group{
        id_btree = IdBtree,
        views = [View1, View2],
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
    #set_view{
        btree = View2Btree
    } = View2,
    AllParts = ordsets:union(ActiveParts, PassiveParts),
    ExpectedActiveBitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedPassiveBitmask = couch_set_view_util:build_bitmask(PassiveParts),
    ExpectedIndexedBitmask = ExpectedActiveBitmask bor ExpectedPassiveBitmask,
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), AllParts),
    ExpectedKVCount = length([I || I <- lists:seq(0, NumDocs - 1),
        ordsets:is_element((I rem num_set_partitions()), AllParts)]),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {ExpectedKVCount, ExpectedIndexedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {ExpectedKVCount, [ExpectedKVCount], ExpectedIndexedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedActiveBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, ExpectedPassiveBitmask, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    MaxPerPart = NumDocs div num_set_partitions(),
    {ok, _, {_, _, _, IdBtreeFoldResult}} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, {Parts, I0, C0, It}) ->
            case C0 >= MaxPerPart of
            true ->
                [_ | RestParts] = Parts,
                [P | _] = RestParts,
                I = P,
                C = 1;
            false ->
                RestParts = Parts,
                [P | _] = RestParts,
                I = I0,
                C = C0 + 1
            end,
            true = (P < num_set_partitions()),
            DocId = doc_id(I),
            Value = [{View1#set_view.id_num, DocId}, {View2#set_view.id_num, DocId}],
            ExpectedKv = {<<P:16, DocId/binary>>, {P, Value}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(It))
            end,
            {ok, {RestParts, I + num_set_partitions(), C, It + 1}}
        end,
        {AllParts, hd(AllParts), 0, 0}, []),
    etap:is(IdBtreeFoldResult, ExpectedKVCount,
        "Id Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, {_, View1BtreeFoldResult}} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, {I, Count}) ->
            PartId = I rem num_set_partitions(),
            DocId = doc_id(I),
            ExpectedKv = {{DocId, DocId}, {PartId, ValueGenFun(I)}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(Count + 1))
            end,
            {ok, {next_i(I, AllParts), Count + 1}}
        end,
        {hd(AllParts), 0}, []),
    etap:is(View1BtreeFoldResult, ExpectedKVCount,
        "View1 Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),

    etap:diag("Verifying the View2 Btree"),
    {ok, _, {_, View2BtreeFoldResult}} = couch_set_view_test_util:fold_view_btree(
        Group,
        View2Btree,
        fun(Kv, _, {I, Count}) ->
            PartId = I rem num_set_partitions(),
            DocId = doc_id(I),
            ExpectedKv = {{DocId, DocId}, {PartId, DocId}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View2 Btree has an unexpected KV at iteration " ++ integer_to_list(Count + 1))
            end,
            {ok, {next_i(I, AllParts), Count + 1}}
        end,
        {hd(AllParts), 0}, []),
    etap:is(View2BtreeFoldResult, ExpectedKVCount,
        "View2 Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),
    ok.


next_i(I, Parts) ->
    case ordsets:is_element((I + 1) rem num_set_partitions(), Parts) of
    true ->
        I + 1;
    false ->
        next_i(I + 1, Parts)
    end.
