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

test_set_name() -> <<"couch_test_set_pending_transition">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 20288.  % keep it a multiple of num_set_partitions()

-record(user_ctx, {
    name = null,
    roles = [],
    handler
}).

admin_user_ctx() ->
    {user_ctx, #user_ctx{roles = [<<"_admin">>]}}.


main(_) ->
    test_util:init_code_path(),

    etap:plan(47),
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
    ValueGenFun1 = fun(I) -> I end,
    update_documents(0, num_docs(), ValueGenFun1),

    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    ok = gen_server:call(GroupPid, {set_auto_cleanup, false}, infinity),

    % build index
    _ = get_group_snapshot(),

    verify_btrees_1(ValueGenFun1),

    etap:diag("Marking all odd partitions for cleanup"),
    ok = couch_set_view:set_partition_states(
        test_set_name(), ddoc_id(), [], [],
        lists:seq(1, num_set_partitions() - 1, 2)),

    verify_btrees_2(ValueGenFun1),

    etap:diag("Marking partition 1 as active and all even partitions, "
              "except partition 0, for cleanup"),
    ok = couch_set_view:set_partition_states(
        test_set_name(), ddoc_id(),
        [1], [], lists:seq(2, num_set_partitions() - 1, 2)),

    verify_btrees_3(ValueGenFun1),

    lists:foreach(fun(PartId) ->
        etap:diag("Deleting partition " ++ integer_to_list(PartId) ++
            ", currently marked for cleanup in the pending transition"),
        ok = couch_set_view_test_util:delete_set_db(test_set_name(), PartId)
    end, lists:seq(2, num_set_partitions() - 1, 2)),
    ok = timer:sleep(5000),
    etap:is(is_process_alive(GroupPid), true, "Group process didn't die"),

    % Recreate database 1, populate new contents, verify that neither old
    % contents nor new contents are in the index after a stale=false request.
    etap:diag("Recreating partition 1 database, currenly marked as active in the"
              " pending transition - shouldn't cause the group process to die"),
    recreate_db(1),
    ok = timer:sleep(6000),
    etap:is(is_process_alive(GroupPid), true, "Group process didn't die"),

    {ok, Db0} = open_db(0),
    Doc = couch_doc:from_json_obj({[
        {<<"meta">>, {[{<<"id">>, doc_id(9000010)}]}},
        {<<"json">>, {[
            {<<"value">>, 9000010}
        ]}}
    ]}),
    ok = couch_db:update_doc(Db0, Doc, []),
    ok = couch_db:close(Db0),

    % update index - updater will trigger a cleanup and apply the pending transition
    _ = get_group_snapshot(),

    verify_btrees_4(ValueGenFun1),
    compact_view_group(),
    verify_btrees_4(ValueGenFun1),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions() - 1),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


recreate_db(PartId) ->
    DbName = iolist_to_binary([test_set_name(), $/, integer_to_list(PartId)]),
    ok = couch_server:delete(DbName, [admin_user_ctx()]),
    ok = timer:sleep(300),
    {ok, Db} = couch_db:create(DbName, [admin_user_ctx()]),
    Doc = couch_doc:from_json_obj({[
        {<<"meta">>, {[{<<"id">>, doc_id(9000009)}]}},
        {<<"json">>, {[
            {<<"value">>, 9000009}
        ]}}
    ]}),
    ok = couch_db:update_doc(Db, Doc, []),
    ok = couch_db:close(Db).


open_db(PartId) ->
    DbName = iolist_to_binary([test_set_name(), $/, integer_to_list(PartId)]),
    {ok, _} = couch_db:open_int(DbName, []).


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


verify_btrees_1(ValueGenFun) ->
    Group = get_group_snapshot(),
    etap:diag("Verifying btrees"),
    #set_view_group{
        id_btree = IdBtree,
        views = [View1],
        index_header = #set_view_index_header{
            pending_transition = PendingTrans,
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = Group,
    #set_view{
        btree = View1Btree
    } = View1,
    ActiveParts = lists:seq(0, num_set_partitions() - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedABitmask = couch_set_view_util:build_bitmask(ActiveParts),
    DbSeqs = couch_set_view_test_util:get_db_seqs(
        test_set_name(), lists:seq(0, num_set_partitions() - 1)),
    ExpectedKVCount = num_docs(),
    ExpectedBtreeViewReduction = num_docs(),

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
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),
    etap:is(PendingTrans, nil, "Header has nil pending transition"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            DocId = doc_id(I),
            Value = [{View1#set_view.id_num, DocId}],
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


verify_btrees_2(ValueGenFun) ->
    Group = get_group_snapshot(),
    etap:diag("Verifying btrees"),
    #set_view_group{
        id_btree = IdBtree,
        views = [View1],
        index_header = #set_view_index_header{
            pending_transition = PendingTrans,
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = Group,
    #set_view{
        btree = View1Btree
    } = View1,
    ActiveParts = lists:seq(0, num_set_partitions() - 1, 2),
    CleanupParts = lists:seq(1, num_set_partitions() - 1, 2),
    ExpectedBitmask = couch_set_view_util:build_bitmask(lists:seq(0, num_set_partitions() - 1)),
    ExpectedABitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedCBitmask = couch_set_view_util:build_bitmask(CleanupParts),
    ExpectedDbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), ActiveParts),
    ExpectedKVCount = num_docs(),
    ExpectedBtreeViewReduction = num_docs(),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {ExpectedKVCount, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {ExpectedKVCount, [ExpectedBtreeViewReduction], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, ExpectedDbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedABitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, ExpectedCBitmask, "Header has right cleanup bitmask"),
    etap:is(PendingTrans, nil, "Header has nil pending transition"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            DocId = doc_id(I),
            Value = [{View1#set_view.id_num, DocId}],
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


verify_btrees_3(ValueGenFun) ->
    Group = get_group_snapshot(),
    etap:diag("Verifying btrees"),
    #set_view_group{
        id_btree = IdBtree,
        views = [View1],
        index_header = #set_view_index_header{
            pending_transition = PendingTrans,
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = Group,
    #set_view{
        btree = View1Btree
    } = View1,
    ActiveParts = [0],
    CleanupParts = lists:seq(1, num_set_partitions() - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(lists:seq(0, num_set_partitions() - 1)),
    ExpectedABitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedCBitmask = couch_set_view_util:build_bitmask(CleanupParts),
    ExpectedDbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), ActiveParts),
    ExpectedKVCount = num_docs(),
    ExpectedBtreeViewReduction = num_docs(),
    ExpectedPendingTrans = #set_view_transition{
        active = [1],
        passive = []
    },

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {ExpectedKVCount, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {ExpectedKVCount, [ExpectedBtreeViewReduction], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, ExpectedDbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedABitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, ExpectedCBitmask, "Header has right cleanup bitmask"),
    etap:is(PendingTrans, ExpectedPendingTrans, "Header has expected pending transition"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            DocId = doc_id(I),
            Value = [{View1#set_view.id_num, DocId}],
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


verify_btrees_4(ValueGenFun) ->
    Group = get_group_snapshot(),
    etap:diag("Verifying btrees"),
    #set_view_group{
        id_btree = IdBtree,
        views = [View1],
        index_header = #set_view_index_header{
            pending_transition = PendingTrans,
            seqs = HeaderUpdateSeqs,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            cbitmask = Cbitmask
        }
    } = Group,
    #set_view{
        btree = View1Btree
    } = View1,
    ActiveParts = [0],
    ExpectedBitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedABitmask = couch_set_view_util:build_bitmask(ActiveParts),
    ExpectedDbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), ActiveParts),
    ExpectedKVCount = (num_docs() div num_set_partitions()) + 1,
    ExpectedBtreeViewReduction = (num_docs() div num_set_partitions()) + 1,

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {ExpectedKVCount, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {ExpectedKVCount, [ExpectedBtreeViewReduction], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, ExpectedDbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedABitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),
    etap:is(PendingTrans, nil, "Header has nil pending transition"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, {_, IdBtreeFoldResult}} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, {I, Count}) ->
            case Count == (ExpectedKVCount - 1) of
            true ->
                DocId = doc_id(9000010),
                PartId = 0;
            false ->
                DocId = doc_id(I),
                PartId = I rem num_set_partitions()
            end,
            Value = [{View1#set_view.id_num, DocId}],
            ExpectedKv = {DocId, {PartId, Value}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(Count))
            end,
            {ok, {I + 64, Count + 1}}
        end,
        {0, 0}, []),
    etap:is(IdBtreeFoldResult, ExpectedKVCount,
        "Id Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, {_, View1BtreeFoldResult}} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, {I, Count}) ->
            case Count == (ExpectedKVCount - 1) of
            true ->
                DocId = doc_id(9000010),
                PartId = 0,
                Value = 9000010;
            false ->
                DocId = doc_id(I),
                PartId = I rem num_set_partitions(),
                Value = ValueGenFun(I)
            end,
            ExpectedKv = {{DocId, DocId}, {PartId, Value}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(Count))
            end,
            {ok, {I + 64, Count + 1}}
        end,
        {0, 0}, []),
    etap:is(View1BtreeFoldResult, ExpectedKVCount,
        "View1 Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),
    ok.


get_group_snapshot() ->
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    {ok, Group, 0} = gen_server:call(
        GroupPid, #set_view_group_req{stale = false, debug = true}, infinity),
    Group.


compact_view_group() ->
    {ok, CompactPid} = couch_set_view_compactor:start_compact(test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, CompactPid),
    etap:diag("Waiting for main view group compaction to finish"),
    receive
    {'DOWN', Ref, process, CompactPid, normal} ->
        ok;
    {'DOWN', Ref, process, CompactPid, noproc} ->
        ok;
    {'DOWN', Ref, process, CompactPid, Reason} ->
        etap:bail("Failure compacting main view group: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for main view group compaction to finish")
    end.
