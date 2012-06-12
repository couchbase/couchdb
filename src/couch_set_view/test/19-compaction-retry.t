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

test_set_name() -> <<"couch_test_set_index_compaction_retry">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs_0() -> 25856.


main(_) ->
    test_util:init_code_path(),

    etap:plan(36),
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

    create_set(),
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),

    ValueGenFun1 = fun(I) -> I end,
    update_documents(0, num_docs_0(), ValueGenFun1),

    % build index
    _ = get_group_snapshot(),

    compact_add_new_docs(128, ValueGenFun1),

    verify_btrees(ValueGenFun1, num_docs_0() + 128),

    ValueGenFun2 = fun(I) -> I * 3 end,
    compact_update_docs(num_docs_0() + 128, ValueGenFun2),

    verify_btrees(ValueGenFun2, num_docs_0() + 128),

    % delete last N docs
    compact_delete_docs(num_docs_0() + 128, 64),

    verify_btrees(ValueGenFun2, num_docs_0() + 64),

    etap:diag("Testing 2 compaction retries work"),
    ValueGenFun3 = fun(I) -> I * 5 end,
    ValueGenFun4 = fun(I) -> I * 11 end,
    compact_2_retries_update_docs(num_docs_0() + 64, ValueGenFun3, ValueGenFun4),

    verify_btrees(ValueGenFun4, num_docs_0() + 64),

    couch_util:shutdown_sync(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


compact_add_new_docs(NewDocCount, ValueGenFun) ->
    {ok, Pid} = couch_set_view_compactor:start_compact(test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, Pid),
    Pid ! pause,
    receive
    {'DOWN', Ref, process, Pid, _Reason} ->
        etap:bail("Compaction finished before we could add new documents and trigger index update")
    after 0 ->
        ok
    end,
    update_documents(num_docs_0(), NewDocCount, ValueGenFun),
    % trigger index update
    _ = get_group_snapshot(),
    Pid ! unpause,
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Compactor died with reason: " ++ lists:flatten(io_lib:format("~p", [Reason])))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for compaction to finish")
    end,
    ok.


compact_update_docs(DocCount, ValueGenFun) ->
    {ok, Pid} = couch_set_view_compactor:start_compact(test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, Pid),
    Pid ! pause,
    receive
    {'DOWN', Ref, process, Pid, _Reason} ->
        etap:bail("Compaction finished before we could update documents and trigger index update")
    after 0 ->
        ok
    end,
    update_documents(0, DocCount, ValueGenFun),
    % trigger index update
    _ = get_group_snapshot(),
    Pid ! unpause,
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Compactor died with reason: " ++ lists:flatten(io_lib:format("~p", [Reason])))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for compaction to finish")
    end,
    ok.


compact_delete_docs(TotalDocCount, ToDeleteCount) ->
    {ok, Pid} = couch_set_view_compactor:start_compact(test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, Pid),
    Pid ! pause,
    receive
    {'DOWN', Ref, process, Pid, _Reason} ->
        etap:bail("Compaction finished before we could delete documents and trigger index update")
    after 0 ->
        ok
    end,
    delete_docs(TotalDocCount - ToDeleteCount, ToDeleteCount),
    % trigger index update
    _ = get_group_snapshot(),
    Pid ! unpause,
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Compactor died with reason: " ++ lists:flatten(io_lib:format("~p", [Reason])))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for compaction to finish")
    end,
    ok.


compact_2_retries_update_docs(DocCount, ValueGenFun1, ValueGenFun2) ->
    {ok, Pid} = couch_set_view_compactor:start_compact(test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, Pid),
    Pid ! pause,
    receive
    {'DOWN', Ref, process, Pid, _Reason} ->
        etap:bail("Compaction finished before we could update documents and trigger index update")
    after 0 ->
        ok
    end,
    update_documents(0, DocCount, ValueGenFun1),
    % trigger index update
    _ = get_group_snapshot(),
    Pid ! unpause,
    Pid ! pause,
    receive
    {'DOWN', Ref, process, Pid, _Reason2} ->
        etap:bail("Compaction retry finished before we could update documents and trigger index update")
    after 0 ->
        ok
    end,
    update_documents(0, DocCount, ValueGenFun2),
    % trigger index update
    _ = get_group_snapshot(),
    Pid ! unpause,
    receive
    {'DOWN', Ref, process, Pid, normal} ->
        ok;
    {'DOWN', Ref, process, Pid, Reason} ->
        etap:bail("Compactor died with reason: " ++ lists:flatten(io_lib:format("~p", [Reason])))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for compaction to finish")
    end,
    ok.


get_group_snapshot() ->
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    {ok, Group, 0} = gen_server:call(
        GroupPid, #set_view_group_req{stale = false, debug = true}, infinity),
    Group.


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
                {<<"reduce">>, <<"_count">>}
            ]}},
            {<<"view_2">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, doc._id); }">>},
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


update_documents(StartId, Count, ValueGenFun) ->
    etap:diag("Updating " ++ integer_to_list(Count) ++ " documents"),
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


doc_id(I) ->
    iolist_to_binary(io_lib:format("doc_~8..0b", [I])).


verify_btrees(ValueGenFun, NumDocs) ->
    Group = get_group_snapshot(),
    #set_view_group{
        id_btree = IdBtree,
        views = [View2, View1],
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
    ActiveParts = lists:seq(0, num_set_partitions() - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(ActiveParts),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), ActiveParts),
    ExpectedKVCount = NumDocs,

    etap:is(
        couch_btree:full_reduce(IdBtree),
        {ok, {ExpectedKVCount, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_btree:full_reduce(View1Btree),
        {ok, {ExpectedKVCount, [ExpectedKVCount], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_btree:fold(
        IdBtree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            DocId = doc_id(I),
            Value = [{View2#set_view.id_num, DocId}, {View1#set_view.id_num, DocId}],
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
            DocId = doc_id(I),
            ExpectedKv = {{DocId, DocId}, {PartId, {json, ?JSON_ENCODE(ValueGenFun(I))}}},
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

    etap:diag("Verifying the View2 Btree"),
    {ok, _, View2BtreeFoldResult} = couch_btree:fold(
        View2Btree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            DocId = doc_id(I),
            ExpectedKv = {{DocId, DocId}, {PartId, {json, ?JSON_ENCODE(DocId)}}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View2 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            {ok, I + 1}
        end,
        0, []),
    etap:is(View2BtreeFoldResult, ExpectedKVCount,
        "View2 Btree has " ++ integer_to_list(ExpectedKVCount) ++ " entries"),
    ok.
