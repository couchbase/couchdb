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

-define(MAX_WAIT_TIME, 900 * 1000).

-define(etap_match(Got, Expected, Desc),
        etap:fun_is(fun(XXXXXX) ->
            case XXXXXX of Expected -> true; _ -> false end
        end, Got, Desc)).

-record(doc, {
    id = <<>>,
    rev = {0, <<>>},
    body = <<"{}">>,
    content_meta = 0,
    deleted = false,
    meta = []
}).

test_set_name() -> <<"couch_test_set_index_database_purges">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
initial_num_docs() -> 17600.  % must be multiple of num_set_partitions()


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

    ValueGenFun1 = fun(I) -> I end,
    create_set(),
    update_documents(0, initial_num_docs(), false, ValueGenFun1),
    trigger_update(),

    verify_btrees(ValueGenFun1, initial_num_docs()),

    etap:diag("Deleting the last " ++ integer_to_list(num_set_partitions()) ++
		  " documents"),
    update_documents(initial_num_docs() - num_set_partitions(),
		     num_set_partitions(),
		     true,
		     ValueGenFun1),

    etap:diag("Purging the last document"),
    LastPurgedId = doc_id(initial_num_docs() - 1),
    {ok, FirstPurgeDb} = couch_set_view_test_util:open_set_db(
        test_set_name(), num_set_partitions() - 1),
    {ok, Doc} = couch_db:open_doc(FirstPurgeDb, LastPurgedId, [deleted]),
    etap:is(Doc#doc.deleted, true, "Last document is flagged as deleted"),
    ok = couch_db:purge_docs(FirstPurgeDb, [LastPurgedId]),
    ok = couch_db:close(FirstPurgeDb),

    {ok, FirstPurgeDb2} = couch_set_view_test_util:open_set_db(
        test_set_name(), num_set_partitions() - 1),
    DocLookup = couch_db:open_doc(FirstPurgeDb2, LastPurgedId, [deleted]),
    etap:is(DocLookup, {not_found, missing}, "Document was purged"),
    ok = couch_db:close(FirstPurgeDb2),

    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, UpdaterPid1} = gen_server:call(GroupPid, {start_updater, []}, infinity),
    Ref1 = erlang:monitor(process, UpdaterPid1),
    etap:diag("Waiting for updater to exit with purge reason"),
    receive
    {'DOWN', Ref1, _, _, {updater_error, purge}} ->
        etap:diag("Updater exited with purge reason");
    {'DOWN', Ref1, _, _, Reason1} ->
        etap:bail("Updater finished with unexpected reason: " ++ couch_util:to_list(Reason1))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for updater to finish")
    end,

    {ok, UpdaterPid2} = gen_server:call(GroupPid, {start_updater, []}, infinity),
    case UpdaterPid2 of
    nil ->
	% already finished
        ok;
    _ ->
        Ref2 = erlang:monitor(process, UpdaterPid2),
        etap:diag("Waiting for updater to exit normally"),
        receive
        {'DOWN', Ref2, _, _, {updater_finished, _}} ->
            etap:diag("Updater finished");
        {'DOWN', Ref2, _, _, noproc} ->
            etap:diag("Updater finished");
        {'DOWN', Ref2, _, _, Reason2} ->
            etap:bail("Updater finished with unexpected reason: " ++
                 couch_util:to_list(Reason2))
        after ?MAX_WAIT_TIME ->
            etap:bail("Timeout waiting for updater to finish")
        end
    end,

    verify_btrees(ValueGenFun1, initial_num_docs() - num_set_partitions()),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


create_set() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view:cleanup_index_files(mapreduce_view, test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                {<<"reduce">>, <<"_count">>}
            ]}}
        ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag("Configuring set view with partitions [0 .. 31] as active and"
	      " [32 .. 63] as passive"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, 31),
        passive_partitions = lists:seq(32, 63),
        use_replica_index = false
    },
    ok = couch_set_view:define_group(
        mapreduce_view, test_set_name(), ddoc_id(), Params).


update_documents(StartId, Count, Deleted, ValueGenFun) ->
    etap:diag("Updating " ++ integer_to_list(Count) ++ " documents"),
    DocList0 = lists:map(
        fun(I) ->
            {I rem num_set_partitions(), {[
                {<<"meta">>, {[
                    {<<"id">>, doc_id(I)}, {<<"deleted">>, Deleted}
                ]}},
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


trigger_update() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, UpdaterPid} = gen_server:call(GroupPid, {start_updater, []}, infinity),
    Ref = erlang:monitor(process, UpdaterPid),
    etap:diag("Waiting for updater to finish"),
    receive
    {'DOWN', Ref, _, _, {updater_finished, _}} ->
        etap:diag("Updater finished");
    {'DOWN', Ref, _, _, Reason} ->
        etap:bail("Updater finished with unexpected reason: " ++ couch_util:to_list(Reason))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for updater to finish")
    end.


verify_btrees(ValueGenFun, NumDocs) ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, Group} = gen_server:call(GroupPid, request_group, infinity),
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
        id_num = 0,
        indexer = #mapreduce_view{
            btree = View1Btree
        }
    } = View1,

    etap:diag("Verifying view group btrees"),
    ExpectedActiveBitmask = couch_set_view_util:build_bitmask(lists:seq(0, 31)),
    ExpectedPassiveBitmask = couch_set_view_util:build_bitmask(lists:seq(32, 63)),
    ExpectedBitmask = ExpectedActiveBitmask bor ExpectedPassiveBitmask,
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), lists:seq(0, 63)),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {NumDocs, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {NumDocs, [NumDocs], ExpectedBitmask}},
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
            ExpectedKv = {
                <<P:16, (doc_id(I))/binary>>,
                {P, [{0, doc_id(I)}]}
            },
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(It))
            end,
            {ok, {P, I + num_set_partitions(), C, It + 1}}
        end,
        {0, 0, 0, 0}, []),
    etap:is(IdBtreeFoldResult, NumDocs,
        "Id Btree has " ++ integer_to_list(NumDocs) ++ " entries"),
    etap:diag("Verifying the View1 Btree"),
    {ok, _, View1BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, Acc) ->
            PartId = Acc rem 64,
            ExpectedKv = {{doc_id(Acc), doc_id(Acc)}, {PartId, ValueGenFun(Acc)}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(Acc))
            end,
            {ok, Acc + 1}
        end,
        0, []),
    etap:is(View1BtreeFoldResult, NumDocs,
        "View1 Btree has " ++ integer_to_list(NumDocs) ++ " entries").
