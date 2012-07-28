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

test_set_name() -> <<"couch_test_set_index_dup_keys">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 15616.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(163),
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

    test_same_key_by_same_doc_multiple_times(),
    test_same_key_by_different_docs_multiple_times(),

    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


test_same_key_by_same_doc_multiple_times() ->
    same_key_by_same_doc_multiple_times_create_set(),
    add_documents(0, num_docs()),

    Group1 = get_group_snapshot(),
    same_key_by_same_doc_multiple_times_verify_btrees_1(Group1),

    compact_view_group(),

    Group2 = get_group_snapshot(),
    same_key_by_same_doc_multiple_times_verify_btrees_1(Group2),

    update_documents(0, num_docs(), false),

    Group3 = get_group_snapshot(),
    same_key_by_same_doc_multiple_times_verify_btrees_2(Group3),

    compact_view_group(),

    Group4 = get_group_snapshot(),
    same_key_by_same_doc_multiple_times_verify_btrees_2(Group4),

    update_documents(0, num_docs(), true),

    Group5 = get_group_snapshot(),
    same_key_by_same_doc_multiple_times_verify_btrees_1(Group5),

    compact_view_group(),

    Group6 = get_group_snapshot(),
    same_key_by_same_doc_multiple_times_verify_btrees_1(Group6),

    etap:diag("Marking partitions lists:seq(1, 63, 2) for cleanup"),
    ok = lists:foreach(
        fun(I) ->
            ok = couch_set_view:set_partition_states(test_set_name(), ddoc_id(), [], [], [I])
        end,
        lists:reverse(lists:seq(1, 63, 2))),

    wait_for_cleanup(),

    Group7 = get_group_snapshot(),
    same_key_by_same_doc_multiple_times_verify_btrees_3(Group7),

    compact_view_group(),

    Group8 = get_group_snapshot(),
    same_key_by_same_doc_multiple_times_verify_btrees_3(Group8),

    delete_documents(0, num_docs()),

    Group9 = get_group_snapshot(),
    same_key_by_same_doc_multiple_times_verify_btrees_4(Group9),

    compact_view_group(),

    Group10 = get_group_snapshot(),
    same_key_by_same_doc_multiple_times_verify_btrees_4(Group10),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok.


test_same_key_by_different_docs_multiple_times() ->
    test_same_key_by_different_docs_multiple_times_create_set(),
    add_documents(0, 64),

    Group1 = get_group_snapshot(),
    test_same_key_by_different_docs_multiple_times_verify_btrees_1(Group1),

    compact_view_group(),

    Group2 = get_group_snapshot(),
    test_same_key_by_different_docs_multiple_times_verify_btrees_1(Group2),

    update_documents(0, 64, false),

    Group3 = get_group_snapshot(),
    test_same_key_by_different_docs_multiple_times_verify_btrees_2(Group3),

    compact_view_group(),

    Group4 = get_group_snapshot(),
    test_same_key_by_different_docs_multiple_times_verify_btrees_2(Group4),

    update_documents(0, 64, true),

    Group5 = get_group_snapshot(),
    test_same_key_by_different_docs_multiple_times_verify_btrees_1(Group5),

    compact_view_group(),

    Group6 = get_group_snapshot(),
    test_same_key_by_different_docs_multiple_times_verify_btrees_1(Group6),

    delete_documents(0, 64),

    Group7 = get_group_snapshot(),
    test_same_key_by_different_docs_multiple_times_verify_btrees_3(Group7),

    compact_view_group(),

    Group8 = get_group_snapshot(),
    test_same_key_by_different_docs_multiple_times_verify_btrees_3(Group8),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok.


get_group_snapshot() ->
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    {ok, Group, 0} = gen_server:call(
        GroupPid, #set_view_group_req{stale = false, debug = true}, infinity),
    Group.


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


wait_for_cleanup() ->
    etap:diag("Waiting for main index cleanup to finish"),
    Pid = spawn(fun() ->
        wait_for_cleanup_loop(get_group_info())
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


same_key_by_same_doc_multiple_times_create_set() ->
    DDoc = {[
        {<<"_id">>, ddoc_id()},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"view_1">>, {[
                {<<"map">>, <<"function(doc) { "
                    "if (doc.emit2) { "
                        "emit(doc._id, doc.value);"
                    "} else {"
                        "emit(doc._id, null);"
                    "}"
                    "if (doc.emit2) { emit(doc._id, doc.value + 1); }"
                "}">>},
                {<<"reduce">>, <<"_count">>}
            ]}}
        ]}}
    ]},
    create_set(DDoc).


test_same_key_by_different_docs_multiple_times_create_set() ->
    DDoc = {[
        {<<"_id">>, ddoc_id()},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"view_1">>, {[
                {<<"map">>, <<"function(doc) { "
                    "emit(doc.value, doc.value * 2);"
                    "if (doc.emit2) { "
                        "emit(doc.value + 1, doc.value * 3);"
                    "}"
                "}">>},
                {<<"reduce">>, <<"_sum">>}
            ]}}
        ]}}
    ]},
    create_set(DDoc).


create_set(DDoc) ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view:cleanup_index_files(test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
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
                {<<"value">>, I},
                {<<"emit2">>, true}
            ]}}
        end,
        lists:seq(StartId, StartId + Count - 1)),
    DocList = [Doc || {_, Doc} <- lists:keysort(1, DocList0)],
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


update_documents(StartId, NumDocs, Emit2) ->
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
                {<<"value">>, I},
                {<<"emit2">>, Emit2}
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
            etap:diag("Updating " ++ integer_to_list(length(DocList)) ++
                " documents in partition " ++ integer_to_list(I)),
            ok = couch_db:update_docs(Db, DocList, [sort_docs]),
            Acc
        end,
        [], Docs),
    ok = lists:foreach(fun({_, Db}) -> ok = couch_db:close(Db) end, dict:to_list(Dbs)).


delete_documents(StartId, NumDocs) ->
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


get_view(_ViewName, []) ->
    undefined;
get_view(ViewName, [#set_view{reduce_funs = RedFuns} = View | Rest]) ->
    case couch_util:get_value(ViewName, RedFuns) of
    undefined ->
        get_view(ViewName, Rest);
    _ ->
        View
    end.


same_key_by_same_doc_multiple_times_verify_btrees_1(Group) ->
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
    etap:is(1, length(Views), "1 view btree in the group"),
    View1 = get_view(<<"view_1">>, Views),
    #set_view{
        btree = View1Btree
    } = View1,
    PartList = lists:seq(0, num_set_partitions() - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(PartList),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), PartList),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {num_docs(), ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {num_docs() * 2, [num_docs() * 2], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            Value = [{View1#set_view.id_num, doc_id(I)}],
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
            PartId = I rem num_set_partitions(),
            ExpectedDups = [I + 1, I],
            ExpectedKv = {
                {doc_id(I), doc_id(I)},
                {PartId, {dups, lists:sort(ExpectedDups)}}
            },
            {OrigKey, {OrigPartId, {dups, OrigDups}}} = Kv,
            case ExpectedKv =:= {OrigKey, {OrigPartId, {dups, lists:sort(OrigDups)}}} of
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
    ok.


same_key_by_same_doc_multiple_times_verify_btrees_2(Group) ->
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
    etap:is(1, length(Views), "1 view btree in the group"),
    View1 = get_view(<<"view_1">>, Views),
    #set_view{
        btree = View1Btree
    } = View1,
    PartList = lists:seq(0, num_set_partitions() - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(PartList),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), PartList),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {num_docs(), ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {num_docs(), [num_docs()], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            Value = [{View1#set_view.id_num, doc_id(I)}],
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
            PartId = I rem num_set_partitions(),
            ExpectedKv = {{doc_id(I), doc_id(I)}, {PartId, null}},
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
    ok.


same_key_by_same_doc_multiple_times_verify_btrees_3(Group) ->
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
    etap:is(1, length(Views), "1 view btree in the group"),
    View1 = get_view(<<"view_1">>, Views),
    #set_view{
        btree = View1Btree
    } = View1,
    PartList = lists:seq(0, num_set_partitions() - 1, 2),
    ExpectedBitmask = couch_set_view_util:build_bitmask(PartList),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), PartList),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {num_docs() div 2, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {num_docs(), [num_docs()], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, {_, IdBtreeFoldResult}} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, {A, I}) ->
            PartId = I rem num_set_partitions(),
            Value = [{View1#set_view.id_num, doc_id(I)}],
            ExpectedKv = {doc_id(I), {PartId, Value}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("Id Btree has an unexpected KV at iteration " ++ integer_to_list(A))
            end,
            {ok, {A + 1, I + 2}}
        end,
        {0, 0}, []),
    etap:is(IdBtreeFoldResult, num_docs(),
        "Id Btree has " ++ integer_to_list(num_docs()) ++ " entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, View1BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            ExpectedKv = {
                {doc_id(I), doc_id(I)},
                {PartId, {dups, [I, I + 1]}}
            },
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            {ok, I + 2}
        end,
        0, []),
    etap:is(View1BtreeFoldResult, num_docs(),
        "View1 Btree has " ++ integer_to_list(num_docs()) ++ " entries"),
    ok.


same_key_by_same_doc_multiple_times_verify_btrees_4(Group) ->
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
    etap:is(1, length(Views), "1 view btree in the group"),
    View1 = get_view(<<"view_1">>, Views),
    #set_view{
        btree = View1Btree
    } = View1,
    PartList = lists:seq(0, num_set_partitions() - 1, 2),
    ExpectedBitmask = couch_set_view_util:build_bitmask(PartList),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), PartList),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {0, 0}},
        "Id Btree is empty"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {0, [0], 0}},
        "View1 Btree is empty"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(_Kv, _, I) ->
            {ok, I + 1}
        end,
        0, []),
    etap:is(IdBtreeFoldResult, 0, "Id Btree has 0 entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, View1BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(_Kv, _, I) ->
            {ok, I + 1}
        end,
        0, []),
    etap:is(View1BtreeFoldResult, 0, "View1 Btree has 0 entries"),
    ok.


test_same_key_by_different_docs_multiple_times_verify_btrees_1(Group) ->
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
    etap:is(1, length(Views), "1 view btree in the group"),
    View1 = get_view(<<"view_1">>, Views),
    #set_view{
        btree = View1Btree
    } = View1,
    PartList = lists:seq(0, num_set_partitions() - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(PartList),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), PartList),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {64, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    ExpectedViewReduction = lists:sum([I * 2 + I * 3 || I <- lists:seq(0, 63)]),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {64 * 2, [ExpectedViewReduction], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            Value = [{View1#set_view.id_num, I}, {View1#set_view.id_num, I + 1}],
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
    etap:is(IdBtreeFoldResult, 64, "Id Btree has 64 entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, {_, _, View1BtreeFoldResult}} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, {DocIdBase, Key, I}) ->
            PartId = DocIdBase rem num_set_partitions(),
            DocId = doc_id(DocIdBase),
            ExpectedKv = case (I + 1) rem 2 of
            0 ->
                {{Key, DocId}, {PartId, DocIdBase * 3}};
            1 ->
                {{Key, DocId}, {PartId, DocIdBase * 2}}
            end,
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            NextDocIdBase = case (I + 1) rem 2 of
            0 ->
                DocIdBase + 1;
            1 ->
                DocIdBase
            end,
            NextKey = case I of
            0 ->
                1;
            _ ->
                case I rem 2 of
                0 ->
                    Key + 1;
                1 ->
                    Key
                end
            end,
            {ok, {NextDocIdBase, NextKey, I + 1}}
        end,
        {0, 0, 0}, []),
    etap:is(View1BtreeFoldResult, 128, "View1 Btree has 128 entries"),
    ok.


test_same_key_by_different_docs_multiple_times_verify_btrees_2(Group) ->
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
    etap:is(1, length(Views), "1 view btree in the group"),
    View1 = get_view(<<"view_1">>, Views),
    #set_view{
        btree = View1Btree
    } = View1,
    PartList = lists:seq(0, num_set_partitions() - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(PartList),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), PartList),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {64, ExpectedBitmask}},
        "Id Btree has the right reduce value"),
    ExpectedViewReduction = lists:sum([I * 2 || I <- lists:seq(0, 63)]),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {64, [ExpectedViewReduction], ExpectedBitmask}},
        "View1 Btree has the right reduce value"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            Value = [{View1#set_view.id_num, I}],
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
    etap:is(IdBtreeFoldResult, 64, "Id Btree has 64 entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, View1BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(Kv, _, I) ->
            PartId = I rem num_set_partitions(),
            DocId = doc_id(I),
            ExpectedKv = {{I, DocId}, {PartId, I * 2}},
            case ExpectedKv =:= Kv of
            true ->
                ok;
            false ->
                etap:bail("View1 Btree has an unexpected KV at iteration " ++ integer_to_list(I))
            end,
            {ok, I + 1}
        end,
        0, []),
    etap:is(View1BtreeFoldResult, 64, "View1 Btree has 64 entries"),
    ok.


test_same_key_by_different_docs_multiple_times_verify_btrees_3(Group) ->
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
    etap:is(1, length(Views), "1 view btree in the group"),
    View1 = get_view(<<"view_1">>, Views),
    #set_view{
        btree = View1Btree
    } = View1,
    PartList = lists:seq(0, num_set_partitions() - 1),
    ExpectedBitmask = couch_set_view_util:build_bitmask(PartList),
    DbSeqs = couch_set_view_test_util:get_db_seqs(test_set_name(), PartList),

    etap:is(
        couch_set_view_test_util:full_reduce_id_btree(Group, IdBtree),
        {ok, {0, 0}},
        "Id Btree is empty"),
    etap:is(
        couch_set_view_test_util:full_reduce_view_btree(Group, View1Btree),
        {ok, {0, [0], 0}},
        "View1 Btree is empty"),

    etap:is(HeaderUpdateSeqs, DbSeqs, "Header has right update seqs list"),
    etap:is(Abitmask, ExpectedBitmask, "Header has right active bitmask"),
    etap:is(Pbitmask, 0, "Header has right passive bitmask"),
    etap:is(Cbitmask, 0, "Header has right cleanup bitmask"),

    etap:diag("Verifying the Id Btree"),
    {ok, _, IdBtreeFoldResult} = couch_set_view_test_util:fold_id_btree(
        Group,
        IdBtree,
        fun(_Kv, _, I) ->
            {ok, I + 1}
        end,
        0, []),
    etap:is(IdBtreeFoldResult, 0, "Id Btree has 0 entries"),

    etap:diag("Verifying the View1 Btree"),
    {ok, _, View1BtreeFoldResult} = couch_set_view_test_util:fold_view_btree(
        Group,
        View1Btree,
        fun(_Kv, _, I) ->
            {ok, I + 1}
        end,
        0, []),
    etap:is(View1BtreeFoldResult, 0, "View1 Btree has 0 entries"),
    ok.
