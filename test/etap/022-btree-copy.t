#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./src/couchdb -sasl errlog_type error -boot start_sasl -noshell -smp enable

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

-record(btree, {
    fd,
    root,
    extract_kv,
    assemble_kv,
    less,
    reduce,
    kv_chunk_threshold = 16#4ff,
    kp_chunk_threshold = 2 * 16#4ff,
    binary_mode = false
}).

path(FileName) ->
    test_util:build_file(FileName).

main(_) ->
    test_util:init_code_path(),
    etap:plan(76),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail()
    end,
    ok.

test() ->
    couch_file_write_guard:sup_start_link(),
    ReduceCount = fun(reduce, KVs) ->
            length(KVs);
        (rereduce, Reds) ->
            lists:sum(Reds)
    end,

    test_copy(10, ReduceCount),
    test_copy(20, ReduceCount),
    test_copy(50, ReduceCount),
    test_copy(100, ReduceCount),
    test_copy(300, ReduceCount),
    test_copy(500, ReduceCount),
    test_copy(700, ReduceCount),
    test_copy(811, ReduceCount),
    test_copy(2333, ReduceCount),
    test_copy(6594, ReduceCount),
    test_copy(6666, ReduceCount),
    test_copy(9999, ReduceCount),
    test_copy(15003, ReduceCount),
    test_copy(21477, ReduceCount),
    test_copy(38888, ReduceCount),
    test_copy(66069, ReduceCount),
    test_copy(150123, ReduceCount),
    test_copy(420789, ReduceCount),
    test_copy(711321, ReduceCount),
    ok.


test_copy(NumItems, ReduceFun) ->
    etap:diag("Running btree copy test for " ++ integer_to_list(NumItems) ++ " items"),
    KVs = [{I, I} || I <- lists:seq(1, NumItems)],

    OriginalFileName = path(
        "test/etap/test_btree_" ++ integer_to_list(length(KVs)) ++ ".dat"),
    CopyFileName = path(
        "test/etap/test_btree_" ++ integer_to_list(NumItems) ++ "_copy.dat"),

    {ok, #btree{fd = Fd} = Btree} = make_btree(OriginalFileName, KVs, ReduceFun),
    {_, Red, _} = couch_btree:get_state(Btree),

    {ok, FdCopy} = couch_file:open(CopyFileName, [create, overwrite]),
    CopyCallback = fun(KV, Acc) -> {KV, Acc + 1} end,
    {ok, RootCopy, FinalAcc} = couch_btree_copy:copy(
        Btree, FdCopy, [{before_kv_write, {CopyCallback, 0}}]),
    ok = couch_file:flush(FdCopy),
    etap:is(FinalAcc, length(KVs),
        "couch_btree_copy returned the right final user acc"),

    {ok, BtreeCopy} = couch_btree:open(
        RootCopy, FdCopy, [{reduce, ReduceFun}]),
    check_btree_copy(BtreeCopy, Red, KVs),

    ok = couch_file:close(Fd),
    ok = couch_file:close(FdCopy),
    ok = file:delete(OriginalFileName),
    ok = file:delete(CopyFileName).


check_btree_copy(Btree, Red, KVs) ->
    {_, RedCopy, _} = couch_btree:get_state(Btree),
    etap:is(Red, RedCopy, "btree copy has same reduce value"),
    {ok, _, CopyKVs} = couch_btree:fold(
        Btree,
        fun(KV, _, Acc) -> {ok, [KV | Acc]} end,
        [], []),
    etap:is(KVs, lists:reverse(CopyKVs), "btree copy has same keys").


make_btree(Filename, KVs, ReduceFun) ->
    {ok, Fd} = couch_file:open(Filename, [create, overwrite]),
    {ok, Btree} = couch_btree:open(
        nil, Fd, [{reduce, ReduceFun}]),
    {ok, Btree2} = couch_btree:add_remove(Btree, KVs, []),
    {_, Red, _} = couch_btree:get_state(Btree2),
    etap:is(length(KVs), Red,
        "Inserted " ++ integer_to_list(length(KVs)) ++ " items into a new btree"),
    ok = couch_file:sync(Fd),
    {ok, Btree2}.
