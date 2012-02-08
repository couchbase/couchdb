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

filename() -> "./test/etap/temp.023".


main(_) ->
    test_util:init_code_path(),
    etap:plan(27),
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
    no_purged_items_test(),
    all_purged_items_test(),
    partial_purges_test(),
    partial_purges_test_2(),
    partial_purges_test_with_stop(),
    add_remove_and_purge_test(),
    ok.


no_purged_items_test() ->
    ReduceFun = fun
        (reduce, KVs) -> length(KVs);
        (rereduce, Reds) -> lists:sum(Reds)
    end,

    {ok, Fd} = couch_file:open(filename(), [create, overwrite]),
    {ok, Btree} = couch_btree:open(nil, Fd, [{reduce, ReduceFun}, {chunk_threshold, 6 * 1024}]),

    N = 211341,
    KVs = [{I, I} || I <- lists:seq(1, N)],
    {ok, Btree2} = couch_btree:add_remove(Btree, KVs, []),
    ok = couch_file:flush(Fd),

    {ok, Red} = couch_btree:full_reduce(Btree2),
    etap:is(Red, N, "Initial reduce value equals N"),

    PurgeFun = fun
        (value, _, Acc) ->
            {keep, Acc};
        (branch, _, Acc) ->
            {keep, Acc}
    end,
    {ok, Btree3, Acc1} = couch_btree:guided_purge(Btree2, PurgeFun, []),
    ok = couch_file:flush(Fd),
    etap:is(Acc1, [], "guided_purge returned right accumulator"),
    {ok, Red2} = couch_btree:full_reduce(Btree3),
    etap:is(Red2, N, "Reduce value after guided purge equals N"),

    FoldFun = fun(KV, _, Acc) ->
        {ok, [KV | Acc]}
    end,
    {ok, _, KVs2} = couch_btree:foldl(Btree3, FoldFun, []),
    etap:is(lists:reverse(KVs2), KVs, "Btree has same values after guided purge"),

    couch_file:close(Fd).


all_purged_items_test() ->
    ReduceFun = fun
        (reduce, KVs) -> length(KVs);
        (rereduce, Reds) -> lists:sum(Reds)
    end,

    {ok, Fd} = couch_file:open(filename(), [create, overwrite]),
    {ok, Btree} = couch_btree:open(nil, Fd, [{reduce, ReduceFun}, {chunk_threshold, 6 * 1024}]),

    N = 211341,
    KVs = [{I, I} || I <- lists:seq(1, N)],
    {ok, Btree2} = couch_btree:add_remove(Btree, KVs, []),
    ok = couch_file:flush(Fd),

    {ok, Red} = couch_btree:full_reduce(Btree2),
    etap:is(Red, N, "Initial reduce value equals N"),

    PurgeFun = fun
        (value, _, {V, B}) ->
            {purge, {V + 1, B}};
        (branch, R, {V, B}) ->
            {purge, {V, B + R}}
    end,
    {ok, Btree3, Acc1} = couch_btree:guided_purge(Btree2, PurgeFun, {0, 0}),
    ok = couch_file:flush(Fd),
    etap:is(Acc1, {0, N}, "guided_purge returned right accumulator - {0, N}"),
    {ok, Red2} = couch_btree:full_reduce(Btree3),
    etap:is(Red2, 0, "Reduce value after guided purge equals 0"),

    FoldFun = fun(KV, _, Acc) ->
        {ok, [KV | Acc]}
    end,
    {ok, _, KVs2} = couch_btree:foldl(Btree3, FoldFun, []),
    etap:is(lists:reverse(KVs2), [], "Btree is empty after guided purge"),

    couch_file:close(Fd).


partial_purges_test() ->
    ReduceFun = fun
        (reduce, KVs) ->
            even_odd_count(KVs);
        (rereduce, Reds) ->
            Even = lists:sum([E || {E, _} <- Reds]),
            Odd = lists:sum([O || {_, O} <- Reds]),
            {Even, Odd}
    end,

    {ok, Fd} = couch_file:open(filename(), [create, overwrite]),
    {ok, Btree} = couch_btree:open(nil, Fd, [{reduce, ReduceFun}, {chunk_threshold, 6 * 1024}]),

    N = 211341,
    KVs = [{I, I} || I <- lists:seq(1, N)],
    {NumEven, NumOdds} = even_odd_count(KVs),

    {ok, Btree2} = couch_btree:add_remove(Btree, KVs, []),
    ok = couch_file:flush(Fd),

    {ok, Red} = couch_btree:full_reduce(Btree2),
    etap:is(Red, {NumEven, NumOdds}, "Initial reduce value equals {NumEven, NumOdd}"),

    PurgeFun = fun
        (value, {K, K}, Count) ->
            case (K rem 2) of
            0 ->
                {keep, Count};
            _ ->
                {purge, Count + 1}
            end;
        (branch, {0, _OdCount}, Count) ->
            {purge, Count};
        (branch, {_, 0}, Count) ->
            {keep, Count};
        (branch, {EvCount, _OdCount}, Count) when EvCount > 0 ->
            {partial_purge, Count}
    end,
    {ok, Btree3, Acc1} = couch_btree:guided_purge(Btree2, PurgeFun, 0),
    ok = couch_file:flush(Fd),
    etap:is(Acc1, NumOdds, "guided_purge returned right accumulator - NumOdds}"),
    {ok, Red2} = couch_btree:full_reduce(Btree3),
    etap:is(Red2, {NumEven, 0}, "Reduce value after guided purge equals {NumEven, 0}"),

    FoldFun = fun(KV, _, Acc) ->
        {ok, [KV | Acc]}
    end,
    {ok, _, KVs2} = couch_btree:foldl(Btree3, FoldFun, []),
    lists:foreach(
        fun({K, K}) ->
            case (K rem 2) of
            0 ->
                ok;
            _ ->
                etap:bail("Got odd value in btree after guided purge: " ++ integer_to_list(K))
            end
        end,
        KVs2),
    etap:diag("Btree has no odd values after guided purge"),
    etap:is(lists:reverse([K || {K, _} <- KVs2]), lists:sort([K || {K, _} <- KVs2]), "Btree keys are sorted"),

    couch_file:close(Fd).


partial_purges_test_2() ->
    ReduceFun = fun
        (reduce, KVs) ->
            {length(KVs), ordsets:from_list([P || {_K, {P, _I}} <- KVs])};
        (rereduce, Reds) ->
            {lists:sum([C || {C, _} <- Reds]), ordsets:union([S || {_, S} <- Reds])}
    end,

    {ok, Fd} = couch_file:open(filename(), [create, overwrite]),
    {ok, Btree} = couch_btree:open(nil, Fd, [{reduce, ReduceFun}, {chunk_threshold, 6 * 1024}]),

    N = 320000,
    KVs = [{iolist_to_binary(io_lib:format("doc_~6..0b", [I])), {I rem 64, I}} || I <- lists:seq(1, N)],

    {ok, Btree2} = couch_btree:add_remove(Btree, KVs, []),
    ok = couch_file:flush(Fd),

    {ok, Red} = couch_btree:full_reduce(Btree2),
    etap:is(Red, {N, lists:seq(0, 63)}, "Initial reduce value equals {N, lists:seq(0, 63)}"),

    PurgeFun = fun
        (value, {_K, {P, _V}}, Count) ->
            case P >= 32 of
            true ->
                {purge, Count + 1};
            false ->
                {keep, Count}
            end;
        (branch, {C, S}, Count) ->
            RemSet = lists:seq(32, 63),
            case ordsets:intersection(S, RemSet) of
            [] ->
                {keep, Count};
            S ->
                {purge, Count + C};
            _ ->
                {partial_purge, Count}
            end
    end,
    {ok, Btree3, Acc1} = couch_btree:guided_purge(Btree2, PurgeFun, 0),
    ok = couch_file:flush(Fd),
    etap:is(Acc1, N div 2, "guided_purge returned right accumulator - N div 2"),
    {ok, Red2} = couch_btree:full_reduce(Btree3),
    etap:is(Red2, {N div 2, lists:seq(0, 31)}, "Reduce value after guided purge equals {N div 2, lists:seq(0, 31)}"),

    FoldFun = fun(KV, _, Acc) ->
        {ok, [KV | Acc]}
    end,
    {ok, _, KVs2} = couch_btree:foldl(Btree3, FoldFun, []),
    lists:foreach(
        fun({_K, {P, V}}) ->
            case ordsets:is_element(P, lists:seq(0, 31)) of
            true ->
                ok;
            false ->
                etap:bail("Got value outside the range [0 .. 31]: " ++ integer_to_list(V))
            end
        end,
        KVs2),
    etap:diag("Btree has no values outside the range [0 .. 31]"),
    etap:is(lists:reverse([K || {K, _} <- KVs2]), lists:sort([K || {K, _} <- KVs2]), "Btree keys are sorted"),

    couch_file:close(Fd).


partial_purges_test_with_stop() ->
    ReduceFun = fun
        (reduce, KVs) ->
            even_odd_count(KVs);
        (rereduce, Reds) ->
            Even = lists:sum([E || {E, _} <- Reds]),
            Odd = lists:sum([O || {_, O} <- Reds]),
            {Even, Odd}
    end,

    {ok, Fd} = couch_file:open(filename(), [create, overwrite]),
    {ok, Btree} = couch_btree:open(nil, Fd, [{reduce, ReduceFun}, {chunk_threshold, 6 * 1024}]),

    N = 211341,
    KVs = [{I, I} || I <- lists:seq(1, N)],
    {NumEven, NumOdds} = even_odd_count(KVs),

    {ok, Btree2} = couch_btree:add_remove(Btree, KVs, []),
    ok = couch_file:flush(Fd),

    {ok, Red} = couch_btree:full_reduce(Btree2),
    etap:is(Red, {NumEven, NumOdds}, "Initial reduce value equals {NumEven, NumOdd}"),

    PurgeFun = fun
        (_, _, Count) when Count >= 4 ->
            {stop, Count};
        (value, {K, K}, Count) ->
            case (K rem 2) of
            0 ->
                {keep, Count};
            _ ->
                {purge, Count + 1}
            end;
        (branch, {0, _OdCount}, Count) ->
            {purge, Count};
        (branch, {_, 0}, Count) ->
            {keep, Count};
        (branch, {EvCount, _OdCount}, Count) when EvCount > 0 ->
            {partial_purge, Count}
    end,
    {ok, Btree3, Acc1} = couch_btree:guided_purge(Btree2, PurgeFun, 0),
    ok = couch_file:flush(Fd),
    etap:is(Acc1, 4, "guided_purge returned right accumulator - 4}"),
    {ok, Red2} = couch_btree:full_reduce(Btree3),
    etap:is(Red2, {NumEven, NumOdds - 4}, "Reduce value after guided purge equals {NumEven, NumOdds - 4}"),

    FoldFun = fun(KV, _, Acc) ->
        {ok, [KV | Acc]}
    end,
    {ok, _, KVs2} = couch_btree:foldl(Btree3, FoldFun, []),
    lists:foreach(
        fun({K, K}) ->
            case lists:member(K, [1, 3, 5, 7]) of
            false ->
                ok;
            true ->
                etap:bail("Got odd value <= 7 in btree after guided purge: " ++ integer_to_list(K))
            end
        end,
        KVs2),
    etap:diag("Btree has no odd values after guided purge"),
    etap:is(lists:reverse([K || {K, _} <- KVs2]), lists:sort([K || {K, _} <- KVs2]), "Btree keys are sorted"),

    couch_file:close(Fd).


add_remove_and_purge_test() ->
    ReduceFun = fun
        (reduce, KVs) ->
            even_odd_count(KVs);
        (rereduce, Reds) ->
            Even = lists:sum([E || {E, _} <- Reds]),
            Odd = lists:sum([O || {_, O} <- Reds]),
            {Even, Odd}
    end,

    {ok, Fd} = couch_file:open(filename(), [create, overwrite]),
    {ok, Btree} = couch_btree:open(nil, Fd, [{reduce, ReduceFun}, {chunk_threshold, 6 * 1024}]),

    N = 211341,
    KVs = [{I, I} || I <- lists:seq(1, N)],
    {NumEven, NumOdds} = even_odd_count(KVs),

    {ok, Btree2} = couch_btree:add_remove(Btree, KVs, []),
    ok = couch_file:flush(Fd),

    {ok, Red} = couch_btree:full_reduce(Btree2),
    etap:is(Red, {NumEven, NumOdds}, "Initial reduce value equals {NumEven, NumOdd}"),

    PurgeFun = fun
        (value, {K, K}, Count) ->
            case (K rem 2) of
            0 ->
                {keep, Count};
            _ ->
                {purge, Count + 1}
            end;
        (branch, {0, _OddCount}, Count) ->
            {purge, Count};
        (branch, {_, 0}, Count) ->
            {keep, Count};
        (branch, {EvCount, _OddCount}, Count) when EvCount > 0 ->
            {partial_purge, Count}
    end,
    {ok, PurgeAcc, Btree3} = couch_btree:add_remove(
        Btree2, [{2, -1}, {14006, -1}, {500000, -1}], [4, 200000, 10], PurgeFun, 0),
    ok = couch_file:flush(Fd),
    etap:is(PurgeAcc, NumOdds, "couch_btree:add_remove/5 returned right accumulator - NumOdds}"),
    {ok, Red2} = couch_btree:full_reduce(Btree3),
    etap:is(Red2, {NumEven - 2, 0}, "Reduce value after guided purge equals {NumEven - 2, 0}"),

    FoldFun = fun(KV, _, Acc) ->
        {ok, [KV | Acc]}
    end,
    {ok, _, KVs2} = couch_btree:foldl(Btree3, FoldFun, []),
    lists:foreach(
        fun({K, V}) ->
            case (K rem 2) =:= 0 of
            true ->
                ok;
            false ->
                etap:bail("Got odd value in btree after purge: " ++ integer_to_list(K))
            end,
            case lists:member(K, [2, 14006, 500000]) of
            true when V =:= -1 ->
                ok;
            true ->
                etap:bail("Key " ++ integer_to_list(K) ++ " has wrong value (expected -1)");
            false ->
                case K =:= V of
                true ->
                    ok;
                false ->
                    etap:bail("Key " ++ integer_to_list(K) ++ " has wrong value")
                end
            end
        end,
        KVs2),
    etap:diag("Btree has no odd values after couch_btree:add_remove/6 call"),
    etap:is(couch_util:get_value(4, KVs2), undefined, "Key 2 not in tree anymore"),
    etap:is(couch_util:get_value(10, KVs2), undefined, "Key 10 not in tree anymore"),
    etap:is(couch_util:get_value(200000, KVs2), undefined, "Key 200000 not in tree anymore"),
    etap:is(lists:reverse([K || {K, _} <- KVs2]), lists:sort([K || {K, _} <- KVs2]), "Btree keys are sorted"),

    couch_file:close(Fd).


even_odd_count(KVs) ->
    Even = lists:sum([1 || {E, _} <- KVs, (E rem 2) =:= 0]),
    Odd = lists:sum([1 || {O, _} <- KVs, (O rem 2) =/= 0]),
    {Even, Odd}.
