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

filename() -> "./test/etap/temp.024".


main(_) ->
    test_util:init_code_path(),
    etap:plan(11),
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
    N = 77651,
    {ok, Btree, Fd} = create_btree(N),
    lists:foreach(fun(I) ->
        many_items_tree_test(Btree, I)
    end, [1, 6, 666, 669, 9999, 15001, 22228, 50703, 77000, 77651]),
    ok = couch_file:close(Fd),
    ok = file:delete(filename()).


create_btree(N) ->
    ReduceFun = fun
        (reduce, KVs) ->
            length(KVs);
        (rereduce, Reds) ->
            lists:sum(Reds)
    end,

    {ok, Fd} = couch_file:open(filename(), [create, overwrite]),
    {ok, Btree} = couch_btree:open(nil, Fd, [{reduce, ReduceFun}]),

    KVs = [{I, I * I} || I <- lists:seq(1, N)],

    {ok, Btree2} = couch_btree:add_remove(Btree, KVs, []),
    ok = couch_file:flush(Fd),

    {ok, FullRed} = couch_btree:full_reduce(Btree2),
    etap:is(FullRed, N, "Initial reduce value equals N"),

    {ok, Btree2, Fd}.


many_items_tree_test(Btree, ItemNumber) ->
    Acc0 = {1, undefined},
    FoldFun = fun
        (value, KV, _Reds, {I, undefined}) ->
            case I == ItemNumber of
            true ->
                {stop, {I, KV}};
            false ->
                {ok, {I + 1, undefined}}
            end;
        (branch, _K, Red, {I, undefined}) ->
            I2 = I + Red,
            case I2 >= ItemNumber of
            true ->
                {ok, {I, undefined}};
            false ->
                {skip, {I2, undefined}}
            end
    end,
    {ok, _, FinalAcc} = couch_btree:fold(Btree, FoldFun, Acc0, []),
    io:format("FinalAcc: ~p~n", [FinalAcc]),
    etap:is(FinalAcc,
            {ItemNumber, {ItemNumber, ItemNumber * ItemNumber}},
            "Got expected final acc").
