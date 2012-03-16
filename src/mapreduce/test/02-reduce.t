#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable

% @copyright 2012 Couchbase, Inc.
%
% @author Filipe Manana  <filipe@couchbase.com>
%
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

main(_) ->
    test_util:init_code_path(),

    etap:plan(126),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    test_reduce_function_bad_syntax(),
    test_reduce_function_runtime_error(),
    test_reduce_function_throw_exception(),
    test_reduce_single_function_count(),
    test_reduce_single_function_sum(),
    test_reduce_multiple_functions(),
    test_burst(reduce, 1000),
    test_burst(reduce, 10000),
    test_burst(reduce, 25000),
    test_burst(rereduce, 1000),
    test_burst(rereduce, 10000),
    test_burst(rereduce, 25000),
    test_parallel_burst(reduce, 7500, 5),
    test_parallel_burst(reduce, 7500, 10),
    test_parallel_burst(reduce, 7500, 20),
    test_parallel_burst(rereduce, 7500, 5),
    test_parallel_burst(rereduce, 7500, 10),
    test_parallel_burst(rereduce, 7500, 20),
    ok = mapreduce:set_timeout(1000),
    test_many_timeouts(reduce, 1),
    test_many_timeouts(reduce, 10),
    test_many_timeouts(rereduce, 1),
    test_many_timeouts(rereduce, 10),
    ok.


test_reduce_function_bad_syntax() ->
    Result = mapreduce:start_reduce_context([
        <<"function(key, values, rereduce) { return values.length; ">>
    ]),
    etap:is(element(1, Result), error, "Got error when specifying reduce function with bad syntax"),
    etap:is(is_binary(element(2, Result)), true, "Error reason is a binary").


test_reduce_function_runtime_error() ->
    {ok, Ctx} = mapreduce:start_reduce_context([
        <<"function(key, values, rereduce) { return values[0].foo.bar; }">>
    ]),
    Result = mapreduce:reduce(Ctx, [
        {<<"\"y\"">>, <<"999">>},
        {<<"\"z\"">>, <<"1000">>}
    ]),
    etap:is(element(1, Result), error, "Got error when reduce function throws exception"),
    etap:is(is_binary(element(2, Result)), true, "Error reason is a binary").


test_reduce_function_throw_exception() ->
    {ok, Ctx} = mapreduce:start_reduce_context([
        <<"function(key, values, rereduce) { throw('foobar'); }">>
    ]),
    Result = mapreduce:reduce(Ctx, [
        {<<"\"y\"">>, <<"999">>},
        {<<"\"z\"">>, <<"1000">>}
    ]),
    etap:is(element(1, Result), error, "Got error when reduce function throws exception"),
    etap:is(is_binary(element(2, Result)), true, "Error reason is a binary").


test_reduce_single_function_count() ->
    FunBin = <<"function(key, values, rereduce) { return values.length; }">>,
    {ok, Ctx} = mapreduce:start_reduce_context([FunBin]),
    Results1 = mapreduce:reduce(Ctx, [
        {<<"\"a\"">>, <<"1">>},
        {<<"\"b\"">>, <<"2">>},
        {<<"\"c\"">>, <<"3">>},
        {<<"\"d\"">>, <<"4">>}
    ]),
    Results2 = mapreduce:reduce(Ctx, [
        {<<"\"x\"">>, <<"666">>}
    ]),
    Results3 = mapreduce:reduce(Ctx, [
        {<<"\"y\"">>, <<"999">>},
        {<<"\"z\"">>, <<"1000">>}
    ]),
    etap:is(Results1, {ok, [<<"4">>]}, "Reduce value is 4"),
    etap:is(Results2, {ok, [<<"1">>]}, "Reduce value is 1"),
    etap:is(Results3, {ok, [<<"2">>]}, "Reduce value is 2"),
    {ok, [Red1]} = Results1,
    {ok, [Red2]} = Results2,
    {ok, [Red3]} = Results3,
    RereduceResult1 = mapreduce:rereduce(Ctx, 1, [Red1, Red2, Red3]),
    etap:is(RereduceResult1, {ok, <<"3">>}, "Rereduce result is 3"),
    RereduceResult2 = mapreduce:rereduce(Ctx, 1, [Red1, Red2]),
    etap:is(RereduceResult2, {ok, <<"2">>}, "Rereduce result is 2"),
    RereduceResult3 = mapreduce:rereduce(Ctx, 1, [Red1]),
    etap:is(RereduceResult3, {ok, <<"1">>}, "Rereduce result is 1").


test_reduce_single_function_sum() ->
    FunBin = <<"function(key, values, rereduce) { return sum(values); }">>,
    {ok, Ctx} = mapreduce:start_reduce_context([FunBin]),
    Results1 = mapreduce:reduce(Ctx, [
        {<<"\"a\"">>, <<"1">>},
        {<<"\"b\"">>, <<"2">>},
        {<<"\"c\"">>, <<"3">>},
        {<<"\"d\"">>, <<"4">>}
    ]),
    Results2 = mapreduce:reduce(Ctx, [
        {<<"\"x\"">>, <<"666">>}
    ]),
    Results3 = mapreduce:reduce(Ctx, [
        {<<"\"y\"">>, <<"999">>},
        {<<"\"z\"">>, <<"1000">>}
    ]),
    etap:is(Results1, {ok, [<<"10">>]}, "Reduce value is 10"),
    etap:is(Results2, {ok, [<<"666">>]}, "Reduce value is 666"),
    etap:is(Results3, {ok, [<<"1999">>]}, "Reduce value is 1999"),
    {ok, [Red1]} = Results1,
    {ok, [Red2]} = Results2,
    {ok, [Red3]} = Results3,
    RereduceResult1 = mapreduce:rereduce(Ctx, 1, [Red1, Red2, Red3]),
    etap:is(RereduceResult1, {ok, <<"2675">>}, "Rereduce result is 2675"),
    RereduceResult2 = mapreduce:rereduce(Ctx, 1, [Red1, Red2]),
    etap:is(RereduceResult2, {ok, <<"676">>}, "Rereduce result is 676"),
    RereduceResult3 = mapreduce:rereduce(Ctx, 1, [Red1]),
    etap:is(RereduceResult3, {ok, <<"10">>}, "Rereduce result is 10").


test_reduce_multiple_functions() ->
    {ok, Ctx} = mapreduce:start_reduce_context([
        <<"function(key, values, rereduce) { return sum(values); }">>,
        <<"function(key, values, rereduce) { return values.length; }">>
    ]),
    Results1 = mapreduce:reduce(Ctx, [
        {<<"\"a\"">>, <<"1">>},
        {<<"\"b\"">>, <<"2">>},
        {<<"\"c\"">>, <<"3">>},
        {<<"\"d\"">>, <<"4">>}
    ]),
    Results2 = mapreduce:reduce(Ctx, [
        {<<"\"x\"">>, <<"666">>}
    ]),
    Results3 = mapreduce:reduce(Ctx, [
        {<<"\"y\"">>, <<"999">>},
        {<<"\"z\"">>, <<"1000">>}
    ]),
    etap:is(Results1, {ok, [<<"10">>, <<"4">>]}, "Reduce values are 10 and 4"),
    etap:is(Results2, {ok, [<<"666">>, <<"1">>]}, "Reduce values are 666 and 1"),
    etap:is(Results3, {ok, [<<"1999">>, <<"2">>]}, "Reduce values are 1999 and 2"),
    Results4 = mapreduce:reduce(Ctx, 1, [
        {<<"\"a\"">>, <<"1">>},
        {<<"\"b\"">>, <<"2">>},
        {<<"\"c\"">>, <<"3">>},
        {<<"\"d\"">>, <<"4">>}
    ]),
    Results5 = mapreduce:reduce(Ctx, 2, [
        {<<"\"a\"">>, <<"1">>},
        {<<"\"b\"">>, <<"2">>},
        {<<"\"c\"">>, <<"3">>},
        {<<"\"d\"">>, <<"4">>}
    ]),
    Results6 = (catch mapreduce:reduce(Ctx, 0, [
        {<<"\"a\"">>, <<"1">>},
        {<<"\"b\"">>, <<"2">>},
        {<<"\"c\"">>, <<"3">>},
        {<<"\"d\"">>, <<"4">>}
    ])),
    Results7 = (catch mapreduce:reduce(Ctx, 3, [
        {<<"\"a\"">>, <<"1">>},
        {<<"\"b\"">>, <<"2">>},
        {<<"\"c\"">>, <<"3">>},
        {<<"\"d\"">>, <<"4">>}
    ])),
    etap:is(Results4, {ok, <<"10">>}, "Reduce value is 10"),
    etap:is(Results5, {ok, <<"4">>}, "Reduce value is 4"),
    etap:is(Results6, {error, <<"invalid reduce function number">>}, "Got error"),
    etap:is(Results7, {error, <<"invalid reduce function number">>}, "Got error"),
    RereduceResult1 = mapreduce:reduce(Ctx, 1, [
        {<<"\"a\"">>, <<"1">>},
        {<<"\"b\"">>, <<"2">>},
        {<<"\"c\"">>, <<"3">>},
        {<<"\"d\"">>, <<"4">>}
    ]),
    RereduceResult2 = mapreduce:reduce(Ctx, 2, [
        {<<"\"a\"">>, <<"1">>},
        {<<"\"b\"">>, <<"2">>},
        {<<"\"c\"">>, <<"3">>},
        {<<"\"d\"">>, <<"4">>}
    ]),
    RereduceResult3 = (catch mapreduce:reduce(Ctx, 3, [
        {<<"\"a\"">>, <<"1">>},
        {<<"\"b\"">>, <<"2">>},
        {<<"\"c\"">>, <<"3">>},
        {<<"\"d\"">>, <<"4">>}
    ])),
    etap:is(RereduceResult1, {ok, <<"10">>}, "Rereduce value is 10"),
    etap:is(RereduceResult2, {ok, <<"4">>}, "Rereduce value is 4"),
    etap:is(RereduceResult3, {error, <<"invalid reduce function number">>}, "Got error"),
    ok.


test_burst(Fun, N) ->
    Results = do_burst(Fun, N),
    ExpectedResults = [
        [list_to_binary(integer_to_list(I + (I + 1) + (I + 2)))] || I <- lists:seq(1, N)
    ],
    etap:is(
        Results,
        ExpectedResults,
        "Correct results after a burst of " ++ integer_to_list(N) ++ " " ++ atom_to_list(Fun) ++ "s").


do_burst(Fun, N) ->
    RedFun = <<"function(key, values, rereduce) { return sum(values); }">>,
    {ok, Ctx} = mapreduce:start_reduce_context([RedFun]),
    lists:foldr(
        fun(I, Acc) ->
            K1 = io_lib:format("\"~p\"", [I]),
            K2 = io_lib:format("\"~p\"", [I + 1]),
            K3 = io_lib:format("\"~p\"", [I + 2]),
            V1 = io_lib:format("~p", [I]),
            V2 = io_lib:format("~p", [I + 1]),
            V3 = io_lib:format("~p", [I + 2]),
            case Fun of
            reduce ->
                {ok, Res} = mapreduce:reduce(Ctx, [{K1, V1}, {K2, V2}, {K3, V3}]),
                [Res | Acc];
            rereduce ->
                {ok, Res} = mapreduce:rereduce(Ctx, 1, [V1, V2, V3]),
                [[Res] | Acc]
            end
        end,
        [], lists:seq(1, N)).


test_parallel_burst(Fun, N, NumWorkers) ->
    Pids = lists:map(
        fun(_) ->
            spawn_monitor(fun() -> exit({ok, do_burst(Fun, N)}) end)
        end, lists:seq(1, NumWorkers)),
    ExpectedResults = [
        [list_to_binary(integer_to_list(I + (I + 1) + (I + 2)))] || I <- lists:seq(1, N)
    ],
    lists:foreach(
        fun({Pid, Ref}) ->
            receive
            {'DOWN', Ref, process, Pid, {ok, Value}} ->
                etap:is(
                    Value,
                    ExpectedResults,
                    "Worker returned correct result for a burst of " ++
                        integer_to_list(N) ++ " " ++ atom_to_list(Fun) ++ "s");
            {'DOWN', Ref, process, Pid, _Reason} ->
                etap:bail("Worker died unexpectedly")
            after 90000 ->
                etap:bail("Timeout waiting for worker result")
            end
        end,
        Pids).


test_many_timeouts(Fun, NumProcesses) ->
    Pids = lists:map(
        fun(_) ->
            spawn_monitor(fun() ->
                {ok, Ctx} = mapreduce:start_reduce_context([
                    <<"function(keys, values, rereduce) { while (true) { }; }">>
                ]),
                case Fun of
                reduce ->
                    exit({ok, mapreduce:reduce(Ctx, [{<<"\"a\"">>, <<"1">>}])});
                rereduce ->
                    exit({ok, mapreduce:rereduce(Ctx, 1, [<<"1">>, <<"2">>])})
                end
            end)
        end,
        lists:seq(1, NumProcesses)),
    lists:foreach(
        fun({Pid, Ref}) ->
            receive
            {'DOWN', Ref, process, Pid, {ok, Value}} ->
                etap:is(Value, {error, <<"timeout">>},
                    "Worker got timeout error for " ++ atom_to_list(Fun));
            {'DOWN', Ref, process, Pid, _Reason} ->
                etap:bail("Worker died unexpectedly")
            after 120000 ->
                etap:bail("Timeout waiting for worker result")
            end
        end,
        Pids).
