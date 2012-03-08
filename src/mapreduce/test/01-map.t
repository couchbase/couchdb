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

    etap:plan(56),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    test_map_function_bad_syntax(),
    test_map_function_throw_exception(),
    test_map_function_runtime_error(),
    test_empty_results_single_function(),
    test_empty_results_multiple_functions(),
    test_single_results_single_function(),
    test_single_results_multiple_functions(),
    test_multiple_results_single_function(),
    test_multiple_results_multiple_functions(),
    test_consecutive_maps(),
    test_utf8(),
    test_burst(1000),
    test_burst(10000),
    test_burst(100000),
    test_parallel_burst_maps(25000, 5),
    test_parallel_burst_maps(25000, 10),
    test_parallel_burst_maps(25000, 20),
    % TODO: test map functions that take too long or go into an infinite loop
    % (protection mechanism not yet implemented).
    ok.


test_map_function_bad_syntax() ->
    Result = mapreduce:start_map_context([<<"function(doc) { emit(doc._id, 1); ">>]),
    etap:is(element(1, Result), error, "Got error when specifying map function with bad syntax"),
    etap:is(is_binary(element(2, Result)), true, "Error reason is a binary"),
    ok.


test_map_function_throw_exception() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { throw('foobar'); }">>
    ]),
    Results = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>),
    etap:is(Results, {error, <<"foobar">>}, "Got error when map function throws exception").


test_map_function_runtime_error() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { emit(doc.foo.bar.z, null); }">>
    ]),
    Results = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>),
    % {error,<<"TypeError: Cannot read property 'bar' of undefined">>}
    etap:is(element(1, Results), error, "Got an error when map function applied over doc"),
    etap:is(is_binary(element(2, Results)), true, "Error reason is a binary").


test_empty_results_single_function() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { if (doc.type === 'foobar') { emit(doc._id, null); } }">>
    ]),
    Results = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>),
    etap:is(Results, {ok, [[]]}, "Map function didn't emit any key").


test_empty_results_multiple_functions() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { if (doc.type === 'foobar') { emit(doc._id, doc._id); } }">>,
        <<"function(doc) { if (doc.type === '123') { emit(doc._id, 123); } }">>,
        <<"function(doc) { }">>
    ]),
    Results = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>),
    etap:is(Results, {ok, [[], [], []]}, "Map functions didn't emit any keys").


test_single_results_single_function() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { emit(doc._id, null); }">>
    ]),
    Results = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>),
    etap:is(Results, {ok, [[{<<"\"doc1\"">>, <<"null">>}]]}, "Map function emitted 1 key").


test_single_results_multiple_functions() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { if (doc.type === 'foobar') { emit(doc._id, doc._id); } }">>,
        <<"function(doc) { emit(doc._id, null); }">>,
        <<"function(doc) { if (doc.type === '123') { emit(doc._id, 123); } }">>
    ]),
    Results = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>),
    etap:is(Results, {ok, [[], [{<<"\"doc1\"">>, <<"null">>}], []]}, "Map functions emitted 1 key").


test_multiple_results_single_function() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { emit(doc._id, 1); emit(doc._id, 2); }">>
    ]),
    Results = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>),
    Expected = [[{<<"\"doc1\"">>, <<"1">>}, {<<"\"doc1\"">>, <<"2">>}]],
    etap:is(Results, {ok, Expected}, "Map function emitted 2 keys").


test_multiple_results_multiple_functions() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { emit(doc._id, 1); emit(doc._id, 2); }">>,
        <<"function(doc) { emit(doc._id, null); }">>
    ]),
    Results = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>),
    Expected = [
        [{<<"\"doc1\"">>, <<"1">>}, {<<"\"doc1\"">>, <<"2">>}],
        [{<<"\"doc1\"">>, <<"null">>}]
    ],
    etap:is(Results, {ok, Expected}, "Map function emitted 3 keys").


test_consecutive_maps() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { emit(doc._id, doc.value); }">>,
        <<"function(doc) { emit(doc._id, doc.value * 3); }">>
    ]),
    Results1 = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>),
    Results2 = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc2\", \"value\": 2}">>),
    Results3 = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc3\", \"value\": 3}">>),
    Expected1 = [[{<<"\"doc1\"">>, <<"1">>}], [{<<"\"doc1\"">>, <<"3">>}]],
    Expected2 = [[{<<"\"doc2\"">>, <<"2">>}], [{<<"\"doc2\"">>, <<"6">>}]],
    Expected3 = [[{<<"\"doc3\"">>, <<"3">>}], [{<<"\"doc3\"">>, <<"9">>}]],
    etap:is(Results1, {ok, Expected1}, "First iteration results are correct"),
    etap:is(Results2, {ok, Expected2}, "Second iteration results are correct"),
    etap:is(Results3, {ok, Expected3}, "Third iteration results are correct").


test_burst(N) ->
    Results = do_burst(N),
    ExpectedResults = [
        [[{list_to_binary(["\"", integer_to_list(I), "\""]), list_to_binary(integer_to_list(I))}]]
            || I <- lists:seq(1, N)
    ],
    etap:is(
        Results,
        ExpectedResults,
        "Correct results after a burst of " ++ integer_to_list(N) ++ " maps").


do_burst(N) ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { emit(doc._id, doc.value); }">>
    ]),
    lists:foldr(
        fun(I, Acc) ->
            Doc = io_lib:format("{\"_id\": \"~p\", \"value\": ~p}", [I, I]),
            {ok, Res} = mapreduce:map_doc(Ctx, Doc),
            [Res | Acc]
        end,
        [], lists:seq(1, N)).


test_parallel_burst_maps(N, NumWorkers) ->
    Pids = lists:map(
        fun(_) ->
            spawn_monitor(fun() -> exit({ok, do_burst(N)}) end)
        end, lists:seq(1, NumWorkers)),
    ExpectedResults = [
        [[{list_to_binary(["\"", integer_to_list(I), "\""]), list_to_binary(integer_to_list(I))}]]
            || I <- lists:seq(1, N)
    ],
    lists:foreach(
        fun({Pid, Ref}) ->
            receive
            {'DOWN', Ref, process, Pid, {ok, Value}} ->
                etap:is(
                    Value,
                    ExpectedResults,
                    "Worker returned correct result for a burst of " ++
                        integer_to_list(N) ++ " maps");
            {'DOWN', Ref, process, Pid, _Reason} ->
                etap:bail("Worker died unexpectedly")
            after 120000 ->
                etap:bail("Timeout waiting for worker result")
            end
        end,
        Pids).


test_utf8() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { emit(doc._id, doc.value); }">>
    ]),

    Results1 = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": \"\\u00c1\"}">>),
    ExpectedResults1 = {ok, [[{<<"\"doc1\"">>, <<"\"", 195, 129, "\"">>}]]},
    Results2 = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": \"", 195, 129, "\"}">>),
    ExpectedResults2 = {ok, [[{<<"\"doc1\"">>, <<"\"", 195, 129, "\"">>}]]},
    etap:is(Results1, ExpectedResults1, "Right map value with A with accent"),
    etap:is(Results2, ExpectedResults2, "Right map value with A with accent"),

    Results3 = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": \"\\u0179\"}">>),
    ExpectedResults3 = {ok, [[{<<"\"doc1\"">>, <<"\"", 197, 185, "\"">>}]]},
    Results4 = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": \"", 197, 185, "\"}">>),
    ExpectedResults4 = {ok, [[{<<"\"doc1\"">>, <<"\"", 197, 185, "\"">>}]]},
    etap:is(Results3, ExpectedResults3, "Right map value with Z with acute"),
    etap:is(Results4, ExpectedResults4, "Right map value with Z with acute"),
    ok.
