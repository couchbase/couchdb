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

    etap:plan(3),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    test_sum_function(),
    test_base64decode_function(),
    test_dateToArray_function(),
    ok.


test_sum_function() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { emit(doc._id, sum(doc.values)); }">>
    ]),
    Results = mapreduce:map_doc(Ctx, <<"{\"_id\": \"doc1\", \"values\": [1, 2, 3, 4]}">>, <<"{}">>),
    etap:is(Results, {ok, [[{<<"\"doc1\"">>, <<"10">>}]]}, "sum() builtin function works").

test_base64decode_function() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc) { emit(doc._id, String.fromCharCode.apply(this, decodeBase64(doc._bin))); }">>
    ]),
    Results = mapreduce:map_doc(Ctx, <<"{ \"_id\": \"counter\", \"_bin\": \"NQ==\" }">>, <<"{}">>),
    etap:is(Results, {ok,[[{<<"\"counter\"">>,<<"\"5\"">>}]]}, "decodeBase64() builtin function works").


test_dateToArray_function() ->
    {ok, Ctx} = mapreduce:start_map_context([
        <<"function(doc, meta) { emit(dateToArray(doc.date), meta.id); }">>
    ]),
    Results = mapreduce:map_doc(Ctx, <<"{ \"date\":\"+033658-09-27T01:46:40.000Z\"}">>, <<"{\"id\":\"foo\"}">>),
    etap:is(Results, {ok,[[{<<"[33658,9,27,1,46,40]">>,<<"\"foo\"">>}]]}, "dateToArray() builtin function works").
