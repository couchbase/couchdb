#!/usr/bin/env escript
%% -*- Mode: Erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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


main(_) ->
    test_util:init_code_path(),
    etap:plan(24),
    case (catch test()) of
    ok ->
        etap:end_tests();
    Other ->
        etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
        etap:bail(Other)
    end,
    ok.


test() ->
    ExpectedUnused = {ok, doc_fields_unused},
    test_single_doc_unused(ExpectedUnused),
    test_single_doc_used(),
    test_multiple_doc_unused(ExpectedUnused),
    test_atleast_one_doc_used(),
    test_docstring_in_mapbody(ExpectedUnused),
    test_eval_in_mapbody(),
    mapreduce:set_optimize_doc_load(false),
    ExpectedUsed = {ok, doc_fields_used},
    test_single_doc_unused(ExpectedUsed),
    test_single_doc_used(),
    test_multiple_doc_unused(ExpectedUsed),
    test_atleast_one_doc_used(),
    test_docstring_in_mapbody(ExpectedUsed),
    test_eval_in_mapbody(),
    ok.

test_single_doc_unused(ExpectedVal) ->
    {ok, Ctx} = start_map_context([
        <<"function(doc, meta) { emit(meta.id, null); }">>
    ]),
    DocUsed = is_doc_used(Ctx),
    etap:is(DocUsed, ExpectedVal, "Document fields are not indexed"),
    Results = map_doc(Ctx, <<"{\"value\": 1}">>, <<"{\"id\": \"doc1\"}">>),
    etap:is(Results, {ok, [[{<<"\"doc1\"">>, <<"null">>}]]},
        "Map function emitted 1 key from meta").

test_single_doc_used() ->
    {ok, Ctx} = start_map_context([
        <<"function(doc) { emit(doc._id, null); }">>
    ]),
    DocUsed = is_doc_used(Ctx),
    etap:is(DocUsed, {ok, doc_fields_used}, "Document fields are indexed"),
    Results = map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>, <<"{}">>),
    etap:is(Results, {ok, [[{<<"\"doc1\"">>, <<"null">>}]]},
        "Map function emitted 1 key from doc").

test_multiple_doc_unused(ExpectedVal) ->
    {ok, Ctx} = start_map_context([
        <<"function(doc, meta) { emit(meta.id, 1); }">>,
        <<"function(doc, meta) { emit(meta.val, 2); }">>,
        <<"function(doc, meta) { emit(meta.rev, 3); }">>
    ]),
    DocUsed = is_doc_used(Ctx),
    etap:is(DocUsed, ExpectedVal, "Document fields are not indexed"),
    Results = map_doc(Ctx, <<"{\"value\": 1}">>, <<"{\"id\": \"doc1\",
        \"val\": \"val1\", \"rev\": \"rev1\"}">>),
    Expected = [
        [{<<"\"doc1\"">>, <<"1">>}],
        [{<<"\"val1\"">>, <<"2">>}],
        [{<<"\"rev1\"">>, <<"3">>}]
    ],
    etap:is(Results, {ok, Expected}, "Map function emitted 3 keys").

test_atleast_one_doc_used() ->
    {ok, Ctx} = start_map_context([
        <<"function(doc, meta) { emit(meta.id, 1); }">>,
        <<"function(doc, meta) { emit(meta.val, 2); }">>,
        <<"function(doc, meta) { emit(doc._id, 3); }">>
    ]),
    DocUsed = is_doc_used(Ctx),
    etap:is(DocUsed, {ok, doc_fields_used}, "Document fields are indexed"),
    Results = map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>,
        <<"{\"id\": \"doc1\", \"val\": \"val1\"}">>),
    Expected = [
        [{<<"\"doc1\"">>, <<"1">>}],
        [{<<"\"val1\"">>, <<"2">>}],
        [{<<"\"doc1\"">>, <<"3">>}]
    ],
    etap:is(Results, {ok, Expected}, "Map function emitted 3 keys").

test_docstring_in_mapbody(ExpectedVal) ->
    {ok, Ctx} = start_map_context([
        <<"function(myDoc, meta) { emit(meta.id, \"doc fields not indexed\"); }">>
    ]),
    DocUsed = is_doc_used(Ctx),
    etap:is(DocUsed, ExpectedVal, "Document fields are not indexed"),
    Results = map_doc(Ctx, <<"{\"value\": 1}">>, <<"{\"id\": \"doc1\"}">>),
    etap:is(Results, {ok, [[{<<"\"doc1\"">>, <<"\"doc fields not indexed\"">>}]]},
        "Not a regex search for usage of doc in map function").

test_eval_in_mapbody() ->
    {ok, Ctx} = start_map_context([
        <<"function(doc) { var x = eval('doc._id'); emit(x, null); }">>
    ]),
    DocUsed = is_doc_used(Ctx),
    etap:is(DocUsed, {ok, doc_fields_used},
        "Document fields are indexed when eval used in mapbody"),
    Results = map_doc(Ctx, <<"{\"_id\": \"doc1\", \"value\": 1}">>, <<"{}">>),
    etap:is(Results, {ok, [[{<<"\"doc1\"">>, <<"null">>}]]},
        "Map function emitted 1 key from doc").

start_map_context(MapFunSources) ->
    mapreduce:start_map_context(mapreduce_view, MapFunSources).

map_doc(Ctx, Doc, Meta) ->
    case mapreduce:map_doc(Ctx, Doc, Meta) of
    {ok, Ret, _Log} ->
        {ok, Ret};
    Other ->
        Other
    end.

is_doc_used(Ctx) ->
    mapreduce:is_doc_used(Ctx).
