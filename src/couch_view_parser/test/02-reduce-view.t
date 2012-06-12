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

    etap:plan(33),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    test_reduce_view_1_row(),
    test_reduce_view_3_rows(),
    test_reduce_view_4_rows_3_errors(),
    test_reduce_view_4_rows_with_debug_info(),
    test_reduce_view_4_rows_with_debug_info_and_errors(),
    ok.


test_reduce_view_1_row() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"rows\": [">>,
            <<"{\"key\": \"foo\",">>, <<"\"value\": 32">>, <<"1}">>,
        <<"]">>,
        <<"}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success parsing reduce result with 1 single row"),
    State1 = couch_view_parser:next_state(Ctx),
    ExpectedState1 = {ok, rows, [{<<"\"foo\"">>, <<"321">>}]},
    etap:is(State1,
            ExpectedState1,
            "First state has a single row"),
    State2 = couch_view_parser:next_state(Ctx),
    etap:is(State2, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing single reduce row but with multiple chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2,
            ExpectedState1,
            "First state has a single row"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_reduce_view_3_rows() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"rows\": [">>,
            <<"{\"key\": \"foo\",">>, <<"\"value\": 32">>, <<"1},">>,
            <<"{\"key\": \"foo2\",">>, <<"\"value\": null},">>,
            <<"{\"key\": [3, 2,1],">>, <<"\"value\": true}">>,
        <<"]">>,
        <<"}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success parsing reduce result with 3 rows"),
    State1 = couch_view_parser:next_state(Ctx),
    ExpectedState1 = {ok, rows, [
        {<<"\"foo\"">>, <<"321">>},
        {<<"\"foo2\"">>, <<"null">>},
        {<<"[3,2,1]">>, <<"true">>}
    ]},
    etap:is(State1,
            ExpectedState1,
            "First state has 3 reduce rows"),
    State2 = couch_view_parser:next_state(Ctx),
    etap:is(State2, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing 3 reduce rows but with multiple chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2,
            ExpectedState1,
            "First state has 3 reduce rows"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_reduce_view_4_rows_3_errors() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"">>, <<"rows\": [">>,
            <<"{\"key\": ">>, <<"1, \"value\": 123},">>,
            <<"{\"key\": 2, \"value\":">>, <<" 124},">>,
            <<"{\"key\": 3, \"value\": 125}">>, <<",">>,
            <<"{\"key\": 4, \"value\": 126}">>,
        <<"],">>,
        <<"\"err">>, <<"ors\":">>, <<"[">>,
            <<"{\"from\": \"http://server1.com\", \"reason\"">>, <<": \"timeout\"},">>,
            <<"{\"from\": \"http://serv">>, <<"er2.com\", \"reason\": \"reset\"},">>,
            <<"{ \"reason\": \"shutdown\", \"from\": \"http://server3.com\"}">>,
        <<"]">>,
        <<"}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success 4 rows result with 3 error entries"),
    State1 = couch_view_parser:next_state(Ctx),
    ExpectedState1 = {ok, rows, [
        {<<"1">>, <<"123">>},
        {<<"2">>, <<"124">>},
        {<<"3">>, <<"125">>},
        {<<"4">>, <<"126">>}
    ]},
    etap:is(State1,
            ExpectedState1,
            "Next state has 4 rows"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedState2 = {ok, errors, [
        {<<"\"http://server1.com\"">>, <<"\"timeout\"">>},
        {<<"\"http://server2.com\"">>, <<"\"reset\"">>},
        {<<"\"http://server3.com\"">>, <<"\"shutdown\"">>}
    ]},
    etap:is(State2,
            ExpectedState2,
            "Next state has 3 error entries"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing 4 rows result with 3 error entries but parsing smaller chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2,
            ExpectedState1,
            "Next state has 4 rows"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2,
            ExpectedState2,
            "Next state has 3 error entries"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_reduce_view_4_rows_with_debug_info() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"debug_info\": {">>,
            <<"\"local\": {\"seqs\"">>, <<": [1,2, 3,4, \"foo\"] },">>,
            <<"\"http://server">>, <<"2.com\": {\"seqs\"">>, <<": [8,null,true,false] }">>,
        <<"},">>,
        <<"\"rows\": [">>,
            <<"{\"key\": 1, \"value\": 123},">>,
            <<"{\"key\": 2, \"value\": 124},">>,
            <<"{\"key\": 3, \"value\": 125},">>,
            <<"{\"key\": 4, \"value\": 126}">>,
        <<"]}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success parsing 4 rows result with debug info"),
    State1 = couch_view_parser:next_state(Ctx),
    ExpectedState1 = {ok, debug_infos, [
        {<<"\"local\"">>, <<"{\"seqs\":[1,2,3,4,\"foo\"]}">>},
        {<<"\"http://server2.com\"">>, <<"{\"seqs\":[8,null,true,false]}">>}
    ]},
    etap:is(State1, ExpectedState1, "Next state has debug info entries"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedState2 = {ok, rows, [
        {<<"1">>, <<"123">>},
        {<<"2">>, <<"124">>},
        {<<"3">>, <<"125">>},
        {<<"4">>, <<"126">>}
    ]},
    etap:is(State2, ExpectedState2, "Next state has 4 rows"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing 4 rows result with debug info but parsing smaller chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, ExpectedState1, "Next state has debug info entries"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2, ExpectedState2, "Next state has 4 rows"),
    State3_2 = couch_view_parser:next_state(Ctx),
    etap:is(State3_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_reduce_view_4_rows_with_debug_info_and_errors() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"debug_info\": {">>,
            <<"\"local\": {\"seqs\"">>, <<": [1,2, 3,4, \"foo\"] },">>,
            <<"\"http://server">>, <<"2.com\": {\"seqs\"">>, <<": [8,null,true,false] }">>,
        <<"},">>,
        <<"\"rows\": [">>,
            <<"{\"key\": 1, \"value\": 123},">>,
            <<"{\"key\": 2, \"value\": 124},">>,
            <<"{\"key\": 3, \"value\": 125},">>,
            <<"{\"key\": 4, \"value\": 126}">>,
        <<"],">>,
        <<"\"err">>, <<"ors\":">>, <<"[">>,
            <<"{\"from\": \"http://server1.com\", \"reason\"">>, <<": \"timeout\"},">>,
            <<"{\"from\": \"http://serv">>, <<"er2.com\", \"reason\": \"reset\"},">>,
            <<"{ \"reason\": \"shutdown\", \"from\": \"http://server3.com\"}">>,
        <<"]}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success parsing 4 rows result with debug info"),
    State1 = couch_view_parser:next_state(Ctx),
    ExpectedState1 = {ok, debug_infos, [
        {<<"\"local\"">>, <<"{\"seqs\":[1,2,3,4,\"foo\"]}">>},
        {<<"\"http://server2.com\"">>, <<"{\"seqs\":[8,null,true,false]}">>}
    ]},
    etap:is(State1, ExpectedState1, "Next state has debug info entries"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedState2 = {ok, rows, [
        {<<"1">>, <<"123">>},
        {<<"2">>, <<"124">>},
        {<<"3">>, <<"125">>},
        {<<"4">>, <<"126">>}
    ]},
    etap:is(State2, ExpectedState2, "Next state has 4 rows"),
    ExpectedState3 = {ok, errors, [
        {<<"\"http://server1.com\"">>, <<"\"timeout\"">>},
        {<<"\"http://server2.com\"">>, <<"\"reset\"">>},
        {<<"\"http://server3.com\"">>, <<"\"shutdown\"">>}
    ]},
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, ExpectedState3, "Next state has error entries"),
    State4 = couch_view_parser:next_state(Ctx),
    etap:is(State4, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing 4 rows result with debug info but parsing smaller chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, ExpectedState1, "Next state has debug info entries"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2, ExpectedState2, "Next state has 4 rows"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, ExpectedState3, "Next state has error entries"),
    State4_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State4_2, {ok, done}, "Next state is {ok, done}"),
    ok.
