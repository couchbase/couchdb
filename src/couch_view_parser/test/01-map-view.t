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

-define(etap_match(Got, Expected, Desc),
        etap:fun_is(fun(XXXXXX) ->
            case XXXXXX of Expected -> true; _ -> false end
        end, Got, Desc)).

main(_) ->
    test_util:init_code_path(),

    etap:plan(164),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    test_bad_json(),
    test_bad_total_rows_value(),
    test_duplicated_total_rows_value(),
    test_incremental_empty_rows(),
    test_one_simple_row(),
    test_multiple_simple_rows(),
    test_multiple_complex_rows(),
    test_row_bad_id_type(),
    test_rows_with_docs(),
    test_row_with_bad_doc(),
    test_rows_with_partition_and_node(),
    test_row_with_bad_partition(),
    test_row_with_bad_node(),
    test_empty_rows_with_1_error(),
    test_empty_rows_with_3_errors(),
    test_4_rows_with_3_errors(),
    test_4_rows_with_3_errors_incremental(),
    test_debug_info_empty_rows(),
    test_debug_info_4_rows(),
    test_debug_info_4_rows_and_errors(),
    test_all_docs_rows(),
    test_quotes_in_string(),
    test_MB_6013(),
    ok.


test_bad_json() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Error = couch_view_parser:parse_chunk(Ctx, <<"{total_rows: 123}">>),
    ?etap_match(Error, {error, _}, "Error parsing {total_rows: 123}"),
    ok.


test_bad_total_rows_value() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Error = couch_view_parser:parse_chunk(Ctx, <<"{\"total_rows\": {}}">>),
    ?etap_match(Error, {error, _}, "Error parsing {\"total_rows\": {}}"),
    ok.


test_duplicated_total_rows_value() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    ok = couch_view_parser:parse_chunk(Ctx, <<"{\"total_rows\": 123,">>),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1,
            {ok, row_count, "123"},
            "Next state is {ok, row_count, \"123\"}"),
    etap:is(couch_view_parser:parse_chunk(Ctx, <<"\"total_rows\": 321}">>),
            ok,
            "No error parsing duplicated total_rows"),
    State2 = couch_view_parser:next_state(Ctx),
    etap:is(State2,
            {ok, done},
            "State after parsing duplicated row_count is {ok, done}"),
    ok.


test_incremental_empty_rows() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, need_more_data}, "Next state is {ok, need_more_data}"),
    etap:is(couch_view_parser:parse_chunk(Ctx, <<>>),
            ok,
            "No error parsing empty chunk"),
    State2 = couch_view_parser:next_state(Ctx),
    etap:is(State2, {ok, need_more_data}, "Next state is {ok, need_more_data}"),
    etap:is(couch_view_parser:parse_chunk(Ctx, <<"{">>),
            ok,
            "Success parsing chunk {"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, need_more_data}, "Next state is {ok, need_more_data}"),
    etap:is(couch_view_parser:parse_chunk(Ctx, <<"\"total_rows\"">>),
            ok,
            "Success parsing chunk \"total_rows\""),
    State4 = couch_view_parser:next_state(Ctx),
    etap:is(State4, {ok, need_more_data}, "Next state is {ok, need_more_data}"),
    etap:is(couch_view_parser:parse_chunk(Ctx, <<":12">>),
            ok,
            "Success parsing chunk :12"),
    State5 = couch_view_parser:next_state(Ctx),
    etap:is(State5, {ok, need_more_data}, "Next state is {ok, need_more_data}"),
    etap:is(couch_view_parser:parse_chunk(Ctx, <<"34,">>),
            ok,
            "Success parsing chunk 34,"),
    State6 = couch_view_parser:next_state(Ctx),
    etap:is(State6, {ok, row_count, "1234"}, "Next state is {ok, row_count, \"1234\"}"),
    etap:is(couch_view_parser:parse_chunk(Ctx, <<"\r\n\"rows\":">>),
            ok,
            "Success parsing chunk \\r\\n\"rows\":"),
    State7 = couch_view_parser:next_state(Ctx),
    etap:is(State7, {ok, need_more_data}, "Next state is {ok, need_more_data}"),
    etap:is(couch_view_parser:parse_chunk(Ctx, <<"[]}">>),
            ok,
            "Success parsing chunk []}"),
    State8 = couch_view_parser:next_state(Ctx),
    etap:is(State8, {ok, done}, "Next state is {ok, done}"),
    ok.


test_one_simple_row() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_rows\": 1,\r\n">>,
        <<"\"rows\": [">>,
            <<"{\"key\": \"foo\", \"id\"">>, <<": \"bar\", \"value\": 32">>, <<"1}">>,
        <<"]">>,
        <<"}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success parsing result with 1 single row"),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, row_count, "1"}, "Next state is {ok, row_count, \"1\"}"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedState2 = {ok, rows, [{{<<"\"foo\"">>, <<"\"bar\"">>}, <<"321">>}]},
    etap:is(State2,
            ExpectedState2,
            "Next state has single row"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing single simple row but with multiple chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, {ok, row_count, "1"}, "Next state is {ok, row_count, \"1\"}"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2,
            ExpectedState2,
            "Next state has single row"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_multiple_simple_rows() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_rows\": 3,\r\n">>,
        <<"\"rows\": [">>,
            <<"{\"key\": \"foo\", \"id\"">>, <<": \"bar\", \"value\": 32">>, <<"1},">>,
            <<"{\"key\": [3, null], \"id\"">>, <<": \"bar2\", \"value\": true},">>,
            <<"{\"key\": [\"a\", false], \"id\"">>, <<":\"bar3\", \"value\": [1,2">>, <<",3]}">>,
        <<"]">>,
        <<"}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success parsing result with 3 rows"),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, row_count, "3"}, "Next state is {ok, row_count, \"3\"}"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedState2 = {ok, rows, [
        {{<<"\"foo\"">>, <<"\"bar\"">>}, <<"321">>},
        {{<<"[3,null]">>, <<"\"bar2\"">>}, <<"true">>},
        {{<<"[\"a\",false]">>, <<"\"bar3\"">>}, <<"[1,2,3]">>}
    ]},
    etap:is(State2,
            ExpectedState2,
            "Next state has correct rows"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing multiple simple rows but with multiple chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, {ok, row_count, "3"}, "Next state is {ok, row_count, \"3\"}"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2,
            ExpectedState2,
            "Next state has correct rows"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_multiple_complex_rows() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_rows\": 3,\r\n">>,
        <<"\"rows\": [">>,
            <<"{\"key\": [[\"foo\"], []], \"id\"">>,
                <<": \"bar\", \"value\": {\"a\":[1,2,[],3]}},">>,
            <<"{\"key\": {\"xyz\": 123, \"qwe\": [{}]}, \"id\"">>,
                <<": \"bar2\", \"value\": { \"v1\": {\"v11\": {}}, ">>,
                    <<"\"v2\": [[null, true, false]] }},">>,
            <<"{\"key\": [[[]], [], [1,2,{}], []], \"id\"">>,
                <<":\"bar3\", \"value\": [{}, {}">>, <<",[{\"qwerty\": [\"foobar\"">>,
                <<", {\"rows\": {}}]}]]}">>,
        <<"]">>,
        <<"}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success parsing result with 3 complex rows"),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, row_count, "3"}, "Next state is {ok, row_count, \"3\"}"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedState2 = {ok, rows, [
        {{<<"[[\"foo\"],[]]">>, <<"\"bar\"">>},
            <<"{\"a\":[1,2,[],3]}">>},
        {{<<"{\"xyz\":123,\"qwe\":[{}]}">>, <<"\"bar2\"">>},
            <<"{\"v1\":{\"v11\":{}},\"v2\":[[null,true,false]]}">>},
        {{<<"[[[]],[],[1,2,{}],[]]">>, <<"\"bar3\"">>},
            <<"[{},{},[{\"qwerty\":[\"foobar\",{\"rows\":{}}]}]]">>}
    ]},
    etap:is(State2,
            ExpectedState2,
            "Next state has correct rows"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing multiple complex rows but with multiple chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, {ok, row_count, "3"}, "Next state is {ok, row_count, \"3\"}"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2,
            ExpectedState2,
            "Next state has correct rows"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_row_bad_id_type() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_rows\": 1,\r\n">>,
        <<"\"rows\": [">>,
            <<"{\"key\": \"foo\", \"id\"">>, <<": 111, \"value\": 321}">>,
        <<"]">>,
        <<"}">>
    ],
    ?etap_match(couch_view_parser:parse_chunk(Ctx, Json),
                {error, _},
                "Error parsing result with row with bad id type"),
    State1 = couch_view_parser:next_state(Ctx),
    ?etap_match(State1, {error, _}, "Next state matches {error, _}"),
    ok.


test_rows_with_docs() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_rows\": 3,\r\n">>,
        <<"\"rows\": [">>,
            <<"{\"key\": \"a\", \"id\": \"b\", \"value\": 1,">>,
                <<"\"doc\": null},">>,
            <<"{\"key\": \"c\", \"id\": \"d\", \"value\": 2,">>,
                <<"\"doc\": {\"value\": 123, \"bar\": [\"x\", \"y\", \"z\"]}},">>,
            <<"{\"key\": \"e\", \"id\": \"f\", \"value\": 3,">>,
                <<"\"doc\": {\"value1\": null, \"value2\": ">>,
                    <<"{\"nested1\": null, \"nested2\": {\"nested3\": null}}}}">>,
        <<"]">>,
        <<"}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Able to parse rows with docs"),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, row_count, "3"}, "Next state matches {ok, row_count, \"3\"}"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedRows = {ok, rows, [
        {{<<"\"a\"">>, <<"\"b\"">>}, <<"1">>, <<"null">>},
        {{<<"\"c\"">>, <<"\"d\"">>}, <<"2">>,
            <<"{\"value\":123,\"bar\":[\"x\",\"y\",\"z\"]}">>},
        {{<<"\"e\"">>, <<"\"f\"">>}, <<"3">>,
            <<"{\"value1\":null,\"value2\":{\"nested1\":null,\"nested2\":{\"nested3\":null}}}">>}
    ]},
    etap:is(State2, ExpectedRows, "Next state has rows with docs"),
    ok.


test_row_with_bad_doc() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_rows\": 1,\r\n">>,
        <<"\"rows\": [">>,
            <<"{\"key\": \"a\", \"id\": \"b\", \"value\": 1,">>,
                <<"\"doc\": 123}">>,
        <<"]">>,
        <<"}">>
    ],
    ?etap_match(couch_view_parser:parse_chunk(Ctx, Json),
                {error, _},
                "Error parsing row with bad doc value"),
    ?etap_match(couch_view_parser:next_state(Ctx),
                {error, _},
                "Error getting state after parsing row with bad doc value"),
    ok.


test_rows_with_partition_and_node() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_rows\": 3,\r\n">>,
        <<"\"rows\": [">>,
            <<"{\"key\": \"foo\", \"id\"">>, <<": \"bar\", ">>,
                <<"\"partition">>, <<"\": 0, \"node\": \"local\",">>,
                <<"\"value\": 32">>, <<"1},">>,
            <<"{\"key\": \"foo2\", \"id\"">>, <<": \"bar2\", ">>,
                <<"\"partition">>, <<"\": 1023, \"node\":">>,
                    <<"\"http://server2.com:9500/_view_merge\",">>,
                <<"\"value\": 32">>, <<"2},">>,
            <<"{\"key\": \"foo3\", \"id\"">>, <<": \"bar3\", ">>,
                <<"\"partition">>, <<"\": 666, \"node\":">>,
                    <<"\"http://server3.com:5984/_view_merge/\",">>,
                <<"\"value\": 32">>, <<"3}">>,
        <<"]">>,
        <<"}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success parsing result with 3 rows with partition and node info"),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, row_count, "3"}, "Next state is {ok, row_count, \"3\"}"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedState2 = {ok, rows, [
        {{<<"\"foo\"">>, <<"\"bar\"">>},
            {<<"0">>, <<"\"local\"">>, <<"321">>}},
        {{<<"\"foo2\"">>, <<"\"bar2\"">>},
            {<<"1023">>, <<"\"http://server2.com:9500/_view_merge\"">>, <<"322">>}},
        {{<<"\"foo3\"">>, <<"\"bar3\"">>},
            {<<"666">>, <<"\"http://server3.com:5984/_view_merge/\"">>, <<"323">>}}
    ]},
    etap:is(State2,
            ExpectedState2,
            "Next state has correct rows"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing multiple rows with partition and node info, but with multiple chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, {ok, row_count, "3"}, "Next state is {ok, row_count, \"3\"}"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2,
            ExpectedState2,
            "Next state has correct rows"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_row_with_bad_partition() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_rows\": 1,\r\n">>,
        <<"\"rows\": [">>,
            <<"{\"key\": \"foo3\", \"id\"">>, <<": \"bar3\", ">>,
                <<"\"partition">>, <<"\": \"666\", \"node\":">>,
                    <<"\"http://server3.com:5984/_view_merge/\",">>,
                <<"\"value\": 32">>, <<"3}">>,
        <<"]">>,
        <<"}">>
    ],
    ?etap_match(couch_view_parser:parse_chunk(Ctx, Json),
                {error, _},
                "Error parsing result with a row having a bad partition field"),
    State1 = couch_view_parser:next_state(Ctx),
    ?etap_match(State1,
                {error, _},
                "Error getting state"),
    ok.


test_row_with_bad_node() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_rows\": 1,\r\n">>,
        <<"\"rows\": [">>,
            <<"{\"key\": \"foo3\", \"id\"">>, <<": \"bar3\", ">>,
                <<"\"partition">>, <<"\": 666, \"node\": null,">>,
                <<"\"value\": 32">>, <<"3}">>,
        <<"]">>,
        <<"}">>
    ],
    ?etap_match(couch_view_parser:parse_chunk(Ctx, Json),
                {error, _},
                "Error parsing result with a row having a bad node field"),
    State1 = couch_view_parser:next_state(Ctx),
    ?etap_match(State1,
                {error, _},
                "Error getting state"),
    ok.


test_empty_rows_with_1_error() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_r">>, <<"ows\": 100,">>,
        <<"\"rows\": [">>,
        <<"],">>,
        <<"\"err">>, <<"ors\":">>, <<"[">>,
            <<"{\"from\": \"http://server1.com\", \"reason\"">>, <<": \"timeout\"}">>,
        <<"]">>,
        <<"}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success empty result with 1 error entry"),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedState2 = {ok, errors, [
        {<<"\"http://server1.com\"">>, <<"\"timeout\"">>}
    ]},
    etap:is(State2,
            ExpectedState2,
            "Next state has 1 error entry"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing empty result with 1 error entry but parsing smaller chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2,
            ExpectedState2,
            "Next state has 1 error entry"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_empty_rows_with_3_errors() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_r">>, <<"ows\": 100,">>,
        <<"\"rows\": [">>,
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
            "Success empty result with 3 error entries"),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
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

    etap:diag("Testing empty result with 3 error entries but parsing smaller chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2,
            ExpectedState2,
            "Next state has 3 error entries"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_4_rows_with_3_errors() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_r">>, <<"ows\": 100,">>,
        <<"\"rows\": [">>,
            <<"{\"key\": 1, \"id\": \"a\", \"value\": 123},">>,
            <<"{\"key\": 2, \"id\": \"b\", \"value\": 124},">>,
            <<"{\"key\": 3, \"id\": \"c\", \"value\": 125},">>,
            <<"{\"key\": 4, \"id\": \"d\", \"value\": 126}">>,
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
    etap:is(State1, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedState2 = {ok, rows, [
        {{<<"1">>, <<"\"a\"">>}, <<"123">>},
        {{<<"2">>, <<"\"b\"">>}, <<"124">>},
        {{<<"3">>, <<"\"c\"">>}, <<"125">>},
        {{<<"4">>, <<"\"d\"">>}, <<"126">>}
    ]},
    etap:is(State2,
            ExpectedState2,
            "Next state has 4 rows"),
    State3 = couch_view_parser:next_state(Ctx),
    ExpectedState3 = {ok, errors, [
        {<<"\"http://server1.com\"">>, <<"\"timeout\"">>},
        {<<"\"http://server2.com\"">>, <<"\"reset\"">>},
        {<<"\"http://server3.com\"">>, <<"\"shutdown\"">>}
    ]},
    etap:is(State3,
            ExpectedState3,
            "Next state has 3 error entries"),
    State4 = couch_view_parser:next_state(Ctx),
    etap:is(State4, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing 4 rows result with 3 error entries but parsing smaller chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2,
            ExpectedState2,
            "Next state has 4 rows"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2,
            ExpectedState3,
            "Next state has 3 error entries"),
    State4_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State4_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_4_rows_with_3_errors_incremental() ->
    etap:diag("Testing incremental parsing of result with 4 rows and 3 errors"),
    {ok, Ctx} = couch_view_parser:start_context(),
    Chunk1 = <<"{">>,
    Chunk2 = <<"\"total_r">>, Chunk3 = <<"ows\": 100,">>,
    Chunk4 = <<"\"rows\": [">>,
    Chunk5 = <<"{\"key\": 1, \"id\": \"a\", \"value\": 123},">>,
    Chunk6 = <<"{\"key\": 2, \"id\": \"b\", \"value\": 124},">>,
    Chunk7 = <<"{\"key\": 3, \"id\": \"c\", \"value\": 125},">>,
    Chunk8 = <<"{\"key\": 4, \"id\": \"d\", \"value\": 126}">>,
    Chunk9 = <<"],">>,
    Chunk10 = <<"\"errors\":">>, Chunk11 = <<"[">>,
    Chunk12 = <<"{\"from\": \"http://server1.com\", \"reason\"">>, Chunk13 = <<": \"timeout\"},">>,
    Chunk14 = <<"{\"from\": \"http://server2.com\", \"reason\": \"reset\"},">>,
    Chunk15 = <<"{ \"reason\": \"shutdown\", \"from\": \"http://server3.com\"}">>,
    Chunk16 = <<"]">>,
    Chunk17 = <<"}">>,

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk1),
            ok,
            "Success parsing chunk 1"),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, need_more_data}, "State1 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk2),
            ok,
            "Success parsing chunk 2"),
    State2 = couch_view_parser:next_state(Ctx),
    etap:is(State2, {ok, need_more_data}, "State2 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk3),
            ok,
            "Success parsing chunk 3"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, row_count, "100"}, "State3 is {ok, row_count, \"100\"}"),

    State4 = couch_view_parser:next_state(Ctx),
    etap:is(State4, {ok, need_more_data}, "State4 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk4),
            ok,
            "Success parsing chunk 4"),
    State5 = couch_view_parser:next_state(Ctx),
    etap:is(State5, {ok, need_more_data}, "State5 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk5),
            ok,
            "Success parsing chunk 5"),
    State6 = couch_view_parser:next_state(Ctx),
    etap:is(State6,
            {ok, rows, [{{<<"1">>, <<"\"a\"">>}, <<"123">>}]},
            "State6 is {ok, rows, [{{<<\"1\">>, <<\"\"a\"\">>}, <<\"123\">>}]}"),

    State7 = couch_view_parser:next_state(Ctx),
    etap:is(State7, {ok, need_more_data}, "State7 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk6),
            ok,
            "Success parsing chunk 6"),
    State8 = couch_view_parser:next_state(Ctx),
    etap:is(State8,
            {ok, rows, [{{<<"2">>, <<"\"b\"">>}, <<"124">>}]},
            "State8 is {ok, rows, [{{<<\"2\">>, <<\"\"b\"\">>}, <<\"124\">>}]}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk7),
            ok,
            "Success parsing chunk 7"),
    State9 = couch_view_parser:next_state(Ctx),
    etap:is(State9,
            {ok, rows, [{{<<"3">>, <<"\"c\"">>}, <<"125">>}]},
            "State9 is {ok, rows, [{{<<\"3\">>, <<\"\"c\"\">>}, <<\"125\">>}]}"),

    State10 = couch_view_parser:next_state(Ctx),
    etap:is(State10, {ok, need_more_data}, "State10 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk8),
            ok,
            "Success parsing chunk 8"),
    State11 = couch_view_parser:next_state(Ctx),
    etap:is(State11,
            {ok, rows, [{{<<"4">>, <<"\"d\"">>}, <<"126">>}]},
            "State11 is {ok, rows, [{{<<\"4\">>, <<\"\"d\"\">>}, <<\"126\">>}]}"),

    State12 = couch_view_parser:next_state(Ctx),
    etap:is(State12, {ok, need_more_data}, "State12 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk9),
            ok,
            "Success parsing chunk 9"),
    State13 = couch_view_parser:next_state(Ctx),
    etap:is(State13, {ok, need_more_data}, "State13 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk10),
            ok,
            "Success parsing chunk 10"),
    State14 = couch_view_parser:next_state(Ctx),
    etap:is(State14, {ok, need_more_data}, "State14 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk11),
            ok,
            "Success parsing chunk 11"),
    State15 = couch_view_parser:next_state(Ctx),
    etap:is(State15, {ok, need_more_data}, "State15 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk12),
            ok,
            "Success parsing chunk 12"),
    State16 = couch_view_parser:next_state(Ctx),
    etap:is(State16, {ok, need_more_data}, "State16 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk13),
            ok,
            "Success parsing chunk 13"),
    State17 = couch_view_parser:next_state(Ctx),
    etap:is(State17,
            {ok, errors, [{<<"\"http://server1.com\"">>, <<"\"timeout\"">>}]},
            "State17 is {ok, errors, [{<<\"\"http://server1.com\"\">>, <<\"\"timeout\"\">>}]}"),

    State18 = couch_view_parser:next_state(Ctx),
    etap:is(State18, {ok, need_more_data}, "State18 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk14),
            ok,
            "Success parsing chunk 14"),
    State19 = couch_view_parser:next_state(Ctx),
    etap:is(State19,
            {ok, errors, [{<<"\"http://server2.com\"">>, <<"\"reset\"">>}]},
            "State19 is {ok, errors, [{<<\"\"http://server2.com\"\">>, <<\"\"reset\"\">>}]}"),

    State20 = couch_view_parser:next_state(Ctx),
    etap:is(State20, {ok, need_more_data}, "State20 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk15),
            ok,
            "Success parsing chunk 15"),
    State21 = couch_view_parser:next_state(Ctx),
    etap:is(State21,
            {ok, errors, [{<<"\"http://server3.com\"">>, <<"\"shutdown\"">>}]},
            "State21 is {ok, errors, [{<<\"\"http://server3.com\"\">>, <<\"\"shutdown\"\">>}]}"),

    State22 = couch_view_parser:next_state(Ctx),
    etap:is(State22, {ok, need_more_data}, "State22 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk16),
            ok,
            "Success parsing chunk 16"),
    State23 = couch_view_parser:next_state(Ctx),
    etap:is(State23, {ok, need_more_data}, "State23 is {ok, need_more_data}"),

    etap:is(couch_view_parser:parse_chunk(Ctx, Chunk17),
            ok,
            "Success parsing chunk 17"),
    State24 = couch_view_parser:next_state(Ctx),
    etap:is(State24, {ok, done}, "State24 is {ok, done}"),
    ok.


test_debug_info_empty_rows() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"debug_info\": {">>,
            <<"\"local\": {\"seqs\"">>, <<": [1,2, 3,4, \"foo\"] },">>,
            <<"\"http://server">>, <<"2.com\": {\"seqs\"">>, <<": [8,null,true,false] }">>,
        <<"},">>,
        <<"\"total_r">>, <<"ows\": 100,">>,
        <<"\"rows\": [">>,
        <<"]}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success empty rows result with debug info"),
    State1 = couch_view_parser:next_state(Ctx),
    ExpectedState1 = {ok, debug_infos, [
        {<<"\"local\"">>, <<"{\"seqs\":[1,2,3,4,\"foo\"]}">>},
        {<<"\"http://server2.com\"">>, <<"{\"seqs\":[8,null,true,false]}">>}
    ]},
    etap:is(State1, ExpectedState1, "Next state has debug info entries"),
    State2 = couch_view_parser:next_state(Ctx),
    etap:is(State2, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing empty rows result with debug info but parsing smaller chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, ExpectedState1, "Next state has debug info entries"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_debug_info_4_rows() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"debug_info\": {">>,
            <<"\"local\": {\"seqs\"">>, <<": [1,2, 3,4, \"foo\"] },">>,
            <<"\"http://server">>, <<"2.com\": {\"seqs\"">>, <<": [8,null,true,false] }">>,
        <<"},">>,
        <<"\"total_r">>, <<"ows\": 100,">>,
        <<"\"rows\": [">>,
            <<"{\"key\": 1, \"id\": \"a\", \"value\": 123},">>,
            <<"{\"key\": 2, \"id\": \"b\", \"value\": 124},">>,
            <<"{\"key\": 3, \"id\": \"c\", \"value\": 125},">>,
            <<"{\"key\": 4, \"id\": \"d\", \"value\": 126}">>,
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
    etap:is(State2, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
    State3 = couch_view_parser:next_state(Ctx),
    ExpectedState3 = {ok, rows, [
        {{<<"1">>, <<"\"a\"">>}, <<"123">>},
        {{<<"2">>, <<"\"b\"">>}, <<"124">>},
        {{<<"3">>, <<"\"c\"">>}, <<"125">>},
        {{<<"4">>, <<"\"d\"">>}, <<"126">>}
    ]},
    etap:is(State3, ExpectedState3, "Next state has 4 rows"),
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
    etap:is(State2_2, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, ExpectedState3, "Next state has 4 rows"),
    State4_2 = couch_view_parser:next_state(Ctx),
    etap:is(State4_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_debug_info_4_rows_and_errors() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"debug_info\": {">>,
            <<"\"local\": {\"seqs\"">>, <<": [1,2, 3,4, \"foo\"] },">>,
            <<"\"http://server">>, <<"2.com\": {\"seqs\"">>, <<": [8,null,true,false] }">>,
        <<"},">>,
        <<"\"total_r">>, <<"ows\": 100,">>,
        <<"\"rows\": [">>,
            <<"{\"key\": 1, \"id\": \"a\", \"value\": 123},">>,
            <<"{\"key\": 2, \"id\": \"b\", \"value\": 124},">>,
            <<"{\"key\": 3, \"id\": \"c\", \"value\": 125},">>,
            <<"{\"key\": 4, \"id\": \"d\", \"value\": 126}">>,
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
    etap:is(State2, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
    State3 = couch_view_parser:next_state(Ctx),
    ExpectedState3 = {ok, rows, [
        {{<<"1">>, <<"\"a\"">>}, <<"123">>},
        {{<<"2">>, <<"\"b\"">>}, <<"124">>},
        {{<<"3">>, <<"\"c\"">>}, <<"125">>},
        {{<<"4">>, <<"\"d\"">>}, <<"126">>}
    ]},
    etap:is(State3, ExpectedState3, "Next state has 4 rows"),
    State4 = couch_view_parser:next_state(Ctx),
    ExpectedState4 = {ok, errors, [
        {<<"\"http://server1.com\"">>, <<"\"timeout\"">>},
        {<<"\"http://server2.com\"">>, <<"\"reset\"">>},
        {<<"\"http://server3.com\"">>, <<"\"shutdown\"">>}
    ]},
    etap:is(State4, ExpectedState4, "Next state has 3 errors"),
    State5 = couch_view_parser:next_state(Ctx),
    etap:is(State5, {ok, done}, "Next state is {ok, done}"),

    etap:diag("Testing 4 rows result with debug info but parsing smaller chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, ExpectedState1, "Next state has debug info entries"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2, {ok, row_count, "100"}, "Next state is {ok, row_count, \"100\"}"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, ExpectedState3, "Next state has 4 rows"),
    State4_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State4_2, ExpectedState4, "Next state has 3 errors"),
    State5_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State5_2, {ok, done}, "Next state is {ok, done}"),
    ok.


test_all_docs_rows() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_r">>, <<"ows\": 100,">>,
        <<"\"rows\": [">>,
            <<"{\"id\":\"doc_001\",\"key\":\"doc_001\",\"value\":{\"rev\":\"1-1\"}},">>,
            <<"{\"key\":\"doc_002\",\"error\":\"not_found\"},">>,
            <<"{\"id\":\"doc_003\",\"key\":\"doc_003\",\"value\":{\"rev\":\"1-3\"}}">>,
        <<"]}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success parsing _all_docs response"),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, row_count, "100"}, "State1 has row count"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedState2 = {ok, rows, [
        {{<<"\"doc_001\"">>, <<"\"doc_001\"">>}, <<"{\"rev\":\"1-1\"}">>},
        {{<<"\"doc_002\"">>, error}, <<"\"not_found\"">>},
        {{<<"\"doc_003\"">>, <<"\"doc_003\"">>}, <<"{\"rev\":\"1-3\"}">>}
    ]},
    etap:is(State2, ExpectedState2, "State2 has _all_docs rows"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, done}, "State2 is {ok, done}"),

    etap:diag("Testing 4 _all_docs response but parsing smaller chunks"),
    {ok, Ctx2} = couch_view_parser:start_context(),
    lists:foreach(fun(Chunk) ->
        ok = couch_view_parser:parse_chunk(Ctx2, Chunk)
    end, Json),
    State1_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State1_2, {ok, row_count, "100"}, "State1 has row count"),
    State2_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State2_2, ExpectedState2, "State2 has _all_docs rows"),
    State3_2 = couch_view_parser:next_state(Ctx2),
    etap:is(State3_2, {ok, done}, "State2 is {ok, done}"),
    ok.


test_quotes_in_string() ->
    etap:diag("Testing double quotes inside strings are escaped"),
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_r">>, <<"ows\": 100,">>,
        <<"\"rows\": [">>,
            <<"{\"id\":\"doc_001\",\"key\":\"doc_001\",\"value\":1, \"doc\": {\"string\": \"foo \\\"bar\\\"\"}}">>,
        <<"]}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Success parsing row"),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, row_count, "100"}, "State1 has row count"),
    State2 = couch_view_parser:next_state(Ctx),
    io:format("State2 is: ~p~n", [State2]),
    ExpectedState2 = {ok, rows, [
        {{<<"\"doc_001\"">>, <<"\"doc_001\"">>}, <<"1">>, <<"{\"string\":\"foo \\\"bar\\\"\"}">>}
    ]},
    etap:is(State2, ExpectedState2, "State2 has expected row"),
    State3 = couch_view_parser:next_state(Ctx),
    etap:is(State3, {ok, done}, "State3 is {ok, done}"),
    ok.


test_MB_6013() ->
    {ok, Ctx} = couch_view_parser:start_context(),
    Json = [
        <<"{">>,
        <<"\"total_rows\": 3,\r\n">>,
        <<"\"rows\": [">>,
            <<"{\"key\": \"a\", \"id\": \"b\", \"value\": 1,">>,
                <<"\"doc\": {\"bool\": true}}">>,
        <<"]">>,
        <<"}">>
    ],
    etap:is(couch_view_parser:parse_chunk(Ctx, Json),
            ok,
            "Able to parse rows with docs"),
    State1 = couch_view_parser:next_state(Ctx),
    etap:is(State1, {ok, row_count, "3"}, "Next state matches {ok, row_count, \"3\"}"),
    State2 = couch_view_parser:next_state(Ctx),
    ExpectedRows = {ok, rows, [
        {{<<"\"a\"">>, <<"\"b\"">>}, <<"1">>, <<"{\"bool\":true}">>}
    ]},
    etap:is(State2, ExpectedRows, "Next state has rows with docs"),
    ok.
