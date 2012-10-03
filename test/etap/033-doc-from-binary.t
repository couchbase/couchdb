#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./src/couchdb -pa ./src/mochiweb -sasl errlog_type false -noshell -smp enable

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

%% XXX: Figure out how to -include("couch_db.hrl")
-record(doc, {id= <<"">>, rev={0, <<>>}, body= <<"{}">>,
        content_meta=0, deleted=false, meta=[]}).
-define(CONTENT_META_JSON, 0).
-define(CONTENT_META_INVALID_JSON, 1).
-define(CONTENT_META_NON_JSON_MODE, 3).

main(_) ->
    test_util:init_code_path(),
    etap:plan(7),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail()
    end,
    ok.

test() ->
    couch_config:start_link(test_util:config_files()),
    ok = test_from_binary(),
    ok.

test_from_binary() ->
    DocTpl = #doc{id = <<"testdoc">>, rev = {0, <<"">>}},
    Cases = [
        {
            {<<"{\"a\":12}">>, true},
            DocTpl#doc{body = <<"{\"a\":12}">>, content_meta = ?CONTENT_META_JSON},
            "Correctly handle JSON object"
        },
        {
            {<<"{\"_a\":12}">>, true},
            DocTpl#doc{body = <<"{\"_a\":12}">>, content_meta = ?CONTENT_META_JSON},
            "Accept formerly reserved keys in JSON"
        },
        {
            {<<"[1,2,3,true,false,null,{}]">>, true},
            DocTpl#doc{body = <<"[1,2,3,true,false,null,{}]">>,
                       content_meta = ?CONTENT_META_JSON},
            "Correctly handle JSON array"
        },
        {
            {<<"99">>, true},
            DocTpl#doc{body = <<"99">>, content_meta = ?CONTENT_META_JSON},
            "Correctly handle JSON bare values"
        },
        {
            {<<"[1,2 ,3 , true,false,null,{}]">>, true},
            DocTpl#doc{body = <<"[1,2 ,3 , true,false,null,{}]">>,
                       content_meta = ?CONTENT_META_JSON},
            "Preserve original whitespace in JSON"
        },
        {
            {<<"{\"a\":12}">>, false},
            DocTpl#doc{body = <<"{\"a\":12}">>,
                       content_meta = ?CONTENT_META_NON_JSON_MODE},
            "Don't mark things as JSON unless we ask for it"
        },
        {
            {<<"[1,2 ,3 , badjson,false,null,{}]">>, true},
            DocTpl#doc{body = <<"[1,2 ,3 , badjson,false,null,{}]">>,
                       content_meta = ?CONTENT_META_INVALID_JSON},
            "Correctly mark invalid JSON"
        }
    ],

    lists:foreach(fun({{Body, WantJSON}, Expect, Mesg}) ->
        etap:is(couch_doc:from_binary(<<"testdoc">>, Body, WantJSON), Expect, Mesg)
    end, Cases),
    ok.
