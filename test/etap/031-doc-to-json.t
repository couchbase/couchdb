#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./src/couchdb -pa ./src/mochiweb -sasl errlog_type false -noshell

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
-record(doc, {id= <<"">>, rev={0, <<>>}, json={[]},
            binary=nil, deleted=false, meta=[]}).

main(_) ->
    test_util:init_code_path(),
    etap:plan(6),
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
    couch_config:set("attachments", "compression_level", "0", false),
    ok = test_to_json_success(),
    ok.

test_to_json_success() ->
    Cases = [
        {
            #doc{},
            {[{<<"_id">>, <<"">>}]},
            "Empty docs are {\"_id\": \"\"}"
        },
        {
            #doc{id= <<"foo">>},
            {[{<<"_id">>, <<"foo">>}]},
            "_id is added."
        },
        {
            #doc{rev={5, <<0>>}},
            {[{<<"_id">>, <<>>}, {<<"_rev">>, <<"5-00">>}]},
            "_rev is added."
        },
        {
            #doc{json={[{<<"foo">>, <<"bar">>}]}},
            {[{<<"_id">>, <<>>}, {<<"foo">>, <<"bar">>}]},
            "Arbitrary fields are added."
        },
        {
            #doc{deleted=true, json={[{<<"foo">>, <<"bar">>}]}},
            {[{<<"_id">>, <<>>}, {<<"foo">>, <<"bar">>}, {<<"_deleted">>, true}]},
            "Deleted docs no longer drop body members."
        },
        {
            #doc{meta=[{local_seq, 5}]},
            {[{<<"_id">>, <<>>}, {<<"_local_seq">>, 5}]},
            "_local_seq is added as an integer."
        }
    ],

    lists:foreach(fun
        ({Doc, EJson, Mesg}) ->
            etap:is(couch_doc:to_json_obj(Doc, []), EJson, Mesg);
        ({Options, Doc, EJson, Mesg}) ->
            etap:is(couch_doc:to_json_obj(Doc, Options), EJson, Mesg)
    end, Cases),
    ok.

