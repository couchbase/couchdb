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
-record(doc, {id= <<"">>, rev={0, <<>>}, body={[]},
        content_meta=0, deleted=false, meta=[]}).

main(_) ->
    test_util:init_code_path(),
    etap:plan(10*2 - 2),
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
    ok = test_to_binary_success(),
    ok.

test_to_binary_success() ->
    Cases = [
        {
            #doc{},
           '{"meta":{"id":""},"json":{}}',
            "Empty docs have empty string as id."
        },
        {
            #doc{id= <<"foo">>},
            '{"meta":{"id":"foo"},"json":{}}',
            "id is added to meta."
        },
                {
            #doc{id= <<255,23>>},
            '{"meta":{"id":[255,23]},"json":{}}',
            "id can be raw binary"
        },
        {
            #doc{rev={5, <<0>>}},
            '{"meta":{"id":"","rev":"5-00"},"json":{}}',
            "rev is added to meta."
        },
        {
            #doc{body={[{<<"_foo">>, <<"bar">>}]}},
            '{"meta":{"id":""},"json":{"_foo":"bar"}}',
            "Arbitrary fields are added to body."
        },
        { % ?CONTENT_META_JSON == 0
            #doc{content_meta=0, body= <<"{\"foobar\":true}">>},
            '{"meta":{"id":""},"json":{"foobar":true}}',
            "Body can be JSON stored as binary."
        },
        { % ?CONTENT_META_JSON == 0
            #doc{content_meta=1, body= <<1,2,3>>},
            '{"meta":{"id":"","att_reason":"invalid_json"},"base64":"AQID"}',
            "Body can be raw binary.", read_only
        },
        {
            #doc{content_meta=0, deleted=true, body= <<"{\"foobaz\":true}">>},
            '{"meta":{"id":"","deleted":true},"json":{"foobaz":true}}',
            "Deleted docs no longer drop body members."
        },
        {
            #doc{content_meta=0, deleted=true, body= <<"{}">>},
            '{"meta":{"id":"","deleted":true}}',
            "Deleted docs without body members.", empty_body
        },
        {
            #doc{meta=[{local_seq, 5}]},
            '{"meta":{"id":"","local_seq":5},"json":{}}',
            "Local_seq is added as an integer.", read_only
        }
    ],

    lists:foreach(fun
        ({Doc, DocJSON, Mesg}) ->
            read_assertions(Doc, DocJSON, Mesg),
            write_assertions(Doc, DocJSON, Mesg);
        ({Doc, DocJSON, Mesg, empty_body}) ->
            empty_body_assertions(Doc, DocJSON, Mesg);
        ({Doc, DocJSON, Mesg, read_only}) ->
            read_assertions(Doc, DocJSON, Mesg)
    end, Cases),
    ok.

read_assertions(Doc, DocJSON, Mesg) ->
    % if I make the #doc into json, do I get the JSON string
    WantJSON = atom_to_binary(DocJSON, utf8),
    ToBinJSON = couch_doc:to_json_bin(Doc),
    etap:is(ToBinJSON, WantJSON, Mesg ++ " to_json_bin").

empty_body_assertions(_Doc, DocJSON, Mesg) ->
    WantJSON0 = atom_to_binary(DocJSON, utf8),
    {WantEJSON} = ejson:decode(WantJSON0),
    ParsedDoc = couch_doc:from_json_obj({WantEJSON}),
    {HaveEJSON0} = couch_doc:to_json_obj(ParsedDoc),
    etap:is({[]}, couch_util:get_value(<<"json">>, HaveEJSON0, undefined), "empty"),
    HaveEJSON = {[{<<"meta">>,couch_util:get_value(<<"meta">>, HaveEJSON0)}]},
    HaveJSON = ejson:encode(HaveEJSON),
    etap:is(HaveJSON, WantJSON0, Mesg ++ " empty").

write_assertions(_Doc, DocJSON, Mesg) ->
    WantJSON = atom_to_binary(DocJSON, utf8),
    % if I make Doc, and back into JSON, it is the same.
    ParsedDoc = couch_doc:from_json_obj(ejson:decode(WantJSON)),
    etap:diag("ParsedDoc ~p", [ParsedDoc]),
    HaveJSON = ejson:encode(couch_doc:to_json_obj(ParsedDoc)),
    etap:diag("HaveJSON ~s", [HaveJSON]),
    etap:diag("WantJSON ~s", [WantJSON]),
    etap:is(HaveJSON, WantJSON, Mesg ++ " json").
