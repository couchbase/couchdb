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
    etap:plan(10*3-2),
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
            '{"meta":{"id":""},"json":{}}',
            "Empty docs have {\"id\": \"\"}"
        },
        {
            #doc{id= <<"foo">>},
            '{"meta":{"id":"foo"},"json":{}}',
            "_id is added."
        },
        {
            #doc{id= <<255,255,99>>},
            '{"meta":{"id":[255,255,99]},"json":{}}',
            "_id is non-UTF8."
        },
        {
            #doc{rev={5, <<0>>}},
            '{"meta":{"id":"","rev":"5-00"},"json":{}}',
            "_rev is added."
        },
        {
            #doc{body={[{<<"foo">>, <<"bar">>}]}},
            '{"meta":{"id":""},"json":{"foo":"bar"}}',
            "Arbitrary fields are added."
        },
        {
            #doc{content_meta=0, deleted=true, body= <<"{\"foobar\":true}">>},
            '{"meta":{"id":"","deleted":true},"json":{"foobar":true}}',
            "Body can be json binary"
        },
        { % ?CONTENT_META_JSON == 0
            #doc{content_meta=3, body= <<255,23>>},
            '{"meta":{"id":"","att_reason":"non-JSON mode"},"base64":\"/xc=\"}',
            "Body can be raw binary", read_only
        },
        {
            #doc{deleted=true, body={[{<<"foo">>, <<"bar">>}]}},
            '{"meta":{"id":"","deleted":true},"json":{"foo":"bar"}}',
            "Deleted docs no longer drop body members."
        },
        {
            #doc{deleted=true, body= <<"{\"foobar\":true}">>},
            '{"meta":{"id":"","deleted":true},"json":{"foobar":true}}',
            "Delete with bin json"
        },
        {
            #doc{meta=[{local_seq, 5}]},
            '{"meta":{"id":"","local_seq":5},"json":{}}',
            "local_seq is added as an integer.", read_only
        }
    ],

    lists:foreach(fun
        ({Doc, DocJSON, Mesg}) ->
            read_assertions(Doc, DocJSON, Mesg),
            write_assertions(Doc, DocJSON, Mesg);
        ({Doc, DocJSON, Mesg, read_only}) ->
            read_assertions(Doc, DocJSON, Mesg)
    end, Cases),
    ok.

read_assertions(Doc, DocJSON, Mesg) ->
    WantJSON = atom_to_binary(DocJSON, utf8),
    DocEJSON = couch_doc:to_json_obj(Doc, []),
    DocDJSON = ejson:encode(DocEJSON),
    etap:is(DocDJSON, WantJSON, Mesg),
    ToBinJSON = couch_doc:to_json_bin(Doc),
    etap:is(DocDJSON, ToBinJSON, Mesg).

write_assertions(Doc, DocJSON, Mesg) ->
    WantJSON = atom_to_binary(DocJSON, utf8),
    WantEJSON = ejson:decode(WantJSON),
    ParsedDoc = couch_doc:from_json_obj(WantEJSON),
    ParsedDocJSON = ejson:encode(couch_doc:to_json_obj(ParsedDoc, [])),
    etap:is(ParsedDocJSON, WantJSON, Mesg ++ " parsed").
