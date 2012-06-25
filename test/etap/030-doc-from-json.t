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

main(_) ->
    test_util:init_code_path(),
    etap:plan(21),
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
    ok = test_from_json_success(),
    ok = test_from_json_errors(),
    ok.

test_from_json_success() ->
    Cases = [
        {
            {[]},
            #doc{},
            "Return an empty document for an empty JSON object."
        },
        {
            {[{<<"meta">>, {[{<<"id">>, <<"zing!">>}]}}]},
            #doc{id= <<"zing!">>},
            "Parses document ids."
        },
        {
            {[{<<"meta">>, {[{<<"id">>, <<"_foo">>}]}}]},
            #doc{id= <<"_foo">>},
            "_underscore ids."
        },
        {
            {[{<<"meta">>, {[{<<"id">>, <<"_design/foo">>}]}}]},
            #doc{id= <<"_design/foo">>},
            "_design/document ids."
        },
        {
            {[{<<"meta">>, {[{<<"id">>, <<"_local/bam">>}]}}]},
            #doc{id= <<"_local/bam">>},
            "_local/document ids."
        },
        {
            {[{<<"meta">>, {[{<<"rev">>, <<"4-1111">>}]}}]},
            #doc{rev={4, <<17,17>>}},
            "_rev stored in revs."
        },
        {
            {[{<<"json">>, {[{<<"soap">>, 35}]}},{<<"meta">>, {[{<<"id">>, <<"foo">>}]}}]},
            #doc{id= <<"foo">>, body= <<"{\"soap\":35}">>},
            "Non meta fields stored in body."
        },
        {
            {[{<<"json">>, {[{<<"_soap">>, 35}]}}]},
            #doc{body= <<"{\"_soap\":35}">>},
            "Underscore fields are legal."
        },
        {
            {[{<<"meta">>, {[{<<"deleted">>, true}]}}]},
            #doc{deleted=true},
            "_deleted controls the deleted field."
        },
        {
            {[{<<"meta">>, {[{<<"deleted">>, false}]}}]},
            #doc{},
            "{\"_deleted\": false} is ok."
        },
        {
            {[{<<"meta">>, {[{<<"revs_info">>, dropping}]}}]},
            #doc{},
            "Drops _revs_info."
        },
        {
            {[{<<"meta">>, {[{<<"local_seq">>, dropping}]}}]},
            #doc{},
            "Drops _local_seq."
        },
        {
            {[{<<"meta">>, {[{<<"conflicts">>, dropping}]}}]},
            #doc{},
            "Drops _conflicts."
        },
        {
            {[{<<"meta">>, {[{<<"deleted_conflicts">>, dropping}]}}]},
            #doc{},
            "Drops _deleted_conflicts."
        }
    ],

    lists:foreach(fun({EJson, Expect, Mesg}) ->
        etap:is(couch_doc:from_json_obj(EJson), Expect, Mesg)
    end, Cases),
    ok.

test_from_json_errors() ->
    Cases = [
        {
            [],
            {bad_request, "Document must be a JSON object"},
            "arrays are invalid"
        },
        {
            4,
            {bad_request, "Document must be a JSON object"},
            "integers are invalid"
        },
        {
            true,
            {bad_request, "Document must be a JSON object"},
            "literals are invalid"
        },
        {
            {[{<<"meta">>, {[{<<"id">>, {[{<<"foo">>, 5}]}}]}}]},
            {bad_request, <<"Document id must be a string">>},
            "Document id must be a string."
        },
        {
            {[{<<"meta">>, {[{<<"rev">>, 5}]}}]},
            {bad_request, <<"Invalid rev format">>},
            "_rev must be a string"
        },
        {
            {[{<<"meta">>, {[{<<"rev">>, "foobar"}]}}]},
            {bad_request, <<"Invalid rev format">>},
            "_rev must be %d-%s"
        },
        {
            {[{<<"meta">>, {[{<<"rev">>, "foo-bar"}]}}]},
            "Error if _rev's integer expection is broken."
        }
    ],

    lists:foreach(fun
        ({EJson, Expect, Mesg}) ->
            Error = (catch couch_doc:from_json_obj(EJson)),
            etap:is(Error, Expect, Mesg);
        ({EJson, Mesg}) ->
            try
                couch_doc:from_json_obj(EJson),
                etap:ok(false, "Conversion failed to raise an exception.")
            catch
                _:_ -> etap:ok(true, Mesg)
            end
    end, Cases),
    ok.
