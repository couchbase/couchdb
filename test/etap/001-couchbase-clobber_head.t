#!/usr/bin/env escript
%% -*- erlang -*-

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

-record(doc,
    {
    id = <<"">>,
    revs = {0, []},

    % the json body object.
    body = {[]},

    atts = [], % attachments

    deleted = false,

    % key/value tuple of meta information, provided when using special options:
    % couch_db:open_doc(Db, Id, Options).
    meta = []
}).

-record(doc_info,
    {
    id = <<"">>,
    high_seq = 0,
    revs = [] % rev_info
}).

-record(rev_info,
    {
    rev,
    seq = 0,
    deleted = false,
    body_sp = nil % stream pointer
    }).

-record(user_ctx, {
    name = null,
    roles = [],
    handler
}).

test_db_name() -> <<"couch_test_clobber_head">>.

main(_) ->
    test_util:init_code_path(),

    etap:plan(18),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.

test() ->
    couch_server_sup:start_link(test_util:config_files()),
    timer:sleep(1000),

    delete_db(),
    {ok, Db} = create_db(),
    etap:diag("Created test database", []),

    Doc1 = test_doc(<<"doc1">>, 1),
    DocCreateResult = couch_db:update_doc(Db, Doc1, []),
    ?etap_match(DocCreateResult, {ok, _}, "Created test document doc1"),

    RevId2 = iolist_to_binary(
        [integer_to_list(2), "-", "10000000000000000000000000000000"]),
    Doc2 = test_doc(<<"doc1">>, 2, RevId2),
    DocUpdateResult2 = (catch couch_db:update_doc(Db, Doc2, [clobber])),
    ?etap_match(DocUpdateResult2, {ok, _}, "Updated test document doc1"),

    {ok, Db2} = couch_db:open_int(test_db_name(), []),
    {ok, Rev1} = DocUpdateResult2,
    {ok, [OpenRev1Result]} = couch_db:open_doc_revs(Db2, <<"doc1">>, [Rev1], [ejson_body]),
    ?etap_match(OpenRev1Result, {ok, _}, "clobber update returns correct revision"),
    {ok, ReadDoc2} = couch_db:open_doc(Db2, <<"doc1">>, [ejson_body]),
    {ok, ReadDoc2ByRev} = OpenRev1Result,
    etap:is(ReadDoc2, ReadDoc2ByRev, "open by rev and by id give same result"),
    {Props2} = couch_doc:to_json_obj(ReadDoc2, []),
    etap:is(2, couch_util:get_value(<<"value">>, Props2), "doc.value is 2"),

    RevId3 = iolist_to_binary(
        [integer_to_list(3), "-", "10000000000000000000000000000001"]),
    Doc3 = test_doc(<<"doc1">>, 3, RevId3),
    DocUpdateResult3 = (catch couch_db:update_doc(Db, Doc3, [clobber])),
    ?etap_match(DocUpdateResult3, {ok, _}, "Updated test document doc1 again"),

    {ok, Db3} = couch_db:open_int(test_db_name(), []),
    {ok, ReadDoc3} = couch_db:open_doc(Db3, <<"doc1">>, [ejson_body]),
    {Props3} = couch_doc:to_json_obj(ReadDoc3, []),
    etap:is(3, couch_util:get_value(<<"value">>, Props3), "doc.value is 3"),

    % Introduce a conflict, clobber should use the winning revision.
    {ok, Db4} = couch_db:open_int(test_db_name(), []),
    {ok, #doc{revs = {Pos, _}}} = couch_db:open_doc(Db4, <<"doc1">>, [ejson_body]),
    etap:is(3, Pos, "Current document revision position is 3"),

    BogusRev = iolist_to_binary(
        [integer_to_list(Pos), "-", "aaaaaaaac40895de23f479daaaaaaaaa"]),
    Doc4 = test_doc(<<"doc1">>, 666, BogusRev),
    DocUpdateResult4 = couch_db:update_doc(Db4, Doc4, [], replicated_changes),
    ?etap_match(DocUpdateResult4, {ok, {3, _}}, "Added conflict version of document doc1"),

    {ok, Db5} = couch_db:open_int(test_db_name(), []),
    {ok, #doc_info{revs = Revs}} = couch_db:get_doc_info(Db5, <<"doc1">>),
    etap:is(2, length(Revs), "Document doc1 has 2 leaf revisions"),
    [#rev_info{rev = {3, WinRev}}, #rev_info{rev = {3, LoseRev}}] = Revs,

    Doc5 = test_doc(<<"doc1">>, 999),
    DocUpdateResult5 = (catch couch_db:update_doc(Db5, Doc5, [clobber])),
    ?etap_match(DocUpdateResult5, {ok, _}, "Updated test document doc1 again"),

    {ok, Db6} = couch_db:open_int(test_db_name(), []),
    {ok, ReadDoc6} = couch_db:open_doc(Db6, <<"doc1">>, [ejson_body, revs, conflicts]),
    #doc{body = {Props6}, revs = {Pos6, RevIds6}, meta = Meta6} = ReadDoc6,
    etap:is(999, couch_util:get_value(<<"value">>, Props6),
        "Updated test document doc1 again"),
    etap:is(4, Pos6, "Current document revision position is 4"),
    [_CurrentRev6, ParentRev6 | _] = RevIds6,
    etap:is(WinRev, ParentRev6, "Clobber used the winning revision"),
    Conflicts6 = couch_util:get_value(conflicts, Meta6),
    etap:is(Conflicts6, [{3, LoseRev}],
        "Conflict revision remains the same after last clobber update"),

    % Test clobber of local docs.
    LocalDoc = test_doc(<<"_local/foo">>, 1),
    LocalDocCreateResult = couch_db:update_doc(Db6, LocalDoc, []),
    ?etap_match(LocalDocCreateResult, {ok, _}, "Created test local document"),

    LocalDoc2 = test_doc(<<"_local/foo">>, 2),
    try
        couch_db:update_doc(Db6, LocalDoc2, []),
        etap:bail("Expected conflict when updating local document")
    catch throw:conflict ->
        etap:diag("Got conflict when updating local document")
    end,

    LocalDocCreateResult2 = (catch couch_db:update_doc(Db6, LocalDoc2, [clobber])),
    ?etap_match(LocalDocCreateResult2, {ok, _}, "Updated test local document"),

    {ok, Db7} = couch_db:open_int(test_db_name(), []),
    {ok, #doc{body = {LocalProps}}} = couch_db:open_doc(
        Db7, <<"_local/foo">>, [ejson_body]),
    etap:is(couch_util:get_value(<<"value">>, LocalProps), 2,
        "Latest local document revision has value 2 in the body"),

    couch_db:close(Db),
    couch_db:close(Db2),
    couch_db:close(Db3),
    couch_db:close(Db4),
    couch_db:close(Db5),
    couch_db:close(Db6),
    couch_db:close(Db7),
    delete_db(),
    couch_server_sup:stop(),
    ok.

admin_user_ctx() ->
    {user_ctx, #user_ctx{roles = [<<"_admin">>]}}.

create_db() ->
    {ok, _Db} = couch_db:create(test_db_name(), [admin_user_ctx()]).

delete_db() ->
    couch_server:delete(test_db_name(), [admin_user_ctx()]).

test_doc(Id, Value) ->
    test_doc(Id, Value, nil).

test_doc(Id, Value, Rev) ->
    Base = [{<<"_id">>, Id}, {<<"value">>, Value}],
    case Rev of
    nil ->
        couch_doc:from_json_obj({Base});
    _ when is_binary(Rev) ->
        couch_doc:from_json_obj({[{<<"_rev">>, Rev} | Base]})
    end.
