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

-module(couch_app_server).

-export([show_doc/5, validate_update/5, filter_docs/5]).
-export([list_view/2, list_start/5, list_row/5, list_end/1, update_doc/5]).

-include("couch_db.hrl").

show_doc(Req, Db, DDoc, ShowName, Doc) ->
    {Req2, Doc2} = mk_json(Req, Db, Doc),
    with_server(DDoc, fun({Module, Server, DDocId}) ->
        Module:show_doc(Server, DDocId, ShowName, Doc2, Req2)
    end).

list_view(DDoc, Fun) ->
    with_server(DDoc, Fun).

list_start({Module, Server, DDocId}, Req, Db, ListName, Head) ->
    Module:list_start(Server, DDocId, ListName, Head, json_req(Req, Db)).

list_row({Module, Server, DDocId}, Db, {{_, _}, _} = Row, IncludeDoc, Conflicts) ->
    JsonRow = couch_httpd_view:view_row_obj(Db, Row, IncludeDoc, Conflicts),
    Module:list_row(Server, DDocId, JsonRow);
list_row({Module, Server, DDocId}, _Db, {Key, Value}, _IncludeDoc, _Conflicts) ->
    JsonRow = {[{key, Key}, {value, Value}]},
    Module:list_row(Server, DDocId, JsonRow).

list_end({Module, Server, DDocId}) ->
    Module:list_end(Server, DDocId).

update_doc(Req, Db, DDoc, UpdateName, Doc) ->
    {Req2, Doc2} = mk_json(Req, Db, Doc),
    with_server(DDoc, fun({Module, Server, DDocId}) ->
        Module:update_doc(Server, DDocId, UpdateName, Doc2, Req2)
    end).

validate_update(DDoc, EditDoc, DiskDoc, Ctx, SecObj) ->
    EditDoc2 = couch_doc:to_json_obj(EditDoc, [revs]),
    DiskDoc2 = couch_util:json_doc(DiskDoc),
    Fun = fun({Module, Server, DDocId}) ->
        Module:validate_update(Server, DDocId, EditDoc2, DiskDoc2, Ctx, SecObj)
    end,
    case (catch with_server(DDoc, Fun)) of
        1 -> ok;
        {[{<<"forbidden">>, Message}]} -> throw({forbidden, Message});
        {[{<<"unauthorized">>, Message}]} -> throw({unauthorized, Message});
        Other -> io:format("OTHER: ~p~n", [Other]), throw(Other)
    end.

filter_docs(Req, Db, DDoc, FilterName, Docs) ->
    Docs2 = [couch_util:json_doc(Doc) || Doc <- Docs],
    with_server(DDoc, fun({Module, Server, DDocId}) ->
        Module:filter_docs(Server, DDocId, FilterName, Docs2, json_req(Req, Db))
    end).

% Private API

json_req({json_req, JsonObj}, _Db) -> JsonObj;
json_req(Req, Db) -> couch_httpd_external:json_req_obj(Req, Db).

mk_json({json_req, JsonObj}, _Db, Doc) ->
    {JsonObj, couch_util:json_doc(Doc)};
mk_json(Req, Db, Doc) ->
    JsonReq = couch_httpd_external:json_req_obj(Req, Db),
    {JsonReq, couch_util:json_doc(Doc)}.

with_server(#doc{id=DDocId, revs={Start, [DiskRev | _]}}=DDoc, Fun) ->
    Rev = couch_doc:rev_to_str({Start, DiskRev}),
    DDocKey = {DDocId, Rev},
    {Module, Server} = get_server(DDoc, DDocKey),
    try
        Fun({Module, Server, DDocId})
    after
        ok = ret_server({Module, Server})
    end.

get_server(#doc{body={Props}} = DDoc, DDocKey) ->
    Lang = couch_util:get_value(<<"language">>, Props, <<"javascript">>),
    {Module, Arg} = get_module_arg(Lang),
    JsonDDoc = couch_util:json_doc(DDoc),
    {ok, Server} = Module:get_server(Arg, JsonDDoc, DDocKey),
    {Module, Server}.

get_module_arg(Lang) ->
    case couch_config:get(<<"app_servers">>, Lang) of
        undefined ->
            throw({error, unknown_view_language, Lang});
        TermStr ->
            {ok, {Module, Arg}} = couch_util:parse_term(TermStr),
            {Module, Arg}
    end.

ret_server(nil) ->
    ok;
ret_server({Module, Server}) ->
    Module:ret_server(Server).

