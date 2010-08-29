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

-module(couch_app_server_os).

show_doc(Req, Db, DDoc, ShowName, Doc) ->
    FunPath = [<<"shows">>, ShowName],
    Args = [couch_doc:to_json_obj(Doc, [revs]), get_json_req(Req, Db)],
    [<<"resp">>, ExternalResp] = ddoc_prompt(DDoc, FunPath, Args),
    {ok, ExternalResp}.

list_view(DDoc, Fun) ->
    with_server(DDoc, Fun).

list_start(Server, Req, Db, ListName, Head) ->
    with_server()
    FunPath = [<<"lists">>, ListName],
    Args = [Head, get_json_req(Req, Db)],
    [<<"start">>, Chunks, JsonResp] = server_prompt(Server, FunPath, Args),
    {ok, Chunks, JsonResp}.

list_row(Server, {Key, Value}, _IncludeDoc) ->
    JsonRow = {[{key, Key}, {value, Value}]},
    list_row(Server, JsonRow);
list_row(Server, {{_, _}, _} = Row, IncludeDoc) ->
    JsonRow = couch_httpd_view:view_row_obj(Db, Row, IncludeDoc),
    list_row(Server, JsonRow).

list_row(Server, JsonRow) ->
    [Go, Chunks] = server_prompt(Server, [<<"list_row">>, JsonRow]),
    {ok, Go, Chunks}.

list_end(Server) ->
    [<<"end">>, Chunks] = server_prompt(Server, [<<"list_end">>]),
    {ok, Chunks}.

update_doc(Req, Db, DDoc, UpdateName, Doc) ->
    FunPath = [<<"updates">>, UpdateName],
    Args = [couch_doc:to_json_obj(Doc, [revs]), get_json_req(Req, Db)],
    [<<"up">>, NewDoc, Resp] = ddoc_prompt(DDoc, FunPath, Args),
    {ok, NewDoc, Resp}.

validate_doc_update(DDoc, EditDoc, DiskDoc, Ctx, SecObj) ->
    JsonEditDoc = couch_doc:to_json_obj(EditDoc, [revs]),
    JsonDiskDoc = json_doc(DiskDoc),
    FunPath = [<<"validate_doc_update">>],
    Args = [JsonEditDoc, JsonDiskDoc, Ctx, SecObj],
    case ddoc_prompt(DDoc, FunPath, Args) of
        1 ->
            ok;
        {[{<<"forbidden">>, Message}]} ->
            throw({forbidden, Message});
        {[{<<"unauthorized">>, Message}]} ->
            throw({unauthorized, Message})
    end.

filter_docs(Req, Db, DDoc, FName, Docs) ->
    FunPath = [<<"filters">>, FName],
    JsonDocs = [couch_doc:to_json_obj(Doc, [revs]) || Doc <- Docs],
    Args = [JsonDocs, get_json_req(Req, Db)],
    [true, Passes] = ddoc_prompt(DDoc, FunPath, Args),
    {ok, Passes}.

