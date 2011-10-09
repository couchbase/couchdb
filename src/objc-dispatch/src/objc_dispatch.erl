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
-module(objc_dispatch).
-on_load(init/0).

-export([create/3, query_view_version/1]).
-export([map/2, map/3, reduce/5, reduce/6]).
-export([app_create/2]).
-export([app_show_doc/4, app_validate_update/6, app_filter_docs/5, app_filter_view/4]).
-export([app_list_start/5, app_list_row/3, app_list_end/2, app_update_doc/5]).


-define(APPNAME, objc_dispatch).
-define(LIBNAME, objc_dispatch).
-define(TIMEOUT, 5000).


create(Name, MapKeys, RedKeys) ->
    create_nif(Name, MapKeys, RedKeys).

query_view_version(MapKey) ->
    {ok, Vers} = query_view_version_nif(MapKey),
    Vers.

map(QueueRef, DocJson) ->
    map(QueueRef, DocJson, ?TIMEOUT).
map(QueueRef, DocJson, Timeout) ->
    nonblocking_call(fun(ReplyRef) -> map_nif(QueueRef, ReplyRef, DocJson) end, Timeout).

reduce(QueueRef, RedIndexes, KeysJson, ValsJson, Rereduce) ->
    reduce(QueueRef, RedIndexes, KeysJson, ValsJson, Rereduce, ?TIMEOUT).
reduce(QueueRef, RedIndexes, KeysJson, ValsJson, Rereduce, Timeout) ->
    nonblocking_call(fun(ReplyRef) -> reduce_nif(QueueRef, ReplyRef, RedIndexes, KeysJson, ValsJson, Rereduce) end, Timeout).

app_create(DDoc, DDocKey) ->
    app_create_nif(DDoc, DDocKey).
app_show_doc(Ctx, ShowName, Doc, Req) ->
    app_show_doc_nif(Ctx, ShowName, Doc, Req).
app_list_start(Ctx, DDocId, ListName, Head, Req) ->
    app_list_start_nif(Ctx, DDocId, ListName, Head, Req).
app_list_row(Ctx, DDocId, Row) ->
    app_list_row_nif(Ctx, DDocId, Row).
app_list_end(Ctx, DDocId) ->
    app_list_end_nif(Ctx, DDocId).
app_update_doc(Ctx, DDocId, UpdateName, Doc, Req) ->
    app_update_doc_nif(Ctx, DDocId, UpdateName, Doc, Req).
app_validate_update(Ctx, DDocId, EditDoc, DiskDoc, Context, SecObj) ->
    app_validate_update_nif(Ctx, DDocId, EditDoc, DiskDoc, Context, SecObj).
app_filter_docs(Ctx, DDocId, FilterName, Docs, Req) ->
    app_filter_docs_nif(Ctx, DDocId, FilterName, Docs, Req).
app_filter_view(Ctx, DDocId, ViewName, Docs) ->
    app_filter_view_nif(Ctx, DDocId, ViewName, Docs).



%% Internal

init() ->
    erlang:load_nif("objc_dispatch", 0).

nonblocking_call(NifCall, Timeout) ->
    ReplyRef = make_ref(),
    ok = NifCall(ReplyRef),
    receive
        {ReplyRef, Response} ->
            Response
    after Timeout ->
        exit({error, timeout, ReplyRef})
    end.


not_loaded(Line) ->
    exit({not_loaded, [{module, ?MODULE}, {line, Line}]}).

create_nif(Name, MapKeys, RedKeys) ->
    not_loaded(?LINE).

query_view_version_nif(MapKey) ->
    not_loaded(?LINE).

map_nif(QueueRef, ReplyRef, DocJson) ->
    not_loaded(?LINE).

reduce_nif(QueueRef, ReplyRef, RedIndexes, KeysJson, ValsJson, Rereduce) ->
    not_loaded(?LINE).

app_create_nif(DDoc, DDocKey) ->
    not_loaded(?LINE).
app_show_doc_nif(Ctx, ShowName, Doc, Req) ->
    not_loaded(?LINE).
app_list_start_nif(Ctx, DDocId, ListName, Head, Req) ->
    not_loaded(?LINE).
app_list_row_nif(Ctx, DDocId, Row) ->
    not_loaded(?LINE).
app_list_end_nif(Ctx, DDocId) ->
    not_loaded(?LINE).
app_update_doc_nif(Ctx, DDocId, UpdateName, Doc, Req) ->
    not_loaded(?LINE).
app_validate_update_nif(Ctx, DDocId, EditDoc, DiskDoc, Context, SecObj) ->
    not_loaded(?LINE).
app_filter_docs_nif(Ctx, DDocId, FilterName, Docs, Req) ->
    not_loaded(?LINE).
app_filter_view_nif(Ctx, DDocId, ViewName, Docs) ->
    not_loaded(?LINE).
