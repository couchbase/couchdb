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

-module(couch_app_server_objc).

-export([get_server/3, ret_server/1]).
-export([show_doc/5, validate_update/6, filter_docs/5, filter_view/4]).
-export([list_start/5, list_row/3, list_end/2, update_doc/5]).

-include("couch_db.hrl").

get_server(_Arg, DDoc, DDocKey) ->
    objc_dispatch:app_create(DDoc, DDocKey).

ret_server(_) ->
    ok.

show_doc(Ctx, DDocId, ShowName, Doc, Req) ->
    objc_dispatch:app_show_doc(Ctx, DDocId, ShowName, Doc, Req).

list_start(Ctx, DDocId, ListName, Head, Req) ->
    {message, [Chunks, Resp]} = objc_dispatch:app_list_start(Ctx, DDocId, ListName, Head, Req),
    {ok, Chunks, Resp}.

list_row(Ctx, DDocId, Row) ->
    objc_dispatch:app_list_row(Ctx, DDocId, Row).

list_end(Ctx, DDocId) ->
    objc_dispatch:app_list_end(Ctx, DDocId).

update_doc(Ctx, DDocId, UpdateName, Doc, Req) ->
    objc_dispatch:app_update_doc(Ctx, DDocId, UpdateName, Doc, Req).

validate_update(Ctx, DDocId, EditDoc, DiskDoc, Context, SecObj) ->
    objc_dispatch:app_validate_update(Ctx, DDocId, EditDoc, DiskDoc, Context, SecObj).

filter_docs(Ctx, DDocId, FilterName, Docs, Req) ->
    objc_dispatch:app_filter_docs(Ctx, DDocId, FilterName, Docs, Req).

filter_view(Ctx, DDocId, ViewName, Docs) ->
    objc_dispatch:app_filter_view(Ctx, DDocId, ViewName, Docs).
