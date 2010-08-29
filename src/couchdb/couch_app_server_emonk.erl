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

-module(couch_app_server_emonk).

-export([get_server/3, ret_server/1]).
-export([show_doc/5, validate_update/6, filter_docs/5]).
-export([list_start/5, list_row/3, list_end/2, update_doc/5]).

-include("couch_db.hrl").

-define(CACHE_NAME, emonk_app_js).

get_server(_Arg, DDoc, _DDocKey) ->
    {ok, Ctx} = emonk:create_ctx(),
    {ok, _} = emonk:eval(Ctx, get_js()),
    {ok, true} = do_call(Ctx, <<"init">>, [DDoc]),
    {ok, Ctx}.

ret_server(_) ->
    ok.

show_doc(Ctx, _DDocId, ShowName, Doc, Req) ->
    Args = [ShowName, Doc, Req],
    {ok, [<<"resp">>, Resp]} = do_call(Ctx, <<"show_doc">>, Args),
    {ok, Resp}.

list_start(Ctx, _DDocId, ListName, Head, Req) ->
    Args = [ListName, Head, Req],
    {message, [Chunks, Resp]} = do_call(Ctx, <<"list_view">>, Args),
    {ok, Chunks, Resp}.

list_row(Ctx, _DDocId, Row) ->
    case do_send(Ctx, [<<"list_row">>, Row]) of
        {message, Chunks} -> {ok, Chunks};
        {ok, Chunks} -> {stop, Chunks}
    end.

list_end(Ctx, _DDocId) ->
    {ok, Chunks} = do_send(Ctx, [<<"list_end">>]),
    {ok, Chunks}.

update_doc(Ctx, _DDocId, UpdateName, Doc, Req) ->
    Args = [UpdateName, Doc, Req],
    {ok, [NewDoc, Resp]} = do_call(Ctx, <<"update_doc">>, Args),
    {ok, NewDoc, Resp}.

validate_update(Ctx, _DDocId, EditDoc, DiskDoc, Context, SecObj) ->
    Args = [EditDoc, DiskDoc, Context, SecObj],
    {ok, Resp} = do_call(Ctx, <<"validate_update">>, Args),
    Resp.

filter_docs(Ctx, _DDocId, FilterName, Docs, Req) ->
    {ok, Passes} = do_call(Ctx, <<"filter_docs">>, [FilterName, Docs, Req]),
    {ok, Passes}.

do_call(Ctx, FName, Args) ->
    handle(Ctx, emonk:call(Ctx, FName, Args)).

do_send(Ctx, Mesg) ->
    handle(Ctx, emonk:send(Ctx, get(emonk_ref), Mesg)).

handle(Ctx, Response) ->
    %io:format("Response: ~p~n", [Response]),
    case Response of
        {ok, [<<"error">>, Id, Reason]} ->
            throw({couch_util:to_existing_atom(Id), Reason});
        {ok, [<<"fatal">>, Id, Reason]} ->
            ?LOG_INFO("Emonk Fatal Error :: ~s ~p",[Id, Reason]),
            throw({couch_util:to_existing_atom(Id), Reason});
        {ok, Resp} ->
            {ok, Resp};
        {message, Ref, [<<"log">>, Msg]} ->
            ?LOG_INFO("Emonk Log :: ~p", [Msg]),
            handle(Ctx, emonk:send(Ctx, Ref, true));
        {message, Ref, Resp} ->
            put(emonk_ref, Ref),
            {message, Resp}
    end.

get_js() ->
    case get(?CACHE_NAME) of
        undefined -> read_js();
        Script -> Script
    end.

read_js() ->
    FileName = couch_config:get(<<"app_server_emonk">>, <<"source">>),
    case file:read_file(FileName) of
        {ok, JS} ->
            put(?CACHE_NAME, JS),
            JS;
        {error, Reason} ->
            Fmt = "Failed to read file (~p): ~p",
            Mesg = ?l2b(io_lib:format(Fmt, [Reason, FileName])),
            ?LOG_ERROR(Mesg, []),
            throw({error, Reason})
    end.
