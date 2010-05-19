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

-module(couch_httpd_spatial_list).
-include("couch_db.hrl").
-include("couch_spatial.hrl").

-export([handle_spatial_list_req/3]).

-import(couch_httpd, [send_json/2, send_method_not_allowed/2,
                      send_error/4, send_chunked_error/2]).


% spatial-list request with view and list from same design doc.
handle_spatial_list_req(#httpd{method='GET',
        path_parts=[_, _, DesignName, _, ListName, SpatialName]}=Req, Db, DDoc) ->
    handle_spatial_list(Req, Db, DDoc, ListName, {DesignName, SpatialName});

% TODO vmx
%% view-list request with view and list from different design docs.
%handle_view_list_req(#httpd{method='GET',
%        path_parts=[_, _, _, _, ListName, ViewDesignName, ViewName]}=Req, Db, DDoc) ->
%    handle_view_list(Req, Db, DDoc, ListName, {ViewDesignName, ViewName}, nil);

handle_spatial_list_req(#httpd{method='GET'}=Req, _Db, _DDoc) ->
    send_error(Req, 404, <<"list_error">>, <<"Invalid path.">>);

% POST isn't supported as spatial indexes don't support mutli-key fetch
handle_spatial_list_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET,HEAD").

handle_spatial_list(Req, Db, DDoc, LName, {SpatialDesignName, SpatialName}) ->
    SpatialDesignId = <<"_design/", SpatialDesignName/binary>>,
    {ok, Index, Group} = couch_httpd_spatial:load_index(
                             Req, Db, {SpatialDesignId, SpatialName}),
    Etag = list_etag(Req, Db, Group, couch_httpd:doc_etag(DDoc)),
    couch_httpd:etag_respond(Req, Etag, fun() ->
        output_list(Req, Db, DDoc, LName, Index, Etag, Group)
    end).

list_etag(#httpd{user_ctx=UserCtx}=Req, Db, Group, More) ->
    Accept = couch_httpd:header_value(Req, "Accept"),
    couch_httpd_spatial:spatial_group_etag(
        Group, Db, {More, Accept, UserCtx#user_ctx.roles}).

output_list(Req, Db, DDoc, LName, Index, Etag, Group) ->
?LOG_DEBUG("(1) output_list: Result", []),
    % XXX vmx DON'T HARDOCDE
    Bbox = {-180,-90,180,90},
    TotalRows = 10,

    couch_query_servers:with_ddoc_proc(DDoc, fun(QServer) ->
        StartListRespFun = couch_httpd_show:make_map_start_resp_fun(
                               QServer, Db, LName),
?LOG_DEBUG("(2) output_list: Result", []),
        CurrentSeq = Group#spatial_group.current_seq,
        SendRowFun = make_spatial_get_row_fun(QServer),
?LOG_DEBUG("(3) output_list: Result", []),
        %{ok, Resp, BeginBody} = StartListRespFun(Req, Etag, [], CurrentSeq),
        {ok, Resp, BeginBody} = StartListRespFun(Req, Etag, TotalRows, null,
                                                 [], CurrentSeq),
?LOG_DEBUG("(4) output_list: Result", []),
        {ok, Result} = couch_spatial:do_bbox_search(Bbox, Group, Index, SendRowFun),
?LOG_DEBUG("output_list: Result: ~p", [Result]),
%        finish_list(Req, QServer, Etag, Result, Resp, CurrentSeq)
        couch_httpd_show:send_non_empty_chunk(Resp, Result),
        {Proc, _DDocId} = QServer,
        [<<"end">>, Chunks] = couch_query_servers:proc_prompt(Proc,
                                                          [<<"list_end">>]),
?LOG_DEBUG("(5) output_list: Result: ~p", [Chunks]),
        Chunk = BeginBody ++ ?b2l(?l2b(Chunks)),
        couch_httpd_show:send_non_empty_chunk(Resp, Chunk),
        couch_httpd:last_chunk(Resp)
    end).

%finish_list(Req, {Proc, _DDocId}, Etag, Result, Resp, CurrentSeq) ->
%    [<<"end">>, Chunks] = couch_query_servers:proc_prompt(Proc,
%                                                          [<<"list_end">>]),
%    Chunk = BeginBody ++ ?b2l(?l2b(Chunks)),
%    couch_http_show:send_non_empty_chunk(Resp, Chunk),
%    couch_httpd:last_chunk(Resp).

% Counterpart to make_spatial_send_row_fun/1 in couch_http_show. The difference
% is that there no direct output, but it returns the result as list
make_spatial_get_row_fun(QueryServer) ->
    fun({_Bbox, _DocId, _Value}=Row, Acc) ->
        [Go, Chunks] = prompt_list_row(QueryServer, Row),
        %Acc ++ prompt_list_row(QueryServer, Row)
        Acc ++ Chunks
    end.
%    fun(Resp, Row, RowFront) ->
%        get_list_row(Resp, QueryServer, Row, RowFront)
%    end.

get_list_row(Resp, QueryServer, Row, RowFront) ->
    try
        [_Go, Chunks] = prompt_list_row(QueryServer, Row),
        Chunk = RowFront ++ ?b2l(?l2b(Chunks))
%        couch_http_show:send_non_empty_chunk(Resp, Chunk),
%        case Go of
%            <<"chunks">> ->
%                {ok, ""};
%            <<"end">> ->
%                {stop, stop}
%        end
    catch
        throw:Error ->
            send_chunked_error(Resp, Error),
            throw({already_sent, Resp, Error})
    end.

%prompt_list_row({Proc, _DDocId}, Db, {{Key, DocId}, Value}, IncludeDoc) ->
%    JsonRow = couch_httpd_view:view_row_obj(Db, {{Key, DocId}, Value}, IncludeDoc),
%    couch_query_servers:proc_prompt(Proc, [<<"list_row">>, JsonRow]).

%prompt_list_row({Proc, _DDocId}, {Bbox, _DocId, Value}) ->
%?LOG_DEBUG("(4) prompt_list_row: Value ~p", [Value]),
%    JsonRow = {[{key, tuple_to_list(Bbox)}, {value, Value}]},
%    couch_query_servers:proc_prompt(Proc, [<<"list_row">>, JsonRow]).
prompt_list_row({Proc, _DDocId}, {Bbox, DocId, Value}) ->
?LOG_DEBUG("(4) prompt_list_row: Value ~p", [Value]),
    JsonRow = {[{id, DocId}, {key, tuple_to_list(Bbox)}, {value, Value}]},
    couch_query_servers:proc_prompt(Proc, [<<"list_row">>, JsonRow]).
