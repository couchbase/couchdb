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

-module(couch_http_view_streamer).

-include("couch_db.hrl").

-export([parse/3]).


parse(DataFun, Queue, FromUrl) ->
    {ok, Ctx} = couch_view_parser:start_context(),
    JsonUrl = ?JSON_ENCODE(FromUrl),
    ok = stream_loop(Ctx, Queue, DataFun, JsonUrl).


stream_loop(Ctx, Queue, DataFun, Url) ->
    case next_streamer_state(Ctx, DataFun) of
    {ok, debug_infos, _DebugInfos} ->
        % TODO, currently broken for reduce views.
        % View response only gets the debug_info for the merger (local) node.
        stream_loop(Ctx, Queue, DataFun, Url);
    {ok, row_count, TotalRowsList} ->
        TotalRows = list_to_integer(TotalRowsList),
        ok = couch_view_merger_queue:queue(Queue, {row_count, TotalRows}),
        stream_loop(Ctx, Queue, DataFun, Url);
    {ok, rows, Rows} ->
        lists:foreach(
            fun(Row) ->
               ok = couch_view_merger_queue:queue(Queue, transform_row(Row, Url))
            end,
            Rows),
        stream_loop(Ctx, Queue, DataFun, Url);
    {ok, errors, Errors} ->
        lists:foreach(
            fun(Error) ->
               ok = couch_view_merger_queue:queue(Queue, make_error_item(Error, Url))
            end,
            Errors),
        stream_loop(Ctx, Queue, DataFun, Url);
    {ok, done} ->
        ok
    end.


next_streamer_state(Ctx, DataFun) ->
    case couch_view_parser:next_state(Ctx) of
    {ok, need_more_data} ->
        case DataFun() of
        {ok, Chunk} ->
            case couch_view_parser:parse_chunk(Ctx, Chunk) of
            ok ->
                next_streamer_state(Ctx, DataFun);
            {error, _} = Error ->
                throw(Error)
            end;
        eof ->
            {ok, done}
        end;
    {error, _Reason} = Error ->
        throw(Error);
    Else ->
        Else
    end.


% _all_docs error row
transform_row({{Key, error}, Reason}, _Url) ->
    RowJson = <<"{\"key\":", Key/binary, ",\"error\":", Reason/binary, "}">>,
    {{{json, Key}, error}, {row_json, RowJson}};

% map view rows
transform_row({{Key, DocId}, Value}, _Url) when is_binary(Value) ->
    RowJson = <<"{\"id\":", DocId/binary, ",\"key\":", Key/binary,
                ",\"value\":", Value/binary, "}">>,
    {{{json, Key}, ?JSON_DECODE(DocId)}, {row_json, RowJson}};

transform_row({{Key, DocId}, Value, Doc}, _Url) when is_binary(Value) ->
    RowJson = <<"{\"id\":", DocId/binary, ",\"key\":", Key/binary,
                ",\"value\":", Value/binary, ",\"doc\":", Doc/binary, "}">>,
    {{{json, Key}, ?JSON_DECODE(DocId)}, {row_json, RowJson}};

transform_row({{Key, DocId}, {PartId, _Node, Value}}, Url) ->
    RowJson = <<"{\"id\":", DocId/binary, ",\"key\":", Key/binary,
                ",\"partition\":", PartId/binary, ",\"node\":", Url/binary,
                ",\"value\":", Value/binary, "}">>,
    {{{json, Key}, ?JSON_DECODE(DocId)}, {row_json, RowJson}};

transform_row({{Key, DocId}, {PartId, _Node, Value}, Doc}, Url) ->
    RowJson = <<"{\"id\":", DocId/binary, ",\"key\":", Key/binary,
                ",\"partition\":", PartId/binary, ",\"node\":", Url/binary,
                ",\"value\":", Value/binary, ",\"doc\":", Doc/binary, "}">>,
    {{{json, Key}, ?JSON_DECODE(DocId)}, {row_json, RowJson}};

% reduce view rows
transform_row({Key, Value}, _Url) when is_binary(Key) ->
    RowJson = <<"{\"key\":", Key/binary, ",\"value\":", Value/binary, "}">>,
    % value_json, in case rereduce needs to be done by the merger
    {{json, Key}, {row_json, RowJson}, {value_json, Value}}.


make_error_item({_Node, Reason}, Url) ->
    Json = <<"{\"from\":", Url/binary, ",\"reason\":", Reason/binary, "}">>,
    {error, row_json, Json}.
