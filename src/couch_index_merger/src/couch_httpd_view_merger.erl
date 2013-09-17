% -*- Mode: Erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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

-module(couch_httpd_view_merger).

-export([handle_req/1, apply_http_config/3]).

-include("couch_db.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").
-include_lib("couch_index_merger/include/couch_view_merger.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1
]).
-import(couch_httpd, [
    qs_json_value/3,
    qs_value/3
]).

-record(sender_acc, {
    req = nil,
    resp = nil,
    on_error,
    acc = <<>>,
    error_acc = [],
    debug_info_acc = [],
    rows_acc = [],
    rows_acc_size = 0,
    first_row_sent = false,
    total_rows = nil
}).

-define(MAX_ROWS_ACC_SIZE, 4096).


setup_http_sender(MergeParams, Req) ->
    MergeParams#index_merge{
        user_acc = #sender_acc{
            req = Req, on_error = MergeParams#index_merge.on_error
        },
        callback = fun http_sender/2
    }.

handle_req(#httpd{method = 'GET'} = Req) ->
    Views = couch_index_merger_validation:views_param(
        qs_json_value(Req, "views", nil)),
    Keys = couch_index_merger_validation:keys_param(
        qs_json_value(Req, "keys", nil)),
    DDocRevision = couch_index_merger_validation:revision_param(
        qs_json_value(Req, <<"ddoc_revision">>, nil)),
    ViewMergeParams = #view_merge{
        keys = Keys
    },
    MergeParams0 = #index_merge{
        indexes = Views,
        ddoc_revision = DDocRevision,
        extra = ViewMergeParams
    },
    MergeParams1 = apply_http_config(Req, [], MergeParams0),
    couch_index_merger:query_index(couch_view_merger, MergeParams1, Req);

handle_req(#httpd{method = 'POST'} = Req) ->
    {Props} = couch_httpd:json_body_obj(Req),
    Views = couch_index_merger_validation:views_param(
        get_value(<<"views">>, Props)),
    Keys = couch_index_merger_validation:keys_param(
        get_value(<<"keys">>, Props, nil)),
    DDocRevision = couch_index_merger_validation:revision_param(
        get_value(<<"ddoc_revision">>, Props, nil)),
    ViewMergeParams = #view_merge{
        keys = Keys
    },
    MergeParams0 = #index_merge{
        indexes = Views,
        ddoc_revision = DDocRevision,
        extra = ViewMergeParams
    },
    MergeParams1 = apply_http_config(Req, Props, MergeParams0),
    couch_index_merger:query_index(couch_view_merger, MergeParams1, Req);

handle_req(Req) ->
    couch_httpd:send_method_not_allowed(Req, "GET,POST").


apply_http_config(Req, Body, MergeParams) ->
    DefConnTimeout = MergeParams#index_merge.conn_timeout,
    QsTimeout = qs_json_value(Req, "connection_timeout", DefConnTimeout),
    ConnTimeout = case get_value(<<"connection_timeout">>, Body, QsTimeout) of
    T when is_integer(T) ->
        T;
    _ ->
        Msg = "Query parameter `connection_timeout` must be an integer",
        throw({bad_request, Msg})
    end,
    OnError = case get_value(<<"on_error">>, Body, nil) of
    nil ->
       qs_value(Req, "on_error", "continue");
    Policy when is_binary(Policy) ->
       Policy
    end,
    setup_http_sender(MergeParams#index_merge{
        conn_timeout = ConnTimeout,
        on_error = couch_index_merger_validation:on_error_param(OnError)
    }, Req).


http_sender({debug_info, From, Info}, SAcc) ->
    DebugInfoAcc2 = [{From, Info} | SAcc#sender_acc.debug_info_acc],
    {ok, SAcc#sender_acc{debug_info_acc = DebugInfoAcc2}};

http_sender(start, #sender_acc{req = Req} = SAcc) ->
    #httpd{mochi_req = MReq} = Req,
    ok = mochiweb_socket:setopts(MReq:get(socket), [{nodelay, true}]),
    {ok, Resp} = couch_httpd:start_json_response(Req, 200, []),
    {ok, SAcc#sender_acc{resp = Resp, acc = <<"\r\n">>}};

http_sender({start, RowCount}, #sender_acc{req = Req} = SAcc) ->
    #httpd{mochi_req = MReq} = Req,
    ok = mochiweb_socket:setopts(MReq:get(socket), [{nodelay, true}]),
    {ok, Resp} = couch_httpd:start_json_response(Req, 200, []),
    {ok, SAcc#sender_acc{resp = Resp, total_rows = RowCount, acc = <<"\r\n">>}};

http_sender({row, Row}, SAcc) when is_binary(Row) ->
    SAcc2 = maybe_flush_rows(Row, SAcc),
    {ok, SAcc2#sender_acc{acc = <<",\r\n">>}};

http_sender(stop, SAcc) ->
    #sender_acc{
        error_acc = ErrorAcc,
        resp = Resp
    }= SAcc,
    case ErrorAcc of
    [] ->
        Buffer1 = <<"\r\n]">>;
    _ ->
        {_, Buffer0} = lists:foldl(
            fun(Row, {Sep, A}) ->
                {<<",\r\n">>, [[Sep, Row] | A]}
            end,
            {<<"\r\n">>, []}, ErrorAcc),
        Buffer1 = [<<"\r\n],\r\n\"errors\":[">> | lists:reverse(Buffer0, [<<"\r\n]">>])]
    end,
    Buffer2 = [make_rows_buffer(SAcc), Buffer1, <<"\r\n}">>],
    couch_httpd:send_chunk(Resp, Buffer2),
    {ok, couch_httpd:end_json_response(Resp)};

http_sender({error, _, _} = Error, #sender_acc{on_error = continue, error_acc = ErrorAcc} = SAcc) ->
    ErrorAcc2 = [make_error_row(Error) | ErrorAcc],
    {ok, SAcc#sender_acc{error_acc = ErrorAcc2}};

http_sender({error, _, _} = Error, #sender_acc{on_error = stop} = SAcc) ->
    #sender_acc{req = Req, resp = Resp} = SAcc,
    ErrorRow = make_error_row(Error),
    case Resp of
    nil ->
        % we haven't started the response yet
        {ok, Resp2} = couch_httpd:start_json_response(Req, 200, []),
        Buffer1 = [
            <<"{\"total_rows\":0,\"rows\":[]\r\n">>,
            <<",\r\n\"errors\":[">>,
            ErrorRow,
            <<"]">>
        ];
    _ ->
       Resp2 = Resp,
       Buffer1 = [make_rows_buffer(SAcc), <<"\r\n],\"errors\":[">>, ErrorRow, <<"]">>]
    end,
    Buffer2 = [Buffer1, <<"\r\n}">>],
    couch_httpd:send_chunk(Resp2, Buffer2),
    couch_httpd:end_json_response(Resp2),
    {stop, Resp2}.

maybe_flush_rows(JsonRow, SAcc) ->
    #sender_acc{
        acc = Acc,
        rows_acc = RowsAcc,
        rows_acc_size = RowsAccSize
    } = SAcc,
    RowsAccSize2 = RowsAccSize + byte_size(JsonRow),
    SAcc2 = SAcc#sender_acc{
        rows_acc = [[Acc, JsonRow] | RowsAcc],
        rows_acc_size = RowsAccSize2
    },
    case RowsAccSize2 >= ?MAX_ROWS_ACC_SIZE of
    true ->
        flush_rows(SAcc2);
    false ->
        SAcc2
    end.

flush_rows(SAcc) ->
    Buffer = make_rows_buffer(SAcc),
    couch_httpd:send_chunk(SAcc#sender_acc.resp, Buffer),
    SAcc#sender_acc{
        rows_acc = [],
        rows_acc_size = 0,
        first_row_sent = true
    }.

make_rows_buffer(#sender_acc{rows_acc = RowsAcc, total_rows = TotalRows} = SAcc) ->
    case SAcc#sender_acc.first_row_sent of
    true ->
        lists:reverse(RowsAcc);
    false ->
        DebugInfo = debug_info_buffer(SAcc),
        Buffer0 = case DebugInfo of
        <<>> ->
            <<"{">>;
        _ ->
            [<<"{">>, DebugInfo, <<",">>]
        end,
        Buffer1 = case TotalRows of
        nil ->
            Buffer0;
        _ ->
            [Buffer0, <<"\"total_rows\":">>, integer_to_list(TotalRows), <<",">>]
        end,
        [Buffer1, <<"\"rows\":[">>, lists:reverse(RowsAcc)]
    end.

debug_info_buffer(#sender_acc{debug_info_acc = []}) ->
    <<>>;
debug_info_buffer(#sender_acc{debug_info_acc = DebugInfoAcc}) ->
    DebugInfo = ?JSON_ENCODE({lists:reverse(DebugInfoAcc)}),
    [<<"\"debug_info\":">>, DebugInfo].


make_error_row({error, row_json, Json}) ->
    Json;
make_error_row({error, Url, Reason}) ->
    ?JSON_ENCODE({[{<<"from">>, iolist_to_binary(Url)},
                   {<<"reason">>, to_binary(Reason)}]}).
