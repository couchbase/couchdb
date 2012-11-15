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
-include("../lhttpc/lhttpc.hrl").

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
    Views = validate_views_param(qs_json_value(Req, "views", nil)),
    Keys = validate_keys_param(qs_json_value(Req, "keys", nil)),
    DDocRevision = couch_index_merger:validate_revision_param(
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
    Views = validate_views_param(get_value(<<"views">>, Props)),
    Keys = validate_keys_param(get_value(<<"keys">>, Props, nil)),
    DDocRevision = couch_index_merger:validate_revision_param(
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
        on_error = validate_on_error_param(OnError)
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
    #sender_acc{rows_acc = RowsAcc, req = Req, resp = Resp} = SAcc,
    ErrorRow = make_error_row(Error),
    case Resp of
    nil ->
        % we haven't started the response yet
        Start = <<"{\"total_rows\":0,\"rows\":[]\r\n">>,
        {ok, Resp2} = couch_httpd:start_json_response(Req, 200, []),
        couch_httpd:send_chunk(Resp2, Start),
        Buffer1 = [
            <<"{\"total_rows\":0,\"rows\":[]\r\n">>,
            <<",\r\n\"errors\":[">>,
            ErrorRow,
            <<"]">>
        ];
    _ ->
       Resp2 = Resp,
       Buffer1 = [<<"\r\n],\"errors\":[">>, ErrorRow, <<"]">>]
    end,
    Buffer2 = [lists:reverse(RowsAcc), Buffer1, <<"\r\n}">>],
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


%% Valid `views` example:
%%
%% {
%%   "views": {
%%     "localdb1": ["ddocname/viewname", ...],
%%     "http://server2/dbname": ["ddoc/view"],
%%     "http://server2/_view_merge": {
%%       "views": {
%%         "localdb3": "viewname", // local to server2
%%         "localdb4": "viewname"  // local to server2
%%       }
%%     }
%%   }
%% }

validate_views_param({[_ | _] = Views}) ->
    lists:flatten(lists:map(
        fun ({<<"sets">>, SetsSpec}) ->
            validate_sets_param(SetsSpec);
        ({DbName, ViewName}) when is_binary(ViewName) ->
            {DDocDbName, DDocId, Vn} = parse_view_name(ViewName),
            #simple_index_spec{
                database = DbName, ddoc_id = DDocId, index_name = Vn,
                ddoc_database = DDocDbName
            };
        ({DbName, ViewNames}) when is_list(ViewNames) ->
            lists:map(
                fun(ViewName) ->
                    {DDocDbName, DDocId, Vn} = parse_view_name(ViewName),
                    #simple_index_spec{
                        database = DbName, ddoc_id = DDocId, index_name = Vn,
                        ddoc_database = DDocDbName
                    }
                end, ViewNames);
        ({MergeUrl, {[_ | _] = Props} = EJson}) ->
            case (catch lhttpc_lib:parse_url(?b2l(MergeUrl))) of
            #lhttpc_url{} ->
                ok;
            _ ->
                throw({bad_request, "Invalid view merge definition object."})
            end,
            case get_value(<<"ddoc_revision">>, Props) of
            undefined ->
                ok;
            _ ->
                Msg = "Nested 'ddoc_revision' specifications are not allowed.",
                throw({bad_request, Msg})
            end,
            case get_value(<<"views">>, Props) of
            {[_ | _]} = SubViews ->
                SubViewSpecs = validate_views_param(SubViews),
                case lists:any(
                    fun(#simple_index_spec{}) -> true;
                       (#set_view_spec{}) -> true;
                       (_) -> false
                    end,
                    SubViewSpecs) of
                true ->
                    ok;
                false ->
                    SubMergeError = io_lib:format("Could not find a non-composed"
                        " view spec in the view merge targeted at `~s`",
                        [couch_index_merger:rem_passwd(MergeUrl)]),
                    throw({bad_request, SubMergeError})
                end,
                #merged_index_spec{url = MergeUrl, ejson_spec = EJson};
            _ ->
                SubMergeError = io_lib:format("Invalid view merge definition for"
                    " sub-merge done at `~s`.",
                    [couch_index_merger:rem_passwd(MergeUrl)]),
                throw({bad_request, SubMergeError})
            end;
        (_) ->
            throw({bad_request, "Invalid view merge definition object."})
        end, Views));

validate_views_param(_) ->
    throw({bad_request, <<"`views` parameter must be an object with at ",
                          "least 1 property.">>}).

validate_sets_param({[_ | _] = Sets}) ->
    lists:map(
        fun ({SetName, {[_|_] = Props}}) ->
            ViewName = get_value(<<"view">>, Props),
            Partitions = get_value(<<"partitions">>, Props),

            case ViewName =:= undefined orelse Partitions =:= undefined of
            true ->
                Msg0 = io_lib:format(
                    "Set view specification for `~s` misses "
                    "`partitions` and/or `view` properties", [SetName]),
                throw({bad_request, Msg0});
            false ->
                ok
            end,

            {DDocDbName, DDocId, Vn} = parse_view_name(ViewName),
            case DDocDbName =/= nil orelse DDocId =:= nil of
            true ->
                Msg1 = io_lib:format(
                    "Invalid `viewname` property for `~s` set view. "
                    "Design document id and view name must specified.",
                         [SetName]),
                throw({bad_request, Msg1});
            false ->
                ok
            end,
            case not(is_list(Partitions)) orelse
                lists:any(fun (X) -> not(is_integer(X)) end, Partitions) of
            true ->
                Msg2 = io_lib:format(
                    "Invalid `partitions` property for `~s` set view",
                    [SetName]),
                throw({bad_request, Msg2});
            false ->
                ok
            end,

            #set_view_spec{
                name = SetName,
                ddoc_id = DDocId, view_name = Vn, partitions = Partitions
            };
        (_) ->
            throw({bad_request, "Invalid set view merge definition object."})
        end, Sets);
validate_sets_param(_) ->
    throw({bad_request, <<"`sets` parameter must be an object with at ",
                          "least 1 property.">>}).

parse_view_name(Name) ->
    Tokens = string:tokens(couch_util:trim(?b2l(Name)), "/"),
    case [?l2b(couch_httpd:unquote(Token)) || Token <- Tokens] of
    [<<"_all_docs">>] ->
        {nil, nil, <<"_all_docs">>};
    [DDocName, ViewName] ->
        {nil, <<"_design/", DDocName/binary>>, ViewName};
    [<<"_design">>, DDocName, ViewName] ->
        {nil, <<"_design/", DDocName/binary>>, ViewName};
    [DDocDbName, DDocName, ViewName] ->
        {DDocDbName, <<"_design/", DDocName/binary>>, ViewName};
    [DDocDbName, <<"_design">>, DDocName, ViewName] ->
        {DDocDbName, <<"_design/", DDocName/binary>>, ViewName};
    _ ->
        throw({bad_request, "A `view` property must have the shape"
            " `ddoc_name/view_name`."})
    end.


validate_keys_param(nil) ->
    nil;
validate_keys_param(Keys) when is_list(Keys) ->
    Keys;
validate_keys_param(_) ->
    throw({bad_request, "`keys` parameter is not an array."}).


validate_on_error_param("continue") ->
    continue;
validate_on_error_param("stop") ->
    stop;
validate_on_error_param(Value) ->
    Msg = io_lib:format("Invalid value (`~s`) for the parameter `on_error`."
        " It must be `continue` (default) or `stop`.", [to_binary(Value)]),
    throw({bad_request, Msg}).


make_error_row({error, row_json, Json}) ->
    Json;
make_error_row({error, Url, Reason}) ->
    ?JSON_ENCODE({[{<<"from">>, iolist_to_binary(Url)},
                   {<<"reason">>, to_binary(Reason)}]}).
