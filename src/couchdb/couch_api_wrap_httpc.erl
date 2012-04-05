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

-module(couch_api_wrap_httpc).

-include("couch_db.hrl").
-include("couch_api_wrap.hrl").
-include("../lhttpc/lhttpc.hrl").

-export([setup/1, tear_down/1]).
-export([send_req/3]).
-export([full_url/2]).

-import(couch_util, [
    get_value/2,
    get_value/3
]).

-define(replace(L, K, V), lists:keystore(K, 1, L, {K, V})).
-define(MAX_WAIT, 5 * 60 * 1000).
-define(NOT_HTTP_ERROR(Code),
    (Code =:= 200 orelse Code =:= 201 orelse
        (Code >= 400 andalso Code < 500))).
-define(IS_HTTP_REDIRECT(Code),
    (Code =:= 301 orelse Code =:= 302 orelse Code =:= 303)).


setup(#httpdb{httpc_pool = nil} = Db) ->
    #httpdb{timeout = Timeout, http_connections = MaxConns} = Db,
    {ok, Pid} = lhttpc_manager:start_link(
        [{connection_timeout, Timeout}, {pool_size, MaxConns}]),
    {ok, Db#httpdb{httpc_pool = Pid}}.


tear_down(#httpdb{httpc_pool = Pool}) ->
    couch_util:shutdown_sync(Pool).


send_req(HttpDb, Params, Callback) ->
    Qs1 = get_value(qs, Params, []),
    Qs2 = [{K, ?b2l(iolist_to_binary(V))} || {K, V} <- Qs1],
    Params2 = ?replace(Params, qs, Qs2),
    Response = send_lhttpc_req(HttpDb, Params2),
    process_response(Response, HttpDb, Params2, Callback).


send_lhttpc_req(#httpdb{headers = BaseHeaders} = HttpDb, Params) ->
    Method = get_value(method, Params, "GET"),
    UserHeaders = lists:keysort(1, get_value(headers, Params, [])),
    Headers1 = lists:ukeymerge(1, UserHeaders, BaseHeaders),
    Headers2 = oauth_header(HttpDb, Params) ++ Headers1,
    Url = full_url(HttpDb, Params),
    Body = get_value(body, Params, []),
    Timeout = HttpDb#httpdb.timeout,
    CallerLhttpcOptions = lists:keysort(1, get_value(lhttpc_options, Params, [])),
    LhttpcOptions = [
        {pool, HttpDb#httpdb.httpc_pool},
        {connect_timeout, Timeout} |
        lists:ukeymerge(1, CallerLhttpcOptions, HttpDb#httpdb.lhttpc_options)
    ],
    try
        lhttpc:request(Url, Method, Headers2, Body, Timeout, LhttpcOptions)
    catch exit:ExitReason ->
        {error, ExitReason}
    end.


process_response({ok, {{Code, _}, Headers, _Body}}, HttpDb, Params, Callback) when
        ?IS_HTTP_REDIRECT(Code) ->
    do_redirect(Code, Headers, HttpDb, Params, Callback);

process_response({ok, {{Code, _}, Headers, Pid}}, HttpDb, Params, Callback) when
        is_pid(Pid) ->
    process_stream_response(Code, Headers, Pid, HttpDb, Params, Callback);

process_response({ok, {{Code, _}, Headers, Body}}, HttpDb, Params, Callback) ->
    case ?NOT_HTTP_ERROR(Code) of
    true ->
        Callback(Code, Headers, decode_body(Body));
    false ->
        maybe_retry({code, Code}, HttpDb, Params, Callback)
    end;

process_response({ok, UploadState0}, HttpDb, Params, Callback) ->
    UploadFun = make_upload_fun(UploadState0, HttpDb),
    try
        Callback(UploadFun)
    catch
    throw:{redirect_req, Code, Headers} ->
        do_redirect(Code, Headers, HttpDb, Params, Callback);
    throw:{maybe_retry_req, Error} ->
        maybe_retry(Error, HttpDb, Params, Callback)
    end;

process_response(Error, HttpDb, Params, Callback) ->
    maybe_retry(Error, HttpDb, Params, Callback).


process_stream_response(Code, Headers, Pid, HttpDb, Params, Callback) ->
    case ?NOT_HTTP_ERROR(Code) of
    true ->
        StreamDataFun = fun() ->
            stream_data_self(HttpDb, Params, Pid, Callback)
        end,
        try
            RetValue = Callback(Code, Headers, StreamDataFun),
            receive {http_eob, Pid, _Trailers} -> ok end,
            RetValue
        catch throw:{maybe_retry_req, Err} ->
            maybe_retry(Err, HttpDb, Params, Callback)
        end;
    false ->
        report_error(HttpDb, Params, {code, Code})
    end.


maybe_retry(Error, #httpdb{retries = 0} = HttpDb, Params, _Callback) ->
    report_error(HttpDb, Params, {error, Error});

maybe_retry(Error, #httpdb{retries = Retries} = HttpDb, Params, Callback) ->
    Method = get_value(method, Params, "GET"),
    Url = couch_util:url_strip_password(full_url(HttpDb, Params)),
    ?LOG_INFO("Retrying ~s request to ~s in ~p seconds due to error ~s",
        [Method, Url, HttpDb#httpdb.wait / 1000, error_cause(Error)]),
    ok = timer:sleep(HttpDb#httpdb.wait),
    HttpDb2 = HttpDb#httpdb{
        retries = Retries - 1,
        wait = erlang:min(HttpDb#httpdb.wait * 2, ?MAX_WAIT)
    },
    send_req(HttpDb2, Params, Callback).


report_error(HttpDb, Params, Error) ->
    Method = get_value(method, Params, "GET"),
    Url = couch_util:url_strip_password(full_url(HttpDb, Params)),
    do_report_error(Url, Method, Error),
    exit({http_request_failed, Method, Url, Error}).


do_report_error(Url, Method, {code, Code}) ->
    ?LOG_ERROR("Replicator, request ~s to ~p failed. The received "
        "HTTP error code is ~p", [Method, Url, Code]);

do_report_error(FullUrl, Method, Error) ->
    ?LOG_ERROR("Replicator, request ~s to ~p failed due to error ~s",
        [Method, FullUrl, error_cause(Error)]).


error_cause({error, Cause}) ->
    lists:flatten(io_lib:format("~p", [Cause]));
error_cause(Cause) ->
    lists:flatten(io_lib:format("~p", [Cause])).


stream_data_self(#httpdb{timeout = T} = HttpDb, Params, Pid, Callback) ->
    try
        case lhttpc:get_body_part(Pid, T) of
        {ok, {http_eob, _Trailers}} ->
            {<<>>, fun() -> throw({maybe_retry_req, more_data_expected}) end};
        {ok, Data} ->
            {Data, fun() -> stream_data_self(HttpDb, Params, Pid, Callback) end};
        Error ->
            throw({maybe_retry_req, Error})
        end
    catch exit:ExitReason ->
        throw({maybe_retry_req, ExitReason})
    end.


make_upload_fun(UploadState, #httpdb{timeout = Timeout} = HttpDb) ->
    fun(eof) ->
        try
            case lhttpc:send_body_part(UploadState, http_eob, Timeout) of
            {ok, {{Code, _}, Headers, Body}} when ?NOT_HTTP_ERROR(Code) ->
                {ok, Code, Headers, decode_body(Body)};
            {ok, {{Code, _}, Headers, _Body}} when ?IS_HTTP_REDIRECT(Code) ->
                throw({redirect_req, Code, Headers});
            {ok, {{Code, _}, _Headers, _Body}} ->
                throw({maybe_retry_req, {code, Code}});
            Error ->
                throw({maybe_retry_req, Error})
            end
        catch exit:ExitReason ->
            throw({maybe_retry_req, ExitReason})
        end;
    (BodyPart) ->
        try
            case lhttpc:send_body_part(UploadState, BodyPart, Timeout) of
            {ok, UploadState2} ->
                {ok, make_upload_fun(UploadState2, HttpDb)};
            Error ->
                throw({maybe_retry_req, Error})
            end
        catch exit:ExitReason ->
            throw({maybe_retry_req, ExitReason})
        end
    end.


full_url(#httpdb{url = BaseUrl}, Params) ->
    Path = get_value(path, Params, []),
    QueryArgs = get_value(qs, Params, []),
    BaseUrl ++ Path ++ query_args_to_string(QueryArgs, []).


query_args_to_string([], []) ->
    "";
query_args_to_string([], Acc) ->
    "?" ++ string:join(lists:reverse(Acc), "&");
query_args_to_string([{K, V} | Rest], Acc) ->
    query_args_to_string(Rest, [K ++ "=" ++ couch_httpd:quote(V) | Acc]).


oauth_header(#httpdb{oauth = nil}, _ConnParams) ->
    [];
oauth_header(#httpdb{url = BaseUrl, oauth = OAuth}, ConnParams) ->
    Consumer = {
        OAuth#oauth.consumer_key,
        OAuth#oauth.consumer_secret,
        OAuth#oauth.signature_method
    },
    Method = get_value(method, ConnParams, "GET"),
    QSL = get_value(qs, ConnParams, []),
    OAuthParams = oauth:signed_params(Method,
        BaseUrl ++ get_value(path, ConnParams, []),
        QSL, Consumer, OAuth#oauth.token, OAuth#oauth.token_secret) -- QSL,
    [{"Authorization",
        "OAuth " ++ oauth_uri:params_to_header_string(OAuthParams)}].


do_redirect(Code, Headers, #httpdb{url = Url} = HttpDb, Params, Cb) ->
    RedirectUrl = redirect_url(Headers, Url),
    {HttpDb2, Params2} = after_redirect(RedirectUrl, Code, HttpDb, Params),
    send_req(HttpDb2, Params2, Cb).


redirect_url(RespHeaders, OrigUrl) ->
    MochiHeaders = mochiweb_headers:make(RespHeaders),
    RedUrl = mochiweb_headers:get_value("Location", MochiHeaders),
    #lhttpc_url{
        host = Host,
        port = Port,
        path = Path,  % includes query string
        is_ssl = IsSsl
    } = lhttpc_lib:parse_url(RedUrl),
    #lhttpc_url{
        user = User,
        password = Passwd
    } = lhttpc_lib:parse_url(OrigUrl),
    Creds = case User of
    [] ->
        [];
    _ when Passwd =/= [] ->
        User ++ ":" ++ Passwd ++ "@";
    _ ->
        User ++ "@"
    end,
    HostPart = case is_ipv6_literal(Host) of
    true ->
        % IPv6 address literals are enclosed by square brackets (RFC2732)
        "[" ++ Host ++ "]";
    false ->
        Host
    end,
    Proto = case IsSsl of
    true ->
        "https://";
    false ->
        "http://"
    end,
    Proto ++ Creds ++ HostPart ++ ":" ++ integer_to_list(Port) ++ Path.

is_ipv6_literal(Host) ->
    case inet_parse:address(Host) of
    {ok, {_, _, _, _, _, _, _, _}} ->
        true;
    _ ->
        false
    end.


after_redirect(RedirectUrl, 303, HttpDb, Params) ->
    after_redirect(RedirectUrl, HttpDb, ?replace(Params, method, "GET"));
after_redirect(RedirectUrl, _Code, HttpDb, Params) ->
    after_redirect(RedirectUrl, HttpDb, Params).

after_redirect(RedirectUrl, HttpDb, Params) ->
    Params2 = lists:keydelete(path, 1, lists:keydelete(qs, 1, Params)),
    {HttpDb#httpdb{url = RedirectUrl}, Params2}.


decode_body(<<>>) ->
    null;
decode_body(undefined) ->
    % HEAD request response body
    null;
decode_body(Body) ->
    ?JSON_DECODE(Body).
