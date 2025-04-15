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

-module(couch_httpd_external).

-export([send_external_response/2, json_req_obj/2, json_req_obj/3]).
-export([default_or_content_type/2, parse_external_response/1]).

-import(couch_httpd,[send_error/4]).

-include("couch_db.hrl").

json_req_obj(Req, Db) -> json_req_obj(Req, Db, null).
json_req_obj(#httpd{mochi_req=Req,
               method=Method,
               path_parts=Path,
               req_body=ReqBody
            }, Db, DocId) ->
    Body = case ReqBody of
        undefined -> mochiweb_request:recv_body(Req);
        Else -> Else
    end,
    ParsedForm = case mochiweb_request:get_primary_header_value("content-type",
                                                                Req) of
        "application/x-www-form-urlencoded" ++ _ ->
            case Body of
            undefined -> [];
            _ -> mochiweb_util:parse_qs(Body)
            end;
        _ ->
            []
    end,
    Headers = mochiweb_request:get(headers, Req),
    Hlist = mochiweb_headers:to_list(Headers),
    {ok, Info} = couch_db:get_db_info(Db),
    
% add headers...
    {[{<<"info">>, {Info}},
        {<<"id">>, DocId},
        {<<"uuid">>, couch_uuids:new()},
        {<<"method">>, Method},
        {<<"path">>, Path},
        {<<"query">>, json_query_keys(to_json_terms(
                                        mochiweb_request:parse_qs(Req)))},
        {<<"headers">>, to_json_terms(Hlist)},
        {<<"body">>, Body},
        {<<"peer">>, ?l2b(mochiweb_request:get(peer, Req))},
        {<<"form">>, to_json_terms(ParsedForm)},
        {<<"cookie">>, to_json_terms(mochiweb_request:parse_cookie(Req))},
        {<<"userCtx">>, couch_util:json_user_ctx(Db)},
        {<<"secObj">>, couch_db:get_security(Db)}]}.

to_json_terms(Data) ->
    to_json_terms(Data, []).

to_json_terms([], Acc) ->
    {lists:reverse(Acc)};
to_json_terms([{Key, Value} | Rest], Acc) when is_atom(Key) ->
    to_json_terms(Rest, [{list_to_binary(atom_to_list(Key)), list_to_binary(Value)} | Acc]);
to_json_terms([{Key, Value} | Rest], Acc) ->
    to_json_terms(Rest, [{list_to_binary(Key), list_to_binary(Value)} | Acc]).

json_query_keys({Json}) ->
    json_query_keys(Json, []).
json_query_keys([], Acc) ->
    {lists:reverse(Acc)};
json_query_keys([{<<"startkey">>, Value} | Rest], Acc) ->
    json_query_keys(Rest, [{<<"startkey">>, ?JSON_DECODE(Value)}|Acc]);
json_query_keys([{<<"endkey">>, Value} | Rest], Acc) ->
    json_query_keys(Rest, [{<<"endkey">>, ?JSON_DECODE(Value)}|Acc]);
json_query_keys([{<<"key">>, Value} | Rest], Acc) ->
    json_query_keys(Rest, [{<<"key">>, ?JSON_DECODE(Value)}|Acc]);
json_query_keys([Term | Rest], Acc) ->
    json_query_keys(Rest, [Term|Acc]).

send_external_response(#httpd{mochi_req=MochiReq}=Req, Response) ->
    #extern_resp_args{
        code = Code,
        data = Data,
        ctype = CType,
        headers = Headers
    } = parse_external_response(Response),
    couch_httpd:log_request(Req, Code),
    Resp = mochiweb_request:respond({Code,
        default_or_content_type(CType, Headers ++ couch_httpd:server_header()),
        Data}, MochiReq),
    {ok, Resp}.

parse_external_response({Response}) ->
    lists:foldl(fun({Key,Value}, Args) ->
        case {Key, Value} of
            {"", _} ->
                Args;
            {<<"code">>, Value} ->
                Args#extern_resp_args{code=Value};
            {<<"stop">>, true} ->
                Args#extern_resp_args{stop=true};
            {<<"json">>, Value} ->
                Args#extern_resp_args{
                    data=?JSON_ENCODE(Value),
                    ctype="application/json"};
            {<<"body">>, Value} ->
                Args#extern_resp_args{data=Value, ctype="text/html; charset=utf-8"};
            {<<"base64">>, Value} ->
                Args#extern_resp_args{
                    data=base64:decode(Value),
                    ctype="application/binary"
                };
            {<<"headers">>, {Headers}} ->
                NewHeaders = lists:map(fun({Header, HVal}) ->
                    {binary_to_list(Header), binary_to_list(HVal)}
                end, Headers),
                Args#extern_resp_args{headers=NewHeaders};
            _ -> % unknown key
                Msg = lists:flatten(io_lib:format("Invalid data from external server: ~p", [{Key, Value}])),
                throw({external_response_error, Msg})
            end
        end, #extern_resp_args{}, Response).

default_or_content_type(DefaultContentType, Headers) ->
    IsContentType = fun({X, _}) -> string:to_lower(X) == "content-type" end,
    case lists:any(IsContentType, Headers) of
    false ->
        [{"Content-Type", DefaultContentType} | Headers];
    true ->
        Headers
    end.
