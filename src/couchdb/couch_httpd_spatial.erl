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

-module(couch_httpd_spatial).
-include("couch_db.hrl").
-include("couch_spatial.hrl").

-export([handle_spatial_req/3, spatial_group_etag/3, load_index/3]).

-import(couch_httpd, [send_json/2, send_method_not_allowed/2]).

handle_spatial_req(#httpd{method='GET',
        path_parts=[_, _, DName, _, SpatialName]}=Req, Db, DDoc) ->
    ?LOG_DEBUG("Spatial query (~p): ~n~p", [DName, DDoc#doc.id]),
    QueryArgs = parse_spatial_params(Req),
    Result = couch_spatial:bbox_search(Db, DDoc, SpatialName, QueryArgs),
    send_json(Req, {[{<<"spatial">>, Result}]});

handle_spatial_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET,HEAD").

load_index(Req, Db, {DesignId, SpatialName}) ->
    QueryArgs = parse_spatial_params(Req),
    Stale = QueryArgs#spatial_query_args.stale,
    case couch_spatial:get_spatial_index(Db, DesignId, SpatialName, Stale) of
    {ok, Index, Group} ->
          {ok, Index, Group, QueryArgs};
    {not_found, Reason} ->
        throw({not_found, Reason})
    end.

% counterpart in couch_httpd_view is view_group_etag/2 resp. /3
spatial_group_etag(Group, Db) ->
    spatial_group_etag(Group, Db, nil).
spatial_group_etag(#spatial_group{sig=Sig, current_seq=CurrentSeq}, _Db, Extra) ->
    couch_httpd:make_etag({Sig, CurrentSeq, Extra}).

parse_spatial_params(Req) ->
    QueryList = couch_httpd:qs(Req),
    QueryParams = lists:foldl(fun({K, V}, Acc) ->
        parse_view_param(K, V) ++ Acc
    end, [], QueryList),
    _QueryArgs = lists:foldl(fun({K, V}, Args2) ->
        validate_spatial_query(K, V, Args2)
    end, #spatial_query_args{}, lists:reverse(QueryParams)).

parse_view_param("bbox", Bbox) ->
    [{bbox, list_to_tuple(?JSON_DECODE("[" ++ Bbox ++ "]"))}];
parse_view_param("stale", "ok") ->
    [{stale, ok}];
parse_view_param("stale", _Value) ->
    throw({query_parse_error, <<"stale only available as stale=ok">>});
parse_view_param(Key, Value) ->
    [{extra, {Key, Value}}].

validate_spatial_query(bbox, Value, Args) ->
    Args#spatial_query_args{bbox=Value};
validate_spatial_query(stale, ok, Args) ->
    Args#spatial_query_args{stale=ok};
validate_spatial_query(stale, _, Args) ->
    Args;
validate_spatial_query(extra, _Value, Args) ->
    Args.
