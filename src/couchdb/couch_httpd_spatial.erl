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

-export([handle_spatial_req/3]).

-import(couch_httpd, [send_json/2, send_method_not_allowed/2]).

handle_spatial_req(#httpd{method='GET',
        path_parts=[_, _, DName, _, SpatialName, Query]}=Req, Db, DDoc) ->
    ?LOG_DEBUG("Spatial query (~p): ~p", [DName, Query]),
    couch_spatial:update_tree(Db, DDoc, DName, SpatialName),
    Bbox = list_to_tuple(?JSON_DECODE(Query)),
    %Foo = couch_spatial:bbox_search({-180, -90, 180, 90}),
    Foo = couch_spatial:bbox_search(Bbox),
%    Foo = <<"bar">>,
    send_json(Req, {[{<<"query1">>, Foo}]});

handle_spatial_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET,HEAD").
