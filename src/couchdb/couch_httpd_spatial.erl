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
        path_parts=[_, _, DName, _, Query]}=Req, Db, _DDoc) ->
    ?LOG_DEBUG("Spatial query (~p): ~p", [DName, Query]),
    %Seq = list_to_integer(binary_to_list(Query)),
    %Bbox = 
    %Foo = couch_spatial:update_tree(Db),
    couch_spatial:update_tree(Db),
    %Bbox = list_to_tuple([-180, -90, 180, 90]),
    Bbox = list_to_tuple(?JSON_DECODE(Query)),
    %Foo = couch_spatial:bbox_search({-180, -90, 180, 90}),
    Foo = couch_spatial:bbox_search(Bbox),
    send_json(Req, {[{<<"query1">>, Foo}]});

handle_spatial_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET,HEAD").
