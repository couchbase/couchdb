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
-module(couch_view_server_objc).

-export([is_lightweight/0, add_view_deps/1]).
-export([get_server/3, ret_server/1]).
-export([map/2, reduce/3, rereduce/3]).

-include("couch_db.hrl").

is_lightweight() ->
    true.

add_view_deps(View) ->
    % Catch any error here; we don't really care what it is, only
    % that there is an error (so we don't correspond to any other
    % pre-computed views). The underlying error will be hit when
    % (and if) the view is actually built.
    ObjCVers = try objc_dispatch:query_view_version(View#view.def)
    catch
        _ -> error
    end,
    [View, ObjCVers].

get_server(_Arg, Maps, ReduceStruct) ->
    % ReduceStruct is [[ViewId, [RedDef, ...]], [ViewId, [...], ...].
    % Turn this into [RedDef, ...] to decode, and a map of [{ViewId, [3, 4]}, ...]
    % where [3,4] are indexes (0-based) into the array of RedDefs.
    {RevIndexMap, RevAllRedDefs, _} = lists:foldl(fun([ViewId, RedDefs], {IdIndexAssoc, RedDefAcc, NextIndex}) ->
        {NewRedDefs, Indexes, NewNextIndex} = lists:foldl(fun(RedDef, {RedDefs, Indexes, Index}) ->
            {[RedDef | RedDefs], [Index | Indexes], Index + 1}
        end, {RedDefAcc, [], NextIndex}, RedDefs),
        {[{ViewId, Indexes} | IdIndexAssoc], NewRedDefs, NewNextIndex}
    end, {[], [], 0}, ReduceStruct),
    AllRedDefs = lists:reverse(RevAllRedDefs),
    RedIndexMap = [{ViewId, lists:reverse(RevIndexes)} || {ViewId, RevIndexes} <- RevIndexMap],
    {ok, QueueRef} = objc_dispatch:create(<<"org.couchdb.objc_view">>, Maps, AllRedDefs),
    {ok, {QueueRef, RedIndexMap}}.

ret_server(_) ->
    ok.

map({QueueRef, _RedIndexMap}, Docs) ->
    Results = lists:map(fun(Doc) ->
        JsonDoc = couch_doc:to_json_obj(Doc, []),
        {ok, EmitsPerView} = objc_dispatch:map(QueueRef, JsonDoc),
        lists:map(fun(EmitListItem) ->
            [list_to_tuple(EmitPair) || EmitPair <- EmitListItem]
        end, EmitsPerView)
    end, Docs),
    {ok, Results}.

reduce({QueueRef, RedIndexMap}, ViewId, KVs) ->
    Indexes = proplists:get_value(ViewId, RedIndexMap),
    {Keys, Vals} = lists:foldl(fun([K, V], {KAcc, VAcc}) ->
        {[K | KAcc], [V | VAcc]}
    end, {[], []}, KVs),
    objc_dispatch:reduce(QueueRef, Indexes, Keys, Vals, false).

rereduce({QueueRef, RedIndexMap}, ViewId, Vals) ->
    Indexes = proplists:get_value(ViewId, RedIndexMap),
    objc_dispatch:reduce(QueueRef, Indexes, null, Vals, true).
