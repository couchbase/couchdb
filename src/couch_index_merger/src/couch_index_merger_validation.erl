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

-module(couch_index_merger_validation).

-export([views_param/1, revision_param/1, keys_param/1, on_error_param/1]).

-include("couch_db.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").
-include_lib("couch_index_merger/include/couch_view_merger.hrl").
-include("../lhttpc/lhttpc.hrl").


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

views_param({[_ | _] = Views}) ->
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
            case couch_util:get_value(<<"ddoc_revision">>, Props) of
            undefined ->
                ok;
            _ ->
                Msg = "Nested 'ddoc_revision' specifications are not allowed.",
                throw({bad_request, Msg})
            end,
            case couch_util:get_value(<<"views">>, Props) of
            {[_ | _]} = SubViews ->
                SubViewSpecs = views_param(SubViews),
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
                        [rem_passwd(MergeUrl)]),
                    throw({bad_request, SubMergeError})
                end,
                #merged_index_spec{url = MergeUrl, ejson_spec = EJson};
            _ ->
                SubMergeError = io_lib:format("Invalid view merge definition for"
                    " sub-merge done at `~s`.",
                    [rem_passwd(MergeUrl)]),
                throw({bad_request, SubMergeError})
            end;
        (_) ->
            throw({bad_request, "Invalid view merge definition object."})
        end, Views));

views_param(_) ->
    throw({bad_request, <<"`views` parameter must be an object with at ",
                          "least 1 property.">>}).

validate_sets_param({[_ | _] = Sets}) ->
    lists:map(
        fun ({SetName, {[_|_] = Props}}) ->
            ViewName = couch_util:get_value(<<"view">>, Props),
            Partitions = couch_util:get_value(<<"partitions">>, Props),

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


revision_param(nil) ->
    nil;
revision_param(<<"auto">>) ->
    auto;
revision_param(Revision) ->
    couch_doc:parse_rev(Revision).


keys_param(nil) ->
    nil;
keys_param(Keys) when is_list(Keys) ->
    Keys;
keys_param(_) ->
    throw({bad_request, "`keys` parameter is not an array."}).


on_error_param("continue") ->
    continue;
on_error_param("stop") ->
    stop;
on_error_param(Value) ->
    Msg = io_lib:format("Invalid value (`~s`) for the parameter `on_error`."
        " It must be `continue` (default) or `stop`.",
        [couch_util:to_binary(Value)]),
    throw({bad_request, Msg}).


rem_passwd(Url) ->
    ?l2b(couch_util:url_strip_password(Url)).
