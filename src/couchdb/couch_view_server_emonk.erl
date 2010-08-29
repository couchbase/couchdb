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

-module(couch_view_server_emonk).

-export([is_lightweight/0]).
-export([get_server/3, ret_server/1]).
-export([map/2, reduce/3, rereduce/3]).

-include("couch_db.hrl").

is_lightweight() ->
    true.

get_server(_Arg, Maps, Reds) ->
    {ok, Ctx} = emonk:create_ctx(),
    {ok, _} = emonk:eval(Ctx, get_js()),
    ReduceLimit = list_to_atom(
        couch_config:get("query_server_config", "reduce_limit", "true")
    ),
    Config = {[{<<"reduce_limit">>, ReduceLimit}]},
    {ok, _} = emonk:call(Ctx, <<"configure">>, [Config]),
    {ok, _} = emonk:call(Ctx, <<"compile">>, [Maps, Reds]),
    {ok, Ctx}.

ret_server(_) ->
    ok.

map(Server, Docs) ->
    Results = lists:map(fun(Doc) ->
        Json = couch_doc:to_json_obj(Doc, []),

        {ok, PreResults} = call(Server, <<"map">>, [Json]),
        FunsResults = lists:map(fun(R) ->
            case R of
                undefined -> [];
                Else -> Else
            end
        end, PreResults),
        % the results are a json array of function map yields like this:
        % [FunResults1, FunResults2 ...]
        % where funresults is are json arrays of key value pairs:
        % [[Key1, Value1], [Key2, Value2]]
        % Convert the key, value pairs to tuples like
        % [{Key1, Value1}, {Key2, Value2}]
        lists:map(fun(FunRs) ->
            [list_to_tuple(FunResult) || FunResult <- FunRs]
        end, FunsResults)
    end, Docs),
    {ok, Results}.

reduce(Server, ViewId, KVs) ->
    call(Server, <<"reduce">>, [ViewId, KVs]).

rereduce(Server, ViewId, Vals) ->
    call(Server, <<"rereduce">>, [ViewId, Vals]).

call(Ctx, Name, Args) ->
    case emonk:call(Ctx, Name, Args) of
        {ok, [<<"fatal">>, Error, Reason]} ->
            throw({Error, Reason});
        {ok, [<<"error">>, Error, Reason]} ->
            throw({Error, Reason});
        {ok, Response} ->
            {ok, Response};
        Error ->
            ?LOG_ERROR("Emonk error: ~p~n", [Error]),
            throw(Error)
    end.

get_js() ->
    case get(mr_support_js) of
        undefined ->
            JS = read_js(),
            put(mr_support_js, JS),
            JS;
        Else ->
            Else
    end.

read_js() ->
    FileName = couch_config:get(<<"view_server_emonk">>, <<"mapred_js">>),
    case file:read_file(FileName) of
        {ok, JS} ->
            JS;
        {error, Reason} ->
            Fmt = "Failed to read file (~p): ~p",
            Mesg = ?l2b(io_lib:format(Fmt, [Reason, FileName])),
            ?LOG_ERROR(Mesg, []),
            throw({error, Reason})
    end.
