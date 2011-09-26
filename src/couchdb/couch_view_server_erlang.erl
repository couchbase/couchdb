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
%
% This file drew much inspiration from erlview, which was written by and
% copyright Michael McDaniel [http://autosys.us], and is also under APL 2.0
%
%
% This module provides the smallest possible native view-server.
% With this module in-place, you can add the following to your couch INI files:
%  [native_query_servers]
%  erlang={couch_native_process, start_link, []}
%
% Which will then allow following example map function to be used:
%
%  fun({Doc}) ->
%    % Below, we emit a single record - the _id as key, null as value
%    DocId = couch_util:get_value(Doc, <<"_id">>, null),
%    Emit(DocId, null)
%  end.
%
% which should be roughly the same as the javascript:
%    emit(doc._id, null);
%
% This module exposes enough functions such that a native erlang server can
% act as a fully-fleged view server, but no 'helper' functions specifically
% for simplifying your erlang view code.  It is expected other third-party
% extensions will evolve which offer useful layers on top of this view server
% to help simplify your view code.
-module(couch_view_server_erlang).

-export([is_lightweight/0, add_view_deps/1]).
-export([get_server/3, ret_server/1]).
-export([map/2, reduce/3, rereduce/3]).

-include("couch_db.hrl").

is_lightweight() ->
    true.

add_view_deps(View) ->
    View.

get_server(_Arg, Maps, Reds) ->
    MapFuns = [compile(map, MapSrc) || MapSrc <- Maps],
    RedFuns = lists:map(fun([ViewId, RedSrcs]) ->
        {ViewId, [compile(red, RedSrc) || RedSrc <- RedSrcs]}
    end, Reds),
    {ok, {MapFuns, RedFuns}}.

ret_server(_) ->
    ok.

map({MapFuns, _}, Docs) ->
    Results = lists:map(fun(Doc) ->
        Json = to_binary(couch_doc:to_json_obj(Doc, [])),

        FunsResults = lists:map(fun(Fun) -> Fun(Json) end, MapFuns),
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

reduce({_, RedFuns}, ViewId, KVs) ->
    ViewReds = proplists:get_value(ViewId, RedFuns),
    {Keys, Vals} =
    lists:foldl(fun([K, V], {KAcc, VAcc}) ->
        {[K | KAcc], [V | VAcc]}
    end, {[], []}, KVs),
    run_reduce(ViewReds, lists:reverse(Keys), lists:reverse(Vals), false).

rereduce({_, RedFuns}, ViewId, Vals) ->
    ViewReds = proplists:get_value(ViewId, RedFuns),
    run_reduce(ViewReds, null, Vals, true).

run_reduce(RedFuns, Keys, Vals, ReReduce) ->
    Reds = lists:map(fun(Fun) ->
        Fun(Keys, Vals, ReReduce)
    end, RedFuns),
    {ok, Reds}.

% thanks to erlview, via:
% http://erlang.org/pipermail/erlang-questions/2003-November/010544.html
compile(map, Source) ->
    {Sig, Fun} = compile(Source),
    fun(Doc) ->
        put(Sig, []),
        Fun(Doc),
        Resp = get(Sig),
        put(Sig, undefined),
        Resp
    end;
compile(red, Source) ->
    {_Sig, Fun} = compile(Source),
    Fun.

compile(Source) ->
    Sig = couch_util:md5(Source),
    BindFuns = bindings(Sig),
    FunStr = binary_to_list(Source),
    {ok, Tokens, _} = erl_scan:string(FunStr),
    Form = case (catch erl_parse:parse_exprs(Tokens)) of
        {ok, [ParsedForm]} ->
            ParsedForm;
        {error, {LineNum, _Mod, [Mesg, Params]}}=Error ->
            io:format(standard_error, "Syntax error on line: ~p~n", [LineNum]),
            io:format(standard_error, "~s~p~n", [Mesg, Params]),
            throw(Error)
    end,
    Bindings = lists:foldl(fun({Name, Fun}, Acc) ->
        erl_eval:add_binding(Name, Fun, Acc)
    end, erl_eval:new_bindings(), BindFuns),
    {value, Fun, _} = erl_eval:expr(Form, Bindings),
    {Sig, Fun}.

bindings(Sig) ->
    Log = fun(Msg) ->
        ?LOG_INFO(Msg, [])
    end,
    Emit = fun(Id, Value) ->
        Curr = erlang:get(Sig),
        erlang:put(Sig, [[Id, Value] | Curr])
    end,
    [{'Log', Log}, {'Emit', Emit}].

to_binary({Data}) ->
    Pred = fun({Key, Value}) ->
        {to_binary(Key), to_binary(Value)}
    end,
    {lists:map(Pred, Data)};
to_binary(Data) when is_list(Data) ->
    [to_binary(D) || D <- Data];
to_binary(null) ->
    null;
to_binary(true) ->
    true;
to_binary(false) ->
    false;
to_binary(Data) when is_atom(Data) ->
    list_to_binary(atom_to_list(Data));
to_binary(Data) ->
    Data.
