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

-module(couch_view_server).

-export([is_lightweight/1, add_view_deps/2]).
-export([get_server/3, ret_server/1]).
-export([map/2, reduce/4, rereduce/4]).

% Used to determine if its ok to hold a handle
% to the server for an extended period of time.
is_lightweight({Module, _Server}) ->
    Module:is_lightweight();
is_lightweight(Lang) ->
    {Module, _} = get_module_arg(Lang),
    Module:is_lightweight().

get_server(Lang, Maps, Reds) ->
    {Module, Arg} = get_module_arg(Lang),
    OSReds = lists:map(fun([ViewId, RedFuns]) ->
        {Keepers, _} = lists:partition(fun
            (<<"_", _/binary>>) -> false;
            (_) -> true
        end, RedFuns),
        [ViewId, Keepers]
    end, Reds),
    {ok, Server} = Module:get_server(Arg, Maps, OSReds),
    {ok, {Module, Server}}.

get_module_arg(Lang) ->
    case couch_config:get(<<"view_servers">>, Lang) of
        undefined ->
            throw({error, unknown_view_language, Lang});
        TermStr ->
            {ok, {Module, Arg}} = couch_util:parse_term(TermStr),
            {Module, Arg}
    end.

ret_server(nil) ->
    ok;
ret_server({Module, Server}) ->
    Module:ret_server(Server).


% Concrete view servers can just return their argument, if the
% results only depends on the code in the view. Or they can have
% language-specific behavior, such as introspecing a runtime
% function (Obj-C), or normalizing whitespace (e.g. JavaScript).
add_view_deps(Lang, View) ->
    {Module, _} = get_module_arg(Lang),
    Module:add_view_deps(View).


map({Module, Server}, Docs) ->
    Module:map(Server, Docs).


reduce(_Lang, _ViewId, [], _KVs) ->
    {ok, []};
reduce({Module, Server}, ViewId, RedSrcs, KVs) ->
    {_OSFuns, BIFuns} = lists:partition(fun
        (<<"_", _/binary>>) -> false;
        (_OSFun) -> true
    end, RedSrcs),
    {ok, OSResults} = Module:reduce(Server, ViewId, KVs),
    {ok, BIResults} = builtin_reduce(reduce, BIFuns, KVs, []),
    Resp = recombine(RedSrcs, OSResults, BIResults, []),
    Resp;
reduce(Lang, ViewId, RedSrcs, KVs) ->
    % A heavy view server requires us to not keep a
    % handle to it for very long.
    {ok, {Module, Server}} = get_server(Lang, [], [[ViewId, RedSrcs]]),
    try reduce({Module, Server}, ViewId, RedSrcs, KVs) of
        {ok, Results} -> {ok, Results}
    after
        Module:ret_server(Server)
    end.


rereduce(_Spec, _ViewId, [], _ReducedValues) ->
    {ok, []};
rereduce({Module, Server}, ViewId, RedSrcs, ReducedValues) ->
    Grouped = group_reductions_results(ReducedValues),
    {OSFuns, BIFuns} = lists:partition(fun
        ({<<"_", _/binary>>, _}) -> false;
        ({_OSFun, _}) -> true
    end, lists:zip(RedSrcs, Grouped)),
    % Gather Value Lists
    OSValLists = lists:map(fun({_Fun, Vals}) -> Vals end, OSFuns),
    {ok, OSResults} = Module:rereduce(Server, ViewId, OSValLists),
    BIResults = lists:map(fun({Fun, Vals}) ->
        % Make the rereduce values look like KVs
        % for the builtins.
        KVedVals =  [[[], V] || V <- Vals],
        {ok, [BIRes]} = builtin_reduce(rereduce, [Fun], KVedVals, []),
        BIRes
    end, BIFuns),
    Resp = recombine(RedSrcs, OSResults, BIResults, []),
    Resp;
rereduce(Lang, ViewId, RedSrcs, ReducedValues) ->
    % A heavy view server requires us to not keep
    % a handle to it for very long.
    {ok, {Module, Server}} = get_server(Lang, [], [[ViewId, RedSrcs]]),
    try rereduce({Module, Server}, ViewId, RedSrcs, ReducedValues) of
        {ok, Results} -> {ok, Results}
    after
        Module:ret_server(Server)
    end.


% Implementations for the builtin reducers.
builtin_reduce(_Re, [], _KVs, Acc) ->
    {ok, lists:reverse(Acc)};
builtin_reduce(Re, [<<"_sum", _/binary>>|BuiltinReds], KVs, Acc) ->
    Sum = builtin_sum_rows(KVs),
    builtin_reduce(Re, BuiltinReds, KVs, [Sum|Acc]);
builtin_reduce(reduce, [<<"_count", _/binary>>|BuiltinReds], KVs, Acc) ->
    Count = length(KVs),
    builtin_reduce(reduce, BuiltinReds, KVs, [Count|Acc]);
builtin_reduce(rereduce, [<<"_count", _/binary>>|BuiltinReds], KVs, Acc) ->
    Count = builtin_sum_rows(KVs),
    builtin_reduce(rereduce, BuiltinReds, KVs, [Count|Acc]);
builtin_reduce(Re, [<<"_stats", _/binary>>|BuiltinReds], KVs, Acc) ->
    Stats = builtin_stats(Re, KVs),
    builtin_reduce(Re, BuiltinReds, KVs, [Stats|Acc]).

builtin_sum_rows(KVs) ->
    lists:foldl(fun
        ([_Key, Value], Acc) when is_number(Value), is_number(Acc) ->
            Acc + Value;
        ([_Key, Value], Acc) when is_list(Value), is_list(Acc) ->
            sum_terms(Acc, Value);
        ([_Key, Value], Acc) when is_number(Value), is_list(Acc) ->
            sum_terms(Acc, [Value]);
        ([_Key, Value], Acc) when is_list(Value), is_number(Acc) ->
            sum_terms([Acc], Value);
        (_Else, _Acc) ->
            throw({invalid_value, <<"builtin _sum function requires map values to be numbers or lists of numbers">>})
    end, 0, KVs).

sum_terms([], []) ->
    [];
sum_terms([_|_]=Xs, []) ->
    Xs;
sum_terms([], [_|_]=Ys) ->
    Ys;
sum_terms([X|Xs], [Y|Ys]) when is_number(X), is_number(Y) ->
    [X+Y | sum_terms(Xs,Ys)];
sum_terms(_, _) ->
    throw({invalid_value, <<"builtin _sum function requires map values to be numbers or lists of numbers">>}).

builtin_stats(reduce, [[_,First]|Rest]) when is_number(First) ->
    Stats = lists:foldl(fun
        ([_K,V], {S,C,Mi,Ma,Sq}) when is_number(V) ->
            {S+V, C+1, erlang:min(Mi,V), erlang:max(Ma,V), Sq+(V*V)};
        (_, _) ->
            throw({invalid_value,
                <<"builtin _stats function requires map values as numbers">>})
    end, {First, 1, First, First, First*First}, Rest),
    {Sum, Cnt, Min, Max, Sqr} = Stats,
    {[{sum,Sum}, {count,Cnt}, {min,Min}, {max,Max}, {sumsqr,Sqr}]};

builtin_stats(rereduce, [[_,First]|Rest]) ->
    {[{sum,Sum0}, {count,Cnt0}, {min,Min0}, {max,Max0}, {sumsqr,Sqr0}]} = First,
    Stats = lists:foldl(fun([_K,Red], {S,C,Mi,Ma,Sq}) ->
        {[{sum,Sum}, {count,Cnt}, {min,Min}, {max,Max}, {sumsqr,Sqr}]} = Red,
        {Sum+S, Cnt+C, erlang:min(Min,Mi), erlang:max(Max,Ma), Sqr+Sq}
    end, {Sum0,Cnt0,Min0,Max0,Sqr0}, Rest),
    {Sum, Cnt, Min, Max, Sqr} = Stats,
    {[{sum,Sum}, {count,Cnt}, {min,Min}, {max,Max}, {sumsqr,Sqr}]}.


% Re-order the reduced results so that the output follows
% the order they were defined in. We need this because of
% how the builtins and os reductions are split before
% calculating.
recombine([], [], [], Acc) ->
    {ok, lists:reverse(Acc)};
recombine([<<"_", _/binary>> | RedSrcs], OSResults, [BIR | BIResults], Acc) ->
    recombine(RedSrcs, OSResults, BIResults, [BIR | Acc]);
recombine([_OsFun | RedSrcs], [OSR | OSResults], BIResults, Acc) ->
    recombine(RedSrcs, OSResults, BIResults, [OSR | Acc]).


% Re-reduce is passed an array of arrays that represents the set
% of reduction values for the kp-pointers in a b-tree node. This
% looks something like such:
%
% [
%   [RedFun1, RedFun2, RedFun3],
%   [RedFun1, RedFun2, RedFun3],
%   ...
% ]
%
% We need to go through and transpose this list to an array of
% reduction values for the given reduce function. Ie, make the
% above list look like such:
%
% [
%   [RedFun1, RedFun1, ...],
%   [RedFun2, RedFun2, ...],
%   [RedFun3, RedFun3, ...],
% ]
% 
% That's what this function does by gathering the first element
% of each list recursively.
group_reductions_results([]) ->
    [];
group_reductions_results(List) ->
    {Heads, Tails} = lists:foldl(fun([H | T], {HAcc, TAcc}) ->
        {[H | HAcc], [T | TAcc]}
    end, {[], []}, List),
    case Tails of
        [[] | _] -> % no tails left
            [Heads];
        _ ->
            [Heads | group_reductions_results(Tails)]
    end.
