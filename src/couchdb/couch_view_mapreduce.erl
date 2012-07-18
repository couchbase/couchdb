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

-module(couch_view_mapreduce).

-include("couch_db.hrl").

-export([start_map_context/1, start_reduce_context/1]).
-export([end_map_context/0, end_reduce_context/1]).
-export([map/1, reduce/2, reduce/3, rereduce/2, rereduce/3]).


start_map_context(#group{views = Views}) ->
    {ok, Ctx} = mapreduce:start_map_context([View#view.def || View <- Views]),
    erlang:put(map_context, Ctx),
    ok.


end_map_context() ->
    erlang:erase(map_context),
    ok.


start_reduce_context(#group{views = Views}) ->
    lists:foreach(fun start_reduce_context/1, Views);

start_reduce_context(#view{ref = Ref, reduce_funs = RedFuns}) ->
    FunSrcs = lists:foldr(
        fun({_Name, <<"_", _/binary>>}, Acc) ->
            Acc;
        ({_Name, Src}, Acc) ->
            [Src | Acc]
        end,
        [], RedFuns),
    case FunSrcs of
    [] ->
        ok;
    _ ->
        {ok, Ctx} = mapreduce:start_reduce_context(FunSrcs),
        erlang:put({reduce_context, Ref}, Ctx),
        ok
    end.


end_reduce_context(#group{views = Views}) ->
    lists:foreach(fun end_reduce_context/1, Views);

end_reduce_context(#view{ref = Ref}) ->
    erlang:erase({reduce_context, Ref}),
    ok.


map(Doc) ->
    Ctx = erlang:get(map_context),
    DocBin = couch_doc:to_raw_json_binary(Doc),
    case mapreduce:map_doc(Ctx, DocBin) of
    {ok, Results} ->
        {ok, [
            [{?JSON_DECODE(K), ?JSON_DECODE(V)} || {K, V} <- FunResult]
                || FunResult <- Results
        ]};
    Error ->
        throw(Error)
    end.


reduce(#view{reduce_funs = []}, _KVs) ->
    {ok, []};
reduce(#view{ref = Ref, reduce_funs = RedFuns}, KVs0) ->
    RedFunSources = [FunSource || {_Name, FunSource} <- RedFuns],
    {NativeFuns, JsFuns} = lists:partition(
        fun(<<"_", _/binary>>) -> true; (_) -> false end,
        RedFunSources),
    case JsFuns of
    [] ->
        builtin_reduce(reduce, NativeFuns, KVs0, []);
    _ ->
        KVs = encode_kvs(KVs0, []),
        {ok, NativeResults} = builtin_reduce(reduce, NativeFuns, KVs, []),
        Ctx = erlang:get({reduce_context, Ref}),
        case mapreduce:reduce(Ctx, KVs) of
        {ok, JsResults0} ->
            JsResults = [?JSON_DECODE(R) || R <- JsResults0],
            recombine_reduce_results(RedFunSources, JsResults, NativeResults, []);
        Error ->
            throw(Error)
        end
    end.


reduce(#view{reduce_funs = []}, _NthRed, _KVs) ->
    {ok, []};
reduce(#view{ref = Ref, reduce_funs = RedFuns}, NthRed, KVs0) ->
    {Before, [{_Name, FunSrc} | _]} = lists:split(NthRed - 1, RedFuns),
    case FunSrc of
    <<"_", _/binary>> ->
        builtin_reduce(reduce, [FunSrc], KVs0, []);
    _ ->
        KVs = encode_kvs(KVs0, []),
        Ctx = erlang:get({reduce_context, Ref}),
        NthRed2 = lists:foldl(
            fun(<<"_", _/binary>>, Acc) ->
                    Acc - 1;
                (_, Acc) ->
                    Acc
            end,
            NthRed,
            Before),
        case mapreduce:reduce(Ctx, NthRed2, KVs) of
        {ok, ReduceValue} ->
            {ok, [?JSON_DECODE(ReduceValue)]};
        Error ->
            throw(Error)
        end
    end.


rereduce(#view{reduce_funs = []}, _ReducedValues) ->
    {ok, []};
rereduce(#view{ref = Ref, reduce_funs = RedFuns}, ReducedValues) ->
    Grouped = group_reductions_results(ReducedValues),
    Ctx = erlang:get({reduce_context, Ref}),
    Results = lists:zipwith(
        fun({native, FunSrc}, Values) ->
            {ok, [Result]} = builtin_reduce(rereduce, [FunSrc], [{[], V} || V <- Values], []),
            Result;
        (Idx, Values) ->
            case mapreduce:rereduce(Ctx, Idx, [?JSON_ENCODE(V) || V <- Values]) of
            {ok, Reduction} ->
                ?JSON_DECODE(Reduction);
            Error ->
                throw(Error)
            end
        end, reduce_fun_indexes(RedFuns), Grouped),
    {ok, Results}.


rereduce(#view{reduce_funs = []}, _NthRed, _ReducedValues) ->
    {ok, []};
rereduce(#view{ref = Ref, reduce_funs = RedFuns}, NthRed, ReducedValues) ->
    {Before, [{_Name, FunSrc} | _]} = lists:split(NthRed - 1, RedFuns),
    [Values] = group_reductions_results(ReducedValues),
    case FunSrc of
    <<"_", _/binary>> ->
        builtin_reduce(rereduce, [FunSrc], [{[], V} || V <- Values], []);
    _ ->
        Ctx = erlang:get({reduce_context, Ref}),
        NthRed2 = lists:foldl(
            fun(<<"_", _/binary>>, Acc) ->
                    Acc - 1;
                (_, Acc) ->
                    Acc
            end,
            NthRed,
            Before),
        case mapreduce:rereduce(Ctx, NthRed2, [?JSON_ENCODE(V) || V <- Values]) of
        {ok, ReduceValue} ->
            {ok, [?JSON_DECODE(ReduceValue)]};
        Error ->
            throw(Error)
        end
    end.


reduce_fun_indexes(RedFuns) ->
    {L, _} = lists:mapfoldl(
        fun({_Name, <<"_", _/binary>> = Src}, Idx) ->
                {{native, Src}, Idx};
            ({_Name, _JsSrc}, Idx) ->
                {Idx, Idx + 1}
        end,
        1, RedFuns),
    lists:reverse(L).


recombine_reduce_results([], [], [], Acc) ->
    {ok, lists:reverse(Acc)};
recombine_reduce_results([<<"_", _/binary>> | RedSrcs], JsResults, [BRes | BuiltinResults], Acc) ->
    recombine_reduce_results(RedSrcs, JsResults, BuiltinResults, [BRes | Acc]);
recombine_reduce_results([_JsFun | RedSrcs], [JsR | JsResults], BuiltinResults, Acc) ->
    recombine_reduce_results(RedSrcs, JsResults, BuiltinResults, [JsR | Acc]).


group_reductions_results([]) ->
    [];
group_reductions_results(List) ->
    {Heads, Tails} = lists:foldl(
        fun([H | T], {HAcc, TAcc}) ->
            {[H | HAcc], [T | TAcc]}
        end,
        {[], []}, List),
    case Tails of
    [[] | _] -> % no tails left
        [Heads];
    _ ->
        [Heads | group_reductions_results(Tails)]
    end.


builtin_reduce(_Re, [], _KVs, Acc) ->
    {ok, lists:reverse(Acc)};
builtin_reduce(Re, [<<"_sum", _/binary>> | BuiltinReds], KVs, Acc) ->
    case Re of
    reduce ->
        KVs2 = contract_kvs(KVs, []);
    rereduce ->
        KVs2 = KVs
    end,
    Sum = builtin_sum_rows(KVs2),
    builtin_reduce(Re, BuiltinReds, KVs, [Sum | Acc]);
builtin_reduce(reduce, [<<"_count", _/binary>> | BuiltinReds], KVs, Acc) ->
    Count = length(KVs),
    builtin_reduce(reduce, BuiltinReds, KVs, [Count | Acc]);
builtin_reduce(rereduce, [<<"_count", _/binary>> | BuiltinReds], KVs, Acc) ->
    Count = builtin_sum_rows(KVs),
    builtin_reduce(rereduce, BuiltinReds, KVs, [Count | Acc]);
builtin_reduce(Re, [<<"_stats", _/binary>> | BuiltinReds], KVs, Acc) ->
    case Re of
    reduce ->
        KVs2 = contract_kvs(KVs, []);
    rereduce ->
        KVs2 = KVs
    end,
    Stats = builtin_stats(Re, KVs2),
    builtin_reduce(Re, BuiltinReds, KVs, [Stats | Acc]);
builtin_reduce(_Re, [InvalidBuiltin | _BuiltinReds], _KVs, _Acc) ->
    throw({invalid_builtin_reduce_function, InvalidBuiltin}).


builtin_sum_rows(KVs) ->
    lists:foldl(fun
        ({_Key, Value}, Acc) when is_number(Value), is_number(Acc) ->
            Acc + Value;
        ({_Key, Value}, Acc) when is_list(Value), is_list(Acc) ->
            sum_terms(Acc, Value);
        ({_Key, Value}, Acc) when is_number(Value), is_list(Acc) ->
            sum_terms(Acc, [Value]);
        ({_Key, Value}, Acc) when is_list(Value), is_number(Acc) ->
            sum_terms([Acc], Value);
        (_Else, _Acc) ->
            throw({invalid_value, <<"builtin _sum function requires map values to be numbers or lists of numbers">>})
    end, 0, KVs).

sum_terms([], []) ->
    [];
sum_terms([_ | _] = Xs, []) ->
    Xs;
sum_terms([], [_ | _] = Ys) ->
    Ys;
sum_terms([X | Xs], [Y | Ys]) when is_number(X), is_number(Y) ->
    [X + Y | sum_terms(Xs, Ys)];
sum_terms(_, _) ->
    throw({invalid_value, <<"builtin _sum function requires map values to be numbers or lists of numbers">>}).

builtin_stats(reduce, []) ->
    {[]};
builtin_stats(reduce, [{_, First} | Rest]) when is_number(First) ->
    Stats = lists:foldl(fun({_K, V}, {S, C , Mi, Ma, Sq}) when is_number(V) ->
        {S + V, C + 1, erlang:min(Mi, V), erlang:max(Ma, V), Sq + (V * V)};
    (_, _) ->
        throw({invalid_value,
            <<"builtin _stats function requires map values to be numbers">>})
    end, {First, 1, First, First, First * First}, Rest),
    {Sum, Cnt, Min, Max, Sqr} = Stats,
    {[{<<"sum">>, Sum}, {<<"count">>, Cnt}, {<<"min">>, Min}, {<<"max">>, Max}, {<<"sumsqr">>, Sqr}]};
builtin_stats(reduce, KVs) when is_list(KVs) ->
    Msg = <<"builtin _stats function requires map values to be numbers">>,
    throw({invalid_value, Msg});

builtin_stats(rereduce, [{_, First} | Rest]) ->
    {[{<<"sum">>, Sum0}, {<<"count">>, Cnt0}, {<<"min">>, Min0}, {<<"max">>, Max0}, {<<"sumsqr">>, Sqr0}]} = First,
    Stats = lists:foldl(fun({_K, Red}, {S, C, Mi, Ma, Sq}) ->
        {[{<<"sum">>, Sum}, {<<"count">>, Cnt}, {<<"min">>, Min}, {<<"max">>, Max}, {<<"sumsqr">>, Sqr}]} = Red,
        {Sum + S, Cnt + C, erlang:min(Min, Mi), erlang:max(Max, Ma), Sqr + Sq}
    end, {Sum0, Cnt0, Min0, Max0, Sqr0}, Rest),
    {Sum, Cnt, Min, Max, Sqr} = Stats,
    {[{<<"sum">>, Sum}, {<<"count">>, Cnt}, {<<"min">>, Min}, {<<"max">>, Max}, {<<"sumsqr">>, Sqr}]}.


contract_kvs([], Acc) ->
    lists:reverse(Acc);
contract_kvs([KV | Rest], Acc) ->
    {{Key, Id}, Value} = KV,
    NKV = {[Key, Id], Value},
    contract_kvs(Rest, [NKV | Acc]).

encode_kvs([], Acc) ->
    lists:reverse(Acc);
encode_kvs([KV | Rest], Acc) ->
    {{Key, Id}, Value} = KV,
    NKV = {?JSON_ENCODE([Key, Id]), ?JSON_ENCODE(Value)},
    encode_kvs(Rest, [NKV | Acc]).
