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

-module(couch_set_view_mapreduce).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-export([start_map_context/1, start_reduce_context/1]).
-export([end_map_context/0, end_reduce_context/1]).
-export([map/1, reduce/2, reduce/3, rereduce/2, rereduce/3]).
-export([builtin_reduce/3]).
-export([validate_ddoc_views/1]).

-define(STATS_ERROR_MSG, <<"Builtin _stats function requires map values to be numbers">>).
-define(SUM_ERROR_MSG,   <<"Builtin _sum function requires map values to be numbers">>).


start_map_context(#set_view_group{views = Views}) ->
    {ok, Ctx} = mapreduce:start_map_context([View#set_view.def || View <- Views]),
    erlang:put(map_context, Ctx),
    ok.


end_map_context() ->
    erlang:erase(map_context),
    ok.


start_reduce_context(#set_view_group{views = Views}) ->
    lists:foreach(fun start_reduce_context/1, Views);

start_reduce_context(#set_view{ref = Ref, reduce_funs = RedFuns}) ->
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


end_reduce_context(#set_view_group{views = Views}) ->
    lists:foreach(fun end_reduce_context/1, Views);

end_reduce_context(#set_view{ref = Ref}) ->
    erlang:erase({reduce_context, Ref}),
    ok.


map(Doc) ->
    Ctx = erlang:get(map_context),
    {DocBody, DocMeta} = couch_doc:to_raw_json_binary_views(Doc),
    case mapreduce:map_doc(Ctx, DocBody, DocMeta) of
    {ok, _Results} = Ok ->
        Ok;
    Error ->
        throw(Error)
    end.


reduce(#set_view{reduce_funs = []}, _KVs) ->
    {ok, []};
reduce(#set_view{ref = Ref, reduce_funs = RedFuns}, KVs0) ->
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
        {ok, JsResults} ->
            recombine_reduce_results(RedFunSources, JsResults, NativeResults, []);
        Error ->
            throw(Error)
        end
    end.


reduce(#set_view{reduce_funs = []}, _NthRed, _KVs) ->
    {ok, []};
reduce(#set_view{ref = Ref, reduce_funs = RedFuns}, NthRed, KVs0) ->
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
            {ok, [ReduceValue]};
        Error ->
            throw(Error)
        end
    end.


rereduce(#set_view{reduce_funs = []}, _ReducedValues) ->
    {ok, []};
rereduce(#set_view{ref = Ref, reduce_funs = RedFuns}, ReducedValues) ->
    Grouped = group_reductions_results(ReducedValues),
    Ctx = erlang:get({reduce_context, Ref}),
    Results = lists:zipwith(
        fun({native, FunSrc}, Values) ->
            {ok, [Result]} = builtin_reduce(rereduce, [FunSrc], [{[], V} || V <- Values], []),
            Result;
        (Idx, Values) ->
            case mapreduce:rereduce(Ctx, Idx, Values) of
            {ok, Reduction} ->
                Reduction;
            Error ->
                throw(Error)
            end
        end, reduce_fun_indexes(RedFuns), Grouped),
    {ok, Results}.


rereduce(#set_view{reduce_funs = []}, _NthRed, _ReducedValues) ->
    {ok, []};
rereduce(#set_view{ref = Ref, reduce_funs = RedFuns}, NthRed, ReducedValues) ->
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
        case mapreduce:rereduce(Ctx, NthRed2, Values) of
        {ok, ReduceValue} ->
            {ok, [ReduceValue]};
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


builtin_reduce(ReduceType, FunSrcs, Values) ->
    builtin_reduce(ReduceType, FunSrcs, Values, []).

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
    Json = ?JSON_ENCODE(length(KVs)),
    builtin_reduce(reduce, BuiltinReds, KVs, [Json | Acc]);
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
    throw({error, <<"Invalid builtin reduce function: ", InvalidBuiltin/binary>>}).


parse_number(NumberBin, ErrorMsg) when is_binary(NumberBin) ->
    parse_number(?b2l(NumberBin), ErrorMsg);
parse_number(NumberStr, ErrorMsg) ->
    case (catch list_to_integer(NumberStr)) of
    {'EXIT', {badarg, _Stack}} ->
        case (catch list_to_float(NumberStr)) of
        {'EXIT', {badarg, _Stack2}} ->
            throw({error, ErrorMsg});
        Float ->
            Float
        end;
    Int ->
        Int
    end.


builtin_sum_rows(KVs) ->
    Result = lists:foldl(fun({_Key, Value}, SumAcc) ->
        parse_number(Value, ?SUM_ERROR_MSG) + SumAcc
    end, 0, KVs),
    ?JSON_ENCODE(Result).


builtin_stats(reduce, []) ->
    <<"{}">>;

builtin_stats(reduce, [{_, First0} | Rest]) ->
    First = parse_number(First0, ?STATS_ERROR_MSG),
    {Sum, Cnt, Min, Max, Sqr} = lists:foldl(fun({_K, V0}, {S, C , Mi, Ma, Sq}) ->
        V = parse_number(V0, ?STATS_ERROR_MSG),
        {S + V, C + 1, erlang:min(Mi, V), erlang:max(Ma, V), Sq + (V * V)}
    end, {First, 1, First, First, First * First}, Rest),
    Result = {[
        {<<"sum">>, Sum},
        {<<"count">>, Cnt},
        {<<"min">>, Min},
        {<<"max">>, Max},
        {<<"sumsqr">>, Sqr}
    ]},
    ?JSON_ENCODE(Result);

builtin_stats(rereduce, [{_, First} | Rest]) ->
    {[{<<"sum">>, Sum0},
      {<<"count">>, Cnt0},
      {<<"min">>, Min0},
      {<<"max">>, Max0},
      {<<"sumsqr">>, Sqr0}]} = ?JSON_DECODE(First),
    {Sum, Cnt, Min, Max, Sqr} = lists:foldl(fun({_K, Red}, {S, C, Mi, Ma, Sq}) ->
        {[{<<"sum">>, Sum},
          {<<"count">>, Cnt},
          {<<"min">>, Min},
          {<<"max">>, Max},
          {<<"sumsqr">>, Sqr}]} = ?JSON_DECODE(Red),
        {Sum + S, Cnt + C, erlang:min(Min, Mi), erlang:max(Max, Ma), Sqr + Sq}
    end, {Sum0, Cnt0, Min0, Max0, Sqr0}, Rest),
    Result = {[
        {<<"sum">>, Sum},
        {<<"count">>, Cnt},
        {<<"min">>, Min},
        {<<"max">>, Max},
        {<<"sumsqr">>, Sqr}
    ]},
    ?JSON_ENCODE(Result).


contract_kvs([], Acc) ->
    lists:reverse(Acc);
contract_kvs([KV | Rest], Acc) ->
    {KeyId, <<_PartId:16, Value/binary>>} = KV,
    contract_kvs(Rest, [{KeyId, Value} | Acc]).


encode_kvs([], Acc) ->
    lists:reverse(Acc);
encode_kvs([KV | Rest], Acc) ->
    {KeyDocId, <<_PartId:16, Value/binary>>} = KV,
    {Key, _DocId} = couch_set_view_util:split_key_docid(KeyDocId),
    encode_kvs(Rest, [{Key, Value} | Acc]).


validate_ddoc_views(#doc{body = {Body}}) ->
    Views = couch_util:get_value(<<"views">>, Body, {[]}),
    case Views of
    {L} when is_list(L) ->
        ok;
    _ ->
        throw({error, <<"The field `views' is not a json object.">>})
    end,
    lists:foreach(
        fun({ViewName, Value}) ->
            validate_view_definition(ViewName, Value)
        end,
        element(1, Views)).


validate_view_definition(<<"">>, _) ->
    throw({error, <<"View name cannot be an empty string">>});
validate_view_definition(ViewName, {ViewProps}) when is_list(ViewProps) ->
    validate_view_name(ViewName, iolist_to_binary(io_lib:format(
        "View name `~s` cannot have leading or trailing whitespace",
        [ViewName]))),
    MapDef = couch_util:get_value(<<"map">>, ViewProps),
    validate_view_map_function(ViewName, MapDef),
    ReduceDef = couch_util:get_value(<<"reduce">>, ViewProps),
    validate_view_reduce_function(ViewName, ReduceDef);
validate_view_definition(ViewName, _) ->
    ErrorMsg = io_lib:format("Value for view `~s' is not "
                             "a json object.", [ViewName]),
    throw({error, iolist_to_binary(ErrorMsg)}).


% Make sure the view name doesn't contain leading or trailing whitespace
% (space, tab, newline or carriage return)
validate_view_name(<<" ", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_view_name(<<"\t", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_view_name(<<"\n", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_view_name(<<"\r", _Rest/binary>>, ErrorMsg) ->
    throw({error, ErrorMsg});
validate_view_name(Bin, ErrorMsg) when size(Bin) > 1 ->
    Size = size(Bin) - 1 ,
    <<_:Size/binary, Trailing/bits>> = Bin,
    % Check for trailing whitespace
    validate_view_name(Trailing, ErrorMsg);
validate_view_name(_, _) ->
    ok.


validate_view_map_function(ViewName, undefined) ->
    ErrorMsg = io_lib:format("The `map' field for the view `~s' is missing.",
                             [ViewName]),
    throw({error, iolist_to_binary(ErrorMsg)});
validate_view_map_function(ViewName, MapDef) when not is_binary(MapDef) ->
    ErrorMsg = io_lib:format("The `map' field of the view `~s' is not "
                             "a json string.", [ViewName]),
    throw({error, iolist_to_binary(ErrorMsg)});
validate_view_map_function(ViewName, MapDef) ->
    case mapreduce:start_map_context([MapDef]) of
    {ok, _Ctx} ->
        ok;
    {error, Reason} ->
        ErrorMsg = io_lib:format("Syntax error in the map function of"
                                 " the view `~s': ~s", [ViewName, Reason]),
        throw({error, iolist_to_binary(ErrorMsg)})
    end.


validate_view_reduce_function(_ViewName, undefined) ->
    ok;
validate_view_reduce_function(ViewName, ReduceDef) when not is_binary(ReduceDef) ->
    ErrorMsg = io_lib:format("The `reduce' field of the view `~s' is not "
                             "a json string.", [ViewName]),
    throw({error, iolist_to_binary(ErrorMsg)});
validate_view_reduce_function(_ViewName, <<"_count">>) ->
    ok;
validate_view_reduce_function(_ViewName, <<"_sum">>) ->
    ok;
validate_view_reduce_function(_ViewName, <<"_stats">>) ->
    ok;
validate_view_reduce_function(ViewName, <<"_", _/binary>> = ReduceDef) ->
    ErrorMsg = io_lib:format("Invalid built-in reduce function "
                             "for view `~s': ~s",
                             [ViewName, ReduceDef]),
    throw({error, iolist_to_binary(ErrorMsg)});
validate_view_reduce_function(ViewName, ReduceDef) ->
    case mapreduce:start_reduce_context([ReduceDef]) of
    {ok, _Ctx} ->
        ok;
    {error, Reason} ->
        ErrorMsg = io_lib:format("Syntax error in the reduce function of"
                                 " the view `~s': ~s", [ViewName, Reason]),
        throw({error, iolist_to_binary(ErrorMsg)})
    end.
