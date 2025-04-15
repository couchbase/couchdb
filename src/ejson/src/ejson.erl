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

-module(ejson).
-export([encode/1, decode/1, validate/1]).
-on_load(init/0).

init() ->
    SoName = case code:priv_dir(ejson) of
    {error, bad_name} ->
        case filelib:is_dir(filename:join(["..", "priv"])) of
        true ->
            filename:join(["..", "priv", "ejson"]);
        false ->
            filename:join(["priv", "ejson"])
        end;
    Dir ->
        filename:join(Dir, "ejson")
    end,
    erlang:load_nif(SoName, 0).

decode(IoList) ->
    nif_decode(IoList).


encode(EJson) ->
    nif_encode(EJson).


nif_decode(IoList) ->
    case reverse_tokens(IoList) of
    {ok, ReverseTokens} ->
        [[EJson]] = make_ejson(ReverseTokens, [[]]),
        EJson;
    Error ->
        throw({invalid_json, {Error, IoList}})
    end.


nif_encode({json, RawJson}) ->
    RawJson;
nif_encode(EJson) ->
    RevList = encode_rev(EJson),
    final_encode(lists:reverse(lists:flatten([RevList]))).


% Encode the json into a reverse list that's almost an iolist
% everything in the list is the final output except for tuples with
% {0, Strings} and {1, Floats}, which are to be converted to strings
% inside the NIF.
encode_rev({json, RawJson}) ->
    RawJson;
encode_rev(true) ->
    <<"true">>;
encode_rev(false) ->
    <<"false">>;
encode_rev(null) ->
    <<"null">>;
encode_rev(I) when is_integer(I) ->
    list_to_binary(integer_to_list(I));
encode_rev(S) when is_binary(S) ->
    {0, S};
encode_rev(S) when is_atom(S) ->
    {0, list_to_binary(atom_to_list(S))};
encode_rev(F) when is_float(F) ->
    {1, F};
encode_rev({Props}) when is_list(Props) ->
    encode_proplist_rev(Props, [<<"{">>]);
encode_rev(Array) when is_list(Array) ->
    encode_array_rev(Array, [<<"[">>]);
encode_rev(Bad) ->
    throw({json_encode, {bad_term, Bad}}).


encode_array_rev([], Acc) ->
    [<<"]">> | Acc];
encode_array_rev([Val | Rest], [<<"[">>]) ->
    encode_array_rev(Rest, [encode_rev(Val), <<"[">>]);
encode_array_rev([Val | Rest], Acc) ->
    encode_array_rev(Rest, [encode_rev(Val), <<",">> | Acc]).


encode_proplist_rev([], Acc) ->
    [<<"}">> | Acc];
encode_proplist_rev([{Key,Val} | Rest], [<<"{">>]) ->
    encode_proplist_rev(
        Rest, [encode_rev(Val), <<":">>, {0, as_binary(Key)}, <<"{">>]);
encode_proplist_rev([{Key,Val} | Rest], Acc) ->
    encode_proplist_rev(
        Rest, [encode_rev(Val), <<":">>, {0, as_binary(Key)}, <<",">> | Acc]).

as_binary(B) when is_binary(B) ->
    B;
as_binary(A) when is_atom(A) ->
    list_to_binary(atom_to_list(A));
as_binary(L) when is_list(L) ->
    list_to_binary(L).


make_ejson([], Stack) ->
    Stack;
make_ejson([0 | RevEvs], [ArrayValues, PrevValues | RestStack]) ->
    % 0 ArrayStart
    make_ejson(RevEvs, [[ArrayValues | PrevValues] | RestStack]);
make_ejson([1 | RevEvs], Stack) ->
    % 1 ArrayEnd
    make_ejson(RevEvs, [[] | Stack]);
make_ejson([2 | RevEvs], [ObjValues, PrevValues | RestStack]) ->
    % 2 ObjectStart
    make_ejson(RevEvs, [[{ObjValues} | PrevValues] | RestStack]);
make_ejson([3 | RevEvs], Stack) ->
    % 3 ObjectEnd
    make_ejson(RevEvs, [[] | Stack]);
make_ejson([{0, Value} | RevEvs], [Vals | RestStack] = _Stack) ->
    % {0, IntegerString}
    make_ejson(RevEvs, [[list_to_integer(binary_to_list(Value)) | Vals] | RestStack]);
make_ejson([{1, Value} | RevEvs], [Vals | RestStack] = _Stack) ->
    % {1, FloatString}
    make_ejson(RevEvs, [[list_to_float(binary_to_list(Value)) | Vals] | RestStack]);
make_ejson([{3, String} | RevEvs], [[PrevValue|RestObject] | RestStack] = _Stack) ->
    % {3 , ObjectKey}
    make_ejson(RevEvs, [[{String, PrevValue}|RestObject] | RestStack]);
make_ejson([Value | RevEvs], [Vals | RestStack] = _Stack) ->
    make_ejson(RevEvs, [[Value | Vals] | RestStack]).


reverse_tokens(_) ->
    erlang:nif_error(ejson_nif_not_loaded).

final_encode(_) ->
    erlang:nif_error(ejson_nif_not_loaded).

validate(_) ->
    erlang:nif_error(ejson_nif_not_loaded).
