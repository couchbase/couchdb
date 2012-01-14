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

-module(couch_set_view_util).

-export([detuple_kvs/2, expand_dups/2, expand_dups/3, partitions_map/2]).
-export([build_bitmask/1, decode_bitmask/1]).
-export([make_btree_purge_fun/1]).
-export([make_key_options/1]).

-include_lib("couch_set_view/include/couch_set_view.hrl").


detuple_kvs([], Acc) ->
    lists:reverse(Acc);
detuple_kvs([KV | Rest], Acc) ->
    {{Key,Id}, {_PartId, Value}} = KV,
    NKV = [[Key, Id], Value],
    detuple_kvs(Rest, [NKV | Acc]).


expand_dups([], Acc) ->
    lists:reverse(Acc);
expand_dups([{Key, {dups, Vals}} | Rest], Acc) ->
    Expanded = lists:map(fun({PartId, Val}) -> {Key, {PartId, Val}} end, Vals),
    expand_dups(Rest, Expanded ++ Acc);
expand_dups([{_Key, {_PartId, _Val}} = Kv | Rest], Acc) ->
    expand_dups(Rest, [Kv | Acc]).


expand_dups([], _Abitmask, Acc) ->
    lists:reverse(Acc);
expand_dups([{Key, {dups, [{PartId, _} | _] = Vals}} | Rest], Abitmask, Acc) ->
    case (1 bsl PartId) band Abitmask of
    0 ->
        expand_dups(Rest, Abitmask, Acc);
    _ ->
        Expanded = lists:map(fun({_PartId, _Val} = V) -> {Key, V} end, Vals),
        expand_dups(Rest, Abitmask, Expanded ++ Acc)
    end;
expand_dups([{_Key, {PartId, _Val}} = Kv | Rest], Abitmask, Acc) ->
    case (1 bsl PartId) band Abitmask of
    0 ->
        expand_dups(Rest, Abitmask, Acc);
    _ ->
        expand_dups(Rest, Abitmask, [Kv | Acc])
    end.


partitions_map([], BitMap) ->
    BitMap;
partitions_map([{_Key, {dups, [{PartitionId, _Val} | _]}} | RestKvs], BitMap) ->
    partitions_map(RestKvs, BitMap bor (1 bsl PartitionId));
partitions_map([{_Key, {PartitionId, _Val}} | RestKvs], BitMap) ->
    partitions_map(RestKvs, BitMap bor (1 bsl PartitionId)).


build_bitmask(ActiveList) ->
    build_bitmask(ActiveList, 0).

build_bitmask([], Acc) ->
    Acc;
build_bitmask([PartId | Rest], Acc) when is_integer(PartId), PartId >= 0 ->
    build_bitmask(Rest, (1 bsl PartId) bor Acc).


decode_bitmask(Bitmask) ->
    decode_bitmask(Bitmask, 0).

decode_bitmask(0, _) ->
    [];
decode_bitmask(Bitmask, PartId) ->
    case Bitmask band 1 of
    1 ->
        [PartId | decode_bitmask(Bitmask bsr 1, PartId + 1)];
    0 ->
        decode_bitmask(Bitmask bsr 1, PartId + 1)
    end.


make_btree_purge_fun(Group) when ?set_cbitmask(Group) =/= 0 ->
    fun(Type, Value, {go, Acc}) ->
        receive
        stop ->
            {stop, {stop, Acc}}
        after 0 ->
            btree_purge_fun(Type, Value, {go, Acc}, ?set_cbitmask(Group))
        end
    end.

btree_purge_fun(value, {_K, {PartId, _}}, {go, Acc}, Cbitmask) ->
    Mask = 1 bsl PartId,
    case (Cbitmask band Mask) of
    Mask ->
        {purge, {go, Acc + 1}};
    0 ->
        {keep, {go, Acc}}
    end;
btree_purge_fun(branch, Red, {go, Acc}, Cbitmask) ->
    Bitmap = element(tuple_size(Red), Red),
    case Bitmap band Cbitmask of
    0 ->
        {keep, {go, Acc}};
    Bitmap ->
        {purge, {go, Acc + element(1, Red)}};
    _ ->
        {partial_purge, {go, Acc}}
    end.


make_key_options(QueryArgs) ->
    couch_httpd_view:make_key_options(QueryArgs).
