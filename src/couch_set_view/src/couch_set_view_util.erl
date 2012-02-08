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
-export([design_doc_to_set_view_group/2, get_ddoc_ids_with_sig/2]).
-export([open_raw_read_fd/2, close_raw_read_fd/1]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").


detuple_kvs([], Acc) ->
    lists:reverse(Acc);
detuple_kvs([KV | Rest], Acc) ->
    {{Key,Id}, {_PartId, Value}} = KV,
    NKV = [[Key, Id], Value],
    detuple_kvs(Rest, [NKV | Acc]).


expand_dups([], Acc) ->
    lists:reverse(Acc);
expand_dups([{Key, {PartId, {dups, Vals}}} | Rest], Acc) ->
    Expanded = lists:map(fun(Val) -> {Key, {PartId, Val}} end, Vals),
    expand_dups(Rest, Expanded ++ Acc);
expand_dups([{_Key, {_PartId, _Val}} = Kv | Rest], Acc) ->
    expand_dups(Rest, [Kv | Acc]).


expand_dups([], _Abitmask, Acc) ->
    lists:reverse(Acc);
expand_dups([{Key, {PartId, {dups, Vals}}} | Rest], Abitmask, Acc) ->
    case (1 bsl PartId) band Abitmask of
    0 ->
        expand_dups(Rest, Abitmask, Acc);
    _ ->
        Expanded = lists:map(fun(V) -> {Key, {PartId, V}} end, Vals),
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


get_ddoc_ids_with_sig(SetName, ViewGroupSig) ->
    {ok, Db} = couch_db:open_int(?master_dbname(SetName), []),
    {ok, DDocList} = couch_db:get_design_docs(Db, no_deletes),
    ok = couch_db:close(Db),
    lists:foldl(
        fun(#doc{id = Id} = DDoc, Acc) ->
            case design_doc_to_set_view_group(SetName, DDoc) of
            #set_view_group{sig = ViewGroupSig} ->
                [Id | Acc];
            #set_view_group{sig = _OtherSig} ->
                Acc
            end
        end,
        [], DDocList).


design_doc_to_set_view_group(SetName, #doc{id = Id, body = {Fields}}) ->
    Language = couch_util:get_value(<<"language">>, Fields, <<"javascript">>),
    {DesignOptions} = couch_util:get_value(<<"options">>, Fields, {[]}),
    {RawViews} = couch_util:get_value(<<"views">>, Fields, {[]}),
    Lib = couch_util:get_value(<<"lib">>, RawViews, {[]}),
    % add the views to a dictionary object, with the map source as the key
    DictBySrc =
    lists:foldl(
        fun({Name, {MRFuns}}, DictBySrcAcc) ->
            case couch_util:get_value(<<"map">>, MRFuns) of
            undefined -> DictBySrcAcc;
            MapSrc ->
                RedSrc = couch_util:get_value(<<"reduce">>, MRFuns, null),
                {ViewOptions} = couch_util:get_value(<<"options">>, MRFuns, {[]}),
                View =
                case dict:find({MapSrc, ViewOptions}, DictBySrcAcc) of
                    {ok, View0} -> View0;
                    error -> #set_view{def = MapSrc, options = ViewOptions}
                end,
                View2 =
                if RedSrc == null ->
                    View#set_view{map_names = [Name | View#set_view.map_names]};
                true ->
                    View#set_view{reduce_funs = [{Name, RedSrc} | View#set_view.reduce_funs]}
                end,
                dict:store({MapSrc, ViewOptions}, View2, DictBySrcAcc)
            end
        end, dict:new(), RawViews),
    % number the views
    {Views, _N} = lists:mapfoldl(
        fun({_Src, View}, N) ->
            {View#set_view{id_num = N}, N + 1}
        end,
        0, lists:sort(dict:to_list(DictBySrc))),
    SetViewGroup = #set_view_group{
        set_name = SetName,
        name = Id,
        lib = Lib,
        views = Views,
        def_lang = Language,
        design_options = DesignOptions
    },
    set_view_sig(SetViewGroup).


set_view_sig(#set_view_group{
            views = Views,
            lib = Lib,
            def_lang = Language,
            design_options = DesignOptions} = G) ->
    Sig = couch_util:md5(term_to_binary({Views, Language, DesignOptions, sort_lib(Lib)})),
    G#set_view_group{sig = Sig}.


sort_lib({Lib}) ->
    sort_lib(Lib, []).
sort_lib([], LAcc) ->
    lists:keysort(1, LAcc);
sort_lib([{LName, {LObj}}|Rest], LAcc) ->
    LSorted = sort_lib(LObj, []), % descend into nested object
    sort_lib(Rest, [{LName, LSorted}|LAcc]);
sort_lib([{LName, LCode}|Rest], LAcc) ->
    sort_lib(Rest, [{LName, LCode}|LAcc]).


open_raw_read_fd(CouchFilePid, FileName) ->
    case file:open(FileName, [read, raw, binary]) of
    {ok, RawReadFd} ->
        erlang:put({CouchFilePid, fast_fd_read}, RawReadFd),
        ok;
    _ ->
        ok
    end.


close_raw_read_fd(CouchFilePid) ->
    case erlang:erase({CouchFilePid, fast_fd_read}) of
    undefined ->
        ok;
    Fd ->
        ok = file:close(Fd)
    end.
