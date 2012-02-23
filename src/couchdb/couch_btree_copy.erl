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

-module(couch_btree_copy).

-export([copy/3, file_sort_output_fun/3]).

-include("couch_db.hrl").

-record(acc, {
    btree,
    fd,
    before_kv_write = {fun(Item, Acc) -> {Item, Acc} end, []},
    filter = fun(_) -> true end,
    chunk_threshold,
    nodes = dict:from_list([{1, []}]),
    cur_level = 1,
    max_level = 1
}).


copy(Btree, Fd, Options) ->
    case lists:member(override, Options) of
    true ->
        ok = couch_file:truncate(Fd, 0);
    false ->
        ok
    end,
    Acc0 = #acc{
        btree = Btree,
        fd = Fd,
        chunk_threshold = Btree#btree.chunk_threshold
    },
    Acc = apply_options(Options, Acc0),
    {ok, _, #acc{cur_level = 1} = FinalAcc0} = couch_btree:fold(
        Btree, fun fold_copy/3, Acc, []),
    {ok, CopyRootState, FinalAcc} = finish_copy(FinalAcc0),
    {_, LastUserAcc} = FinalAcc#acc.before_kv_write,
    {ok, CopyRootState, LastUserAcc}.

% this will create a function suitable for receiving the output of
% Erlang file_sorter:sort/2.
file_sort_output_fun(OrigBtree, Fd, Options) ->
    Acc0 = #acc{
        btree = OrigBtree,
        fd = Fd,
        chunk_threshold = OrigBtree#btree.chunk_threshold
    },
    Acc = apply_options(Options, Acc0),
    fun(Item) -> file_sort_loop(Item, Acc) end.


file_sort_loop(close, Acc) ->
    {ok, CopyRootState, _FinalAcc} = finish_copy(Acc),
    {ok, CopyRootState};
file_sort_loop(Binaries, Acc) ->
    Items = [binary_to_term(Bin) || Bin <- Binaries],
    Acc4 = lists:foldl(fun(Item, Acc2) ->
        {ok, Acc3} = fold_copy(Item, ignored, Acc2),
        Acc3
        end, Acc, Items),
    fun(Item2) -> file_sort_loop(Item2, Acc4) end.


apply_options([], Acc) ->
    Acc;
apply_options([{before_kv_write, {Fun, UserAcc}} | Rest], Acc) ->
    apply_options(Rest, Acc#acc{before_kv_write = {Fun, UserAcc}});
apply_options([{filter, Fun} | Rest], Acc) ->
    apply_options(Rest, Acc#acc{filter = Fun});
apply_options([override | Rest], Acc) ->
    apply_options(Rest, Acc);
apply_options([{chunk_threshold, Threshold} | Rest], Acc) ->
    apply_options(Rest, Acc#acc{chunk_threshold = Threshold}).


extract(#acc{btree = #btree{extract_kv = Extract}}, Value) ->
    Extract(Value).


assemble(#acc{btree = #btree{assemble_kv = Assemble}}, Key, Value) ->
    Assemble(Key, Value).


before_leaf_write(#acc{before_kv_write = {Fun, UserAcc0}} = Acc, KVs) ->
    {NewKVs, NewUserAcc} = lists:mapfoldl(
        fun({K, V}, UAcc) ->
            Item = assemble(Acc, K, V),
            {NewItem, UAcc2} = Fun(Item, UAcc),
            {K, _NewValue} = NewKV = extract(Acc, NewItem),
            {NewKV, UAcc2}
        end,
        UserAcc0, KVs),
    {NewKVs, Acc#acc{before_kv_write = {Fun, NewUserAcc}}}.


write_leaf(#acc{fd = Fd, btree = Bt}, {NodeType, NodeList}, Red) ->
    if Bt#btree.binary_mode ->
        Bin = couch_btree:encode_node(NodeType, NodeList),
        {ok, Pos, Size} = couch_file:append_binary_crc32(Fd, Bin);
    true ->
        {ok, Pos, Size} = couch_file:append_term(Fd, {NodeType, NodeList})
    end,
    {ok, {Pos, Red, Size}}.


write_kp_node(#acc{fd = Fd, btree = Bt}, NodeList) ->
    {ChildrenReds, ChildrenSize} = lists:foldr(
        fun({_Key, {_P, Red, Sz}}, {AccR, AccSz}) ->
            {[Red | AccR], Sz + AccSz}
        end,
        {[], 0}, NodeList),
    Red = case Bt#btree.reduce of
    nil -> [];
    _ ->
        couch_btree:final_reduce(Bt, {[], ChildrenReds})
    end,
    if Bt#btree.binary_mode ->
        Bin = couch_btree:encode_node(kp_node, NodeList),
        {ok, Pos, Size} = couch_file:append_binary_crc32(Fd, Bin);
    true ->
        {ok, Pos, Size} = couch_file:append_term(Fd, {kp_node, NodeList})
    end,
    {ok, {Pos, Red, ChildrenSize + Size}}.


fold_copy(Item, _Reds, #acc{nodes = Nodes, cur_level = 1, filter = Filter} = Acc) ->
    case Filter(Item) of
    false ->
        {ok, Acc};
    true ->
        {K, V} = extract(Acc, Item),
        LevelNode = dict:fetch(1, Nodes),
        LevelNodes2 = [{K, V} | LevelNode],
        NextAcc = case ?term_size(LevelNodes2) >= Acc#acc.chunk_threshold of
        true ->
            {LeafState, Acc2} = flush_leaf(LevelNodes2, Acc),
            bubble_up({K, LeafState}, Acc2);
        false ->
            Acc#acc{nodes = dict:store(1, LevelNodes2, Nodes)}
        end,
        {ok, NextAcc}
    end.


bubble_up({Key, NodeState}, #acc{cur_level = Level} = Acc) ->
    bubble_up({Key, NodeState}, Level, Acc).

bubble_up({Key, NodeState}, Level, Acc) ->
    #acc{max_level = MaxLevel, nodes = Nodes} = Acc,
    Acc2 = Acc#acc{nodes = dict:store(Level, [], Nodes)},
    case Level of
    MaxLevel ->
        Acc2#acc{
            nodes = dict:store(Level + 1, [{Key, NodeState}], Acc2#acc.nodes),
            max_level = Level + 1
        };
    _ when Level < MaxLevel ->
        NextLevelNodes = dict:fetch(Level + 1, Acc2#acc.nodes),
        NextLevelNodes2 = [{Key, NodeState} | NextLevelNodes],
        case ?term_size(NextLevelNodes2) >= Acc#acc.chunk_threshold of
        true ->
            {ok, NewNodeState} = write_kp_node(
                Acc2, lists:reverse(NextLevelNodes2)),
            bubble_up({Key, NewNodeState}, Level + 1, Acc2);
        false ->
            Acc2#acc{
                nodes = dict:store(Level + 1, NextLevelNodes2, Acc2#acc.nodes)
            }
        end
    end.


finish_copy(#acc{cur_level = 1, max_level = 1, nodes = Nodes} = Acc) ->
    case dict:fetch(1, Nodes) of
    [] ->
        {ok, nil, Acc};
    [{_Key, _Value} | _] = KvList ->
        {RootState, Acc2} = flush_leaf(KvList, Acc),
        {ok, RootState, Acc2}
    end;

finish_copy(#acc{cur_level = Level, max_level = Level, nodes = Nodes} = Acc) ->
    case dict:fetch(Level, Nodes) of
    [{_Key, {Pos, Red, Size}}] ->
        {ok, {Pos, Red, Size}, Acc};
    NodeList ->
        {ok, RootState} = write_kp_node(Acc, lists:reverse(NodeList)),
        {ok, RootState, Acc}
    end;

finish_copy(#acc{cur_level = Level, nodes = Nodes} = Acc) ->
    case dict:fetch(Level, Nodes) of
    [] ->
        Acc2 = Acc#acc{cur_level = Level + 1},
        finish_copy(Acc2);
    [{LastKey, _} | _] = NodeList ->
        {UpperNodeState, Acc2} = case Level of
        1 ->
            flush_leaf(NodeList, Acc);
        _ when Level > 1 ->
            {ok, KpNodeState} = write_kp_node(Acc, lists:reverse(NodeList)),
            {KpNodeState, Acc}
        end,
        ParentNode = dict:fetch(Level + 1, Nodes),
        Acc3 = Acc2#acc{
            nodes = dict:store(Level + 1, [{LastKey, UpperNodeState} | ParentNode], Nodes),
            cur_level = Level + 1
        },
        finish_copy(Acc3)
    end.


flush_leaf(KVs, #acc{btree = Btree} = Acc) ->
    {NewKVs, Acc2} = before_leaf_write(Acc, lists:reverse(KVs)),
    Red = case Btree#btree.reduce of
    nil -> [];
    _ ->
        Items = lists:map(
            fun({K, V}) -> assemble(Acc2, K, V) end,
            NewKVs),
        couch_btree:final_reduce(Btree, {Items, []})
    end,
    {ok, LeafState} = write_leaf(Acc2, {kv_node, NewKVs}, Red),
    {LeafState, Acc2}.
