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

-export([copy/3, from_sorted_file/4, file_sort_output_fun/3]).

-include("couch_db.hrl").

-define(FLUSH_PAGE_CACHE_AFTER, 102400).

-record(acc, {
    btree,
    fd,
    before_kv_write = nil,
    user_acc = [],
    filter = fun(_) -> true end,
    kv_chunk_threshold,
    kp_chunk_threshold,
    nodes = array:new(),
    cur_level = 1,
    max_level = 1,
    % only used while at bottom level (1)
    values = [],
    leaf_size = 0
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
        kv_chunk_threshold = Btree#btree.kv_chunk_threshold,
        kp_chunk_threshold = Btree#btree.kp_chunk_threshold
    },
    Acc = apply_options(Options, Acc0),
    {ok, _, #acc{cur_level = 1} = FinalAcc0} = couch_btree:fold(
        Btree, fun(Kv, _Reds, A) -> fold_copy(Kv, A) end, Acc, []),
    {ok, CopyRootState, FinalAcc} = finish_copy(FinalAcc0),
    {ok, CopyRootState, FinalAcc#acc.user_acc}.



from_sorted_file(EmptyBtree, SortedFileName, DestFd, BinToKvFun) ->
    Acc = #acc{
        btree = EmptyBtree,
        fd = DestFd,
        kv_chunk_threshold = EmptyBtree#btree.kv_chunk_threshold,
        kp_chunk_threshold = EmptyBtree#btree.kp_chunk_threshold
    },
    {ok, SourceFd} = file:open(SortedFileName, [read, raw, binary, read_ahead]),
    {ok, Acc2} = try
        sorted_file_fold(SourceFd, SortedFileName, BinToKvFun, 0, 0, Acc)
    after
        ok = file:close(SourceFd)
    end,
    {ok, CopyRootState, _FinalAcc} = finish_copy(Acc2),
    {ok, CopyRootState}.


sorted_file_fold(Fd, FileName, BinToKvFun, AdviseOffset, BytesRead, Acc) ->
    case file:read(Fd, 4) of
    {ok, <<Len:32>>} ->
        case file:read(Fd, Len) of
        {ok, KvBin} ->
            BytesRead2 = BytesRead + 4 + Len,
            case (BytesRead2 - AdviseOffset) >= ?FLUSH_PAGE_CACHE_AFTER of
            true ->
                AdviseOffset2 = BytesRead2,
                (catch file:advise(Fd, AdviseOffset, BytesRead2, dont_need));
            false ->
                AdviseOffset2 = AdviseOffset
            end,
            Kv = BinToKvFun(KvBin),
            {ok, Acc2} = fold_copy(Kv, Len, Acc),
            sorted_file_fold(Fd, FileName, BinToKvFun, AdviseOffset2, BytesRead2, Acc2);
        eof ->
            throw({unexpected_eof, FileName});
        {error, Error} ->
            throw({file_read_error, FileName, Error})
        end;
    eof ->
        (catch file:advise(Fd, AdviseOffset, BytesRead, dont_need)),
        {ok, Acc};
    {error, Error} ->
        throw({file_read_error, FileName, Error})
    end.


% This will create a function suitable for receiving the output of
% Erlang file_sorter:sort/2.
file_sort_output_fun(OrigBtree, Fd, Options) ->
    Acc0 = #acc{
        btree = OrigBtree,
        fd = Fd,
        kv_chunk_threshold = OrigBtree#btree.kv_chunk_threshold,
        kp_chunk_threshold = OrigBtree#btree.kp_chunk_threshold
    },
    Acc = apply_options(Options, Acc0),
    fun(Item) -> file_sort_loop(Item, Acc) end.


file_sort_loop(close, Acc) ->
    {ok, CopyRootState, _FinalAcc} = finish_copy(Acc),
    {ok, CopyRootState};
file_sort_loop(Binaries, Acc) ->
    Acc4 = lists:foldl(fun(Bin, Acc2) ->
        Item = binary_to_term(Bin),
        {ok, Acc3} = fold_copy(Item, byte_size(Bin), Acc2),
        Acc3
        end, Acc, Binaries),
    fun(Item2) -> file_sort_loop(Item2, Acc4) end.


apply_options([], Acc) ->
    Acc;
apply_options([{before_kv_write, {Fun, UserAcc}} | Rest], Acc) ->
    apply_options(Rest, Acc#acc{before_kv_write = Fun, user_acc = UserAcc});
apply_options([{filter, Fun} | Rest], Acc) ->
    apply_options(Rest, Acc#acc{filter = Fun});
apply_options([override | Rest], Acc) ->
    apply_options(Rest, Acc);
apply_options([{kv_chunk_threshold, Threshold} | Rest], Acc) ->
    apply_options(Rest, Acc#acc{kv_chunk_threshold = Threshold});
apply_options([{kp_chunk_threshold, Threshold} | Rest], Acc) ->
    apply_options(Rest, Acc#acc{kp_chunk_threshold = Threshold}).


extract(#acc{btree = #btree{extract_kv = Extract}}, Value) ->
    Extract(Value).


assemble(#acc{btree = #btree{assemble_kv = Assemble}}, Key, Value) ->
    Assemble(Key, Value).


before_leaf_write(#acc{before_kv_write = nil} = Acc, KVs) ->
    {KVs, Acc};
before_leaf_write(#acc{before_kv_write = Fun, user_acc = UserAcc0} = Acc, KVs) ->
    {NewKVs, NewUserAcc} = lists:mapfoldl(
        fun({K, V}, UAcc) ->
            Item = assemble(Acc, K, V),
            {NewItem, UAcc2} = Fun(Item, UAcc),
            {K, _NewValue} = NewKV = extract(Acc, NewItem),
            {NewKV, UAcc2}
        end,
        UserAcc0, KVs),
    {NewKVs, Acc#acc{user_acc = NewUserAcc}}.


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
    nil ->
        case Bt#btree.binary_mode of
        false -> [];
        true -> <<>>
        end;
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


fold_copy(Item, #acc{filter = Filter} = Acc) ->
    case Filter(Item) of
    true ->
        fold_copy(Item, ?term_size(Item), Acc);
    false ->
        {ok, Acc}
    end.

fold_copy(Item, ItemSize, #acc{cur_level = 1} = Acc) ->
    #acc{
        values = Values,
        leaf_size = LeafSize
    } = Acc,
    Kv = extract(Acc, Item),
    LeafSize2 = LeafSize + ItemSize,
    Values2 = [Kv | Values],
    NextAcc = case LeafSize2 >= Acc#acc.kv_chunk_threshold of
    true ->
        {LeafState, Acc2} = flush_leaf(Values2, Acc),
        {K, _V} = Kv,
        bubble_up({K, LeafState}, Acc2);
    false ->
        Acc#acc{values = Values2, leaf_size = LeafSize2}
    end,
    {ok, NextAcc}.


bubble_up({Key, NodeState}, #acc{cur_level = Level} = Acc) ->
    bubble_up({Key, NodeState}, Level, Acc).

bubble_up({Key, NodeState}, Level, Acc) ->
    #acc{max_level = MaxLevel, nodes = Nodes} = Acc,
    Acc2 = case Level of
    1 ->
        Acc#acc{values = [], leaf_size = 0};
    _ ->
        Acc#acc{nodes = array:set(Level, {0, []}, Nodes)}
    end,
    Kp = {Key, NodeState},
    KpSize = ?term_size(Kp),
    case Level of
    MaxLevel ->
        Acc2#acc{
            nodes = array:set(Level + 1, {KpSize, [Kp]}, Acc2#acc.nodes),
            max_level = Level + 1
        };
    _ when Level < MaxLevel ->
        {Size, NextLevelNodes} = array:get(Level + 1, Acc2#acc.nodes),
        NextLevelNodes2 = [Kp | NextLevelNodes],
        Size2 = Size + KpSize,
        case Size2 >= Acc#acc.kp_chunk_threshold of
        true ->
            {ok, NewNodeState} = write_kp_node(
                Acc2, lists:reverse(NextLevelNodes2)),
            bubble_up({Key, NewNodeState}, Level + 1, Acc2);
        false ->
            Acc2#acc{
                nodes = array:set(Level + 1, {Size2, NextLevelNodes2}, Acc2#acc.nodes)
            }
        end
    end.


finish_copy(#acc{nodes = Nodes, values = LeafValues, leaf_size = LeafSize} = Acc) ->
    Acc2 = Acc#acc{
        nodes = array:set(1, {LeafSize, LeafValues}, Nodes)
    },
    finish_copy_loop(Acc2).

finish_copy_loop(#acc{cur_level = 1, max_level = 1, values = LeafValues} = Acc) ->
    case LeafValues of
    [] ->
        {ok, nil, Acc};
    [{_Key, _Value} | _] = KvList ->
        {RootState, Acc2} = flush_leaf(KvList, Acc),
        {ok, RootState, Acc2}
    end;

finish_copy_loop(#acc{cur_level = Level, max_level = Level, nodes = Nodes} = Acc) ->
    case array:get(Level, Nodes) of
    {_Size, [{_Key, {Pos, Red, Size}}]} ->
        {ok, {Pos, Red, Size}, Acc};
    {_Size, NodeList} ->
        {ok, RootState} = write_kp_node(Acc, lists:reverse(NodeList)),
        {ok, RootState, Acc}
    end;

finish_copy_loop(#acc{cur_level = Level, nodes = Nodes} = Acc) ->
    case array:get(Level, Nodes) of
    {0, []} ->
        Acc2 = Acc#acc{cur_level = Level + 1},
        finish_copy_loop(Acc2);
    {_Size, [{LastKey, _} | _] = NodeList} ->
        {UpperNodeState, Acc2} = case Level of
        1 ->
            flush_leaf(NodeList, Acc);
        _ when Level > 1 ->
            {ok, KpNodeState} = write_kp_node(Acc, lists:reverse(NodeList)),
            {KpNodeState, Acc}
        end,
        Kp = {LastKey, UpperNodeState},
        {ParentSize, ParentNode} = array:get(Level + 1, Nodes),
        ParentSize2 = ParentSize + ?term_size(Kp),
        Acc3 = Acc2#acc{
            nodes = array:set(Level + 1, {ParentSize2, [Kp | ParentNode]}, Nodes),
            cur_level = Level + 1
        },
        finish_copy_loop(Acc3)
    end.


flush_leaf(KVs, #acc{btree = Btree} = Acc) ->
    {NewKVs, Acc2} = before_leaf_write(Acc, lists:reverse(KVs)),
    Red = case Btree#btree.reduce of
    nil ->
        case Btree#btree.binary_mode of
        false -> [];
        true -> <<>>
        end;
    _ ->
        Items = lists:map(
            fun({K, V}) -> assemble(Acc2, K, V) end,
            NewKVs),
        couch_btree:final_reduce(Btree, {Items, []})
    end,
    {ok, LeafState} = write_leaf(Acc2, {kv_node, NewKVs}, Red),
    {LeafState, Acc2}.
