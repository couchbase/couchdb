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

-module(couch_btree).

-export([open/2, open/3, query_modify/4, add/2, add_remove/3]).
-export([query_modify_raw/2, query_modify_raw/4]).
-export([fold/4, full_reduce/1, final_reduce/2, size/1, foldl/3, foldl/4, lookup_sorted/2]).
-export([modify/3, fold_reduce/4, lookup/2, get_state/1, set_state/2, set_options/2]).
-export([add_remove/5, query_modify/6]).
-export([guided_purge/3]).
-export([less/3]).

-export([encode_node/2, decode_node/3]).

-include("couch_db.hrl").

-define(KEY_BITS,       12).
-define(VALUE_BITS,     28).
-define(POINTER_BITS,   48).
-define(TREE_SIZE_BITS, 48).
-define(RED_BITS,       16).

-define(MAX_KEY_SIZE,     ((1 bsl ?KEY_BITS) - 1)).
-define(MAX_VALUE_SIZE,   ((1 bsl ?VALUE_BITS) - 1)).
-define(MAX_RED_SIZE,     ((1 bsl ?RED_BITS) - 1)).

-define(KV_NODE_TYPE, 1).
-define(KP_NODE_TYPE, 0).


extract(#btree{extract_kv = identity}, Value) ->
    Value;
extract(#btree{extract_kv = Extract}, Value) ->
    Extract(Value).

assemble(#btree{assemble_kv = identity}, KeyValue) ->
    KeyValue;
assemble(#btree{assemble_kv = Assemble}, KeyValue) ->
    Assemble(KeyValue).

less(#btree{less=Less}, A, B) ->
    Less(A, B).

% pass in 'nil' for State if a new Btree.
open(State, Fd) ->
    {ok, #btree{root=State, fd=Fd}}.

set_options(Bt, []) ->
    Bt;
set_options(Bt, [{split, Extract}|Rest]) ->
    set_options(Bt#btree{extract_kv=Extract}, Rest);
set_options(Bt, [{join, Assemble}|Rest]) ->
    set_options(Bt#btree{assemble_kv=Assemble}, Rest);
set_options(Bt, [{less, Less}|Rest]) ->
    set_options(Bt#btree{less=Less}, Rest);
set_options(Bt, [{reduce, Reduce}|Rest]) ->
    set_options(Bt#btree{reduce=Reduce}, Rest);
set_options(Bt, [{kv_chunk_threshold, Threshold}|Rest]) ->
    set_options(Bt#btree{kv_chunk_threshold = Threshold}, Rest);
set_options(Bt, [{kp_chunk_threshold, Threshold}|Rest]) ->
    set_options(Bt#btree{kp_chunk_threshold = Threshold}, Rest);
set_options(#btree{root = Root} = Bt, [{binary_mode, true}|Rest]) when is_binary(Root) ->
    <<Pointer:?POINTER_BITS, Size:?TREE_SIZE_BITS, Red0/binary>> = Root,
    Red = binary:copy(Red0),
    set_options(Bt#btree{root = {Pointer, Red, Size}, binary_mode = true}, Rest);
set_options(Bt, [{binary_mode, Bool}|Rest]) ->
    set_options(Bt#btree{binary_mode = Bool}, Rest).


open(State, Fd, Options) ->
    {ok, set_options(#btree{root=State, fd=Fd}, Options)}.

get_state(#btree{root={Pointer, Reduction, Size}, binary_mode=true}) ->
    <<Pointer:?POINTER_BITS, Size:?TREE_SIZE_BITS, Reduction/binary>>;
get_state(#btree{root=Root}) ->
    Root.

set_state(Btree, State) when is_binary(State) ->
    <<Pointer:?POINTER_BITS, Size:?TREE_SIZE_BITS, Red0/binary>> = State,
    Red = binary:copy(Red0),
    Btree#btree{root = {Pointer, Red, Size}};
set_state(Btree, {_Pointer, _Red, _Size} = State) ->
    Btree#btree{root = State};
set_state(Btree, nil = State) ->
    Btree#btree{root = State}.

final_reduce(#btree{reduce=Reduce}, Val) ->
    final_reduce(Reduce, Val);
final_reduce(Reduce, {[], []}) ->
    Reduce(reduce, []);
final_reduce(_Bt, {[], [Red]}) ->
    Red;
final_reduce(Reduce, {[], Reductions}) ->
    Reduce(rereduce, Reductions);
final_reduce(Reduce, {KVs, Reductions}) ->
    Red = Reduce(reduce, KVs),
    final_reduce(Reduce, {[], [Red | Reductions]}).

fold_reduce(#btree{root=Root}=Bt, Fun, Acc, Options) ->
    Dir = couch_util:get_value(dir, Options, fwd),
    StartKey = couch_util:get_value(start_key, Options),
    InEndRangeFun = make_key_in_end_range_function(Bt, Dir, Options),
    KeyGroupFun = couch_util:get_value(key_group_fun, Options, fun(_,_) -> true end),
    FilterFun = couch_util:get_value(
        filter_fun, Options, fun(value, _) -> true; (branch, _) -> all end),
    try
        {ok, Acc2, GroupedRedsAcc2, GroupedKVsAcc2, GroupedKey2} =
            reduce_stream_node(Bt, Dir, Root, StartKey, InEndRangeFun, undefined, [], [],
            KeyGroupFun, Fun, FilterFun, Acc),
        if GroupedKey2 == undefined ->
            {ok, Acc2};
        true ->
            case Fun(GroupedKey2, {GroupedKVsAcc2, GroupedRedsAcc2}, Acc2) of
            {ok, Acc3} -> {ok, Acc3};
            {stop, Acc3} -> {ok, Acc3}
            end
        end
    catch
        throw:{stop, AccDone} -> {ok, AccDone}
    end.

full_reduce(#btree{root=nil,reduce=Reduce}) ->
    {ok, Reduce(reduce, [])};
full_reduce(#btree{root=Root}) ->
    {ok, element(2, Root)}.

size(#btree{root = nil}) ->
    0;
size(#btree{root = {_P, _Red, Size}}) ->
    Size.

% wraps a 2 arity function with the proper 3 arity function
convert_fun_arity(Fun) when is_function(Fun, 2) ->
    fun(value, KV, _Reds, AccIn) ->
            Fun(KV, AccIn);
        (branch, _K, _Red, AccIn) ->
            {ok, AccIn}
    end;
convert_fun_arity(Fun) when is_function(Fun, 3) ->
    fun(value, Kv, Reds, AccIn) ->
            Fun(Kv, Reds, AccIn);
        (branch, _K, _Red, AccIn) ->
            {ok, AccIn}
    end;
convert_fun_arity(Fun) when is_function(Fun, 4) ->
    Fun.    % Already arity 4

make_key_in_end_range_function(#btree{less=Less}, fwd, Options) ->
    case couch_util:get_value(end_key_gt, Options) of
    undefined ->
        case couch_util:get_value(end_key, Options) of
        undefined ->
            fun(_Key) -> true end;
        LastKey ->
            fun(Key) -> not Less(LastKey, Key) end
        end;
    EndKey ->
        fun(Key) -> Less(Key, EndKey) end
    end;
make_key_in_end_range_function(#btree{less=Less}, rev, Options) ->
    case couch_util:get_value(end_key_gt, Options) of
    undefined ->
        case couch_util:get_value(end_key, Options) of
        undefined ->
            fun(_Key) -> true end;
        LastKey ->
            fun(Key) -> not Less(Key, LastKey) end
        end;
    EndKey ->
        fun(Key) -> Less(EndKey, Key) end
    end.


foldl(Bt, Fun, Acc) ->
    fold(Bt, Fun, Acc, []).

foldl(Bt, Fun, Acc, Options) ->
    fold(Bt, Fun, Acc, Options).


fold(#btree{root=nil}, _Fun, Acc, _Options) ->
    {ok, {[], []}, Acc};
fold(#btree{root=Root}=Bt, Fun, Acc, Options) ->
    Dir = couch_util:get_value(dir, Options, fwd),
    InRange = make_key_in_end_range_function(Bt, Dir, Options),
    Result =
    case couch_util:get_value(start_key, Options) of
    undefined ->
        stream_node(Bt, [], Bt#btree.root, InRange, Dir,
                convert_fun_arity(Fun), Acc);
    StartKey ->
        stream_node(Bt, [], Bt#btree.root, StartKey, InRange, Dir,
                convert_fun_arity(Fun), Acc)
    end,
    case Result of
    {ok, Acc2}->
        FullReduction = element(2, Root),
        {ok, {[], [FullReduction]}, Acc2};
    {stop, LastReduction, Acc2} ->
        {ok, LastReduction, Acc2}
    end.

add(Bt, InsertKeyValues) ->
    add_remove(Bt, InsertKeyValues, []).

add_remove(Bt, InsertKeyValues, RemoveKeys) ->
    {ok, _, Bt2} = add_remove(Bt, InsertKeyValues, RemoveKeys, nil, nil),
    {ok, Bt2}.

add_remove(Bt, InsertKeyValues, RemoveKeys, PurgeFun, PurgeFunAcc) ->
    {ok, [], PurgeFunAcc2, Bt2} = query_modify(
        Bt, [], InsertKeyValues, RemoveKeys, PurgeFun, PurgeFunAcc),
    {ok, PurgeFunAcc2, Bt2}.


query_modify(Bt, LookupKeys, InsertValues, RemoveKeys) ->
    {ok, QueryResults, _, Bt2} =
        query_modify(Bt, LookupKeys, InsertValues, RemoveKeys, nil, nil),
    {ok, QueryResults, Bt2}.

query_modify(Bt, LookupKeys, InsertValues, RemoveKeys, PurgeFun, PurgeFunAcc) ->
    #btree{root=Root} = Bt,
    InsertActions = lists:map(
        fun(KeyValue) ->
            {Key, Value} = extract(Bt, KeyValue),
            {insert, Key, Value}
        end, InsertValues),
    RemoveActions = [{remove, Key, nil} || Key <- RemoveKeys],
    FetchActions = [{fetch, Key, nil} || Key <- LookupKeys],
    SortFun =
        fun({OpA, A, _}, {OpB, B, _}) ->
            case A == B of
            % A and B are equal, sort by op.
            true -> op_order(OpA) < op_order(OpB);
            false ->
                less(Bt, A, B)
            end
        end,
    Actions = lists:sort(SortFun, lists:append([InsertActions, RemoveActions, FetchActions])) ,
    {ok, KeyPointers, QueryResults, nil, PurgeFunAcc2, _KeepPurging2, Bt2} =
        modify_node(Bt, Root, Actions, [], nil, PurgeFun, PurgeFunAcc, is_function(PurgeFun, 3)),
    {ok, NewRoot, Bt3} = complete_root(Bt2, KeyPointers),
    {ok, QueryResults, PurgeFunAcc2, Bt3#btree{root=NewRoot}}.


% Similar to query_modify, except the keys values must be sorted and tagged
% tuples of {action, Key, Value} and sorted by the sorted by Key, then by
% the rules in the function op_order.
query_modify_raw(#btree{root=Root} = Bt, SortedActions) ->
    {ok, KeyPointers, QueryResults, nil, nil, false, Bt2} =
        modify_node(Bt, Root, SortedActions, [], nil, nil, nil, false),
    {ok, NewRoot, Bt3} = complete_root(Bt2, KeyPointers),
    {ok, QueryResults, Bt3#btree{root=NewRoot}}.

query_modify_raw(#btree{root=Root} = Bt, SortedActions, PurgeFun, PurgeAcc) ->
    KeepPurging = is_function(PurgeFun, 3),
    {ok, KeyPointers, QueryResults, nil, PurgeAcc2, _KeepPurging2, Bt2} =
        modify_node(Bt, Root, SortedActions, [], nil, PurgeFun, PurgeAcc, KeepPurging),
    {ok, NewRoot, Bt3} = complete_root(Bt2, KeyPointers),
    {ok, QueryResults, PurgeAcc2, Bt3#btree{root=NewRoot}}.


modify(Bt, KeyFuns, Acc) ->
    #btree{root=Root} = Bt,
    Actions = [{modify, Key, Fun} || {Key, Fun} <- KeyFuns],
    {ok, KeyPointers, BeforeAfterResults, Acc2, nil, false, Bt2} =
            modify_node(Bt, Root, Actions, [], Acc, nil, nil, false),
    {ok, NewRoot, Bt3} = complete_root(Bt2, KeyPointers),
    {ok, BeforeAfterResults, Acc2, Bt3#btree{root=NewRoot}}.


% for ordering different operations with the same key.
% fetch < remove < insert
op_order(fetch) -> 1;
op_order(remove) -> 2;
op_order(insert) -> 3.


lookup_sorted(#btree{root=Root}=Bt, Keys) ->
    {ok, KeyResults} = lookup(Bt, Root, Keys),
    KeyResults.

lookup(#btree{root=Root, less=Less}=Bt, Keys) ->
    SortedKeys = lists:sort(Less, Keys),
    {ok, SortedResults} = lookup(Bt, Root, SortedKeys),
    % We want to return the results in the same order as the keys were input
    % but we may have changed the order when we sorted. So we need to put the
    % order back into the results.
    couch_util:reorder_results(Keys, SortedResults).

lookup(_Bt, nil, Keys) ->
    {ok, [{Key, not_found} || Key <- Keys]};
lookup(Bt, Node, Keys) ->
    Pointer = element(1, Node),
    {NodeType, NodeList} = get_node(Bt, Pointer),
    case NodeType of
    kp_node ->
        lookup_kpnode(Bt, list_to_tuple(NodeList), 1, Keys, []);
    kv_node ->
        lookup_kvnode(Bt, list_to_tuple(NodeList), 1, Keys, [])
    end.

lookup_kpnode(_Bt, _NodeTuple, _LowerBound, [], Output) ->
    {ok, lists:reverse(Output)};
lookup_kpnode(_Bt, NodeTuple, LowerBound, Keys, Output) when tuple_size(NodeTuple) < LowerBound ->
    {ok, lists:reverse(Output, [{Key, not_found} || Key <- Keys])};
lookup_kpnode(Bt, NodeTuple, LowerBound, [FirstLookupKey | _] = LookupKeys, Output) ->
    N = find_first_gteq(Bt, NodeTuple, LowerBound, tuple_size(NodeTuple), FirstLookupKey),
    {Key, PointerInfo} = element(N, NodeTuple),
    SplitFun = fun(LookupKey) -> not less(Bt, Key, LookupKey) end,
    case lists:splitwith(SplitFun, LookupKeys) of
    {[], GreaterQueries} ->
        lookup_kpnode(Bt, NodeTuple, N + 1, GreaterQueries, Output);
    {LessEqQueries, GreaterQueries} ->
        {ok, Results} = lookup(Bt, PointerInfo, LessEqQueries),
        lookup_kpnode(Bt, NodeTuple, N + 1, GreaterQueries, lists:reverse(Results, Output))
    end.


lookup_kvnode(_Bt, _NodeTuple, _LowerBound, [], Output) ->
    {ok, lists:reverse(Output)};
lookup_kvnode(_Bt, NodeTuple, LowerBound, Keys, Output) when tuple_size(NodeTuple) < LowerBound ->
    % keys not found
    {ok, lists:reverse(Output, [{Key, not_found} || Key <- Keys])};
lookup_kvnode(Bt, NodeTuple, LowerBound, [LookupKey | RestLookupKeys], Output) ->
    N = find_first_gteq(Bt, NodeTuple, LowerBound, tuple_size(NodeTuple), LookupKey),
    KV = {Key, _Value} = element(N, NodeTuple),
    case less(Bt, LookupKey, Key) of
    true ->
        % LookupKey is less than Key
        lookup_kvnode(Bt, NodeTuple, N, RestLookupKeys, [{LookupKey, not_found} | Output]);
    false ->
        case less(Bt, Key, LookupKey) of
        true ->
            % LookupKey is greater than Key
            lookup_kvnode(Bt, NodeTuple, N+1, RestLookupKeys, [{LookupKey, not_found} | Output]);
        false ->
            % LookupKey is equal to Key
            lookup_kvnode(Bt, NodeTuple, N, RestLookupKeys, [{LookupKey, {ok, assemble(Bt, KV)}} | Output])
        end
    end.


complete_root(Bt, []) ->
    {ok, nil, Bt};
complete_root(Bt, [{_Key, PointerInfo}])->
    {ok, PointerInfo, Bt};
complete_root(Bt, KPs) ->
    {ok, ResultKeyPointers, Bt2} = write_node(Bt, kp_node, KPs),
    complete_root(Bt2, ResultKeyPointers).

chunkify(#btree{kp_chunk_threshold = T}, kp_node, InList) ->
    chunkify(T, InList);
chunkify(#btree{kv_chunk_threshold = T}, kv_node, InList) ->
    chunkify(T, InList).

chunkify(ChunkThreshold0, InList) ->
    case ?term_size(InList) of
    Size when Size > ChunkThreshold0 ->
        ChunkThreshold1 = ChunkThreshold0 div 2,
        NumberOfChunksLikely = ((Size div ChunkThreshold1) + 1),
        ChunkThreshold = Size div NumberOfChunksLikely,
        chunkify(InList, ChunkThreshold, [], 0, []);
    _Else ->
        [InList]
    end.

chunkify([], _ChunkThreshold, [], 0, OutputChunks) ->
    lists:reverse(OutputChunks);
chunkify([], _ChunkThreshold, OutList, _OutListSize, OutputChunks) ->
    lists:reverse([lists:reverse(OutList) | OutputChunks]);
chunkify([InElement | RestInList], ChunkThreshold, OutList, OutListSize, OutputChunks) ->
    case ?term_size(InElement) of
    Size when (Size + OutListSize) > ChunkThreshold andalso OutList /= [] ->
        chunkify(RestInList, ChunkThreshold, [], 0, [lists:reverse([InElement | OutList]) | OutputChunks]);
    Size ->
        chunkify(RestInList, ChunkThreshold, [InElement | OutList], OutListSize + Size, OutputChunks)
    end.

modify_node(Bt, RootPointerInfo, Actions, QueryOutput, Acc, PurgeFun, PurgeFunAcc, KeepPurging) ->
    {NodeType, NodeList} = case RootPointerInfo of
    nil ->
        {kv_node, []};
    _Tuple ->
        Pointer = element(1, RootPointerInfo),
        get_node(Bt, Pointer)
    end,

    case NodeType of
    kp_node ->
        NodeTuple = list_to_tuple(NodeList),
        {ok, NewNodeList, QueryOutput2, Acc2, PurgeFunAcc2, KeepPurging2, Bt2} =
            modify_kpnode(Bt, NodeTuple, 1, Actions, [], QueryOutput, Acc, PurgeFun, PurgeFunAcc, KeepPurging);
    kv_node ->
        {ok, NewNodeList1, PurgeFunAcc2, KeepPurging2, Bt1} =
            maybe_purge(kv_node, Bt, NodeList, PurgeFun, PurgeFunAcc, KeepPurging),
        NodeTuple = list_to_tuple(NewNodeList1),
        {ok, NewNodeList, QueryOutput2, Acc2, Bt2} =
            modify_kvnode(Bt1, NodeTuple, 1, Actions, [], QueryOutput, Acc)
    end,
    case NewNodeList of
    [] ->  % no nodes remain
        {ok, [], QueryOutput2, Acc2, PurgeFunAcc2, KeepPurging2, Bt2};
    NodeList ->  % nothing changed
        {LastKey, _LastValue} = element(tuple_size(NodeTuple), NodeTuple),
        {ok, [{LastKey, RootPointerInfo}], QueryOutput2, Acc2, PurgeFunAcc2, KeepPurging2, Bt2};
    _Else2 ->
        {ok, ResultList, Bt3} = write_node(Bt2, NodeType, NewNodeList),
        {ok, ResultList, QueryOutput2, Acc2, PurgeFunAcc2, KeepPurging2, Bt3}
    end.


reduce_node(#btree{reduce=nil, binary_mode = BinMode}, _NodeType, _NodeList) ->
    if BinMode -> <<>>; true -> [] end;
reduce_node(#btree{reduce=R}, kp_node, NodeList) ->
    R(rereduce, [element(2, Node) || {_K, Node} <- NodeList]);
reduce_node(#btree{reduce=R, assemble_kv=identity}, kv_node, NodeList) ->
    R(reduce, NodeList);
reduce_node(#btree{reduce=R}=Bt, kv_node, NodeList) ->
    R(reduce, [assemble(Bt, KV) || KV <- NodeList]).

reduce_tree_size(kv_node, NodeSize, _KvList) ->
    NodeSize;
reduce_tree_size(kp_node, NodeSize, []) ->
    NodeSize;
reduce_tree_size(kp_node, NodeSize, [{_K, {_P, _Red, Sz}} | NodeList]) ->
    reduce_tree_size(kp_node, NodeSize + Sz, NodeList).

get_node(#btree{fd = Fd,binary_mode=false}, NodePos) ->
    {ok, Term} = couch_file:pread_term(Fd, NodePos),
    Term;
get_node(#btree{fd = Fd,binary_mode=true}, NodePos) ->
    {ok, CompressedBin} = couch_file:pread_binary(Fd, NodePos),
    <<TypeInt:8, NodeBin/binary>> = couch_compress:decompress(CompressedBin),
    Type = type_int_to_atom(TypeInt),
    decode_node(NodeBin, Type, []).

type_int_to_atom(?KV_NODE_TYPE) ->
    kv_node;
type_int_to_atom(?KP_NODE_TYPE) ->
    kp_node.

decode_node(<<>>, Type, Acc) ->
    {Type, lists:reverse(Acc)};
decode_node(<<SizeK:?KEY_BITS, SizeV:?VALUE_BITS,
              K:SizeK/binary, V:SizeV/binary,
              Rest/binary>>,
            Type, Acc) ->
    case Type of
    kv_node ->
        Val = binary:copy(V);
    kp_node ->
        <<Pointer:?POINTER_BITS,
          SubtreeSize:?TREE_SIZE_BITS,
          RedSize:?RED_BITS,
          Reduction:RedSize/binary>> = V,
        Val = {Pointer, binary:copy(Reduction), SubtreeSize}
    end,
    decode_node(Rest, Type, [{binary:copy(K), Val} | Acc]).

encode_node(kv_node, Kvs) ->
    Bin = [encode_node_kv(K, V) || {K, V} <- Kvs],
    couch_compress:compress([?KV_NODE_TYPE | Bin]);
encode_node(kp_node, Kvs) ->
    KvBins = [
        begin
            RedSize = iolist_size(Reduction),
            case RedSize > ?MAX_RED_SIZE of
            true ->
                throw({error, {reduction_too_long, Reduction}});
            false ->
                ok
            end,
            V = [
                <<Pointer:?POINTER_BITS, SubtreeSize:?TREE_SIZE_BITS, RedSize:?RED_BITS>>,
                Reduction
            ],
            encode_node_kv(K, V)
        end
        || {K, {Pointer, Reduction, SubtreeSize}} <- Kvs
    ],
    couch_compress:compress([?KP_NODE_TYPE | KvBins]).

encode_node_kv(K, V) ->
    SizeK = erlang:iolist_size(K),
    SizeV = erlang:iolist_size(V),
    case SizeK > ?MAX_KEY_SIZE of
    true -> throw({error, {key_too_long, K}});
    false -> ok
    end,
    case SizeV > ?MAX_VALUE_SIZE of
    true -> throw({error, {value_too_long, K, SizeV}});
    false -> ok
    end,
    [<<SizeK:?KEY_BITS, SizeV:?VALUE_BITS>>, K, V].

write_node(#btree{fd = Fd, binary_mode = BinMode} = Bt, NodeType, NodeList) ->
    % split up nodes into smaller sizes
    NodeListList = chunkify(Bt, NodeType, NodeList),
    % now write out each chunk and return the KeyPointer pairs for those nodes
    ResultList = [
        begin
            if BinMode ->
                Bin = encode_node(NodeType, ANodeList),
                {ok, Pointer, Size} = couch_file:append_binary_crc32(Fd, Bin);
            true ->
                {ok, Pointer, Size} = couch_file:append_term(Fd,
                        {NodeType, ANodeList})
            end,
            {LastKey, _} = lists:last(ANodeList),
            SubTreeSize = reduce_tree_size(NodeType, Size, ANodeList),
            {LastKey, {Pointer, reduce_node(Bt, NodeType, ANodeList), SubTreeSize}}
        end
    ||
        ANodeList <- NodeListList
    ],
    {ok, ResultList, Bt}.

modify_kpnode(Bt, {}, _LowerBound, Actions, [], QueryOutput, Acc, PurgeFun, PurgeFunAcc, KeepPurging) ->
    modify_node(Bt, nil, Actions, QueryOutput, Acc, PurgeFun, PurgeFunAcc, KeepPurging);
modify_kpnode(Bt, NodeTuple, LowerBound, [], ResultNode, QueryOutput, Acc, PurgeFun, PurgeFunAcc, KeepPurging) ->
    NodeList = bounded_tuple_to_list(
        NodeTuple, LowerBound, tuple_size(NodeTuple), []),
    {ok, NewNodeList, PurgeFunAcc2, KeepPurging2, Bt2} =
        maybe_purge(kp_node, Bt, NodeList, PurgeFun, PurgeFunAcc, KeepPurging),
    ResultNode2 = lists:reverse(ResultNode, NewNodeList),
    {ok, ResultNode2, QueryOutput, Acc, PurgeFunAcc2, KeepPurging2, Bt2};
modify_kpnode(Bt, NodeTuple, LowerBound,
        [{_, FirstActionKey, _}|_]=Actions, ResultNode, QueryOutput, Acc, PurgeFun, PurgeFunAcc, KeepPurging) ->
    Sz = tuple_size(NodeTuple),
    N = find_first_gteq(Bt, NodeTuple, LowerBound, Sz, FirstActionKey),
    case N =:= Sz of
    true  ->
        % perform remaining actions on last node
        {_, PointerInfo} = element(Sz, NodeTuple),
        {ok, ChildKPs, QueryOutput2, Acc2, PurgeFunAcc2, KeepPurging2, Bt2} =
            modify_node(Bt, PointerInfo, Actions, QueryOutput, Acc, PurgeFun, PurgeFunAcc, KeepPurging),
        PrevNodes = bounded_tuple_to_list(NodeTuple, LowerBound, Sz - 1, []),
        {ok, NewPrevNodes, PurgeFunAcc3, KeepPurging3, Bt3} =
            maybe_purge(kp_node, Bt2, PrevNodes, PurgeFun, PurgeFunAcc2, KeepPurging2),
        NodeList = lists:reverse(ResultNode, NewPrevNodes ++ ChildKPs),
        {ok, NodeList, QueryOutput2, Acc2, PurgeFunAcc3, KeepPurging3, Bt3};
    false ->
        {NodeKey, PointerInfo} = element(N, NodeTuple),
        SplitFun = fun({_ActionType, ActionKey, _ActionValue}) ->
                not less(Bt, NodeKey, ActionKey)
            end,
        {LessEqQueries, GreaterQueries} = lists:splitwith(SplitFun, Actions),
        {ok, ChildKPs, QueryOutput2, Acc2, PurgeFunAcc2, KeepPurging2, Bt2} =
                modify_node(Bt, PointerInfo, LessEqQueries, QueryOutput, Acc, PurgeFun, PurgeFunAcc, KeepPurging),
        PrevNodes = bounded_tuple_to_list(NodeTuple, LowerBound, N - 1, []),
        {ok, NewPrevNodes, PurgeFunAcc3, KeepPurging3, Bt3} =
            maybe_purge(kp_node, Bt2, PrevNodes, PurgeFun, PurgeFunAcc2, KeepPurging2),
        ResultNode2 = lists:reverse(ChildKPs, lists:reverse(NewPrevNodes, ResultNode)),
        modify_kpnode(Bt3, NodeTuple, N + 1, GreaterQueries, ResultNode2, QueryOutput2, Acc2, PurgeFun, PurgeFunAcc3, KeepPurging3)
    end.

maybe_purge(_NodeType, Bt, NodeList, _PurgeFun, PurgeFunAcc, false) ->
    {ok, NodeList, PurgeFunAcc, false, Bt};
maybe_purge(kp_node, Bt, NodeList, PurgeFun, PurgeFunAcc, true) ->
    {ok, NewNodeList, PurgeFunAcc2, Bt2, Go} =
        kp_guided_purge(Bt, NodeList, PurgeFun, PurgeFunAcc),
    {ok, NewNodeList, PurgeFunAcc2, Go =/= stop, Bt2};
maybe_purge(kv_node, Bt, NodeList, PurgeFun, PurgeFunAcc, true) ->
    {ok, NewNodeList, PurgeFunAcc2, Bt2, Go} =
        kv_guided_purge(Bt, NodeList, PurgeFun, PurgeFunAcc),
    {ok, NewNodeList, PurgeFunAcc2, Go =/= stop, Bt2}.

bounded_tuple_to_revlist(_Tuple, Start, End, Tail) when Start > End ->
    Tail;
bounded_tuple_to_revlist(Tuple, Start, End, Tail) ->
    bounded_tuple_to_revlist(Tuple, Start+1, End, [element(Start, Tuple)|Tail]).

bounded_tuple_to_list(Tuple, Start, End, Tail) ->
    bounded_tuple_to_list2(Tuple, Start, End, [], Tail).

bounded_tuple_to_list2(_Tuple, Start, End, Acc, Tail) when Start > End ->
    lists:reverse(Acc, Tail);
bounded_tuple_to_list2(Tuple, Start, End, Acc, Tail) ->
    bounded_tuple_to_list2(Tuple, Start + 1, End, [element(Start, Tuple) | Acc], Tail).

find_first_gteq(_Bt, _Tuple, Start, End, _Key) when Start == End ->
    End;
find_first_gteq(Bt, Tuple, Start, End, Key) ->
    Mid = Start + ((End - Start) div 2),
    {TupleKey, _} = element(Mid, Tuple),
    case less(Bt, TupleKey, Key) of
    true ->
        find_first_gteq(Bt, Tuple, Mid+1, End, Key);
    false ->
        find_first_gteq(Bt, Tuple, Start, Mid, Key)
    end.


modify_value(Bt, ModFun, OldValue, UserAcc, ResultNodeAcc) ->
    case ModFun(OldValue, UserAcc) of
    {nil, UserAcc2} ->
        {ResultNodeAcc, nil, UserAcc2};
    {NewValue, UserAcc2} ->
        {[extract(Bt, NewValue)|ResultNodeAcc], NewValue, UserAcc2}
    end.


modify_kvnode(Bt, NodeTuple, LowerBound, [], ResultNode, QueryOutput, Acc) ->
    {ok, lists:reverse(ResultNode, bounded_tuple_to_list(NodeTuple, LowerBound, tuple_size(NodeTuple), [])), QueryOutput, Acc, Bt};
modify_kvnode(Bt, NodeTuple, LowerBound, [{ActionType, ActionKey, ActionValue} | RestActions], ResultNode, QueryOutput, Acc) when LowerBound > tuple_size(NodeTuple) ->
    case ActionType of
    insert ->
        modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput, Acc);
    remove ->
        % just drop the action
        modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, ResultNode, QueryOutput, Acc);
    modify ->
        ModFun = ActionValue,
        {ResultNode2, ResultValue2, Acc2} =
                modify_value(Bt, ModFun, nil, Acc, ResultNode),
        modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, ResultNode2,
                    [{nil, ResultValue2} | QueryOutput], Acc2);
    fetch ->
        % the key/value must not exist in the tree
        modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, ResultNode, [{not_found, {ActionKey, nil}} | QueryOutput], Acc)
    end;
modify_kvnode(Bt, NodeTuple, LowerBound, [{ActionType, ActionKey, ActionValue} | RestActions], AccNode, QueryOutput, Acc) ->
    N = find_first_gteq(Bt, NodeTuple, LowerBound, tuple_size(NodeTuple), ActionKey),
    KV = {Key, _Value} = element(N, NodeTuple),
    ResultNode =  bounded_tuple_to_revlist(NodeTuple, LowerBound, N - 1, AccNode),
    case less(Bt, ActionKey, Key) of
    true ->
        case ActionType of
        insert ->
            % ActionKey is less than the Key, so insert
            modify_kvnode(Bt, NodeTuple, N, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput, Acc);
        remove ->
            % ActionKey is less than the Key, just drop the action
            modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, QueryOutput, Acc);
        modify ->
            ModFun = ActionValue,
            {ResultNode2, ResultValue2, Acc2} =
                    modify_value(Bt, ModFun, nil, Acc, ResultNode),
            modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode2,
                        [{nil, ResultValue2} | QueryOutput], Acc2);
        fetch ->
            % ActionKey is less than the Key, the key/value must not exist in the tree
            modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, [{not_found, {ActionKey, nil}} | QueryOutput], Acc)
        end;
    false ->
        % ActionKey and Key are maybe equal.
        case less(Bt, Key, ActionKey) of
        false ->
            case ActionType of
            insert ->
                modify_kvnode(Bt, NodeTuple, N+1, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput, Acc);
            remove ->
                modify_kvnode(Bt, NodeTuple, N+1, RestActions, ResultNode, QueryOutput, Acc);
            modify ->
                OldValue = assemble(Bt, KV),
                ModFun = ActionValue,
                {ResultNode2, ResultValue2, Acc2} =
                        modify_value(Bt, ModFun, OldValue, Acc, ResultNode),
                modify_kvnode(Bt, NodeTuple, N+1, RestActions, ResultNode2,
                            [{OldValue, ResultValue2} | QueryOutput], Acc2);
            fetch ->
                % ActionKey is equal to the Key, insert into the QueryOuput, but re-process the node
                % since an identical action key can follow it.
                modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, [{ok, assemble(Bt, KV)} | QueryOutput], Acc)
            end;
        true ->
            modify_kvnode(Bt, NodeTuple, N + 1, [{ActionType, ActionKey, ActionValue} | RestActions], [KV | ResultNode], QueryOutput, Acc)
        end
    end.


reduce_stream_node(_Bt, _Dir, nil, _KeyStart, _InEndRangeFun, GroupedKey, GroupedKVsAcc,
        GroupedRedsAcc, _KeyGroupFun, _Fun, _FilterFun, Acc) ->
    {ok, Acc, GroupedRedsAcc, GroupedKVsAcc, GroupedKey};
reduce_stream_node(Bt, Dir, Node, KeyStart, InEndRangeFun, GroupedKey, GroupedKVsAcc,
        GroupedRedsAcc, KeyGroupFun, Fun, FilterFun, Acc) ->
    P = element(1, Node),
    case get_node(Bt, P) of
    {kp_node, NodeList} ->
        NodeList2 = adjust_dir(Dir, NodeList),
        reduce_stream_kp_node(Bt, Dir, NodeList2, KeyStart, InEndRangeFun, GroupedKey,
                GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, FilterFun, Acc);
    {kv_node, KVs} ->
        KVs2 = adjust_dir(Dir, KVs),
        reduce_stream_kv_node(Bt, Dir, KVs2, KeyStart, InEndRangeFun, GroupedKey,
                GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, FilterFun, Acc)
    end.

reduce_stream_kv_node(Bt, Dir, KVs, KeyStart, InEndRangeFun,
                        GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
                        KeyGroupFun, Fun, FilterFun, Acc) ->

    GTEKeyStartKVs =
    case KeyStart of
    undefined ->
        KVs;
    _ ->
        DropFun = case Dir of
        fwd ->
            fun({Key, _}) -> less(Bt, Key, KeyStart) end;
        rev ->
            fun({Key, _}) -> less(Bt, KeyStart, Key) end
        end,
        lists:dropwhile(DropFun, KVs)
    end,
    KVs2 = lists:takewhile(
        fun({Key, _}) -> InEndRangeFun(Key) end, GTEKeyStartKVs),
    reduce_stream_kv_node2(Bt, KVs2, GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
                        KeyGroupFun, Fun, FilterFun, Acc).


reduce_stream_kv_node2(_Bt, [], GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
        _KeyGroupFun, _Fun, _FilterFun, Acc) ->
    {ok, Acc, GroupedRedsAcc, GroupedKVsAcc, GroupedKey};
reduce_stream_kv_node2(Bt, [{Key, _Value} = KV | RestKVs], GroupedKey, GroupedKVsAcc,
        GroupedRedsAcc, KeyGroupFun, Fun, FilterFun, Acc) ->
    AssembledValue = assemble(Bt, KV),
    case GroupedKey of
    undefined ->
        case FilterFun(value, AssembledValue) of
        true ->
            reduce_stream_kv_node2(Bt, RestKVs, Key,
                [AssembledValue], [], KeyGroupFun, Fun, FilterFun, Acc);
        false ->
            reduce_stream_kv_node2(Bt, RestKVs, GroupedKey,
                GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, FilterFun, Acc)
        end;
    _ ->

        case KeyGroupFun(GroupedKey, Key) of
        true ->
            case FilterFun(value, AssembledValue) of
            true ->
                reduce_stream_kv_node2(Bt, RestKVs, GroupedKey,
                    [AssembledValue|GroupedKVsAcc], GroupedRedsAcc, KeyGroupFun,
                    Fun, FilterFun, Acc);
            false ->
                reduce_stream_kv_node2(Bt, RestKVs, GroupedKey,
                    GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun,
                    Fun, FilterFun, Acc)
            end;
        false ->
            case FilterFun(value, AssembledValue) of
            true ->
                case Fun(GroupedKey, {GroupedKVsAcc, GroupedRedsAcc}, Acc) of
                {ok, Acc2} ->
                    reduce_stream_kv_node2(Bt, RestKVs, Key, [AssembledValue],
                        [], KeyGroupFun, Fun, FilterFun, Acc2);
                {stop, Acc2} ->
                    throw({stop, Acc2})
                end;
            false ->
                reduce_stream_kv_node2(Bt, RestKVs, GroupedKey, GroupedKVsAcc,
                    GroupedRedsAcc, KeyGroupFun, Fun, FilterFun, Acc)
            end
        end
    end.

reduce_stream_kp_node(Bt, Dir, NodeList, KeyStart, InEndRangeFun,
                        GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
                        KeyGroupFun, Fun, FilterFun, Acc) ->
    Nodes =
    case KeyStart of
    undefined ->
        NodeList;
    _ ->
        case Dir of
        fwd ->
            lists:dropwhile(fun({Key, _}) -> less(Bt, Key, KeyStart) end, NodeList);
        rev ->
            RevKPs = lists:reverse(NodeList),
            case lists:splitwith(fun({Key, _}) -> less(Bt, Key, KeyStart) end, RevKPs) of
            {_Before, []} ->
                NodeList;
            {Before, [FirstAfter | _]} ->
                [FirstAfter | lists:reverse(Before)]
            end
        end
    end,
    {InRange, MaybeInRange} = lists:splitwith(
        fun({Key, _}) -> InEndRangeFun(Key) end, Nodes),
    NodesInRange = case MaybeInRange of
    [FirstMaybeInRange | _] when Dir =:= fwd ->
        InRange ++ [FirstMaybeInRange];
    _ ->
        InRange
    end,
    reduce_stream_kp_node2(Bt, Dir, NodesInRange, KeyStart, InEndRangeFun,
        GroupedKey, GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, FilterFun, Acc).


reduce_stream_kp_node2(Bt, Dir, [{_Key, NodeInfo} | RestNodeList], KeyStart, InEndRangeFun,
                        undefined, [], [], KeyGroupFun, Fun, FilterFun, Acc) ->
    {ok, Acc2, GroupedRedsAcc2, GroupedKVsAcc2, GroupedKey2} =
            reduce_stream_node(Bt, Dir, NodeInfo, KeyStart, InEndRangeFun, undefined,
                [], [], KeyGroupFun, Fun, FilterFun, Acc),
    reduce_stream_kp_node2(Bt, Dir, RestNodeList, KeyStart, InEndRangeFun, GroupedKey2,
            GroupedKVsAcc2, GroupedRedsAcc2, KeyGroupFun, Fun, FilterFun, Acc2);
reduce_stream_kp_node2(Bt, Dir, NodeList, KeyStart, InEndRangeFun,
        GroupedKey, GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, FilterFun, Acc) ->
    {Grouped0, Ungrouped0} = lists:splitwith(fun({Key,_}) ->
        KeyGroupFun(GroupedKey, Key) end, NodeList),
    {GroupedNodes, UngroupedNodes} =
    case Grouped0 of
    [] ->
        {Grouped0, Ungrouped0};
    _ ->
        [FirstGrouped | RestGrouped] = lists:reverse(Grouped0),
        {RestGrouped, [FirstGrouped | Ungrouped0]}
    end,
    {NotFilter, NeedFilter} = filter_branch(FilterFun, GroupedNodes),
    GroupedReds1 = [element(2, Node) || {_, Node} <- NotFilter],
    case NeedFilter of
    [] ->
        Acc2 = Acc,
        GroupedReds2 = GroupedReds1 ++ GroupedRedsAcc,
        GroupedKVsAcc2 = GroupedKVsAcc,
        GroupedKey2 = GroupedKey;
    _ ->
        #btree{reduce=ReduceFun} = Bt,
        {ok, Acc2, GroupedReds2Tmp, GroupedKVsAcc2, GroupedKey2} = lists:foldl(
            fun({_, Node}, {ok, A, RedsAcc, KVsAcc, K}) ->
                {ok, A2, RedsAcc2, KVsAcc2, K2} = reduce_stream_node(
                    Bt, Dir, Node, KeyStart, InEndRangeFun, K,
                    KVsAcc, RedsAcc, KeyGroupFun, Fun, FilterFun, A),
                % Reduce the KVs early to reduce memory usage
                Red = ReduceFun(reduce, KVsAcc2),
                RedsAcc3 = [Red | RedsAcc2],
                {ok, A2, RedsAcc3, [], K2}
            end,
            {ok, Acc, GroupedReds1 ++ GroupedRedsAcc, GroupedKVsAcc, GroupedKey},
            NeedFilter),
        % Rereduce the reduces early to reduce memory usage
        GroupedReds2 = [ReduceFun(rereduce, GroupedReds2Tmp)]
    end,
    case UngroupedNodes of
    [{_Key, NodeInfo}|RestNodes] ->
        {ok, Acc3, GroupedRedsAcc3, GroupedKVsAcc3, GroupedKey3} =
            reduce_stream_node(Bt, Dir, NodeInfo, KeyStart, InEndRangeFun, GroupedKey2,
                GroupedKVsAcc2, GroupedReds2, KeyGroupFun, Fun, FilterFun, Acc2),
        reduce_stream_kp_node2(Bt, Dir, RestNodes, KeyStart, InEndRangeFun, GroupedKey3,
                GroupedKVsAcc3, GroupedRedsAcc3, KeyGroupFun, Fun, FilterFun, Acc3);
    [] ->
        {ok, Acc2, GroupedReds2, GroupedKVsAcc, GroupedKey}
    end.

filter_branch(FilterFun, Nodes) ->
    filter_branch(FilterFun, Nodes, [], []).

filter_branch(_FilterFun, [], AllAcc, PartialAcc) ->
    {lists:reverse(AllAcc), lists:reverse(PartialAcc)};
filter_branch(FilterFun, [{_K, V} = Node | Rest], AllAcc, PartialAcc) ->
    case FilterFun(branch, element(2, V)) of
    all ->
        filter_branch(FilterFun, Rest, [Node | AllAcc], PartialAcc);
    partial ->
        filter_branch(FilterFun, Rest, AllAcc, [Node | PartialAcc]);
    none ->
        filter_branch(FilterFun, Rest, AllAcc, PartialAcc)
    end.

adjust_dir(fwd, List) ->
    List;
adjust_dir(rev, List) ->
    lists:reverse(List).

stream_node(Bt, Reds, Node, StartKey, InRange, Dir, Fun, Acc) ->
    Pointer = element(1, Node),
    {NodeType, NodeList} = get_node(Bt, Pointer),
    case NodeType of
    kp_node ->
        stream_kp_node(Bt, Reds, adjust_dir(Dir, NodeList), StartKey, InRange, Dir, Fun, Acc);
    kv_node ->
        stream_kv_node(Bt, Reds, adjust_dir(Dir, NodeList), StartKey, InRange, Dir, Fun, Acc)
    end.

stream_node(Bt, Reds, Node, InRange, Dir, Fun, Acc) ->
    Pointer = element(1, Node),
    {NodeType, NodeList} = get_node(Bt, Pointer),
    case NodeType of
    kp_node ->
        stream_kp_node(Bt, Reds, adjust_dir(Dir, NodeList), InRange, Dir, Fun, Acc);
    kv_node ->
        stream_kv_node2(Bt, Reds, [], adjust_dir(Dir, NodeList), InRange, Dir, Fun, Acc)
    end.

stream_kp_node(_Bt, _Reds, [], _InRange, _Dir, _Fun, Acc) ->
    {ok, Acc};
stream_kp_node(Bt, Reds, [{Key, Node} | Rest], InRange, Dir, Fun, Acc) ->
    Red = element(2, Node),
    case Fun(branch, Key, Red, Acc) of
    {ok, Acc2} ->
        case stream_node(Bt, Reds, Node, InRange, Dir, Fun, Acc2) of
        {ok, Acc3} ->
            stream_kp_node(Bt, [Red | Reds], Rest, InRange, Dir, Fun, Acc3);
        {stop, LastReds, Acc3} ->
            {stop, LastReds, Acc3}
        end;
    {skip, Acc2} ->
        stream_kp_node(Bt, [Red | Reds], Rest, InRange, Dir, Fun, Acc2)
    end.

drop_nodes(_Bt, Reds, _StartKey, []) ->
    {Reds, []};
drop_nodes(Bt, Reds, StartKey, [{NodeKey, Node} | RestKPs]) ->
    case less(Bt, NodeKey, StartKey) of
    true ->
        drop_nodes(Bt, [element(2, Node) | Reds], StartKey, RestKPs);
    false ->
        {Reds, [{NodeKey, Node} | RestKPs]}
    end.

stream_kp_node(Bt, Reds, KPs, StartKey, InRange, Dir, Fun, Acc) ->
    {NewReds, NodesToStream} =
    case Dir of
    fwd ->
        % drop all nodes sorting before the key
        drop_nodes(Bt, Reds, StartKey, KPs);
    rev ->
        % keep all nodes sorting before the key, AND the first node to sort after
        RevKPs = lists:reverse(KPs),
         case lists:splitwith(fun({Key, _Pointer}) -> less(Bt, Key, StartKey) end, RevKPs) of
        {_RevsBefore, []} ->
            % everything sorts before it
            {Reds, KPs};
        {RevBefore, [FirstAfter | Drop]} ->
            {[element(2, Node) || {_K, Node} <- Drop] ++ Reds,
                 [FirstAfter | lists:reverse(RevBefore)]}
        end
    end,
    case NodesToStream of
    [] ->
        {ok, Acc};
    [{_Key, Node} | Rest] ->
        case stream_node(Bt, NewReds, Node, StartKey, InRange, Dir, Fun, Acc) of
        {ok, Acc2} ->
            Red = element(2, Node),
            stream_kp_node(Bt, [Red | NewReds], Rest, InRange, Dir, Fun, Acc2);
        {stop, LastReds, Acc2} ->
            {stop, LastReds, Acc2}
        end
    end.

stream_kv_node(Bt, Reds, KVs, StartKey, InRange, Dir, Fun, Acc) ->
    DropFun =
    case Dir of
    fwd ->
        fun({Key, _}) -> less(Bt, Key, StartKey) end;
    rev ->
        fun({Key, _}) -> less(Bt, StartKey, Key) end
    end,
    {LTKVs, GTEKVs} = lists:splitwith(DropFun, KVs),
    case Bt#btree.assemble_kv of
    identity ->
        AssembleLTKVs = LTKVs;
    _ ->
        AssembleLTKVs = [assemble(Bt, KV) || KV <- LTKVs]
    end,
    stream_kv_node2(Bt, Reds, AssembleLTKVs, GTEKVs, InRange, Dir, Fun, Acc).

stream_kv_node2(_Bt, _Reds, _PrevKVs, [], _InRange, _Dir, _Fun, Acc) ->
    {ok, Acc};
stream_kv_node2(Bt, Reds, PrevKVs, [{K, _V} = KV | RestKVs], InRange, Dir, Fun, Acc) ->
    case InRange(K) of
    false ->
        {stop, {PrevKVs, Reds}, Acc};
    true ->
        AssembledKV = assemble(Bt, KV),
        case Fun(value, AssembledKV, {PrevKVs, Reds}, Acc) of
        {ok, Acc2} ->
            stream_kv_node2(Bt, Reds, [AssembledKV | PrevKVs], RestKVs, InRange, Dir, Fun, Acc2);
        {stop, Acc2} ->
            {stop, {PrevKVs, Reds}, Acc2}
        end
    end.


guided_purge(#btree{root = Root} = Bt, GuideFun, GuideAcc0) ->
    % inspired by query_modify/4
    {ok, KeyPointers, FinalGuideAcc, Bt2, _Go} = guided_purge(Bt, Root, GuideFun, GuideAcc0),
    {ok, NewRoot, Bt3} = complete_root(Bt2, KeyPointers),
    {ok, Bt3#btree{root = NewRoot}, FinalGuideAcc}.


guided_purge(Bt, NodeState, GuideFun, GuideAcc) ->
    % inspired by modify_node/5
    {NodeType, NodeList} = case NodeState of
    nil ->
        {kv_node, []};
    _Tuple ->
        Pointer = element(1, NodeState),
        get_node(Bt, Pointer)
    end,
    {ok, NewNodeList, GuideAcc2, Bt2, Go} =
    case NodeType of
    kp_node ->
        kp_guided_purge(Bt, NodeList, GuideFun, GuideAcc);
    kv_node ->
        kv_guided_purge(Bt, NodeList, GuideFun, GuideAcc)
    end,
    case NewNodeList of
    [] ->  % no nodes remain
        {ok, [], GuideAcc2, Bt2, Go};
    NodeList ->  % nothing changed
        {LastKey, _LastValue} = lists:last(NodeList),
        {ok, [{LastKey, NodeState}], GuideAcc2, Bt2, Go};
    _ ->
        {ok, ResultList, Bt3} = write_node(Bt2, NodeType, NewNodeList),
        {ok, ResultList, GuideAcc2, Bt3, Go}
    end.


kv_guided_purge(Bt, KvList, GuideFun, GuideAcc) ->
    kv_guided_purge(Bt, KvList, GuideFun, GuideAcc, []).

kv_guided_purge(Bt, [], _GuideFun, GuideAcc, ResultKvList) ->
    {ok, lists:reverse(ResultKvList), GuideAcc, Bt, ok};

kv_guided_purge(Bt, [KV | Rest] = KvList, GuideFun, GuideAcc, ResultKvList) ->
    AssembledKv = assemble(Bt, KV),
    case GuideFun(value, AssembledKv, GuideAcc) of
    {stop, GuideAcc2} ->
        {ok, lists:reverse(ResultKvList, KvList), GuideAcc2, Bt, stop};
    {purge, GuideAcc2} ->
        kv_guided_purge(Bt, Rest, GuideFun, GuideAcc2, ResultKvList);
    {keep, GuideAcc2} ->
        kv_guided_purge(Bt, Rest, GuideFun, GuideAcc2, [KV | ResultKvList])
    end.


kp_guided_purge(Bt, NodeList, GuideFun, GuideAcc) ->
    kp_guided_purge(Bt, NodeList, GuideFun, GuideAcc, []).

kp_guided_purge(Bt, [], _GuideFun, GuideAcc, ResultNode) ->
    {ok, lists:reverse(ResultNode), GuideAcc, Bt, ok};

kp_guided_purge(Bt, [{Key, NodeState} | Rest] = KpList, GuideFun, GuideAcc, ResultNode) ->
    case GuideFun(branch, element(2, NodeState), GuideAcc) of
    {stop, GuideAcc2} ->
        {ok, lists:reverse(ResultNode, KpList), GuideAcc2, Bt, stop};
    {keep, GuideAcc2} ->
        kp_guided_purge(Bt, Rest, GuideFun, GuideAcc2, [{Key, NodeState} | ResultNode]);
    {purge, GuideAcc2} ->
        kp_guided_purge(Bt, Rest, GuideFun, GuideAcc2, ResultNode);
    {partial_purge, GuideAcc2} ->
        {ok, ChildKPs, GuideAcc3, Bt2, Go} = guided_purge(Bt, NodeState, GuideFun, GuideAcc2),
        case Go of
        ok ->
            kp_guided_purge(Bt2, Rest, GuideFun, GuideAcc3, lists:reverse(ChildKPs, ResultNode));
        stop ->
            {ok, lists:reverse(ResultNode) ++ ChildKPs ++ Rest, GuideAcc3, Bt2, stop}
        end
    end.
