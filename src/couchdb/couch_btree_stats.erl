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

-module(couch_btree_stats).

-export([compute/1]).

-include("couch_db.hrl").

-record(stats, {
    kv_count = 0,
    kp_nodes = 0,
    kv_nodes = 0,
    depths = [],
    reduction_sizes = [],
    elements_per_kp_node = [],
    elements_per_kv_node = [],
    kp_node_sizes = [],
    compressed_kp_node_sizes = [],
    kv_node_sizes = [],
    compressed_kv_node_sizes = [],
    key_sizes = [],
    value_sizes = []
}).


% Use only for debugging/analysis. This will traverse the whole btree.
compute(#btree{root = Root, fd = Fd} = Bt) ->
    #stats{
        depths = Depths0,
        reduction_sizes = RedSizes0,
        elements_per_kv_node = ElementsPerKvNode0,
        elements_per_kp_node = ElementsPerKpNode0,
        kv_node_sizes = KvNodeSizes0,
        kp_node_sizes = KpNodeSizes0,
        compressed_kv_node_sizes = CompKvNodeSizes0,
        compressed_kp_node_sizes = CompKpNodeSizes0,
        key_sizes = KeySizes0,
        value_sizes = ValueSizes0
    } = StatsRec0 = collect_stats(Root, Bt, 0, #stats{}),

    StatsRec = StatsRec0#stats{
        depths = lists:sort(Depths0),
        reduction_sizes = lists:sort(RedSizes0),
        elements_per_kv_node = lists:sort(ElementsPerKvNode0),
        elements_per_kp_node = lists:sort(ElementsPerKpNode0),
        kv_node_sizes = lists:sort(KvNodeSizes0),
        kp_node_sizes = lists:sort(KpNodeSizes0),
        compressed_kv_node_sizes = lists:sort(CompKvNodeSizes0),
        compressed_kp_node_sizes = lists:sort(CompKpNodeSizes0),
        key_sizes = lists:sort(KeySizes0),
        value_sizes = lists:sort(ValueSizes0)
    },

    StatNames = record_info(fields, stats),
    StatPoses = lists:seq(2, record_info(size, stats)),
    ExpandableStats = [
        {depths, <<"depth">>},
        {reduction_sizes, <<"reduction_size">>},
        {elements_per_kp_node, <<"elements_per_kp_node">>},
        {elements_per_kv_node, <<"elements_per_kv_node">>},
        {kp_node_sizes, <<"kp_node_size">>},
        {kv_node_sizes, <<"kv_node_size">>},
        {compressed_kv_node_sizes, <<"compressed_kv_node_size">>},
        {compressed_kp_node_sizes, <<"compressed_kp_node_size">>},
        {key_sizes, <<"key_size">>},
        {value_sizes, <<"value_size">>}
    ],
    Stats = lists:foldr(
        fun({StatName, StatPos}, Acc) ->
            Val = element(StatPos, StatsRec),
            case couch_util:get_value(StatName, ExpandableStats) of
            undefined ->
                [{?l2b(atom_to_list(StatName)), Val} | Acc];
            StatName2 ->
                expand_stat(Val, StatName2) ++ Acc
            end
        end,
        [],
        lists:zip(StatNames, StatPoses)),
    {ok, FileSize} = couch_file:bytes(Fd),
    BtSize = couch_btree:size(Bt),
    Frag = case FileSize of
    0 ->
        0;
    _ ->
        ((FileSize - BtSize) / FileSize) * 100
    end,
    [
        {<<"btree_size">>, BtSize},
        {<<"file_size">>, FileSize},
        {<<"fragmentation">>, Frag},
        {<<"kv_chunk_threshold">>, Bt#btree.kv_chunk_threshold},
        {<<"kp_chunk_threshold">>, Bt#btree.kp_chunk_threshold} | Stats
    ].


collect_stats(nil, _Bt, _Depth, Stats) ->
    Stats;
collect_stats(Node, Bt, Depth, Stats) ->
    Pointer = element(1, Node),
    Reduction = element(2, Node),
    {{NodeType, NodeList}, NodeCompSize, NodeSize} = get_node(Bt, Pointer),
    ChildCount = length(NodeList),
    RedSize = thing_size(Reduction),
    case NodeType of
    kv_node ->
        Stats#stats{
            kv_nodes = Stats#stats.kv_nodes + 1,
            depths = [Depth + 1 | Stats#stats.depths],
            kv_count = Stats#stats.kv_count + ChildCount,
            elements_per_kv_node = [ChildCount | Stats#stats.elements_per_kv_node],
            kv_node_sizes = [NodeSize | Stats#stats.kv_node_sizes],
            compressed_kv_node_sizes =
                [NodeCompSize | Stats#stats.compressed_kv_node_sizes],
            reduction_sizes = [RedSize | Stats#stats.reduction_sizes],
            key_sizes =
                [thing_size(K) || {K, _} <- NodeList] ++ Stats#stats.key_sizes,
            value_sizes =
                [thing_size(V) || {_, V} <- NodeList] ++ Stats#stats.value_sizes
        };
    kp_node ->
        Stats2 = Stats#stats{
            kp_nodes = Stats#stats.kp_nodes + 1,
            elements_per_kp_node = [ChildCount | Stats#stats.elements_per_kp_node],
            kp_node_sizes = [NodeSize | Stats#stats.kp_node_sizes],
            compressed_kp_node_sizes =
                [NodeCompSize | Stats#stats.compressed_kp_node_sizes],
            reduction_sizes = [RedSize | Stats#stats.reduction_sizes]
        },
        lists:foldl(
            fun({_Key, NodeState}, StatsAcc) ->
                collect_stats(NodeState, Bt, Depth + 1, StatsAcc)
            end,
            Stats2,
            NodeList)
    end.


get_node(#btree{fd = Fd, binary_mode = false}, NodePos) ->
    {ok, CompressedBin} = couch_file:pread_binary(Fd, NodePos),
    Bin = couch_compress:decompress(CompressedBin),
    {binary_to_term(Bin), byte_size(CompressedBin), byte_size(Bin)};
get_node(#btree{fd = Fd, binary_mode = true}, NodePos) ->
    {ok, CompressedBin} = couch_file:pread_binary(Fd, NodePos),
    <<TypeInt, NodeBin/binary>> = Bin = couch_compress:decompress(CompressedBin),
    Type = if TypeInt == 1 -> kv_node; true -> kp_node end,
    {couch_btree:decode_node(Type, NodeBin, []), byte_size(CompressedBin), byte_size(Bin)}.


expand_stat(Values, StatName) ->
    MaxName = iolist_to_binary(["max_", StatName]),
    MinName = iolist_to_binary(["min_", StatName]),
    AvgName = iolist_to_binary(["avg_", StatName]),
    P90Name = iolist_to_binary([StatName, "_90_percentile"]),
    P95Name = iolist_to_binary([StatName, "_95_percentile"]),
    P99Name = iolist_to_binary([StatName, "_99_percentile"]),
    case Values of
    [] ->
        MaxValue = 0,
        MinValue = 0,
        AvgValue = 0;
    _ ->
        MaxValue = lists:last(Values),
        MinValue = hd(Values),
        AvgValue = lists:sum(Values) / length(Values)
    end,
    [
         {MaxName, MaxValue},
         {MinName, MinValue},
         {AvgName, AvgValue},
         {P90Name, percentile(Values, 90)},
         {P95Name, percentile(Values, 95)},
         {P99Name, percentile(Values, 99)}
    ].


percentile([], _Perc) ->
    0;
percentile(Values, Perc) ->
    lists:nth(round((Perc / 100) * length(Values) + 0.5), Values).


thing_size(Bin) when is_binary(Bin) ->
    byte_size(Bin);
thing_size(Term) ->
    ?term_size(Term).
