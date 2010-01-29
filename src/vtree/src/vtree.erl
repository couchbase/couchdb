-module(vtree).

-export([lookup/2, within/2, intersect/2, disjoint/2, insert/2, area/1,
         merge_mbr/2, find_min_nth/1, find_min_nth2/1, find_min_nth3/1,
         is_leaf_node/1, find_area_min_nth/1, partition_leaf_node/1,
         calc_nodes_mbr/1, calc_mbr/1, best_split/1, minimal_overlap/2,
         calc_overlap/2, minimal_coverage/2]).

% The bounding box coordinate order follows the GeoJSON specification (http://geojson.org/): {x-low, y-low, x-high, y-high}

% Design question: Should not fully filled nodes have only as many members as nodes, or be filled up with nils to their maximum number of nodes? - Current implementation is the first one (dynamic number of members).

% Nodes maximum/minimum filling grade (TODO vmx: shouldn't be hard-coded)
-define(MAX_FILLED, 4).
-define(MIN_FILLED, 2).
%-define(IS_LEAF_NODE(List), is_binary(element(2, hd(list)))).



lookup(_Bbox, []) ->
    [];

lookup(Bbox, Tree) ->
    Entries = lists:foldl(
        fun(Entry, Acc) ->
            case Entry of
                % Entry of an inner node
                {_Mbr, ChildNodes} when is_list(ChildNodes) ->
                    lookup(Bbox, ChildNodes);
                % Entry of a leaf node
                {Mbr, _} ->
                    Disjoint = disjoint(Mbr, Bbox),
                    if 
                        not Disjoint ->
                            Acc ++ [Entry];
                        true ->
                            Acc
                    end;
                _ ->
                    io:format("Tree/node is invalid"),
                    error
            end
        end, [], Tree),
    Entries.


% Tests if Inner is within Outer box
within(Inner, Outer) ->
    {IW, IS, IE, IN} = Inner,
    {OW, OS, OE, ON} = Outer,
    (IW >= OW) and (IS >= OS) and (IE =< OE) and (IN =< ON).


% Returns true if one Mbr intersects with another Mbr
intersect(Mbr1, Mbr2) ->
    {W1, S1, E1, N1} = Mbr1,
    {W2, S2, E2, N2} = Mbr2,
    % N or S of Mbr1 is potentially intersected with a vertical edge
    % from Mbr2
    ((((N2 >= N1) and (S2 =< N1)) or ((N2 >= S1) and (S2 =< S1))) and
    % N or S of Mbr1 *is* intersected if a vertical line of Mbr2 isn't
    % next to Mbr1
    (((W2 >= W1) and (W2 =< E1)) or ((E2 >= W1) and (E2 =< E1))))
    or
    % W or E of Mbr1 is potentially intersected with a horizontal edge
    % from Mbr2
    ((((E2 >= E1) and (W2 =< E1)) or ((E2 >= W1) and (W2 =< W1))) and
    % W or E of Mbr1 *is* intersected if a horizontal line of Mbr2 isn't
    % above or below Mbr1
    (((S2 >= S1) and (S2 =< N1)) or ((N2 >= S1) and (N2 =< N1)))).


% Returns true if two MBRs are spatially disjoint
disjoint(Mbr1, Mbr2) ->
    not (within(Mbr1, Mbr2) or within(Mbr2, Mbr1) or intersect(Mbr1, Mbr2)).



insert({Mbr, Id}, {}) ->
    {Mbr, [{Mbr, Id}]};

insert(NewNode, Tree) ->
    {TreeMbr, Entries} = Tree,
    %io:format("Entries: ~p~n", [Entries]),
    NodeSize = length(Entries),
    IsLeafNode = is_leaf_node(Entries),
    %{_Mbr, ChildNodes} = Tree,
    if
        %?IS_LEAF_NODE(Tree) ->
        IsLeafNode ->
            NewLeafNodeMbr = merge_mbr(TreeMbr, element(1, NewNode)),
            NewLeafNode = {NewLeafNodeMbr, Entries ++ [NewNode]},
            if
                NodeSize < ?MAX_FILLED ->
                    %io:format("New leaf node: ~p~n", [NewLeafNode]),
                    NewLeafNode;
                true ->
                    Partition = partition_leaf_node(NewLeafNode),
                    %io:format("Partition: ~p~n", [Partition]),
                    SplittedNodes = best_split(Partition),
                    case SplittedNodes of
                        {tie, PartitionMbrs} ->
                            SplittedNodes2 = minimal_overlap(Partition,
                                                             PartitionMbrs),
                            case SplittedNodes2 of
                                tie ->
                                    SplittedNodes3 = minimal_coverage(
                                                       Partition,
                                                       PartitionMbrs),
                                    [{Mbr1, _}, {Mbr2, _}] = SplittedNodes3,
                                    {merge_mbr(Mbr1, Mbr2), SplittedNodes3};
                                _ ->
                                    [{Mbr1, _}, {Mbr2, _}] = SplittedNodes2,
                                    {merge_mbr(Mbr1, Mbr2), SplittedNodes2}
                            end;
                        _ ->
                            %io:format("Split nodes into: ~p~n", [SplittedNodes]),
                            [{Mbr1, _}, {Mbr2, _}] = SplittedNodes,
                            {merge_mbr(Mbr1, Mbr2), SplittedNodes}
                    end
            end;
        % "Tree" is an inner node
        true ->
            % Get entry where smallest MBR expansion is needed
            %io:format("Inner Node!~n", []),
            Expanded = lists:map(
                fun(Entry) ->
                    {EntryMbr, _} = Entry,
                    {NewNodeMbr, _} = NewNode,
                    EntryArea = area(EntryMbr),
                    AreaMbr = merge_mbr(EntryMbr, NewNodeMbr),
                    NewArea = area(AreaMbr),
                    AreaDiff = NewArea - EntryArea,
                    %{EntryArea, NewArea, AreaDiff}
                    {AreaDiff, AreaMbr}
                    % XXX TODO vmx: Select right node and add new node
                end, Entries),
            MinPos = find_area_min_nth(Expanded),
            %io:format("Tree|Expanded|MinPos: ~p|~p|~p~n", [Tree, Expanded, MinPos]),
            SubTree = lists:nth(MinPos, Entries),
            %{_Area, NewMbr} = lists:nth(MinPos, Expanded),
            NewInnerNode = insert(NewNode, SubTree),
            NewMbr = merge_mbr(TreeMbr, element(1, NewInnerNode)),
            {A, B} = lists:split(MinPos-1, Entries),
            {NewMbr, A ++ [NewInnerNode] ++ tl(B)}
    end.

area(Mbr) ->
    {W, S, E, N} = Mbr,
    abs(E-W) * abs(N-S).


merge_mbr(Mbr1, Mbr2) ->
    {W1, S1, E1, N1} = Mbr1,
    {W2, S2, E2, N2} = Mbr2,
    {min(W1, W2), min(S1, S2), max(E1, E2), max(N1, N2)}.


% Find the position of the smallest element in a list
find_min_nth([H|T]) ->
    find_min_nth(1, T, {H, 1}).

find_min_nth(Count, [], {Min, MinCount}) ->
    MinCount;

find_min_nth(Count, [H|T], {Min, MinCount}) ->
    if
        H < Min ->
            find_min_nth(Count+1, T, {H, Count+1});
        true ->
            find_min_nth(Count+1, T, {Min, MinCount})
   end.


find_min_nth2([H|T]) ->
    {_, {_, Nth}} = lists:foldl(
        fun(H, {Count, {Min, MinCount}}) ->
            if
                H < Min ->
                    {Count+1, {H, Count+1}};
                true ->
                    {Count+1, {Min, MinCount}}
            end
        end, {1, {H, 1}}, T),
    Nth.

find_min_nth3([H|T]) ->
    find_min_nth(1, T, {H, 1}).

find_min_nth3(Count, [], {_Min, MinCount}) ->
    MinCount;

find_min_nth3(Count, [H|T], {Min, _MinCount}) when H < Min->
    find_min_nth(Count+1, T, {H, Count+1});

find_min_nth3(Count, [_H|T], {Min, MinCount}) ->
    find_min_nth(Count+1, T, {Min, MinCount}).


find_area_min_nth([H|T]) ->
    %io:format("H: ~p~n", [H]),
    find_area_min_nth(1, T, {H, 1}).

find_area_min_nth(_Count, [], {_Min, MinCount}) ->
    MinCount;

find_area_min_nth(Count, [{HMin, HMbr}|T], {{Min, _Mbr}, _MinCount})
  when HMin < Min->
    %io:format("H (H<MIN): ~p~n", [HMin]),
    find_area_min_nth(Count+1, T, {{HMin, HMbr}, Count+1});

find_area_min_nth(Count, [_H|T], {{Min, Mbr}, MinCount}) ->
    %io:format("H|Min (H>MIN): ~p|~p~n", [H, Min]),
    find_area_min_nth(Count+1, T, {{Min, Mbr}, MinCount}).



is_leaf_node([H|_T]) ->
    %io:format("is_leaf_node: ~p~n", [H]),
    {Mbr, NodeId} = H,
    %{_Mbr1, ChildNodes} = H,
    %{_Mbr2, NodeId} = hd(ChildNodes),
    is_binary(NodeId).


% Partitions a leaf node into for nodes (one for every direction)
partition_leaf_node({Mbr, Nodes}) ->
    {MbrW, MbrS, MbrE, MbrN} = Mbr,
    %io:format("MBRs: ~p, ~p, ~p, ~p~n", [MbrW, MbrS, MbrE, MbrN]),
    lists:foldl(
        fun(Node,  {AccW, AccS, AccE, AccN}) ->
            {{W, S, E, N}, _Id} = Node,
            if
                W-MbrW < MbrE-E ->
                    NewAccW = AccW ++ [Node],
                    NewAccE = AccE;
                true ->
                    NewAccW = AccW,
                    NewAccE = AccE ++ [Node]
            end,
            if
                S-MbrS < MbrN-N ->
                    NewAccS = AccS ++ [Node],
                    NewAccN = AccN;
                true ->
                    NewAccS = AccS,
                    NewAccN = AccN ++ [Node]
            end,
            {NewAccW, NewAccS, NewAccE, NewAccN}
        end, {[],[],[],[]}, Nodes).


calc_nodes_mbr(Nodes) ->
    {Mbrs, _Ids} = lists:unzip(Nodes),
    calc_mbr(Mbrs).

calc_mbr([H|T]) ->
    calc_mbr(T, H).

calc_mbr([Mbr|T], AccMbr) ->
    MergedMbr = merge_mbr(Mbr, AccMbr),
    calc_mbr(T, MergedMbr);
calc_mbr([], Acc) ->
    Acc.


best_split({PartW, PartS, PartE, PartN}) ->
    MbrW = calc_nodes_mbr(PartW),
    MbrE = calc_nodes_mbr(PartE),
    MbrS = calc_nodes_mbr(PartS),
    MbrN = calc_nodes_mbr(PartN),
    MaxWE = max(length(PartW), length(PartE)),
    MaxSN = max(length(PartS), length(PartN)),
    if
        MaxWE < MaxSN ->
            [{MbrW, PartW}, {MbrE, PartE}];
        MaxWE > MaxSN ->
            [{MbrS, PartS}, {MbrN, PartN}];
        true ->
            % MBRs are needed for further calculation
            {tie,  {MbrW, MbrS, MbrE, MbrN}}
    end.


minimal_overlap({PartW, PartS, PartE, PartN}, {MbrW, MbrS, MbrE, MbrN}) ->
    OverlapWE = area(calc_overlap(MbrW, MbrE)),
    OverlapSN = area(calc_overlap(MbrS, MbrN)),
    %io:format("overlap: ~p|~p~n", [OverlapWE, OverlapSN]),
    if
        OverlapWE < OverlapSN ->
            [{MbrW, PartW}, {MbrE, PartE}];
        OverlapWE > OverlapSN ->
            [{MbrS, PartS}, {MbrN, PartN}];
        true ->
            tie
    end.

minimal_coverage({PartW, PartS, PartE, PartN}, {MbrW, MbrS, MbrE, MbrN}) ->
    CoverageWE = area(MbrW) + area(MbrE),
    CoverageSN = area(MbrS) + area(MbrN),
    %io:format("coverage: ~p|~p~n", [CoverageWE, CoverageSN]),
    if
        CoverageWE < CoverageSN ->
            [{MbrW, PartW}, {MbrE, PartE}];
        true ->
            [{MbrS, PartS}, {MbrN, PartN}]
    end.

calc_overlap(Mbr1, Mbr2) ->
    IsDisjoint = disjoint(Mbr1, Mbr2),
    if
        not IsDisjoint ->
            {W1, S1, E1, N1} = Mbr1,
            {W2, S2, E2, N2} = Mbr2,
            {max(W1, W2), max(S1, S2), min(E1, E2), min(N1, N2)};
        true ->
            {0, 0, 0, 0}
    end.


min(A, B) ->
    if A > B -> B ; true -> A end.


max(A, B) ->
    if A > B -> A ; true -> B end.
