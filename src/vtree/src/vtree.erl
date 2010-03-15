-module(vtree).

-export([lookup/2, within/2, intersect/2, disjoint/2, insert/3, area/1,
         merge_mbr/2, find_area_min_nth/1, partition_node/1,
         calc_nodes_mbr/1, calc_mbr/1, best_split/1, minimal_overlap/2,
         calc_overlap/2, minimal_coverage/2]).

-export([get_node/2]).

% The bounding box coordinate order follows the GeoJSON specification (http://geojson.org/): {x-low, y-low, x-high, y-high}

% Design question: Should not fully filled nodes have only as many members as nodes, or be filled up with nils to their maximum number of nodes? - Current implementation is the first one (dynamic number of members).

% Nodes maximum/minimum filling grade (TODO vmx: shouldn't be hard-coded)
-define(MAX_FILLED, 4).
-define(MIN_FILLED, 2).

-define(FILENAME, "/tmp/vtree.bin").


% XXX vmx: rethink "leaf" type. At the moment it's used for the nodes that
%    contain the actual data (the doc IDs) and their parent node.
-record(node, {
    % type = inner | leaf
    type=inner}).


lookup(_Bbox, {}) ->
    {};

lookup(Bbox, Tree) ->
    {_, Nodes} = Tree,
    Entries = lists:foldl(
        fun(Entry, Acc) ->
            case Entry of
                % Entry of an inner node
                {_Mbr, ChildNodes} when is_list(ChildNodes) ->
%                    lookup(Bbox, ChildNodes);
%                    lookup(Bbox, Entry);
                    Acc ++ lookup(Bbox, Entry);
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
%        end, [], Tree),
        end, [], Nodes),
    Entries.


% Tests if Inner is within Outer box
within(Inner, Outer) ->
%    io:format("(within) Inner, Outer: ~p, ~p~n", [Inner, Outer]),
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
    %io:format("(disjoint) Mbr1, Mbr2: ~p, ~p~n", [Mbr1, Mbr2]),
    not (within(Mbr1, Mbr2) or within(Mbr2, Mbr1) or intersect(Mbr1, Mbr2)).


split_node({_Mbr, Meta, _EntriesPos}=Node) ->
    %io:format("We need to split~n", []),
    Partition = partition_node(Node),
    SplittedLeaf = best_split(Partition),
    [{Mbr1, Children1}, {Mbr2, Children2}] = case SplittedLeaf of
    {tie, PartitionMbrs} ->
        SplittedLeaf2 = minimal_overlap(Partition, PartitionMbrs),
        case SplittedLeaf2 of
        tie ->
            minimal_coverage(Partition, PartitionMbrs);
        _ ->
            SplittedLeaf2
        end;
    _ ->
        SplittedLeaf
    end,
    case Meta#node.type of
    leaf ->
        {merge_mbr(Mbr1, Mbr2),
         {Mbr1, #node{type=leaf}, Children1},
         {Mbr2, #node{type=leaf}, Children2}};
    inner ->
        % Child nodes were expanded (read from file) to do some calculations,
        % now get the pointers to their position in the file back and return
        % only a list of these positions.
        ChildrenPos1 = lists:map(fun(Entry) ->
            {_, _, EntryPos} = Entry,
            EntryPos
        end, Children1),
        ChildrenPos2 = lists:map(fun(Entry) ->
            {_, _, EntryPos} = Entry,
            EntryPos
        end, Children2),
        {merge_mbr(Mbr1, Mbr2),
         {Mbr1, #node{type=inner}, ChildrenPos1},
         {Mbr2, #node{type=inner}, ChildrenPos2}}
    end.


% Return values of insert:
% At top-level: {ok, MBR, position_in_file}
% If a split occurs: {splitted, MBR_of_both_nodes, position_in_file_node1,
%                     position_in_file_node2}
insert(Fd, -1, {Mbr, Meta, Id}) ->
    InitialTree = {Mbr, #node{type=leaf}, [{Mbr, Meta, Id}]},
    {ok, Pos} = couch_file:append_term(Fd, InitialTree),
    {ok, Mbr, Pos};

insert(Fd, RootPos, NewNode) ->
    insert(Fd, RootPos, NewNode, 0).

insert(Fd, RootPos, {NewNodeMbr, NewNodeMeta, NewNodeId}, CallDepth) ->
    NewNode = {NewNodeMbr, NewNodeMeta#node{type=leaf}, NewNodeId},
    {ok, {TreeMbr, Meta, EntriesPos}} = couch_file:pread_term(Fd, RootPos),
    % EntriesPos is only a pointer to the node (position in file)
    Entries = case Meta#node.type of
    leaf ->
        EntriesPos;
        % NOTE vmx: Currently we don't save each entry individually, but the
        %     whole set of entries. This might be worth changing in the future
        %lists:map(fun(EntryPos) ->
        %    {ok, CurEntry} = couch_file:pread_term(Fd, EntryPos),
        %    CurEntry
        %end, EntriesPos),
    % If the nodes are inner nodes, they only contain pointers to their child
    % nodes. We only need their MBRs, position, but not their children's
    % position. Read them from disk, but store their position in file (pointer
    % from parent node) instead of their child nodes.
    inner ->
        lists:map(fun(EntryPos) ->
            {ok, CurEntry} = couch_file:pread_term(Fd, EntryPos),
            {EntryMbr, EntryMeta, _} = CurEntry,
            {EntryMbr, EntryMeta, EntryPos}
        end, EntriesPos)
    end,
    %io:format("Entries: ~p~n", [Entries]),
    EntryNum = length(Entries),
    Inserted = case Meta#node.type of
    leaf ->
        %io:format("I'm a leaf node~n", []),
        LeafNodeMbr = merge_mbr(TreeMbr, element(1, NewNode)),
        LeafNode = {LeafNodeMbr, #node{type=leaf}, Entries ++ [NewNode]},
        if
        EntryNum < ?MAX_FILLED ->
            %io:format("There's plenty of space (leaf node)~n", []),
            {ok, Pos} = couch_file:append_term(Fd, LeafNode),
            {ok, LeafNodeMbr, Pos};
        % do the fancy split algorithm
        true ->
            %io:format("We need to split (leaf node)~p~n", [CallDepth]),
            {SplittedMbr, Node1, Node2} = split_node(LeafNode),
            {ok, Pos1} = couch_file:append_term(Fd, Node1),
            {ok, Pos2} = couch_file:append_term(Fd, Node2),
            {splitted, SplittedMbr, Pos1, Pos2}
        end;
    inner ->
        %io:format("I'm an inner node~n", []),
        % Get entry where smallest MBR expansion is needed
        Expanded = lists:map(
            fun(Entry) ->
                {EntryMbr, _, _} = Entry,
                {NewNodeMbr2, _, _} = NewNode,
                EntryArea = area(EntryMbr),
                AreaMbr = merge_mbr(EntryMbr, NewNodeMbr2),
                NewArea = area(AreaMbr),
                AreaDiff = NewArea - EntryArea,
                %{EntryArea, NewArea, AreaDiff}
                {AreaDiff, AreaMbr}
            end, Entries),
        MinPos = find_area_min_nth(Expanded),
        SubTreePos = lists:nth(MinPos, EntriesPos),
        case insert(Fd, SubTreePos, NewNode, CallDepth+1) of
        {ok, ChildMbr, ChildPos} ->
            NewMbr = merge_mbr(TreeMbr, ChildMbr),
            {A, B} = lists:split(MinPos-1, EntriesPos),
            % NOTE vmx: I guess child nodes don't really have an order, we
            %     could remove the old node and append the new one at the
            %     end of the list.
            NewNode2 = {NewMbr, #node{type=inner}, A ++ [ChildPos] ++ tl(B)},
            {ok, Pos} = couch_file:append_term(Fd, NewNode2),
            {ok, NewMbr, Pos};
        {splitted, ChildMbr, ChildPos1, ChildPos2} ->
            NewMbr = merge_mbr(TreeMbr, ChildMbr),
            %io:format("EntryNum: ~p~n", [EntryNum]),
            if
            % Both nodes of the split fit in the current inner node
            EntryNum+1 < ?MAX_FILLED ->
                % NOTE vmx: Weird, it seems that this point is never reached
                %io:format("There's plenty of space (inner node)~n", []),
                {A, B} = lists:split(MinPos-1, EntriesPos),
                NewNode2 = {NewMbr, #node{type=inner},
                            A ++ [ChildPos1, ChildPos2] ++ tl(B)},
                {ok, Pos} = couch_file:append_term(Fd, NewNode2),
                {ok, NewMbr, Pos};
            % We need to split the inner node
            true ->
                %io:format("We need to split (inner node)~n", []),
                {SplittedMbr, Node1, Node2} = split_node(
                        {NewMbr, #node{type=inner}, Entries}),
                {ok, Pos1} = couch_file:append_term(Fd, Node1),
                {ok, Pos2} = couch_file:append_term(Fd, Node2),
                {splitted, SplittedMbr, Pos1, Pos2}
            end
        end
    end,
    case {Inserted, CallDepth} of
        % Root node needs to be split => new root node
        {{splitted, NewRootMbr, SplittedNode1, SplittedNode2}, 0} ->
            %io:format("Creating new root node~n", []),
            NewRoot = {NewRootMbr, #node{type=inner},
                           [SplittedNode1, SplittedNode2]},
            {ok, NewRootPos} = couch_file:append_term(Fd, NewRoot),
            {ok, NewRootMbr, NewRootPos};
        _ ->
            Inserted
    end.

area(Mbr) ->
    {W, S, E, N} = Mbr,
    abs(E-W) * abs(N-S).


merge_mbr(Mbr1, Mbr2) ->
    {W1, S1, E1, N1} = Mbr1,
    {W2, S2, E2, N2} = Mbr2,
    {min(W1, W2), min(S1, S2), max(E1, E2), max(N1, N2)}.


find_area_min_nth([H|T]) ->
    find_area_min_nth(1, T, {H, 1}).

find_area_min_nth(_Count, [], {_Min, MinCount}) ->
    MinCount;

find_area_min_nth(Count, [{HMin, HMbr}|T], {{Min, _Mbr}, _MinCount})
  when HMin < Min->
    find_area_min_nth(Count+1, T, {{HMin, HMbr}, Count+1});

find_area_min_nth(Count, [_H|T], {{Min, Mbr}, MinCount}) ->
    find_area_min_nth(Count+1, T, {{Min, Mbr}, MinCount}).

partition_node({Mbr, _Meta, Nodes}) ->
    {MbrW, MbrS, MbrE, MbrN} = Mbr,
%    io:format("(partition_node) Mbr: ~p~n", [Mbr]),
%    io:format("(partition_node) Nodes: ~p~n", [Nodes]),
    Tmp = lists:foldl(
        fun(Node,  {AccW, AccS, AccE, AccN}) ->
            {{W, S, E, N}, _Meta, _Id} = Node,
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
        end, {[],[],[],[]}, Nodes),
%    io:format("(partition_node) Partitioned: ~p~n", [Tmp]),
    % XXX vmx This is a hack! A better partitioning algorithm should be used.
    %     If the two corresponding partitions are empty, split node in the
    %     middle
    case Tmp of
        {[], [], Es, Ns} ->
            {NewW, NewE} = lists:split(length(Es) div 2, Es),
            {NewS, NewN} = lists:split(length(Ns) div 2, Ns),
            {NewW, NewS, NewE, NewN};
        {Ws, Ss, [], []} ->
            {NewW, NewE} = lists:split(length(Ws) div 2, Ws),
            {NewS, NewN} = lists:split(length(Ss) div 2, Ss),
            {NewW, NewS, NewE, NewN};
        {Ws, [], [], Ns} ->
            {NewW, NewE} = lists:split(length(Ws) div 2, Ws),
            {NewS, NewN} = lists:split(length(Ns) div 2, Ns),
            {NewW, NewS, NewE, NewN};
        {[], Ss, Es, []} ->
            {NewS, NewN} = lists:split(length(Ss) div 2, Ss),
            {NewW, NewE} = lists:split(length(Es) div 2, Es),
            {NewW, NewS, NewE, NewN};
        _ ->
            Tmp
    end.


calc_nodes_mbr(Nodes) ->
    {Mbrs, _Meta, _Ids} = lists:unzip3(Nodes),
    calc_mbr(Mbrs).

calc_mbr(Mbrs) when length(Mbrs) == 0 ->
    error;
calc_mbr([H|T]) ->
    calc_mbr(T, H).

calc_mbr([Mbr|T], AccMbr) ->
    MergedMbr = merge_mbr(Mbr, AccMbr),
    calc_mbr(T, MergedMbr);
calc_mbr([], Acc) ->
    Acc.


best_split({PartW, PartS, PartE, PartN}) ->
    %io:format("(best_split) PartW, PartS, PartE, PartN: ~p, ~p, ~p, ~p~n", [PartW, PartS, PartE, PartN]),
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
    %io:format("(minimal_overlap) MbrW, MbrS, MbrE, MbrN: ~p, ~p, ~p, ~p~n", [MbrW, MbrS, MbrE, MbrN]),
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
%    io:format("(calc_overlap) Mbr1, Mbr2: ~p, ~p~n", [Mbr1, Mbr2]),
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


get_node(Fd, Pos) ->
    couch_file:pread_term(Fd, Pos).
