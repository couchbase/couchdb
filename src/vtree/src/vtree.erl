-module(vtree).

-export([lookup/2, within/2, intersect/2, disjoint/2, insert/3, area/1,
         merge_mbr/2, find_min_nth/1, find_min_nth2/1, find_min_nth3/1,
         is_leaf_node/1, find_area_min_nth/1, partition_leaf_node/1,
         calc_nodes_mbr/1, calc_mbr/1, best_split/1, minimal_overlap/2,
         calc_overlap/2, minimal_coverage/2]).

-export([get_node/2]).
-export([insert2/3]).

% The bounding box coordinate order follows the GeoJSON specification (http://geojson.org/): {x-low, y-low, x-high, y-high}

% Design question: Should not fully filled nodes have only as many members as nodes, or be filled up with nils to their maximum number of nodes? - Current implementation is the first one (dynamic number of members).

% Nodes maximum/minimum filling grade (TODO vmx: shouldn't be hard-coded)
-define(MAX_FILLED, 40).
-define(MIN_FILLED, 20).
%-define(IS_LEAF_NODE(List), is_binary(element(2, hd(list)))).

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


split_node({LeafNodeMbr, LeafNodeMeta, EntriesPos}=LeafNode) ->
    %io:format("We need to split~n", []),
    Partition = partition_leaf_node(LeafNode),
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
    case LeafNodeMeta#node.type of
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
    %{SplittedLeafMbr, Node1, Node2}.


% Return values of insert:
% At top-level: {ok, MBR, position_in_file}
% If a split occurs: {splitted, MBR_of_both_nodes, position_in_file_node1,
%                     position_in_file_node2}
insert2(Fd, -1, {Mbr, Meta, Id}) ->
    InitialTree = {Mbr, #node{type=leaf}, [{Mbr, Meta, Id}]},
    {ok, Pos} = couch_file:append_term(Fd, InitialTree),
    {ok, Mbr, Pos};

insert2(Fd, RootPos, NewNode) ->
    insert2(Fd, RootPos, NewNode, 0).

insert2(Fd, RootPos, {NewNodeMbr, NewNodeMeta, NewNodeId}, CallDepth) ->
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
    % XXX vmx: leaf node may transform into an inner node => change meta data
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
                % NOTE vmx (20100310): Is this TODO still valid?
                % XXX TODO vmx: Select right node and add new node
            end, Entries),
        MinPos = find_area_min_nth(Expanded),
        %{_, _, SubTreePos} = lists:nth(MinPos, Entries),
        SubTreePos = lists:nth(MinPos, EntriesPos),
        %case insert2(Fd, SubTreePos, NewNode) of
        case insert2(Fd, SubTreePos, NewNode, CallDepth+1) of
        {ok, ChildMbr, ChildPos} ->
            NewMbr = merge_mbr(TreeMbr, ChildMbr),
            {A, B} = lists:split(MinPos-1, EntriesPos),
            % NOTE vmx: I guess child nodes don't really have an order, we
            %     could remove the old node and append the new one at the
            %     end of the list.
            NewNode2 = {NewMbr, #node{type=inner}, A ++ [ChildPos] ++ tl(B)},
            {ok, Pos} = couch_file:append_term(Fd, NewNode2),
            {ok, NewMbr, Pos};
%        {splitted, _, _, _} ->
%            io:format("Matched!~n", []);
%        _ ->
%            io:format("Not matched at all!~n", [])
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
%    case Inserted of
%        % Root node needs to be split => new root node
%        {splitted, NewRootMbr, {SplittedNode1, SplittedNode2}} ->
%            %io:format("Creating new root node~n", []),
%            NewRoot = {NewRootMbr, #node{type=inner},
%                           [SplittedNode1, SplittedNode2]},
%            {ok, NewRootPos} = couch_file:append_term(Fd, NewRoot),
%            {ok, NewRootMbr, NewRootPos};
%        {ok, _, _} ->
%            Inserted
%    end.
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

%    {OpInfo, NewRootMbr, {SplittedNode1, SplittedNode2}} = Inserted,
%    io:format("OpInfo: ~p~n", [OpInfo]),
%    % Root node needs to be split => new root node
%    %if OpInfo == splitted and CallDepth == 0 ->
%    if CallDepth == 0 ->
%        %io:format("Creating new root node~n", []),
%        NewRoot = {NewRootMbr, #node{type=inner},
%                   [SplittedNode1, SplittedNode2]},
%        {ok, NewRootPos} = couch_file:append_term(Fd, NewRoot),
%        {ok, NewRootMbr, NewRootPos};
%    true ->
%        Inserted
%    end.
    %io:format("Inserted: ~p~n", [Inserted]),
    %Inserted.



insert(Fd, {Mbr, Id}, -1) ->
    InitialTree = {Mbr, [{Mbr, Id}]},
    {ok, Pos} = couch_file:append_term(Fd, InitialTree),
    {Pos, Mbr};
%    Filename = ?FILENAME,
%    case couch_file:open(Filename, [create, overwrite]) of
%    {ok, Fd} ->
%        InitialTree = {Mbr, [{Mbr, Id}]},
%        couch_file:append_term(Fd, InitialTree);
%        %InitialTree;
%    {error, Reason} ->
%        io:format("ERROR: Couldn't open file (~s) for tree storage~n",
%                  [Filename])
%    end;

insert(Fd, NewNode, RootPos) ->
    {ok, {TreeMbr, Entries}} = couch_file:pread_term(Fd, RootPos),
    io:format("Entries: ~p~n", [Entries]),
    NodeSize = length(Entries),
    IsLeafNode = is_leaf_node(Entries),
    %{_Mbr, ChildNodes} = Tree,
    if
        %?IS_LEAF_NODE(Tree) ->
        IsLeafNode ->
            NewLeafNodeMbr = merge_mbr(TreeMbr, element(1, NewNode)),
            NewLeafNode = {NewLeafNodeMbr, Entries ++ [NewNode]},
            NewLeafNode2 = if
                NodeSize < ?MAX_FILLED ->
                    io:format("New leaf node: ~p~n", [NewLeafNode]),
                    NewLeafNode;
                true ->
                    Partition = partition_leaf_node(NewLeafNode),
                    %io:format("(insert) Partition: ~p~n", [Partition]),
                    SplittedNodes = best_split(Partition),
                    {SplittedNodesMbr, [Node1, Node2]} = case SplittedNodes of
                        {tie, PartitionMbrs} ->
                            %io:format("(insert) PartitionMbrs: ~p~n", [PartitionMbrs]),
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
                    end,
                    {ok, Pos1} = couch_file:append_term(Fd, Node1),
                    {ok, Pos2} = couch_file:append_term(Fd, Node2),
                    {SplittedNodesMbr, [Pos1, Pos2]}
            end,
            io:format("NewLeafNode2: ~p~n", [NewLeafNode2]),
            {ok, Pos} = couch_file:append_term(Fd, NewLeafNode2),
            {NewLeafNode2Mbr, _} = NewLeafNode2,
            {Pos, NewLeafNode2Mbr};
        % "Tree" is an inner node
        true ->
            % Get entry where smallest MBR expansion is needed
            io:format("Inner Node!~n", []),
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
                    % NOTE vmx (20100310): Is this TODO still valid?
                    % XXX TODO vmx: Select right node and add new node
                end, Entries),
            MinPos = find_area_min_nth(Expanded),
            %io:format("Tree|Expanded|MinPos: ~p|~p|~p~n", [Tree, Expanded, MinPos]),
            SubTree = lists:nth(MinPos, Entries),
            %{_Area, NewMbr} = lists:nth(MinPos, Expanded),
            {NewInnerNodePos, NewInnerNodeMbr} = insert(Fd, NewNode, SubTree),
            io:format("insert returned: ~p~n", [NewInnerNodeMbr]),
            NewMbr = merge_mbr(TreeMbr, NewInnerNodeMbr),
            {A, B} = lists:split(MinPos-1, Entries),
            % XXX TODO vmx: If node is too
            NewNode2 = {NewMbr, A ++ [NewInnerNodePos] ++ tl(B)},
            {ok, Pos} = couch_file:append_term(Fd, NewNode2),
            {Pos, NewMbr}
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
    io:format("is_leaf_node: ~p~n", [H]),
    {Mbr, NodeId} = H,
    %{_Mbr1, ChildNodes} = H,
    %{_Mbr2, NodeId} = hd(ChildNodes),
    is_binary(NodeId).


% XXX vmx: Partition problem. If one node surrounds all others, they might be all put into the "else" case.
%(partition_leaf_node) Mbr: {47,218,580,947}
%(partition_leaf_node) Nodes: [{{47,218,580,947},<<"Node48">>},
%                              {{517,580,476,692},<<"Node152">>},
%                              {{424,749,532,922},<<"Node160">>},
%                              {{941,788,481,753},<<"Node196">>},
%                              {{551,827,544,754},<<"Node226">>}]
%(partition_leaf_node) Partitioned: {[],[],
%                                    [{{47,218,580,947},<<"Node48">>},
%                                     {{517,580,476,692},<<"Node152">>},
%                                     {{424,749,532,922},<<"Node160">>},
%                                     {{941,788,481,753},<<"Node196">>},
%                                     {{551,827,544,754},<<"Node226">>}],
%                                    [{{47,218,580,947},<<"Node48">>},
%                                     {{517,580,476,692},<<"Node152">>},
%                                     {{424,749,532,922},<<"Node160">>},
%                                     {{941,788,481,753},<<"Node196">>},
%                                     {{551,827,544,754},<<"Node226">>}]}
% Similar problem here:
% (partition_leaf_node) Mbr: {59,444,990,946}
%(partition_leaf_node) Nodes: [{{93,444,724,946},<<"Node1">>},
%                              {{210,698,160,559},<<"Node4">>},
%                              {{215,458,422,6},<<"Node5">>},
%                              {{563,476,401,310},<<"Node6">>},
%                              {{59,579,990,331},<<"Node7">>}]
%(partition_leaf_node) Partitioned: {[{{93,444,724,946},<<"Node1">>},
%                                     {{210,698,160,559},<<"Node4">>},
%                                     {{215,458,422,6},<<"Node5">>},
%                                     {{563,476,401,310},<<"Node6">>},
%                                     {{59,579,990,331},<<"Node7">>}],
%                                    [{{93,444,724,946},<<"Node1">>},
%                                     {{210,698,160,559},<<"Node4">>},
%                                     {{215,458,422,6},<<"Node5">>},
%                                     {{563,476,401,310},<<"Node6">>},
%                                     {{59,579,990,331},<<"Node7">>}],
%                                    [],[]}
% Partitions a leaf node into for nodes (one for every direction)
%partition_leaf_node({Mbr, Nodes}) ->
partition_leaf_node({Mbr, Meta, Nodes}) ->
    {MbrW, MbrS, MbrE, MbrN} = Mbr,
%    io:format("(partition_leaf_node) Mbr: ~p~n", [Mbr]),
%    io:format("(partition_leaf_node) Nodes: ~p~n", [Nodes]),
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
%    io:format("(partition_leaf_node) Partitioned: ~p~n", [Tmp]),
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
