-module(testplan).
-export([start/0]).

start() ->
%    etap:ok(true, "the 'true' atom is recognized"),
%    etap:is(1 + 1, 2, "simple math"),
%    etap:isnt(2 + 2, 5, "some would argue"),
    %test_within(),
    %test_intersect(),
    %test_disjoint(),
    test_lookup(),
    test_insert(),

    etap:end_tests().

test_within() ->
    etap:plan(4),
    Bbox1 = {-20, -10, 30, 21},
    Bbox2 = {-20, -10, 0, 0},
    Mbr1_2 = {-18,-3,13,15},
    Node1 = {{10,5,13,15}, <<"Node1">>},
    {Node1Mbr, _} = Node1,
    etap:is(vtree:within(Node1Mbr, Bbox1), true, "MBR is within the BBox"),
    etap:is(vtree:within(Node1Mbr, Node1Mbr), true, "MBR is within itself"),
    etap:is(vtree:within(Node1Mbr, Bbox2), false,
            "MBR is not at all within BBox"),
    etap:is(vtree:within(Mbr1_2, Bbox2), false, "MBR intersects BBox"),
    ok.

test_intersect() ->
    etap:plan(17),
    Mbr1_2 = {-18,-3,13,15},
    etap:is(vtree:intersect(Mbr1_2, {-20, -11, 0, 0}), true,
            "MBR intersectton (S and W edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-21, 4, -2, 11}), true,
            "MBR intersecttion (W edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-21, 4, -2, 17}), true,
            "MBR intersection (W and N edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-13, 4, -2, 17}), true,
            "MBR intersection (N edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-13, 4, 16, 17}), true,
            "MBR intersection (N and E edge)"),
    etap:is(vtree:intersect(Mbr1_2, {5, -1, 16, 10}), true,
            "MBR intersection (E edge)"),
    etap:is(vtree:intersect(Mbr1_2, {5, -9, 16, 10}), true,
            "MBR intersection (E and S edge)"),
    etap:is(vtree:intersect(Mbr1_2, {5, -9, 11, 10}), true,
            "MBR intersection (S edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-27, -9, 11, 10}), true,
            "MBR intersection (S and W edge)"),
    etap:is(vtree:intersect(Mbr1_2, {-27, -9, 18, 10}), true,
            "MBR intersection (W and E edge (bottom))"),
    etap:is(vtree:intersect(Mbr1_2, {-27, 2, 18, 10}), true,
            "MBR intersection (W and E edge (middle))"),
    etap:is(vtree:intersect(Mbr1_2, {-10, -9, 18, 10}), true,
            "MBR intersection (W and E edge (top))"),
    etap:is(vtree:intersect(Mbr1_2, {-25, -4, 2, 12}), true, 
            "MBR intersection (N and S edge (left))"),
    etap:is(vtree:intersect(Mbr1_2, {-15, -4, 2, 12}), true,
            "MBR intersection (N and S edge (middle))"),
    etap:is(vtree:intersect(Mbr1_2, {-15, -4, 2, 22}), true,
            "MBR intersection (N and S edge (right))"),
    etap:is(vtree:intersect(Mbr1_2, {-14, -1, 10, 5}), false,
            "One MBR within the other"),
    etap:is(vtree:intersect(Mbr1_2, Mbr1_2), true,
            "MBR is within itself"),
    ok.

test_disjoint() ->
    etap:plan(2),
    Mbr1_2 = {-18,-3,13,15},
    etap:is(vtree:disjoint(Mbr1_2, {27, 20, 38, 40}), true,
            "MBRs are disjoint"),
    etap:is(vtree:disjoint(Mbr1_2, {-27, 2, 18, 10}), false,
            "MBRs are not disjoint").
    

test_lookup() ->
    etap:plan(6),
    Bbox1 = {-20, -10, 30, 21},
    Bbox2 = {-20, -10, 0, 0},
    Bbox3 = {100, 200, 300, 400},
    % {MBR, ID}
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    Mbr1_2 = {-18,-3,13,15},
    EmptyTree = [],
    %Tree1 = [[Mbr1_2, {Node1, Node2, nil, nil}], nil, nil, nil],
    Tree1 = [Node1, Node2, nil, nil],
    %Tree2 = [[Mbr1_2, {Node1, Node2, nil, nil}], [{test}, nil, nil], nil, nil],
    %Tree2 = [[Mbr1_2, [Node1, Node2, nil, nil]], [{test}, nil, nil], nil, nil],
    Tree2 = [{Mbr1_2, [Node1, Node2, nil, nil]}, nil, nil, nil],
    etap:is(vtree:lookup(Bbox1, EmptyTree), [], "Lookup in empty tree"),
    etap:is(vtree:lookup(Bbox1, Tree1), [Node1, Node2],
            "Find all nodes in tree (tree height=1)"),
    etap:is(vtree:lookup(Bbox2, Tree1), [Node2],
            "Find some nodes in tree (tree height=1)"),
    etap:is(vtree:lookup(Bbox3, Tree1), [],
            "Query window outside of all nodes (tree height=1)"),
    etap:is(vtree:lookup(Bbox2, Tree2), [Node2],
            "Find some nodes in tree (tree height=2)"),
    etap:is(vtree:lookup(Bbox3, Tree2), [],
            "Query window outside of all nodes (tree height=2)"),
    ok.

test_insert() ->
    etap:plan(1),
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    EmptyTree = [nil, nil, nil, nil],
    etap:is(vtree:insert(Node1, EmptyTree), [Node1, nil, nil, nil],
            "Insert a node into an empty tree"),
    ok.
