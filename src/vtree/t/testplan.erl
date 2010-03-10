-module(testplan).
-export([start/0]).

% XXX TODO vmx: update lookup tests to new tree structure ( {} instead of []
%     at the root)

start() ->
%    test_within(),
%    test_intersect(),
%    test_disjoint(),
    test_lookup(),
%    test_insert(),
%    test_area(),
%    test_merge_mbr(),
%    test_find_min_nth(),
%    test_find_min_nth2(),
%    test_find_min_nth3(),
%    test_find_area_min_nth(),
%    test_is_leaf_node(),
%    test_partition_leaf_node(),
%    test_calc_mbr(),
%    test_calc_nodes_mbr(),
%    test_best_split(),
%    test_minimal_overlap(),
%    test_minimal_coverage(),
%    test_calc_overlap(),
%    %profile_find_min_nth(),
%
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
    Bbox4 = {-22, -33, 20, -15},
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, <<"Node4">>},
    Mbr1_2 = {-18,-3,13,15},
    Mbr1_2_3_4 = {-21,-32,19,15},
    Mbr3_4 = {-21,-32,19,14},
    EmptyTree = {},
    Tree1 = {Mbr1_2, [Node1, Node2]},
    Tree2 = {Mbr1_2_3_4, [{Mbr1_2, [Node1, Node2]}, {Mbr3_4, [Node3, Node4]}]},
    etap:is(vtree:lookup(Bbox1, EmptyTree), {}, "Lookup in empty tree"),
    etap:is(vtree:lookup(Bbox1, Tree1), [Node1, Node2],
            "Find all nodes in tree (tree height=1)"),
    etap:is(vtree:lookup(Bbox2, Tree1), [Node2],
            "Find some nodes in tree (tree height=1)"),
    etap:is(vtree:lookup(Bbox3, Tree1), [],
            "Query window outside of all nodes (tree height=1)"),
    etap:is(vtree:lookup(Bbox2, Tree2), [Node2],
            "Find some nodes in tree (tree height=2) (a)"),
    etap:is(vtree:lookup(Bbox4, Tree2), [Node4],
            "Find some nodes in tree (tree height=2) (b)"),
    etap:is(vtree:lookup(Bbox3, Tree2), [],
            "Query window outside of all nodes (tree height=2)"),
    ok.

test_insert() ->
    etap:plan(9),
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, <<"Node4">>},
    Node5 = {{-5,-16,4,19}, <<"Node5">>},
    Node6 = {{-25,-26,-14,-19}, <<"Node6">>},
    Node7 = {{-30,-12,4,-1}, <<"Node7">>},
    Node8 = {{-3,-2,-1,-1}, <<"Node8">>},
    Node9 = {{0,15,1,18}, <<"Node9">>},
    Mbr1 = {10,5,13,15},
    Mbr1_2 = Mbr1_2_8 = {-18,-3,13,15},
    Mbr1_2_3 = {-21,-3,13,15},
    Mbr1_2_3_4 = {-21,-32,19,15},
    Mbr1_2_3_4_5 = {-21,-32,19,19},
    Mbr1_2_3_4_5_6 = Mbr1_2_3_4_5_6_8 = {-25,-32,19,19},
    Mbr1_2_3_4_5_7 = Mbr1_2_3_4_5_7_9 = {-30,-32,19,19},
    Mbr1_2_3_4_5_6_7 = {-30,-32,19,19},
    Mbr1_4_5 = {-5,-32,19,19},
    Mbr2_3 = {-21,-3,-10,14},
    Mbr3_4 = {-21,-32,19,14},
    Mbr3_4_5 = {-21,-32,19,19},
    Mbr3_4_5_6 = {-25,-32,19,19},
    Mbr3_4_5_7 = Mbr3_4_5_7_9 = {-30,-32,19,19},
    Mbr3_4_5_6_7 = {-30,-32,19,19},
    Mbr3_5_7 = {-30,-16,4,19},
    Mbr4_5_9 = {-5,-32,19,19},
    Mbr4_6 = {-25,-32,19,-19},
    Mbr3_7 = {-30,-12,4,14},
    EmptyTree = {},
    Tree1Node1 = {Mbr1, [Node1]},
    Tree1Node1_2 = {Mbr1_2, [Node1, Node2]},
    Tree1Node1_2_3 = {Mbr1_2_3, [Node1, Node2, Node3]},
    Tree1Node1_2_3_4 = {Mbr1_2_3_4, [Node1, Node2, Node3, Node4]},
    Tree1Node1_2_3_4_5 = {Mbr1_2_3_4_5, [{Mbr2_3, [Node2, Node3]},
                                         {Mbr1_4_5, [Node1, Node4, Node5]}]},
    Tree2 = {Mbr1_2_3_4, [{Mbr1_2, [Node1, Node2]}, {Mbr3_4, [Node3, Node4]}]},
    Tree2Node5 = {Mbr1_2_3_4_5, [{Mbr1_2, [Node1, Node2]},
                                 {Mbr3_4_5, [Node3, Node4, Node5]}
                                ]},
    Tree2Node6 = {Mbr1_2_3_4_5_6, [{Mbr1_2, [Node1, Node2]},
                                   {Mbr3_4_5_6, [Node3, Node4, Node5, Node6]}
                                  ]},
    Tree2Node7b = {Mbr1_2_3_4_5_7, [{Mbr1_2, [Node1, Node2]},
                                   {Mbr3_4_5_7, [Node3, Node4, Node5, Node7]}
                                  ]},
    % XXX vmx: Not yet right
    Tree2Node7 = {Mbr1_2_3_4_5_6_7, [{Mbr1_2, [Node1, Node2]},
                                     {Mbr3_4_5_6_7, [{Mbr4_6, [Node4, Node6]},
                                                     {Mbr3_5_7, [Node3, Node5, Node7]}]}
                                    ]},
    Tree2Node8 = {Mbr1_2_3_4_5_6_8, [{Mbr1_2_8, [Node1, Node2, Node8]},
                                     {Mbr3_4_5_6, [Node3, Node4, Node5, Node6]}
                                    ]},
    Tree2Node9 = {Mbr1_2_3_4_5_7_9, [{Mbr1_2, [Node1, Node2]},
                                     {Mbr3_4_5_7_9,
                                      [{Mbr3_7, [Node3, Node7]},
                                       {Mbr4_5_9, [Node4, Node5, Node9]}
                                      ]}]},
    etap:is(vtree:insert(Node1, EmptyTree), Tree1Node1,
            "Insert a node into an empty tree"),
    etap:is(vtree:insert(Node2, Tree1Node1), Tree1Node1_2, 
            "Insert a node into a not yet full leaf node (root node)"),
    etap:is(vtree:insert(Node4, Tree1Node1_2_3), Tree1Node1_2_3_4,
            "Insert a nodes into a then to be full leaf node (root node)"),
    etap:is(vtree:insert(Node5, Tree2), Tree2Node5,
            "Insert a nodes into a not yet full leaf node (depth=2) (a)"),
    etap:is(vtree:insert(Node8, Tree2Node6), Tree2Node8,
            "Insert a nodes into a not yet full leaf node (depth=2) (b)"),
    etap:is(vtree:insert(Node6, Tree2Node5), Tree2Node6,
            "Insert a nodes into then to be full leaf node (depth=2)"),
    etap:is(vtree:insert(Node5, Tree1Node1_2_3_4), Tree1Node1_2_3_4_5,
            "Insert a nodes into a full leaf node (root node)"),
    etap:is(vtree:insert(Node7, Tree2Node6), Tree2Node7,
            "Insert a nodes into a full leaf node (depth=2)" ++
            "(tie on best_split occurs)"),
    etap:is(vtree:insert(Node9, Tree2Node7b), Tree2Node9,
            "Insert a nodes into a full leaf node (depth=2)" ++
            " (split in WE direction)"),
    ok.

test_area() ->
    etap:plan(5),
    Mbr1 = {10,5,13,15},
    Mbr2 = {-18,-3,-10,-1},
    Mbr3 = {-21,2,-10,14},
    Mbr4 = {5,-32,19,-25},
    Mbr5 = {-5,-16,4,19},
    etap:is(vtree:area(Mbr1), 30, "Area of MBR in the NE"),
    etap:is(vtree:area(Mbr2), 16, "Area of MBR in the SW"),
    etap:is(vtree:area(Mbr3), 132, "Area of MBR in the NW"),
    etap:is(vtree:area(Mbr4), 98, "Area of MBR in the SE"),
    etap:is(vtree:area(Mbr5), 315, "Area of MBR covering all quadrants"),
    ok.

test_merge_mbr() ->
    etap:plan(7),
    Mbr1 = {10,5,13,15},
    Mbr2 = {-18,-3,-10,-1},
    Mbr3 = {-21,2,-10,14},
    Mbr4 = {5,-32,19,-25},
    etap:is(vtree:merge_mbr(Mbr1, Mbr2), {-18, -3, 13, 15},
            "Merge MBR of MBRs in NE and SW"),
    etap:is(vtree:merge_mbr(Mbr1, Mbr3), {-21, 2, 13, 15},
            "Merge MBR of MBRs in NE and NW"),
    etap:is(vtree:merge_mbr(Mbr1, Mbr4), {5, -32, 19, 15},
            "Merge MBR of MBRs in NE and SE"),
    etap:is(vtree:merge_mbr(Mbr2, Mbr3), {-21, -3, -10, 14},
            "Merge MBR of MBRs in SW and NW"),
    etap:is(vtree:merge_mbr(Mbr2, Mbr4), {-18, -32, 19, -1},
            "Merge MBR of MBRs in SW and SE"),
    etap:is(vtree:merge_mbr(Mbr3, Mbr4), {-21, -32, 19, 14},
            "Merge MBR of MBRs in NW and SE"),
    etap:is(vtree:merge_mbr(Mbr1, Mbr1), Mbr1,
            "Merge MBR of equal MBRs"),
    ok.

test_find_min_nth() ->
    etap:plan(5),
    etap:is(vtree:find_min_nth([5]), 1,
            "Find position of minimum in a list with one element"),
    etap:is(vtree:find_min_nth([538, 29]), 2,
            "Find position of minimum in a list with two elements (1>2)"),
    etap:is(vtree:find_min_nth([54, 538]), 1,
            "Find position of minimum in a list with two elements (1<2)"),
    etap:is(vtree:find_min_nth([54, 54]), 1,
            "Find position of minimum in a list with two equal elements"),
    etap:is(vtree:find_min_nth([329, 930, 203, 72, 402, 2904, 283]), 4,
            "Find position of minimum in a list"),
    ok.


test_find_min_nth2() ->
    etap:plan(5),
    etap:is(vtree:find_min_nth2([5]), 1,
            "Find position of minimum in a list with one element"),
    etap:is(vtree:find_min_nth2([538, 29]), 2,
            "Find position of minimum in a list with two elements (1>2)"),
    etap:is(vtree:find_min_nth2([54, 538]), 1,
            "Find position of minimum in a list with two elements (1<2)"),
    etap:is(vtree:find_min_nth2([54, 54]), 1,
            "Find position of minimum in a list with two equal elements"),
    etap:is(vtree:find_min_nth2([329, 930, 203, 72, 402, 2904, 283]), 4,
            "Find position of minimum in a list"),
    ok.

test_find_min_nth3() ->
    etap:plan(5),
    etap:is(vtree:find_min_nth3([5]), 1,
            "Find position of minimum in a list with one element"),
    etap:is(vtree:find_min_nth3([538, 29]), 2,
            "Find position of minimum in a list with two elements (1>2)"),
    etap:is(vtree:find_min_nth3([54, 538]), 1,
            "Find position of minimum in a list with two elements (1<2)"),
    etap:is(vtree:find_min_nth3([54, 54]), 1,
            "Find position of minimum in a list with two equal elements"),
    etap:is(vtree:find_min_nth3([329, 930, 203, 72, 402, 2904, 283]), 4,
            "Find position of minimum in a list"),
    ok.


test_find_area_min_nth() ->
    etap:plan(5),
    etap:is(vtree:find_min_nth3([{5, {23,64,24,79}}]), 1,
            "Find position of minimum area in a list with one element"),
    etap:is(vtree:find_min_nth3([{538, {2,64,4,79}}, {29, {2,64,4,79}}]), 2,
            "Find position of minimum area in a list with two elements (1>2)"),
    etap:is(vtree:find_min_nth3([{54, {2,64,4,79}}, {538, {2,64,4,79}}]), 1,
            "Find position of minimum area in a list with two elements (1<2)"),
    etap:is(vtree:find_min_nth3([{54, {2,64,4,79}}, {54, {2,64,4,79}}]), 1,
            "Find position of minimum area in a list with two equal elements"),
    etap:is(vtree:find_min_nth3(
              [{329, {2,64,4,79}}, {930, {2,64,4,79}}, {203, {2,64,4,79}},
               {72, {2,64,4,79}}, {402, {2,64,4,79}}, {2904, {2,64,4,79}},
               {283, {2,64,4,79}}]), 4,
            "Find position of minimum area in a list"),
    ok.

test_is_leaf_node() ->
    etap:plan(2),
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    Mbr1_2 = {-18,-3,13,15},
    Tree1 = [Node1, Node2],
    Tree2 = [{Mbr1_2, [Node1, Node2]}],
    etap:is(vtree:is_leaf_node(Tree1), true, "Node is a leaf node"),
    etap:is(vtree:is_leaf_node(Tree2), false, "Node is not a leaf node"),
    ok.


test_partition_leaf_node() ->
    etap:plan(3),
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, <<"Node4">>},
    Node5 = {{-5,-16,4,19}, <<"Node5">>},
    LeafChildren3 = [Node1, Node2, Node4],
    LeafChildren4 = LeafChildren3 ++ [Node3],
    LeafChildren5 = LeafChildren4 ++ [Node5],
    Mbr1_2_4 = {-18,-25,19,15},
    Mbr1_2_3_4_5 = {-21,-25,19,19},
    etap:is(vtree:partition_leaf_node({Mbr1_2_4, LeafChildren3}),
            {[Node2], [Node4], [Node1, Node4], [Node1, Node2]},
            "Partition 3 nodes"),
    etap:is(vtree:partition_leaf_node({Mbr1_2_3_4_5, LeafChildren4}),
            {[Node2, Node3], [Node4],
             [Node1, Node4], [Node1, Node2, Node3]},
            "Partition 4 nodes"),
    etap:is(vtree:partition_leaf_node({Mbr1_2_3_4_5, LeafChildren5}),
            {[Node2, Node3], [Node4],
             [Node1, Node4, Node5], [Node1, Node2, Node3, Node5]},
            "Partition 5 nodes"),
    ok.


test_calc_mbr() ->
    etap:plan(9),
    Mbr1 = {10,5,13,15},
    Mbr2 = {-18,-3,-10,-1},
    Mbr3 = {-21,2,-10,14},
    Mbr4 = {5,-32,19,-25},
    etap:is(vtree:calc_mbr([]), error,
            "Calculate MBR of an empty list"),
    etap:is(vtree:calc_mbr([Mbr1]), {10, 5, 13, 15},
            "Calculate MBR of a single MBR"),
    etap:is(vtree:calc_mbr([Mbr1, Mbr2]), {-18, -3, 13, 15},
            "Calculate MBR of MBRs in NE and SW"),
    etap:is(vtree:calc_mbr([Mbr1, Mbr3]), {-21, 2, 13, 15},
            "Calculate MBR of MBRs in NE and NW"),
    etap:is(vtree:calc_mbr([Mbr1, Mbr4]), {5, -32, 19, 15},
            "Calculate MBR of MBRs in NE and SE"),
    etap:is(vtree:calc_mbr([Mbr2, Mbr3]), {-21, -3, -10, 14},
            "Calculate MBR of MBRs in SW and NW"),
    etap:is(vtree:calc_mbr([Mbr2, Mbr4]), {-18, -32, 19, -1},
            "Calculate MBR of MBRs in SW and SE"),
    etap:is(vtree:calc_mbr([Mbr3, Mbr4]), {-21, -32, 19, 14},
            "Calculate MBR of MBRs in NW and SE"),
    etap:is(vtree:calc_mbr([Mbr1, Mbr2, Mbr3]), {-21, -3, 13, 15},
            "Calculate MBR of MBRs in NE, SW, NW"),
    etap:is(vtree:calc_mbr([Mbr1, Mbr2, Mbr4]), {-18, -32, 19, 15},
            "Calculate MBR of MBRs in NE, SW, SE"),
    ok.

test_calc_nodes_mbr() ->
    etap:plan(9),
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, <<"Node4">>},
    etap:is(vtree:calc_nodes_mbr([Node1]), {10, 5, 13, 15},
            "Calculate MBR of a single nodes"),
    etap:is(vtree:calc_nodes_mbr([Node1, Node2]), {-18, -3, 13, 15},
            "Calculate MBR of nodes in NE and SW"),
    etap:is(vtree:calc_nodes_mbr([Node1, Node3]), {-21, 2, 13, 15},
            "Calculate MBR of nodes in NE and NW"),
    etap:is(vtree:calc_nodes_mbr([Node1, Node4]), {5, -32, 19, 15},
            "Calculate MBR of nodes in NE and SE"),
    etap:is(vtree:calc_nodes_mbr([Node2, Node3]), {-21, -3, -10, 14},
            "Calculate MBR of nodes in SW and NW"),
    etap:is(vtree:calc_nodes_mbr([Node2, Node4]), {-18, -32, 19, -1},
            "Calculate MBR of nodes in SW and SE"),
    etap:is(vtree:calc_nodes_mbr([Node3, Node4]), {-21, -32, 19, 14},
            "Calculate MBR of nodes in NW and SE"),
    etap:is(vtree:calc_nodes_mbr([Node1, Node2, Node3]), {-21, -3, 13, 15},
            "Calculate MBR of nodes in NE, SW, NW"),
    etap:is(vtree:calc_nodes_mbr([Node1, Node2, Node4]), {-18, -32, 19, 15},
            "Calculate MBR of nodes in NE, SW, SE"),
    ok.

test_best_split() ->
    etap:plan(4),
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, <<"Node4">>},
    Node5 = {{-5,-16,4,19}, <<"Node5">>},
    Node6 = {{-15,10,-12,17}, <<"Node6">>},
    {Mbr2, _} = Node2,
    {Mbr4, _} = Node4,
    Mbr1_2 = {-18,-3,13,15},
    Mbr1_4 = {5,-32,19,15},
    Mbr1_4_5 = {-5,-32,19,19},
    Mbr2_3 = {-21,-3,-10,14},
    Mbr4_6 = {-15,-32,19,17},
    Partition3 = {[Node2], [Node4], [Node1, Node4], [Node1, Node2]},
    Partition4 = {[Node2, Node3], [Node4],
                  [Node1, Node4], [Node1, Node2, Node3]},
    Partition5 = {[Node2, Node3], [Node4],
                  [Node1, Node4, Node5], [Node1, Node2, Node3, Node5]},
    Partition4b = {[Node2], [Node4, Node6],
                   [Node1, Node4, Node6], [Node1, Node2]},
    etap:is(vtree:best_split(Partition3), {tie, {Mbr2, Mbr4, Mbr1_4, Mbr1_2}},
            "Best split: tie (3 nodes)"),
    etap:is(vtree:best_split(Partition4), [{Mbr2_3, [Node2, Node3]},
                                           {Mbr1_4, [Node1, Node4]}],
            "Best split: horizontal (W/E) nodes win (4 nodes)"),
    etap:is(vtree:best_split(Partition5), [{Mbr2_3, [Node2, Node3]},
                                           {Mbr1_4_5, [Node1, Node4, Node5]}],
            "Best split: horizontal (W/E) nodes win (5 nodes)"),
    etap:is(vtree:best_split(Partition4b), [{Mbr4_6, [Node4, Node6]},
                                            {Mbr1_2, [Node1, Node2]}],
            "Best split: vertical (S/N) nodes win (4 nodes)"),
    ok.

test_minimal_overlap() ->
    % XXX vmx: test fir S/N split is missing
    etap:plan(2),
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, <<"Node4">>},
    Node5 = {{-11,-9,12,10}, <<"Node5">>},
    {Mbr2, _} = Node2,
    {Mbr4, _} = Node4,
    Mbr1_2 = {-18,-3,13,15},
    Mbr1_3 = {-21,2,13,15},
    Mbr1_4 = {5,-32,19,15},
    Mbr1_4_5 = {-11,-32,19,15},
    Mbr2_3 = {-21,-3,-10,14},
    Mbr2_4_5 = {-18,-32,19,10},
    Partition3 = {[Node2], [Node4], [Node1, Node4], [Node1, Node2]},
    Partition5 = {[Node2, Node3], [Node2, Node4, Node5],
                  [Node1, Node4, Node5], [Node1, Node3]},
    etap:is(vtree:minimal_overlap(
                Partition5, {Mbr2_3, Mbr2_4_5, Mbr1_4_5, Mbr1_3}),
            [{Mbr2_3, [Node2, Node3]}, {Mbr1_4_5, [Node1, Node4, Node5]}],
            "Minimal Overlap: horizontal (W/E) nodes win (5 Nodes)"),
    etap:is(vtree:minimal_overlap(Partition3, {Mbr2, Mbr4, Mbr1_4, Mbr1_2}),
            tie, "Minimal Overlap: tie"),
    ok.


test_minimal_coverage() ->
    % XXX vmx: test for equal coverage is missing
    etap:plan(2),
    Node1 = {{10,5,13,15}, <<"Node1">>},
    Node2 = {{-18,-3,-10,-1}, <<"Node2">>},
    Node3 = {{-21,2,-10,14}, <<"Node3">>},
    Node4 = {{5,-32,19,-25}, <<"Node4">>},
    Node5 = {{-11,-9,12,10}, <<"Node5">>},
    Node6 = {{-11,-9,12,24}, <<"Node6">>},
    {Mbr4, _} = Node4,
    Mbr1_3 = {-21,2,13,15},
    Mbr1_2_3_6 = {-21,-9,13,24},
    Mbr1_4_5 = {-11,-32,19,15},
    Mbr1_4_6 = {-11,-32,19,24},
    Mbr2_3 = {-21,-3,-10,14},
    Mbr2_4_5 = {-18,-32,19,10},
    Mbr3_7 = {-21,2,-10,14}, %132
    Mbr3_8 = {-21,2,16,14}, %408
    Partition5 = {[Node2, Node3], [Node2, Node4, Node5],
                  [Node1, Node4, Node5], [Node1, Node3]},
    Partition6 = {[Node2, Node3], [Node4],
                  [Node1, Node4, Node6], [Node1, Node2, Node3, Node6]},
    etap:is(vtree:minimal_coverage(
                Partition5, {Mbr2_3, Mbr2_4_5, Mbr1_4_5, Mbr1_3}),
            [{Mbr2_3, [Node2, Node3]}, {Mbr1_4_5, [Node1, Node4, Node5]}],
            "Minimal Overlap: horizontal (W/E) nodes win)"),
    etap:is(vtree:minimal_coverage(
                Partition6, {Mbr2_3, Mbr4, Mbr1_4_6, Mbr1_2_3_6}),
            [{Mbr4, [Node4]}, {Mbr1_2_3_6, [Node1, Node2, Node3, Node6]}],
            "Minimal Overlap: vertical (S/N) nodes win"),
    ok.


test_calc_overlap() ->
    etap:plan(7),
    Mbr1 = {10,5,13,15},
    Mbr2 = {-18,-3,-10,-1},
    Mbr3 = {-21,2,-10,14},
    Mbr4 = {5,-32,19,-25},
    Mbr5 = {-11,-9,12,10},
    Mbr6 = {-5,-6,4,9},
    Mbr7 = {4,-11,20,-3},
    etap:is(vtree:calc_overlap(Mbr1, Mbr5), {10, 5, 12, 10},
            "Calculate overlap of MBRs in NE and center"),
    etap:is(vtree:calc_overlap(Mbr2, Mbr5), {-11, -3, -10, -1},
            "Calculate overlap of MBRs in SW and center"),
    etap:is(vtree:calc_overlap(Mbr3, Mbr5), {-11, 2, -10, 10},
            "Calculate overlap of MBRs in NW and center"),
    etap:is(vtree:calc_overlap(Mbr7, Mbr5), {4, -9, 12, -3},
            "Calculate overlap of MBRs in SE and center"),
    etap:is(vtree:calc_overlap(Mbr6, Mbr5), {-5, -6, 4, 9},
            "Calculate overlap of one MBRs enclosing the other (1)"),
    etap:is(vtree:calc_overlap(Mbr5, Mbr6), {-5, -6, 4, 9},
            "Calculate overlap of one MBRs enclosing the other (2)"),
    etap:is(vtree:calc_overlap(Mbr4, Mbr5), {0, 0, 0, 0},
            "Calculate overlap of MBRs with no overlap"),
    ok.


    
profile_find_min_nth() ->
    TestList = [329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375, 329, 930, 203, 72, 402, 2904, 283, 382, 230, 34, 450, 45, 30, 3204, 30, 30, 30, 340, 23049, 305, 204, 304, 2304, 324,03,45,9,83,2498,5984,7589,47,59,4,375 ],

    io:format("NTH:~n", []),
    test_avg(vtree, find_min_nth, [TestList], 100000),
    io:format("NTH2:~n", []),
    test_avg(vtree, find_min_nth2, [TestList], 100000),
    io:format("NTH3:~n", []),
    test_avg(vtree, find_min_nth3, [TestList], 100000),
    ok.


% From http://www.trapexit.org/Measuring_Function_Execution_Time (2010-01-17)
test_avg(M, F, A, N) when N > 0 ->
    L = test_loop(M, F, A, N, []),
    Length = length(L),
    Min = lists:min(L),
    Max = lists:max(L),
    Med = lists:nth(round((Length / 2)), lists:sort(L)),
    Avg = round(lists:foldl(fun(X, Sum) -> X + Sum end, 0, L) / Length),
    io:format("Range: ~b - ~b mics~n"
	      "Median: ~b mics~n"
	      "Average: ~b mics~n",
	      [Min, Max, Med, Avg]),
    Med.

test_loop(_M, _F, _A, 0, List) ->
    List;
test_loop(M, F, A, N, List) ->
    {T, _Result} = timer:tc(M, F, A),
    test_loop(M, F, A, N - 1, [T|List]).

