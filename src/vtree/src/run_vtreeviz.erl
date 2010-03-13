-module(run_vtreeviz).
-export([run/0]).

-define(FILENAME, "/tmp/vtree_huge.bin").
-define(NODE_NUM, 5000).

-record(node, {
    % type = inner | leaf
    type=leaf}).

run() ->
    case couch_file:open(?FILENAME, [create, overwrite]) of
    {ok, Fd} ->
        TreePos = build_tree(Fd, ?NODE_NUM),
        %io:format("Tree: ~p~n", [TreePos]),
        vtreeviz:visualize(Fd, TreePos),
        ok;
    {error, Reason} ->
        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
                  [Reason, ?FILENAME])
    end.

build_tree(Fd, Size) ->
    Max = 1000,
    lists:foldl(fun(Count, CurTreePos) ->
        RandomMbr = {random:uniform(Max), random:uniform(Max),
                     random:uniform(Max), random:uniform(Max)},
        %io:format("~p~n", [RandomMbr]),
        {ok, _, NewRootPos} = vtree:insert2(
            Fd, CurTreePos,
            {RandomMbr, #node{type=leaf},
             list_to_binary("Node" ++ integer_to_list(Count))}),
        %io:format("test_insertion: ~p~n", [NewRootPos]),
        NewRootPos
    end, -1, lists:seq(1,Size)).
