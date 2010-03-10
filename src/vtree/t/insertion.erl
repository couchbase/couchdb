-module(insertion).
-export([start/0]).

start() ->
    test_insertion(),
    etap:end_tests().

test_insertion() ->
    etap:plan(1),
    Max = 1000,
    Tree = lists:foldl(
        fun(Count, CurTree) ->
            RandomMbr = {random:uniform(Max), random:uniform(Max),
                         random:uniform(Max), random:uniform(Max)},
            io:format("~p~n", [RandomMbr]),
            vtree:insert({RandomMbr,
                          list_to_binary("Node" ++ integer_to_list(Count))},
                         CurTree)
        end, {}, lists:seq(1,10000)),
    io:format("Tree: ~p~n", [Tree]),
    ok.
