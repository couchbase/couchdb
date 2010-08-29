#! /usr/bin/env escript

test_vm_creation() ->
    etap:fun_is(
        fun({ok, _}) -> true; (_) -> false end,
        emonk:create_ctx(),
        "Created a context successfully."
    ),
    erlang:garbage_collect().

main([]) ->
    code:add_pathz("test"),
    code:add_pathz("ebin"),

    etap:plan(101),

    test_vm_creation(),

    lists:foreach(fun(_) -> test_vm_creation() end, lists:seq(1, 100, 1)),
    erlang:garbage_collect(),
    
    etap:end_tests().

