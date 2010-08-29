#! /usr/bin/env escript

main([]) ->
    code:add_pathz("test"),
    code:add_pathz("ebin"),

    Modules = [
        emonk
    ],

    etap:plan(length(Modules)),
    lists:foreach(fun(Mod) ->
        Mesg = atom_to_list(Mod) ++ " module loaded",
        etap:loaded_ok(Mod, Mesg)
    end, Modules),
    etap:end_tests().

