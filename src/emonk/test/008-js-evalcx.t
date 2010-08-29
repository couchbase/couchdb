#!/usr/bin/env escript

main(_) ->
    code:add_pathz("test"),
    code:add_pathz("ebin"),

    etap:plan(unknown),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail()
    end,
    ok.

test() ->
    {ok, Ctx} = emonk:create_ctx(),
    test_evalcx_exists(Ctx),
    test_run_in_sandbox(Ctx),
    ok.

test_evalcx_exists(Ctx) ->
    JS = <<"var f = (erlang.evalcx !== undefined); f;">>,
    etap:is(emonk:eval(Ctx, JS), {ok, true}, "erlang.send function exists.").

test_run_in_sandbox(Ctx) ->
    JS = <<
        "var sb = erlang.evalcx('');\n"
        "var fun = null;\n"
        "var mk = function(src) {fun = erlang.evalcx(src, sb); return true;}\n"
        "var get = function() {return fun.toSource();};\n"
        "var go = function(arg) {return fun(arg);}\n"
    >>,
    Fun = <<"function(arg) {return arg * 3;}">>,
    {ok, undefined} = emonk:eval(Ctx, JS),
    emonk:call(Ctx, <<"mk">>, [Fun]),
    etap:is(emonk:call(Ctx, <<"go">>, [3]), {ok, 9}, "Ran with sandbox"),
    Fun2 = <<"function(arg) {try{fun;return false;} catch(e) {return true;}}">>,
    emonk:call(Ctx, <<"mk">>, [Fun2]),
    etap:is(emonk:call(Ctx, <<"go">>, [0]), {ok, true}, "Sandbox worked.").

