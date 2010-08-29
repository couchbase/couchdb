#!/usr/bin/env escript

main(_) ->
    code:add_pathz("test"),
    code:add_pathz("ebin"),

    etap:plan(10),
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

    test_eval_ok(Ctx),
    test_call_ok(Ctx),

    test_eval_undefined(Ctx),
    test_call_undefined(Ctx),
    
    test_eval_error(Ctx),
    test_call_error(Ctx),
    ok.

test_eval_ok(Ctx) ->
    etap:is(
        emonk:eval(Ctx, <<"var x = 2; x*3;">>),
        {ok, 6},
        "Successful roundtrip through the JS vm."
    ).

test_call_ok(Ctx) ->
    etap:fun_is(
        fun({ok, undefined}) -> true; (_) -> false end,
        emonk:eval(Ctx, <<"var g = function(x) {return x*2};">>),
        "Created function ok."
    ),

    etap:is(
        emonk:call(Ctx, <<"g">>, [6]),
        {ok, 12},
        "Successful function call round trip with an argument string."
    ),
    
    etap:is(
        emonk:call(Ctx, <<"g">>, [600, foo]),
        {ok, 1200},
        "Successful call roundtrip with an argument list."
    ).

test_eval_undefined(Ctx) ->
    etap:is(
        emonk:eval(Ctx, <<"var x = function() {};">>),
        {ok, undefined},
        "Successfully ignored non-JSON response."
    ).

test_call_undefined(Ctx) ->
    etap:fun_is(
        fun({ok, undefined}) -> true; (_) -> false end,
        emonk:eval(Ctx, <<"var h = function(x) {return g};">>),
        "Created function ok."
    ),

    etap:is(
        emonk:call(Ctx, <<"h">>, []),
        {ok, undefined},
        "Successfully ignored non-JSON response."
    ).

test_eval_error(Ctx) ->
    etap:fun_is(
        fun({error, {_, _, _}}) -> true; (_E) -> throw(_E) end,
        emonk:eval(Ctx, <<"f * 3">>),
        "Reported the undefined error."
    ),
    
    etap:fun_is(
        fun({error, {_, _, _}}) -> true; (_) -> false end,
        emonk:eval(Ctx, <<"throw \"foo\";">>),
        "Reported the thrown exception."
    ).

test_call_error(Ctx) ->
    {ok, undefined} = emonk:eval(Ctx, <<"var k = function(x) {throw(2);};">>),
    etap:fun_is(
        fun({error, {_, _, _}}) -> true; (_E) -> false end,
        emonk:call(Ctx, <<"k">>, []),
        "Reported a thrown error."
    ).
