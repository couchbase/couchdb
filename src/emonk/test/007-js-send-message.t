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
    test_send_exists(Ctx),
    test_send_message(Ctx),
    ok.

test_send_exists(Ctx) ->
    JS = <<"var f = (erlang.send !== undefined); f;">>,
    etap:is(emonk:eval(Ctx, JS), {ok, true}, "erlang.send function exists.").

test_send_message(Ctx) ->
    JS = <<"var f = erlang.send(1.3) == 2.6; f;">>,
    {message, Token, Data} = emonk:eval(Ctx, JS),
    etap:is(emonk:send(Ctx, Token, Data * 2), {ok, true}, "message passed ok").