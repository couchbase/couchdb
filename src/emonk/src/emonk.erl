
-module(emonk).
-on_load(init/0).


-export([create_ctx/0, create_ctx/1]).
-export([eval/2, eval/3, call/3, call/4, send/3, send/4]).


-define(APPNAME, emonk).
-define(LIBNAME, emonk).
-define(CTX_STACK, 8192).
-define(TIMEOUT, 5000).

create_ctx() ->
    create_ctx(?CTX_STACK).

create_ctx(_) ->
    not_loaded(?LINE).


eval(Ctx, Script) ->
    eval(Ctx, Script, ?TIMEOUT).

eval(Ctx, Script, Timeout) ->
    Ref = make_ref(),
    ok = eval(Ctx, Ref, self(), Script),
    receive
        {Ref, {message, Resp}} ->
            {message, Ref, Resp};
        {Ref, Resp} ->
            Resp
    after Timeout ->
        exit({error, timeout, Ref})
    end.

eval(_Ctx, _Ref, _Dest, _Script) ->
    not_loaded(?LINE).


call(Ctx, Name, Args) ->
    call(Ctx, Name, Args, ?TIMEOUT).

call(Ctx, Name, Args, Timeout) ->
    Ref = make_ref(),
    ok = call(Ctx, Ref, self(), Name, Args),
    receive
        {Ref, {message, Resp}} ->
            {message, Ref, Resp};
        {Ref, Resp} ->
            Resp
    after Timeout ->
        exit({error, timeout, Ref})
    end.

call(_Ctx, _Ref, _Dest, _Name, _Args) ->
    not_loaded(?LINE).

send(Ctx, Ref, Data) ->
    send(Ctx, Ref, Data, ?TIMEOUT).

send(Ctx, Ref, Data, Timeout) ->
    ok = send(Ctx, Data),
    receive
        {Ref, {message, Resp}} ->
            {message, Ref, Resp};
        {Ref, Resp} ->
            Resp
    after Timeout ->
        exit({error, timeout, Ref})
    end.

send(_Ctx, _Data) ->
    not_loaded(?LINE).

%% Internal API

init() ->
    SoName = "emonk",
    erlang:load_nif(SoName, 0).


not_loaded(Line) ->
    exit({not_loaded, [{module, ?MODULE}, {line, Line}]}).
