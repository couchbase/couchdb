%% @author Bob Ippolito <bob@mochimedia.com>
%% @copyright 2007 Mochi Media, Inc.

%% @doc MochiWeb socket server.

-module(mochiweb_socket_server).
-author('bob@mochimedia.com').
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("internal.hrl").

-export([start/1, start_link/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2, code_change/3,
         handle_info/2]).
-export([get/2, set/3]).

-record(mochiweb_socket_server,
        {port,
         loop,
         name=undefined,
         max=2048,
         ip=any,
         listen=null,
         nodelay=false,
         recbuf=?RECBUF_SIZE,
         buffer=undefined,
         backlog=128,
         active_sockets=0,
         acceptor_pool_size=16,
         ssl=false,
         ssl_opts=[{ssl_imp, new}],
         acceptor_pool=sets:new(),
         profile_fun=undefined,
         tls_alert_handler=undefined,
         ssl_handshake_opts_fun=undefined}).

-define(is_old_state(State), not is_record(State, mochiweb_socket_server)).

start_link(Options) ->
    start_server(start_link, parse_options(Options)).

start(Options) ->
    case lists:keytake(link, 1, Options) of
        {value, {_Key, false}, Options1} ->
            start_server(start, parse_options(Options1));
        _ ->
            %% TODO: https://github.com/mochi/mochiweb/issues/58
            %%   [X] Phase 1: Add new APIs (Sep 2011)
            %%   [_] Phase 2: Add deprecation warning
            %%   [_] Phase 3: Change default to {link, false} and ignore link
            %%   [_] Phase 4: Add deprecation warning for {link, _} option
            %%   [_] Phase 5: Remove support for {link, _} option
            start_link(Options)
    end.

get(Name, Property) ->
    gen_server:call(Name, {get, Property}).

set(Name, profile_fun, Fun) ->
    gen_server:cast(Name, {set, profile_fun, Fun});
set(Name, Property, _Value) ->
    ?LOG_INFO("?MODULE:set for ~p with ~p not implemented~n",
              [Name, Property]).

stop(Name) when is_atom(Name) orelse is_pid(Name) ->
    gen_server:call(Name, stop);
stop({Scope, Name}) when Scope =:= local orelse Scope =:= global ->
    stop(Name);
stop(Options) ->
    State = parse_options(Options),
    stop(State#mochiweb_socket_server.name).

%% Internal API

parse_options(State=#mochiweb_socket_server{}) ->
    State;
parse_options(Options) ->
    parse_options(Options, #mochiweb_socket_server{}).


parse_options([], State=#mochiweb_socket_server{acceptor_pool_size=PoolSize,
                                                max=Max}) ->
    case Max < PoolSize of
        true ->
            ?LOG_INFO(
               #{label => {mochiweb_socket_server, info},
                 report => [{warning,
                             "max is set lower than acceptor_pool_size"},
                            {max, Max},
                            {acceptor_pool_size, PoolSize}]},
               #{domain => [mochiweb],
                 report_cb => fun logger:format_otp_report/1,
                 logger_formatter => #{title => "INFO REPORT"}});
        false ->
            ok
    end,
    State;
parse_options([{name, L} | Rest], State) when is_list(L) ->
    Name = {local, list_to_atom(L)},
    parse_options(Rest, State#mochiweb_socket_server{name=Name});
parse_options([{name, A} | Rest], State) when A =:= undefined ->
    parse_options(Rest, State#mochiweb_socket_server{name=A});
parse_options([{name, A} | Rest], State) when is_atom(A) ->
    Name = {local, A},
    parse_options(Rest, State#mochiweb_socket_server{name=Name});
parse_options([{name, Name} | Rest], State) ->
    parse_options(Rest, State#mochiweb_socket_server{name=Name});
parse_options([{port, L} | Rest], State) when is_list(L) ->
    Port = list_to_integer(L),
    parse_options(Rest, State#mochiweb_socket_server{port=Port});
parse_options([{port, Port} | Rest], State) ->
    parse_options(Rest, State#mochiweb_socket_server{port=Port});
parse_options([{ip, Ip} | Rest], State) ->
    ParsedIp = case Ip of
                   any ->
                       any;
                   Ip when is_tuple(Ip) ->
                       Ip;
                   Ip when is_list(Ip) ->
                       {ok, IpTuple} = inet_parse:address(Ip),
                       IpTuple
               end,
    parse_options(Rest, State#mochiweb_socket_server{ip=ParsedIp});
parse_options([{loop, Loop} | Rest], State) ->
    parse_options(Rest, State#mochiweb_socket_server{loop=Loop});
parse_options([{backlog, Backlog} | Rest], State) ->
    parse_options(Rest, State#mochiweb_socket_server{backlog=Backlog});
parse_options([{nodelay, NoDelay} | Rest], State) ->
    parse_options(Rest, State#mochiweb_socket_server{nodelay=NoDelay});
parse_options([{recbuf, RecBuf} | Rest], State) when is_integer(RecBuf) orelse
                                                RecBuf == undefined ->
    %% XXX: `recbuf' value which is passed to `gen_tcp'
    %% and value reported by `inet:getopts(P, [recbuf])' may
    %% differ. They depends on underlying OS. From linux mans:
    %%
    %% The kernel doubles this value (to allow space for
    %% bookkeeping overhead) when it is set using setsockopt(2),
    %% and this doubled value is returned by getsockopt(2).
    %%
    %% See: man 7 socket | grep SO_RCVBUF
    %%
    %% In case undefined is passed instead of the default buffer
    %% size ?RECBUF_SIZE, no size is set and the OS can control it dynamically
    parse_options(Rest, State#mochiweb_socket_server{recbuf=RecBuf});
parse_options([{buffer, Buffer} | Rest], State) when is_integer(Buffer) orelse
                                                Buffer == undefined ->
    %% `buffer` sets Erlang's userland socket buffer size. The size of this
    %% buffer affects the maximum URL path that can be parsed. URL sizes that
    %% are larger than this plus the size of the HTTP verb and some whitespace
    %% will result in an `emsgsize` TCP error.
    %%
    %% If this value is not set Erlang sets it to 1460 which might be too low.
    parse_options(Rest, State#mochiweb_socket_server{buffer=Buffer});
parse_options([{acceptor_pool_size, Max} | Rest], State) ->
    MaxInt = ensure_int(Max),
    parse_options(Rest,
                  State#mochiweb_socket_server{acceptor_pool_size=MaxInt});
parse_options([{max, Max} | Rest], State) ->
    MaxInt = ensure_int(Max),
    parse_options(Rest, State#mochiweb_socket_server{max=MaxInt});
parse_options([{ssl, Ssl} | Rest], State) when is_boolean(Ssl) ->
    parse_options(Rest, State#mochiweb_socket_server{ssl=Ssl});
parse_options([{ssl_opts, SslOpts} | Rest], State) when is_list(SslOpts) ->
    SslOpts1 = [{ssl_imp, new} | proplists:delete(ssl_imp, SslOpts)],
    parse_options(Rest, State#mochiweb_socket_server{ssl_opts=SslOpts1});
parse_options([{profile_fun, ProfileFun} | Rest], State) when is_function(ProfileFun) ->
    parse_options(Rest, State#mochiweb_socket_server{profile_fun=ProfileFun});
parse_options([{tls_alert_handler, Handler} | Rest], State)
  when is_function(Handler, 2) orelse Handler =:= undefined ->
    parse_options(Rest,
                  State#mochiweb_socket_server{tls_alert_handler=Handler});
%% A fun that returns ssl options to be passed to ssl:handshake/3 for each
%% accepted connection, in addition to ssl_opts, which are given to
%% ssl:listen/2. Options that are secret, like the private key, should be
%% passed this way: ssl keeps the options it listens with in the listen
%% socket, which gets printed with the state of the processes holding it.
%% The fun is called for every connection, so it can return new options,
%% like a new certificate, without the server being restarted.
parse_options([{ssl_handshake_opts_fun, Fun} | Rest], State)
  when is_function(Fun, 0) ->
    parse_options(Rest,
                  State#mochiweb_socket_server{ssl_handshake_opts_fun=Fun}).


start_server(F, State=#mochiweb_socket_server{name=Name}) ->
    case Name of
        undefined ->
            gen_server:F(?MODULE, State, []);
        _ ->
            gen_server:F(Name, ?MODULE, State, [])
    end.

ensure_int(N) when is_integer(N) ->
    N;
ensure_int(S) when is_list(S) ->
    list_to_integer(S).

ipv6_supported() ->
    case (catch inet:getaddr("localhost", inet6)) of
        {ok, _Addr} ->
            true;
        {error, _} ->
            false
    end.

init(State=#mochiweb_socket_server{ip=Ip, port=Port, backlog=Backlog,
                                   nodelay=NoDelay, recbuf=RecBuf,
                                   buffer=Buffer}) ->
    process_flag(trap_exit, true),

    BaseOpts = [binary,
                {reuseaddr, true},
                {packet, 0},
                {backlog, Backlog},
                {exit_on_close, false},
                {active, false},
                {nodelay, NoDelay}],
    Opts = case Ip of
        any ->
            case ipv6_supported() of % IPv4, and IPv6 if supported
                true -> [inet, inet6 | BaseOpts];
                _ -> BaseOpts
            end;
        {_, _, _, _} -> % IPv4
            [inet, {ip, Ip} | BaseOpts];
        {_, _, _, _, _, _, _, _} -> % IPv6
                   %% If ipv6_v6only is not specified it has different behavior
                   %% on different platforms (at least linux, mac and windows
                   %% tested)
                   [inet6, {ip, Ip}, {ipv6_v6only, true} | BaseOpts]
    end,
    OptsBuf = set_buffer_opts(RecBuf, Buffer, Opts),
    listen(Port, OptsBuf, State).


set_buffer_opts(undefined, undefined, Opts) ->
    % If recbuf is undefined, user space buffer is set to the default 1460
    % value. That unexpectedly break the {packet, http} parser and any URL
    % lines longer than 1460 would error out with emsgsize. So when recbuf is
    % undefined, use previous value of recbuf for buffer in order to keep older
    % code from breaking.
    [{buffer, ?RECBUF_SIZE} | Opts];
set_buffer_opts(RecBuf, undefined, Opts) ->
    [{recbuf, RecBuf} | Opts];
set_buffer_opts(undefined, Buffer, Opts) ->
    [{buffer, Buffer} | Opts];
set_buffer_opts(RecBuf, Buffer, Opts) ->
    % Note: order matters, recbuf will override buffer unless buffer value
    % comes first, except on older versions of Erlang (ex. 17.0) where it works
    % exactly the opposite.
    [{buffer, Buffer}, {recbuf, RecBuf} | Opts].


new_acceptor_pool(State=#mochiweb_socket_server{acceptor_pool_size=Size}) ->
    lists:foldl(fun (_, S) -> new_acceptor(S) end, State, lists:seq(1, Size)).

new_acceptor(State=#mochiweb_socket_server{acceptor_pool=Pool,
                                           recbuf=RecBuf,
                                           loop=Loop,
                                           listen=Listen,
                                           tls_alert_handler=AlertHandler,
                                           ssl_handshake_opts_fun=HsOptsFun}) ->
    LoopOpts = [{recbuf, RecBuf},
                {tls_alert_handler, AlertHandler}],
    Pid = mochiweb_acceptor:start_link(self(), Listen, Loop, LoopOpts,
                                       HsOptsFun),
    State#mochiweb_socket_server{
      acceptor_pool=sets:add_element(Pid, Pool)}.

listen(Port, Opts, State=#mochiweb_socket_server{ssl=Ssl, ssl_opts=SslOpts}) ->
    case mochiweb_socket:listen(Ssl, Port, Opts, SslOpts) of
        {ok, Listen} ->
            {ok, ListenPort} = mochiweb_socket:port(Listen),
            {ok, new_acceptor_pool(State#mochiweb_socket_server{
                                     listen=Listen,
                                     port=ListenPort})};
        {error, Reason} ->
            {stop, Reason}
    end.

do_get(port, #mochiweb_socket_server{port=Port}) ->
    Port;
do_get(waiting_acceptors, #mochiweb_socket_server{acceptor_pool=Pool}) ->
    sets:size(Pool);
do_get(active_sockets, #mochiweb_socket_server{active_sockets=ActiveSockets}) ->
    ActiveSockets.


state_to_proplist(#mochiweb_socket_server{name=Name,
                                          port=Port,
                                          active_sockets=ActiveSockets}) ->
    [{name, Name}, {port, Port}, {active_sockets, ActiveSockets}].

upgrade_state(State = #mochiweb_socket_server{}) ->
    State;
upgrade_state({mochiweb_socket_server, Port, Loop, Name,
             Max, IP, Listen, NoDelay, Backlog, ActiveSockets,
             AcceptorPoolSize, SSL, SSL_opts,
             AcceptorPool}) ->
    #mochiweb_socket_server{port=Port, loop=Loop, name=Name, max=Max, ip=IP,
                            listen=Listen, nodelay=NoDelay, backlog=Backlog,
                            active_sockets=ActiveSockets,
                            acceptor_pool_size=AcceptorPoolSize,
                            ssl=SSL,
                            ssl_opts=SSL_opts,
                            acceptor_pool=AcceptorPool}.

handle_call(Req, From, State) when ?is_old_state(State) ->
    handle_call(Req, From, upgrade_state(State));
handle_call({get, Property}, _From, State) ->
    Res = do_get(Property, State),
    {reply, Res, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Message, _From, State) ->
    Res = error,
    {reply, Res, State}.


handle_cast(Req, State) when ?is_old_state(State) ->
    handle_cast(Req, upgrade_state(State));
handle_cast({accepted, Pid, Timing},
            State=#mochiweb_socket_server{active_sockets=ActiveSockets}) ->
    State1 = State#mochiweb_socket_server{active_sockets=1 + ActiveSockets},
    case State#mochiweb_socket_server.profile_fun of
        undefined ->
            undefined;
        F when is_function(F) ->
            catch F([{timing, Timing} | state_to_proplist(State1)])
    end,
    {noreply, recycle_acceptor(Pid, State1)};
handle_cast({set, profile_fun, ProfileFun}, State) ->
    State1 = case ProfileFun of
                 ProfileFun when is_function(ProfileFun); ProfileFun =:= undefined ->
                     State#mochiweb_socket_server{profile_fun=ProfileFun};
                 _ ->
                     State
             end,
    {noreply, State1}.


terminate(Reason, State) when ?is_old_state(State) ->
    terminate(Reason, upgrade_state(State));
terminate(_Reason, #mochiweb_socket_server{listen=Listen}) ->
    mochiweb_socket:close(Listen).

code_change(_OldVsn, State, _Extra) ->
    State.

recycle_acceptor(Pid, State=#mochiweb_socket_server{
                        acceptor_pool=Pool,
                        acceptor_pool_size=PoolSize,
                        max=Max,
                        active_sockets=ActiveSockets}) ->
    %% A socket is considered to be active from immediately after it
    %% has been accepted (see the {accepted, Pid, Timing} cast above).
    %% This function will be called when an acceptor is transitioning
    %% to an active socket, or when either type of Pid dies. An acceptor
    %% Pid will always be in the acceptor_pool set, and an active socket
    %% will be in that set during the transition but not afterwards.
    Pool1 = sets:del_element(Pid, Pool),
    NewSize = sets:size(Pool1),
    ActiveSockets1 = case NewSize =:= sets:size(Pool) of
                         %% Pid has died and it is not in the acceptor set,
                         %% it must be an active socket.
                         true -> max(0, ActiveSockets - 1);
                         false -> ActiveSockets
                     end,
    State1 = State#mochiweb_socket_server{
               acceptor_pool=Pool1,
               active_sockets=ActiveSockets1},
    %% Spawn a new acceptor only if it will not overrun the maximum socket
    %% count or the maximum pool size.
    case NewSize + ActiveSockets1 < Max andalso NewSize < PoolSize of
        true -> new_acceptor(State1);
        false -> State1
    end.

handle_info(Msg, State) when ?is_old_state(State) ->
    handle_info(Msg, upgrade_state(State));
handle_info({'EXIT', Pid, normal}, State) ->
    {noreply, recycle_acceptor(Pid, State)};
handle_info({'EXIT', Pid, Reason},
            State=#mochiweb_socket_server{acceptor_pool=Pool}) ->
    case sets:is_element(Pid, Pool) of
        true ->
            %% If there was an unexpected error accepting, log and sleep.
            ?LOG_ERROR(#{label => {mochiweb_socket_server, acceptor_error},
                         report => [{location, {?MODULE, ?LINE}},
                                    {acceptor_error, Reason}]},
                       #{domain => [mochiweb],
                         report_cb => fun logger:format_otp_report/1,
                         logger_formatter => #{title => "ERROR REPORT"}}),
            timer:sleep(100);
        false ->
            ok
    end,
    {noreply, recycle_acceptor(Pid, State)};

% this is what release_handler needs to get a list of modules,
% since our supervisor modules list is set to 'dynamic'
% see sasl-2.1.9.2/src/release_handler_1.erl get_dynamic_mods
handle_info({From, Tag, get_modules}, State = #mochiweb_socket_server{name={local,Mod}}) ->
    From ! {element(2,Tag), [Mod]},
    {noreply, State};

% If for some reason we can't get the module name, send empty list to avoid release_handler timeout:
handle_info({From, Tag, get_modules}, State) ->
    ?LOG_INFO("mochiweb_socket_server replying to dynamic modules request as '[]'~n",[]),
    From ! {element(2,Tag), []},
    {noreply, State};

handle_info(Info, State) ->
    ?LOG_INFO(#{label => {mochiweb_socket_server, info},
                report => [{'INFO', Info}, {'State', State}]},
              #{domain => [mochiweb],
                report_cb => fun logger:format_otp_report/1,
                logger_formatter => #{title => "INFO REPORT"}}),
    {noreply, State}.



%%
%% Tests
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

upgrade_state_test() ->
    OldState = {mochiweb_socket_server,
                port, loop, name,
                max, ip, listen,
                nodelay, backlog,
                active_sockets,
                acceptor_pool_size,
                ssl, ssl_opts, acceptor_pool},
    State = upgrade_state(OldState),
    CmpState = #mochiweb_socket_server{port=port, loop=loop,
                                       name=name, max=max, ip=ip,
                                       listen=listen, nodelay=nodelay,
                                       backlog=backlog,
                                       active_sockets=active_sockets,
                                       acceptor_pool_size=acceptor_pool_size,
                                       ssl=ssl, ssl_opts=ssl_opts,
                                       acceptor_pool=acceptor_pool,
                                       profile_fun=undefined},
    ?assertEqual(CmpState, State).


set_buffer_opts_test() ->
    ?assertEqual([{buffer, 8192}], set_buffer_opts(undefined, undefined, [])),
    ?assertEqual([{recbuf, 5}], set_buffer_opts(5, undefined, [])),
    ?assertEqual([{buffer, 6}], set_buffer_opts(undefined, 6, [])),
    ?assertEqual([{buffer, 6}, {recbuf, 5}], set_buffer_opts(5, 6, [])).

ssl_handshake_opts_fun_test_() ->
    {setup,
     fun () ->
             {ok, Apps} = application:ensure_all_started(ssl),
             Apps
     end,
     fun (Apps) ->
             [application:stop(A) || A <- lists:reverse(Apps)]
     end,
     ?_test(ssl_handshake_opts_fun())}.

ssl_handshake_opts_fun() ->
    {CertA, KeyA} = test_cert_and_key(),
    {CertB, KeyB} = test_cert_and_key(),
    OptsKey = {?MODULE, ssl_handshake_opts},
    %% Without ciphers in ssl_opts, the server listens with TLS 1.2 ciphers
    %% only (see mochiweb_socket:add_unbroken_ciphers_default/1), so this also
    %% checks that the options passed to the handshake override those
    Ciphers = ssl:cipher_suites(default, 'tlsv1.3') ++
        ssl:cipher_suites(default, 'tlsv1.2'),
    SetCertAndKey =
        fun (Cert, Key) ->
                persistent_term:put(OptsKey,
                                    [{ciphers, Ciphers},
                                     {certs_keys, [#{cert => Cert,
                                                     key => Key}]}])
        end,
    SetCertAndKey(CertA, KeyA),
    {ok, Pid} = start_link(
                  [{port, 0}, {ip, {127, 0, 0, 1}}, {ssl, true},
                   {ssl_opts, [{versions, ['tlsv1.3', 'tlsv1.2']}]},
                   {ssl_handshake_opts_fun,
                    fun () -> persistent_term:get(OptsKey) end},
                   {loop, fun (Socket, _Opts) ->
                                  %% wait for the client to close
                                  _ = mochiweb_socket:recv(Socket, 0, 5000),
                                  mochiweb_socket:close(Socket)
                          end}]),
    try
        Port = get(Pid, port),
        ?assertEqual(CertA, test_peer_cert(Port)),

        %% The key isn't kept in the state of the server, which includes the
        %% listen socket
        {_, KeyDer} = KeyA,
        Needle = string:trim(io_lib:format("~w", [binary:part(KeyDer, 32, 16)]),
                             both, "<>"),
        ?assertNotEqual(nomatch,
                        string:find(io_lib:format("~w", [KeyDer]), Needle)),
        ?assertEqual(nomatch,
                     string:find(io_lib:format("~w", [sys:get_state(Pid)]),
                                 Needle)),

        %% New options are used for new connections
        SetCertAndKey(CertB, KeyB),
        ?assertEqual(CertB, test_peer_cert(Port))
    after
        stop(Pid),
        persistent_term:erase(OptsKey)
    end.

ssl_handshake_opts_error_test_() ->
    {setup,
     fun () ->
             {ok, Apps} = application:ensure_all_started(ssl),
             Apps
     end,
     fun (Apps) ->
             [application:stop(A) || A <- lists:reverse(Apps)]
     end,
     ?_test(ssl_handshake_opts_error())}.

%% ssl puts the value of an option it rejects into the error, which the
%% acceptor logs, so the value, which can be the private key, is dropped
ssl_handshake_opts_error() ->
    {Cert, {KeyType, KeyDer}} = test_cert_and_key(),
    {ok, ListenSocket} = ssl:listen(0, [{versions, ['tlsv1.3', 'tlsv1.2']},
                                        {active, false}]),
    try
        {ok, {_, Port}} = ssl:sockname(ListenSocket),
        spawn_link(fun () ->
                           _ = ssl:connect({127, 0, 0, 1}, Port,
                                           [{verify, verify_none}], 5000)
                   end),
        {ok, Socket} = mochiweb_socket:transport_accept({ssl, ListenSocket}),
        %% The key in a form ssl doesn't take
        BadKey = {KeyType, KeyDer, not_encrypted},
        ?assertEqual({error, {error, {options, key}}},
                     mochiweb_socket:finish_accept(
                       Socket,
                       fun () ->
                               [{certs_keys, [#{cert => Cert, key => BadKey}]}]
                       end))
    after
        ssl:close(ListenSocket)
    end.

test_cert_and_key() ->
    CertOpts = [{key, {rsa, 2048, 65537}}, {digest, sha256}],
    Chain = #{root => CertOpts, intermediates => [], peer => CertOpts},
    #{server_config := Config} =
        public_key:pkix_test_data(#{server_chain => Chain,
                                    client_chain => Chain}),
    {proplists:get_value(cert, Config), proplists:get_value(key, Config)}.

test_peer_cert(Port) ->
    {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port,
                               [{verify, verify_none}, {active, false}],
                               5000),
    {ok, Cert} = ssl:peercert(Socket),
    ssl:close(Socket),
    Cert.

-endif.
