%% @author Bob Ippolito <bob@mochimedia.com>
%% @copyright 2010 Mochi Media, Inc.
%%
%% Permission is hereby granted, free of charge, to any person obtaining a
%% copy of this software and associated documentation files (the "Software"),
%% to deal in the Software without restriction, including without limitation
%% the rights to use, copy, modify, merge, publish, distribute, sublicense,
%% and/or sell copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
%% THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
%% DEALINGS IN THE SOFTWARE.

%% @doc MochiWeb acceptor.

-module(mochiweb_acceptor).

-author('bob@mochimedia.com').

-include_lib("kernel/include/logger.hrl").
-include("internal.hrl").

-export([init/5, start_link/3, start_link/4, start_link/5]).

-define(EMFILE_SLEEP_MSEC, 100).

start_link(Server, Listen, Loop) ->
    start_link(Server, Listen, Loop, []).

start_link(Server, Listen, Loop, Opts) ->
    start_link(Server, Listen, Loop, Opts, undefined).

start_link(Server, Listen, Loop, Opts, HsOptsFun) ->
    proc_lib:spawn_link(?MODULE, init,
			[Server, Listen, Loop, Opts, HsOptsFun]).

do_accept(Server, Listen, Handler, HsOptsFun) ->
    T1 = os:timestamp(),
    case mochiweb_socket:transport_accept(Listen) of
      {ok, Socket} ->
	  gen_server:cast(Server,
			  {accepted, self(),
			   timer:now_diff(os:timestamp(), T1)}),
	  finish_accept(Socket, HsOptsFun, Handler);
      Other -> Other
    end.

finish_accept(Socket, HsOptsFun, undefined) ->
    mochiweb_socket:finish_accept(Socket, HsOptsFun);
finish_accept(Socket, HsOptsFun, Handler) ->
    case mochiweb_socket:type(Socket) of
      plain ->
	  mochiweb_socket:finish_accept(Socket, HsOptsFun);
      ssl ->
	  %% Capture the peer first: a failed handshake closes the socket.
	  ConnInfo = conn_info(Socket),
	  case mochiweb_socket:finish_accept(Socket, HsOptsFun) of
	    {error, {tls_alert, _} = Reason} = Err ->
		notify_tls_alert(Handler, Reason, ConnInfo),
		Err;
	    Other ->
		Other
	  end
    end.

conn_info(Socket) ->
    #{peername => addr(mochiweb_socket:peername(Socket)),
      sockname => addr(mochiweb_socket:sockname(Socket))}.

addr({ok, {Host, Port}}) -> {Host, Port};
addr(_) -> undefined.

notify_tls_alert(Handler, Reason, ConnInfo) ->
    (catch Handler(Reason, ConnInfo)),
    ok.

init(Server, Listen, Loop, Opts, HsOptsFun) ->
    Handler = proplists:get_value(tls_alert_handler, Opts, undefined),
    case catch do_accept(Server, Listen, Handler, HsOptsFun) of
      {ok, Socket} -> call_loop(Loop, Socket, Opts);
      {error, Err}
	  when Err =:= closed orelse
		 Err =:= esslaccept orelse Err =:= timeout ->
	  exit(normal);
      Other ->
	  %% Mitigate out of file descriptor scenario by sleeping for a
	  %% short time to slow error rate
	  case Other of
	    {error, emfile} ->
		receive  after ?EMFILE_SLEEP_MSEC -> ok end;
	    _ -> ok
	  end,
      %% Other isn't formatted here, so that a logger handler can still
      %% remove secrets from it: the ssl options a handshake failed with
      %% (like the private key) can be in it
      ?LOG_ERROR(
         #{label => {mochiweb_acceptor, accept_error},
           report => [{application, mochiweb},
                      {accept_error, Other}]},
         #{domain => [mochiweb],
           report_cb => fun logger:format_otp_report/1,
           logger_formatter => #{title => "ERROR REPORT"}}),
	  exit({error, accept_failed})
    end.

call_loop({M, F}, Socket, Opts) when is_atom(M) ->
    M:F(Socket, Opts);
call_loop({M, F, [A1]}, Socket, Opts) when is_atom(M) ->
    M:F(Socket, Opts, A1);
call_loop({M, F, A}, Socket, Opts) when is_atom(M) ->
    erlang:apply(M, F, [Socket, Opts | A]);
call_loop(Loop, Socket, Opts) -> Loop(Socket, Opts).
