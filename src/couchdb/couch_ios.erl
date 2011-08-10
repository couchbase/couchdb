% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.
-module(couch_ios).
-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([get_port/0, set_port/1]).
-on_load(nif_init/0).

-include("couch_db.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

nif_init() ->
    (catch erlang:load_nif("ios", 0)).

init(_) ->
    io:format("Open iosdrv port~n"),
    Port = open_port({spawn, "ios_drv"}, []),
    {ok, Port}.

% If the iOS NIF doesn't load, always respond with 0.
set_port(_) -> ok.
get_port() -> 0.

terminate(_Reason, _) ->
    ok.

handle_call(Msg, From, State) ->
    ?LOG_ERROR("Unknown call message to ~p from ~p: ~p", [?MODULE, From, Msg]),
    {stop, error, State}.

handle_cast(Msg, State) ->
    ?LOG_ERROR("Unknown cast message to ~p: ~p", [?MODULE, Msg]),
    {stop, error, State}.

handle_info({foregrounded}, Port) ->
    CurrentPort = mochiweb_socket_server:get(couch_httpd, port),
    ?LOG_INFO("iOS application suspended, restarting. Current port: ~p", [CurrentPort]),
    set_port(CurrentPort),
    couch_server_sup:restart_core_server(),
    {ok, Port};

handle_info(Msg, State) ->
    ?LOG_ERROR("Unexpected info message to ~p: ~p", [?MODULE, Msg]),
    {stop, error, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
