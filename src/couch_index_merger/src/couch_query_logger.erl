%% @author Couchbase <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc Query volume logger
%%

-module(couch_query_logger).
-behaviour(gen_server).

-include("couch_db.hrl").

-define(INTERVAL, 60000).
-define(ENABLED, true).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0, log/3]).
-export([set_interval/1, reset_interval/0]).
-export([enable/0, disable/0]).

-record(state, {interval, enabled}).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    ets:new(?MODULE, [named_table, private]),
    erlang:send_after(?INTERVAL, self(), dump),
    {ok, #state{interval = ?INTERVAL, enabled = ?ENABLED}}.

handle_call({enable, Enabled}, _From, State) ->
    {reply, ok, State#state{enabled = Enabled}};

handle_call({interval, Interval}, _From, State) ->
    {reply, ok, State#state{interval = Interval}};

handle_call({Path, Origin, Parameter, Value}, _From, #state{enabled = true} = State) ->
    Pos = pos(Origin, Parameter),
    try
        ets:update_counter(?MODULE, Path, {Pos, Value})
    catch
        error:badarg ->
            ets:insert(?MODULE, default(Path, Pos, Value))
    end,
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(dump, #state{interval = Interval} = State) ->
    dump(),
    ets:delete_all_objects(?MODULE),
    erlang:send_after(Interval, self(), dump),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State) ->
    dump(),
    Len = process_info(self(), message_queue_len),
    Msg = ?LOG_USERDATA(Reason),
    ?LOG_ERROR("couch_query_logger terminating because of ~s : ~p", [Msg, Len]),
    ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ===================================================================
%% module callbacks
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

log(#httpd{path_parts=Parts, mochi_req=MochiReq} = Req, QueryArgument, Value) ->
    {Origin, Path} = case Parts of
        [_, <<"_design">>, _, <<"_view">>, _] ->
            {external, mochiweb_request:get(path, MochiReq)};
        _ ->
            {internal, ?b2l(couch_util:log_parse_post(Req))}
    end,
    gen_server:call(?MODULE, {Path, Origin, QueryArgument, Value}).

set_interval(Interval) when is_integer(Interval)->
    gen_server:call(?MODULE, {interval, Interval}).

reset_interval() ->
    gen_server:call(?MODULE, {interval, ?INTERVAL}).

enable() ->
    gen_server:call(?MODULE, {enable, true}).

disable() ->
    gen_server:call(?MODULE, {enable, false}).

%% ===================================================================
%% helper functions
%% ===================================================================

pos(internal, ok) -> 2;
pos(internal, update_after) -> 3;
pos(internal, false) -> 4;
pos(internal, response_size) -> 5;
pos(external, ok) -> 6;
pos(external, update_after) -> 7;
pos(external, false) -> 8;
pos(external, response_size) -> 9.

default(Path, Pos, Value) ->
    %Iok, Iua, Ifalse, Ibytes, Eok, Eua, Efalse, Ebytes
    erlang:setelement(Pos, {Path, 0, 0, 0, 0, 0, 0, 0, 0}, Value).

tostring({Path, IO, IU, IF, IBytes, EO, EU, EF, EBytes}, Acc) ->
    [io_lib:format("~s | internal={stale: {ok: ~B, upd_after: ~B, false: ~B}, ResultBytes: ~B}"
                   " | external={stale: {ok: ~B, upd_after: ~B, false: ~B}, ResultBytes: ~B}~n",
                   [?LOG_USERDATA(Path), IO, IU, IF, IBytes, EO, EU, EF, EBytes]) | Acc].

dump() ->
    QVol = ets:foldl(fun tostring/2, [], ?MODULE),
    Msg = ["Query-Volume", $\n, QVol, "---"],
    ?LOG_INFO("~s", [?l2b(Msg)]).
