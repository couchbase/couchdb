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

-module(couch_set_view_update_daemon).
-behaviour(gen_server).

% public API
-export([start_link/0, config_change/3]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").


-record(state, {
    interval,
    num_changes,
    timer_ref
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    State = #state{
        interval = list_to_integer(
            couch_config:get("set_views", "update_interval", "5000")),
        num_changes = list_to_integer(
            couch_config:get("set_views", "update_min_changes", "5000"))
    },
    ok = couch_config:register(fun ?MODULE:config_change/3),
    ?LOG_INFO("Set view update daemon, starting with the following settings:~n"
              "  update interval:           ~pms~n"
              "  minimum number of changes: ~p~n",
              [State#state.interval, State#state.num_changes]),
    {ok, schedule_timer(State)}.


handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.


handle_cast(trigger_updates, #state{num_changes = MinNumChanges} = State) ->
    ok = ets:foldl(
        fun({{_SetName, _Sig}, Pid}, ok) ->
            ok = gen_server:cast(Pid, {update, MinNumChanges})
        end,
        ok, couch_sig_to_setview_pid),
    {noreply, schedule_timer(State)};

handle_cast({interval, NewInterval}, State) ->
    ?LOG_INFO("Set view update daemon, interval updated to ~pms (was ~pms)",
              [NewInterval, State#state.interval]),
    {noreply, schedule_timer(State#state{interval = NewInterval})};

handle_cast({num_changes, NewNumChanges}, State) ->
    ?LOG_INFO("Set view update daemon, minimum number of changes updated"
              " to ~p (was ~p)",
              [NewNumChanges, State#state.num_changes]),
    {noreply, schedule_timer(State#state{num_changes = NewNumChanges})}.


handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.


config_change("set_views", "update_interval", ValueList) ->
    Value = list_to_integer(ValueList),
    ok = gen_server:cast(?MODULE, {interval, Value});
config_change("set_views", "update_min_changes", ValueList) ->
    Value = list_to_integer(ValueList),
    ok = gen_server:cast(?MODULE, {num_changes, Value}).


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


schedule_timer(#state{interval = 0} = State) ->
    cancel_timer(State);
schedule_timer(#state{num_changes = 0} = State) ->
    cancel_timer(State);
schedule_timer(#state{interval = Interval} = State) ->
    _ = cancel_timer(State),
    {ok, NewTimerRef} = timer:apply_after(
        Interval, gen_server, cast, [?MODULE, trigger_updates]),
    State#state{timer_ref = NewTimerRef}.


cancel_timer(#state{timer_ref = undefined} = State) ->
    State;
cancel_timer(#state{timer_ref = Ref} = State) ->
    {ok, cancel} = timer:cancel(Ref),
    State#state{timer_ref = undefined}.
