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

-module(couch_indexer_manager).
-behaviour(gen_server).

% public API
-export([start_link/0, enter/0, leave/0]).

% gen_server API
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("couch_db.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3
]).

-record(state, {
    current = [],
    limit,
    waiting = queue:new(),
    mon_refs = dict:new()
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

enter() ->
    ok = gen_server:call(?MODULE, {enter, self()}, infinity).

leave() ->
    ok = gen_server:cast(?MODULE, {leave, self()}).


init([]) ->
    State = #state{
        limit = list_to_integer(
            couch_config:get("couchdb", "max_parallel_indexers", "4"))
    },
    ok = couch_config:register(
        fun("couchdb", "max_parallel_indexers", Limit) ->
            ok = gen_server:cast(?MODULE, {limit, list_to_integer(Limit)})
        end),
    {ok, State}.


handle_call({enter, Pid}, From, #state{current = Current, waiting = Waiting} = State) ->
    MonRef = erlang:monitor(process, Pid),
    MonRefs2 = dict:store(Pid, MonRef, State#state.mon_refs),
    State2 = State#state{mon_refs = MonRefs2},
    case length(Current) >= State#state.limit of
    true ->
        {noreply, State2#state{waiting = queue:in({From, Pid}, Waiting)}};
    false ->
        {reply, ok, State2#state{current = [Pid | Current]}}
    end.


handle_cast({leave, Pid}, State) ->
    {noreply, handle_leave(Pid, State)};

handle_cast({limit, Limit}, State) ->
    {noreply, State#state{limit = Limit}}.


handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    {noreply, handle_leave(Pid, State)}.


handle_leave(Pid, #state{current = Current, waiting = Waiting} = State) ->
    MRef = dict:fetch(Pid, State#state.mon_refs),
    erlang:demonitor(MRef, [flush]),
    State2 = State#state{mon_refs = dict:erase(Pid, State#state.mon_refs)},
    case Current -- [Pid] of
    Current ->
        Waiting2 = queue:filter(fun({_, Pid0}) -> Pid0 =/= Pid end, Waiting),
        State2#state{waiting = Waiting2};
    Current2 ->
        case queue:out(Waiting) of
        {empty, Waiting2} ->
            Current3 = Current2;
        {{value, {From, FromPid}}, Waiting2} ->
            gen_server:reply(From, ok),
            Current3 = [FromPid | Current2]
        end,
        State2#state{current = Current3, waiting = Waiting2}
    end.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.
