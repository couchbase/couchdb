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

-module(couch_view_server_os).
-behaviour(gen_server).

-include("couch_db.hrl").

-export([is_lightweight/0]).
-export([get_server/3, ret_server/1]).
-export([map/2, reduce/3, rereduce/3]).

-export([start_link/0]).
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-define(MAX_PROCS, "max_processes").
-define(IDLE_PROCS, "idle_processes").

is_lightweight() ->
    false.

get_server(Arg, Maps, Reds) ->
    {ok, Pid} = gen_server:call(?MODULE, {get_server, Arg}),
    link(Pid),
    true = couch_os_process:prompt(Pid, [<<"compile">>, Maps, Reds]),
    {ok, Pid}.

ret_server(Pid) ->
    % If a process has died then it'll get cleaned
    % up on its own.
    case is_process_alive(Pid) of
        true ->
            true = couch_os_process:prompt(Pid, [<<"reset">>]),
            unlink(Pid),
            gen_server:call(?MODULE, {ret_server, Pid});
        _ ->
            ok
    end.

map(Pid, Docs) ->
    Results = lists:map(fun(Doc) ->
        Json = couch_doc:to_json_obj(Doc, []),

        [true, FunsResults] = couch_os_process:prompt(Pid, [<<"map">>, Json]),
        % the results are a json array of function map yields like this:
        % [FunResults1, FunResults2 ...]
        % where funresults is are json arrays of key value pairs:
        % [[Key1, Value1], [Key2, Value2]]
        % Convert the key, value pairs to tuples like
        % [{Key1, Value1}, {Key2, Value2}]
        lists:map(fun(FunRs) ->
            [list_to_tuple(FunResult) || FunResult <- FunRs]
        end, FunsResults)
    end, Docs),
    {ok, Results}.

reduce(Pid, ViewId, KVs) ->
    [true, Reds] = couch_os_process:prompt(Pid, [<<"reduce">>, ViewId, KVs]),
    {ok, Reds}.

rereduce(Pid, ViewId, Vals) ->
    [true, Reds] = couch_os_process:prompt(Pid, [<<"rereduce">>, ViewId, Vals]),
    {ok, Reds}.

% This gen_server is repsonsible for handling the OS level processes that
% will be used to service map/reduce execution.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    CodeChange = fun("view_server_os" ++ _, _) ->
        supervisor:terminate_child(couch_secondary_services, query_servers),
        [supervisor:restart_child(couch_secondary_services, query_servers)]
    end,
    ok = couch_config:register(CodeChange),
    put(max_pids, list_to_integer(couch_config:get("view_server_os", ?MAX_PROCS, "32"))),
    put(idle_pids, list_to_integer(couch_config:get("view_server_os", ?IDLE_PROCS, "4"))),
    Pids = ets:new(?MODULE, [ordered_set, public]),
    %spawn_link(fun() -> dump_table(Pids) end),
    {ok, {Pids, []}}.

dump_table(Pids) ->
    lists:foreach(fun({Pid, _Arg, Status, Client}) ->
        io:format("~p ~p ~p~n", [Pid, Status, Client])
    end, ets:tab2list(Pids)),
    timer:sleep(5000),
    dump_table(Pids).

terminate(_Reason, {Pids, Waiters}) ->
    Mesg = {error, terminating},
    [catch gen_server:reply(W, Mesg) || {W, _} <- Waiters],
    [couch_util:shutdown_sync(P) || {P,_,_,_} <- ets:tab2list(Pids)],
    ok.

handle_call({get_server, Arg}, {Client, _}=From, {Pids, Waiters}) ->
    case ets:match(Pids, {'$1', Arg, idle, '_'}) of
        [[Pid] | _] ->
            true = ets:update_element(Pids, Pid, [{3, busy}, {4, Client}]),
            {reply, {ok, Pid}, {Pids, Waiters}};
        _ ->
            {ok, Waiters2} = start_waiters(Pids, [{From, Arg} | Waiters]),
            {noreply, {Pids, Waiters2}}
    end;
handle_call({ret_server, Pid}, _From, {Pids, Waiters}) ->
    [{Pid, _Arg, busy, _}] = ets:lookup(Pids, Pid),
    true = ets:update_element(Pids, Pid, [{3, idle}, {4, nil}]),
    % If we can start any waiters, do it.
    {ok, Waiters2} = start_waiters(Pids, Waiters),
    
    {reply, ok, {Pids, Waiters2}}.

handle_cast(_Mesg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Status}, {Pids, Waiters}) ->
    % If a server pid dies, remove it from the table.
    % If a client pid dies, release its server pid.
    case ets:lookup(Pids, Pid) of
        [{Pid, _, Running, _}] ->
            report_exit(Pid, Status, Running),
            true = ets:delete(Pids, Pid);
        _ ->
            case ets:match(Pids, {'$1', '_', '_', Pid}) of
                [] ->
                    report_exit(Pid, Status, unknown);
                PidList when is_list(PidList) ->
                    lists:foreach(fun(P) ->
                        ets:update_element(Pids, P, [{3, idle}, {4, nil}])
                    end, PidList)
            end
    end,
    
    % Free as many waiters as we can.
    {ok, Waiters2} = start_waiters(Pids, Waiters),
    {noreply, {Pids, Waiters2}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Start as many waiters as possible but prefer to start off
% the oldest waiter.
%
%  1. If the oldest waiter finds an idle pid, use that.
%  2. If there's room, start a new pid and use that.
%  3. If nothing idle is available, see if we can kill
%     an idle process for a different command (Arg). If
%     so, kill it and start a fresh process.
%  4. Check the next waiter.
start_waiters(Pids, Waiters) ->
    Waiters2 = lists:foldl(fun(Waiter, Acc) ->
        case start_waiters_filt(Pids, Waiter) of
            false -> Acc;
            _ -> [Waiter | Acc]
        end
    end, [], Waiters),
    cleanup_idle(Pids),
    {ok, Waiters2}.

% Trim inactive processes above the idle threshold.
cleanup_idle(Pids) ->
    NumBusy = length(ets:match(Pids, {'$1', '_', busy, '_'})),
    IdlePids = ets:match(Pids, {'$1', '_', idle, '_'}),
    NumIdle = length(IdlePids),
    
    lists:foldl(fun([Pid], Running) ->
        case Running > get(idle_pids) of
            true ->
                couch_os_process:stop(Pid),
                true = ets:update_element(Pids, Pid, [{3, dying}, {4, nil}]),
                Running-1;
            _ ->
                Running
        end
    end, NumBusy + NumIdle, IdlePids).

start_waiters_filt(Pids, {{Client, _}=Dest, Arg}=Waiter) ->
    case ets:match(Pids, {'$1', Arg, idle, nil}) of
        [[Pid] | _] ->
            true = ets:update_element(Pids, Pid, [{3, busy}, {4, Client}]),
            return_process(Dest, Pid),
            false;
        _ ->
            NumDying = length(ets:match(Pids, {'$1', '_', dying, '_'})),
            Room = get(max_pids) - (ets:info(Pids, size) - NumDying),
            case Room > 0 of
                true ->
                    start_process(Pids, Waiter),
                    false;
                _ ->
                    case ets:match(Pids, {'$1', '_', idle, nil}) of
                        [[Pid2] | _] ->
                            couch_os_process:stop(Pid2),
                            start_process(Pids, Waiter),
                            false;
                        _ ->
                            true
                    end
            end
    end.


start_process(Pids, {{ClientPid, _}=Client, Arg}) ->
    {ok, Pid} = couch_os_process:start_link(Arg),
    true = ets:insert(Pids, {Pid, Arg, busy, ClientPid}),
    return_process(Client, Pid).

return_process(Dest, Pid) ->
    ReduceLimit = list_to_atom(
        couch_config:get("query_server_config","reduce_limit","true")
    ),
    Config = [<<"configure">>, {[{<<"reduce_limit">>, ReduceLimit}]}],
    true = couch_os_process:prompt(Pid, Config),
    gen_server:reply(Dest, {ok, Pid}).

%report_exit(_Pid, _Status, dying) ->
%    % We closed this process on purpose to open up a slot for someone
%    % else.
%    ok;
%report_exit(Pid, normal, Running) ->
%    Fmt = "OS view server process exited normaly while ~p: ~p",
%    ?LOG_DEBUG(Fmt, [Running, Pid]);
report_exit(Pid, Status, Running) ->
    Fmt = "OS view server process exited abnormally while ~p: ~p (reason ~p)",
    ?LOG_INFO(Fmt, [Running, Pid, Status]).
