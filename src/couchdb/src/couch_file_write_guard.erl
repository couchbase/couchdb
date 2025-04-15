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

-module(couch_file_write_guard).
-behaviour(gen_server).

-include("couch_db.hrl").

% This module is a lot like a supervisor, but it's purpose isn't to keep files
% open but rather prevent duplicate couch_file writers from being opened on
% the same file, to prevent multiple process from performing uncoordinated
% writes.
% Without this module, it's possible that a not fully killed process tree
% might be recreated and start writing while some dieing child processes
% will still be writing to the file. Also it's possibe for a race 
% condition bug to do the same.
% We don't use OTP supervisors as they scale poorly for lots of child
% starts/stops (linear scans for child specs)

-export([add/2, remove/1, disable_for_testing/0]).
-export([init/1, handle_call/3, sup_start_link/0]).
-export([handle_cast/2, code_change/3, handle_info/2, terminate/2]).

add(Filepath, Pid) ->
    gen_server:call(couch_file_write_guard, {add, Filepath, Pid}, infinity).


remove(Pid) ->
    gen_server:call(couch_file_write_guard, {remove, Pid}, infinity).


disable_for_testing() ->
    gen_server:call(couch_file_write_guard, disable_for_testing, infinity).


sup_start_link() ->
    gen_server:start_link({local,couch_file_write_guard},couch_file_write_guard,[],[]).


init([]) ->
    ets:new(couch_files_by_name, [set, private, named_table]),
    ets:new(couch_files_by_pid, [set, private, named_table]),
    {ok, true}.


ets_add(Filepath, Pid) ->
    case ets:insert_new(couch_files_by_name, {Filepath, Pid}) of
    true ->
        Ref = erlang:monitor(process, Pid),
        true = ets:insert_new(couch_files_by_pid, {Pid, Filepath, Ref}),
        ok;
    false ->
        already_added_to_file_write_guard
    end.


ets_remove(Pid) ->
    case ets:lookup(couch_files_by_pid, Pid) of
    [{Pid, Filepath, Ref}] ->
        true = demonitor(Ref, [flush]),
        true = ets:delete(couch_files_by_name, Filepath),
        true = ets:delete(couch_files_by_pid, Pid),
        {reply, ok, true};
    _ ->
        {reply, removing_unadded_file, true}
        end.


terminate(_Reason, _Srv) ->
    % kill all files we are guarding, then wait for their 'DOWN'
    [exit(Pid, kill) || {_, Pid} <-
            ets:tab2list(couch_files_by_name)],
    [receive {'DOWN', _MonRef, _Type, Pid, _} -> ok end || {_, Pid} <-
            ets:tab2list(couch_files_by_name)],
    ok.


handle_call({add, Filepath, Pid}, _From, true) ->
    case ets_add(Filepath, Pid) of
    ok ->
        {reply, ok, true};
    already_added_to_file_write_guard ->
        [{Filepath, ExistingPid}] = ets:lookup(couch_files_by_name, Filepath),
        case is_process_alive(ExistingPid) of
        true ->
            ?LOG_ERROR("Unable to add new writer pid: `~p` for `~s` as there"
                       " is already an active writer process pid: `~p`",
                       [ExistingPid, Filepath, Pid]),
            {reply, already_added_to_file_write_guard, true};
        false ->
            {reply, ok, true} = ets_remove(ExistingPid),
            ok = ets_add(Filepath, Pid),
            {reply, ok, true}
        end
    end;
handle_call({add, _Filepath, _Pid}, _From, false) ->
    % no-op for testing
    {reply, ok, false};
handle_call({remove, Pid}, _From, true) ->
    ets_remove(Pid);
handle_call({remove, _Pid}, _From, false) ->
    % no-op for testing
    {reply, ok, false};
handle_call(disable_for_testing, _From, _State) ->
    {reply, ok, false}.


handle_cast(Msg, _Server) ->
    exit({unknown_cast_message, Msg}).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_info({'DOWN', MonRef, _Type, Pid, Reason}, true) ->
    case ets:lookup(couch_files_by_pid, Pid) of
    [{Pid, Filepath, MonRef}] ->
        true = ets:delete(couch_files_by_name, Filepath),
        true = ets:delete(couch_files_by_pid, Pid),
        {noreply, true};
    _ ->
        ?LOG_ERROR("Unexpected down message in couch_file_write_guard: ~p",
                [Reason]),
        exit(shutdown)
    end;
handle_info({'DOWN', _MonRef, _Type, _Pid, _Reason}, false) ->
    % no-op for testing
    {noreply, false}.

