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

-export([add/2, remove/1]).
-export([init/1, handle_call/3, sup_start_link/0]).
-export([handle_cast/2, code_change/3, handle_info/2, terminate/2]).

add(Filepath, Pid) ->
    gen_server:call(couch_file_write_guard, {add, Filepath, Pid}, infinity).


remove(Pid) ->
    gen_server:call(couch_file_write_guard, {remove, Pid}, infinity).


sup_start_link() ->
    gen_server:start_link({local,couch_file_write_guard},couch_file_write_guard,[],[]).


init([]) ->
    ets:new(couch_files_by_name, [set, private, named_table]),
    ets:new(couch_files_by_pid, [set, private, named_table]),
    {ok, ok}.


terminate(_Reason, _Srv) ->
    % kill all files we are guarding, then wait for their 'DOWN'
    [exit(Pid, kill) || {_, Pid} <-
            ets:tab2list(couch_files_by_name)],
    [receive {'DOWN', _MonRef, _Type, Pid, _Reason} -> ok end || {_, Pid} <-
            ets:tab2list(couch_files_by_name)],
    ok.


handle_call({add, Filepath, Pid}, _From, Server) ->
    case ets:insert_new(couch_files_by_name, {Filepath, Pid}) of
    true ->
        Ref = erlang:monitor(process, Pid),
        true = ets:insert_new(couch_files_by_pid, {Pid, Filepath, Ref}),
        {reply, ok, Server};
    false ->
        {reply, already_added_to_file_write_guard, Server}
    end;
handle_call({remove, Pid}, _From, Server) ->
    case ets:lookup(couch_files_by_pid, Pid) of
    [{Pid, Filepath, Ref}] ->
        true = demonitor(Ref, [flush]),
        true = ets:delete(couch_files_by_name, Filepath),
        true = ets:delete(couch_files_by_pid, Pid),
        {reply, ok, Server};
    _ ->
        {reply, removing_unadded_file, Server}
    end.


handle_cast(Msg, _Server) ->
    exit({unknown_cast_message, Msg}).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_info({'DOWN', MonRef, _Type, Pid, Reason}, Server) ->
    case ets:lookup(couch_files_by_pid, Pid) of
    [{Pid, Filepath, MonRef}] ->
        true = ets:delete(couch_files_by_name, Filepath),
        true = ets:delete(couch_files_by_pid, Pid),
        {noreply, Server};
    _ ->
        ?LOG_ERROR("Unexpected down message in couch_file_write_guard: ~p",
                [Reason]),
        exit(shutdown)
    end.

