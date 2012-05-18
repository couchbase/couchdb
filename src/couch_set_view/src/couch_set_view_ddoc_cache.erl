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

-module(couch_set_view_ddoc_cache).
-behaviour(gen_server).

% public API
-export([start_link/0]).
-export([get_ddoc/2]).

% gen_server API
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-define(BY_DDOC_ID, set_view_by_ddoc_id_ets).
-define(BY_ATIME, set_view_by_atime_ets).

-record(state, {
    max_cache_size = 0,
    byte_size = 0,
    db_notifier = nil
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


get_ddoc(SetName, DDocId) ->
    case ets:lookup(?BY_DDOC_ID, {SetName, DDocId}) of
    [{_, _ATime, DDoc, _DDocSize}] ->
        ok = gen_server:cast(?MODULE, {cache_hit, SetName, DDocId}),
        {ok, DDoc};
    [] ->
        case couch_db:open_int(?master_dbname(SetName), []) of
        {ok, Db} ->
            try
                case couch_db:open_doc(Db, DDocId, [ejson_body]) of
                {ok, DDoc} ->
                    Size = erlang:external_size(DDoc),
                    ok = gen_server:call(?MODULE, {add_ddoc, SetName, DDoc, Size}, infinity),
                    {ok, DDoc};
                DocOpenError ->
                    {doc_open_error, DocOpenError}
                end
            after
                ok = couch_db:close(Db)
            end;
        DbOpenError ->
            {db_open_error, DbOpenError}
        end
    end.


init(_) ->
    ?BY_DDOC_ID = ets:new(?BY_DDOC_ID,
                          [set, protected, named_table, {read_concurrency, true}]),
    ?BY_ATIME = ets:new(?BY_ATIME,
                        [ordered_set, private, named_table]),
    process_flag(trap_exit, true),
    ok = couch_config:register(fun handle_config_change/3),
    {ok, Notifier} = couch_db_update_notifier:start_link(fun handle_db_event/1),
    MaxSize = couch_config:get("set_views", "ddoc_cache_size", "1048576"),
    State = #state{
        db_notifier = Notifier,
        max_cache_size = list_to_integer(MaxSize)
    },
    {ok, State}.


handle_call({add_ddoc, SetName, DDoc, DDocSize}, From, State) ->
    gen_server:reply(From, ok),
    #doc{id = Id, rev = Rev} = DDoc,
    Key = {SetName, Id},
    case ets:lookup(?BY_DDOC_ID, Key) of
    [] ->
        Now = os:timestamp(),
        true = ets:insert(?BY_ATIME, {Now, Key, DDocSize}),
        true = ets:insert(?BY_DDOC_ID, {Key, Now, DDoc, DDocSize}),
        NewSize = State#state.byte_size + DDocSize;
    [{_, _ATime, #doc{rev = OldRev}, _OldDDocSize}] when OldRev > Rev ->
        NewSize = State#state.byte_size;
    [{_, ATime, _OldDDoc, OldDDocSize}] ->
        Now = os:timestamp(),
        true = ets:delete(?BY_ATIME, ATime),
        true = ets:insert(?BY_ATIME, {Now, Key, DDocSize}),
        true = ets:insert(?BY_DDOC_ID, {Key, Now, DDoc, DDocSize}),
        NewSize = State#state.byte_size + DDocSize - OldDDocSize
    end,
    CacheSize = free_old_entries(State#state.max_cache_size, NewSize),
    {noreply, State#state{byte_size = CacheSize}};

handle_call({update_ddoc, SetName, DDoc, DDocSize}, From, State) ->
    gen_server:reply(From, ok),
    #doc{id = Id, rev = Rev} = DDoc,
    Key = {SetName, Id},
    case ets:lookup(?BY_DDOC_ID, Key) of
    [] ->
        {noreply, State};
    [{_, _ATime, #doc{rev = OldRev}, _OldDDocSize}] when OldRev > Rev ->
        {noreply, State};
    [{_, ATime, _OldDDoc, OldDDocSize}] ->
        % ddoc update, using current access time stamp
        true = ets:update_element(?BY_ATIME, ATime, {3, DDocSize}),
        true = ets:insert(?BY_DDOC_ID, {Key, ATime, DDoc, DDocSize}),
        NewSize = State#state.byte_size + DDocSize - OldDDocSize,
        CacheSize = free_old_entries(State#state.max_cache_size, NewSize),
        {noreply, State#state{byte_size = CacheSize}}
    end;

handle_call({delete_ddoc, SetName, Id}, From, State) ->
    gen_server:reply(From, ok),
    Key = {SetName, Id},
    case ets:lookup(?BY_DDOC_ID, Key) of
    [] ->
        {noreply, State};
    [{_, ATime, _DDoc, DDocSize}] ->
        true = ets:delete(?BY_DDOC_ID, Key),
        true = ets:delete(?BY_ATIME, ATime),
        NewSize = State#state.byte_size - DDocSize,
        {noreply, State#state{byte_size = NewSize}}
    end;

handle_call({set_deleted, SetName}, From, State) ->
    gen_server:reply(From, ok),
    Entries = ets:match_object(?BY_DDOC_ID, {{SetName, '_'}, '_', '_'}),
    lists:foreach(fun({Key, ATime, _DDoc, _DDocSize}) ->
        true = ets:delete(?BY_DDOC_ID, Key),
        true = ets:delete(?BY_ATIME, ATime)
    end, Entries),
    {noreply, State};

handle_call({new_max_cache_size, NewMaxSize}, _From, State) ->
    Size = free_old_entries(NewMaxSize, State#state.byte_size),
    {reply, ok, State#state{byte_size = Size}}.



handle_cast({cache_hit, SetName, DDocId}, State) ->
    Key = {SetName, DDocId},
    case ets:lookup(?BY_DDOC_ID, Key) of
    [] ->
        ok;
    [{_, OldATime, _DDoc, DDocSize}] ->
        NewATime = os:timestamp(),
        true = ets:delete(?BY_ATIME, OldATime),
        true = ets:insert(?BY_ATIME, {NewATime, Key, DDocSize}),
        true = ets:update_element(?BY_DDOC_ID, Key, {2, NewATime})
    end,
    {noreply, State}.


handle_info(shutdown, State) ->
    {stop, shutdown, State}.


terminate(_Reason, #state{db_notifier = Notifier}) ->
    couch_db_update_notifier:stop(Notifier),
    true = ets:delete(?BY_DDOC_ID),
    true = ets:delete(?BY_ATIME),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


free_old_entries(MaxSize, CurSize) when CurSize =< MaxSize ->
    CurSize;
free_old_entries(MaxSize, CurSize) ->
    ATime = ets:first(?BY_ATIME),
    [{_, {_SetName, _DDocId} = Key, DDocSize}] = ets:lookup(?BY_ATIME, ATime),
    true = ets:delete(?BY_ATIME, ATime),
    true = ets:delete(?BY_DDOC_ID, Key),
    free_old_entries(MaxSize, CurSize - DDocSize).


handle_config_change("set_views", "ddoc_cache_size", NewSizeList) ->
    NewSize = list_to_integer(NewSizeList),
    ok = gen_server:call(?MODULE, {new_max_cache_size, NewSize}, infinity).


handle_db_event({deleted, DbName}) ->
    case string:tokens(?b2l(DbName), "/") of
    [SetNameList, "master"] ->
        ok = gen_server:call(?MODULE, {set_deleted, ?l2b(SetNameList)}, infinity);
    _ ->
        ok
    end;
handle_db_event({ddoc_updated, {DbName, Id}}) ->
    case string:tokens(?b2l(DbName), "/") of
    [SetNameList, "master"] ->
        SetName = ?l2b(SetNameList),
        case couch_db:open_int(DbName, []) of
        {ok, Db} ->
            try
                case couch_db:open_doc(Db, Id, [ejson_body]) of
                {ok, Doc} ->
                    Size = erlang:external_size(Doc),
                    ok = gen_server:call(?MODULE, {update_ddoc, SetName, Doc, Size}, infinity);
                _ ->
                    % Maybe ddoc got deleted in the meanwhile. If not a subsequent request
                    % will add it again to the cache. This approach make code simpler.
                    ok = gen_server:call(?MODULE, {delete_ddoc, SetName, Id}, infinity)
                end
            after
                ok = couch_db:close(Db)
            end;
        _ ->
            % Maybe db just got deleted, maybe we run out of file descriptors, etc.
            % Just let future cache misses populate again the cache, this makes it
            % simpler for an uncommon case.
            ok = gen_server:call(?MODULE, {delete_set, SetName}, infinity)
        end;
    _ ->
        ok
    end;
handle_db_event({ddoc_deleted, {DbName, Id}}) ->
    case string:tokens(?b2l(DbName), "/") of
    [SetNameList, "master"] ->
        ok = gen_server:call(?MODULE, {delete_ddoc, ?l2b(SetNameList), Id}, infinity);
    _ ->
        ok
    end;
handle_db_event(_) ->
    ok.
