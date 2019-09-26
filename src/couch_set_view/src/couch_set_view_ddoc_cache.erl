% -*- Mode: Erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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
-export([get_ddoc/2, get_view/3]).

% gen_server API
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-define(BY_DDOC_ID, set_view_by_ddoc_id_ets).
-define(BY_ATIME, set_view_by_atime_ets).
-define(BY_VIEW_ID, set_view_by_view_id_ets).
-define(BY_ATIME_VIEW, set_view_by_atime_view_ets).

-record(state, {
    view_caching_enabled = false,
    cached_view_size = 0,
    max_cache_size = 0,
    byte_size = 0,
    db_notifier = nil
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

view_caching_enabled() ->
    gen_server:call(?MODULE, view_caching_enabled).

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

get_view(undefined, _, _) ->
    undefined;
get_view(_, undefined, _) ->
    undefined;
get_view(_, _, undefined) ->
    undefined;
get_view(SetName, DDocId, ViewId) ->
    case view_caching_enabled() of
    true ->
        case ets:lookup(?BY_VIEW_ID, {SetName, DDocId, ViewId}) of
        [{_, _ATime, View, _DDocSize}] ->
            ok = gen_server:cast(?MODULE, {cache_hit_view, SetName, DDocId, ViewId}),
            View;
        [] ->
            case get_ddoc(SetName, DDocId) of
            {ok, DDoc} ->
                try
                    ViewList = couch_util:get_view_list(DDoc#doc.body),
                    View = couch_util:get_value(ViewId, ViewList),
                    ok = gen_server:call(?MODULE, {add_view, SetName, DDocId, ViewId, View}, infinity),
                    View
                catch
                    _:_ ->
                        undefined
                end;
            _ ->
                undefined
            end;
        _ ->
            undefined
        end;
    _ ->
        undefined
    end.

init(_) ->
    ?BY_DDOC_ID = ets:new(?BY_DDOC_ID,
                          [set, protected, named_table, {read_concurrency, true}]),
    ?BY_ATIME = ets:new(?BY_ATIME,
                        [ordered_set, private, named_table]),
    ?BY_VIEW_ID = ets:new(?BY_VIEW_ID,
                          [set, protected, named_table, {read_concurrency, true}]),
    ?BY_ATIME_VIEW = ets:new(?BY_ATIME_VIEW,
                        [ordered_set, private, named_table]),
    process_flag(trap_exit, true),
    ok = couch_config:register(fun handle_config_change/3),
    {ok, Notifier} = couch_db_update_notifier:start_link(fun handle_db_event/1),
    MaxSize = couch_config:get("set_views", "ddoc_cache_size", "1048576"),
    CacheViews = couch_config:get("set_views", "cache_views_def", "false"),
    State = #state{
        view_caching_enabled = list_to_atom(CacheViews),
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
    CacheSize = free_old_entries(State#state.max_cache_size, NewSize, ?BY_ATIME, ?BY_DDOC_ID),
    {noreply, State#state{byte_size = CacheSize}};

handle_call({add_view, SetName, DDocId, ViewId, View}, From, State) ->
    gen_server:reply(From, ok),
    ViewSize = erlang:external_size(View),
    Key = {SetName, DDocId, ViewId},
    case ets:lookup(?BY_VIEW_ID, Key) of
    [] ->
        Now = os:timestamp(),
        true = ets:insert(?BY_ATIME_VIEW, {Now, Key, ViewSize}),
        true = ets:insert(?BY_VIEW_ID, {Key, Now, View, ViewSize}),
        NewSize = State#state.cached_view_size + ViewSize;
    [{_, ATime, _View, OldViewSize}] ->
        Now = os:timestamp(),
        true = ets:delete(?BY_ATIME_VIEW, ATime),
        true = ets:insert(?BY_ATIME_VIEW, {Now, Key, OldViewSize}),
        true = ets:insert(?BY_VIEW_ID, {Key, Now, View, ViewSize}),
        NewSize = State#state.cached_view_size + ViewSize - OldViewSize
    end,
    CacheSize = free_old_entries(State#state.max_cache_size, NewSize, ?BY_ATIME_VIEW, ?BY_VIEW_ID),
    {noreply, State#state{cached_view_size = CacheSize}};

handle_call(view_caching_enabled, _From, State) ->
    {reply, State#state.view_caching_enabled, State};

handle_call({update_ddoc, SetName, DDoc, DDocSize}, From, State) ->
    gen_server:reply(From, ok),
    Key = {SetName, DDoc#doc.id},
    case ets:lookup(?BY_DDOC_ID, Key) of
    [] ->
        {noreply, State};
    [{_, ATime, _OldDDoc, OldDDocSize}] ->
        % ddoc update, using current access time stamp
        true = ets:insert(?BY_ATIME, {ATime, Key, DDocSize}),
        true = ets:insert(?BY_DDOC_ID, {Key, ATime, DDoc, DDocSize}),
        NewSize = State#state.byte_size + DDocSize - OldDDocSize,
        CacheSize = free_old_entries(State#state.max_cache_size, NewSize, ?BY_ATIME, ?BY_DDOC_ID),
        {noreply, State#state{byte_size = CacheSize}}
    end;

handle_call({update_view, SetName, DDoc}, From, State) ->
    gen_server:reply(From, ok),
    Entries = ets:match_object(?BY_VIEW_ID, {{SetName, DDoc#doc.id, '_'}, '_', '_', '_'}),
    NewViewSize = lists:foldl(fun({Key, ATime, _, OldViewSize}, NewSize) ->
        true = ets:delete(?BY_VIEW_ID, Key),
        true = ets:delete(?BY_ATIME_VIEW, ATime),
        NewSize - OldViewSize
    end, State#state.cached_view_size, Entries),
    {noreply, State#state{cached_view_size = NewViewSize}};

handle_call({delete_ddoc, SetName, Id}, From, State) ->
    gen_server:reply(From, ok),
    NewViewSize = delete_matched_object_view(SetName, Id, '_', State#state.cached_view_size),

    Key = {SetName, Id},
    case ets:lookup(?BY_DDOC_ID, Key) of
    [] ->
        {noreply, State#state{cached_view_size = NewViewSize}};
    [{_, ATime, _DDoc, DDocSize}] ->
        true = ets:delete(?BY_DDOC_ID, Key),
        true = ets:delete(?BY_ATIME, ATime),
        NewSize = State#state.byte_size - DDocSize,
        {noreply, State#state{byte_size = NewSize,
                              cached_view_size = NewViewSize}}
    end;

handle_call({set_deleted, SetName}, From, State) ->
    gen_server:reply(From, ok),
    Entries = ets:match_object(?BY_DDOC_ID, {{SetName, '_'}, '_', '_', '_'}),
    lists:foreach(fun({Key, ATime, _DDoc, _DDocSize}) ->
        true = ets:delete(?BY_DDOC_ID, Key),
        true = ets:delete(?BY_ATIME, ATime)
    end, Entries),

    NewSize = delete_matched_object_view(SetName, '_', '_', State#state.cached_view_size),
    {noreply, State#state{cached_view_size = NewSize}};

handle_call({new_max_cache_size, NewMaxSize}, _From, State) ->
    DDocSize = free_old_entries(NewMaxSize, State#state.byte_size, ?BY_ATIME, ?BY_DDOC_ID),
    ViewSize = free_old_entries(NewMaxSize, State#state.cached_view_size, ?BY_ATIME_VIEW, ?BY_VIEW_ID),
    {reply, ok, State#state{byte_size = DDocSize, cached_view_size = ViewSize}};

handle_call({new_view_cache_setting, Value}, _From, State) ->
    State2 = case Value of
    false ->
        ets:delete_all_objects(?BY_ATIME_VIEW),
        ets:delete_all_objects(?BY_VIEW_ID),
        State#state{view_caching_enabled = false, cached_view_size = 0};
    true ->
        State#state{view_caching_enabled = true};
    _ ->
        State
    end,
    {reply, ok, State2}.

handle_cast({cache_hit, SetName, DDocId}, State) ->
    Key = {SetName, DDocId},
    case ets:lookup(?BY_DDOC_ID, Key) of
    [] ->
        ok;
    [{_, OldATime, DDoc, DDocSize}] ->
        NewATime = os:timestamp(),
        true = ets:delete(?BY_ATIME, OldATime),
        true = ets:insert(?BY_ATIME, {NewATime, Key, DDocSize}),
        true = ets:insert(?BY_DDOC_ID, {Key, NewATime, DDoc, DDocSize})
    end,
    {noreply, State};
handle_cast({cache_hit_view, SetName, DDocId, ViewId}, State) ->
    Key = {SetName, DDocId, ViewId},
    case ets:lookup(?BY_VIEW_ID, Key) of
    [] ->
        ok;
    [{_, OldATime, View, ViewSize}] ->
        NewATime = os:timestamp(),
        true = ets:delete(?BY_ATIME_VIEW, OldATime),
        true = ets:insert(?BY_ATIME_VIEW, {NewATime, Key, ViewSize}),
        true = ets:insert(?BY_VIEW_ID, {Key, NewATime, View, ViewSize})
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


free_old_entries(MaxSize, CurSize, _, _) when CurSize =< MaxSize ->
    CurSize;
free_old_entries(MaxSize, CurSize, AtimeTable, IdTable) ->
    ATime = ets:first(AtimeTable),
    [{_, Key, Size}] = ets:lookup(AtimeTable, ATime),
    true = ets:delete(AtimeTable, ATime),
    true = ets:delete(IdTable, Key),
    free_old_entries(MaxSize, CurSize - Size, AtimeTable, IdTable).


handle_config_change("set_views", "ddoc_cache_size", NewSizeList) ->
    NewSize = list_to_integer(NewSizeList),
    ok = gen_server:call(?MODULE, {new_max_cache_size, NewSize}, infinity);
handle_config_change("set_views", "cache_views_def", Value) ->
    Value2 = list_to_atom(Value),
    ok = gen_server:call(?MODULE, {new_view_cache_setting, Value2}, infinity).

handle_db_event({deleted, DbName}) ->
    case couch_set_view_util:split_set_db_name(DbName) of
    {ok, SetName, master} ->
        ok = gen_server:call(?MODULE, {set_deleted, SetName}, infinity);
    _ ->
        ok
    end;
handle_db_event({ddoc_updated, {DbName, DDoc}}) ->
    case couch_set_view_util:split_set_db_name(DbName) of
    {ok, SetName, master} ->
        case DDoc#doc.deleted of
        false ->
            DDoc2 = couch_doc:with_ejson_body(DDoc),
            Size = erlang:external_size(DDoc2),
            ok = gen_server:call(?MODULE, {update_ddoc, SetName, DDoc2, Size}, infinity),
            ok = gen_server:call(?MODULE, {update_view, SetName, DDoc2}, infinity);
        true ->
            ok = gen_server:call(?MODULE, {delete_ddoc, SetName, DDoc#doc.id}, infinity)
        end;
    _ ->
        ok
    end;
handle_db_event(_) ->
    ok.

delete_matched_object_view(SetName, DDocId, ViewId, CurrValue) ->
    Entries = ets:match_object(?BY_VIEW_ID, {{SetName, DDocId, ViewId}, '_', '_', '_'}),
    lists:foldl(fun({Key, ATime, _, OldViewSize}, NewSize) ->
        true = ets:delete(?BY_VIEW_ID, Key),
        true = ets:delete(?BY_ATIME_VIEW, ATime),
        NewSize - OldViewSize
    end, CurrValue, Entries).
