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

-module(couch_db_set).
-behaviour(gen_server).

% public API
-export([open/2, close/1]).
-export([get_seqs/1, get_seqs/2]).
-export([add_partitions/2, remove_partitions/2]).

% gen_server API
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-record(state, {
    set_name,
    db_refs,
    db_seqs,
    master_db,
    db_notifier
}).


open(SetName, Partitions) ->
    proc_lib:start_link(?MODULE, init, [{SetName, lists:usort(Partitions)}]).

close(Pid) ->
    ok = gen_server:call(Pid, close, infinity).

add_partitions(_Pid, []) ->
    ok;
add_partitions(Pid, Partitions) ->
    ok = gen_server:call(Pid, {add_partitions, Partitions}, infinity).

remove_partitions(_Pid, []) ->
    ok;
remove_partitions(Pid, Partitions) ->
    ok = gen_server:call(Pid, {remove_partitions, Partitions}, infinity).

get_seqs(Pid) ->
    get_seqs(Pid, nil).

get_seqs(Pid, nil) ->
    {ok, _Seqs} = gen_server:call(Pid, get_seqs, infinity);
get_seqs(Pid, FilterSortedSet) ->
    {ok, Seqs} = gen_server:call(Pid, get_seqs, infinity),
    {ok, [{P, S} || {P, S} <- Seqs, ordsets:is_element(P, FilterSortedSet)]}.


init({SetName, Partitions} = Args) ->
    {ok, State} = try
        do_init(Args)
    catch _:Error ->
        ?LOG_ERROR("Error opening database set `~s`: ~p~n"
                   "initial partitions:  ~w~n",
                   [SetName, Error, Partitions]),
        exit(Error)
    end,
    proc_lib:init_ack({ok, self()}),
    gen_server:enter_loop(?MODULE, [], State).

do_init({SetName, Partitions}) ->
    Server = self(),
    EventFun = fun({compacted, DbName} = Ev) ->
            case is_set_db(DbName, SetName) of
            true ->
                ok = gen_server:cast(Server, Ev);
            false ->
                ok
            end;
        ({updated, {DbName, _NewSeq}} = Ev) ->
            case is_set_db(DbName, SetName) of
            true ->
                ok = gen_server:cast(Server, Ev);
            false ->
                ok
            end;
        (_) ->
            ok
    end,
    MasterDb = case couch_db:open_int(?master_dbname(SetName), []) of
    {ok, MDb} ->
        MDb;
    Error ->
        raise_db_open_error(?master_dbname(SetName), Error)
    end,
    _Ref = couch_db:monitor(MasterDb),
    {DbRefs, DbSeqs} = lists:foldl(
        fun(PartId, {AccDbRefs, AccDbSeqs}) ->
            Name = ?dbname(SetName, PartId),
            case couch_db:open_int(Name, []) of
            {ok, Db} ->
                AccDbRefs2 = dict:store(
                    Db#db.name, {Db, couch_db:monitor(Db)}, AccDbRefs),
                AccDbSeqs2 = orddict:store(PartId, Db#db.update_seq, AccDbSeqs),
                {AccDbRefs2, AccDbSeqs2};
            Error2 ->
                raise_db_open_error(Name, Error2)
            end
        end,
        {dict:new(), orddict:new()}, Partitions),
    {ok, Notifier} = couch_db_update_notifier:start_link(EventFun),
    State = #state{
        set_name = SetName,
        master_db = MasterDb,
        db_notifier = Notifier,
        db_refs = DbRefs,
        db_seqs = DbSeqs
    },
    {ok, State}.


handle_call(get_seqs, _From, State) ->
    {reply, {ok, State#state.db_seqs}, State};

handle_call({add_partitions, PartList}, _From, State) ->
    {DbRefs2, DbSeqs2} = lists:foldl(
        fun(Id, {AccDbRefs, AccDbSeqs}) ->
            DbName = ?dbname((State#state.set_name), Id),
            case dict:find(DbName, AccDbRefs) of
            error ->
                {ok, Db} = couch_db:open_int(DbName, []),
                Ref = couch_db:monitor(Db),
                AccDbRefs2 = dict:store(DbName, {Db, Ref}, AccDbRefs),
                AccDbSeqs2 = orddict:store(Id, Db#db.update_seq, AccDbSeqs),
                {AccDbRefs2, AccDbSeqs2};
            {ok, {_Db, _Ref}} ->
                {AccDbRefs, AccDbSeqs}
            end
        end,
        {State#state.db_refs, State#state.db_seqs},
        PartList),
    {reply, ok, State#state{db_refs = DbRefs2, db_seqs = DbSeqs2}};

handle_call({remove_partitions, PartList}, _From, State) ->
    {DbRefs2, DbSeqs2} = lists:foldl(
        fun(Id, {AccDbRefs, AccDbSeqs}) ->
            DbName = ?dbname((State#state.set_name), Id),
            case dict:find(DbName, AccDbRefs) of
            error ->
                {AccDbRefs, AccDbSeqs};
            {ok, {Db, Ref}} ->
                erlang:demonitor(Ref, [flush]),
                catch couch_db:close(Db),
                {dict:erase(DbName, AccDbRefs), orddict:erase(Id, AccDbSeqs)}
            end
        end,
        {State#state.db_refs, State#state.db_seqs},
        PartList),
    {reply, ok, State#state{db_refs = DbRefs2, db_seqs = DbSeqs2}};

handle_call(close, _From, State) ->
    {stop, normal, ok, State}.


handle_cast({updated, {DbName, NewSeq}}, State) ->
    case dict:find(DbName, State#state.db_refs) of
    {ok, {_Db, _Ref}} ->
        {ok, PartId} = get_part_id(DbName),
        DbSeqs2 = orddict:store(PartId, NewSeq, State#state.db_seqs),
        {noreply, State#state{db_seqs = DbSeqs2}};
    error ->
        {noreply, State}
    end;

handle_cast({compacted, DbName}, State) ->
    case dict:find(DbName, State#state.db_refs) of
    error ->
        case DbName =:= couch_db:name(State#state.master_db) of
        false ->
            {noreply, State};
        true ->
            {ok, Db2} = couch_db:reopen(State#state.master_db),
            {noreply, State#state{master_db = Db2}}
        end;
    {ok, {Db, Ref}} ->
        {ok, Db2} = couch_db:reopen(Db),
        DbRefs2 = dict:store(DbName, {Db2, Ref}, State#state.db_refs),
        {noreply, State#state{db_refs = DbRefs2}}
    end.


handle_info({'DOWN', _Ref, _, Pid, Reason}, State) ->
    ?LOG_INFO("Shutting down couch_db_set ~p for set `~s` because partition `~s` "
        "died with reason: ~p",
        [self(), State#state.set_name, get_db_name(Pid, State), Reason]),
    {stop, Reason, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, State) ->
    couch_db_update_notifier:stop(State#state.db_notifier).


raise_db_open_error(DbName, Error) ->
    Msg = io_lib:format("Couldn't open database `~s`, reason: ~w", [DbName, Error]),
    throw({db_open_error, DbName, Error, iolist_to_binary(Msg)}).


get_db_name(Pid, #state{master_db = #db{main_pid = Pid} = MasterDb}) ->
    MasterDb#db.name;
get_db_name(Pid, #state{db_refs = DbRefs}) ->
    find_db_name(Pid, dict:to_list(DbRefs)).

find_db_name(_Pid, []) ->
    undefined;
find_db_name(Pid, [{Name, {#db{main_pid = Pid}, _}} | _]) ->
    Name;
find_db_name(Pid, [_ | Rest]) ->
    find_db_name(Pid, Rest).


is_set_db(DbName, SetName) ->
    Sz = byte_size(SetName),
    case DbName of
    <<SetName:Sz/binary, $/, _/binary>> ->
        true;
    _ ->
        false
    end.


get_part_id(DbName) ->
    case string:tokens(?b2l(DbName), "/") of
    [_, PartIdList] ->
        PartId = (catch list_to_integer(PartIdList)),
        case is_integer(PartId) andalso (PartId >= 0) of
        true ->
            {ok, PartId};
        false ->
            error
        end;
    _ ->
        error
    end.
