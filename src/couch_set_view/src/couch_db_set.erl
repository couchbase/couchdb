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
-export([open/4, close/1]).
-export([get_seqs/1]).
-export([set_active/2, set_passive/2, remove_partitions/2]).

% gen_server API
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-record(state, {
    set_name,
    dbs_active,
    dbs_passive,
    master_db,
    db_notifier,
    db_open_options
}).


open(SetName, Active, Passive, DbOpenOptions) ->
    Args = {SetName, Active, Passive, DbOpenOptions},
    proc_lib:start_link(?MODULE, init, [Args]).

close(Pid) ->
    ok = gen_server:call(Pid, close, infinity).

set_active(_Pid, []) ->
    ok;
set_active(Pid, PartList) ->
    ok = gen_server:call(Pid, {set_active, PartList}, infinity).

set_passive(_Pid, []) ->
    ok;
set_passive(Pid, PartList) ->
    ok = gen_server:call(Pid, {set_passive, PartList}, infinity).

remove_partitions(_Pid, []) ->
    ok;
remove_partitions(Pid, PartList) ->
    ok = gen_server:call(Pid, {remove_partitions, PartList}, infinity).

get_seqs(Pid) ->
    {ok, Seqs} = gen_server:call(Pid, get_seqs, infinity),
    {ok, lists:keysort(1, Seqs)}.


init({SetName, Active, Passive, _} = Args) ->
    try
        {ok, State} = do_init(Args),
        proc_lib:init_ack({ok, self()}),
        gen_server:enter_loop(?MODULE, [], State)
    catch _:Error ->
        ?LOG_ERROR("Error opening database set `~s`: ~p~n"
            "initial active partitions:  ~w~n"
            "initial passive partitions: ~w~n",
            [SetName, Error, Active, Passive]),
        exit(Error)
    end.

do_init({SetName, Active0, Passive0, DbOpenOptions}) ->
    Active = lists:usort(Active0),
    Passive = lists:usort(Passive0),
    OpenFun = fun(P, Acc) ->
        Name = ?dbname(SetName, P),
        case couch_db:open(Name, DbOpenOptions) of
        {ok, Db} ->
            dict:store(Name, {Db, P, couch_db:monitor(Db)}, Acc);
        Error ->
            raise_db_open_error(Name, Error)
        end
    end,
    DbsActive = lists:foldl(OpenFun, dict:new(), Active),
    DbsPassive = lists:foldl(OpenFun, dict:new(), Passive),
    MasterDb = case couch_db:open_int(?master_dbname(SetName), []) of
    {ok, Db} ->
        Db;
    Error ->
        raise_db_open_error(?master_dbname(SetName), Error)
    end,
    _Ref = couch_db:monitor(MasterDb),
    Server = self(),
    EventFun = fun({compacted, Name} = Ev) ->
            Sz = byte_size(SetName),
            case Name of
            <<SetName:Sz/binary, $/, _/binary>> ->
                ok = gen_server:cast(Server, Ev);
            _ ->
                ok
            end;
        (_) ->
            ok
    end,
    {ok, Notifier} = couch_db_update_notifier:start_link(EventFun),
    State = #state{
        set_name = SetName,
        dbs_active = DbsActive,
        dbs_passive = DbsPassive,
        master_db = MasterDb,
        db_notifier = Notifier,
        db_open_options = DbOpenOptions
    },
    {ok, State}.


handle_call(get_seqs, _From, State) ->
    FoldFun = fun(_DbName, {Db, P, _Ref}, Acc) ->
        {ok, S} = couch_db:get_current_seq(Db),
        [{P, S} | Acc]              
    end,
    Seqs0 = dict:fold(FoldFun, [], State#state.dbs_active),
    Seqs = dict:fold(FoldFun, Seqs0, State#state.dbs_passive),
    {reply, {ok, Seqs}, State};

handle_call(get_dbs, _From, State) ->
    FoldFun = fun(_, {Db, P, _Ref}, A) -> [{P, Db#db.name} | A] end,
    Active = dict:fold(FoldFun, [], State#state.dbs_active),
    Passive = dict:fold(FoldFun, [], State#state.dbs_passive),
    UserCtx = case Active of
    [{_, DbName} | _] ->
        {Db, _, _} = dict:fetch(DbName, State#state.dbs_active),
        Db#db.user_ctx;
    [] ->
        case Passive of
        [{_, DbName} | _] ->
            {Db, _, _} = dict:fetch(DbName, State#state.dbs_passive),
            Db#db.user_ctx;
        [] ->
            couch_util:get_value(user_ctx, State#state.db_open_options, #user_ctx{})
        end
    end,
    {reply, {ok, Active, Passive, UserCtx}, State};

handle_call({set_passive, PartList}, _From, State) ->
    #state{
        db_open_options = OpenOpts,
        set_name = SetName,
        dbs_active = Active0,
        dbs_passive = Passive0
    } = State,
    {Active, Passive} = switch_partitions_state(
        PartList, SetName, OpenOpts, Active0, Passive0),
    {reply, ok, State#state{dbs_active = Active, dbs_passive = Passive}};

handle_call({set_active, PartList}, _From, State) ->
    #state{
        db_open_options = OpenOpts,
        set_name = SetName,
        dbs_active = Active0,
        dbs_passive = Passive0
    } = State,
    {Passive, Active} = switch_partitions_state(
        PartList, SetName, OpenOpts, Passive0, Active0),
    {reply, ok, State#state{dbs_active = Active, dbs_passive = Passive}};

handle_call({remove_partitions, PartList}, _From, State) ->
    {Active, Passive} = lists:foldl(
        fun(Id, {A, P}) ->
            DbName = ?dbname((State#state.set_name), Id),
            case dict:find(DbName, A) of
            error ->
                case dict:find(DbName, P) of
                error ->
                    {A, P};
                {ok, {Db, Id, Ref}} ->
                    erlang:demonitor(Ref, [flush]),
                    catch couch_db:close(Db),
                    {A, dict:erase(DbName, P)}
                end;
            {ok, {Db, Id, Ref}} ->
                erlang:demonitor(Ref, [flush]),
                catch couch_db:close(Db),
                {dict:erase(DbName, A), P}
            end
        end,
        {State#state.dbs_active, State#state.dbs_passive},
        PartList),
    {reply, ok, State#state{dbs_active = Active, dbs_passive = Passive}};

handle_call(close, _From, State) ->
    {stop, normal, ok, State}.


handle_cast({compacted, DbName}, State) ->
    case dict:find(DbName, State#state.dbs_active) of
    error ->
        case dict:find(DbName, State#state.dbs_passive) of
        error ->
            case DbName =:= couch_db:name(State#state.master_db) of
            false ->
                {noreply, State};
            true ->
                {ok, Db2} = couch_db:reopen(State#state.master_db),
                {noreply, State#state{master_db = Db2}}
            end;
        {ok, {Db, P, Ref}} ->
            {ok, Db2} = couch_db:reopen(Db),
            DbsPassive = dict:store(DbName, {Db2, P, Ref}, State#state.dbs_passive),
            {noreply, State#state{dbs_passive = DbsPassive}}
        end;
    {ok, {Db, P, Ref}} ->
        {ok, Db2} = couch_db:reopen(Db),
        DbsActive = dict:store(DbName, {Db2, P, Ref}, State#state.dbs_active),
        {noreply, State#state{dbs_active = DbsActive}}
    end.


handle_info({'DOWN', _Ref, _, _, Reason}, State) ->
    ?LOG_INFO("Shutting down couch_db_set for set `~s` because a partition "
              "died with reason: ~p", [State#state.set_name, Reason]),
    {stop, Reason, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, State) ->
    couch_db_update_notifier:stop(State#state.db_notifier).


switch_partitions_state(PartList, SetName, OpenOpts, SetA, SetB) ->
    lists:foldl(
        fun(Id, {A, B}) ->
            DbName = ?dbname(SetName, Id),
            case dict:find(DbName, A) of
            error ->
                 case dict:find(DbName, B) of
                 error ->
                     {ok, Db} = couch_db:open(DbName, OpenOpts),
                     Ref = couch_db:monitor(Db),
                     {A, dict:store(DbName, {Db, Id, Ref}, B)};
                 {ok, _} ->
                     {A, B}
                 end;
            {ok, Val} ->
                {dict:erase(DbName, A), dict:store(DbName, Val, B)}
            end
        end,
        {SetA, SetB}, PartList).


raise_db_open_error(DbName, Error) ->
    Msg = io_lib:format("Couldn't open database `~s`, reason: ~w", [DbName, Error]),
    throw({db_open_error, DbName, Error, iolist_to_binary(Msg)}).
