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
    db_seqs,
    ets
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
    Tid = get_ets_id(Pid),
    [{seqs, Seqs}] = ets:lookup(Tid, seqs),
    {ok, Seqs};
get_seqs(Pid, FilterSortedSet) ->
    Tid = get_ets_id(Pid),
    [{seqs, Seqs}] = ets:lookup(Tid, seqs),
    {ok, [{P, S} || {P, S} <- Seqs, ordsets:is_element(P, FilterSortedSet)]}.


get_ets_id(Pid) ->
    Key = {db_set_ets, Pid},
    case erlang:get(Key) of
    undefined ->
        {ok, Tid} = gen_server:call(Pid, get_ets_id, infinity);
    Tid ->
        erlang:put(Key, Tid)
    end,
    Tid.


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
    % parent will shutdown us if any database process dies. No need
    % to monitor databases here.
    DbSeqs = lists:foldl(
        fun(PartId, Acc) ->
            Name = ?dbname(SetName, PartId),
            case couch_db:open_int(Name, []) of
            {ok, Db} ->
                ok = couch_db:add_update_listener(Db, self(), PartId),
                ok = couch_db:close(Db),
                orddict:store(PartId, Db#db.update_seq, Acc);
            Error2 ->
                raise_db_open_error(Name, Error2)
            end
        end,
        orddict:new(), Partitions),
    Ets = ets:new(db_set_update_seqs_ets, [set, protected]),
    true = ets:insert(Ets, [{seqs, DbSeqs}]),
    State = #state{
        set_name = SetName,
        db_seqs = DbSeqs,
        ets = Ets
    },
    {ok, State}.


handle_call(get_ets_id, _From, State) ->
    {reply, {ok, State#state.ets}, State};

% Used for debugging/troubleshooting only
handle_call(get_seqs_debug, _From, State) ->
    RealSeqs = orddict:fold(
        fun(PartId, _, Acc) ->
            DbName = ?dbname((State#state.set_name), PartId),
            {ok, Db} = couch_db:open_int(DbName, []),
            ok = couch_db:close(Db),
            orddict:store(PartId, Db#db.update_seq, Acc)
        end,
        orddict:new(),
        State#state.db_seqs),
    {reply, {ok, State#state.db_seqs, RealSeqs}, State};

handle_call({add_partitions, PartList}, _From, State) ->
    DbSeqs2 = lists:foldl(
        fun(Id, AccDbSeqs) ->
            case orddict:is_key(Id, AccDbSeqs) of
            false ->
                DbName = ?dbname((State#state.set_name), Id),
                {ok, Db} = couch_db:open_int(DbName, []),
                ok = couch_db:add_update_listener(Db, self(), Id),
                ok = couch_db:close(Db),
                orddict:store(Id, Db#db.update_seq, AccDbSeqs);
            true ->
                AccDbSeqs
            end
        end,
        State#state.db_seqs,
        PartList),
    true = ets:insert(State#state.ets, [{seqs, DbSeqs2}]),
    {reply, ok, State#state{db_seqs = DbSeqs2}};

handle_call({remove_partitions, PartList}, _From, State) ->
    DbSeqs2 = lists:foldl(
        fun(Id, AccDbSeqs) ->
            case orddict:is_key(Id, AccDbSeqs) of
            false ->
                AccDbSeqs;
            true ->
                DbName = ?dbname((State#state.set_name), Id),
                case couch_db:open_int(DbName, []) of
                {ok, Db} ->
                    ok = couch_db:remove_update_listener(Db, self()),
                    ok = couch_db:close(Db);
                {not_found, no_db_file} ->
                    ok % deleted, ignore
                end,
                clean_update_messages(Id),
                orddict:erase(Id, AccDbSeqs)
            end
        end,
        State#state.db_seqs,
        PartList),
    true = ets:insert(State#state.ets, [{seqs, DbSeqs2}]),
    {reply, ok, State#state{db_seqs = DbSeqs2}};

handle_call(close, _From, State) ->
    {stop, normal, ok, State}.


handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.


handle_info({db_updated, PartId, NewSeq}, State) ->
    DbSeqs2 = orddict:store(PartId, NewSeq, State#state.db_seqs),
    true = ets:insert(State#state.ets, [{seqs, DbSeqs2}]),
    {noreply, State#state{db_seqs = DbSeqs2}};

handle_info(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.


raise_db_open_error(DbName, Error) ->
    Msg = io_lib:format("Couldn't open database `~s`, reason: ~w", [DbName, Error]),
    throw({db_open_error, DbName, Error, iolist_to_binary(Msg)}).


clean_update_messages(PartId) ->
    receive
    {db_updated, PartId, _NewSeq} ->
        clean_update_messages(PartId)
    after 0 ->
        ok
    end.

