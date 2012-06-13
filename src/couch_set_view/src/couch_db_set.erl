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
    EventFun = fun({updated, {DbName, _NewSeq}} = Ev) ->
            case is_set_db(DbName, SetName) of
            true ->
                ok = gen_server:cast(Server, Ev);
            false ->
                ok
            end;
        (_) ->
            ok
    end,
    DbSeqs = lists:foldl(
        fun(PartId, Acc) ->
            Name = ?dbname(SetName, PartId),
            case couch_db:open_int(Name, []) of
            {ok, Db} ->
                ok = couch_db:close(Db),
                orddict:store(PartId, Db#db.update_seq, Acc);
            Error2 ->
                raise_db_open_error(Name, Error2)
            end
        end,
        orddict:new(), Partitions),
    {ok, Notifier} = couch_db_update_notifier:start_link(EventFun),
    State = #state{
        set_name = SetName,
        db_notifier = Notifier,
        db_seqs = DbSeqs
    },
    {ok, State}.


handle_call(get_seqs, _From, State) ->
    {reply, {ok, State#state.db_seqs}, State};

handle_call({add_partitions, PartList}, _From, State) ->
    DbSeqs2 = lists:foldl(
        fun(Id, AccDbSeqs) ->
            case orddict:is_key(Id, AccDbSeqs) of
            false ->
                DbName = ?dbname((State#state.set_name), Id),
                {ok, Db} = couch_db:open_int(DbName, []),
                ok = couch_db:close(Db),
                orddict:store(Id, Db#db.update_seq, AccDbSeqs);
            true ->
                AccDbSeqs
            end
        end,
        State#state.db_seqs,
        PartList),
    {reply, ok, State#state{db_seqs = DbSeqs2}};

handle_call({remove_partitions, PartList}, _From, State) ->
    DbSeqs2 = lists:foldl(
        fun(Id, AccDbSeqs) ->
            case orddict:is_key(Id, AccDbSeqs) of
            false ->
                AccDbSeqs;
            true ->
                orddict:erase(Id, AccDbSeqs)
            end
        end,
        State#state.db_seqs,
        PartList),
    {reply, ok, State#state{db_seqs = DbSeqs2}};

handle_call(close, _From, State) ->
    {stop, normal, ok, State}.


handle_cast({updated, {DbName, NewSeq}}, State) ->
    case get_part_id(DbName) of
    {ok, PartId} ->
        case orddict:is_key(PartId, State#state.db_seqs) of
        true ->
            DbSeqs2 = orddict:store(PartId, NewSeq, State#state.db_seqs),
            {noreply, State#state{db_seqs = DbSeqs2}};
        false ->
            {noreply, State}
        end;
    _ ->
        {noreply, State}
    end.


handle_info(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, State) ->
    couch_db_update_notifier:stop(State#state.db_notifier).


raise_db_open_error(DbName, Error) ->
    Msg = io_lib:format("Couldn't open database `~s`, reason: ~w", [DbName, Error]),
    throw({db_open_error, DbName, Error, iolist_to_binary(Msg)}).


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
