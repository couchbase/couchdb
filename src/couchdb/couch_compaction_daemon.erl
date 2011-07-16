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

-module(couch_compaction_daemon).
-behaviour(gen_server).

% public API
-export([start_link/0]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("couch_db.hrl").

-define(CONFIG_ETS, couch_compaction_daemon_config).
% The period to pause for after checking (and eventually compact) all
% databases and view groups.
-define(PAUSE_PERIOD, 1).               % minutes
-define(DISK_CHECK_PERIOD, 1).          % minutes
-define(KV_RE,
    [$^, "\\s*", "([^=]+?)", "\\s*", $=, "\\s*", "([^=]+?)", "\\s*", $$]).
-define(PERIOD_RE,
    [$^, "([^-]+?)", "\\s*", $-, "\\s*", "([^-]+?)", $$]).

-record(state, {
    loop_pid
}).

-record(config, {
    db_frag = nil,
    view_frag = nil,
    period = nil,
    abortion = false
}).

-record(period, {
    from,
    to
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    process_flag(trap_exit, true),
    ?CONFIG_ETS = ets:new(?CONFIG_ETS, [named_table, set, protected]),
    Server = self(),
    ok = couch_config:register(
        fun("compaction_daemon", Db, NewValue) ->
            ok = gen_server:cast(Server, {config_update, Db, NewValue})
        end),
    load_config(),
    case start_os_mon() of
    ok ->
        Loop = spawn_link(fun() -> compact_loop(Server) end),
        {ok, #state{loop_pid = Loop}};
    {error, Error} ->
        {stop, Error}
    end.


start_os_mon() ->
    _ = application:load(os_mon),
    ok = application:set_env(
        os_mon, disk_space_check_interval, ?DISK_CHECK_PERIOD),
    ok = application:set_env(os_mon, disk_almost_full_threshold, 1),
    ok = application:set_env(os_mon, start_memsup, false),
    ok = application:set_env(os_mon, start_cpu_sup, false),
    _ = application:start(sasl),
    case application:start(os_mon) of
    ok ->
        ok;
    {error, {already_started, os_mon}} ->
        ok = disksup:set_check_interval(?DISK_CHECK_PERIOD),
        ok = disksup:set_almost_full_threshold(1);
    {error, _} = Error ->
        Error
    end.


handle_cast({config_update, DbName, deleted}, State) ->
    true = ets:delete(?CONFIG_ETS, ?l2b(DbName)),
    {noreply, State};

handle_cast({config_update, DbName, Config}, #state{loop_pid = Loop} = State) ->
    NewConfig = parse_config(Config),
    WasEmpty = (ets:info(?CONFIG_ETS, size) =:= 0),
    true = ets:insert(?CONFIG_ETS, {?l2b(DbName), NewConfig}),
    case WasEmpty of
    true ->
        Loop ! {self(), have_config};
    false ->
        ok
    end,
    {noreply, State};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.


handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.


handle_info({'EXIT', Pid, Reason}, #state{loop_pid = Pid} = State) ->
    {stop, {compaction_loop_died, Reason}, State};

handle_info(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.


terminate(_Reason, _State) ->
    true = ets:delete(?CONFIG_ETS).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


compact_loop(Parent) ->
    {ok, _} = couch_server:all_databases(
        fun(DbName, Acc) ->
            case ets:info(?CONFIG_ETS, size) =:= 0 of
            true ->
                {stop, Acc};
            false ->
                case get_db_config(DbName) of
                nil ->
                    ok;
                {ok, Config} ->
                    maybe_compact_db(DbName, Config)
                end,
                {ok, Acc}
            end
        end, ok),
    case ets:info(?CONFIG_ETS, size) =:= 0 of
    true ->
        receive {Parent, have_config} -> ok end;
    false ->
        ok = timer:sleep(?PAUSE_PERIOD * 60 * 1000)
    end,
    compact_loop(Parent).


maybe_compact_db(DbName, Config) ->
    case (catch couch_db:open_int(DbName, [])) of
    {ok, Db} ->
        case can_db_compact(Config, Db) of
        true ->
            {ok, CompactPid} = couch_db:start_compact(Db),
            TimeLeft = compact_time_left(Config),
            MonRef = erlang:monitor(process, CompactPid),
            receive
            {'DOWN', MonRef, process, CompactPid, normal} ->
                maybe_compact_views(Db, Config);
            {'DOWN', MonRef, process, CompactPid, Reason} ->
                ?LOG_ERROR("Compaction daemon - an error ocurred while"
                    " compacting the database `~s`: ~p", [DbName, Reason])
            after TimeLeft ->
                ?LOG_INFO("Compaction daemon - aborting compaction for database"
                    " `~s` because it's exceeding the allowed period.",
                    [DbName]),
                ok = couch_db:abort_compact(Db)
            end;
        false ->
            ok
        end,
        couch_db:close(Db);
    _ ->
        ok
    end.


maybe_compact_views(Db, Config) ->
    {ok, _, ok} = couch_db:enum_docs(
        Db,
        fun(#full_doc_info{id = <<"_design/", Id/binary>>}, _, Acc) ->
            maybe_compact_view(Db, Id, Config),
            {ok, Acc};
        (_, _, Acc) ->
            {stop, Acc}
        end, ok, [{start_key, <<"_design/">>}, {end_key_gt, <<"_design0">>}]).


maybe_compact_view(#db{name = DbName} = Db, GroupId, Config) ->
    DDocId = <<"_design/", GroupId/binary>>,
    case (catch couch_view:get_group_info(Db, DDocId)) of
    {ok, GroupInfo} ->
        case can_view_compact(Config, Db, GroupId, GroupInfo) of
        true ->
            {ok, CompactPid} = couch_view_compactor:start_compact(DbName, GroupId),
            TimeLeft = compact_time_left(Config),
            MonRef = erlang:monitor(process, CompactPid),
            receive
            {'DOWN', MonRef, process, CompactPid, normal} ->
                ok;
            {'DOWN', MonRef, process, CompactPid, Reason} ->
                ?LOG_ERROR("Compaction daemon - an error ocurred while compacting"
                    " the view group `~s` from database `~s`: ~p",
                    [GroupId, DbName, Reason])
            after TimeLeft ->
                ?LOG_INFO("Compaction daemon - aborting the compaction for the "
                    "vew group `~s` of the database `~s` because it's exceeding"
                    " the allowed period.", [GroupId, DbName]),
                ok = couch_view_compactor:abort_compact(DbName, GroupId)
            end;
        false ->
            ok
        end;
    _ ->
        ok
    end.


compact_time_left(#config{abortion = false}) ->
    infinity;
compact_time_left(#config{period = nil}) ->
    infinity;
compact_time_left(#config{period = #period{to = {ToH, ToM} = To}}) ->
    {H, M, _} = time(),
    case To > {H, M} of
    true ->
        ((ToH - H) * 60 * 60 * 1000) + (abs(ToM - M) * 60 * 1000);
    false ->
        ((24 - H + ToH) * 60 * 60 * 1000) + (abs(ToM - M) * 60 * 1000)
    end.


get_db_config(DbName) ->
    case ets:lookup(?CONFIG_ETS, DbName) of
    [] ->
        case ets:lookup(?CONFIG_ETS, <<"_default">>) of
        [] ->
            nil;
        [{<<"_default">>, Config}] ->
            {ok, Config}
        end;
    [{DbName, Config}] ->
        {ok, Config}
    end.


can_db_compact(#config{db_frag = Threshold} = Config, Db) ->
    case check_period(Config) of
    false ->
        false;
    true ->
        {ok, DbInfo} = couch_db:get_db_info(Db),
        {Frag, SpaceRequired} = frag(DbInfo),
        ?LOG_DEBUG("Fragmentation for database `~s` is ~p%, estimated space for"
           " compaction is ~p bytes.", [Db#db.name, Frag, SpaceRequired]),
        case check_frag(Threshold, Frag) of
        false ->
            false;
        true ->
            Free = free_space(couch_config:get("couchdb", "database_dir")),
            case Free >= SpaceRequired of
            true ->
                true;
            false ->
                ?LOG_INFO("Compaction daemon - skipping database `~s` "
                    "compaction: the estimated necessary disk space is about ~p"
                    " bytes but the currently available disk space is ~p bytes.",
                   [Db#db.name, SpaceRequired, Free]),
                false
            end
        end
    end.

can_view_compact(Config, Db, GroupId, GroupInfo) ->
    case check_period(Config) of
    false ->
        false;
    true ->
        {Frag, SpaceRequired} = frag(GroupInfo),
        ?LOG_DEBUG("Fragmentation for view group `~s` (database `~s`) is ~p%, "
           "estimated space for compaction is ~p bytes.",
           [GroupId, Db#db.name, Frag, SpaceRequired]),
        case check_frag(Config#config.view_frag, Frag) of
        false ->
            false;
        true ->
            Free = free_space(couch_config:get("couchdb", "view_index_dir")),
            case Free >= SpaceRequired of
            true ->
                true;
            false ->
                ?LOG_INFO("Compaction daemon - skipping view group `~s` "
                    "compaction (database `~s`): the estimated necessary disk "
                    "space is about ~p bytes but the currently available disk "
                    "space is ~p bytes.",
                    [GroupId, Db#db.name, SpaceRequired, Free]),
                false
            end
        end
    end.


check_period(#config{period = nil}) ->
    true;
check_period(#config{period = #period{from = From, to = To}}) ->
    {HH, MM, _} = erlang:time(),
    case From < To of
    true ->
        ({HH, MM} >= From) andalso ({HH, MM} < To);
    false ->
        ({HH, MM} >= From) orelse ({HH, MM} < To)
    end.


check_frag(nil, _) ->
    true;
check_frag(Threshold, Frag) ->
    Frag >= Threshold.


frag(Props) ->
    FileSize = couch_util:get_value(disk_size, Props),
    case couch_util:get_value(data_size, Props) of
    null ->
        {100, FileSize};
    0 ->
        {0, FileSize};
    DataSize ->
        {round(((FileSize - DataSize) / FileSize * 100)), space_required(DataSize)}
    end.

% Rough, and pessimistic, estimation of necessary disk space to compact a
% database or view index.
space_required(DataSize) ->
    round(DataSize * 2.0).


load_config() ->
    lists:foreach(
        fun({DbName, ConfigString}) ->
            Config = parse_config(ConfigString),
            true = ets:insert(?CONFIG_ETS, {?l2b(DbName), Config})
        end,
        couch_config:get("compaction_daemon")).


parse_config(ConfigString) ->
    KVs = lists:map(
        fun(Pair) ->
            {match, [K, V]} = re:run(Pair, ?KV_RE, [{capture, [1, 2], list}]),
            {K, V}
        end,
        string:tokens(string:to_lower(ConfigString), ",")),
    lists:foldl(
        fun({"db_fragmentation", V0}, Config) ->
            [V] = string:tokens(V0, "%"),
            Config#config{db_frag = list_to_integer(V)};
        ({"view_fragmentation", V0}, Config) ->
            [V] = string:tokens(V0, "%"),
            Config#config{view_frag = list_to_integer(V)};
        ({"period", V}, Config) ->
            {match, [From, To]} = re:run(
                V, ?PERIOD_RE, [{capture, [1, 2], list}]),
            [FromHH, FromMM] = string:tokens(From, ":"),
            [ToHH, ToMM] = string:tokens(To, ":"),
            Config#config{
                period = #period{
                    from = {list_to_integer(FromHH), list_to_integer(FromMM)},
                    to = {list_to_integer(ToHH), list_to_integer(ToMM)}
                }
            };
        ({"abortion", V}, Config) when V =:= "yes"; V =:= "true" ->
            Config#config{abortion = true};
        ({"abortion", V}, Config) when V =:= "no"; V =:= "false" ->
            Config#config{abortion = false}
        end, #config{}, KVs).


free_space(Path) ->
    DiskData = lists:sort(
        fun({PathA, _, _}, {PathB, _, _}) ->
            length(filename:split(PathA)) > length(filename:split(PathB))
        end,
        disksup:get_disk_data()),
    free_space_rec(abs_path(Path), DiskData).

free_space_rec(_Path, []) ->
    undefined;
free_space_rec(Path, [{MountPoint0, Total, Usage} | Rest]) ->
    MountPoint = abs_path(MountPoint0),
    case MountPoint =:= string:substr(Path, 1, length(MountPoint)) of
    false ->
        free_space_rec(Path, Rest);
    true ->
        trunc(Total - (Total * (Usage / 100))) * 1024
    end.

abs_path(Path0) ->
    Path = filename:absname(Path0),
    case lists:last(Path) of
    $/ ->
        Path;
    _ ->
        Path ++ "/"
    end.
