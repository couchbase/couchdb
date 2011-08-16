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
    cancel = false,
    parallel_view_compact = false
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
        fun("compactions", Db, NewValue) ->
            ok = gen_server:cast(Server, {config_update, Db, NewValue})
        end),
    load_config(),
    case start_os_mon() of
    ok ->
        Loop = spawn_link(fun() -> compact_loop(Server) end),
        {ok, #state{loop_pid = Loop}};
    Error ->
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
        ok;
    Error ->
        Error
    end.


handle_cast({config_update, DbName, deleted}, State) ->
    true = ets:delete(?CONFIG_ETS, ?l2b(DbName)),
    {noreply, State};

handle_cast({config_update, DbName, Config}, #state{loop_pid = Loop} = State) ->
    {ok, NewConfig} = parse_config(Config),
    WasEmpty = (ets:info(?CONFIG_ETS, size) =:= 0),
    true = ets:insert(?CONFIG_ETS, {?l2b(DbName), NewConfig}),
    case WasEmpty of
    true ->
        Loop ! {self(), have_config};
    false ->
        ok
    end,
    {noreply, State}.


handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.


handle_info({'EXIT', Pid, Reason}, #state{loop_pid = Pid} = State) ->
    {stop, {compaction_loop_died, Reason}, State}.


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
        PausePeriod = list_to_integer(
            couch_config:get("compaction_daemon", "check_interval", "1")),
        ok = timer:sleep(PausePeriod * 1000)
    end,
    compact_loop(Parent).


maybe_compact_db(DbName, Config) ->
    case (catch couch_db:open_int(DbName, [])) of
    {ok, Db} ->
        DDocNames = db_ddoc_names(Db),
        case can_db_compact(Config, Db) of
        true ->
            {ok, DbCompactPid} = couch_db:start_compact(Db),
            TimeLeft = compact_time_left(Config),
            case Config#config.parallel_view_compact of
            true ->
                ViewsCompactPid = spawn_link(fun() ->
                    maybe_compact_views(DbName, DDocNames, Config)
                end),
                ViewsMonRef = erlang:monitor(process, ViewsCompactPid);
            false ->
                ViewsMonRef = nil
            end,
            DbMonRef = erlang:monitor(process, DbCompactPid),
            receive
            {'DOWN', DbMonRef, process, _, normal} ->
                couch_db:close(Db),
                case Config#config.parallel_view_compact of
                true ->
                    ok;
                false ->
                    maybe_compact_views(DbName, DDocNames, Config)
                end;
            {'DOWN', DbMonRef, process, _, Reason} ->
                couch_db:close(Db),
                ?LOG_ERROR("Compaction daemon - an error ocurred while"
                    " compacting the database `~s`: ~p", [DbName, Reason])
            after TimeLeft ->
                ?LOG_INFO("Compaction daemon - canceling compaction for database"
                    " `~s` because it's exceeding the allowed period.",
                    [DbName]),
                erlang:demonitor(DbMonRef, [flush]),
                ok = couch_db:cancel_compact(Db),
                couch_db:close(Db)
            end,
            case ViewsMonRef of
            nil ->
                ok;
            _ ->
                receive
                {'DOWN', ViewsMonRef, process, _, _Reason} ->
                    ok
                end
            end;
        false ->
            ok
        end;
    _ ->
        ok
    end.


maybe_compact_views(_DbName, [], _Config) ->
    ok;
maybe_compact_views(DbName, [DDocName | Rest], Config) ->
    case maybe_compact_view(DbName, DDocName, Config) of
    ok ->
        maybe_compact_views(DbName, Rest, Config);
    timeout ->
        ok
    end.


db_ddoc_names(Db) ->
    {ok, _, DDocNames} = couch_db:enum_docs(
        Db,
        fun(#full_doc_info{id = <<"_design/", _/binary>>, deleted = true}, _, Acc) ->
            {ok, Acc};
        (#full_doc_info{id = <<"_design/", Id/binary>>}, _, Acc) ->
            {ok, [Id | Acc]};
        (_, _, Acc) ->
            {stop, Acc}
        end, [], [{start_key, <<"_design/">>}, {end_key_gt, <<"_design0">>}]),
    DDocNames.


maybe_compact_view(DbName, GroupId, Config) ->
    DDocId = <<"_design/", GroupId/binary>>,
    case (catch couch_view:get_group_info(DbName, DDocId)) of
    {ok, GroupInfo} ->
        case can_view_compact(Config, DbName, GroupId, GroupInfo) of
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
                    [GroupId, DbName, Reason]),
                ok
            after TimeLeft ->
                ?LOG_INFO("Compaction daemon - canceling the compaction for the "
                    "view group `~s` of the database `~s` because it's exceeding"
                    " the allowed period.", [GroupId, DbName]),
                erlang:demonitor(MonRef, [flush]),
                ok = couch_view_compactor:cancel_compact(DbName, GroupId),
                timeout
            end;
        false ->
            ok
        end;
    Error ->
        ?LOG_ERROR("Error opening view group `~s` from database `~s`: ~p",
            [GroupId, DbName, Error]),
        ok
    end.


compact_time_left(#config{cancel = false}) ->
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

can_view_compact(Config, DbName, GroupId, GroupInfo) ->
    case check_period(Config) of
    false ->
        false;
    true ->
        case couch_util:get_value(updater_running, GroupInfo) of
        true ->
            false;
        false ->
            {Frag, SpaceRequired} = frag(GroupInfo),
            ?LOG_DEBUG("Fragmentation for view group `~s` (database `~s`) is "
                "~p%, estimated space for compaction is ~p bytes.",
                [GroupId, DbName, Frag, SpaceRequired]),
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
                        "compaction (database `~s`): the estimated necessary "
                        "disk space is about ~p bytes but the currently available"
                        " disk space is ~p bytes.",
                        [GroupId, DbName, SpaceRequired, Free]),
                    false
                end
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
    MinFileSize = list_to_integer(
        couch_config:get("compaction_daemon", "min_file_size", "131072")),
    case FileSize < MinFileSize of
    true ->
        {0, FileSize};
    false ->
        case couch_util:get_value(data_size, Props) of
        null ->
            {100, FileSize};
        0 ->
            {0, FileSize};
        DataSize ->
            Frag = round(((FileSize - DataSize) / FileSize * 100)),
            {Frag, space_required(DataSize)}
        end
    end.

% Rough, and pessimistic, estimation of necessary disk space to compact a
% database or view index.
space_required(DataSize) ->
    round(DataSize * 2.0).


load_config() ->
    lists:foreach(
        fun({DbName, ConfigString}) ->
            case (catch parse_config(ConfigString)) of
            {ok, Config} ->
                true = ets:insert(?CONFIG_ETS, {?l2b(DbName), Config});
            _ ->
                ?LOG_ERROR("Invalid compaction configuration for database "
                    "`~s`: `~s`", [DbName, ConfigString])
            end
        end,
        couch_config:get("compactions")).


parse_config(ConfigString) ->
    KVs = lists:map(
        fun(Pair) ->
            {match, [K, V]} = re:run(Pair, ?KV_RE, [{capture, [1, 2], list}]),
            {K, V}
        end,
        string:tokens(string:to_lower(ConfigString), ",")),
    Config = lists:foldl(
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
        ({"strict_window", V}, Config) when V =:= "yes"; V =:= "true" ->
            Config#config{cancel = true};
        ({"strict_window", V}, Config) when V =:= "no"; V =:= "false" ->
            Config#config{cancel = false};
        ({"parallel_view_compaction", V}, Config) when V =:= "yes"; V =:= "true" ->
            Config#config{parallel_view_compact = true};
        ({"parallel_view_compaction", V}, Config) when V =:= "no"; V =:= "false" ->
            Config#config{parallel_view_compact = false}
        end, #config{}, KVs),
    {ok, Config}.


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
