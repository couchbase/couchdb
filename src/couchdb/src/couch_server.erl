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

-module(couch_server).
-behaviour(gen_server).

-export([open/2,create/2,delete/2,get_version/0]).
-export([all_databases/0, all_databases/2, all_known_databases_with_prefix/1]).
-export([init/1, handle_call/3,sup_start_link/0]).
-export([handle_cast/2,code_change/3,handle_info/2,terminate/2]).
-export([dev_start/0,is_admin/2,has_admins/0,get_stats/0]).

-include("couch_db.hrl").

-record(server,{
    root_dir = [],
    dbname_regexp,
    dbs_open=0,
    start_time=""
    }).

dev_start() ->
    couch:stop(),
    up_to_date = make:all([load, debug_info]),
    couch:start().

get_version() ->
    Apps = application:loaded_applications(),
    case lists:keysearch(couch, 1, Apps) of
    {value, {_, _, Vsn}} ->
        Vsn;
    false ->
        "0.0.0"
    end.

get_stats() ->
    {ok, #server{start_time=Time,dbs_open=Open}} =
            gen_server:call(couch_server, get_server),
    [{start_time, ?l2b(Time)}, {dbs_open, Open}].

sup_start_link() ->
    gen_server:start_link({local, couch_server}, couch_server, [], []).

open(DbName, Options) ->
    couch_log:pre_db_open(DbName),
    case gen_server:call(couch_server, {open, DbName, Options}, infinity) of
    {ok, Db} ->
        Ctx = couch_util:get_value(user_ctx, Options, #user_ctx{}),
        {ok, Db#db{user_ctx=Ctx}};
    Error ->
        Error
    end.

create(DbName, Options) ->
    case gen_server:call(couch_server, {create, DbName, Options}, infinity) of
    {ok, Db} ->
        Ctx = couch_util:get_value(user_ctx, Options, #user_ctx{}),
        {ok, Db#db{user_ctx=Ctx}};
    Error ->
        Error
    end.

delete(DbName, Options) ->
    gen_server:call(couch_server, {delete, DbName, Options}, infinity).

check_dbname(#server{dbname_regexp=RegExp}, DbName) ->
    case re:run(DbName, RegExp, [{capture, none}]) of
    nomatch ->
        case DbName of
            "_users" -> ok;
            "_replicator" -> ok;
            _Else ->
                {error, illegal_database_name}
            end;
    match ->
        ok
    end.

is_admin(User, ClearPwd) ->
    case couch_config:get("admins", User) of
    "-hashed-" ++ HashedPwdAndSalt ->
        [HashedPwd, Salt] = string:tokens(HashedPwdAndSalt, ","),
        couch_util:to_hex(crypto:hash(sha, ClearPwd ++ Salt)) == HashedPwd;
    _Else ->
        false
    end.

has_admins() ->
    couch_config:get("admins") /= [].

get_full_filename(Server, DbName) ->
    filename:join([Server#server.root_dir, "./" ++ DbName ++ ".couch"]).

hash_admin_passwords() ->
    hash_admin_passwords(true).

hash_admin_passwords(Persist) ->
    lists:foreach(
        fun({_User, "-hashed-" ++ _}) ->
            ok; % already hashed
        ({User, ClearPassword}) ->
            Salt = ?b2l(couch_uuids:random()),
            Hashed = couch_util:to_hex(crypto:hash(sha, ClearPassword ++ Salt)),
            couch_config:set("admins",
                User, "-hashed-" ++ Hashed ++ "," ++ Salt, Persist)
        end, couch_config:get("admins")).

init([]) ->
    % read config and register for configuration changes

    % just stop if one of the config settings change. couch_server_sup
    % will restart us and then we will pick up the new settings.

    RootDir = couch_config:get("couchdb", "database_dir", "."),
    ok = couch_file:init_delete_dir(RootDir),
    hash_admin_passwords(),
    ok = couch_config:register(
        fun("admins", _Key, _Value, Persist) ->
            % spawn here so couch_config doesn't try to call itself
            spawn(fun() -> hash_admin_passwords(Persist) end)
        end, false),
    {ok, RegExp} = re:compile("^[A-Za-z0-9\\_\\.\\%\\-\\/]*$"),
    ets:new(couch_dbs_by_name, [ordered_set, protected, named_table]),
    ets:new(couch_dbs_by_pid, [set, private, named_table]),
    ets:new(couch_sys_dbs, [set, private, named_table]),

    % ets tables to capture view query timing stats
    % marking it public because it needs to be accessed by
    % modules within couch_index_merger
    ets:new(?QUERY_TIMING_STATS_ETS, [ordered_set, public, named_table,
                                      {write_concurrency, true}]),
    ets:new(map_context_store, [set, public, named_table, {read_concurrency, true}]),
    ets:new(reduce_context_store, [set, public, named_table, {read_concurrency, true}]),
    process_flag(trap_exit, true),
    {ok, #server{root_dir=RootDir,
                dbname_regexp=RegExp,
                start_time=httpd_util:rfc1123_date()}}.

terminate(_Reason, _Srv) ->
    lists:foreach(
        fun({_DbName, Pid}) ->
            couch_util:shutdown_sync(Pid)
        end,
        ets:tab2list(couch_dbs_by_name)).

all_databases() ->
    {ok, DbList} = all_databases(
        fun(DbName, Acc) -> {ok, [DbName | Acc]} end, []),
    {ok, lists:usort(DbList)}.

strip_file_num(FilePath) ->
    Tokens = string:tokens(FilePath, "."),
    string:join(lists:sublist(Tokens, length(Tokens) - 1), ".").

all_databases(Fun, Acc0) ->
    {ok, #server{root_dir=Root}} = gen_server:call(couch_server, get_server),
    NormRoot = couch_util:normpath(Root),
    FinalAcc = try
        file2:fold_files(Root, "^[a-z0-9\\_\\$()\\+\\-]*[\\.]couch[\\.][0-9]*$", true,
            fun(Filename, AccIn) ->
                NormFilename = couch_util:normpath(Filename),
                case NormFilename -- NormRoot of
                [$/ | RelativeFilename] -> ok;
                RelativeFilename -> ok
                end,
                RF = strip_file_num(RelativeFilename),
                case Fun(?l2b(filename:rootname(RF, ".couch")), AccIn) of
                {ok, NewAcc} -> NewAcc;
                {stop, NewAcc} -> throw({stop, Fun, NewAcc})
                end
            end, Acc0)
    catch throw:{stop, Fun, Acc1} ->
         Acc1
    end,
    {ok, FinalAcc}.

all_known_databases_with_prefix(Prefix) ->
    PreLen = erlang:size(Prefix),
    Init = case ets:lookup(couch_dbs_by_name, Prefix) of
    [] ->
        [];
    [_] ->
        [Prefix]
    end,
    all_known_databases_with_prefix_loop(Prefix, PreLen, Prefix, Init).

all_known_databases_with_prefix_loop(Prefix, PreLen, K, Acc) ->
    K2 = ets:next(couch_dbs_by_name, K),
    case K2 of
    <<Prefix:PreLen/binary, _/binary>> ->
        all_known_databases_with_prefix_loop(Prefix, PreLen, K2, [K2 | Acc]);
    _ ->
        Acc
    end.

do_open_db(DbName, Server, Options, {FromPid, _}) ->
    DbNameList = binary_to_list(DbName),
    case check_dbname(Server, DbNameList) of
    ok ->
        Filepath = get_full_filename(Server, DbNameList),
        case couch_db:start_link(DbName, Filepath, Options) of
        {ok, DbPid} ->
            true = ets:insert(couch_dbs_by_name, {DbName, DbPid}),
            true = ets:insert(couch_dbs_by_pid, {DbPid, DbName}),
            case lists:member(create, Options) of
            true ->
                couch_db_update_notifier:notify({created, DbName});
            false ->
                ok
            end,
            DbsOpen = Server#server.dbs_open + 1,
            NewServer = Server#server{dbs_open = DbsOpen},
            Reply = (catch couch_db:open_ref_counted(DbPid, FromPid)),
            {reply, Reply, NewServer};
        Error ->
            {reply, Error, Server}
        end;
     Error ->
        {reply, Error, Server}
     end.

handle_call(get_server, _From, Server) ->
    {reply, {ok, Server}, Server};
handle_call(get_known_databases, _From, Server) ->
    Names = ets:match(couch_dbs_by_name, {'$1', '_'}),
    {reply, [N || [N] <- Names], Server};
handle_call({open, DbName, Options}, {FromPid,_}=From, Server) ->
    case ets:lookup(couch_dbs_by_name, DbName) of
    [] ->
        do_open_db(DbName, Server, Options, From);
    [{_, MainPid}] ->
        {reply, couch_db:open_ref_counted(MainPid, FromPid), Server}
    end;
handle_call({create, DbName, Options}, From, Server) ->
    case ets:lookup(couch_dbs_by_name, DbName) of
    [] ->
        do_open_db(DbName, Server, [create | Options], From);
    [_AlreadyRunningDb] ->
        {reply, file_exists, Server}
    end;
handle_call({delete, DbName, _Options}, _From, Server) ->
    ?LOG_INFO("Deleting database ~s", [DbName]),
    DbNameList = binary_to_list(DbName),
    case check_dbname(Server, DbNameList) of
    ok ->
        FullFilepath = get_full_filename(Server, DbNameList),
        Files = filelib:wildcard(FullFilepath ++ ".*"),
        case Files of
        [] ->
            ok;
        _ ->
            couch_db_update_notifier:sync_notify({before_delete, DbName})
        end,
        UpdateState =
        case ets:lookup(couch_dbs_by_name, DbName) of
        [] -> false;
        [{_, Pid}] ->
            couch_util:shutdown_sync(Pid),
            true = ets:delete(couch_dbs_by_name, DbName),
            true = ets:delete(couch_dbs_by_pid, Pid),
            true
        end,
        Server2 = case UpdateState of
        true ->
            DbsOpen = case ets:member(couch_sys_dbs, DbName) of
            true ->
                true = ets:delete(couch_sys_dbs, DbName),
                Server#server.dbs_open;
            false ->
                Server#server.dbs_open - 1
            end,
            Server#server{dbs_open = DbsOpen};
        false ->
            Server
        end,
        Result = lists:map(fun(F) ->
                ?LOG_INFO("Deleting file ~s", [F]),
                couch_file:delete(Server#server.root_dir, F)
            end, Files),

        case Result of
        [ok|_] ->
            couch_db_update_notifier:notify({deleted, DbName}),
            {reply, ok, Server2};
        [] ->
            {reply, not_found, Server2};
        [Else|_] ->
            {reply, Else, Server2}
        end;
    Error ->
        {reply, Error, Server}
    end.

handle_cast(Msg, _Server) ->
    exit({unknown_cast_message, Msg}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info(Error, _Server) ->
    ?LOG_ERROR("Unexpected message, restarting couch_server: ~p", [Error]),
    exit(kill).
