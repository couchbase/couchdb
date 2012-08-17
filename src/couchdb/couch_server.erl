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
-export([all_databases/0, all_databases/2, all_known_databases/0]).
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
        couch_util:to_hex(crypto:sha(ClearPwd ++ Salt)) == HashedPwd;
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
            Hashed = couch_util:to_hex(crypto:sha(ClearPassword ++ Salt)),
            couch_config:set("admins",
                User, "-hashed-" ++ Hashed ++ "," ++ Salt, Persist)
        end, couch_config:get("admins")).

init([]) ->
    % read config and register for configuration changes

    % just stop if one of the config settings change. couch_server_sup
    % will restart us and then we will pick up the new settings.

    RootDir = couch_config:get("couchdb", "database_dir", "."),
    Self = self(),
    ok = couch_config:register(
        fun("couchdb", "database_dir") ->
            exit(Self, config_change)
        end),
    ok = couch_file:init_delete_dir(RootDir),
    hash_admin_passwords(),
    ok = couch_config:register(
        fun("admins", _Key, _Value, Persist) ->
            % spawn here so couch_config doesn't try to call itself
            spawn(fun() -> hash_admin_passwords(Persist) end)
        end, false),
    {ok, RegExp} = re:compile("^[A-Za-z0-9\\_\\.\\%\\-\\/]*$"),
    ets:new(couch_dbs_by_name, [set, private, named_table]),
    ets:new(couch_dbs_by_pid, [set, private, named_table]),
    ets:new(couch_dbs_by_lru, [ordered_set, private, named_table]),
    ets:new(couch_sys_dbs, [set, private, named_table]),
    process_flag(trap_exit, true),
    {ok, #server{root_dir=RootDir,
                dbname_regexp=RegExp,
                start_time=httpd_util:rfc1123_date()}}.

terminate(_Reason, _Srv) ->
    lists:foreach(
        fun({_DbName, {_Status, Pid, _LruTime}}) ->
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
        filelib:fold_files(Root, "^[a-z0-9\\_\\$()\\+\\-]*[\\.]couch[\\.][0-9]*$", true,
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

-spec all_known_databases() -> [binary()].
all_known_databases() ->
    gen_server:call(couch_server, get_known_databases, infinity).

open_async(Server, Froms, DbName, Filepath, Options) ->
    Parent = self(),
    Opener = spawn_link(fun() ->
            Res = couch_db:start_link(DbName, Filepath, Options),
            gen_server:call(
                Parent, {open_result, DbName, Res, Options}, infinity
            ),
            unlink(Parent),
            case Res of
            {ok, DbReader} ->
                unlink(DbReader);
            _ ->
                ok
            end
        end),
    true = ets:insert(couch_dbs_by_name, {DbName, {opening, Opener, Froms}}),
    true = ets:insert(couch_dbs_by_pid, {Opener, DbName}),
    DbsOpen = case lists:member(sys_db, Options) of
    true ->
        true = ets:insert(couch_sys_dbs, {DbName, true}),
        Server#server.dbs_open;
    false ->
        Server#server.dbs_open + 1
    end,
    Server#server{dbs_open = DbsOpen}.

handle_call(get_server, _From, Server) ->
    {reply, {ok, Server}, Server};
handle_call(get_known_databases, _From, Server) ->
    Names = ets:match(couch_dbs_by_name, {'$1', '_'}),
    {reply, [N || [N] <- Names], Server};
handle_call({open_result, DbName, {ok, OpenedDbPid}, Options}, From, Server) ->
    case ets:lookup(couch_dbs_by_name, DbName) of
    [{DbName, {opening, Opener, Froms}}] ->
        link(OpenedDbPid),
        lists:foreach(fun({FromPid, _} = From2) ->
            gen_server:reply(From2,
                    catch couch_db:open_ref_counted(OpenedDbPid, FromPid))
        end, Froms),
        LruTime = now(),
        true = ets:insert(couch_dbs_by_name,
                {DbName, {opened, OpenedDbPid, LruTime}}),
        true = ets:delete(couch_dbs_by_pid, Opener),
        true = ets:insert(couch_dbs_by_pid, {OpenedDbPid, DbName}),
        true = ets:insert(couch_dbs_by_lru, {LruTime, DbName}),
        case lists:member(create, Options) of
        true ->
            couch_db_update_notifier:notify({created, DbName});
        false ->
            ok
        end,
        {reply, ok, Server};
    [] ->
        {OpenerPid, _Ref} = From,
        false = is_process_alive(OpenerPid),
        % db file previously deleted
        couch_util:shutdown_sync(OpenedDbPid),
        {noreply, Server}
    end;
handle_call({open_result, DbName, Error, Options}, From, Server) ->
    case ets:lookup(couch_dbs_by_name, DbName) of
    [{DbName, {opening, Opener, Froms}}] ->
    % only notify the first openner, retry for all others since it's possible
    % that an external writer created the file after the first open request,
    % but before the subsequent open requests
        {FromsNext, [FirstOpen]} = lists:split(length(Froms) - 1, Froms),
        gen_server:reply(FirstOpen, Error),
        true = ets:delete(couch_dbs_by_name, DbName),
        true = ets:delete(couch_dbs_by_pid, Opener),
        DbsOpen = case lists:member(sys_db, Options) of
        true ->
            true = ets:delete(couch_sys_dbs, DbName),
            Server#server.dbs_open;
        false ->
            Server#server.dbs_open - 1
        end,
        Server2 = Server#server{dbs_open = DbsOpen},
        case FromsNext of
        [] ->
            Server3 = Server2;
        _ ->
            % Retry
            Filepath = get_full_filename(Server, binary_to_list(DbName)),
            Server3 = open_async(Server2, FromsNext, DbName, Filepath, Options)
        end,
        {reply, ok, Server3};
    [] ->
        {OpenerPid, _Ref} = From,
        false = is_process_alive(OpenerPid),
        {noreply, Server}
    end;
handle_call({open, DbName, Options}, {FromPid,_}=From, Server) ->
    LruTime = now(),
    case ets:lookup(couch_dbs_by_name, DbName) of
    [] ->
        open_db(DbName, Server, Options, [From]);
    [{_, {opening, Opener, Froms}}] ->
        true = ets:insert(couch_dbs_by_name, {DbName, {opening, Opener, [From|Froms]}}),
        {noreply, Server};
    [{_, {opened, MainPid, PrevLruTime}}] ->
        true = ets:insert(couch_dbs_by_name, {DbName, {opened, MainPid, LruTime}}),
        true = ets:delete(couch_dbs_by_lru, PrevLruTime),
        true = ets:insert(couch_dbs_by_lru, {LruTime, DbName}),
        {reply, couch_db:open_ref_counted(MainPid, FromPid), Server}
    end;
handle_call({create, DbName, Options}, From, Server) ->
    case ets:lookup(couch_dbs_by_name, DbName) of
    [] ->
        open_db(DbName, Server, [create | Options], [From]);
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
        [{_, {opening, Pid, Froms}}] ->
            couch_util:shutdown_sync(Pid),
            true = ets:delete(couch_dbs_by_name, DbName),
            true = ets:delete(couch_dbs_by_pid, Pid),
            [gen_server:reply(F, not_found) || F <- Froms],
            true;
        [{_, {opened, Pid, LruTime}}] ->
            couch_util:shutdown_sync(Pid),
            true = ets:delete(couch_dbs_by_name, DbName),
            true = ets:delete(couch_dbs_by_pid, Pid),
            true = ets:delete(couch_dbs_by_lru, LruTime),
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
    
handle_info({'EXIT', _Pid, config_change}, Server) ->
    {noreply, shutdown, Server};
handle_info({'EXIT', Pid, snappy_nif_not_loaded}, Server) ->
    Server2 = case ets:lookup(couch_dbs_by_pid, Pid) of
    [{Pid, Db}] ->
        [{Db, {opening, Pid, Froms}}] = ets:lookup(couch_dbs_by_name, Db),
        Msg = io_lib:format("To open the database `~s`, Apache CouchDB "
            "must be built with Erlang OTP R13B04 or higher.", [Db]),
        ?LOG_ERROR(Msg, []),
        lists:foreach(
            fun(F) -> gen_server:reply(F, {bad_otp_release, Msg}) end,
            Froms),
        true = ets:delete(couch_dbs_by_name, Db),
        true = ets:delete(couch_dbs_by_pid, Pid),
        case ets:lookup(couch_sys_dbs, Db) of
        [{Db, _}] ->
            true = ets:delete(couch_sys_dbs, Db),
            Server;
        [] ->
            Server#server{dbs_open = Server#server.dbs_open - 1}
        end;
    _ ->
        Server
    end,
    {noreply, Server2};
handle_info(Error, _Server) ->
    ?LOG_ERROR("Unexpected message, restarting couch_server: ~p", [Error]),
    exit(kill).

open_db(DbName, Server, Options, Froms) ->
    DbNameList = binary_to_list(DbName),
    case check_dbname(Server, DbNameList) of
    ok ->
        Filepath = get_full_filename(Server, DbNameList),
        {noreply, open_async(Server, Froms, DbName, Filepath, Options)};
     Error ->
        {reply, Error, Server}
     end.
