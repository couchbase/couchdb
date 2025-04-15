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

-module(couch_log).
-behaviour(gen_server).

% public API
-export([start_link/0, stop/0]).
-export([debug/2, info/2, error/2]).
-export([debug_on/0, info_on/0, get_level/0, get_level_integer/0, set_level/1]).
-export([read/2]).
-export([pre_db_open/1]).

%% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2,
         handle_info/2,code_change/3]).

%% logger callbacks.
-export([adding_handler/1, removing_handler/1, log/2]).

-define(LEVEL_ERROR, 3).
-define(LEVEL_INFO, 2).
-define(LEVEL_DEBUG, 1).

-define(DISK_LOGGER, couch_disk_logger).

-record(state, {
    level,
    sasl
}).

debug(Format, Args) ->
    {ConsoleMsg, FileMsg} = get_log_messages(self(), debug, Format, Args),
    ok = disk_log:balog(?DISK_LOGGER, FileMsg),
    gen_server:call(?MODULE, {couch_debug, ConsoleMsg}).

info(Format, Args) ->
    {ConsoleMsg, FileMsg} = get_log_messages(self(), info, Format, Args),
    ok = disk_log:balog(?DISK_LOGGER, FileMsg),
    gen_server:call(?MODULE, {couch_info, ConsoleMsg}).

error(Format, Args) ->
    {ConsoleMsg, FileMsg} = get_log_messages(self(), error, Format, Args),
    % Synchronous logging for error messages only. We want to reduce the
    % chances of missing any if server is killed.
    ok = disk_log:blog(?DISK_LOGGER, FileMsg),
    gen_server:call(?MODULE, {couch_error, ConsoleMsg}).


level_integer(error)    -> ?LEVEL_ERROR;
level_integer(info)     -> ?LEVEL_INFO;
level_integer(debug)    -> ?LEVEL_DEBUG;
level_integer(_Else)    -> ?LEVEL_ERROR. % anything else default to ERROR level

level_atom(?LEVEL_ERROR) -> error;
level_atom(?LEVEL_INFO) -> info;
level_atom(?LEVEL_DEBUG) -> debug.


start_link() ->
    try
        logger:remove_handler(?MODULE),
        {ok, Pid} = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
        ok = logger:add_handler(?MODULE, ?MODULE,
                                #{level => error, filter_default => log}),
        {ok, Pid}
    catch
        Type:Reason ->
            catch(gen_server:stop(?MODULE)),
            {error, {Type, Reason}}
    end.

stop() ->
    catch(gen_server:stop(?MODULE)).

-spec adding_handler(logger:handler_config()) ->
    {ok, logger:handler_config()} | {error, term()}.
adding_handler(#{id:=?MODULE} = Config) ->
    case whereis(?MODULE) of
        undefined ->
            {error, noproc};
        _ ->
            {ok, Config}
    end.

-spec removing_handler(logger:handler_config()) -> ok.
removing_handler(#{id:=?MODULE}) ->
    ok.

-spec log(logger:log_event(), logger:handler_config()) -> ok.
log(LogMsg, _Config) ->
    gen_server:cast(?MODULE, LogMsg).

init([]) ->
    % read config and register for configuration changes

    % just stop if one of the config settings change. couch_server_sup
    % will restart us and then we will pick up the new settings.
    ok = couch_config:register(
        fun("log", "file") ->
            ?MODULE:stop();
        ("log", "level") ->
            ?MODULE:stop();
        ("log", "include_sasl") ->
            ?MODULE:stop()
        end),

    Filename = couch_config:get("log", "file", "couchdb.log"),
    Level = level_integer(list_to_atom(couch_config:get("log", "level", "info"))),
    Sasl = couch_config:get("log", "include_sasl", "true") =:= "true",

    case ets:info(?MODULE) of
    undefined -> ets:new(?MODULE, [named_table]);
    _ -> ok
    end,
    ets:insert(?MODULE, {level, Level}),

    DiskLogOptions = [
        {file, Filename}, {name, ?DISK_LOGGER},
        {format, external}, {type, halt}, {notify, true}
    ],
    case disk_log:open(DiskLogOptions) of
    {ok, ?DISK_LOGGER} ->
        {ok, #state{level = Level, sasl = Sasl}};
    Error ->
        {stop, Error}
    end.

debug_on() ->
    get_level_integer() =< ?LEVEL_DEBUG.

info_on() ->
    get_level_integer() =< ?LEVEL_INFO.

set_level(LevelAtom) ->
    set_level_integer(level_integer(LevelAtom)).

get_level() ->
    level_atom(get_level_integer()).

get_level_integer() ->
    try
        ets:lookup_element(?MODULE, level, 2)
    catch error:badarg ->
        ?LEVEL_ERROR
    end.

set_level_integer(Int) ->
    gen_server:call(?MODULE, {set_level_integer, Int}).

handle_cast(#{level:=Level, msg:=Msg, meta:=Meta}, #state{sasl = true} = St) ->
    {ConMsg, FileMsg} = get_log_messages(Level, Msg, Meta),
    ok = disk_log:blog(?DISK_LOGGER, FileMsg),
    ok = io:put_chars(ConMsg),
    {noreply, St};
handle_cast(_, State) ->
    {noreply, State}.

handle_call({couch_error, ConMsg}, _From, State) ->
    ok = io:put_chars(ConMsg),
    {reply, ok, State};
handle_call({couch_info, ConMsg}, _From, #state{level = LogLevel} = State)
  when LogLevel =< ?LEVEL_INFO ->
    ok = io:put_chars(ConMsg),
    {reply, ok, State};
handle_call({couch_debug, ConMsg}, _From, #state{level = LogLevel} = State)
  when LogLevel =< ?LEVEL_DEBUG ->
    ok = io:put_chars(ConMsg),
    {reply, ok, State};
handle_call({set_level_integer, NewLevel}, _From, State) ->
    ets:insert(?MODULE, {level, NewLevel}),
    {reply, ok, State#state{level = NewLevel}}.

handle_info({disk_log, _Node, _Log, {error_status, Status}}, State) ->
    io:format("Disk logger error: ~p~n", [Status]),
    %% couch_primary_sup will restart us.
    {stop, {disk_log, {error, Status}}, State};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Arg, _State) ->
    logger:remove_handler(?MODULE),
    ok = disk_log:close(?DISK_LOGGER).

get_log_messages(Pid, Level, Format, Args) ->
    ConsoleMsg = unicode:characters_to_binary(io_lib:format(
        "[~s] [~p] " ++ Format ++ "~n", [Level, Pid | Args])),
    FileMsg = ["[", httpd_util:rfc1123_date(), "] ", ConsoleMsg],
    {ConsoleMsg, iolist_to_binary(FileMsg)}.

get_log_messages(Level, Msg, #{pid:=Pid} = Meta) ->
    {Format, Args} = get_log(Msg, Meta),
    get_log_messages(Pid, Level, Format, Args).

get_log({string, Msg}, _Meta) ->
    {"~ts", [Msg]};
get_log({report, Msg}, Meta) ->
    handle_report(Msg, Meta);
get_log({Format, Args}, _Meta) when is_list(Format), is_list(Args) ->
    {Format, Args}.

handle_report(Report, Meta) ->
    case maps:get(report_cb, Meta, fun logger:format_otp_report/1) of
        RCBFun when is_function(RCBFun, 1) ->
            try RCBFun(Report) of
                {F, A} when is_list(F), is_list(A) ->
                    {F, A};
                Other ->
                    {"REPORT_CB ERROR: ~tp; Returned: ~tp", [Report, Other]}
            catch C:R ->
                      {"REPORT_CB CRASH: ~tp; Reason: ~tp", [Report, {C, R}]}
             end;
        RCBFun when is_function(RCBFun, 2) ->
            try RCBFun(Report, #{depth => unlimited,
                                 chars_limit => unlimited,
                                 single_line => false}) of
                Chardata when (is_list(Chardata) orelse is_binary(Chardata)) ->
                    {"~ts", [Chardata]};
                Other ->
                    {"REPORT_CB ERROR: ~tp; Returned: ~tp", [Report, Other]}
            catch C:R ->
                      {"REPORT_CB CRASH: ~tp; Reason: ~tp", [Report, {C, R}]}
             end
     end.

read(Bytes, Offset) ->
    LogFileName = couch_config:get("log", "file"),
    LogFileSize = filelib:file_size(LogFileName),

    {ok, Fd} = file2:open(LogFileName, [read]),
    Start = lists:max([LogFileSize - Bytes, 0]) + Offset,

    % TODO: truncate chopped first line
    % TODO: make streaming

    {ok, Chunk} = file:pread(Fd, Start, LogFileSize),
    ok = file:close(Fd),
    Chunk.

pre_db_open(_) ->
    ok.
