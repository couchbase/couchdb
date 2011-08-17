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

-module(couch_access_log).
-behaviour(gen_event).

% public API
-export([start_link/0, stop/0]).
-export([log/3]).

% gen_event callbacks
-export([init/1, handle_event/2, terminate/2, code_change/3]).
-export([handle_info/2, handle_call/2]).

-define(DISK_LOGGER, couch_disk_access_logger).
-define(DEFAULT_FORMAT, "extended").

log(MochiReq, Code, Body) ->
    case is_enabled() of
    false -> ok;
    _True ->
        FileMsg = get_log_messages(MochiReq, Code, Body),
        ok = disk_log:balog(?DISK_LOGGER, FileMsg)
    end.

start_link() ->
    couch_event_sup:start_link({local, couch_access_log}, error_logger, couch_access_log, []).

stop() ->
    couch_event_sup:stop(couch_access_log).

init([]) ->
    % read config and register for configuration changes

    % just stop if one of the config settings change. couch_server_sup
    % will restart us and then we will pick up the new settings.
    ok = couch_config:register(
        fun("access_log", "enable") ->
            ?MODULE:stop();
        ("access_log", "file") ->
            ?MODULE:stop();
        ("access_log", "format") ->
            ?MODULE:stop()
        end),

    Enable = couch_config:get("access_log", "enable", "false") =:= "true",
    Format = couch_config:get("access_log", "format", ?DEFAULT_FORMAT),

    case ets:info(?MODULE) of
    undefined -> ets:new(?MODULE, [named_table]);
    _ -> ok
    end,
    ets:insert(?MODULE, {enable, Enable}),
    ets:insert(?MODULE, {format, Format}),

    Filename = couch_config:get("access_log", "file", "couchdb_access.log"),
    DiskLogOptions = [
        {file, Filename}, {name, ?DISK_LOGGER},
        {format, external}, {type, halt}, {notify, true}
    ],
    case disk_log:open(DiskLogOptions) of
    {ok, ?DISK_LOGGER} ->
        {ok, ok};
    Error ->
        {stop, Error}
    end.

is_enabled() ->
    try
        ets:lookup_element(?MODULE, enable, 2)
    catch error:badarg ->
        false
    end.

get_format() ->
    try
        ets:lookup_element(?MODULE, format, 2)
    catch error:badarg ->
        ?DEFAULT_FORMAT
    end.


handle_event({couch_access, ConMsg}, State) ->
    ok = io:put_chars(ConMsg),
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_info({disk_log, _Node, _Log, {error_status, Status}}, _State) ->
    io:format("Access Log disk logger error: ~p~n", [Status]),
    % couch_event_sup will restart us.
    remove_handler;
handle_info(_Info, State) ->
    {ok, State}.

handle_call(_Args, State) ->
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok = disk_log:close(?DISK_LOGGER).

get_date() ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:universal_time(),
    io_lib:format("[~w/~w/~w:~w:~w:~w +00:00]", [Day, Month, Year, Hour, Minute, Second]).

get_path_info(MochiReq) ->
    "\""
    ++ atom_to_list(MochiReq:get(method))
    ++ " "
    ++ MochiReq:get(path)
    ++ " HTTP/"
    ++ get_version(MochiReq) ++ "\"".

get_version(MochiReq) ->
    case MochiReq:get(version) of
	{1, 0} -> "1.0";
	{1, 1} -> "1.1"
    end.

get_auth_user(MochiReq) ->
    case MochiReq:get_header_value("Authorization") of
    undefined -> "-";
    "Basic " ++ UserPass64 ->
        UserPass = base64:decode(UserPass64),
        [User, _Pass] = string:tokens(UserPass, ":"),
	binary_to_list(User)
    end.

get_log_messages(MochiReq, Code, [_, Body, _, _]) ->
    get_log_messages(MochiReq, Code, Body);
get_log_messages(MochiReq, Code, Body) ->
    BaseFormat = [
      MochiReq:get(peer),
      "-", % ident, later
      get_auth_user(MochiReq),
      get_date(),
      get_path_info(MochiReq),
      io_lib:format("~w", [Code]),
      case Body of
      chunked -> "chunked";
      _Else -> io_lib:format("~w", [iolist_size(Body) + 1]) % +1 for \n, maybe
      end
      ],
    Args = case get_format() of
    "common" ->
        BaseFormat;
    _Extended ->
        BaseFormat ++ [
	    get_header_value2(MochiReq, "Referer"),
	    get_header_value2(MochiReq, "User-agent")
	]
    end,
    string:join(Args ++ ["\n"], " ").

get_header_value2(MochiReq, Header) ->
    case MochiReq:get_header_value(Header) of
        undefined -> "-";
        Value -> Value
    end.
