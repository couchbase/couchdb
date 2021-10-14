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

-module(couch_system_event).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([ddoc_created/3, ddoc_deleted/2, ddoc_modified/3]).
-export([settings_changed/4]).
-include("couch_db.hrl").

-record(state, {
    url = undefined,
    auth_header = [],
    queue
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    State = #state{
                queue = queue:new()
            },
    {ok, State}.

handle_call({system_event, Data}, _, #state{queue = Queue} = State) ->
    NewQueue = queue_put(Queue, Data),
    State2 = State#state{queue=NewQueue},
    self() ! send,
    {reply, ok, State2};
handle_call(_, _, State) ->
    {reply, ignored, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(send, #state{url=Url, auth_header=Headers, queue=Queue}=State) ->
    {Url3, Headers4, Queue3, Timer2} = case fill_headers_url(Url, Headers) of
                        {error, Url2, Header2} ->
                            {Url2, Header2, Queue, 1000};
                        {ok, Url2, Headers2} ->
                            {Headers3, Queue2, Time} = log_system_event(Url2, Headers2, Queue),
                            {Url2, Headers3, Queue2, Time}
                        end,
    case Timer2 of
    undefined -> ok;
    _ -> erlang:send_after(Timer2, self(), send)
    end,
    {noreply, State#state{url=Url3, auth_header= Headers4, queue=Queue3}};
handle_info(_Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

fill_headers_url(undefined, Headers) ->
    case get_system_log_url() of
    undefined ->
        {error, undefined, Headers};
    Url ->
        fill_headers_url(Url, Headers)
    end;
fill_headers_url(Url, undefined) ->
    case get_auth_header() of
    undefined ->
        {error, Url, undefined};
    Headers ->
        fill_headers_url(Url, Headers)
    end;
fill_headers_url(Url, Header) ->
    {ok, Url, Header}.

log_system_event(Url, Headers, Queue) ->
    case queue:out(Queue) of
    {empty, Queue} ->
        {Headers, Queue, undefined};
    {{value, Log}, NewQueue} ->
        case send_system_event(Url, Headers, Log, 5, undefined) of
        {retry_after, Time} ->
            {Headers, Queue, Time};
        %% This can be due to econnrefused or some issue with the connection
        %% Try the request after 1 sec backoff
        {error, _} ->
            {Headers, Queue, 1000};
        %% Issue with the format of the log
        %% Dump it into couchdb log
        {server_error, Error} ->
            ?LOG_ERROR("Unable to send system event: ~p, Log: <ud>~p</ud>", [Error, Log]),
            log_system_event(Url, Headers, NewQueue);
        {ok, Headers2} ->
            log_system_event(Url, Headers2, NewQueue)
        end
    end.

send_system_event(_, _, _, 0, Err) ->
    {error, Err};
send_system_event(Url, Header, Log, N, _) ->
    case lhttpc:request(Url, "POST", Header, Log, 60) of
    {ok, {{200, _}, _RespHeaders, _Body}} ->
        {ok, Header};
    {error, Err2} ->
        timer:sleep(10),
        send_system_event(Url, Header, Log, N-1, Err2);
    {ok, {{Code, _}, RespHeaders, Body}} ->
        case retry_error(Code, RespHeaders) of
        {retry_after, Time} ->
            {retry_after, Time};
        {fetch_header, Header2} ->
            send_system_event(Url, Header2, Log, N-1, Body);
        %% error due to unable to fetch the header
        %% retry it after 1 second
        {error, Body} ->
            {retry_after, 1000};
        false ->
            {server_error, Body}
        end
    end.

retry_error(503, RespHeaders) ->
    Retry = case lists:keyfind("Retry-After", 1, RespHeaders) of
        false ->
            1000;
        {_, Time} ->
            list_to_integer(Time)*1000
        end,
    {retry_after, Retry};
retry_error(401, _) ->
    case get_auth_header() of
    undefined ->
        {error, <<"Unable to fetch auth headers">>};
    Headers ->
        {fetch_header, Headers}
    end;
retry_error(403, _) ->
    case get_auth_header() of
    undefined ->
        {error, <<"Unable to fetch auth headers">>};
    Headers ->
        {fetch_header, Headers}
    end;
retry_error(_, _) ->
    false.

queue_put(Queue, Data) ->
    Queue2 =
        case queue:len(Queue) > 1000 of
            true ->
                ?LOG_ERROR("Dropping system events to info logs", []),
                %% Drop queue messages till queue size becomes 500
                Drop = queue:len(Queue) - 500,
                drop_system_events(Queue, Drop);
            false ->
                Queue
        end,
    queue:in(Data, Queue2).

drop_system_events(Queue, 0) ->
    Queue;
drop_system_events(Queue, N) ->
    case queue:out(Queue) of
    {empty, _} ->
        queue:new();
    {{value, Log}, NewQueue} ->
        ?LOG_INFO("Dropped system events: <ud>~p</ud>", [Log]),
        drop_system_events(NewQueue, N-1)
    end.

event(ddoc_created) ->
    {10240, <<"Ddoc created">>, views, info};
event(ddoc_deleted) ->
    {10241, <<"Ddoc deleted">>, views, info};
event(ddoc_modified) ->
    {10242, <<"Ddoc modified">>, views, info};
event(settings_changed) ->
    {10243, <<"view engine settings changed">>, views, info}.

settings_changed(Section, Key, OldValue, NewValue) ->
    system_log(settings_changed, [couch_util:to_json_key_value(section, Section),
                    couch_util:to_json_key_value(key, Key),
                    couch_util:to_json_key_value(old_value, OldValue),
                    couch_util:to_json_key_value(new_value, NewValue)]).

ddoc_created(Bucket, DDocId, NumViews) ->
    system_log(ddoc_created, [couch_util:to_json_key_value(bucket, Bucket),
                    couch_util:to_json_key_value(ddoc, DDocId),
                    couch_util:to_json_key_value(num_views, NumViews)]).

ddoc_deleted(Bucket, DDocId) ->
    system_log(ddoc_deleted, [couch_util:to_json_key_value(bucket, Bucket),
                    couch_util:to_json_key_value(ddoc, DDocId)]).

ddoc_modified(Bucket, DDocId, NumViews) ->
    system_log(ddoc_modified, [couch_util:to_json_key_value(bucket, Bucket),
                    couch_util:to_json_key_value(ddoc, DDocId),
                    couch_util:to_json_key_value(num_views, NumViews)]).

build_mandatory_attributes(Event) ->
    {Code, Desc, Comp, Level} = event(Event),
    [{event_id, Code}, {component, Comp},
     {description, Desc}, {severity, Level}].

build_extra_attributes(Extra) ->
    [{extra_attributes, {lists:flatten(Extra)}}].

system_log(Event, Extras) ->
    Timestamp = couch_util:timestamp_iso8601(erlang:timestamp()),
    Id = couch_uuids:uuid4(),

    Log = lists:flatten([couch_util:to_json_key_value(timestamp, Timestamp),
                         couch_util:to_json_key_value(uuid, Id),
                         build_mandatory_attributes(Event),
                         build_extra_attributes(Extras)]),

    Body = ejson:encode({Log}),
    gen_server:call(?MODULE, {system_event, Body}).

get_system_log_url() ->
    try
        Port = service_ports:get_port(rest_port),
        misc:local_url(Port, "/_event", [])
    catch
        _:_:_ ->
            undefined
    end.

get_auth_header() ->
    case get_auth(2) of
    undefined ->
        undefined;
    {User, Passwd} ->
        [menelaus_rest:basic_auth_header(User, Passwd),
         {"Content-Type","application/json"}]
    end.

get_auth(0) ->
    undefined;
get_auth(N) ->
    case cb_auth_info:get() of
    {auth, User, Passwd} ->
        {binary_to_list(User), binary_to_list(Passwd)};
    {error, server_not_ready} ->
        timer:sleep(1000),
        get_auth(N-1)
    end.
