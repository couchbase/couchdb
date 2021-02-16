%% @author Couchbase <info@couchbase.com>
%% @copyright 2019 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc couch audit
%%

-module(couch_audit).
-behaviour(gen_server).

-include("couch_db.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).
-export([config_change/2]).
-export([audit_view_delete/5, audit_view_query_request/4,
         audit_view_meta_query/4, audit_view_create_update/5]).
-export([log_error/4]).

-record(state, {
    audit_enabled = false,
    enabled_events = [],
    disabled_userid = [],
    memcached_socket = no_socket
}).

code(ddoc_created)->
    40960;
code(ddoc_deleted) ->
    40961;
code(query_meta_data) ->
    40962;
code(query_view) ->
    40963;
code(ddoc_updated) ->
    40964;
code(config_changed) ->
    40965.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

default() ->
    "[{audit_enabled,false},
     {disabled_users,[]},
     {enabled_events,[40960,40961,40962,40964]}]".

init([]) ->
    {ok, AuditList} = couch_util:parse_term(couch_config:get("security","audit", default())),
    State = set_audit_values(AuditList, #state{}),
    ok = couch_config:register(fun ?MODULE:config_change/2),
    {ok, State}.

handle_call({audit, NewAuditSettings}, _From, State) ->
    {reply, true, set_audit_values(NewAuditSettings, State)};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({log, {config_changed, Settings}}, #state{audit_enabled=true,
                                memcached_socket=Socket}=State) ->
    Socket2 = try_connecting_memcached(Socket),
    memcached_calls:audit_put(Socket2, code(config_changed), Settings),
    {noreply, State#state{memcached_socket=Socket2}};
handle_cast({log, {Opcode, Req, Body}},  #state{audit_enabled=true,
                                enabled_events=EnabledEvents,
                                disabled_userid=DisabledUserId,
                                memcached_socket=Socket}=State) ->
    Data = prepare(Req, Body),
    Socket2 = try_connecting_memcached(Socket),

    Opcode2 = code(Opcode),
    case check_logging(Opcode2, Data, EnabledEvents, DisabledUserId) of
    true ->
        memcached_calls:audit_put(Socket2, Opcode2, Data);
    false ->
        ok
    end,
    {noreply, State#state{memcached_socket=Socket2}};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

set_audit_values(List, State) ->
    AuditEnabled = couch_util:get_value(audit_enabled, List, false),
    DisabledUser = couch_util:get_value(disabled_users, List, []),
    EnabledEvents = couch_util:get_value(enabled_events, List, [40960,40961,40962]),
    DisabledUser2 = case DisabledUser of
                    undefined -> [];
                    _ -> DisabledUser
                    end,
    EnabledEvents2 = case EnabledEvents of
                     undefined -> [];
                     _ -> EnabledEvents
                     end,
    State#state{audit_enabled=AuditEnabled,
           disabled_userid= DisabledUser2,
           enabled_events = EnabledEvents2
          }.

config_change("security", "audit") ->
    {ok, AuditList} = couch_util:parse_term(couch_config:get("security","audit", default())),
    case gen_server:call(?MODULE, {audit, AuditList}) of
    true -> audit_config_changed(AuditList);
    false -> ok
    end.

audit_config_changed(NewSettings) ->
    Settings = [prepare_audit_setting(S) || S <- NewSettings],
    gen_server:cast(?MODULE, {log, {config_changed, Settings}}).

prepare_audit_setting({enabled_events, List}) ->
    {enabled, to_binary({list,List})};
prepare_audit_setting({disabled_users, Users}) ->
    {disabled_userids, to_binary({list, [get_user_identity(U) || U <- Users]})};
prepare_audit_setting(Setting) ->
    Setting.

check_logging(Opcode, Message, EnabledEvents, DisabledUsers) ->
    RealUserIdCheck = case couch_util:get_value(real_userid, Message) of
                      undefined -> true;
                      {[{domain, Domain}, {user, UserId}]} ->
                        not lists:member({?b2l(UserId), Domain}, DisabledUsers)
                      end,
    lists:member(Opcode, EnabledEvents) andalso RealUserIdCheck.

get_real_user_id(Req) ->
    case mochiweb_request:get_header_value("authorization", Req) of
    "Basic " ++ Value ->
        {Auth, Password} = parse_basic_auth_header(Value),
        menelaus_auth:authenticate({Auth, Password});
    undefined ->
        case mochiweb_request:get_header_value("ns-server-auth-token", Req) of
        undefined ->
            undefined;
        Token ->
            menelaus_auth:authenticate({token, Token})
        end;
    _ ->
        error
    end.

get_user_key(Req) ->
    case mochiweb_request:get_header_value("authorization", Req) of
    "Basic " ++ Value ->
        parse_basic_auth_header(Value);
    "SCRAM-" ++ Value ->
        {scram_sha, Value};
    undefined ->
        {undefined, undefined};
    _ ->
        {undefined, undefined}
    end.

parse_basic_auth_header(Value) ->
    case (catch base64:decode_to_string(Value)) of
        Auth when is_list(Auth) ->
            case string:chr(Auth, $:) of
            0 ->
                {undefined, undefined};
            I ->
                {string:substr(Auth, 1, I-1), string:substr(Auth, I + 1)}
            end;
        _ ->
            {"", ""}
    end.

audit_view_create_update(#httpd{path_parts = PathParts} = Req, Code, ErrorStr, ReasonStr, OldDDoc) ->
    {BucketName, DDocName} = parse_path(PathParts),
    Body = [{timestamp, now_to_iso8601(os:timestamp())},
            {bucket, BucketName}, {ddoc_name, DDocName},
            {method, put}, {status, Code},
            {error, ErrorStr},{reason, ReasonStr}],

    ViewBody = get_req_view_definition(Req),

    Body2 = [{view_definition, ViewBody} | Body],
    {Msg, Body3} = case OldDDoc of
    not_found ->
        case ErrorStr of
        <<"invalid_design_document">> ->
            case ReasonStr of
            <<"Content is not json.">> ->
                {ddoc_created, Body2};
            _ ->
                case couch_set_view_ddoc_cache:get_ddoc(BucketName, <<"_design/", DDocName/binary>>) of
                {ok, DDoc} ->
                    {Created, Modified, Deleted} = compare_view_definition(ViewBody, DDoc#doc.body),
                    {ddoc_updated, [{old_view_definition, DDoc#doc.body}, {new_views, {list, Created}},
                            {modified_views, {list, Modified}}, {deleted_views, {list, Deleted}}| Body2]};
                {doc_open_error, _} ->
                    {ddoc_created, Body2}
                end
            end;
        _ ->
            {ddoc_created, Body2}
        end;
    _ ->
        {Created, Modified, Deleted} = compare_view_definition(ViewBody, OldDDoc),
        {ddoc_updated, [{old_view_definition, OldDDoc}, {new_views, {list, Created}},
                        {modified_views, {list, Modified}}, {deleted_views, {list, Deleted}}| Body2]}
    end,
    gen_server:cast(?MODULE, {log, {Msg, Req, Body3}}).

audit_view_delete(#httpd{path_parts = PathParts} = Req, Code, ErrorStr, ReasonStr, OldDDoc) ->
    {BucketName, DDocName} = parse_path(PathParts),
    DDocDeleted = case OldDDoc of
                    not_found -> undefined;
                    _ -> OldDDoc
                    end,
    Body = [{timestamp, now_to_iso8601(os:timestamp())},
            {bucket, BucketName}, {ddoc_name, DDocName},
            {deleted_ddoc_definition, DDocDeleted},
            {method, delete}, {status, Code},
            {error, ErrorStr},{reason, ReasonStr}],
    gen_server:cast(?MODULE, {log, {ddoc_deleted, Req, Body}}).

audit_view_meta_query(#httpd{path_parts = PathParts} = Req, Code, ErrorStr, ReasonStr) ->
    {BucketName, DDocName} = parse_path(PathParts),
    Body = [{timestamp, now_to_iso8601(os:timestamp())},
            {bucket, BucketName}, {ddoc_name, DDocName},
            {method, get}, {status, Code},
            {error, ErrorStr},{reason, ReasonStr}],
    gen_server:cast(?MODULE, {log, {query_meta_data, Req, Body}}).

audit_view_query_request(Req, Code, ErrorStr, ReasonStr) ->
    {Origin, BucketName, DDocName, ViewName, Parameters} = case query_params(Req) of
    error -> {undefined, undefiend, undefined, undefined, undefined};
    Result -> Result
    end,

    DDocName2 = case DDocName of
    undefined ->
        undefined;
    <<"_design/", _/binary>> ->
        DDocName;
    _ ->
        <<"_design/", DDocName/binary>>
    end,

    ViewDef = couch_set_view_ddoc_cache:get_view(BucketName, DDocName2, ViewName),
    Body = [{timestamp, now_to_iso8601(os:timestamp())},
            {status, Code}, {view_definition, ViewDef}, {query_parameters,
            {propset,Parameters}}, {request_type, Origin},
            {bucket, BucketName}, {ddoc_name, DDocName},
            {view_name, ViewName},{error, ErrorStr},
            {reason, ReasonStr}],
    gen_server:cast(?MODULE, {log, {query_view, Req, Body}}).

format_iso8601({{YYYY, MM, DD}, {Hour, Min, Sec}}, Microsecs, Offset) ->
    io_lib:format("~4.4.0w-~2.2.0w-~2.2.0wT~2.2.0w:~2.2.0w:~2.2.0w.~3.3.0w",
                  [YYYY, MM, DD, Hour, Min, Sec, Microsecs div 1000]) ++ Offset.

now_to_iso8601(Now = {_, _, Microsecs}) ->
    LocalNow = calendar:now_to_local_time(Now),

    UTCSec = calendar:datetime_to_gregorian_seconds(calendar:now_to_universal_time(Now)),
    LocSec = calendar:datetime_to_gregorian_seconds(LocalNow),
    Offset =
        case (LocSec - UTCSec) div 60 of
            0 ->
                "Z";
            OffsetTotalMins ->
                OffsetHrs = OffsetTotalMins div 60,
                OffsetMin = abs(OffsetTotalMins rem 60),
                OffsetSign = case OffsetHrs < 0 of
                                 true ->
                                     "-";
                                 false ->
                                     "+"
                             end,
                io_lib:format("~s~2.2.0w:~2.2.0w", [OffsetSign, abs(OffsetHrs), OffsetMin])
        end,
    format_iso8601(LocalNow, Microsecs, Offset).

prepare(#httpd{mochi_req=Req}, Body) ->
    {Remote, {Auth, _Password}, UserAgent} =
        case Req of
            undefined ->
                {undefined, {undefined, undefined}, undefined};
            _ ->
                {get_remote(Req),
                 get_user_key(Req),
                 get_user_agent(Req)
                }
        end,
    UserId = case get_real_user_id(Req) of
    {ok, {User, Domain}} ->
        {propset, [{domain, convert_domain(Domain)}, {user, to_binary(User)}]};
    _ ->
        undefined
    end,

    Body2 = [{remote, Remote},
            {real_userid, UserId},
            {auth, Auth},
            {user_agent, UserAgent}| Body],
    prepare_list(Body2).

prepare_list(List) ->
    lists:foldl(
      fun ({_Key, undefined}, Acc) ->
              Acc;
          ({_Key, "undefined"}, Acc) ->
              Acc;
          ({_Key, <<>>}, Acc) ->
              Acc;
          ({_Key, []}, Acc) ->
              Acc;
          ({Key, Value}, Acc) ->
              [{key_to_binary(Key), to_binary(Value)} | Acc]
      end, [], List).

get_user_agent(Req) ->
    case mochiweb_request:get_header_value("user-agent", Req) of
    undefined ->
        case mochiweb_request:get_header_value("ns-server-ui", Req) of
        "yes" ->
            "ns_server_ui";
        _ ->
            ""
        end;
    User_Agent ->
        User_Agent
    end.

get_remote(Req) ->
    {Ip2, Port2} = case mochiweb_request:get_header_value("ns-server-ui", Req) of
    "yes" ->
        List = mochiweb_request:get_header_value("Forwarded", Req),
        case parse_for_value(List) of
        undefined ->
            Socket = mochiweb_request:get(socket, Req),
            extract_hostport(Socket);
        Remote ->
            Remote
        end;
    _ ->
        Socket = mochiweb_request:get(socket, Req),
        extract_hostport(Socket)
    end,
    case {Ip2, Port2} of
    {undefined, undefined} ->
        undefined;
    {_,_} ->
        {[{ip, to_binary(Ip2)}, {port, Port2}]}
    end.

%% peername calls ssl:peername which can einval if client's socket is prematurely closed
extract_hostport(Socket) ->
    case mochiweb_socket:peername(Socket) of
    {ok, {Host, Port}} ->
        {inet_parse:ntoa(Host), Port};
    _ -> {undefined, undefined}
    end.

parse_for_value(undefined) ->
    undefined;
parse_for_value(List) ->
    Tokens = string:tokens(List, ";"),
    case couch_util:find_match(Tokens, "for=", undefined) of
    undefined ->
        undefined;
    Matched ->
        case string:split(Matched, ":", trailing) of
        [Host, Port] ->
            {Host, list_to_integer(Port)};
        _ ->
            undefined
        end
    end.

to_binary({_Key, undefined}) ->
    [];
to_binary({list, List}) ->
    [to_binary(Term) || Term <- List];
to_binary(List) when is_list(List) ->
    iolist_to_binary(List);
to_binary({propset, <<>>}) ->
    [];
to_binary({propset, Props}) when is_list(Props) ->
    {[kv_to_binary(Kv) || Kv <- Props]};
to_binary(Term) ->
    Term.

kv_to_binary({Key, Value}) ->
    {key_to_binary(Key), to_binary(Value)}.

key_to_binary(List) when is_list(List) ->
    iolist_to_binary(List);
key_to_binary(Term) when is_tuple(Term) ->
    iolist_to_binary(io_lib:format("~p", [Term]));
key_to_binary(Term) ->
    Term.

get_user_identity(undefined) ->
    undefined;
get_user_identity({User, Domain}) ->
    {[{domain, convert_domain(Domain)}, {user, to_binary(User)}]}.

convert_domain(admin) ->
    builtin;
convert_domain(ro_admin) ->
    builtin;
convert_domain(D) ->
    D.

query_params(#httpd{path_parts=Parts} = Req) ->
    try
        case Parts of
        [BucketName, <<"_design">>, DDocName, <<"_view">>, ViewName] ->
            {external, BucketName, DDocName, ViewName, couch_httpd:qs(Req)};
        _ ->
        [<<"/">>, BucketName, <<"/">>, DDocName, <<"/_view/">>, ViewName] =
                                                                couch_util:log_do_parse(Req),
            {internal, BucketName, DDocName, ViewName, couch_httpd:qs(Req)}
        end
    catch
        _:_ ->
            error
    end.

log_error(#httpd{method = Method}=Req, Code, ErrorStr, ReasonStr) ->
    case Method of
    'PUT' ->
        audit_view_create_update(Req, Code, ErrorStr, ReasonStr, not_found);
    'GET' ->
        audit_view_query_request(Req, Code,ErrorStr, ReasonStr);
    'DELETE' ->
        audit_view_delete(Req, Code, ErrorStr, ReasonStr, not_found);
    _ ->
        ok
    end.

parse_path(Path) ->
    case Path of
        [BucketName, <<"_design">>, DDocName | _] -> {BucketName, DDocName};
        [BucketName | _ ] -> {BucketName, undefined};
        _ -> {undefined, undefined}
    end.

try_connecting_memcached(Socket) ->
    case Socket of
    no_socket ->
        memcached_calls:connect_memcached(3);
    _ ->
        Socket
    end.

compare_view_definition(NewViewDef1, OldViewDef1) ->
    OldViewDef = couch_util:get_view_list(OldViewDef1),
    NewViewDef = couch_util:get_view_list(NewViewDef1),
    {Created, Modified} = lists:foldl(fun({ViewName, ViewDef}, {Created, Modified}) ->
                                        case couch_util:get_value(ViewName, OldViewDef) of
                                        undefined ->
                                            {[ViewName | Created], Modified};
                                        OldView ->
                                            case compare(ViewDef, OldView) of
                                                true -> {Created, [ViewName | Modified]};
                                                false -> {Created, Modified}
                                            end
                                        end
                          end, {[],[]}, NewViewDef),
    Deleted = lists:foldl(fun({ViewName, _}, Deleted) ->
                            case couch_util:get_value(ViewName, NewViewDef) of
                            undefined -> [ViewName | Deleted];
                            _ -> Deleted
                           end
              end, [], OldViewDef),
    {Created, Modified, Deleted}.

compare({ViewDef}, {OldView}) ->
    lists:foldl(fun({Type, Def}, Modified) ->
                    case couch_util:get_value(Type, OldView) of
                        undefined -> true;
                        Value -> case Value of
                                    Def -> Modified;
                                    _ -> true
                                end
                        end
    end, false, ViewDef);
compare(_, _) ->
    false.

get_req_view_definition(#httpd{req_body = undefined} = Req) ->
    case couch_httpd:is_ctype(Req, "application/json") of
    true ->
        try couch_httpd:json_body(Req) of
        Definition -> Definition
        catch _:_ -> "{}"
        end;
    false ->
        couch_httpd:body(Req)
    end;
get_req_view_definition(#httpd{req_body = RequestBody}) ->
    RequestBody.
