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

-module(couch_api_wrap).

% This module wraps the native erlang API, and allows for performing
% operations on a remote vs. local databases via the same API.
%
% Notes:
% Many options and apis aren't yet supported here, they are added as needed.

-include("couch_db.hrl").
-include("couch_api_wrap.hrl").

-export([
    db_open/2,
    db_open/3,
    db_close/1,
    get_db_info/1,
    update_doc/3,
    update_docs/3,
    update_docs/4,
    ensure_full_commit/1,
    get_missing_revs/2,
    open_doc/3,
    open_doc/5,
    couch_doc_open/3,
    changes_since/5,
    db_uri/1
    ]).

-import(couch_api_wrap_httpc, [
    httpdb_setup/1,
    send_req/3
    ]).

-import(couch_util, [
    encode_doc_id/1,
    get_value/2,
    get_value/3
    ]).


db_uri(#httpdb{url = Url}) ->
    couch_util:url_strip_password(Url);

db_uri(#db{name = Name}) ->
    db_uri(Name);

db_uri(DbName) ->
    ?b2l(DbName).


db_open(Db, Options) ->
    db_open(Db, Options, false).

db_open(#httpdb{} = Db1, _Options, Create) ->
    {ok, Db} = couch_api_wrap_httpc:setup(Db1),
    case Create of
    false ->
        ok;
    true ->
        send_req(Db, [{method, put}], fun(_, _, _) -> ok end)
    end,
    send_req(Db, [{method, head}],
        fun(200, _, _) ->
            {ok, Db};
        (401, _, _) ->
            throw({unauthorized, ?l2b(db_uri(Db))});
        (_, _, _) ->
            throw({db_not_found, ?l2b(db_uri(Db))})
        end);
db_open(DbName, Options, Create) ->
    try
        case Create of
        false ->
            ok;
        true ->
            ok = couch_httpd:verify_is_server_admin(
                get_value(user_ctx, Options)),
            couch_db:create(DbName, Options)
        end,
        case couch_db:open(DbName, Options) of
        {not_found, _Reason} ->
            throw({db_not_found, DbName});
        {ok, _Db} = Success ->
            Success
        end
    catch
    throw:{unauthorized, _} ->
        throw({unauthorized, DbName})
    end.

db_close(#httpdb{httpc_pool = Pool}) ->
    unlink(Pool),
    ok = couch_httpc_pool:stop(Pool);
db_close(DbName) ->
    catch couch_db:close(DbName).


get_db_info(#httpdb{} = Db) ->
    send_req(Db, [],
        fun(200, _, {Props}) ->
            {ok, Props}
        end);
get_db_info(#db{name = DbName, user_ctx = UserCtx}) ->
    {ok, Db} = couch_db:open(DbName, [{user_ctx, UserCtx}]),
    {ok, Info} = couch_db:get_db_info(Db),
    couch_db:close(Db),
    {ok, [{couch_util:to_binary(K), V} || {K, V} <- Info]}.


ensure_full_commit(#httpdb{} = Db) ->
    send_req(
        Db,
        [{method, post}, {path, "_ensure_full_commit"},
            {headers, [{"Content-Type", "application/json"}]}],
        fun(201, _, {Props}) ->
            {ok, get_value(<<"instance_start_time">>, Props)};
        (_, _, {Props}) ->
            {error, get_value(<<"error">>, Props)}
        end);
ensure_full_commit(Db) ->
    couch_db:ensure_full_commit(Db).


get_missing_revs(#httpdb{} = Db, IdRevList) ->
    JsonBody = {[{Id, couch_doc:rev_to_str(Rev)} || {Id, Rev} <- IdRevList]},
    send_req(
        Db,
        [{method, post}, {path, "_revs_diff"}, {body, ?JSON_ENCODE(JsonBody)}],
        fun(200, _, {Props}) ->
            ConvertToNativeFun = fun({Id, {Result}}) ->
                MissingRev = couch_doc:parse_rev(
                    get_value(<<"missing">>, Result)
                ),
                {Id, MissingRev}
            end,
            {ok, lists:map(ConvertToNativeFun, Props)}
        end);
get_missing_revs(Db, IdRevList) ->
    couch_db:get_missing_revs(Db, IdRevList).


open_doc(#httpdb{}, _Id, _Options, Fun, Acc) ->
    {ok, Fun({error, <<"not_found">>}, Acc)};
open_doc(Db, Id, Options, Fun, Acc) ->
    {ok, Result} = couch_db:open_doc(Db, Id, Options),
    {ok, Fun(Result, Acc)}.


open_doc(#httpdb{}, _Id, _Options) ->
    {error, <<"not_found">>};
open_doc(Db, Id, Options) ->
    case couch_db:open_doc(Db, Id, Options) of
    {ok, _} = Ok ->
        Ok;
    {not_found, _Reason} ->
        {error, <<"not_found">>}
    end.


couch_doc_open(Db, DocId, Options) ->
    case open_doc(Db, DocId, Options) of
    {ok, Doc} ->
        Doc;
     Error ->
        throw(Error)
    end.


update_doc(Db, Doc, Options) ->
    couch_db:update_doc(Db, Doc, Options).

update_docs(Db, DocList, Options) ->
    ok = couch_db:update_docs(Db, DocList, Options).

update_docs(Db, DocList, Options, replicated_changes) ->
    ok = couch_db:update_docs(Db, DocList, Options),
    {ok, []}.


changes_since(#httpdb{headers = Headers1} = HttpDb, Style, StartSeq,
    UserFun, Options) ->
    BaseQArgs = case get_value(continuous, Options, false) of
    false ->
        [{"feed", "normal"}];
    true ->
        [{"feed", "continuous"}, {"heartbeat", "10000"}]
    end ++ [
        {"style", atom_to_list(Style)}, {"since", couch_util:to_list(StartSeq)}
    ],
    DocIds = get_value(doc_ids, Options),
    {QArgs, Method, Body, Headers} = case DocIds of
    undefined ->
        QArgs1 = maybe_add_changes_filter_q_args(BaseQArgs, Options),
        {QArgs1, get, [], Headers1};
    _ when is_list(DocIds) ->
        Headers2 = [{"Content-Type", "application/json"} | Headers1],
        JsonDocIds = ?JSON_ENCODE({[{<<"doc_ids">>, DocIds}]}),
        {[{"filter", "_doc_ids"} | BaseQArgs], post, JsonDocIds, Headers2}
    end,
    send_req(
        HttpDb,
        [{method, Method}, {path, "_changes"}, {qs, QArgs},
            {headers, Headers}, {body, Body},
            {ibrowse_options, [{stream_to, {self(), once}}]}],
        fun(200, _, DataStreamFun) ->
                parse_changes_feed(Options, UserFun, DataStreamFun);
            (405, _, _) when is_list(DocIds) ->
                % CouchDB versions < 1.1.0 don't have the builtin _changes feed
                % filter "_doc_ids" neither support POST
                send_req(HttpDb, [{method, get}, {path, "_changes"},
                    {qs, BaseQArgs}, {headers, Headers1},
                    {ibrowse_options, [{stream_to, {self(), once}}]}],
                    fun(200, _, DataStreamFun2) ->
                        UserFun2 = fun(#doc_info{id = Id} = DocInfo) ->
                            case lists:member(Id, DocIds) of
                            true ->
                                UserFun(DocInfo);
                            false ->
                                ok
                            end
                        end,
                        parse_changes_feed(Options, UserFun2, DataStreamFun2)
                    end)
        end);
changes_since(Db, Style, StartSeq, UserFun, Options) ->
    Filter = case get_value(doc_ids, Options) of
    undefined ->
        ?b2l(get_value(filter, Options, <<>>));
    _DocIds ->
        "_doc_ids"
    end,
    Args = #changes_args{
        style = Style,
        since = StartSeq,
        filter = Filter,
        feed = case get_value(continuous, Options, false) of
            true ->
                "continuous";
            false ->
                "normal"
        end,
        timeout = infinity
    },
    QueryParams = get_value(query_params, Options, {[]}),
    Req = changes_json_req(Db, Filter, QueryParams, Options),
    ChangesFeedFun = couch_changes:handle_changes(Args, {json_req, Req}, Db),
    ChangesFeedFun(fun({change, Change, _}, _) ->
            UserFun(json_to_doc_info(Change));
        (_, _) ->
            ok
    end).


% internal functions

maybe_add_changes_filter_q_args(BaseQS, Options) ->
    case get_value(filter, Options) of
    undefined ->
        BaseQS;
    FilterName ->
        {Params} = get_value(query_params, Options, {[]}),
        [{"filter", ?b2l(FilterName)} | lists:foldl(
            fun({K, V}, QSAcc) ->
                Ks = couch_util:to_list(K),
                case lists:keymember(Ks, 1, QSAcc) of
                true ->
                    QSAcc;
                false ->
                    [{Ks, couch_util:to_list(V)} | QSAcc]
                end
            end,
            BaseQS, Params)]
    end.

parse_changes_feed(Options, UserFun, DataStreamFun) ->
    case get_value(continuous, Options, false) of
    true ->
        continuous_changes(DataStreamFun, UserFun);
    false ->
        EventFun = fun(Ev) ->
            changes_ev1(Ev, fun(DocInfo, _) -> UserFun(DocInfo) end, [])
        end,
        json_stream_parse:events(DataStreamFun, EventFun)
    end.

changes_json_req(_Db, "", _QueryParams, _Options) ->
    {[]};
changes_json_req(_Db, "_doc_ids", _QueryParams, Options) ->
    {[{<<"doc_ids">>, get_value(doc_ids, Options)}]};
changes_json_req(Db, FilterName, {QueryParams}, _Options) ->
    {ok, Info} = couch_db:get_db_info(Db),
    % simulate a request to db_name/_changes
    {[
        {<<"info">>, {Info}},
        {<<"id">>, null},
        {<<"method">>, 'GET'},
        {<<"path">>, [couch_db:name(Db), <<"_changes">>]},
        {<<"query">>, {[{<<"filter">>, FilterName} | QueryParams]}},
        {<<"headers">>, []},
        {<<"body">>, []},
        {<<"peer">>, <<"replicator">>},
        {<<"form">>, []},
        {<<"cookie">>, []},
        {<<"userCtx">>, couch_util:json_user_ctx(Db)}
    ]}.


changes_ev1(object_start, UserFun, UserAcc) ->
    fun(Ev) -> changes_ev2(Ev, UserFun, UserAcc) end.

changes_ev2({key, <<"results">>}, UserFun, UserAcc) ->
    fun(Ev) -> changes_ev3(Ev, UserFun, UserAcc) end;
changes_ev2(_, UserFun, UserAcc) ->
    fun(Ev) -> changes_ev2(Ev, UserFun, UserAcc) end.

changes_ev3(array_start, UserFun, UserAcc) ->
    fun(Ev) -> changes_ev_loop(Ev, UserFun, UserAcc) end.

changes_ev_loop(object_start, UserFun, UserAcc) ->
    fun(Ev) ->
        json_stream_parse:collect_object(Ev,
            fun(Obj) ->
                UserAcc2 = UserFun(json_to_doc_info(Obj), UserAcc),
                fun(Ev2) -> changes_ev_loop(Ev2, UserFun, UserAcc2) end
            end)
    end;
changes_ev_loop(array_end, _UserFun, _UserAcc) ->
    fun(_Ev) -> changes_ev_done() end.

changes_ev_done() ->
    fun(_Ev) -> changes_ev_done() end.

continuous_changes(DataFun, UserFun) ->
    {DataFun2, _, Rest} = json_stream_parse:events(
        DataFun,
        fun(Ev) -> parse_changes_line(Ev, UserFun) end),
    continuous_changes(fun() -> {Rest, DataFun2} end, UserFun).

parse_changes_line(object_start, UserFun) ->
    fun(Ev) ->
        json_stream_parse:collect_object(Ev,
            fun(Obj) -> UserFun(json_to_doc_info(Obj)) end)
    end.

json_to_doc_info({Props}) ->
    {Change} = get_value(<<"changes">>, Props),
    Rev = couch_doc:parse_rev(get_value(<<"rev">>, Change)),
    Del = (true =:= get_value(<<"deleted">>, Change)),
    #doc_info{
        id = get_value(<<"id">>, Props),
        local_seq = get_value(<<"seq">>, Props),
        rev = Rev,
        deleted = Del
    }.

