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

-module(couch_index_merger).

-export([query_index/2, query_index/3]).

% Only needed for indexer implementation. Those functions should perhaps go into
% a utils module.
% The functions open_db/3, dec_counter/1, should_check_rev/2, get_ddoc/2 are
% also needed by this file
-export([open_db/3, collect_rows/4, collect_row_count/6,
    merge_indexes_no_acc/2, merge_indexes_no_limit/1, handle_skip/1,
    dec_counter/1, get_group_id/2, void_event/1, should_check_rev/2,
    ddoc_rev_str/1, ddoc_unchanged/2, validate_revision_param/1,
    rem_passwd/1, ddoc_not_found_msg/2]).

-include("couch_db.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").
-include_lib("couch_index_merger/include/couch_view_merger.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-import(couch_util, [
    get_value/2,
    to_binary/1
]).

-define(LOCAL, <<"local">>).

-define(RETRY_INTERVAL, 1000).
-define(MAX_RETRIES, 30).


query_index(Mod, #index_merge{http_params = HttpParams, user_ctx = UserCtx} = IndexMergeParams) when HttpParams =/= nil, UserCtx =/= nil ->
    #index_merge{
        indexes = Indexes,
        user_ctx = UserCtx,
        conn_timeout = Timeout
    } = IndexMergeParams,
    {ok, DDoc, IndexName} = get_first_ddoc(Indexes, UserCtx, Timeout),
    query_index_loop(Mod, IndexMergeParams, DDoc, IndexName, ?MAX_RETRIES).

% Special and simpler case, trigger a lighter and faster code path.
query_index(Mod, #index_merge{indexes = [#set_view_spec{}]} = Params, Req) ->
    #index_merge{
        indexes = Indexes,
        conn_timeout = Timeout,
        ddoc_revision = DesiredDDocRevision
    } = Params,
    {ok, DDoc, _} = get_first_ddoc(Indexes, Req#httpd.user_ctx, Timeout),
    DDocRev = ddoc_rev(DDoc),
    case should_check_rev(Params, DDoc) of
    true ->
        case DesiredDDocRevision of
        auto ->
            ok;
        DDocRev ->
            ok;
        OtherDDocRev ->
            ?LOG_ERROR("View merger, revision mismatch for design document `~s',"
                       " wanted ~s, got ~s",
                       [DDoc#doc.id,
                        rev_str(DesiredDDocRevision),
                        rev_str(OtherDDocRev)]),
            throw({error, revision_mismatch})
        end;
    false ->
        ok
    end,
    Mod:simple_set_view_query(Params, DDoc, Req);

query_index(Mod, IndexMergeParams0, #httpd{user_ctx = UserCtx} = Req) ->
    #index_merge{
        indexes = Indexes,
        conn_timeout = Timeout,
        extra = Extra
    } = IndexMergeParams0,
    {ok, DDoc, IndexName} = get_first_ddoc(Indexes, UserCtx, Timeout),
    IndexMergeParams = IndexMergeParams0#index_merge{
        user_ctx = UserCtx,
        http_params = Mod:parse_http_params(Req, DDoc, IndexName, Extra)
    },
    query_index_loop(Mod, IndexMergeParams, DDoc, IndexName, ?MAX_RETRIES).


query_index_loop(_Mod, _IndexMergeParams, _DDoc, _IndexName, 0) ->
    throw({error, revision_sync_failed});
query_index_loop(Mod, IndexMergeParams, DDoc, IndexName, N) ->
    try
        do_query_index(Mod, IndexMergeParams, DDoc, IndexName)
    catch
    throw:retry ->
        timer:sleep(?RETRY_INTERVAL),
        #index_merge{
            indexes = Indexes,
            user_ctx = UserCtx,
            conn_timeout = Timeout
        } = IndexMergeParams,
        {ok, DDoc2, IndexName} = get_first_ddoc(Indexes, UserCtx, Timeout),
        query_index_loop(Mod, IndexMergeParams, DDoc2, IndexName, N - 1)
    end.


do_query_index(Mod, IndexMergeParams, DDoc, IndexName) ->
    #index_merge{
       indexes = Indexes, callback = Callback, user_acc = UserAcc,
       ddoc_revision = DesiredDDocRevision, user_ctx = UserCtx
    } = IndexMergeParams,

    DDocRev = ddoc_rev(DDoc),
    case should_check_rev(IndexMergeParams, DDoc) of
    true ->
        case DesiredDDocRevision of
        auto ->
            ok;
        DDocRev ->
            ok;
        OtherDDocRev ->
            ?LOG_ERROR("View merger, revision mismatch for design document `~s',"
                       " wanted ~s, got ~s",
                       [DDoc#doc.id,
                        rev_str(DesiredDDocRevision),
                        rev_str(OtherDDocRev)]),
            throw({error, revision_mismatch})
        end;
    false ->
        ok
    end,

    {LessFun, FoldFun, MergeFun, CollectorFun, Extra2} = Mod:make_funs(
        DDoc, IndexName, IndexMergeParams),
    NumFolders = length(Indexes),
    QueueLessFun = fun
        (set_view_outdated, _) ->
            true;
        (_, set_view_outdated) ->
            false;
        (revision_mismatch, _) ->
            true;
        (_, revision_mismatch) ->
            false;
        ({debug_info, _Url, _Info}, _) ->
            true;
        (_, {debug_info, _Url, _Info}) ->
            false;
        ({row_count, _}, _) ->
            true;
        (_, {row_count, _}) ->
            false;
        ({error, _Url, _Reason}, _) ->
            true;
        (_, {error, _Url, _Reason}) ->
            false;
        (RowA, RowB) ->
            case LessFun of
            nil ->
                % That's where the actual less fun is. But as bounding box
                % requests don't return a sorted order, we just return true
                true;
             _ ->
                LessFun(RowA, RowB)
            end
    end,
    % We want to trap exits to avoid this process (mochiweb worker) to die.
    % If the mochiweb worker dies, the client will not get a response back.
    % Link the queue to the folders, so that if one folder dies, all the others
    % will be killed and not hang forever (mochiweb reuses workers for different
    % requests).
    TrapExitBefore = process_flag(trap_exit, true),
    {ok, Queue} = couch_view_merger_queue:start_link(NumFolders, QueueLessFun),
    Folders = lists:foldr(
        fun(Index, Acc) ->
            Pid = spawn_link(fun() ->
                link(Queue),
                index_folder(Mod, Index, IndexMergeParams, UserCtx, DDoc, Queue, FoldFun)
            end),
            [Pid | Acc]
        end,
        [], Indexes),
    Collector = CollectorFun(NumFolders, Callback, UserAcc),
    {Skip, Limit} = Mod:get_skip_and_limit(IndexMergeParams#index_merge.http_params),
    MergeParams = #merge_params{
        index_name = IndexName,
        queue = Queue,
        collector = Collector,
        skip = Skip,
        limit = Limit,
        extra = Extra2
    },
    try
        case MergeFun(MergeParams) of
        set_view_outdated ->
            throw({error, set_view_outdated});
        revision_mismatch ->
            case DesiredDDocRevision of
            auto ->
                throw(retry);
            OtherDDocRev2 ->
                ?LOG_ERROR("View merger, revision mismatch for design document `~s',"
                           " wanted ~s, got ~s",
                           [DDoc#doc.id,
                            rev_str(DesiredDDocRevision),
                            rev_str(OtherDDocRev2)]),
                throw({error, revision_mismatch})
            end;
        {ok, Resp} ->
            Resp;
        {stop, Resp} ->
            Resp
        end
    after
        unlink(Queue),
        lists:foreach(fun erlang:unlink/1, Folders),
        % Important, shutdown the queue first. This ensures any blocked
        % HTTP folders (bloked by queue calls) will get an error/exit and
        % then stream all the remaining data from the socket, otherwise
        % the socket can't be reused for future requests.
        QRef = erlang:monitor(process, Queue),
        exit(Queue, shutdown),
        FolderRefs = lists:map(fun(Pid) ->
                Ref = erlang:monitor(process, Pid),
                exit(Pid, shutdown),
                Ref
            end, Folders),
        lists:foreach(fun(Ref) ->
                receive {'DOWN', Ref, _, _, _} -> ok end
            end, [QRef | FolderRefs]),
        Reason = clean_exit_messages(normal),
        process_flag(trap_exit, TrapExitBefore),
        case Reason of
        normal ->
            ok;
        shutdown ->
            ok;
        _ ->
            exit(Reason)
        end
    end.


clean_exit_messages(FinalReason) ->
    receive
    {'EXIT', _Pid, normal} ->
        clean_exit_messages(FinalReason);
    {'EXIT', _Pid, shutdown} ->
        clean_exit_messages(FinalReason);
    {'EXIT', _Pid, Reason} ->
        clean_exit_messages(Reason)
    after 0 ->
        FinalReason
    end.


get_first_ddoc([], _UserCtx, _Timeout) ->
    throw({error, <<"A view spec can not consist of merges exclusively.">>});

get_first_ddoc([#simple_index_spec{ddoc_id = nil} = Spec | _],
        _UserCtx, _Timeout) ->
    #simple_index_spec{index_name = <<"_all_docs">>} = Spec,
    {ok, nil, <<"_all_docs">>};

get_first_ddoc([#set_view_spec{} = Spec | _], _UserCtx, _Timeout) ->
    #set_view_spec {
        name = SetName, ddoc_id = Id, view_name = ViewName
    } = Spec,

    case couch_set_view_ddoc_cache:get_ddoc(SetName, Id) of
    {ok, DDoc} ->
        {ok, DDoc, ViewName};
    {db_open_error, {not_found, _}} ->
        throw({not_found, db_not_found_msg(?master_dbname(SetName))});
    {db_open_error, Error} ->
        throw(Error);
    {doc_open_error, {not_found, _}} ->
        throw({not_found, ddoc_not_found_msg(?master_dbname(SetName), Id)})
    end;

get_first_ddoc([#simple_index_spec{} = Spec | _], UserCtx, Timeout) ->
    #simple_index_spec{
        database = DbName, ddoc_database = DDocDbName, ddoc_id = Id,
        index_name = IndexName
    } = Spec,
    {ok, Db} = case DDocDbName of
    nil ->
        open_db(DbName, UserCtx, Timeout);
    _ when is_binary(DDocDbName) ->
        open_db(DDocDbName, UserCtx, Timeout)
    end,
    {ok, DDoc} = get_ddoc(Db, Id),
    close_db(Db),

    {ok, DDoc, IndexName};

get_first_ddoc([_MergeSpec | Rest], UserCtx, Timeout) ->
    get_first_ddoc(Rest, UserCtx, Timeout).


open_db(<<"http://", _/binary>> = DbName, _UserCtx, Timeout) ->
    HttpDb = #httpdb{
        url = maybe_add_trailing_slash(DbName),
        timeout = Timeout
    },
    {ok, HttpDb#httpdb{lhttpc_options = lhttpc_options(HttpDb)}};
open_db(<<"https://", _/binary>> = DbName, _UserCtx, Timeout) ->
    HttpDb = #httpdb{
        url = maybe_add_trailing_slash(DbName),
        timeout = Timeout
    },
    {ok, HttpDb#httpdb{lhttpc_options = lhttpc_options(HttpDb)}};
open_db(DbName, UserCtx, _Timeout) ->
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, _} = Ok ->
        Ok;
    {not_found, _} ->
        throw({not_found, db_not_found_msg(DbName)});
    Error ->
        throw(Error)
    end.

maybe_add_trailing_slash(Url) when is_binary(Url) ->
    maybe_add_trailing_slash(?b2l(Url));
maybe_add_trailing_slash(Url) ->
    case lists:last(Url) of
    $/ ->
        Url;
    _ ->
        Url ++ "/"
    end.

close_db(#httpdb{}) ->
    ok;
close_db(Db) ->
    couch_db:close(Db).

get_ddoc(#httpdb{} = HttpDb, Id) ->
    #httpdb{
        url = BaseUrl,
        headers = Headers,
        timeout = Timeout,
        lhttpc_options = Options
    } = HttpDb,
    Url = BaseUrl ++ ?b2l(Id),
    case lhttpc:request(Url, "GET", Headers, [], Timeout, Options) of
    {ok, {{200, _}, _RespHeaders, Body}} ->
        Doc = couch_doc:from_json_obj(?JSON_DECODE(Body)),
        {ok, couch_doc:with_ejson_body(Doc)};
    {ok, {{_Code, _}, _RespHeaders, Body}} ->
        {Props} = ?JSON_DECODE(Body),
        case {get_value(<<"error">>, Props), get_value(<<"reason">>, Props)} of
        {not_found, _} ->
            throw({not_found, ddoc_not_found_msg(HttpDb, Id)});
        Error ->
            Msg = io_lib:format("Error getting design document `~s` from "
                "database `~s`: ~s", [Id, db_uri(HttpDb), Error]),
            throw({error, iolist_to_binary(Msg)})
        end;
    {error, Error} ->
        Msg = io_lib:format("Error getting design document `~s` from database "
            "`~s`: ~s", [Id, db_uri(HttpDb), to_binary(Error)]),
        throw({error, iolist_to_binary(Msg)})
    end;
get_ddoc(Db, Id) ->
    case couch_db:open_doc(Db, Id, [ejson_body]) of
    {ok, _} = Ok ->
        Ok;
    {not_found, _} ->
        throw({not_found, ddoc_not_found_msg(Db#db.name, Id)})
    end.

% Returns the group ID of the indexer group that contains the Design Document
% In Couchbase the Design Document is stored in a so-called master database.
% This is Couchbase specific
get_group_id(nil, DDocId) ->
    DDocId;
get_group_id(DDocDbName, DDocId) when is_binary(DDocDbName) ->
    DDocDb = case couch_db:open_int(DDocDbName, []) of
    {ok, DDocDb1} ->
        DDocDb1;
    {not_found, _} ->
        throw(ddoc_db_not_found)
    end,
    {DDocDb, DDocId}.

db_uri(#httpdb{url = Url}) ->
    db_uri(Url);
db_uri(#db{name = Name}) ->
    Name;
db_uri(Url) when is_binary(Url) ->
    ?l2b(couch_util:url_strip_password(Url)).


db_not_found_msg(DbName) ->
    iolist_to_binary(io_lib:format(
        "Database `~s` doesn't exist.", [db_uri(DbName)])).

ddoc_not_found_msg(DbName, DDocId) ->
    Msg = io_lib:format(
        "Design document `~s` missing in database `~s`.",
        [DDocId, db_uri(DbName)]),
    iolist_to_binary(Msg).


lhttpc_options(#httpdb{timeout = T}) ->
    % TODO: add SSL options like verify and cacertfile, which should
    % configurable somewhere.
    [
        {connect_timeout, T},
        {connect_options, [{keepalive, true}, {nodelay, true}]},
        {pool, whereis(couch_index_merger_connection_pool)}
    ].


collect_row_count(RecvCount, AccCount, PreprocessFun, Callback, UserAcc, Item) ->
    case Item of
    {error, _DbUrl, _Reason} = Error ->
        case Callback(Error, UserAcc) of
        {stop, Resp} ->
            {stop, Resp};
        {ok, UserAcc2} ->
            case RecvCount > 1 of
            false ->
                {ok, UserAcc3} = Callback({start, AccCount}, UserAcc2),
                {ok, fun (Item2) ->
                    collect_rows(
                        PreprocessFun, Callback, UserAcc3, Item2)
                end};
            true ->
                {ok, fun (Item2) ->
                    collect_row_count(
                        RecvCount - 1, AccCount, PreprocessFun, Callback,
                        UserAcc2, Item2)
                end}
            end
        end;
    {row_count, Count} ->
        AccCount2 = AccCount + Count,
        case RecvCount > 1 of
        false ->
            % TODO: what about offset and update_seq?
            % TODO: maybe add etag like for regular views? How to
            %       compute them?
            {ok, UserAcc2} = Callback({start, AccCount2}, UserAcc),
            {ok, fun (Item2) ->
                collect_rows(PreprocessFun, Callback, UserAcc2, Item2)
            end};
        true ->
            {ok, fun (Item2) ->
                collect_row_count(
                    RecvCount - 1, AccCount2, PreprocessFun, Callback, UserAcc, Item2)
            end}
        end;
    {debug_info, _From, _Info} = DebugInfo ->
        {ok, UserAcc2} = Callback(DebugInfo, UserAcc),
        {ok, fun (Item2) ->
            collect_row_count(RecvCount, AccCount, PreprocessFun, Callback, UserAcc2, Item2)
        end};
    stop ->
        {_, UserAcc2} = Callback(stop, UserAcc),
        {stop, UserAcc2}
    end.

% PreprocessFun is called on every row (which comes from the fold function
% of the underlying data structure) before it gets passed into the Callback
% function
collect_rows(PreprocessFun, Callback, UserAcc, Item) ->
    case Item of
    {error, _DbUrl, _Reason} = Error ->
        case Callback(Error, UserAcc) of
        {stop, Resp} ->
            {stop, Resp};
        {ok, UserAcc2} ->
            {ok, fun (Item2) ->
                collect_rows(PreprocessFun, Callback, UserAcc2, Item2)
            end}
        end;
    {row, Row} ->
        RowEJson = PreprocessFun(Row),
        {ok, UserAcc2} = Callback({row, RowEJson}, UserAcc),
        {ok, fun (Item2) ->
            collect_rows(PreprocessFun, Callback, UserAcc2, Item2)
        end};
    {debug_info, _From, _Info} = DebugInfo ->
        {ok, UserAcc2} = Callback(DebugInfo, UserAcc),
        {ok, fun (Item2) ->
            collect_rows(PreprocessFun, Callback, UserAcc2, Item2)
        end};
    stop ->
        {ok, UserAcc2} = Callback(stop, UserAcc),
        {stop, UserAcc2}
    end.

merge_indexes_common(Params, RowFun) ->
    #merge_params{
        queue = Queue, collector = Col
    } = Params,
    case couch_view_merger_queue:pop(Queue) of
    closed ->
        {stop, Resp} = Col(stop),
        {ok, Resp};
    {ok, {debug_info, _From, _Info} = DebugInfo} ->
        ok = couch_view_merger_queue:flush(Queue),
        {ok, Col2} = Col(DebugInfo),
        merge_indexes_common(Params#merge_params{collector = Col2}, RowFun);
    {ok, revision_mismatch} ->
        revision_mismatch;
    {ok, set_view_outdated} ->
        set_view_outdated;
    {ok, {error, _Url, _Reason} = Error} ->
        ok = couch_view_merger_queue:flush(Queue),
        case Col(Error) of
        {ok, Col2} ->
            merge_indexes_common(Params#merge_params{collector = Col2}, RowFun);
        {stop, Resp} ->
            {stop, Resp}
        end;
    {ok, {row_count, _} = RowCount} ->
        ok = couch_view_merger_queue:flush(Queue),
        {ok, Col2} = Col(RowCount),
        merge_indexes_common(Params#merge_params{collector = Col2}, RowFun);
    {ok, MinRow} ->
        RowFun(Params, MinRow)
    end.

merge_indexes_no_limit(Params) ->
    merge_indexes_common(
      Params,
      fun (#merge_params{collector=Col}, _MinRow) ->
          Col(stop)
      end).

% Simple case when there are no (or we don't care about) accumulated rows
% MinRowFun is a function that it called if the
% couch_view_merger_queue returns a row that is neither an error, nor a count.
merge_indexes_no_acc(Params, MinRowFun) ->
    merge_indexes_common(
      Params,
      fun (AccParams, MinRow) ->
          AccParams2 = MinRowFun(AccParams, MinRow),
          {params, AccParams2}
      end).

handle_skip(Params) ->
    #merge_params{
        limit = Limit, skip = Skip, collector = Col,
        row_acc = [RowToSend | Rest]
    } = Params,
    case Skip > 0 of
    true ->
        Limit2 = Limit,
        Col2 = Col;
    false ->
        {ok, Col2} = Col({row, RowToSend}),
        Limit2 = dec_counter(Limit)
    end,
    Params#merge_params{
        skip = dec_counter(Skip), limit = Limit2, row_acc = Rest,
        collector = Col2
    }.

dec_counter(0) -> 0;
dec_counter(N) -> N - 1.


index_folder(Mod, #simple_index_spec{database = <<"http://", _/binary>>} =
        IndexSpec, MergeParams, _UserCtx, DDoc, Queue, _FoldFun) ->
    http_index_folder(Mod, IndexSpec, MergeParams, DDoc, Queue);

index_folder(Mod, #simple_index_spec{database = <<"https://", _/binary>>} =
        IndexSpec, MergeParams, _UserCtx, DDoc, Queue, _FoldFun) ->
    http_index_folder(Mod, IndexSpec, MergeParams, DDoc, Queue);

index_folder(Mod, #merged_index_spec{} = IndexSpec,
        MergeParams, _UserCtx, DDoc, Queue, _FoldFun) ->
    http_index_folder(Mod, IndexSpec, MergeParams, DDoc, Queue);

index_folder(_Mod, #set_view_spec{} = ViewSpec, MergeParams,
        UserCtx, DDoc, Queue, FoldFun) ->
    FoldFun(nil, ViewSpec, MergeParams, UserCtx, DDoc, Queue);

index_folder(_Mod, IndexSpec, MergeParams, UserCtx, DDoc, Queue, FoldFun) ->
    #simple_index_spec{
        database = DbName, ddoc_database = DDocDbName, ddoc_id = DDocId
    } = IndexSpec,
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, Db} ->
        try
            FoldFun(Db, IndexSpec, MergeParams, UserCtx, DDoc, Queue)
        catch
        queue_shutdown ->
            ok;
        {not_found, Reason} when Reason =:= missing; Reason =:= deleted ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DbName, DDocId)});
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            Stack = erlang:get_stacktrace(),
            ?LOG_ERROR("Caught unexpected error while serving "
                       "index query for `~s/~s`:~n~p", [DbName, DDocId, Stack]),
            couch_view_merger_queue:queue(Queue, parse_error(Error))
        after
            ok = couch_view_merger_queue:done(Queue),
            couch_db:close(Db)
        end;
    {not_found, _} ->
        ok = couch_view_merger_queue:queue(
            Queue, {error, ?LOCAL, db_not_found_msg(DbName)}),
        ok = couch_view_merger_queue:done(Queue)
    end.


% `invalid_value` only happens on reduces
parse_error({invalid_value, Reason}) ->
    {error, ?LOCAL, to_binary(Reason)};
parse_error(Error) ->
    {error, ?LOCAL, to_binary(Error)}.


% Fold function for remote indexes
http_index_folder(Mod, IndexSpec, MergeParams, DDoc, Queue) ->
    % Trap exits, so that when we receive a shutdown message from the parent,
    % or an error/exit when queing an item/error, we get all the remaining data
    % from the socket - this is required in order to ensure the connection can
    % be reused for other requests and for lhttpc to handle the socket back to
    % connection pool.
    process_flag(trap_exit, true),
    try
        run_http_index_folder(Mod, IndexSpec, MergeParams, DDoc, Queue)
    catch
    throw:queue_shutdown ->
        ok
    after
        Streamer = get(streamer_pid),
        case is_pid(Streamer) andalso is_process_alive(Streamer) of
        true ->
            catch stream_all(Streamer, MergeParams#index_merge.conn_timeout, []);
        false ->
            ok
        end
    end.

run_http_index_folder(Mod, IndexSpec, MergeParams, DDoc, Queue) ->
    {Url, Method, Headers, Body, BaseOptions} =
        Mod:http_index_folder_req_details(IndexSpec, MergeParams, DDoc),
    #index_merge{
        conn_timeout = Timeout
    } = MergeParams,
    LhttpcOptions = [{partial_download, [{window_size, 3}]} | BaseOptions],

    case lhttpc:request(Url, Method, Headers, Body, Timeout, LhttpcOptions) of
    {ok, {{200, _}, _RespHeaders, Pid}} when is_pid(Pid) ->
        put(streamer_pid, Pid),
        try
            case os:type() of
            {win32, _} ->
                % TODO: make couch_view_parser build and run on Windows
                EventFun = Mod:make_event_fun(MergeParams#index_merge.http_params, Queue),
                DataFun = fun() -> stream_data(Pid, Timeout) end,
                json_stream_parse:events(DataFun, EventFun);
            _ ->
                DataFun = fun() -> next_chunk(Pid, Timeout) end,
                ok = couch_http_view_streamer:parse(DataFun, Queue, get(from_url))
            end
        catch throw:{error, Error} ->
            ok = couch_view_merger_queue:queue(Queue, {error, Url, Error})
        after
            ok = couch_view_merger_queue:done(Queue)
        end;
    {ok, {{Code, _}, _RespHeaders, Pid}} when is_pid(Pid) ->
        put(streamer_pid, Pid),
        Error = try
            stream_all(Pid, Timeout, [])
        catch throw:{error, _Error} ->
            <<"Error code ", (?l2b(integer_to_list(Code)))/binary>>
        end,
        case (catch ?JSON_DECODE(Error)) of
        {Props} when is_list(Props) ->
            case {get_value(<<"error">>, Props), get_value(<<"reason">>, Props)} of
            {<<"not_found">>, Reason} when Reason =/= <<"missing">>, Reason =/= <<"deleted">> ->
                ok = couch_view_merger_queue:queue(Queue, {error, Url, Reason});
            {<<"not_found">>, _} ->
                ok = couch_view_merger_queue:queue(Queue, {error, Url, <<"not_found">>});
            {<<"error">>, <<"revision_mismatch">>} ->
                ok = couch_view_merger_queue:queue(Queue, revision_mismatch);
            {<<"error">>, <<"set_view_outdated">>} ->
                ok = couch_view_merger_queue:queue(Queue, set_view_outdated);
            ErrorTuple ->
                ok = couch_view_merger_queue:queue(Queue, {error, Url, to_binary(ErrorTuple)})
            end;
        _ ->
            ok = couch_view_merger_queue:queue(Queue, {error, Url, to_binary(Error)})
        end,
        ok = couch_view_merger_queue:done(Queue);
    {error, Error} ->
        ok = couch_view_merger_queue:queue(Queue, {error, Url, Error}),
        ok = couch_view_merger_queue:done(Queue)
    end.


stream_data(Pid, Timeout) ->
    case lhttpc:get_body_part(Pid, Timeout) of
    {ok, {http_eob, _Trailers}} ->
         {<<>>, fun() -> throw({error, <<"more view data expected">>}) end};
    {ok, Data} ->
         {Data, fun() -> stream_data(Pid, Timeout) end};
    {error, _} = Error ->
         throw(Error)
    end.


next_chunk(Pid, Timeout) ->
    case lhttpc:get_body_part(Pid, Timeout) of
    {ok, {http_eob, _Trailers}} ->
         eof;
    {ok, _Data} = Ok ->
         Ok;
    {error, _} = Error ->
         throw(Error)
    end.


stream_all(Pid, Timeout, Acc) ->
    case stream_data(Pid, Timeout) of
    {<<>>, _} ->
        iolist_to_binary(lists:reverse(Acc));
    {Data, _} ->
        stream_all(Pid, Timeout, [Data | Acc])
    end.


void_event(_Ev) ->
    fun void_event/1.

ddoc_rev(nil) ->
    nil;
ddoc_rev(#doc{rev = Rev}) ->
    Rev.

ddoc_rev_str(DDoc) ->
    rev_str(ddoc_rev(DDoc)).

should_check_rev(#index_merge{ddoc_revision = DDocRevision}, DDoc) ->
    DDocRevision =/= nil andalso DDoc =/= nil.

rev_str(nil) ->
    "nil";
rev_str(auto) ->
    "auto";
rev_str(DocRev) ->
    couch_doc:rev_to_str(DocRev).

ddoc_unchanged(DbName, DDoc) when is_binary(DbName) ->
    case couch_db:open_int(DbName, []) of
    {ok, Db} ->
        try
            DDocId = DDoc#doc.id,
            {ok, MaybeUpdatedDDoc} = get_ddoc(Db, DDocId),
            ddoc_rev(DDoc) =:= ddoc_rev(MaybeUpdatedDDoc)
        after
            couch_db:close(Db)
        end;
    {not_found, _} ->
        throw(ddoc_db_not_found)
    end;
ddoc_unchanged(Db, DDoc) ->
    DbName = couch_db:name(Db),
    case couch_db:open_int(DbName, []) of
    {ok, Db1} ->
        try
            case couch_db:get_update_seq(Db) =:= couch_db:get_update_seq(Db1) of
            true ->
                %% nothing changed
                true;
            false ->
                %% design document may have changed
                DDocId = DDoc#doc.id,
                {ok, MaybeUpdatedDDoc} = get_ddoc(Db1, DDocId),
                ddoc_rev(DDoc) =:= ddoc_rev(MaybeUpdatedDDoc)
            end
        after
            couch_db:close(Db1)
        end;
    {not_found, _} ->
        throw(ddoc_db_not_found)
    end.

validate_revision_param(nil) ->
    nil;
validate_revision_param(<<"auto">>) ->
    auto;
validate_revision_param(Revision) ->
    couch_doc:parse_rev(Revision).

rem_passwd(Url) ->
    ?l2b(couch_util:url_strip_password(Url)).
