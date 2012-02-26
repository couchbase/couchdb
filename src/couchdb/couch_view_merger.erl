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

-module(couch_view_merger).

% export callbacks
-export([parse_http_params/4, make_funs/3, get_skip_and_limit/1,
    http_index_folder_req_details/3, make_event_fun/2]).

-include("couch_db.hrl").
-include("couch_index_merger.hrl").
-include("couch_view_merger.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-define(LOCAL, <<"local">>).

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1,
    get_nested_json_value/2
]).

-define(DEFAULT_STALENESS, update_after).


% callback!
parse_http_params(Req, DDoc, ViewName, #view_merge{keys = Keys}) ->
    % view type =~ query type
    {_Collation, ViewType0, _ViewLang} = view_details(DDoc, ViewName),
    ViewType = case {ViewType0, couch_httpd:qs_value(Req, "reduce", "true")} of
    {reduce, "false"} ->
       red_map;
    _ ->
       ViewType0
    end,

    StaleDefined = couch_httpd:qs_value(Req, "stale") =/= undefined,
    QueryArgs = couch_httpd_view:parse_view_params(Req, Keys, ViewType),
    QueryArgs1 = QueryArgs#view_query_args{view_name = ViewName},

    case StaleDefined of
    true ->
        QueryArgs1;
    false ->
        QueryArgs1#view_query_args{stale = ?DEFAULT_STALENESS}
    end.

% callback!
make_funs(DDoc, ViewName, IndexMergeParams) ->
    #index_merge{
       extra = Extra,
       http_params = #view_query_args{debug = DebugMode} = ViewArgs
    } = IndexMergeParams,
    #view_merge{
       rereduce_fun = InRedFun,
       rereduce_fun_lang = InRedFunLang,
       make_row_fun = MakeRowFun0
    } = Extra,
    {Collation, ViewType0, ViewLang} = view_details(DDoc, ViewName),
    ViewType = case {ViewType0, ViewArgs#view_query_args.run_reduce} of
    {reduce, false} ->
       red_map;
    _ ->
       ViewType0
    end,
    {RedFun, RedFunLang} = case {ViewType, InRedFun} of
    {reduce, nil} ->
        {reduce_function(DDoc, ViewName), ViewLang};
    {reduce, _} when is_binary(InRedFun) ->
        {InRedFun, InRedFunLang};
    _ ->
        {nil, nil}
    end,
    LessFun = view_less_fun(Collation, ViewArgs#view_query_args.direction,
        ViewType),
    {FoldFun, MergeFun} = case ViewType of
    reduce ->
        {fun reduce_view_folder/6, fun merge_reduce_views/1};
    _ when ViewType =:= map; ViewType =:= red_map ->
        {fun map_view_folder/6, fun merge_map_views/1}
    end,
    CollectorFun = case ViewType of
    reduce ->
        fun (_NumFolders, Callback2, UserAcc2) ->
            fun (Item) ->
                {ok, UserAcc3} = Callback2(start, UserAcc2),
                MakeRowFun = case is_function(MakeRowFun0) of
                true ->
                    MakeRowFun0;
                false ->
                    fun(RowDetails) -> view_row_obj_reduce(RowDetails, DebugMode) end
                end,
                couch_index_merger:collect_rows(MakeRowFun, Callback2, UserAcc3, Item)
            end
        end;
     % red_map|map
     _ ->
        fun (NumFolders, Callback2, UserAcc2) ->
            fun (Item) ->
                MakeRowFun = case is_function(MakeRowFun0) of
                true ->
                    MakeRowFun0;
                false ->
                    fun(RowDetails) -> view_row_obj_map(RowDetails, DebugMode) end
                end,
                couch_index_merger:collect_row_count(
                    NumFolders, 0, MakeRowFun, Callback2, UserAcc2, Item)
            end
        end
    end,
    Extra2 = #view_merge{
        rereduce_fun = RedFun,
        rereduce_fun_lang = RedFunLang
    },
    {LessFun, FoldFun, MergeFun, CollectorFun, Extra2}.

% callback!
get_skip_and_limit(#view_query_args{skip=Skip, limit=Limit}) ->
    {Skip, Limit}.

% callback!
make_event_fun(ViewArgs, Queue) ->
    fun(Ev) ->
        http_view_fold(Ev, ViewArgs#view_query_args.view_type, Queue)
    end.

% callback!
http_index_folder_req_details(#merged_index_spec{
        url = MergeUrl0, ejson_spec = {EJson}}, MergeParams, DDoc) ->
    #index_merge{
        conn_timeout = Timeout,
        http_params = ViewArgs,
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    {ok, #httpdb{url = Url, ibrowse_options = Options} = Db} =
        couch_index_merger:open_db(MergeUrl0, nil, Timeout),
    MergeUrl = Url ++ view_qs(ViewArgs, MergeParams),
    Headers = [{"Content-Type", "application/json"} | Db#httpdb.headers],

    EJson1 = case Keys of
    nil ->
        EJson;
    _ ->
        [{<<"keys">>, Keys} | EJson]
    end,

    EJson2 = case couch_index_merger:should_check_rev(MergeParams, DDoc) of
    true ->
        P = fun (Tuple) -> element(1, Tuple) =/= <<"ddoc_revision">> end,
        [{<<"ddoc_revision">>, couch_index_merger:ddoc_rev_str(DDoc)} |
            lists:filter(P, EJson1)];
    false ->
        EJson1
    end,

    Body = {EJson2},
    put(from_url, Url),
    {MergeUrl, post, Headers, ?JSON_ENCODE(Body), Options};

http_index_folder_req_details(#simple_index_spec{
        database = DbUrl, ddoc_id = DDocId, index_name = ViewName},
        MergeParams, _DDoc) ->
    #index_merge{
        conn_timeout = Timeout,
        http_params = ViewArgs,
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    {ok, #httpdb{url = Url, ibrowse_options = Options} = Db} =
        couch_index_merger:open_db(DbUrl, nil, Timeout),
    ViewUrl = Url ++ case ViewName of
    <<"_all_docs">> ->
        "_all_docs";
    _ ->
        ?b2l(DDocId) ++ "/_view/" ++ ?b2l(ViewName)
    end ++ view_qs(ViewArgs, MergeParams),
    Headers = [{"Content-Type", "application/json"} | Db#httpdb.headers],
    put(from_url, DbUrl),
    case Keys of
    nil ->
        {ViewUrl, get, [], [], Options};
    _ ->
        {ViewUrl, post, Headers, ?JSON_ENCODE({[{<<"keys">>, Keys}]}), Options}
    end.


view_details(nil, <<"_all_docs">>) ->
    {<<"raw">>, map, nil};

view_details(#doc{body = DDoc}, ViewName) ->
    {Props} = DDoc,
    {ViewDef} = get_nested_json_value(DDoc, [<<"views">>, ViewName]),
    {ViewOptions} = get_value(<<"options">>, ViewDef, {[]}),
    Collation = get_value(<<"collation">>, ViewOptions, <<"default">>),
    ViewType = case get_value(<<"reduce">>, ViewDef) of
    undefined ->
        map;
    RedFun when is_binary(RedFun) ->
        reduce
    end,
    Lang = get_value(<<"language">>, Props, <<"javascript">>),
    {Collation, ViewType, Lang}.


reduce_function(#doc{body = DDoc}, ViewName) ->
    {ViewDef} = get_nested_json_value(DDoc, [<<"views">>, ViewName]),
    get_value(<<"reduce">>, ViewDef).


view_less_fun(Collation, Dir, ViewType) ->
    LessFun = case Collation of
    <<"default">> ->
        case ViewType of
        _ when ViewType =:= map; ViewType =:= red_map ->
            fun(RowA, RowB) ->
                couch_view:less_json_ids(element(1, RowA), element(1, RowB))
            end;
        reduce ->
            fun({KeyA, _}, {KeyB, _}) -> couch_view:less_json(KeyA, KeyB) end
        end;
    <<"raw">> ->
        fun(A, B) -> A < B end
    end,
    case Dir of
    fwd ->
        LessFun;
    rev ->
        fun(A, B) -> not LessFun(A, B) end
    end.

view_row_obj_map({{Key, error}, Value}, _DebugMode) ->
    {[{key, Key}, {error, Value}]};

% set view
view_row_obj_map({{Key, DocId}, {PartId, Value}}, true) ->
    {[{id, DocId}, {key, Key}, {partition, PartId}, {value, Value}]};
view_row_obj_map({{Key, DocId}, {_PartId, Value}}, false) ->
    {[{id, DocId}, {key, Key}, {value, Value}]};

view_row_obj_map({{Key, DocId}, Value}, _DebugMode) ->
    {[{id, DocId}, {key, Key}, {value, Value}]};

% set view
view_row_obj_map({{Key, DocId}, {PartId, Value}, Doc}, true) ->
    {[{id, DocId}, {key, Key}, {partition, PartId}, {value, Value}, Doc]};
view_row_obj_map({{Key, DocId}, {_PartId, Value}, Doc}, false) ->
    {[{id, DocId}, {key, Key}, {value, Value}, Doc]};

view_row_obj_map({{Key, DocId}, Value, Doc}, _DebugMode) ->
    {[{id, DocId}, {key, Key}, {value, Value}, Doc]}.

view_row_obj_reduce({Key, Value}, _DebugMode) ->
    {[{key, Key}, {value, Value}]}.


merge_map_views(#merge_params{limit = 0} = Params) ->
    couch_index_merger:merge_indexes_no_limit(Params);

merge_map_views(#merge_params{row_acc = []} = Params) ->
    case couch_index_merger:merge_indexes_no_acc(
        Params, fun merge_map_min_row/2) of
    {params, Params2} ->
        merge_map_views(Params2);
    Else ->
        Else
    end;

merge_map_views(Params) ->
    Params2 = couch_index_merger:handle_skip(Params),
    merge_map_views(Params2).


% A new Params record is returned
merge_map_min_row(Params, MinRow) ->
    #merge_params{
        queue = Queue, index_name = ViewName
    } = Params,
    {RowToSend, RestToSend} = handle_duplicates(ViewName, MinRow, Queue),
    ok = couch_view_merger_queue:flush(Queue),
    couch_index_merger:handle_skip(
        Params#merge_params{row_acc=[RowToSend|RestToSend]}).



handle_duplicates(<<"_all_docs">>, MinRow, Queue) ->
    handle_all_docs_row(MinRow, Queue);

handle_duplicates(_ViewName, MinRow, Queue) ->
    handle_duplicates_allowed(MinRow, Queue).


handle_all_docs_row(MinRow, Queue) ->
    {Key0, Id0} = element(1, MinRow),
    % Group rows by similar keys, split error "not_found" from normal ones. If all
    % are "not_found" rows, squash them into one. If there are "not_found" ones
    % and others with a value, discard the "not_found" ones.
    {ValueRows, ErrorRows} = case Id0 of
    error ->
        pop_similar_rows(Key0, Queue, [], [MinRow]);
    _ when is_binary(Id0) ->
        pop_similar_rows(Key0, Queue, [MinRow], [])
    end,
    case {ValueRows, ErrorRows} of
    {[], [ErrRow | _]} ->
        {ErrRow, []};
    {[ValRow], []} ->
        {ValRow, []};
    {[FirstVal | RestVal], _} ->
        {FirstVal, RestVal}
    end.

handle_duplicates_allowed(MinRow, _Queue) ->
    {MinRow, []}.

pop_similar_rows(Key0, Queue, Acc, AccError) ->
    case couch_view_merger_queue:peek(Queue) of
    empty ->
        {Acc, AccError};
    {ok, Row} ->
        {Key, DocId} = element(1, Row),
        case Key =:= Key0 of
        false ->
            {Acc, AccError};
        true ->
            {ok, Row} = couch_view_merger_queue:pop_next(Queue),
            case DocId of
            error ->
                pop_similar_rows(Key0, Queue, Acc, [Row | AccError]);
            _ ->
                pop_similar_rows(Key0, Queue, [Row | Acc], AccError)
            end
        end
    end.


merge_reduce_views(#merge_params{limit = 0} = Params) ->
    couch_index_merger:merge_indexes_no_limit(Params);

merge_reduce_views(Params) ->
    case couch_index_merger:merge_indexes_no_acc(
        Params, fun merge_reduce_min_row/2) of
    {params, Params2} ->
        merge_reduce_views(Params2);
    Else ->
        Else
    end.

merge_reduce_min_row(Params, MinRow) ->
    #merge_params{
        queue = Queue, limit = Limit, skip = Skip, collector = Col
    } = Params,
    case group_keys_for_rereduce(Queue, [MinRow]) of
    revision_mismatch -> revision_mismatch;
    RowGroup ->
        ok = couch_view_merger_queue:flush(Queue),
        {Row, Col2} = case RowGroup of
        [R] ->
            {{row, R}, Col};
        [{K, _}, _ | _] ->
            try
                RedVal = rereduce(RowGroup, Params),
                {{row, {K, RedVal}}, Col}
            catch
            _Tag:Error ->
                Stack = erlang:get_stacktrace(),
                ?LOG_ERROR("Caught unexpected error while "
                           "merging reduce view: ~p~n~p", [Error, Stack]),
                on_rereduce_error(Col, Error)
            end
        end,
        case Row of
        {stop, _Resp} = Stop ->
            Stop;
        _ ->
            case Skip > 0 of
            true ->
                Limit2 = Limit,
                Col3 = Col2;
            false ->
                case Row of
                {row, _} ->
                    {ok, Col3} = Col2(Row);
                _ ->
                    Col3 = Col2,
                    ok
                end,
                Limit2 = couch_index_merger:dec_counter(Limit)
            end,
            Params#merge_params{
                skip = couch_index_merger:dec_counter(Skip), limit = Limit2,
                collector = Col3
            }
        end
    end.


on_rereduce_error(Col, Error) ->
    case Col(reduce_error(Error)) of
    {stop, _Resp} = Stop ->
            {Stop, undefined};
    Other ->
            Other
    end.

reduce_error({invalid_value, Reason}) ->
    {error, ?LOCAL, to_binary(Reason)};
reduce_error(Error) ->
    {error, ?LOCAL, to_binary(Error)}.


group_keys_for_rereduce(Queue, [{K, _} | _] = Acc) ->
    case couch_view_merger_queue:peek(Queue) of
    empty ->
        Acc;
    {ok, {K, _} = Row} ->
        {ok, Row} = couch_view_merger_queue:pop_next(Queue),
        group_keys_for_rereduce(Queue, [Row | Acc]);
    {ok, revision_mismatch} ->
        revision_mismatch;
    {ok, _} ->
        Acc
    end.


rereduce(Rows, #merge_params{extra = Extra}) ->
    #view_merge{
        rereduce_fun = RedFun,
        rereduce_fun_lang = Lang
    } = Extra,
    Reds = [[Val] || {_Key, Val} <- Rows],
    {ok, [Value]} = couch_query_servers:rereduce(Lang, [RedFun], Reds),
    Value.

get_set_view(GetSetViewFn, SetName, DDocId, ViewName, ViewGroupReq, Partitions) ->
    ViewGroupReq1 = ViewGroupReq#set_view_group_req{
        stale = ok
    },
    case GetSetViewFn(SetName, DDocId, ViewName, ViewGroupReq1, Partitions) of
    {ok, StaleView, StaleGroup, []} ->
        case ViewGroupReq#set_view_group_req.stale of
        ok ->
            {ok, StaleView, StaleGroup, []};
        _Other ->
            couch_set_view:release_group(StaleGroup),
            ViewGroupReq2 = ViewGroupReq#set_view_group_req{
                update_stats = false
            },
            GetSetViewFn(SetName, DDocId, ViewName, ViewGroupReq2, Partitions)
        end;
    Other ->
        Other
    end.

prepare_set_view(ViewSpec, ViewGroupReq, Queue, GetSetViewFn) ->
    #set_view_spec{
        name = SetName,
        ddoc_id = DDocId, view_name = ViewName,
        partitions = Partitions
    } = ViewSpec,

    try
        case get_set_view(GetSetViewFn, SetName, DDocId,
                          ViewName, ViewGroupReq, Partitions) of
        {ok, View, Group, []} ->
            {View, Group};
        {ok, _, Group, MissingPartitions} ->
            ?LOG_INFO("Set view `~s`, group `~s`, missing partitions: ~w",
                      [SetName, DDocId, MissingPartitions]),
            couch_set_view:release_group(Group),
            couch_view_merger_queue:queue(Queue, set_view_outdated),
            couch_view_merger_queue:done(Queue),
            error;
        {not_found, missing_named_view} ->
            not_found
        end
    catch _:Error ->
        QueueError = queue_get_view_group_error(Error, SetName, DDocId),
        couch_view_merger_queue:queue(Queue, QueueError),
        couch_view_merger_queue:done(Queue),
        error
    end.


queue_get_view_group_error({error, {invalid_value, Msg}}, _SetName, _DDocId) ->
    {error, ?LOCAL, Msg};
queue_get_view_group_error({error, Reason}, _SetName, _DDocId) ->
    {error, ?LOCAL, Reason};
queue_get_view_group_error(view_undefined, SetName, DDocId) ->
    {error, ?LOCAL, view_undefined_msg(SetName, DDocId)};
queue_get_view_group_error(Error, _SetName, _DDocId) ->
    {error, ?LOCAL, Error}.


map_view_folder(Db, #simple_index_spec{index_name = <<"_all_docs">>},
        MergeParams, _UserCtx, _DDoc, Queue) ->
    #index_merge{
        http_params = ViewArgs,
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    {ok, Info} = couch_db:get_db_info(Db),
    ok = couch_view_merger_queue:queue(
        Queue, {row_count, get_value(doc_count, Info)}),
    % TODO: add support for ?update_seq=true and offset
    fold_local_all_docs(Keys, Db, Queue, ViewArgs);

map_view_folder(_Db, #set_view_spec{} = ViewSpec, MergeParams, UserCtx, DDoc, Queue) ->
    map_set_view_folder(ViewSpec, MergeParams, UserCtx, DDoc, Queue);

map_view_folder(Db, ViewSpec, MergeParams, _UserCtx, DDoc, Queue) ->
    #simple_index_spec{
        ddoc_database = DDocDbName, ddoc_id = DDocId, index_name = ViewName
    } = ViewSpec,
    #index_merge{
        http_params = ViewArgs,
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    #view_query_args{
        stale = Stale,
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = ViewArgs,
    FoldlFun = make_map_fold_fun(IncludeDocs, Conflicts, Db, Queue),
    {DDocDb, View} = get_map_view(Db, DDocDbName, DDocId, ViewName, Stale),

    case not(couch_index_merger:should_check_rev(MergeParams, DDoc)) orelse
            couch_index_merger:ddoc_unchanged(DDocDb, DDoc) of
    true ->
        {ok, RowCount} = couch_view:get_row_count(View),
        ok = couch_view_merger_queue:queue(Queue, {row_count, RowCount}),
        case Keys of
        nil ->
            FoldOpts = couch_httpd_view:make_key_options(ViewArgs),
            {ok, _, _} = couch_view:fold(View, FoldlFun, [], FoldOpts);
        _ when is_list(Keys) ->
            lists:foreach(
                fun(K) ->
                    FoldOpts = couch_httpd_view:make_key_options(
                        ViewArgs#view_query_args{start_key = K, end_key = K}),
                    {ok, _, _} = couch_view:fold(View, FoldlFun, [], FoldOpts)
                end,
                Keys)
        end;
    false ->
        ok = couch_view_merger_queue:queue(Queue, revision_mismatch)
    end,
    catch couch_db:close(DDocDb).


map_set_view_folder(ViewSpec, MergeParams, UserCtx, DDoc, Queue) ->
    #set_view_spec{
        name = SetName, ddoc_id = DDocId
    } = ViewSpec,
    #index_merge{
        http_params = ViewArgs
    } = MergeParams,
    #view_query_args{
        include_docs = IncludeDocs,
        conflicts = Conflicts,
        stale = Stale
    } = ViewArgs,
    DDocDbName = ?master_dbname(SetName),

    PrepareResult = case (ViewSpec#set_view_spec.view =/= nil) andalso
        (ViewSpec#set_view_spec.group =/= nil) of
    true ->
        {ViewSpec#set_view_spec.view, ViewSpec#set_view_spec.group};
    false ->
        ViewGroupReq1 = #set_view_group_req{
            stale = Stale,
            update_stats = true
        },
        case prepare_set_view(
            ViewSpec, ViewGroupReq1, Queue, fun couch_set_view:get_map_view/5) of
        not_found ->
            ViewGroupReq2 = ViewGroupReq1#set_view_group_req{
                update_stats = false
            },
            case prepare_set_view(
                ViewSpec, ViewGroupReq2, Queue, fun couch_set_view:get_reduce_view/5) of
            {RedView, Group0} ->
                {couch_set_view:extract_map_view(RedView), Group0};
            Else ->
                Else
            end;
        Else ->
            Else
        end
    end,

    case PrepareResult of
    error ->
        %%  handled by prepare_set_view
        ok;
    {View, Group} ->
        queue_debug_info(ViewArgs, Group, Queue),
        try
            FoldFun = make_map_set_fold_fun(IncludeDocs, Conflicts, SetName,
                UserCtx, Queue),

            case not(couch_index_merger:should_check_rev(MergeParams, DDoc)) orelse
                couch_index_merger:ddoc_unchanged(DDocDbName, DDoc) of
            true ->
                {ok, RowCount} = couch_set_view:get_row_count(View),
                ok = couch_view_merger_queue:queue(Queue, {row_count, RowCount}),
                {ok, _, _} = couch_set_view:fold(Group, View, FoldFun, [], ViewArgs);
            false ->
                ok = couch_view_merger_queue:queue(Queue, revision_mismatch)
            end
        catch
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL,
                    couch_index_merger:ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            Stack = erlang:get_stacktrace(),
            ?LOG_ERROR("Caught unexpected error "
                       "while serving view query ~s/~s: ~p~n~p",
                       [SetName, DDocId, Error, Stack]),
            couch_view_merger_queue:queue(Queue, {error, ?LOCAL, to_binary(Error)})
        after
            couch_set_view:release_group(Group),
            ok = couch_view_merger_queue:done(Queue)
        end
    end.

fold_local_all_docs(nil, Db, Queue, ViewArgs) ->
    #view_query_args{
        start_key = StartKey,
        start_docid = StartDocId,
        end_key = EndKey,
        end_docid = EndDocId,
        direction = Dir,
        inclusive_end = InclusiveEnd,
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = ViewArgs,
    StartId = if is_binary(StartKey) -> StartKey;
        true -> StartDocId
    end,
    EndId = if is_binary(EndKey) -> EndKey;
        true -> EndDocId
    end,
    FoldOptions = [
        {start_key, StartId}, {dir, Dir},
        {if InclusiveEnd -> end_key; true -> end_key_gt end, EndId}
    ],
    FoldFun = fun(DocInfo, _Offset, Acc) ->
        case DocInfo of
        #doc_info{deleted = true} ->
            ok;
        _ ->
            Row = all_docs_row(DocInfo, Db, IncludeDocs, Conflicts),
            ok = couch_view_merger_queue:queue(Queue, Row)
        end,
        {ok, Acc}
    end,
    {ok, _LastOffset, _} = couch_db:enum_docs(Db, FoldFun, [], FoldOptions);

fold_local_all_docs(Keys, Db, Queue, ViewArgs) ->
    #view_query_args{
        direction = Dir,
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = ViewArgs,
    FoldFun = case Dir of
    fwd ->
        fun lists:foldl/3;
    rev ->
        fun lists:foldr/3
    end,
    FoldFun(
        fun(Key, _Acc) ->
            Row = case (catch couch_db:get_doc_info(Db, Key)) of
            {ok, #doc_info{} = DocInfo} ->
                all_docs_row(DocInfo, Db, IncludeDocs, Conflicts);
            not_found ->
                {{Key, error}, not_found}
            end,
            ok = couch_view_merger_queue:queue(Queue, Row)
        end, [], Keys).


all_docs_row(DocInfo, Db, IncludeDoc, Conflicts) ->
    #doc_info{id = Id, rev = Rev, deleted = Del} = DocInfo,
    Value = {[{<<"rev">>, couch_doc:rev_to_str(Rev)}] ++ case Del of
    true ->
        [{<<"deleted">>, true}];
    false ->
        []
    end},
    case IncludeDoc of
    true ->
        case Del of
        true ->
            DocVal = {<<"doc">>, null};
        false ->
            DocOptions = if Conflicts -> [conflicts]; true -> [] end,
            [DocVal] = couch_httpd_view:doc_member(Db, DocInfo, DocOptions),
            DocVal
        end,
        {{Id, Id}, Value, DocVal};
    false ->
        {{Id, Id}, Value}
    end.


http_view_fold(object_start, map, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end;
http_view_fold(object_start, red_map, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end;
http_view_fold(object_start, reduce, Queue) ->
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rc_1({key, <<"total_rows">>}, Queue) ->
    fun(Ev) -> http_view_fold_rc_2(Ev, Queue) end;
http_view_fold_rc_1(_Ev, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end.

http_view_fold_rc_2(RowCount, Queue) when is_number(RowCount) ->
    ok = couch_view_merger_queue:queue(Queue, {row_count, RowCount}),
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rows_1({key, <<"rows">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_view_fold_rows_2(Ev, Queue) end end;
http_view_fold_rows_1(_Ev, Queue) ->
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rows_2(array_end, Queue) ->
    fun(Ev) -> http_view_fold_extra(Ev, Queue) end;
http_view_fold_rows_2(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Row) ->
                http_view_fold_queue_row(Row, Queue),
                fun(Ev2) -> http_view_fold_rows_2(Ev2, Queue) end
            end)
    end.

http_view_fold_extra({key, <<"errors">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_view_fold_errors(Ev, Queue) end end;
http_view_fold_extra({key, <<"debug_info">>}, Queue) ->
    fun(object_start) -> fun(Ev) -> http_view_fold_debug_info(Ev, Queue, []) end end;
http_view_fold_extra(_Ev, _Queue) ->
    fun couch_index_merger:void_event/1.

http_view_fold_errors(array_end, _Queue) ->
    fun couch_index_merger:void_event/1;
http_view_fold_errors(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Error) ->
                http_view_fold_queue_error(Error, Queue),
                fun(Ev2) -> http_view_fold_errors(Ev2, Queue) end
            end)
    end.

http_view_fold_debug_info({key, Key}, Queue, Acc) ->
    fun(object_start) ->
        fun(Ev) ->
            json_stream_parse:collect_object(
                Ev,
                fun(DebugInfo) ->
                    fun(Ev2) -> http_view_fold_debug_info(Ev2, Queue, [{Key, DebugInfo} | Acc]) end
                end)
        end
    end;
http_view_fold_debug_info(object_end, Queue, Acc) ->
    case Acc of
    [{?LOCAL, Info}] ->
        ok;
    _ ->
        Info = {lists:reverse(Acc)}
    end,
    ok = couch_view_merger_queue:queue(Queue, {debug_info, ?l2b(get(from_url)), Info}),
    fun(Ev2) -> http_view_fold_extra(Ev2, Queue) end.


http_view_fold_queue_error({Props}, Queue) ->
    Reason = get_value(<<"reason">>, Props, null),
    ok = couch_view_merger_queue:queue(Queue, {error, get(from_url), Reason}).


http_view_fold_queue_row({Props}, Queue) ->
    Key = get_value(<<"key">>, Props, null),
    Id = get_value(<<"id">>, Props, nil),
    Val = get_value(<<"value">>, Props),
    Value = case get_value(<<"partition">>, Props, nil) of
    nil ->
        Val;
    PartId ->
        {PartId, Val}
    end,
    Row = case get_value(<<"error">>, Props, nil) of
    nil ->
        case Id of
        nil ->
            % reduce row
            {Key, Val};
        _ ->
            % map row
            case get_value(<<"doc">>, Props, nil) of
            nil ->
                {{Key, Id}, Value};
            Doc ->
                {{Key, Id}, Value, {doc, Doc}}
            end
        end;
    Error ->
        % error in a map row
        {{Key, error}, Error}
    end,
    ok = couch_view_merger_queue:queue(Queue, Row).

reduce_view_folder(_Db, #set_view_spec{} = ViewSpec, MergeParams,
                   _UserCtx, DDoc, Queue) ->
    reduce_set_view_folder(ViewSpec, MergeParams, DDoc, Queue);

reduce_view_folder(Db, ViewSpec, MergeParams, _UserCtx, DDoc, Queue) ->
    #simple_index_spec{
        ddoc_database = DDocDbName, ddoc_id = DDocId, index_name = ViewName
    } = ViewSpec,
    #index_merge{
        http_params = ViewArgs,
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    #view_query_args{
        stale = Stale
    } = ViewArgs,
    FoldlFun = make_reduce_fold_fun(ViewArgs, Queue),
    KeyGroupFun = make_group_rows_fun(ViewArgs),
    {DDocDb, View} = get_reduce_view(Db, DDocDbName, DDocId, ViewName, Stale),

    case not(couch_index_merger:should_check_rev(MergeParams, DDoc)) orelse
            couch_index_merger:ddoc_unchanged(DDocDb, DDoc) of
    true ->
        case Keys of
        nil ->
            FoldOpts = [{key_group_fun, KeyGroupFun} |
                couch_httpd_view:make_key_options(ViewArgs)],
            {ok, _} = couch_view:fold_reduce(View, FoldlFun, [], FoldOpts);
        _ when is_list(Keys) ->
            lists:foreach(
                fun(K) ->
                    FoldOpts = [{key_group_fun, KeyGroupFun} |
                        couch_httpd_view:make_key_options(
                            ViewArgs#view_query_args{
                                start_key = K, end_key = K})],
                    {ok, _} = couch_view:fold_reduce(View, FoldlFun, [], FoldOpts)
                end,
                Keys)
        end;
    false ->
        ok = couch_view_merger_queue:queue(Queue, revision_mismatch)
    end,
    catch couch_db:close(DDocDb).

reduce_set_view_folder(ViewSpec, MergeParams, DDoc, Queue) ->
    #set_view_spec{
        name = SetName, ddoc_id = DDocId
    } = ViewSpec,
    #index_merge{
        http_params = ViewArgs
    } = MergeParams,

    DDocDbName = ?master_dbname(SetName),
    PrepareResult = case (ViewSpec#set_view_spec.view =/= nil) andalso
        (ViewSpec#set_view_spec.group =/= nil) of
    true ->
        {ViewSpec#set_view_spec.view, ViewSpec#set_view_spec.group};
    false ->
        ViewGroupReq = #set_view_group_req{
            stale = ViewArgs#view_query_args.stale,
            update_stats = true
        },
        prepare_set_view(ViewSpec, ViewGroupReq, Queue, fun couch_set_view:get_reduce_view/5)
    end,

    case PrepareResult of
    error ->
        %%  handled by prepare_set_view
        ok;
    {View, Group} ->
        queue_debug_info(ViewArgs, Group, Queue),
        try
            FoldFun = make_reduce_fold_fun(ViewArgs, Queue),
            KeyGroupFun = make_group_rows_fun(ViewArgs),

            case not(couch_index_merger:should_check_rev(MergeParams, DDoc)) orelse
                couch_index_merger:ddoc_unchanged(DDocDbName, DDoc) of
            true ->
                {ok, _} = couch_set_view:fold_reduce(Group, View, FoldFun, [], KeyGroupFun, ViewArgs);
            false ->
                ok = couch_view_merger_queue:queue(Queue, revision_mismatch)
            end
        catch
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL,
                    couch_index_merger:ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            Stack = erlang:get_stacktrace(),
            ?LOG_ERROR("Caught unexpected error "
                       "while serving view query ~s/~s: ~p~n~p",
                       [SetName, DDocId, Error, Stack]),
            couch_view_merger_queue:queue(Queue, {error, ?LOCAL, to_binary(Error)})
        after
            couch_set_view:release_group(Group),
            ok = couch_view_merger_queue:done(Queue)
        end
    end.

get_reduce_view(Db, DDocDbName, DDocId, ViewName, Stale) ->
    GroupId = couch_index_merger:get_group_id(DDocDbName, DDocId),
    {ok, View, _} = couch_view:get_reduce_view(Db, GroupId, ViewName, Stale),
    case GroupId of
        {DDocDb, DDocId} -> {DDocDb, View};
        DDocId -> {nil, View}
    end.


make_group_rows_fun(#view_query_args{group_level = 0}) ->
    fun(_, _) -> true end;

make_group_rows_fun(#view_query_args{group_level = L}) when is_integer(L) ->
    fun({KeyA, _}, {KeyB, _}) when is_list(KeyA) andalso is_list(KeyB) ->
        lists:sublist(KeyA, L) == lists:sublist(KeyB, L);
    ({KeyA, _}, {KeyB, _}) ->
        KeyA == KeyB
    end;

make_group_rows_fun(_) ->
    fun({KeyA, _}, {KeyB, _}) -> KeyA == KeyB end.


make_reduce_fold_fun(#view_query_args{group_level = 0}, Queue) ->
    fun(_Key, Red, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, {null, Red}),
        {ok, Acc}
    end;

make_reduce_fold_fun(#view_query_args{group_level = L}, Queue) when is_integer(L) ->
    fun(Key, Red, Acc) when is_list(Key) ->
        ok = couch_view_merger_queue:queue(Queue, {lists:sublist(Key, L), Red}),
        {ok, Acc};
    (Key, Red, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, {Key, Red}),
        {ok, Acc}
    end;

make_reduce_fold_fun(_QueryArgs, Queue) ->
    fun(Key, Red, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, {Key, Red}),
        {ok, Acc}
    end.


get_map_view(Db, DDocDbName, DDocId, ViewName, Stale) ->
    GroupId = couch_index_merger:get_group_id(DDocDbName, DDocId),
    View = case couch_view:get_map_view(Db, GroupId, ViewName, Stale) of
    {ok, MapView, _} ->
        MapView;
    {not_found, _} ->
        {ok, RedView, _} = couch_view:get_reduce_view(
            Db, GroupId, ViewName, Stale),
        couch_view:extract_map_view(RedView)
    end,
    case GroupId of
        {DDocDb, DDocId} -> {DDocDb, View};
        DDocId -> {nil, View}
    end.

make_map_set_fold_fun(false, _Conflicts, _SetName, _UserCtx, Queue) ->
    fun({{Key, DocId}, {PartId, Value}}, _, Acc) ->
        Kv = {{Key, DocId}, {PartId, Value}},
        ok = couch_view_merger_queue:queue(Queue, Kv),
        {ok, Acc}
    end;

make_map_set_fold_fun(true, Conflicts, SetName, UserCtx, Queue) ->
    DocOpenOpts = if Conflicts -> [conflicts]; true -> [] end,
    fun({{Key, DocId}, {PartId, Value}} = Kv, _, Acc) ->
        JsonDoc = couch_set_view_http:get_row_doc(
                Kv, SetName, true, UserCtx, DocOpenOpts),
        Row = {{Key, DocId}, {PartId, Value}, {doc, JsonDoc}},
        ok = couch_view_merger_queue:queue(Queue, Row),
        {ok, Acc}
    end.

make_map_fold_fun(false, _Conflicts, _Db, Queue) ->
    fun(Row, _, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, Row),
        {ok, Acc}
    end;

make_map_fold_fun(true, Conflicts, Db, Queue) ->
    DocOpenOpts = if Conflicts -> [conflicts]; true -> [] end,
    fun({{_Key, error}, _Value} = Row, _, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, Row),
        {ok, Acc};
    ({{_Key, DocId} = Kd, {Props} = Value}, _, Acc) ->
        IncludeId = get_value(<<"_id">>, Props, DocId),
        [Doc] = couch_httpd_view:doc_member(Db, IncludeId, DocOpenOpts),
        ok = couch_view_merger_queue:queue(Queue, {Kd, Value, Doc}),
        {ok, Acc};
    ({{_Key, DocId} = Kd, Value}, _, Acc) ->
        [Doc] = couch_httpd_view:doc_member(Db, DocId, DocOpenOpts),
        ok = couch_view_merger_queue:queue(Queue, {Kd, Value, Doc}),
        {ok, Acc}
    end.

view_undefined_msg(SetName, DDocId) ->
    Msg = io_lib:format(
        "Undefined set view `~s` for `~s` design document.", [SetName, DDocId]),
    iolist_to_binary(Msg).

view_qs(ViewArgs, MergeParams) ->
    DefViewArgs = #view_query_args{},
    #view_query_args{
        start_key = StartKey, end_key = EndKey,
        start_docid = StartDocId, end_docid = EndDocId,
        direction = Dir,
        inclusive_end = IncEnd,
        group_level = GroupLevel,
        view_type = ViewType,
        include_docs = IncDocs,
        conflicts = Conflicts,
        stale = Stale,
        debug = Debug
    } = ViewArgs,
    #index_merge{on_error = OnError} = MergeParams,

    QsList = case StartKey =:= DefViewArgs#view_query_args.start_key of
    true ->
        [];
    false ->
        ["startkey=" ++ json_qs_val(StartKey)]
    end ++
    case EndKey =:= DefViewArgs#view_query_args.end_key of
    true ->
        [];
    false ->
        ["endkey=" ++ json_qs_val(EndKey)]
    end ++
    case {Dir, StartDocId =:= DefViewArgs#view_query_args.start_docid} of
    {fwd, false} ->
        ["startkey_docid=" ++ qs_val(StartDocId)];
    _ ->
        []
    end ++
    case {Dir, EndDocId =:= DefViewArgs#view_query_args.end_docid} of
    {fwd, false} ->
        ["endkey_docid=" ++ qs_val(EndDocId)];
    _ ->
        []
    end ++
    case Dir of
    fwd ->
        [];
    rev ->
        StartDocId1 = reverse_key_default(StartDocId),
        EndDocId1 = reverse_key_default(EndDocId),
        ["descending=true"] ++
        case StartDocId1 =:= DefViewArgs#view_query_args.start_docid of
        true ->
            [];
        false ->
            ["startkey_docid=" ++ qs_val(StartDocId1)]
        end ++
        case EndDocId1 =:= DefViewArgs#view_query_args.end_docid of
        true ->
            [];
        false ->
            ["endkey_docid=" ++ qs_val(EndDocId1)]
        end
    end ++
    case IncEnd =:= DefViewArgs#view_query_args.inclusive_end of
    true ->
        [];
    false ->
        ["inclusive_end=" ++ atom_to_list(IncEnd)]
    end ++
    case GroupLevel =:= DefViewArgs#view_query_args.group_level of
    true ->
        [];
    false ->
        case GroupLevel of
        exact ->
            ["group=true"];
        _ when is_number(GroupLevel) ->
            ["group_level=" ++ integer_to_list(GroupLevel)]
        end
    end ++
    case ViewType of
    red_map ->
        ["reduce=false"];
    _ ->
        []
    end ++
    case IncDocs =:= DefViewArgs#view_query_args.include_docs of
    true ->
        [];
    false ->
        ["include_docs=" ++ atom_to_list(IncDocs)]
    end ++
    case Conflicts =:= DefViewArgs#view_query_args.conflicts of
    true ->
        [];
    false ->
        ["conflicts=" ++ atom_to_list(Conflicts)]
    end ++
    %% we now have different default
    case Stale =:= ?DEFAULT_STALENESS of
    true ->
        [];
    false ->
        ["stale=" ++ atom_to_list(Stale)]
    end ++
    case OnError =:= ?ON_ERROR_DEFAULT of
    true ->
        [];
    false ->
        ["on_error=" ++ atom_to_list(OnError)]
    end ++
    case Debug =:= DefViewArgs#view_query_args.debug of
    true ->
        [];
    false ->
        ["debug=" ++ atom_to_list(Debug)]
    end,
    case QsList of
    [] ->
        [];
    _ ->
        "?" ++ string:join(QsList, "&")
    end.

json_qs_val(Value) ->
    couch_httpd:quote(?b2l(iolist_to_binary(?JSON_ENCODE(Value)))).

qs_val(Value) ->
    couch_httpd:quote(couch_util:to_list(Value)).

reverse_key_default(?MIN_STR) -> ?MAX_STR;
reverse_key_default(?MAX_STR) -> ?MIN_STR;
reverse_key_default(Key) -> Key.


queue_debug_info(#view_query_args{debug = false}, _Group, _Queue) ->
    ok;
queue_debug_info(_QueryArgs, #set_view_group{} = Group, Queue) ->
    #set_view_debug_info{
        original_abitmask = OrigMainAbitmask,
        original_pbitmask = OrigMainPbitmask,
        stats = Stats,
        replica_partitions = ReplicaPartitions
    } = Group#set_view_group.debug_info,
    OrigMainActive = couch_set_view_util:decode_bitmask(OrigMainAbitmask),
    ModMainActive = couch_set_view_util:decode_bitmask(?set_abitmask(Group)),
    OrigMainPassive = couch_set_view_util:decode_bitmask(OrigMainPbitmask),
    ModMainPassive = couch_set_view_util:decode_bitmask(?set_pbitmask(Group)),
    MainCleanup = couch_set_view_util:decode_bitmask(?set_cbitmask(Group)),
    % 0 padded so that a pretty print JSON can sanely sort the keys (partition IDs)
    IndexedSeqs = [{?l2b(io_lib:format("~4..0b", [P])), S} || {P, S} <- ?set_seqs(Group)],
    MainInfo = [
        {<<"active_partitions">>, ordsets:from_list(ModMainActive)},
        {<<"original_active_partitions">>, ordsets:from_list(OrigMainActive)},
        {<<"passive_partitions">>, ordsets:from_list(ModMainPassive)},
        {<<"original_passive_partitions">>, ordsets:from_list(OrigMainPassive)},
        {<<"cleanup_partitions">>, ordsets:from_list(MainCleanup)},
        {<<"replica_partitions">>, ordsets:from_list(ReplicaPartitions)},
        {<<"replicas_on_transfer">>, ?set_replicas_on_transfer(Group)},
        {<<"indexed_seqs">>, {IndexedSeqs}},
        {<<"stats">>, set_view_group_stats_ejson(Stats)}
    ],
    RepInfo = replica_group_debug_info(Group),
    Info = case RepInfo of
    [] ->
        { [{<<"main_group">>, {MainInfo}}] };
    _ ->
        { [{<<"main_group">>, {MainInfo}}, {<<"replica_group">>, {RepInfo}}] }
    end,
    ok = couch_view_merger_queue:queue(Queue, {debug_info, ?LOCAL, Info}).

replica_group_debug_info(#set_view_group{replica_group = nil}) ->
    [];
replica_group_debug_info(#set_view_group{replica_group = RepGroup}) ->
    #set_view_group{
        debug_info = #set_view_debug_info{
            original_abitmask = OrigRepAbitmask,
            original_pbitmask = OrigRepPbitmask,
            stats = Stats
        }
    } = RepGroup,
    OrigRepActive = couch_set_view_util:decode_bitmask(OrigRepAbitmask),
    ModRepActive = couch_set_view_util:decode_bitmask(?set_abitmask(RepGroup)),
    OrigRepPassive = couch_set_view_util:decode_bitmask(OrigRepPbitmask),
    ModRepPassive = couch_set_view_util:decode_bitmask(?set_pbitmask(RepGroup)),
    RepCleanup = couch_set_view_util:decode_bitmask(?set_cbitmask(RepGroup)),
    % 0 padded so that a pretty print JSON can sanely sort the keys (partition IDs)
    IndexedSeqs = [{?l2b(io_lib:format("~4..0b", [P])), S} || {P, S} <- ?set_seqs(RepGroup)],
    [
        {<<"replica_active_partitions">>, ordsets:from_list(ModRepActive)},
        {<<"replica_original_active_partitions">>, ordsets:from_list(OrigRepActive)},
        {<<"replica_passive_partitions">>, ordsets:from_list(ModRepPassive)},
        {<<"replica_original_passive_partitions">>, ordsets:from_list(OrigRepPassive)},
        {<<"replica_cleanup_partitions">>, ordsets:from_list(RepCleanup)},
        {<<"replica_indexed_seqs">>, {IndexedSeqs}},
        {<<"replica_stats">>, set_view_group_stats_ejson(Stats)}
    ].


set_view_group_stats_ejson(Stats) ->
    StatNames = record_info(fields, set_view_group_stats),
    StatPoses = lists:seq(2, record_info(size, set_view_group_stats)),
    {lists:foldl(
        fun({ets_key, _}, Acc) ->
            Acc;
        ({StatName, StatPos}, Acc) ->
            [{StatName, element(StatPos, Stats)} | Acc]
        end,
        [],
        lists:zip(StatNames, StatPoses))}.
