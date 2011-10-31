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

-export([query_view/2]).

-include("couch_db.hrl").
-include("couch_view_merger.hrl").
-include("couch_set_view.hrl").

-define(LOCAL, <<"local">>).

-define(RETRY_INTERVAL, 1000).
-define(MAX_RETRIES, 30).

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1,
    get_nested_json_value/2
]).

-record(merge_params, {
    view_name,
    queue,
    rered_fun,
    rered_lang,
    less_fun,
    collector,
    skip,
    limit,
    row_acc = []
}).

-record(httpdb, {
   url,
   timeout,
   headers = [{"Accept", "application/json"}],
   ibrowse_options = []
}).


query_view(Req, ViewMergeParams) ->
    query_view_loop(Req, ViewMergeParams, ?MAX_RETRIES).

query_view_loop(_Req, _ViewMergeParams, 0) ->
    throw({error, revision_sync_failed});
query_view_loop(Req, ViewMergeParams, N) ->
    try
        do_query_view(Req, ViewMergeParams)
    catch
    throw:retry ->
        timer:sleep(?RETRY_INTERVAL),
        query_view_loop(Req, ViewMergeParams, N - 1)
    end.

do_query_view(#httpd{user_ctx = UserCtx} = Req, ViewMergeParams) ->
    #view_merge{
       views = Views, keys = Keys, callback = Callback, user_acc = UserAcc,
       rereduce_fun = InRedFun, rereduce_fun_lang = InRedFunLang,
       ddoc_revision = DesiredDDocRevision
    } = ViewMergeParams,

    ?LOG_DEBUG("Running a view merging for the following views: ~p", [Views]),

    {ok, DDoc, DDocViewSpec} = get_first_ddoc(Views, ViewMergeParams, UserCtx),
    DDocRev = ddoc_rev(DDoc),

    case should_check_rev(ViewMergeParams, DDoc) of
    true ->
        case DesiredDDocRevision of
        auto ->
            ok;
        DDocRev ->
            ok;
        _ ->
            throw({error, revision_mismatch})
        end;
    false ->
        ok
    end,

    % view type =~ query type
    {Collation, ViewType0, ViewLang} = view_details(DDoc, DDocViewSpec),
    ViewType = case {ViewType0, couch_httpd:qs_value(Req, "reduce", "true")} of
    {reduce, "false"} ->
       red_map;
    _ ->
       ViewType0
    end,
    {RedFun, RedFunLang} = case {ViewType, InRedFun} of
    {reduce, nil} ->
        {reduce_function(DDoc, DDocViewSpec), ViewLang};
    {reduce, _} when is_binary(InRedFun) ->
        {InRedFun, InRedFunLang};
    _ ->
        {nil, nil}
    end,
    ViewArgs = couch_httpd_view:parse_view_params(Req, Keys, ViewType),
    LessFun = view_less_fun(Collation, ViewArgs#view_query_args.direction, ViewType),
    {FoldFun, MergeFun} = case ViewType of
    reduce ->
        {fun reduce_view_folder/7, fun merge_reduce_views/1};
    _ when ViewType =:= map; ViewType =:= red_map ->
        {fun map_view_folder/7, fun merge_map_views/1}
    end,
    NumFolders = length(Views),
    QueueLessFun = fun
        (set_view_outdated, _) ->
            true;
        (_, set_view_outdated) ->
            false;
        (revision_mismatch, _) ->
            true;
        (_, revision_mismatch) ->
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
            LessFun(RowA, RowB)
    end,
    {ok, Queue} = couch_view_merger_queue:start_link(NumFolders, QueueLessFun),
    Collector = make_collector(ViewType, NumFolders, Callback, UserAcc),
    Folders = lists:foldr(
        fun(View, Acc) ->
            Pid = spawn_link(fun() ->
                FoldFun(View, ViewMergeParams, Req, Keys, ViewArgs, DDoc, Queue)
            end),
            [Pid | Acc]
        end,
        [], Views),
    MergeParams = #merge_params{
        view_name = view_name(DDocViewSpec),
        queue = Queue,
        rered_fun = RedFun,
        rered_lang = RedFunLang,
        less_fun = LessFun,
        collector = Collector,
        skip = ViewArgs#view_query_args.skip,
        limit = ViewArgs#view_query_args.limit
    },

    try
        case MergeFun(MergeParams) of
        set_view_outdated ->
            throw({error, set_view_outdated});
        revision_mismatch ->
            case DesiredDDocRevision of
            auto ->
                throw(retry);
            _ ->
                throw({error, revision_mismatch})
            end;
        {ok, Resp} ->
            Resp;
        {stop, Resp} ->
            Resp
        end
    after
        lists:foreach(
            fun (P) ->
                catch unlink(P),
                catch exit(P, kill)
            end, Folders),
        catch unlink(Queue),
        catch exit(Queue, kill)
    end.

view_details(nil, #simple_view_spec{view_name = <<"_all_docs">>}) ->
    {<<"raw">>, map, nil};

view_details(#doc{body = DDoc}, ViewSpec) ->
    ViewName = view_name(ViewSpec),

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

reduce_function(#doc{body = DDoc}, ViewSpec) ->
    ViewName = view_name(ViewSpec),
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
        fun erlang:'<'/2
    end,
    case Dir of
    fwd ->
        LessFun;
    rev ->
        fun(A, B) -> not LessFun(A, B) end
    end.

make_collector(ViewType, NumFolders, Callback, UserAcc) ->
    fun (Item) ->
        collector_loop(ViewType, NumFolders, Callback, UserAcc, Item)
    end.

collector_loop(red_map, NumFolders, Callback, UserAcc, Item) ->
    collector_loop(map, NumFolders, Callback, UserAcc, Item);

collector_loop(map, NumFolders, Callback, UserAcc, Item) ->
    collect_row_count(map, NumFolders, 0, Callback, UserAcc, Item);

collector_loop(reduce, _NumFolders, Callback, UserAcc, Item) ->
    {ok, UserAcc2} = Callback(start, UserAcc),
    collect_rows(reduce, Callback, UserAcc2, Item).


collect_row_count(ViewType, RecvCount, AccCount, Callback, UserAcc, Item) ->
    case Item of
    {error, _DbUrl, _Reason} = Error ->
        case Callback(Error, UserAcc) of
        {stop, Resp} ->
            {stop, Resp};
        {ok, UserAcc2} ->
            case RecvCount > 1 of
            false ->
                {ok, UserAcc3} = Callback({start, AccCount}, UserAcc2),
                {ok,
                 fun (It) ->
                     collect_rows(ViewType, Callback, UserAcc3, It)
                 end};
            true ->
                {ok,
                 fun (It) ->
                     collect_row_count(ViewType, RecvCount - 1, AccCount,
                                       Callback, UserAcc2, It)
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
            {ok,
             fun (It) ->
                 collect_rows(ViewType, Callback, UserAcc2, It)
             end};
        true ->
            {ok,
             fun (It) ->
                 collect_row_count(ViewType, RecvCount - 1,
                     AccCount2, Callback, UserAcc, It)
             end}
        end
    end.

collect_rows(ViewType, Callback, UserAcc, Item) ->
    case Item of
    {error, _DbUrl, _Reason} = Error ->
        case Callback(Error, UserAcc) of
        {stop, Resp} ->
            {stop, Resp};
        {ok, UserAcc2} ->
            {ok,
             fun (It) ->
                 collect_rows(ViewType, Callback, UserAcc2, It)
             end}
        end;
    {row, Row} ->
        RowEJson = view_row_obj(ViewType, Row),
        {ok, UserAcc2} = Callback({row, RowEJson}, UserAcc),
        {ok,
         fun (It) ->
                 collect_rows(ViewType, Callback, UserAcc2, It)
         end};
    stop ->
        {ok, UserAcc2} = Callback(stop, UserAcc),
        {stop, UserAcc2}
    end.


view_row_obj(map, {{Key, error}, Value}) ->
    {[{key, Key}, {error, Value}]};

view_row_obj(map, {{Key, DocId}, Value}) ->
    {[{id, DocId}, {key, Key}, {value, Value}]};

view_row_obj(map, {{Key, DocId}, Value, Doc}) ->
    {[{id, DocId}, {key, Key}, {value, Value}, Doc]};

view_row_obj(reduce, {Key, Value}) ->
    {[{key, Key}, {value, Value}]}.


merge_map_views(#merge_params{limit = 0, collector = Col}) ->
    Col(stop);

merge_map_views(#merge_params{row_acc = []} = Params) ->
    #merge_params{
        queue = Queue, limit = Limit, skip = Skip,
        collector = Col, view_name = ViewName
    } = Params,
    case couch_view_merger_queue:pop(Queue) of
    closed ->
        {stop, Resp} = Col(stop),
        {ok, Resp};
    {ok, revision_mismatch} ->
        revision_mismatch;
    {ok, set_view_outdated} ->
        set_view_outdated;
    {ok, {error, _Url, _Reason} = Error} ->
        ok = couch_view_merger_queue:flush(Queue),
        case Col(Error) of
        {ok, Col2} ->
            merge_map_views(Params#merge_params{collector=Col2});
        {stop, Resp} ->
            {stop, Resp}
        end;
    {ok, {row_count, _} = RowCount} ->
        {ok, Col2} = Col(RowCount),
        ok = couch_view_merger_queue:flush(Queue),
        merge_map_views(Params#merge_params{collector=Col2});
    {ok, MinRow} ->
        {RowToSend, RestToSend} = handle_duplicates(ViewName, MinRow, Queue),
        ok = couch_view_merger_queue:flush(Queue),
        case Skip > 0 of
        true ->
            Limit2 = Limit,
            Col2 = Col;
        false ->
            {ok, Col2} = Col({row, RowToSend}),
            Limit2 = dec_counter(Limit)
        end,
        Params2 = Params#merge_params{
            skip = dec_counter(Skip), limit = Limit2, row_acc = RestToSend,
            collector = Col2
        },
        merge_map_views(Params2)
    end;

merge_map_views(#merge_params{row_acc = [RowToSend | Rest]} = Params) ->
    #merge_params{
        limit = Limit, skip = Skip, collector = Col
    } = Params,
    case Skip > 0 of
    true ->
        Limit2 = Limit,
        Col2 = Col;
    false ->
        {ok, Col2} = Col({row, RowToSend}),
        Limit2 = dec_counter(Limit)
    end,
    Params2 = Params#merge_params{
        skip = dec_counter(Skip), limit = Limit2, row_acc = Rest,
        collector = Col2
    },
    merge_map_views(Params2).


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


merge_reduce_views(#merge_params{limit = 0, collector = Col}) ->
    Col(stop);

merge_reduce_views(Params) ->
    #merge_params{
        queue = Queue, limit = Limit, skip = Skip, collector = Col
    } = Params,
    case couch_view_merger_queue:pop(Queue) of
    closed ->
        {stop, Resp} = Col(stop),
        {ok, Resp};
    {ok, revision_mismatch} ->
        revision_mismatch;
    {ok, set_view_outdated} ->
        set_view_outdated;
    {ok, {error, _Url, _Reason} = Error} ->
        ok = couch_view_merger_queue:flush(Queue),
        case Col(Error) of
        {ok, Col2} ->
            merge_reduce_views(Params#merge_params{collector = Col2});
        {stop, Resp} ->
            {stop, Resp}
        end;
    {ok, {row_count, _} = RowCount} ->
        {ok, Col2} = Col(RowCount),
        ok = couch_view_merger_queue:flush(Queue),
        merge_reduce_views(Params#merge_params{collector = Col2});
    {ok, MinRow} ->
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
                    Limit2 = dec_counter(Limit)
                end,
                Params2 = Params#merge_params{
                    skip = dec_counter(Skip), limit = Limit2, collector = Col3
                },
                merge_reduce_views(Params2)
            end
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


rereduce(Rows, #merge_params{rered_lang = Lang, rered_fun = RedFun}) ->
    Reds = [[Val] || {_Key, Val} <- Rows],
    {ok, [Value]} = couch_query_servers:rereduce(Lang, [RedFun], Reds),
    Value.


dec_counter(0) -> 0;
dec_counter(N) -> N - 1.

get_set_view(GetSetViewFn, SetName, DDocId, ViewName, Stale, Partitions) ->
    case GetSetViewFn(SetName, DDocId, ViewName, ok, Partitions) of
    {ok, StaleView, StaleGroup, []} ->
        case Stale of
        ok ->
            {ok, StaleView, StaleGroup, []};
        _Other ->
            couch_set_view:release_group(StaleGroup),
            GetSetViewFn(SetName, DDocId, ViewName, Stale, Partitions)
        end;
    Other ->
        Other
    end.

prepare_set_view(ViewSpec, ViewArgs, Queue, GetSetViewFn) ->
    #set_view_spec{
        name = SetName,
        ddoc_id = DDocId, view_name = ViewName,
        partitions = Partitions
    } = ViewSpec,
    #view_query_args{
        stale = Stale
    } = ViewArgs,

    try
        case get_set_view(GetSetViewFn, SetName, DDocId,
                          ViewName, Stale, Partitions) of
        {ok, View, Group, []} ->
            {View, Group};
        {ok, _, Group, MissingPartitions} ->
            ?LOG_INFO("Set view `~s`, group `~s`, missing partitions: ~w",
                      [SetName, DDocId, MissingPartitions]),
            couch_set_view:release_group(Group),
            couch_view_merger_queue:queue(Queue, set_view_outdated),
            couch_view_merger_queue:done(Queue),
            error
        end
    catch
    view_undefined ->
        couch_view_merger_queue:queue(Queue,
            {error, ?LOCAL, view_undefined_msg(SetName, DDocId)}),
        couch_view_merger_queue:done(Queue),
        error
    end.

map_view_folder(#simple_view_spec{database = <<"http://", _/binary>>} = ViewSpec,
                MergeParams, _Req, Keys, ViewArgs, DDoc, Queue) ->
    http_view_folder(ViewSpec, MergeParams, Keys, ViewArgs, DDoc, Queue);

map_view_folder(#simple_view_spec{database = <<"https://", _/binary>>} = ViewSpec,
                MergeParams, _Req, Keys, ViewArgs, DDoc, Queue) ->
    http_view_folder(ViewSpec, MergeParams, Keys, ViewArgs, DDoc, Queue);

map_view_folder(#merged_view_spec{} = ViewSpec,
                MergeParams, _Req, Keys, ViewArgs, DDoc, Queue) ->
    http_view_folder(ViewSpec, MergeParams, Keys, ViewArgs, DDoc, Queue);

map_view_folder(#simple_view_spec{view_name = <<"_all_docs">>, database = DbName},
    _MergeParams, #httpd{user_ctx = UserCtx}, Keys, ViewArgs, _DDoc, Queue) ->
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, Db} ->
        try
            {ok, Info} = couch_db:get_db_info(Db),
            ok = couch_view_merger_queue:queue(
                Queue, {row_count, get_value(doc_count, Info)}),
            % TODO: add support for ?update_seq=true and offset
            fold_local_all_docs(Keys, Db, Queue, ViewArgs)
        after
            ok = couch_view_merger_queue:done(Queue),
            couch_db:close(Db)
        end;
    {not_found, _} ->
        ok = couch_view_merger_queue:queue(
               Queue, {error, ?LOCAL, db_not_found_msg(DbName)}),
        ok = couch_view_merger_queue:done(Queue)
    end;

map_view_folder(#set_view_spec{} = ViewSpec, MergeParams,
                Req, Keys, ViewArgs, DDoc, Queue) ->
    map_set_view_folder(ViewSpec, MergeParams, Req, Keys, ViewArgs, DDoc, Queue);

map_view_folder(ViewSpec, MergeParams,
                #httpd{user_ctx = UserCtx}, Keys, ViewArgs, DDoc, Queue) ->
    #simple_view_spec{
        database = DbName, ddoc_database = DDocDbName,
        ddoc_id = DDocId, view_name = ViewName
    } = ViewSpec,
    #view_query_args{
        stale = Stale,
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = ViewArgs,
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, Db} ->
        try
            FoldlFun = make_map_fold_fun(IncludeDocs, Conflicts, Db, Queue),
            {DDocDb, View} = get_map_view(Db, DDocDbName, DDocId, ViewName, Stale),

            case not(should_check_rev(MergeParams, DDoc)) orelse
                ddoc_unchanged(DDocDb, DDoc) of
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
            catch couch_db:close(DDocDb)
        catch
        {not_found, Reason} when Reason =:= missing; Reason =:= deleted ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DbName, DDocId)});
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            couch_view_merger_queue:queue(Queue, {error, ?LOCAL, to_binary(Error)})
        after
            ok = couch_view_merger_queue:done(Queue),
            couch_db:close(Db)
        end;
    {not_found, _} ->
        ok = couch_view_merger_queue:queue(
               Queue, {error, ?LOCAL, db_not_found_msg(DbName)}),
        ok = couch_view_merger_queue:done(Queue)
    end.

map_set_view_folder(ViewSpec, MergeParams, Req, Keys, ViewArgs, DDoc, Queue) ->
    #set_view_spec{
        name = SetName, ddoc_id = DDocId
    } = ViewSpec,

    DefaultViewArgs = #view_query_args{},
    Limit = DefaultViewArgs#view_query_args.limit,
    Skip = DefaultViewArgs#view_query_args.skip,
    DDocDbName = ?master_dbname(SetName),

    case prepare_set_view(ViewSpec, ViewArgs, Queue,
                          fun couch_set_view:get_map_view/5) of
    error ->
        %%  handled by prepare_set_view
        ok;
    {View, Group} ->
        try
            StartRespFun =
                fun (_Req, _Etag, _TotalViewCount, _Offset, RowFunAcc) ->
                        {ok, nil, RowFunAcc}
                end,
            SendRowFun =
                fun (_Resp, Row, nil, RowFunAcc) ->
                        ok = couch_view_merger_queue:queue(Queue, Row),
                        {ok, RowFunAcc};
                    (_Resp, {Kd, Value}, Doc, RowFunAcc) ->
                        Row = {Kd, Value, {doc, Doc}},
                        ok = couch_view_merger_queue:queue(Queue, Row),
                        {ok, RowFunAcc}
                end,

            HelperFuns =
                #view_fold_helper_funs{
                    start_response = StartRespFun,
                    send_row = SendRowFun,
                    reduce_count = fun couch_set_view:reduce_to_count/1
                },

            {ok, RowCount} = couch_set_view:get_row_count(View),

            FoldFun = couch_httpd_set_view:make_view_fold_fun(Req, ViewArgs,
                                                              nil, Group,
                                                              RowCount, HelperFuns),
            FoldAccInit = {Limit, Skip, undefined, []},

            case not(should_check_rev(MergeParams, DDoc)) orelse
                ddoc_unchanged(DDocDbName, DDoc) of
            true ->
                ok = couch_view_merger_queue:queue(Queue, {row_count, RowCount}),

                case Keys of
                nil ->
                    FoldOpts = couch_httpd_set_view:make_key_options(ViewArgs),
                    {ok, _, _} = couch_set_view:fold(Group, View, FoldFun,
                                     FoldAccInit, FoldOpts);
                _ when is_list(Keys) ->
                    {_, _} =
                        lists:foldl(
                            fun(Key, {_, FoldAcc}) ->
                                FoldOpts =
                                    couch_httpd_set_view:make_key_options(
                                        ViewArgs#view_query_args{
                                            start_key=Key, end_key=Key}),

                                {ok, LastReduce, FoldResult} =
                                    couch_set_view:fold(Group, View, FoldFun,
                                        FoldAcc, FoldOpts),
                        {LastReduce, FoldResult}
                    end, {{[],[]}, FoldAccInit}, Keys)
                end;
            false ->
                ok = couch_view_merger_queue:queue(Queue, revision_mismatch)
            end
        catch
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
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
    FoldFun = fun(FullDocInfo, _Offset, Acc) ->
        DocInfo = couch_doc:to_doc_info(FullDocInfo),
        #doc_info{revs = [#rev_info{deleted = Deleted} | _]} = DocInfo,
        case Deleted of
        true ->
            ok;
        false ->
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
    #doc_info{id = Id, revs = [RevInfo | _]} = DocInfo,
    #rev_info{rev = Rev, deleted = Del} = RevInfo,
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


http_view_folder(ViewSpec, MergeParams, Keys, ViewArgs, DDoc, Queue) ->
    {Url, Method, Headers, Body, Options} = http_view_folder_req_details(
        ViewSpec, MergeParams, Keys, ViewArgs, DDoc),
    {ok, Conn} = ibrowse:spawn_link_worker_process(Url),
    R =  ibrowse:send_req_direct(
           Conn, Url, Headers, Method, Body,
           [{stream_to, {self(), once}} | Options]),

    case R of
    {error, Reason} ->
        ok = couch_view_merger_queue:queue(Queue,
            {error, Url, ibrowse_error_msg(Reason)}),
        ok = couch_view_merger_queue:done(Queue);
    {ibrowse_req_id, ReqId} ->
        receive
        {ibrowse_async_headers, ReqId, "200", _RespHeaders} ->
            ibrowse:stream_next(ReqId),
            DataFun = fun() -> stream_data(ReqId) end,
            EventFun = fun(Ev) ->
                http_view_fold(Ev, ViewArgs#view_query_args.view_type, Queue)
            end,
            try
                json_stream_parse:events(DataFun, EventFun)
            catch throw:{error, Error} ->
                ok = couch_view_merger_queue:queue(Queue, {error, Url, Error})
            after
                stop_conn(Conn),
                ok = couch_view_merger_queue:done(Queue)
            end;
        {ibrowse_async_headers, ReqId, Code, _RespHeaders} ->
            Error = try
                stream_all(ReqId, [])
            catch throw:{error, _Error} ->
                <<"Error code ", (?l2b(Code))/binary>>
            end,
            case (catch ?JSON_DECODE(Error)) of
            {Props} when is_list(Props) ->
                case {get_value(<<"error">>, Props), get_value(<<"reason">>, Props)} of
                {<<"not_found">>, Reason} when
                        Reason =/= <<"missing">>, Reason =/= <<"deleted">> ->
                    ok = couch_view_merger_queue:queue(Queue, {error, Url, Reason});
                {<<"not_found">>, _} ->
                    ok = couch_view_merger_queue:queue(Queue, {error, Url, <<"not_found">>});
                {<<"error">>, <<"revision_mismatch">>} ->
                    ok = couch_view_merger_queue:queue(Queue, revision_mismatch);
                {<<"error">>, <<"set_view_outdated">>} ->
                    ?LOG_DEBUG("Got `set_view_outdated` from ~s", [Url]),
                    ok = couch_view_merger_queue:queue(Queue, set_view_outdated);
                JsonError ->
                    ok = couch_view_merger_queue:queue(
                        Queue, {error, Url, to_binary(JsonError)})
                end;
            _ ->
                ok = couch_view_merger_queue:queue(Queue, {error, Url, to_binary(Error)})
            end,
            ok = couch_view_merger_queue:done(Queue),
            stop_conn(Conn);
        {ibrowse_async_response, ReqId, {error, Error}} ->
            stop_conn(Conn),
            ok = couch_view_merger_queue:queue(Queue, {error, Url, Error}),
            ok = couch_view_merger_queue:done(Queue)
        end
    end.


http_view_folder_req_details(#merged_view_spec{
        url = MergeUrl0, ejson_spec = {EJson}},
        MergeParams, Keys, ViewArgs, DDoc) ->
    {ok, #httpdb{url = Url, ibrowse_options = Options} = Db} =
        open_db(MergeUrl0, nil, MergeParams),
    MergeUrl = Url ++ view_qs(ViewArgs),
    Headers = [{"Content-Type", "application/json"} | Db#httpdb.headers],

    EJson1 = case Keys of
    nil ->
        EJson;
    _ ->
        [{<<"keys">>, Keys} | EJson]
    end,

    EJson2 = case should_check_rev(MergeParams, DDoc) of
    true ->
        P = fun (Tuple) -> element(1, Tuple) =/= <<"ddoc_revision">> end,
        [{<<"ddoc_revision">>, ddoc_rev_str(DDoc)} | lists:filter(P, EJson1)];
    false ->
        EJson1
    end,

    Body = {EJson2},
    put(from_url, Url),
    {MergeUrl, post, Headers, ?JSON_ENCODE(Body), Options};

http_view_folder_req_details(#simple_view_spec{
        database = DbUrl, ddoc_id = DDocId, view_name = ViewName},
        MergeParams, Keys, ViewArgs, _DDoc) ->
    {ok, #httpdb{url = Url, ibrowse_options = Options} = Db} =
        open_db(DbUrl, nil, MergeParams),
    ViewUrl = Url ++ case ViewName of
    <<"_all_docs">> ->
        "_all_docs";
    _ ->
        ?b2l(DDocId) ++ "/_view/" ++ ?b2l(ViewName)
    end ++ view_qs(ViewArgs),
    Headers = [{"Content-Type", "application/json"} | Db#httpdb.headers],
    put(from_url, DbUrl),
    case Keys of
    nil ->
        {ViewUrl, get, [], [], Options};
    _ ->
        {ViewUrl, post, Headers, ?JSON_ENCODE({[{<<"keys">>, Keys}]}), Options}
    end.


stream_data(ReqId) ->
    receive
    {ibrowse_async_response, ReqId, {error, _} = Error} ->
        throw(Error);
    {ibrowse_async_response, ReqId, <<>>} ->
        ibrowse:stream_next(ReqId),
        stream_data(ReqId);
    {ibrowse_async_response, ReqId, Data} ->
        ibrowse:stream_next(ReqId),
        {Data, fun() -> stream_data(ReqId) end};
    {ibrowse_async_response_end, ReqId} ->
        {<<>>, fun() -> throw({error, <<"more view data expected">>}) end}
    end.


stream_all(ReqId, Acc) ->
    case stream_data(ReqId) of
    {<<>>, _} ->
        iolist_to_binary(lists:reverse(Acc));
    {Data, _} ->
        stream_all(ReqId, [Data | Acc])
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
    fun(Ev) -> http_view_fold_errors_1(Ev, Queue) end;
http_view_fold_rows_2(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Row) ->
                http_view_fold_queue_row(Row, Queue),
                fun(Ev2) -> http_view_fold_rows_2(Ev2, Queue) end
            end)
    end.

http_view_fold_errors_1({key, <<"errors">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_view_fold_errors_2(Ev, Queue) end end;
http_view_fold_errors_1(_Ev, _Queue) ->
    fun void_event/1.

http_view_fold_errors_2(array_end, _Queue) ->
    fun void_event/1;
http_view_fold_errors_2(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Error) ->
                http_view_fold_queue_error(Error, Queue),
                fun(Ev2) -> http_view_fold_errors_2(Ev2, Queue) end
            end)
    end.


http_view_fold_queue_error({Props}, Queue) ->
    From0 = get_value(<<"from">>, Props, ?LOCAL),
    From = case From0 of
        ?LOCAL ->
        get(from_url);
    _ ->
        From0
    end,
    Reason = get_value(<<"reason">>, Props, null),
    ok = couch_view_merger_queue:queue(Queue, {error, From, Reason}).


http_view_fold_queue_row({Props}, Queue) ->
    Key = get_value(<<"key">>, Props, null),
    Id = get_value(<<"id">>, Props, nil),
    Val = get_value(<<"value">>, Props),
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
                {{Key, Id}, Val};
            Doc ->
                {{Key, Id}, Val, {doc, Doc}}
            end
        end;
    Error ->
        % error in a map row
        {{Key, error}, Error}
    end,
    ok = couch_view_merger_queue:queue(Queue, Row).

void_event(_Ev) ->
    fun void_event/1.

reduce_view_folder(#simple_view_spec{database = <<"http://", _/binary>>} = ViewSpec,
                MergeParams, _Req, Keys, ViewArgs, DDoc, Queue) ->
    http_view_folder(ViewSpec, MergeParams, Keys, ViewArgs, DDoc, Queue);

reduce_view_folder(#simple_view_spec{database = <<"https://", _/binary>>} = ViewSpec,
                MergeParams, _Req, Keys, ViewArgs, DDoc, Queue) ->
    http_view_folder(ViewSpec, MergeParams, Keys, ViewArgs, DDoc, Queue);

reduce_view_folder(#merged_view_spec{} = ViewSpec,
                MergeParams, _Req, Keys, ViewArgs, DDoc, Queue) ->
    http_view_folder(ViewSpec, MergeParams, Keys, ViewArgs, DDoc, Queue);

reduce_view_folder(#set_view_spec{} = ViewSpec, MergeParams,
                   Req, Keys, ViewArgs, DDoc, Queue) ->
    reduce_set_view_folder(ViewSpec, MergeParams, Req,
                           Keys, ViewArgs, DDoc, Queue);

reduce_view_folder(ViewSpec, MergeParams,
                   #httpd{user_ctx = UserCtx}, Keys, ViewArgs, DDoc, Queue) ->
    #simple_view_spec{
        database = DbName, ddoc_database = DDocDbName,
        ddoc_id = DDocId, view_name = ViewName
    } = ViewSpec,
    #view_query_args{
        stale = Stale
    } = ViewArgs,
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, Db} ->
        try
            FoldlFun = make_reduce_fold_fun(ViewArgs, Queue),
            KeyGroupFun = make_group_rows_fun(ViewArgs),
            {DDocDb, View} = get_reduce_view(Db, DDocDbName,
                                             DDocId, ViewName, Stale),

            case not(should_check_rev(MergeParams, DDoc)) orelse
                ddoc_unchanged(DDocDb, DDoc) of
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
                            {ok, _} = couch_view:fold_reduce(View, FoldlFun,
                                                             [], FoldOpts)
                        end,
                        Keys)
                end;
            false ->
                ok = couch_view_merger_queue:queue(Queue, revision_mismatch)
            end,
            catch couch_db:close(DDocDb)
        catch
        {not_found, Reason} when Reason =:= missing; Reason =:= deleted ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DbName, DDocId)});
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            couch_view_merger_queue:queue(Queue, reduce_error(Error))
        after
            ok = couch_view_merger_queue:done(Queue),
            couch_db:close(Db)
        end;
    {not_found, _} ->
        ok = couch_view_merger_queue:queue(
            Queue, {error, ?LOCAL, db_not_found_msg(DbName)}),
        ok = couch_view_merger_queue:done(Queue)
    end.

reduce_set_view_folder(ViewSpec, MergeParams, Req, Keys, ViewArgs, DDoc, Queue) ->
    #set_view_spec{
        name = SetName, ddoc_id = DDocId
    } = ViewSpec,
    #view_query_args{
        group_level = GroupLevel
    } = ViewArgs,

    DefaultViewArgs = #view_query_args{},
    Limit = DefaultViewArgs#view_query_args.limit,
    Skip = DefaultViewArgs#view_query_args.skip,
    DDocDbName = ?master_dbname(SetName),

    case prepare_set_view(ViewSpec, ViewArgs, Queue,
                          fun couch_set_view:get_reduce_view/5) of
    error ->
        ok;
    {View, Group} ->
        try
            StartRespFun =
                fun (_Req, _Etag, Acc) ->
                    {ok, nil, Acc}
                end,
            SendRowFun =
                fun (_Resp, Row, Acc) ->
                    ok = couch_view_merger_queue:queue(Queue, Row),
                    {ok, Acc}
                end,

            HelperFuns =
                #reduce_fold_helper_funs{
                    start_response = StartRespFun,
                    send_row = SendRowFun
                },

            {ok, KeyGroupFun, FoldFun} =
                couch_httpd_set_view:make_reduce_fold_funs(
                    Req, GroupLevel, ViewArgs, nil, HelperFuns),
            FoldAccInit = {Limit, Skip, undefined, []},

            case not(should_check_rev(MergeParams, DDoc)) orelse
                ddoc_unchanged(DDocDbName, DDoc) of
            true ->
                case Keys of
                nil ->
                    FoldOpts = [{key_group_fun, KeyGroupFun} |
                        couch_httpd_set_view:make_key_options(ViewArgs)],
                    {ok, _} = couch_set_view:fold_reduce(
                                  Group, View, FoldFun, FoldAccInit, FoldOpts);
                _ when is_list(Keys) ->
                    lists:foreach(
                        fun(K) ->
                            FoldOpts = [{key_group_fun, KeyGroupFun} |
                                couch_httpd_set_view:make_key_options(
                                    ViewArgs#view_query_args{
                                        start_key = K, end_key = K})],
                            {ok, _} =
                                couch_set_view:fold_reduce(
                                    Group, View, FoldFun, FoldAccInit, FoldOpts)
                        end,
                        Keys)
                end;
            false ->
                ok = couch_view_merger_queue:queue(Queue, revision_mismatch)
            end
        catch
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            couch_view_merger_queue:queue(Queue, {error, ?LOCAL, to_binary(Error)})
        after
            couch_set_view:release_group(Group),
            ok = couch_view_merger_queue:done(Queue)
        end
    end.

get_reduce_view(Db, DDocDbName, DDocId, ViewName, Stale) ->
    GroupId = case DDocDbName of
    nil ->
        DDocDb = nil,
        DDocId;
    _ when is_binary(DDocDbName) ->
        DDocDb = case couch_db:open_int(DDocDbName, []) of
        {ok, DDocDb1} ->
            DDocDb1;
        {not_found, _} ->
            throw(ddoc_db_not_found)
        end,
        {DDocDb, DDocId}
    end,
    {ok, View, _} = couch_view:get_reduce_view(Db, GroupId, ViewName, Stale),
    {DDocDb, View}.


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
    GroupId = case DDocDbName of
    nil ->
        DDocDb = nil,
        DDocId;
    _ when is_binary(DDocDbName) ->
        DDocDb = case couch_db:open_int(DDocDbName, []) of
        {ok, DDocDb1} ->
            DDocDb1;
        {not_found, _} ->
            throw(ddoc_db_not_found)
        end,
        {DDocDb, DDocId}
    end,
    View = case couch_view:get_map_view(Db, GroupId, ViewName, Stale) of
    {ok, MapView, _} ->
        MapView;
    {not_found, _} ->
        {ok, RedView, _} = couch_view:get_reduce_view(Db, GroupId, ViewName, Stale),
        couch_view:extract_map_view(RedView)
    end,
    {DDocDb, View}.

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
        Rev = case get_value(<<"_rev">>, Props, nil) of
        nil ->
            nil;
        Rev0 ->
            couch_doc:parse_rev(Rev0)
        end,
        IncludeId = get_value(<<"_id">>, Props, DocId),
        [Doc] = couch_httpd_view:doc_member(Db, {IncludeId, Rev}, DocOpenOpts),
        ok = couch_view_merger_queue:queue(Queue, {Kd, Value, Doc}),
        {ok, Acc};
    ({{_Key, DocId} = Kd, Value}, _, Acc) ->
        [Doc] = couch_httpd_view:doc_member(Db, {DocId, nil}, DocOpenOpts),
        ok = couch_view_merger_queue:queue(Queue, {Kd, Value, Doc}),
        {ok, Acc}
    end.


get_first_ddoc([], _MergeParams, _UserCtx) ->
    throw({error, <<"A view spec can not consist of merges exclusively.">>});

get_first_ddoc([#simple_view_spec{view_name = <<"_all_docs">>} = ViewSpec | _],
               _MergeParams, _UserCtx) ->
    {ok, nil, ViewSpec};

get_first_ddoc([#set_view_spec{} = Spec | _], MergeParams, UserCtx) ->
    #set_view_spec {
        name = SetName, ddoc_id = Id
    } = Spec,

    {ok, Db} = open_db(?master_dbname(SetName), UserCtx, MergeParams),
    {ok, DDoc} = get_ddoc(Db, Id),
    close_db(Db),

    {ok, DDoc, Spec};

get_first_ddoc([#simple_view_spec{} = Spec | _], MergeParams, UserCtx) ->
    #simple_view_spec{
        database = DbName, ddoc_database = DDocDbName, ddoc_id = Id
    } = Spec,
    {ok, Db} = case DDocDbName of
    nil ->
        open_db(DbName, UserCtx, MergeParams);
    _ when is_binary(DDocDbName) ->
        open_db(DDocDbName, UserCtx, MergeParams)
    end,
    {ok, DDoc} = get_ddoc(Db, Id),
    close_db(Db),

    {ok, DDoc, Spec};

get_first_ddoc([_MergeSpec | Rest], MergeParams, UserCtx) ->
    get_first_ddoc(Rest, MergeParams, UserCtx).


open_db(<<"http://", _/binary>> = DbName, _UserCtx, MergeParams) ->
    HttpDb = #httpdb{
        url = maybe_add_trailing_slash(DbName),
        timeout = MergeParams#view_merge.conn_timeout
    },
    {ok, HttpDb#httpdb{ibrowse_options = ibrowse_options(HttpDb)}};
open_db(<<"https://", _/binary>> = DbName, _UserCtx, MergeParams) ->
    HttpDb = #httpdb{
        url = maybe_add_trailing_slash(DbName),
        timeout = MergeParams#view_merge.conn_timeout
    },
    {ok, HttpDb#httpdb{ibrowse_options = ibrowse_options(HttpDb)}};
open_db(DbName, UserCtx, _MergeParams) ->
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


get_ddoc(#httpdb{url = BaseUrl, headers = Headers} = HttpDb, Id) ->
    Url = BaseUrl ++ ?b2l(Id) ++ "?revs=true",
    case ibrowse:send_req(
        Url, Headers, get, [], HttpDb#httpdb.ibrowse_options) of
    {ok, "200", _RespHeaders, Body} ->
        {ok, couch_doc:from_json_obj(?JSON_DECODE(Body))};
    {ok, _Code, _RespHeaders, Body} ->
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
            "`~s`: ~s", [Id, db_uri(HttpDb), Error]),
        throw({error, iolist_to_binary(Msg)})
    end;
get_ddoc(Db, Id) ->
    case couch_db:open_doc(Db, Id, [ejson_body]) of
    {ok, _} = Ok ->
        Ok;
    {not_found, _} ->
        throw({not_found, ddoc_not_found_msg(Db, Id)})
    end.


db_uri(#httpdb{url = Url}) ->
    db_uri(Url);
db_uri(#db{name = Name}) ->
    Name;
db_uri(Url) when is_binary(Url) ->
    ?l2b(couch_util:url_strip_password(Url)).


db_not_found_msg(DbName) ->
    iolist_to_binary(io_lib:format("Database `~s` doesn't exist.", [db_uri(DbName)])).

ddoc_not_found_msg(DbName, DDocId) ->
    Msg = io_lib:format(
        "Design document `~s` missing in database `~s`.", [DDocId, db_uri(DbName)]),
    iolist_to_binary(Msg).

view_undefined_msg(SetName, DDocId) ->
    Msg = io_lib:format(
        "Undefined set view `~s` for `~s` design document.",
            [SetName, DDocId]),
    iolist_to_binary(Msg).

ibrowse_error_msg(Reason) when is_atom(Reason) ->
    to_binary(Reason);
ibrowse_error_msg(Reason) when is_tuple(Reason) ->
    to_binary(element(1, Reason)).

ibrowse_options(#httpdb{timeout = T, url = Url}) ->
    [{inactivity_timeout, T}, {connect_timeout, infinity},
        {response_format, binary}, {socket_options, [{keepalive, true}]}] ++
    case Url of
    "https://" ++ _ ->
        % TODO: add SSL options like verify and cacertfile
        [{is_ssl, true}];
    _ ->
        []
    end.


view_qs(ViewArgs) ->
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
        stale = Stale
    } = ViewArgs,

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
        ["startkey_docid=" ++ ?b2l(StartDocId)];
    _ ->
        []
    end ++
    case {Dir, EndDocId =:= DefViewArgs#view_query_args.end_docid} of
    {fwd, false} ->
        ["endkey_docid=" ++ ?b2l(EndDocId)];
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
            ["startkey_docid=" ++ json_qs_val(StartDocId1)]
        end ++
        case EndDocId1 =:= DefViewArgs#view_query_args.end_docid of
        true ->
            [];
        false ->
            ["endkey_docid=" ++ json_qs_val(EndDocId1)]
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
    case Stale =:= DefViewArgs#view_query_args.stale of
    true ->
        [];
    false ->
        ["stale=" ++ atom_to_list(Stale)]
    end,
    case QsList of
    [] ->
        [];
    _ ->
        "?" ++ string:join(QsList, "&")
    end.

json_qs_val(Value) ->
    couch_httpd:quote(?b2l(iolist_to_binary(?JSON_ENCODE(Value)))).

reverse_key_default(?MIN_STR) -> ?MAX_STR;
reverse_key_default(?MAX_STR) -> ?MIN_STR;
reverse_key_default(Key) -> Key.


stop_conn(Conn) ->
    unlink(Conn),
    receive {'EXIT', Conn, _} -> ok after 0 -> ok end,
    catch ibrowse:stop_worker_process(Conn).

ddoc_rev(nil) ->
    nil;
ddoc_rev(#doc{revs = {Pos, [RevId | _]}}) ->
    {Pos, RevId}.

ddoc_rev_str(DDoc) ->
    couch_doc:rev_to_str(ddoc_rev(DDoc)).

should_check_rev(#view_merge{ddoc_revision = DDocRevision}, DDoc) ->
    DDocRevision =/= nil andalso DDoc =/= nil.

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

view_name(#simple_view_spec{view_name = ViewName}) ->
    ViewName;
view_name(#set_view_spec{view_name = ViewName}) ->
    ViewName.
