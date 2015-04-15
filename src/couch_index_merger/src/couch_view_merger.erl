% -*- Mode: Erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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
-export([parse_http_params/4, make_funs/3, get_skip_and_limit/1]).
-export([make_event_fun/2, view_qs/2, process_extra_params/2]).
-export([map_view_merge_callback/2, reduce_view_merge_callback/2]).
-export([simple_set_view_query/3]).

% exports for spatial_merger
-export([queue_debug_info/4, debug_info/3, get_set_view/5,
         prepare_set_view/5]).

-include("couch_db.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").
-include_lib("couch_index_merger/include/couch_view_merger.hrl").
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
    ViewType0 = view_type(DDoc, ViewName),
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
       http_params = ViewArgs,
       make_row_fun = MakeRowFun0
    } = IndexMergeParams,
    #view_merge{
       rereduce_fun = InRedFun
    } = Extra,
    #view_query_args{
        debug = DebugMode,
        view_type = ViewType0,
        direction = Dir
    } = ViewArgs,
    ViewType = case ViewType0 of
    nil ->
        view_type(DDoc, ViewName);
    _ when ViewType0 == map; ViewType0 == red_map; ViewType0 == reduce ->
        ViewType0
    end,
    RedFun = case {ViewType, InRedFun} of
    {reduce, nil} ->
        reduce_function(DDoc, ViewName);
    {reduce, _} when is_binary(InRedFun) ->
        InRedFun;
    _ ->
        nil
    end,
    LessFun = view_less_fun(Dir, ViewType),
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
        rereduce_fun = RedFun
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
process_extra_params(#view_merge{keys = nil}, EJson) ->
    EJson;
process_extra_params(#view_merge{keys = Keys}, EJson) ->
    [{<<"keys">>, Keys} | EJson].

% callback!
map_view_merge_callback(start, Acc) ->
    {ok, Acc};
map_view_merge_callback({start, _}, Acc) ->
    {ok, Acc};
map_view_merge_callback(stop, Acc) ->
    {ok, Acc};
map_view_merge_callback({row, Row}, Macc) ->
    #merge_acc{
        fold_fun = Fun,
        acc = Acc
    } = Macc,
    case Fun(Row, nil, Acc) of
    {ok, Acc2} ->
        {ok, Macc#merge_acc{acc = Acc2}};
    {stop, Acc2} ->
        {stop, Macc#merge_acc{acc = Acc2}}
    end;
map_view_merge_callback({debug_info, _From, _Info}, Acc) ->
    {ok, Acc}.


reduce_view_merge_callback(start, Acc) ->
    {ok, Acc};
reduce_view_merge_callback({start, _}, Acc) ->
    {ok, Acc};
reduce_view_merge_callback(stop, Acc) ->
    {ok, Acc};
reduce_view_merge_callback({row, {Key, Red}}, Macc) ->
    #merge_acc{
        fold_fun = Fun,
        acc = Acc
    } = Macc,
    case Fun(Key, Red, Acc) of
    {ok, Acc2} ->
        {ok, Macc#merge_acc{acc = Acc2}};
    {stop, Acc2} ->
        {stop, Macc#merge_acc{acc = Acc2}}
    end;
reduce_view_merge_callback({debug_info, _From, _Info}, Acc) ->
    {ok, Acc}.


view_type(DDoc, ViewName) ->
    {ViewDef} = get_view_def(DDoc, ViewName),
    case get_value(<<"reduce">>, ViewDef) of
    undefined ->
        map;
    RedFun when is_binary(RedFun) ->
        reduce
    end.


reduce_function(#doc{id = DDocId} = DDoc, ViewName) ->
    {ViewDef} = get_view_def(DDoc, ViewName),
    case get_value(<<"reduce">>, ViewDef) of
    FunString when is_binary(FunString) ->
        FunString;
    _ ->
        NotFoundMsg = io_lib:format("Reduce field for view `~s`, local "
            "design document `~s`, is missing or is not a string.",
            [ViewName, DDocId]),
        throw({error, iolist_to_binary(NotFoundMsg)})
    end.


get_view_def(#doc{body = DDoc, id = DDocId}, ViewName) ->
    try
        get_nested_json_value(DDoc, [<<"views">>, ViewName])
    catch throw:{not_found, _} ->
        NotFoundMsg = io_lib:format("View `~s` not defined in local "
            "design document `~s`.", [ViewName, DDocId]),
        throw({not_found, iolist_to_binary(NotFoundMsg)})
    end.


view_less_fun(Dir, ViewType) ->
    LessFun = case ViewType of
    reduce ->
        fun couch_set_view:reduce_view_key_compare/2;
    _ ->
        fun couch_set_view:map_view_key_compare/2
    end,
    case Dir of
    fwd ->
        fun(RowA, RowB) -> LessFun(element(1, RowA), element(1, RowB)) end;
    rev ->
        fun(RowA, RowB) -> not LessFun(element(1, RowA), element(1, RowB)) end
    end.

% Optimized path, row assembled by couch_http_view_streamer
view_row_obj_map({_KeyDocId, {row_json, RowJson}}, _Debug) ->
    RowJson;

% Row from local node, query with ?debug=true
view_row_obj_map({{Key, DocId}, {PartId, Value}}, true) when is_integer(PartId) ->
    {json, RawValue} = Value,
    <<"{\"id\":", (?JSON_ENCODE(DocId))/binary,
      ",\"key\":", (?JSON_ENCODE(Key))/binary,
      ",\"partition\":", (?l2b(integer_to_list(PartId)))/binary,
      ",\"node\":\"", (?LOCAL)/binary, "\"",
      ",\"value\":", RawValue/binary, "}">>;

% Row from remote node, using Erlang based stream JSON parser, query with ?debug=true
view_row_obj_map({{Key, DocId}, {PartId, Node, Value}}, true) when is_integer(PartId) ->
    {json, RawValue} = Value,
    <<"{\"id\":", (?JSON_ENCODE(DocId))/binary,
      ",\"key\":", (?JSON_ENCODE(Key))/binary,
      ",\"partition\":", (?l2b(integer_to_list(PartId)))/binary,
      ",\"node\":", (?JSON_ENCODE(Node))/binary,
      ",\"value\":", RawValue/binary, "}">>;

% Row from local node, query with ?debug=false
view_row_obj_map({{Key, DocId}, {PartId, Value}}, false) when is_integer(PartId) ->
    {json, RawValue} = Value,
    <<"{\"id\":", (?JSON_ENCODE(DocId))/binary,
      ",\"key\":", (?JSON_ENCODE(Key))/binary,
      ",\"value\":", RawValue/binary, "}">>;

% Row from local node, old couchdb views
view_row_obj_map({{Key, DocId}, Value}, _DebugMode) ->
    <<"{\"id\":", (?JSON_ENCODE(DocId))/binary,
      ",\"key\":", (?JSON_ENCODE(Key))/binary,
      ",\"value\":", (?JSON_ENCODE(Value))/binary, "}">>;

% Row from local node, old couchdb views (no partition id)
view_row_obj_map({{Key, DocId}, Value, Doc}, _DebugMode) ->
    <<"{\"id\":", (?JSON_ENCODE(DocId))/binary,
      ",\"key\":", (?JSON_ENCODE(Key))/binary,
      ",\"value\":", (?JSON_ENCODE(Value))/binary,
      ",\"doc\":", (?JSON_ENCODE(Doc))/binary, "}">>.

% Optimized path, reduce row assembled by couch_http_view_streamer
view_row_obj_reduce({_Key, {row_json, RowJson}, _ValueJson}, _DebugMode) ->
    RowJson;
% Reduce row from local node
view_row_obj_reduce({Key, Value}, _DebugMode) ->
    <<"{\"key\":", (?JSON_ENCODE(Key))/binary,
      ",\"value\":", (?JSON_ENCODE(Value))/binary, "}">>.


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
    ok = couch_view_merger_queue:flush(Params#merge_params.queue),
    couch_index_merger:handle_skip(Params#merge_params{row_acc=[MinRow]}).


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
        [FirstRow, _ | _] ->
            try
                RedVal = rereduce(RowGroup, Params),
                {{row, {element(1, FirstRow), {json, RedVal}}}, Col}
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
                    Col3 = Col2
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

reduce_error({error, Reason}) ->
    {error, ?LOCAL, to_binary(Reason)};
reduce_error(Error) ->
    {error, ?LOCAL, to_binary(Error)}.


group_keys_for_rereduce(Queue, [Row | _] = Acc) ->
    K = element(1, Row),
    case couch_view_merger_queue:peek(Queue) of
    empty ->
        Acc;
    {ok, Row2} when element(1, Row2) == K ->
        {ok, Row2} = couch_view_merger_queue:pop_next(Queue),
        group_keys_for_rereduce(Queue, [Row2 | Acc]);
    {ok, revision_mismatch} ->
        revision_mismatch;
    {ok, _} ->
        Acc
    end.


rereduce(Reds0, #merge_params{extra = #view_merge{rereduce_fun = <<"_", _/binary>> = FunSrc}}) ->
    Reds = lists:map(
        fun({Key, _RowJson, {value_json, ValueJson}}) ->
            {Key, ValueJson};
        ({Key, {json, ValueJson}}) ->
            {Key, ValueJson};
        ({Key, Ejson}) ->
            {Key, ?JSON_ENCODE(Ejson)}
        end, Reds0),
    {ok, [Value]} = couch_set_view_mapreduce:builtin_reduce(rereduce, [FunSrc], Reds),
    Value;

rereduce(Rows, #merge_params{extra = #view_merge{rereduce_fun = FunSrc}}) ->
    Reds = lists:map(
        fun({_Key, _RowJson, {value_json, ValueJson}}) ->
            ValueJson;
        ({_Key, {json, ValueJson}}) ->
            ValueJson;
        ({_Key, Val}) ->
            ?JSON_ENCODE(Val)
        end, Rows),
    case get(reduce_context) of
    undefined ->
        {ok, Ctx} = mapreduce:start_reduce_context([FunSrc]),
        erlang:put(reduce_context, Ctx);
    Ctx ->
        ok
    end,
    case mapreduce:rereduce(Ctx, 1, Reds) of
    {ok, Value} ->
        Value;
    Error ->
        throw(Error)
    end.

get_set_view(GetSetViewFn, SetName, DDoc, ViewName,
        #set_view_group_req{stale = false} = ViewGroupReq) ->
    ViewGroupReq1 = ViewGroupReq#set_view_group_req{update_stats = false},
    case GetSetViewFn(SetName, DDoc, ViewName, ViewGroupReq1) of
    {ok, _View, Group, []} = Reply ->
        couch_set_view:inc_group_access_stat(Group),
        Reply;
    Other ->
        Other
    end;

get_set_view(GetSetViewFn, SetName, DDoc, ViewName, ViewGroupReq) ->
    GetSetViewFn(SetName, DDoc, ViewName, ViewGroupReq).

prepare_set_view(ViewSpec, ViewGroupReq, DDoc, Queue, GetSetViewFn) ->
    #set_view_spec{
        name = SetName,
        ddoc_id = DDocId,
        view_name = ViewName
    } = ViewSpec,
    try
        case get_set_view(GetSetViewFn, SetName, DDoc, ViewName, ViewGroupReq) of
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


queue_get_view_group_error({error, {error, Reason}}, _SetName, _DDocId) ->
    {error, ?LOCAL, Reason};
queue_get_view_group_error({error, Reason}, _SetName, _DDocId) ->
    {error, ?LOCAL, Reason};
queue_get_view_group_error(view_undefined, SetName, DDocId) ->
    {error, ?LOCAL, view_undefined_msg(SetName, DDocId)};
queue_get_view_group_error(Error, _SetName, _DDocId) ->
    {error, ?LOCAL, Error}.


map_view_folder(_Db, #set_view_spec{} = ViewSpec, MergeParams, _UserCtx, DDoc, Queue) ->
    map_set_view_folder(ViewSpec, MergeParams, DDoc, Queue).


map_set_view_folder(ViewSpec, MergeParams, DDoc, Queue) ->
    #set_view_spec{
        name = SetName,
        ddoc_id = DDocId,
        partitions = WantedPartitions0
    } = ViewSpec,
    #index_merge{
        http_params = ViewArgs
    } = MergeParams,
    #view_query_args{
        stale = Stale,
        debug = Debug,
        type = IndexType
    } = ViewArgs,
    DDocDbName = ?master_dbname(SetName),

    PrepareResult = case (ViewSpec#set_view_spec.view =/= nil) andalso
        (ViewSpec#set_view_spec.group =/= nil) of
    true ->
        ViewGroupReq2 = nil,
        {ViewSpec#set_view_spec.view, ViewSpec#set_view_spec.group};
    false ->
        WantedPartitions = case IndexType of
        main ->
            WantedPartitions0;
        replica ->
            []
        end,
        ViewGroupReq1 = #set_view_group_req{
            stale = Stale,
            update_stats = true,
            wanted_partitions = WantedPartitions,
            debug = Debug,
            type = IndexType
        },
        case prepare_set_view(
            ViewSpec, ViewGroupReq1, DDoc, Queue, fun couch_set_view:get_map_view/4) of
        not_found ->
            ViewGroupReq2 = ViewGroupReq1#set_view_group_req{
                update_stats = false
            },
            case prepare_set_view(
                ViewSpec, ViewGroupReq2, DDoc, Queue, fun couch_set_view:get_reduce_view/4) of
            {RedView, Group0} ->
                {couch_set_view:extract_map_view(RedView), Group0};
            Else ->
                Else
            end;
        Else ->
            ViewGroupReq2 = ViewGroupReq1,
            Else
        end
    end,

    case PrepareResult of
    error ->
        %%  handled by prepare_set_view
        ok;
    {View, Group} ->
        queue_debug_info(Debug, Group, ViewGroupReq2, Queue),
        try
            FoldFun = make_map_set_fold_fun(Queue),

            case not(couch_index_merger:should_check_rev(MergeParams, DDoc)) orelse
                couch_index_merger:ddoc_unchanged(DDocDbName, DDoc) of
            true ->
                RowCount = couch_set_view:get_row_count(Group, View),
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
        throw:queue_shutdown ->
            % The merger process shutdown our queue, limit was reached and this is
            % expected, so don't long unnecessary error message and stack trace.
            ok;
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


http_view_fold(object_start, map, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end;
http_view_fold(object_start, red_map, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end;
http_view_fold(object_start, reduce, Queue) ->
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rc_1({key, <<"total_rows">>}, Queue) ->
    fun(Ev) -> http_view_fold_rc_2(Ev, Queue) end;
http_view_fold_rc_1({key, <<"debug_info">>}, Queue) ->
    fun(object_start) ->
        fun(Ev) ->
            http_view_fold_debug_info(Ev, Queue, [], fun http_view_fold_rc_1/2)
        end
    end;
http_view_fold_rc_1(_Ev, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end.

http_view_fold_rc_2(RowCount, Queue) when is_number(RowCount) ->
    ok = couch_view_merger_queue:queue(Queue, {row_count, RowCount}),
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rows_1({key, <<"rows">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_view_fold_rows_2(Ev, Queue) end end;
http_view_fold_rows_1({key, <<"debug_info">>}, Queue) ->
    fun(object_start) ->
        fun(Ev) ->
            http_view_fold_debug_info(Ev, Queue, [], fun http_view_fold_rows_1/2)
        end
    end;
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

http_view_fold_debug_info({key, Key}, Queue, Acc, RetFun) ->
    fun(object_start) ->
        fun(Ev) ->
            json_stream_parse:collect_object(
                Ev,
                fun(DebugInfo) ->
                    Acc2 = [{Key, DebugInfo} | Acc],
                    fun(Ev2) -> http_view_fold_debug_info(Ev2, Queue, Acc2, RetFun) end
                end)
        end
    end;
http_view_fold_debug_info(object_end, Queue, Acc, RetFun) ->
    case Acc of
    [{?LOCAL, Info}] ->
        ok;
    _ ->
        Info = {lists:reverse(Acc)}
    end,
    ok = couch_view_merger_queue:queue(Queue, {debug_info, get(from_url), Info}),
    fun(Ev2) -> RetFun(Ev2, Queue) end.


http_view_fold_queue_error({Props}, Queue) ->
    Reason = get_value(<<"reason">>, Props, null),
    ok = couch_view_merger_queue:queue(Queue, {error, get(from_url), Reason}).


http_view_fold_queue_row({Props}, Queue) ->
    Key = {json, ?JSON_ENCODE(get_value(<<"key">>, Props, null))},
    Id = get_value(<<"id">>, Props, nil),
    Val = {json, ?JSON_ENCODE(get_value(<<"value">>, Props))},
    Value = case get_value(<<"partition">>, Props, nil) of
    nil ->
        Val;
    PartId ->
        % we're in debug mode, add node info
        {PartId, get(from_url), Val}
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
                {{Key, Id}, Value, Doc}
            end
        end;
    Error ->
        % error in a map row
        {{Key, error}, Error}
    end,
    ok = couch_view_merger_queue:queue(Queue, Row).

reduce_view_folder(_Db, #set_view_spec{} = ViewSpec, MergeParams,
                   _UserCtx, DDoc, Queue) ->
    reduce_set_view_folder(ViewSpec, MergeParams, DDoc, Queue).


reduce_set_view_folder(ViewSpec, MergeParams, DDoc, Queue) ->
    #set_view_spec{
        name = SetName,
        ddoc_id = DDocId,
        partitions = WantedPartitions0
    } = ViewSpec,
    #index_merge{
        http_params = ViewArgs
    } = MergeParams,
    #view_query_args{
        stale = Stale,
        debug = Debug,
        type = IndexType
    } = ViewArgs,

    DDocDbName = ?master_dbname(SetName),
    PrepareResult = case (ViewSpec#set_view_spec.view =/= nil) andalso
        (ViewSpec#set_view_spec.group =/= nil) of
    true ->
        ViewGroupReq = nil,
        {ViewSpec#set_view_spec.view, ViewSpec#set_view_spec.group};
    false ->
        WantedPartitions = case IndexType of
        main ->
            WantedPartitions0;
        replica ->
            []
        end,
        ViewGroupReq = #set_view_group_req{
            stale = Stale,
            update_stats = true,
            wanted_partitions = WantedPartitions,
            debug = Debug,
            type = IndexType
        },
        prepare_set_view(ViewSpec, ViewGroupReq, DDoc, Queue, fun couch_set_view:get_reduce_view/4)
    end,

    case PrepareResult of
    error ->
        %%  handled by prepare_set_view
        ok;
    {View, Group} ->
        queue_debug_info(Debug, Group, ViewGroupReq, Queue),
        try
            FoldFun = fun(GroupedKey, Red, Acc) ->
                ok = couch_view_merger_queue:queue(Queue, {GroupedKey, Red}),
                {ok, Acc}
            end,

            case not(couch_index_merger:should_check_rev(MergeParams, DDoc)) orelse
                couch_index_merger:ddoc_unchanged(DDocDbName, DDoc) of
            true ->
                {ok, _} = couch_set_view:fold_reduce(Group, View, FoldFun, [], ViewArgs);
            false ->
                ok = couch_view_merger_queue:queue(Queue, revision_mismatch)
            end
        catch
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL,
                    couch_index_merger:ddoc_not_found_msg(DDocDbName, DDocId)});
        throw:queue_shutdown ->
            % The merger process shutdown our queue, limit was reached and this is
            % expected, so don't long unnecessary error message and stack trace.
            ok;
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


make_map_set_fold_fun(Queue) ->
    fun(Kv, _, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, Kv),
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
        conflicts = Conflicts,
        stale = Stale,
        limit = Limit,
        debug = Debug,
        filter = Filter,
        type = IndexType,
        skip = Skip
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
    case Limit =:= DefViewArgs#view_query_args.limit of
    true ->
        [];
    false ->
        ["limit=" ++ integer_to_list(Limit + Skip)]
    end ++
    case Debug =:= DefViewArgs#view_query_args.debug of
    true ->
        [];
    false ->
        ["debug=" ++ atom_to_list(Debug)]
    end ++
    case Filter =:= DefViewArgs#view_query_args.filter of
    true ->
        [];
    false ->
        ["_filter=" ++ atom_to_list(Filter)]
    end ++
    case IndexType =:= DefViewArgs#view_query_args.type of
    true ->
        [];
    false ->
        ["_type=" ++ atom_to_list(IndexType)]
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


queue_debug_info(Debug, Group, GroupReq, Queue) ->
    case debug_info(Debug, Group, GroupReq) of
    nil ->
        ok;
    DebugInfo ->
        ok = couch_view_merger_queue:queue(Queue, DebugInfo)
    end.

debug_info(false, _Group, _GroupReq) ->
    nil;
debug_info(true, #set_view_group{} = Group, GroupReq) ->
    #set_view_debug_info{
        original_abitmask = OrigMainAbitmask,
        original_pbitmask = OrigMainPbitmask,
        stats = Stats,
        replica_partitions = ReplicaPartitions,
        wanted_seqs = WantedSeqs0
    } = Group#set_view_group.debug_info,
    OrigMainActive = couch_set_view_util:decode_bitmask(OrigMainAbitmask),
    ModMainActive = couch_set_view_util:decode_bitmask(?set_abitmask(Group)),
    OrigMainPassive = couch_set_view_util:decode_bitmask(OrigMainPbitmask),
    ModMainPassive = couch_set_view_util:decode_bitmask(?set_pbitmask(Group)),
    MainCleanup = couch_set_view_util:decode_bitmask(?set_cbitmask(Group)),
    % 0 padded so that a pretty print JSON can sanely sort the keys (partition IDs)
    IndexableSeqs = [{?l2b(io_lib:format("~4..0b", [P])), S} || {P, S} <- ?set_seqs(Group)],
    UnindexableSeqs = [{?l2b(io_lib:format("~4..0b", [P])), S} ||
        {P, S} <- ?set_unindexable_seqs(Group)],
    WantedSeqs = [{?l2b(io_lib:format("~4..0b", [P])), S} || {P, S} <- WantedSeqs0],
    MainInfo = [
        {<<"active_partitions">>, ordsets:from_list(ModMainActive)},
        {<<"original_active_partitions">>, ordsets:from_list(OrigMainActive)},
        {<<"passive_partitions">>, ordsets:from_list(ModMainPassive)},
        {<<"original_passive_partitions">>, ordsets:from_list(OrigMainPassive)},
        {<<"cleanup_partitions">>, ordsets:from_list(MainCleanup)},
        {<<"replica_partitions">>, ordsets:from_list(ReplicaPartitions)},
        {<<"replicas_on_transfer">>, ?set_replicas_on_transfer(Group)},
        {<<"indexable_seqs">>, {IndexableSeqs}},
        {<<"unindexeable_seqs">>, {UnindexableSeqs}},
        {<<"wanted_seqs">>, {WantedSeqs}},
        case GroupReq of
        nil ->
            {<<"wanted_partitions">>, null};
        #set_view_group_req{wanted_partitions = WantedPartitions} ->
            {<<"wanted_partitions">>, WantedPartitions}
        end,
        pending_transition_debug_info(Group),
        {<<"stats">>, set_view_group_stats_ejson(Stats)}
    ],
    RepInfo = replica_group_debug_info(Group),
    Info = case RepInfo of
    [] ->
        { [{<<"main_group">>, {MainInfo}}] };
    _ ->
        { [{<<"main_group">>, {MainInfo}}, {<<"replica_group">>, {RepInfo}}] }
    end,
    {debug_info, ?LOCAL, Info}.

replica_group_debug_info(#set_view_group{replica_group = nil}) ->
    [];
replica_group_debug_info(#set_view_group{replica_group = RepGroup}) ->
    #set_view_group{
        debug_info = #set_view_debug_info{
            original_abitmask = OrigRepAbitmask,
            original_pbitmask = OrigRepPbitmask,
            stats = Stats,
            wanted_seqs = WantedSeqs0
        }
    } = RepGroup,
    OrigRepActive = couch_set_view_util:decode_bitmask(OrigRepAbitmask),
    ModRepActive = couch_set_view_util:decode_bitmask(?set_abitmask(RepGroup)),
    OrigRepPassive = couch_set_view_util:decode_bitmask(OrigRepPbitmask),
    ModRepPassive = couch_set_view_util:decode_bitmask(?set_pbitmask(RepGroup)),
    RepCleanup = couch_set_view_util:decode_bitmask(?set_cbitmask(RepGroup)),
    % 0 padded so that a pretty print JSON can sanely sort the keys (partition IDs)
    IndexableSeqs = [{?l2b(io_lib:format("~4..0b", [P])), S} || {P, S} <- ?set_seqs(RepGroup)],
    UnindexableSeqs = [{?l2b(io_lib:format("~4..0b", [P])), S} ||
        {P, S} <- ?set_unindexable_seqs(RepGroup)],
    WantedSeqs = [{?l2b(io_lib:format("~4..0b", [P])), S} || {P, S} <- WantedSeqs0],
    [
        {<<"replica_active_partitions">>, ordsets:from_list(ModRepActive)},
        {<<"replica_original_active_partitions">>, ordsets:from_list(OrigRepActive)},
        {<<"replica_passive_partitions">>, ordsets:from_list(ModRepPassive)},
        {<<"replica_original_passive_partitions">>, ordsets:from_list(OrigRepPassive)},
        {<<"replica_cleanup_partitions">>, ordsets:from_list(RepCleanup)},
        {<<"replica_indexable_seqs">>, {IndexableSeqs}},
        {<<"replica_unindexable_seqs">>, {UnindexableSeqs}},
        {<<"replica_wanted_seqs">>, {WantedSeqs}},
        pending_transition_debug_info(RepGroup),
        {<<"replica_stats">>, set_view_group_stats_ejson(Stats)}
    ].


pending_transition_debug_info(#set_view_group{index_header = Header}) ->
    Pt = Header#set_view_index_header.pending_transition,
    case Pt of
    nil ->
        {<<"pending_transition">>, null};
    #set_view_transition{} ->
        {<<"pending_transition">>,
            {[
                {<<"active">>, Pt#set_view_transition.active},
                {<<"passive">>, Pt#set_view_transition.passive},
                {<<"unindexable">>, Pt#set_view_transition.unindexable}
            ]}
        }
    end.


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


% Query with a single view to merge, trigger a simpler code path
% (no queue, no child processes, etc).
simple_set_view_query(Params, DDoc, Req) ->
    #index_merge{
        callback = Callback,
        user_acc = UserAcc,
        indexes = [SetViewSpec],
        extra = #view_merge{keys = Keys}
    } = Params,
    #set_view_spec{
        name = SetName,
        partitions = Partitions0,
        ddoc_id = DDocId,
        view_name = ViewName,
        category = Category
    } = SetViewSpec,

    Stale = list_to_existing_atom(string:to_lower(
        couch_httpd:qs_value(Req, "stale", "update_after"))),
    Debug = couch_set_view_http:parse_bool_param(
        couch_httpd:qs_value(Req, "debug", "false")),
    IndexType = list_to_existing_atom(
        couch_httpd:qs_value(Req, "_type", "main")),
    Partitions = case IndexType of
    main ->
        Partitions0;
    replica ->
        []
    end,
    GroupReq = #set_view_group_req{
        stale = Stale,
        update_stats = true,
        wanted_partitions = Partitions,
        debug = Debug,
        type = IndexType,
        category = Category
    },

    case get_set_view(
        fun couch_set_view:get_map_view/4, SetName, DDoc, ViewName, GroupReq) of
    {ok, View, Group, MissingPartitions} ->
        ViewType = map;
    {not_found, _} ->
        GroupReq2 = GroupReq#set_view_group_req{
            update_stats = false
        },
        case get_set_view(
            fun couch_set_view:get_reduce_view/4, SetName, DDoc, ViewName, GroupReq2) of
        {ok, ReduceView, Group, MissingPartitions} ->
            Reduce = list_to_existing_atom(
                string:to_lower(couch_httpd:qs_value(Req, "reduce", "true"))),
            case Reduce of
            false ->
                ViewType = red_map,
                View = couch_set_view:extract_map_view(ReduceView);
            true ->
                ViewType = reduce,
                View = ReduceView
            end;
        Error ->
            MissingPartitions = Group = View = ViewType = nil,
            ErrorMsg = io_lib:format("Error opening view `~s`, from set `~s`, "
                "design document `~s`: ~p", [ViewName, SetName, DDocId, Error]),
            throw({not_found, iolist_to_binary(ErrorMsg)})
        end
    end,

    case MissingPartitions of
    [] ->
        ok;
    _ ->
        couch_set_view:release_group(Group),
        ?LOG_INFO("Set view `~s`, group `~s`, missing partitions: ~w",
                  [SetName, DDocId, MissingPartitions]),
        throw({error, set_view_outdated})
    end,

    QueryArgs = couch_httpd_view:parse_view_params(Req, Keys, ViewType),
    QueryArgs2 = QueryArgs#view_query_args{
        view_name = ViewName,
        stale = Stale
     },

    case debug_info(Debug, Group, GroupReq) of
    nil ->
        Params2 = Params#index_merge{user_ctx = Req#httpd.user_ctx};
    DebugInfo ->
        {ok, UserAcc2} = Callback(DebugInfo, UserAcc),
        Params2 = Params#index_merge{
            user_ctx = Req#httpd.user_ctx,
            user_acc = UserAcc2
        }
    end,

    try
        case ViewType of
        reduce ->
            simple_set_view_reduce_query(Params2, Group, View, QueryArgs2);
        _ ->
            simple_set_view_map_query(Params2, Group, View, QueryArgs2)
        end
    after
        couch_set_view:release_group(Group)
    end.


simple_set_view_map_query(Params, Group, View, ViewArgs) ->
    #index_merge{
        callback = Callback,
        user_acc = UserAcc
    } = Params,
    #view_query_args{
        limit = Limit,
        skip = Skip,
        debug = DebugMode
    } = ViewArgs,

    FoldFun = fun(_Kv, _, {0, _, _} = Acc) ->
            {stop, Acc};
        (_Kv, _, {AccLim, AccSkip, UAcc}) when AccSkip > 0 ->
            {ok, {AccLim, AccSkip - 1, UAcc}};
        (Kv, _, {AccLim, 0, UAcc}) ->
            Row = view_row_obj_map(Kv, DebugMode),
            {ok, UAcc2} = Callback({row, Row}, UAcc),
            {ok, {AccLim - 1, 0, UAcc2}}
    end,

    RowCount = couch_set_view:get_row_count(Group, View),
    {ok, UserAcc2} = Callback({start, RowCount}, UserAcc),

    {ok, _, {_, _, UserAcc3}} = couch_set_view:fold(
        Group, View, FoldFun, {Limit, Skip, UserAcc2}, ViewArgs),
    Callback(stop, UserAcc3).


simple_set_view_reduce_query(Params, Group, View, ViewArgs) ->
    #index_merge{
        callback = Callback,
        user_acc = UserAcc
    } = Params,
    #view_query_args{
        limit = Limit,
        skip = Skip,
        debug = DebugMode
    } = ViewArgs,

    FoldFun = fun(_GroupedKey, _Red, {0, _, _} = Acc) ->
            {stop, Acc};
        (_GroupedKey, _Red, {AccLim, AccSkip, UAcc}) when AccSkip > 0 ->
            {ok, {AccLim, AccSkip - 1, UAcc}};
        (GroupedKey, Red, {AccLim, 0, UAcc}) ->
            Row = view_row_obj_reduce({GroupedKey, Red}, DebugMode),
            {ok, UAcc2} = Callback({row, Row}, UAcc),
            {ok, {AccLim - 1, 0, UAcc2}}
    end,

    {ok, UserAcc2} = Callback(start, UserAcc),
    {ok, {_, _, UserAcc3}} = couch_set_view:fold_reduce(
        Group, View, FoldFun, {Limit, Skip, UserAcc2}, ViewArgs),
    Callback(stop, UserAcc3).
