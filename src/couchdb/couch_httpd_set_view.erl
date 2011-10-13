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

-module(couch_httpd_set_view).

-export([handle_req/1]).

-export([parse_view_params/3]).
-export([make_view_fold_fun/6, finish_view_fold/4, finish_view_fold/5, view_row_obj/2]).
-export([view_etag/2, view_etag/3, make_reduce_fold_funs/5]).
-export([design_doc_view/6, parse_bool_param/1]).
-export([make_key_options/1, load_view/6]).

-import(couch_httpd,
    [send_json/2,send_json/3,send_json/4,send_method_not_allowed/2,send_chunk/2,
    start_json_response/2, start_json_response/3, end_json_response/1,
    send_chunked_error/2]).

-include("couch_db.hrl").
-include("couch_set_view.hrl").



handle_req(#httpd{path_parts = PathParts} = Req) ->
    [<<"_set_view">>, SetName, <<"_design">>, DesignName | Rest] = PathParts,
    route_request(Req, SetName, <<"_design/", DesignName/binary>>, Rest).

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_define">>]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {Fields} = couch_httpd:json_body_obj(Req),
    SetViewParams = #set_view_params{
        max_partitions = couch_util:get_value(<<"number_partitions">>, Fields, 0),
        active_partitions = couch_util:get_value(<<"active_partitions">>, Fields, []),
        passive_partitions = couch_util:get_value(<<"passive_partitions">>, Fields, [])
    },
    ok = couch_set_view:define_group(SetName, DDocId, SetViewParams),
    couch_httpd:send_json(Req, 201, {[{ok, true}]});

route_request(Req, _SetName, _DDocId, [<<"_define">>]) ->
    send_method_not_allowed(Req, "POST");

route_request(#httpd{method = 'GET'} = Req, SetName, DDocId, [<<"_info">>]) ->
    {ok, Info} = couch_set_view:get_group_info(SetName, DDocId),
    couch_httpd:send_json(Req, 200, {Info});

route_request(#httpd{method = 'GET'} = Req, SetName, DDocId, [<<"_view">>, ViewName]) ->
    Keys = couch_httpd:qs_json_value(Req, "keys", nil),
    FilteredPartitions = couch_httpd:qs_json_value(Req, "partitions", []),
    validate_json_partition_list(FilteredPartitions),
    design_doc_view(Req, SetName, DDocId, ViewName, FilteredPartitions, Keys);

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_view">>, ViewName]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {Fields} = couch_httpd:json_body_obj(Req),
    Keys = couch_util:get_value(<<"keys">>, Fields, nil),
    case Keys of
    nil ->
        ok;
    _ when is_list(Keys) ->
        ok;
    _ ->
        throw({bad_request, "`keys` member must be a array."})
    end,
    FilteredPartitions = couch_util:get_value(<<"partitions">>, Fields, []),
    validate_json_partition_list(FilteredPartitions),
    design_doc_view(Req, SetName, DDocId, ViewName, FilteredPartitions, Keys);

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_compact">>]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {ok, _Pid} = couch_set_view_compactor:start_compact(SetName, DDocId),
    couch_httpd:send_json(Req, 202, {[{ok, true}]});

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_passive_partitions">>]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    PartList = couch_httpd:json_body(Req),
    validate_json_partition_list(PartList),
    couch_set_view:set_passive_partitions(SetName, DDocId, PartList),
    couch_httpd:send_json(Req, 201, {[{ok, true}]});

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_active_partitions">>]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    PartList = couch_httpd:json_body(Req),
    validate_json_partition_list(PartList),
    couch_set_view:set_active_partitions(SetName, DDocId, PartList),
    couch_httpd:send_json(Req, 201, {[{ok, true}]});

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_cleanup_partitions">>]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    PartList = couch_httpd:json_body(Req),
    validate_json_partition_list(PartList),
    couch_set_view:set_cleanup_partitions(SetName, DDocId, PartList),
    couch_httpd:send_json(Req, 201, {[{ok, true}]}).


validate_json_partition_list(L) when is_list(L) ->
    lists:foreach(
        fun(P) when not is_number(P) ->
                throw({bad_request, "Expected a JSON array of partition IDs."});
            (_) ->
                ok
        end, L);
validate_json_partition_list(_) ->
    throw({bad_request, "Expected a JSON array of partition IDs."}).


design_doc_view(Req, SetName, DDocId, ViewName, FilteredPartitions, Keys) ->
    Stale = get_stale_type(Req),
    Reduce = get_reduce_type(Req),
    case couch_set_view:get_map_view(SetName, DDocId, ViewName, Stale, FilteredPartitions) of
    {ok, View, Group, _} ->
        QueryArgs = parse_view_params(Req, Keys, map),
        Result = output_map_view(Req, View, Group, QueryArgs, Keys),
        couch_set_view:release_group(Group);
    {not_found, Reason} ->
        case couch_set_view:get_reduce_view(SetName, DDocId, ViewName, Stale, FilteredPartitions) of
        {ok, ReduceView, Group, _} ->
            case Reduce of
            false ->
                QueryArgs = parse_view_params(Req, Keys, red_map),
                MapView = couch_set_view:extract_map_view(ReduceView),
                Result = output_map_view(Req, MapView, Group, QueryArgs, Keys),
                couch_set_view:release_group(Group);
            _ ->
                QueryArgs = parse_view_params(Req, Keys, reduce),
                Result = output_reduce_view(Req, ReduceView, Group, QueryArgs, Keys),
                couch_set_view:release_group(Group)
            end;
        _ ->
            Result = nil,
            throw({not_found, Reason})
        end
    end,
    couch_stats_collector:increment({httpd, view_reads}),
    Result.

output_map_view(Req, View, Group, QueryArgs, nil) ->
    #view_query_args{
        limit = Limit,
        skip = SkipCount
    } = QueryArgs,
    CurrentEtag = view_etag(Group, View),
    couch_httpd:etag_respond(Req, CurrentEtag, fun() ->
        {ok, RowCount} = couch_set_view:get_row_count(View),
        FoldlFun = make_view_fold_fun(Req, QueryArgs, CurrentEtag, Group, RowCount, #view_fold_helper_funs{reduce_count=fun couch_set_view:reduce_to_count/1}),
        FoldAccInit = {Limit, SkipCount, undefined, []},
        {ok, LastReduce, FoldResult} = couch_set_view:fold(Group, View,
                FoldlFun, FoldAccInit, make_key_options(QueryArgs)),
        finish_view_fold(Req, RowCount,
                couch_set_view:reduce_to_count(LastReduce), FoldResult)
    end);

output_map_view(Req, View, Group, QueryArgs, Keys) ->
    #view_query_args{
        limit = Limit,
        skip = SkipCount
    } = QueryArgs,
    CurrentEtag = view_etag(Group, View, Keys),
    couch_httpd:etag_respond(Req, CurrentEtag, fun() ->
        {ok, RowCount} = couch_set_view:get_row_count(View),
        FoldAccInit = {Limit, SkipCount, undefined, []},
        {LastReduce, FoldResult} = lists:foldl(fun(Key, {_, FoldAcc}) ->
            FoldlFun = make_view_fold_fun(Req, QueryArgs#view_query_args{},
                    CurrentEtag, Group, RowCount,
                    #view_fold_helper_funs{
                        reduce_count = fun couch_set_view:reduce_to_count/1
                    }),
            {ok, LastReduce, FoldResult} = couch_set_view:fold(Group, View, FoldlFun,
                    FoldAcc, make_key_options(
                         QueryArgs#view_query_args{start_key=Key, end_key=Key})),
            {LastReduce, FoldResult}
        end, {{[],[]}, FoldAccInit}, Keys),
        finish_view_fold(Req, RowCount, couch_set_view:reduce_to_count(LastReduce),
                FoldResult, [])
    end).

output_reduce_view(Req, View, Group, QueryArgs, nil) ->
    #view_query_args{
        limit = Limit,
        skip = Skip,
        group_level = GroupLevel
    } = QueryArgs,
    CurrentEtag = view_etag(Group, View),
    couch_httpd:etag_respond(Req, CurrentEtag, fun() ->
        {ok, GroupRowsFun, RespFun} = make_reduce_fold_funs(Req, GroupLevel,
                QueryArgs, CurrentEtag, #reduce_fold_helper_funs{}),
        FoldAccInit = {Limit, Skip, undefined, []},
        {ok, {_, _, Resp, _}} = couch_set_view:fold_reduce(
                Group, View,
                RespFun, FoldAccInit, [{key_group_fun, GroupRowsFun} |
                make_key_options(QueryArgs)]),
        finish_reduce_fold(Req, Resp)
    end);

output_reduce_view(Req, View, Group, QueryArgs, Keys) ->
    #view_query_args{
        limit = Limit,
        skip = Skip,
        group_level = GroupLevel
    } = QueryArgs,
    CurrentEtag = view_etag(Group, View, Keys),
    couch_httpd:etag_respond(Req, CurrentEtag, fun() ->
        {ok, GroupRowsFun, RespFun} = make_reduce_fold_funs(Req, GroupLevel,
                QueryArgs, CurrentEtag, #reduce_fold_helper_funs{}),
        {Resp, _RedAcc3} = lists:foldl(
            fun(Key, {Resp, RedAcc}) ->
                % run the reduce once for each key in keys, with limit etc
                % reapplied for each key
                FoldAccInit = {Limit, Skip, Resp, RedAcc},
                {_, {_, _, Resp2, RedAcc2}} = couch_set_view:fold_reduce(
                        Group, View,
                        RespFun, FoldAccInit, [{key_group_fun, GroupRowsFun} |
                        make_key_options(QueryArgs#view_query_args{
                            start_key=Key, end_key=Key})]),
                % Switch to comma
                {Resp2, RedAcc2}
            end,
        {undefined, []}, Keys), % Start with no comma
        finish_reduce_fold(Req, Resp, [])
    end).

reverse_key_default(?MIN_STR) -> ?MAX_STR;
reverse_key_default(?MAX_STR) -> ?MIN_STR;
reverse_key_default(Key) -> Key.

get_stale_type(Req) ->
    list_to_existing_atom(couch_httpd:qs_value(Req, "stale", "false")).

get_reduce_type(Req) ->
    list_to_existing_atom(couch_httpd:qs_value(Req, "reduce", "true")).

load_view(Req, SetName, DDocDbName, DDocId, ViewName, Keys) ->
    Stale = get_stale_type(Req),
    Reduce = get_reduce_type(Req),
    case couch_set_view:get_map_view(SetName, DDocDbName, DDocId, ViewName, Stale) of
    {ok, View, Group} ->
        QueryArgs = parse_view_params(Req, Keys, map),
        {map, View, Group, QueryArgs};
    {not_found, _Reason} ->
        case couch_set_view:get_reduce_view(SetName, DDocDbName, DDocId, ViewName, Stale) of
        {ok, ReduceView, Group} ->
            case Reduce of
            false ->
                QueryArgs = parse_view_params(Req, Keys, map_red),
                MapView = couch_set_view:extract_map_view(ReduceView),
                {map, MapView, Group, QueryArgs};
            _ ->
                QueryArgs = parse_view_params(Req, Keys, reduce),
                {reduce, ReduceView, Group, QueryArgs}
            end;
        {not_found, Reason} ->
            throw({not_found, Reason})
        end
    end.

% query_parse_error could be removed
% we wouldn't need to pass the view type, it'd just parse params.
% I'm not sure what to do about the error handling, but
% it might simplify things to have a parse_view_params function
% that doesn't throw().
parse_view_params(Req, Keys, ViewType) ->
    QueryList = couch_httpd:qs(Req),
    QueryParams =
    lists:foldl(fun({K, V}, Acc) ->
        parse_view_param(K, V) ++ Acc
    end, [], QueryList),
    IsMultiGet = (Keys =/= nil),
    Args = #view_query_args{
        view_type=ViewType,
        multi_get=IsMultiGet
    },
    QueryArgs = lists:foldl(fun({K, V}, Args2) ->
        validate_view_query(K, V, Args2)
    end, Args, lists:reverse(QueryParams)), % Reverse to match QS order.
    warn_on_empty_key_range(QueryArgs),
    GroupLevel = QueryArgs#view_query_args.group_level,
    case {ViewType, GroupLevel, IsMultiGet} of
    {reduce, exact, true} ->
        QueryArgs;
    {reduce, _, false} ->
        QueryArgs;
    {reduce, _, _} ->
        % we can simplify code if we just drop this error message.
        Msg = <<"Multi-key fetchs for reduce "
                "view must include `group=true`">>,
        throw({query_parse_error, Msg});
    _ ->
        QueryArgs
    end,
    QueryArgs.

parse_view_param("", _) ->
    [];
parse_view_param("key", Value) ->
    JsonKey = ?JSON_DECODE(Value),
    [{start_key, JsonKey}, {end_key, JsonKey}];
% TODO: maybe deprecate startkey_docid
parse_view_param("startkey_docid", Value) ->
    [{start_docid, ?l2b(Value)}];
parse_view_param("start_key_doc_id", Value) ->
    [{start_docid, ?l2b(Value)}];
% TODO: maybe deprecate endkey_docid
parse_view_param("endkey_docid", Value) ->
    [{end_docid, ?l2b(Value)}];
parse_view_param("end_key_doc_id", Value) ->
    [{end_docid, ?l2b(Value)}];
% TODO: maybe deprecate startkey
parse_view_param("startkey", Value) ->
    [{start_key, ?JSON_DECODE(Value)}];
parse_view_param("start_key", Value) ->
    [{start_key, ?JSON_DECODE(Value)}];
% TODO: maybe deprecate endkey
parse_view_param("endkey", Value) ->
    [{end_key, ?JSON_DECODE(Value)}];
parse_view_param("end_key", Value) ->
    [{end_key, ?JSON_DECODE(Value)}];
parse_view_param("limit", Value) ->
    [{limit, parse_positive_int_param(Value)}];
parse_view_param("count", _Value) ->
    throw({query_parse_error, <<"Query parameter 'count' is now 'limit'.">>});
parse_view_param("stale", "ok") ->
    [{stale, ok}];
parse_view_param("stale", "update_after") ->
    [{stale, update_after}];
parse_view_param("stale", _Value) ->
    throw({query_parse_error,
            <<"stale only available as stale=ok or as stale=update_after">>});
parse_view_param("update", _Value) ->
    throw({query_parse_error, <<"update=false is now stale=ok">>});
parse_view_param("descending", Value) ->
    [{descending, parse_bool_param(Value)}];
parse_view_param("skip", Value) ->
    [{skip, parse_int_param(Value)}];
parse_view_param("group", Value) ->
    case parse_bool_param(Value) of
        true -> [{group_level, exact}];
        false -> [{group_level, 0}]
    end;
parse_view_param("group_level", Value) ->
    [{group_level, parse_positive_int_param(Value)}];
parse_view_param("inclusive_end", Value) ->
    [{inclusive_end, parse_bool_param(Value)}];
parse_view_param("reduce", Value) ->
    [{reduce, parse_bool_param(Value)}];
parse_view_param("include_docs", Value) ->
    [{include_docs, parse_bool_param(Value)}];
parse_view_param("conflicts", Value) ->
    [{conflicts, parse_bool_param(Value)}];
parse_view_param("list", Value) ->
    [{list, ?l2b(Value)}];
parse_view_param("callback", _) ->
    []; % Verified in the JSON response functions
parse_view_param(Key, Value) ->
    [{extra, {Key, Value}}].

warn_on_empty_key_range(#view_query_args{start_key=undefined}) ->
    ok;
warn_on_empty_key_range(#view_query_args{end_key=undefined}) ->
    ok;
warn_on_empty_key_range(#view_query_args{start_key=A, end_key=A}) ->
    ok;
warn_on_empty_key_range(#view_query_args{
    start_key=StartKey, end_key=EndKey, direction=Dir}) ->
    case {Dir, couch_set_view:less_json(StartKey, EndKey)} of
        {fwd, false} ->
            throw({query_parse_error,
            <<"No rows can match your key range, reverse your ",
                "start_key and end_key or set descending=true">>});
        {rev, true} ->
            throw({query_parse_error,
            <<"No rows can match your key range, reverse your ",
                "start_key and end_key or set descending=false">>});
        _ -> ok
    end.

validate_view_query(start_key, Value, Args) ->
    case Args#view_query_args.multi_get of
    true ->
        Msg = <<"Query parameter `start_key` is "
                "not compatible with multi-get">>,
        throw({query_parse_error, Msg});
    _ ->
        Args#view_query_args{start_key=Value}
    end;
validate_view_query(start_docid, Value, Args) ->
    Args#view_query_args{start_docid=Value};
validate_view_query(end_key, Value, Args) ->
    case Args#view_query_args.multi_get of
    true->
        Msg = <<"Query parameter `end_key` is "
                "not compatible with multi-get">>,
        throw({query_parse_error, Msg});
    _ ->
        Args#view_query_args{end_key=Value}
    end;
validate_view_query(end_docid, Value, Args) ->
    Args#view_query_args{end_docid=Value};
validate_view_query(limit, Value, Args) ->
    Args#view_query_args{limit=Value};
validate_view_query(list, Value, Args) ->
    Args#view_query_args{list=Value};
validate_view_query(stale, ok, Args) ->
    Args#view_query_args{stale=ok};
validate_view_query(stale, update_after, Args) ->
    Args#view_query_args{stale=update_after};
validate_view_query(stale, _, Args) ->
    Args;
validate_view_query(descending, true, Args) ->
    case Args#view_query_args.direction of
    rev -> Args; % Already reversed
    fwd ->
        Args#view_query_args{
            direction = rev,
            start_docid =
                reverse_key_default(Args#view_query_args.start_docid),
            end_docid =
                reverse_key_default(Args#view_query_args.end_docid)
        }
    end;
validate_view_query(descending, false, Args) ->
    Args; % Ignore default condition
validate_view_query(skip, Value, Args) ->
    Args#view_query_args{skip=Value};
validate_view_query(group_level, Value, Args) ->
    case Args#view_query_args.view_type of
    reduce ->
        Args#view_query_args{group_level=Value};
    _ ->
        Msg = <<"Invalid URL parameter 'group' or "
                " 'group_level' for non-reduce view.">>,
        throw({query_parse_error, Msg})
    end;
validate_view_query(inclusive_end, Value, Args) ->
    Args#view_query_args{inclusive_end=Value};
validate_view_query(reduce, false, Args) ->
    Args;
validate_view_query(reduce, _, Args) ->
    case Args#view_query_args.view_type of
    map ->
        Msg = <<"Invalid URL parameter `reduce` for map view.">>,
        throw({query_parse_error, Msg});
    _ ->
        Args
    end;
validate_view_query(include_docs, true, Args) ->
    case Args#view_query_args.view_type of
    reduce ->
        Msg = <<"Query parameter `include_docs` "
                "is invalid for reduce views.">>,
        throw({query_parse_error, Msg});
    _ ->
        Args#view_query_args{include_docs=true}
    end;
% Use the view_query_args record's default value
validate_view_query(include_docs, _Value, Args) ->
    Args;
validate_view_query(conflicts, true, Args) ->
    case Args#view_query_args.view_type of
    reduce ->
        Msg = <<"Query parameter `conflicts` "
                "is invalid for reduce views.">>,
        throw({query_parse_error, Msg});
    _ ->
        Args#view_query_args{conflicts = true}
    end;
validate_view_query(extra, _Value, Args) ->
    Args.

make_view_fold_fun(Req, QueryArgs, Etag, Group, TotalViewCount, HelperFuns) ->
    #view_fold_helper_funs{
        start_response = StartRespFun,
        send_row = SendRowFun,
        reduce_count = ReduceCountFun
    } = apply_default_helper_funs(HelperFuns),

    #view_query_args{
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = QueryArgs,
    #set_view_group{
        set_name = SetName
    } = Group,
    DocOpenOptions = case Conflicts of
    true ->
        [conflicts];
    false ->
        []
    end,
    
    fun({{Key, DocId}, {PartId, Value}}, OffsetReds,
            {AccLimit, AccSkip, Resp, RowFunAcc}) ->
        case {AccLimit, AccSkip, Resp} of
        {0, _, _} ->
            % we've done "limit" rows, stop foldling
            {stop, {0, 0, Resp, RowFunAcc}};
        {_, AccSkip, _} when AccSkip > 0 ->
            % just keep skipping
            {ok, {AccLimit, AccSkip - 1, Resp, RowFunAcc}};
        {_, _, undefined} ->
            % rendering the first row, first we start the response
            Offset = ReduceCountFun(OffsetReds),
            {ok, Resp2, RowFunAcc0} = StartRespFun(Req, Etag,
                TotalViewCount, Offset, RowFunAcc),
            Kv = {{Key, DocId}, Value},
            JsonDoc = get_row_doc(
                Kv, SetName, PartId, IncludeDocs, Req#httpd.user_ctx, DocOpenOptions),
            {Go, RowFunAcc2} = SendRowFun(Resp2, Kv, JsonDoc, RowFunAcc0),
            {Go, {AccLimit - 1, 0, Resp2, RowFunAcc2}};
        {AccLimit, _, Resp} when (AccLimit > 0) ->
            % rendering all other rows
            Kv = {{Key, DocId}, Value},
            JsonDoc = get_row_doc(
                Kv, SetName, PartId, IncludeDocs, Req#httpd.user_ctx, DocOpenOptions),
            {Go, RowFunAcc2} = SendRowFun(Resp, Kv, JsonDoc, RowFunAcc),
            {Go, {AccLimit - 1, 0, Resp, RowFunAcc2}}
        end
    end.


get_row_doc(_Kv, _SetName, _PartId, false, _UserCtx, _DocOpenOptions) ->
    nil;

get_row_doc({{_Key, DocId}, {Props}}, SetName, PartId, true, UserCtx, DocOpenOptions) ->
    Rev = case couch_util:get_value(<<"_rev">>, Props) of
    undefined ->
        nil;
    Rev0 ->
        couch_doc:parse_rev(Rev0)
    end,
    Id = couch_util:get_value(<<"_id">>, Props, DocId),
    open_row_doc(SetName, PartId, Id, Rev, UserCtx, DocOpenOptions);

get_row_doc({{_Key, DocId}, _Value}, SetName, PartId, true, UserCtx, DocOpenOptions) ->
    open_row_doc(SetName, PartId, DocId, nil, UserCtx, DocOpenOptions).


open_row_doc(SetName, PartId, Id, Rev, UserCtx, DocOptions) ->
    {ok, Db} = couch_db:open(
        ?dbname(SetName, PartId), [{user_ctx, UserCtx}]),
    JsonDoc = case (catch couch_db_frontend:couch_doc_open(Db, Id, Rev, DocOptions)) of
    #doc{} = Doc ->
        couch_doc:to_json_obj(Doc, []);
    _ ->
        null
    end,
    ok = couch_db:close(Db),
    JsonDoc.


make_reduce_fold_funs(Req, GroupLevel, _QueryArgs, Etag, HelperFuns) ->
    #reduce_fold_helper_funs{
        start_response = StartRespFun,
        send_row = SendRowFun
    } = apply_default_helper_funs(HelperFuns),

    GroupRowsFun =
        fun({_Key1,_}, {_Key2,_}) when GroupLevel == 0 ->
            true;
        ({Key1,_}, {Key2,_})
                when is_integer(GroupLevel) and is_list(Key1) and is_list(Key2) ->
            lists:sublist(Key1, GroupLevel) == lists:sublist(Key2, GroupLevel);
        ({Key1,_}, {Key2,_}) ->
            Key1 == Key2
        end,

    RespFun = fun
    (_Key, _Red, {AccLimit, AccSkip, Resp, RowAcc}) when AccSkip > 0 ->
        % keep skipping
        {ok, {AccLimit, AccSkip - 1, Resp, RowAcc}};
    (_Key, _Red, {0, _AccSkip, Resp, RowAcc}) ->
        % we've exhausted limit rows, stop
        {stop, {0, _AccSkip, Resp, RowAcc}};

    (_Key, Red, {AccLimit, 0, undefined, RowAcc0}) when GroupLevel == 0 ->
        % we haven't started responding yet and group=false
        {ok, Resp2, RowAcc} = StartRespFun(Req, Etag, RowAcc0),
        {Go, RowAcc2} = SendRowFun(Resp2, {null, Red}, RowAcc),
        {Go, {AccLimit - 1, 0, Resp2, RowAcc2}};
    (_Key, Red, {AccLimit, 0, Resp, RowAcc}) when GroupLevel == 0 ->
        % group=false but we've already started the response
        {Go, RowAcc2} = SendRowFun(Resp, {null, Red}, RowAcc),
        {Go, {AccLimit - 1, 0, Resp, RowAcc2}};

    (Key, Red, {AccLimit, 0, undefined, RowAcc0})
            when is_integer(GroupLevel), is_list(Key) ->
        % group_level and we haven't responded yet
        {ok, Resp2, RowAcc} = StartRespFun(Req, Etag, RowAcc0),
        {Go, RowAcc2} = SendRowFun(Resp2,
                {lists:sublist(Key, GroupLevel), Red}, RowAcc),
        {Go, {AccLimit - 1, 0, Resp2, RowAcc2}};
    (Key, Red, {AccLimit, 0, Resp, RowAcc})
            when is_integer(GroupLevel), is_list(Key) ->
        % group_level and we've already started the response
        {Go, RowAcc2} = SendRowFun(Resp,
                {lists:sublist(Key, GroupLevel), Red}, RowAcc),
        {Go, {AccLimit - 1, 0, Resp, RowAcc2}};

    (Key, Red, {AccLimit, 0, undefined, RowAcc0}) ->
        % group=true and we haven't responded yet
        {ok, Resp2, RowAcc} = StartRespFun(Req, Etag, RowAcc0),
        {Go, RowAcc2} = SendRowFun(Resp2, {Key, Red}, RowAcc),
        {Go, {AccLimit - 1, 0, Resp2, RowAcc2}};
    (Key, Red, {AccLimit, 0, Resp, RowAcc}) ->
        % group=true and we've already started the response
        {Go, RowAcc2} = SendRowFun(Resp, {Key, Red}, RowAcc),
        {Go, {AccLimit - 1, 0, Resp, RowAcc2}}
    end,
    {ok, GroupRowsFun, RespFun}.

apply_default_helper_funs(
        #view_fold_helper_funs{
            start_response = StartResp,
            send_row = SendRow
        }=Helpers) ->
    StartResp2 = case StartResp of
    undefined -> fun json_view_start_resp/5;
    _ -> StartResp
    end,

    SendRow2 = case SendRow of
    undefined -> fun send_json_view_row/4;
    _ -> SendRow
    end,

    Helpers#view_fold_helper_funs{
        start_response = StartResp2,
        send_row = SendRow2
    };


apply_default_helper_funs(
        #reduce_fold_helper_funs{
            start_response = StartResp,
            send_row = SendRow
        }=Helpers) ->
    StartResp2 = case StartResp of
    undefined -> fun json_reduce_start_resp/3;
    _ -> StartResp
    end,

    SendRow2 = case SendRow of
    undefined -> fun send_json_reduce_row/3;
    _ -> SendRow
    end,

    Helpers#reduce_fold_helper_funs{
        start_response = StartResp2,
        send_row = SendRow2
    }.

make_key_options(#view_query_args{direction = Dir}=QueryArgs) ->
     [{dir,Dir} | make_start_key_option(QueryArgs) ++
            make_end_key_option(QueryArgs)].

make_start_key_option(
        #view_query_args{
            start_key = StartKey,
            start_docid = StartDocId}) ->
    if StartKey == undefined ->
        [];
    true ->
        [{start_key, {StartKey, StartDocId}}]
    end.

make_end_key_option(#view_query_args{end_key = undefined}) ->
    [];
make_end_key_option(
        #view_query_args{end_key = EndKey,
            end_docid = EndDocId,
            inclusive_end = true}) ->
    [{end_key, {EndKey, EndDocId}}];
make_end_key_option(
        #view_query_args{
            end_key = EndKey,
            end_docid = EndDocId,
            inclusive_end = false}) ->
    [{end_key_gt, {EndKey,reverse_key_default(EndDocId)}}].

json_view_start_resp(Req, Etag, TotalViewCount, Offset, _Acc) ->
    {ok, Resp} = start_json_response(Req, 200, [{"Etag", Etag}]),
    % TODO: likely, remove offset, won't make sense with passive partitions
    BeginBody = io_lib:format(
        "{\"total_rows\":~w,\"offset\":~w,\"rows\":[\r\n",
        [TotalViewCount, Offset]),
    {ok, Resp, BeginBody}.

send_json_view_row(Resp, Kv, Doc, RowFront) ->
    JsonObj = view_row_obj(Kv, Doc),
    send_chunk(Resp, RowFront ++  ?JSON_ENCODE(JsonObj)),
    {ok, ",\r\n"}.

json_reduce_start_resp(Req, Etag, _Acc0) ->
    {ok, Resp} = start_json_response(Req, 200, [{"Etag", Etag}]),
    {ok, Resp, "{\"rows\":[\r\n"}.

send_json_reduce_row(Resp, {Key, Value}, RowFront) ->
    send_chunk(Resp, RowFront ++ ?JSON_ENCODE({[{key, Key}, {value, Value}]})),
    {ok, ",\r\n"}.

view_etag(Group, View) ->
    view_etag(Group, View, nil).

view_etag(Group, {reduce, _, _, View}, Extra) ->
    view_etag(Group, View, Extra);
view_etag(#set_view_group{sig = Sig, index_header = Header},
        #set_view{update_seqs = UpdateSeqs, purge_seqs = PurgeSeqs},
        Extra) ->
    #set_view_index_header{
        num_partitions = NumPartitions,
        abitmask = Abitmask
    } = Header,
    couch_httpd:make_etag(
        {Sig, UpdateSeqs, PurgeSeqs, Extra, NumPartitions, Abitmask}).

% the view row has an error
view_row_obj({{Key, error}, Value}, _Doc) ->
    {[{key, Key}, {error, Value}]};
view_row_obj({{Key, DocId}, Value}, nil) ->
    {[{id, DocId}, {key, Key}, {value, Value}]};
view_row_obj({{Key, DocId}, Value}, Doc) ->
    {[{id, DocId}, {key, Key}, {value, Value}, {doc, Doc}]}.


finish_view_fold(Req, TotalRows, Offset, FoldResult) ->
    finish_view_fold(Req, TotalRows, Offset, FoldResult, []).

finish_view_fold(Req, TotalRows, Offset, FoldResult, Fields) ->
    case FoldResult of
    {_, _, undefined, _} ->
        % nothing found in the view or keys, nothing has been returned
        % send empty view
        send_json(Req, 200, {[
            {total_rows, TotalRows},
            {offset, Offset},
            {rows, []}
        ] ++ Fields});
    {_, _, Resp, _} ->
        % end the view
        send_chunk(Resp, "\r\n]}"),
        end_json_response(Resp)
    end.

finish_reduce_fold(Req, Resp) ->
    finish_reduce_fold(Req, Resp, []).

finish_reduce_fold(Req, Resp, Fields) ->
    case Resp of
    undefined ->
        send_json(Req, 200, {[
            {rows, []}
        ] ++ Fields});
    Resp ->
        send_chunk(Resp, "\r\n]}"),
        end_json_response(Resp)
    end.

parse_bool_param(Val) ->
    case string:to_lower(Val) of
    "true" -> true;
    "false" -> false;
    _ ->
        Msg = io_lib:format("Invalid boolean parameter: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

parse_int_param(Val) ->
    case (catch list_to_integer(Val)) of
    IntVal when is_integer(IntVal) ->
        IntVal;
    _ ->
        Msg = io_lib:format("Invalid value for integer parameter: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

parse_positive_int_param(Val) ->
    case parse_int_param(Val) of
    IntVal when IntVal >= 0 ->
        IntVal;
    _ ->
        Fmt = "Invalid value for positive integer parameter: ~p",
        Msg = io_lib:format(Fmt, [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.

