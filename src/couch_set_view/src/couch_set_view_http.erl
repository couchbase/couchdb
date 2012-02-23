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

-module(couch_set_view_http).

-export([handle_req/1]).

-export([make_view_fold_fun/6, finish_view_fold/4, finish_view_fold/5]).
-export([view_etag/2, view_etag/3, make_reduce_fold_funs/5]).
-export([design_doc_view/6, parse_bool_param/1, get_row_doc/5]).

-import(couch_httpd,
    [send_json/2,send_json/3,send_json/4,send_method_not_allowed/2,send_chunk/2,
    start_json_response/2, start_json_response/3, end_json_response/1,
    send_chunked_error/2]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").



handle_req(#httpd{path_parts = [<<"_set_view">>, SetName, <<"_cleanup">>]} = Req) ->
    case Req#httpd.method of
    'POST' ->
         couch_httpd:validate_ctype(Req, "application/json"),
         ok = couch_set_view:cleanup_index_files(SetName),
         send_json(Req, 202, {[{ok, true}]});
     _ ->
         send_method_not_allowed(Req, "POST")
     end;

handle_req(#httpd{path_parts = PathParts} = Req) ->
    [<<"_set_view">>, SetName, <<"_design">>, DesignName | Rest] = PathParts,
    route_request(Req, SetName, <<"_design/", DesignName/binary>>, Rest).

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_define">>]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {Fields} = couch_httpd:json_body_obj(Req),
    SetViewParams = #set_view_params{
        max_partitions = couch_util:get_value(<<"number_partitions">>, Fields, 0),
        active_partitions = couch_util:get_value(<<"active_partitions">>, Fields, []),
        passive_partitions = couch_util:get_value(<<"passive_partitions">>, Fields, []),
        use_replica_index = couch_util:get_value(<<"use_replica_index">>, Fields, false)
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
    {ok, _Pid} = couch_set_view_compactor:start_compact(SetName, DDocId, main),
    couch_httpd:send_json(Req, 202, {[{ok, true}]});

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_compact">>, <<"main">>]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {ok, _Pid} = couch_set_view_compactor:start_compact(SetName, DDocId, main),
    couch_httpd:send_json(Req, 202, {[{ok, true}]});

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_compact">>, <<"replica">>]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {ok, _Pid} = couch_set_view_compactor:start_compact(SetName, DDocId, replica),
    couch_httpd:send_json(Req, 202, {[{ok, true}]});

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_set_partition_states">>]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {Fields} = couch_httpd:json_body_obj(Req),
    Active = couch_util:get_value(<<"active">>, Fields, []),
    Passive = couch_util:get_value(<<"passive">>, Fields, []),
    Cleanup = couch_util:get_value(<<"cleanup">>, Fields, []),
    ok = couch_set_view:set_partition_states(
        SetName, DDocId, Active, Passive, Cleanup),
    couch_httpd:send_json(Req, 201, {[{ok, true}]});

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_add_replica_partitions">>]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    List = [_ | _] = couch_httpd:json_body(Req),
    ok = couch_set_view:add_replica_partitions(SetName, DDocId, List),
    couch_httpd:send_json(Req, 201, {[{ok, true}]});

route_request(#httpd{method = 'POST'} = Req, SetName, DDocId, [<<"_remove_replica_partitions">>]) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    List = [_ | _] = couch_httpd:json_body(Req),
    ok = couch_set_view:remove_replica_partitions(SetName, DDocId, List),
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
    GroupReq = #set_view_group_req{
        stale = Stale,
        update_stats = true
    },
    case couch_set_view:get_map_view(SetName, DDocId, ViewName, GroupReq, FilteredPartitions) of
    {ok, View, Group, _} ->
        QueryArgs = parse_view_params(Req, Keys, ViewName, map),
        Result = output_map_view(Req, View, Group, QueryArgs),
        couch_set_view:release_group(Group);
    {not_found, Reason} ->
        GroupReq2 = GroupReq#set_view_group_req{
            update_stats = false
        },
        case couch_set_view:get_reduce_view(SetName, DDocId, ViewName, GroupReq2, FilteredPartitions) of
        {ok, ReduceView, Group, _} ->
            case Reduce of
            false ->
                QueryArgs = parse_view_params(Req, Keys, ViewName, red_map),
                MapView = couch_set_view:extract_map_view(ReduceView),
                Result = output_map_view(Req, MapView, Group, QueryArgs),
                couch_set_view:release_group(Group);
            _ ->
                QueryArgs = parse_view_params(Req, Keys, ViewName, reduce),
                Result = output_reduce_view(Req, ReduceView, Group, QueryArgs),
                couch_set_view:release_group(Group)
            end;
        _ ->
            Result = nil,
            throw({not_found, Reason})
        end
    end,
    couch_stats_collector:increment({httpd, view_reads}),
    Result.

output_map_view(Req, View, Group, QueryArgs) ->
    #view_query_args{
        limit = Limit,
        skip = SkipCount
    } = QueryArgs,
    CurrentEtag = view_etag(Group, View, QueryArgs#view_query_args.keys),
    couch_httpd:etag_respond(Req, CurrentEtag, fun() ->
        RowCount = get_row_count(Group, View),
        RedCountFun = get_reduce_count_fun(Group),
        FoldHelpers = #view_fold_helper_funs{reduce_count = RedCountFun},
        FoldlFun = make_view_fold_fun(Req, QueryArgs, CurrentEtag, Group, RowCount, FoldHelpers),
        FoldAccInit = {Limit, SkipCount, undefined, []},
        {ok, LastReduce, FoldResult} = couch_set_view:fold(Group, View, FoldlFun, FoldAccInit, QueryArgs),
        finish_view_fold(Req, RowCount, RedCountFun(LastReduce), FoldResult)
    end).

output_reduce_view(Req, View, Group, QueryArgs) ->
    #view_query_args{
        limit = Limit,
        skip = Skip,
        group_level = GroupLevel
    } = QueryArgs,
    CurrentEtag = view_etag(Group, View, QueryArgs#view_query_args.keys),
    couch_httpd:etag_respond(Req, CurrentEtag, fun() ->
        {ok, KeyGroupFun, FoldFun} = make_reduce_fold_funs(
            Req, GroupLevel, QueryArgs, CurrentEtag, #reduce_fold_helper_funs{}),
        FoldAccInit = {Limit, Skip, undefined, []},
        {ok, {_, _, Resp, _}} = couch_set_view:fold_reduce(
            Group, View, FoldFun, FoldAccInit, KeyGroupFun, QueryArgs),
        finish_reduce_fold(Req, Resp)
    end).


get_row_count(#set_view_group{replica_group = nil}, View) ->
    {ok, RowCount} = couch_set_view:get_row_count(View),
    RowCount;
get_row_count(#set_view_group{replica_group = RepGroup}, View) ->
    RepView = lists:nth(View#set_view.id_num + 1, RepGroup#set_view_group.views),
    {ok, RowCount1} = couch_set_view:get_row_count(View),
    {ok, RowCount2} = couch_set_view:get_row_count(RepView),
    RowCount1 + RowCount2.


get_reduce_count_fun(#set_view_group{replica_group = nil}) ->
    fun couch_set_view:reduce_to_count/1;
get_reduce_count_fun(#set_view_group{replica_group = #set_view_group{}}) ->
    fun(_) -> nil end.


get_stale_type(Req) ->
    list_to_existing_atom(couch_httpd:qs_value(Req, "stale", "false")).

get_reduce_type(Req) ->
    list_to_existing_atom(couch_httpd:qs_value(Req, "reduce", "true")).

parse_view_params(Req, Keys, ViewName, ViewType) ->
    Params = couch_httpd_view:parse_view_params(Req, Keys, ViewType),
    Params#view_query_args{view_name = ViewName}.

make_view_fold_fun(Req, QueryArgs, Etag, Group, TotalViewCount, HelperFuns) ->
    #view_fold_helper_funs{
        start_response = StartRespFun,
        send_row = SendRowFun,
        reduce_count = ReduceCountFun
    } = apply_default_helper_funs(HelperFuns),

    #view_query_args{
        include_docs = IncludeDocs,
        conflicts = Conflicts,
        debug = Debug
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
    
    fun({{_Key, _DocId}, {_PartId, _Value}} = Kv, OffsetReds,
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
            JsonDoc = get_row_doc(
                Kv, SetName, IncludeDocs, Req#httpd.user_ctx, DocOpenOptions),
            {Go, RowFunAcc2} = SendRowFun(Resp2, Kv, JsonDoc, RowFunAcc0, Debug),
            {Go, {AccLimit - 1, 0, Resp2, RowFunAcc2}};
        {AccLimit, _, Resp} when (AccLimit > 0) ->
            % rendering all other rows
            JsonDoc = get_row_doc(
                Kv, SetName, IncludeDocs, Req#httpd.user_ctx, DocOpenOptions),
            {Go, RowFunAcc2} = SendRowFun(Resp, Kv, JsonDoc, RowFunAcc, Debug),
            {Go, {AccLimit - 1, 0, Resp, RowFunAcc2}}
        end
    end.


get_row_doc(_Kv, _SetName, false, _UserCtx, _DocOpenOptions) ->
    nil;

get_row_doc({{_Key, DocId}, {PartId, {Props}}}, SetName, true, UserCtx, DocOpenOptions) ->
    Id = couch_util:get_value(<<"_id">>, Props, DocId),
    open_row_doc(SetName, PartId, Id, UserCtx, DocOpenOptions);

get_row_doc({{_Key, DocId}, {PartId, _Value}}, SetName, true, UserCtx, DocOpenOptions) ->
    open_row_doc(SetName, PartId, DocId, UserCtx, DocOpenOptions).


open_row_doc(SetName, PartId, Id, UserCtx, DocOptions) ->
    {ok, Db} = couch_db:open(
        ?dbname(SetName, PartId), [{user_ctx, UserCtx}]),
    JsonDoc = case (catch couch_db_frontend:open_doc(Db, Id, DocOptions)) of
    {ok, #doc{} = Doc} ->
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
    undefined -> fun send_json_view_row/5;
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

json_view_start_resp(Req, Etag, TotalRowCount, Offset, _Acc) ->
    {ok, Resp} = start_json_response(Req, 200, [{"Etag", Etag}]),
    % TODO: likely, remove offset, won't make sense with passive partitions.
    %       Also, merged views don't have it.
    BeginBody0 = io_lib:format("{\"total_rows\":~w,", [TotalRowCount]),
    BeginBody = case is_number(Offset) of
    true ->
        [BeginBody0, io_lib:format("\"offset\":~w,", [Offset])];
    false ->
        BeginBody0
    end,
    {ok, Resp, [BeginBody, "\"rows\":[\r\n"]}.

send_json_view_row(Resp, Kv, Doc, RowFront, DebugMode) ->
    JsonObj = view_row_obj(Kv, Doc, DebugMode),
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
view_row_obj({{Key, error}, Value}, _Doc, _DebugMode) ->
    {[{key, Key}, {error, Value}]};
view_row_obj({{Key, DocId}, {_PartId, Value}}, nil, false) ->
    {[{id, DocId}, {key, Key}, {value, Value}]};
view_row_obj({{Key, DocId}, {PartId, Value}}, nil, true) ->
    {[{id, DocId}, {key, Key}, {partition, PartId}, {value, Value}]};
view_row_obj({{Key, DocId}, {_PartId, Value}}, Doc, false) ->
    {[{id, DocId}, {key, Key}, {value, Value}, {doc, Doc}]};
view_row_obj({{Key, DocId}, {PartId, Value}}, Doc, true) ->
    {[{id, DocId}, {key, Key}, {partition, PartId}, {value, Value}, {doc, Doc}]}.


finish_view_fold(Req, TotalRows, Offset, FoldResult) ->
    finish_view_fold(Req, TotalRows, Offset, FoldResult, []).

finish_view_fold(Req, TotalRows, Offset, FoldResult, Fields) ->
    case FoldResult of
    {_, _, undefined, _} ->
        % nothing found in the view or keys, nothing has been returned
        % send empty view
        Props = case is_number(Offset) of
        true ->
            [{total_rows, TotalRows}, {offset, Offset}, {rows, []}];
        false ->
            [{total_rows, TotalRows}, {rows, []}]
        end,
        send_json(Req, 200, {Props ++ Fields});
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
    couch_httpd_view:parse_bool_param(Val).
