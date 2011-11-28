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

-module(couch_httpd_db).
-include("couch_db.hrl").

-export([handle_request/1, handle_compact_req/2, handle_design_req/2,
    db_req/2, handle_changes_req/2,
    update_doc_result_to_json/1, update_doc_result_to_json/2,
    handle_design_info_req/3, handle_view_cleanup_req/2]).

-import(couch_httpd,
    [send_json/2,send_json/3,send_json/4,send_method_not_allowed/2,
    send_response/4,start_json_response/2,start_json_response/3,
    send_chunk/2,last_chunk/1,end_json_response/1,
    start_chunked_response/3, absolute_uri/2, send/2,
    start_response_length/4, send_error/4]).

-record(doc_query_args, {
    options = [],
    rev = nil,
    open_revs = [],
    update_type = interactive_edit,
    atts_since = nil
}).

% Database request handlers
handle_request(#httpd{path_parts=[DbName|RestParts],method=Method,
        db_frontend = DbFrontend, db_url_handlers=DbUrlHandlers}=Req)->
    case {Method, RestParts} of
    {'PUT', []} ->
        create_db_req(Req, DbName);
    {'DELETE', []} ->
        % if we get ?rev=... the user is using a faulty script where the
        % document id is empty by accident. Let them recover safely.
        case couch_httpd:qs_value(Req, "rev", false) of
            false -> delete_db_req(Req, DbName);
            _Rev -> throw({bad_request,
                "You tried to DELETE a database with a ?=rev parameter. "
                ++ "Did you mean to DELETE a document instead?"})
        end;
    {_, []} ->
        DbFrontend:do_db_req(Req, fun db_req/2);
    {_, [SecondPart|_]} ->
        Handler = couch_util:dict_find(SecondPart, DbUrlHandlers, fun db_req/2),
        DbFrontend:do_db_req(Req, Handler)
    end.

handle_changes_req(#httpd{method='POST'}=Req, Db) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    handle_changes_req1(Req, Db);
handle_changes_req(#httpd{method='GET'}=Req, Db) ->
    handle_changes_req1(Req, Db);
handle_changes_req(#httpd{path_parts=[_,<<"_changes">>]}=Req, _Db) ->
    send_method_not_allowed(Req, "GET,HEAD,POST").

handle_changes_req1(Req, Db) ->
    MakeCallback = fun(Resp) ->
        fun({change, Change, _}, "continuous") ->
            send_chunk(Resp, [?JSON_ENCODE(Change) | "\n"]);
        ({change, Change, Prepend}, _) ->
            send_chunk(Resp, [Prepend, ?JSON_ENCODE(Change)]);
        (start, "continuous") ->
            ok;
        (start, _) ->
            send_chunk(Resp, "{\"results\":[\n");
        ({stop, EndSeq}, "continuous") ->
            send_chunk(
                Resp,
                [?JSON_ENCODE({[{<<"last_seq">>, EndSeq}]}) | "\n"]
            ),
            end_json_response(Resp);
        ({stop, EndSeq}, _) ->
            send_chunk(
                Resp,
                io_lib:format("\n],\n\"last_seq\":~w}\n", [EndSeq])
            ),
            end_json_response(Resp);
        (timeout, _) ->
            send_chunk(Resp, "\n")
        end
    end,
    ChangesArgs = parse_changes_query(Req),
    DbFrontend = Req#httpd.db_frontend,
    ChangesFun = DbFrontend:handle_changes(ChangesArgs, Req, Db),
    WrapperFun = case ChangesArgs#changes_args.feed of
    "normal" ->
        {ok, Info} = DbFrontend:get_db_info(Db),
        CurrentEtag = couch_httpd:make_etag(Info),
        fun(FeedChangesFun) ->
            couch_httpd:etag_respond(
                Req,
                CurrentEtag,
                fun() ->
                    {ok, Resp} = couch_httpd:start_json_response(
                         Req, 200, [{"Etag", CurrentEtag}]
                    ),
                    FeedChangesFun(MakeCallback(Resp))
                end
            )
        end;
    _ ->
        % "longpoll" or "continuous"
        {ok, Resp} = couch_httpd:start_json_response(Req, 200),
        fun(FeedChangesFun) ->
            FeedChangesFun(MakeCallback(Resp))
        end
    end,
    couch_stats_collector:increment(
        {httpd, clients_requesting_changes}
    ),
    try
        WrapperFun(ChangesFun)
    after
    couch_stats_collector:decrement(
        {httpd, clients_requesting_changes}
    )
    end.

handle_compact_req(#httpd{method='POST',
                          path_parts=[DbName,_,Id|_],
                          db_frontend=DbFrontend}=Req, Db) ->
    ok = DbFrontend:check_is_admin(Db),
    couch_httpd:validate_ctype(Req, "application/json"),
    {ok, _} = DbFrontend:start_view_compact(DbName, Id),
    send_json(Req, 202, {[{ok, true}]});

handle_compact_req(#httpd{method='POST',db_frontend=DbFrontend}=Req, Db) ->
    ok = DbFrontend:check_is_admin(Db),
    couch_httpd:validate_ctype(Req, "application/json"),
    {ok, _} = DbFrontend:start_db_compact(Db),
    send_json(Req, 202, {[{ok, true}]});

handle_compact_req(Req, _Db) ->
    send_method_not_allowed(Req, "POST").

handle_view_cleanup_req(#httpd{method='POST',db_frontend=DbFrontend}=Req, Db) ->
    % delete unreferenced index files
    ok = DbFrontend:check_is_admin(Db),
    couch_httpd:validate_ctype(Req, "application/json"),
    ok = DbFrontend:cleanup_view_index_files(Db),
    send_json(Req, 202, {[{ok, true}]});

handle_view_cleanup_req(Req, _Db) ->
    send_method_not_allowed(Req, "POST").


handle_design_req(#httpd{
        path_parts=[_DbName, _Design, DesignName, <<"_",_/binary>> = Action | _Rest],
        design_url_handlers = DesignUrlHandlers,
        db_frontend = DbFrontend
    }=Req, Db) ->
    % load ddoc
    DesignId = <<"_design/", DesignName/binary>>,
    DDoc = DbFrontend:open_doc(Db, DesignId, [ejson_body]),
    Handler = couch_util:dict_find(Action, DesignUrlHandlers, fun(_, _, _) ->
            throw({not_found, <<"missing handler: ", Action/binary>>})
        end),
    Handler(Req, Db, DDoc);

handle_design_req(Req, Db) ->
    db_req(Req, Db).

handle_design_info_req(#httpd{
            method='GET',
            path_parts=[_DbName, _Design, DesignName, _],
            db_frontend=DbFrontend
        }=Req, Db, _DDoc) ->
    DesignId = <<"_design/", DesignName/binary>>,
    {ok, GroupInfoList} = DbFrontend:get_group_info(Db, DesignId),
    send_json(Req, 200, {[
        {name, DesignName},
        {view_index, {GroupInfoList}}
    ]});

handle_design_info_req(Req, _Db, _DDoc) ->
    send_method_not_allowed(Req, "GET").

create_db_req(#httpd{user_ctx=UserCtx,db_frontend=DbFrontend}=Req, DbName) ->
    ok = couch_httpd:verify_is_server_admin(Req),
    case DbFrontend:create_db(DbName, UserCtx) of
    ok ->
        DbUrl = absolute_uri(Req, "/" ++ couch_util:url_encode(DbName)),
        send_json(Req, 201, [{"Location", DbUrl}], {[{ok, true}]});
    Error ->
        throw(Error)
    end.

delete_db_req(#httpd{user_ctx=UserCtx,db_frontend=DbFrontend}=Req, DbName) ->
    ok = couch_httpd:verify_is_server_admin(Req),
    case DbFrontend:delete_db(DbName, UserCtx) of
    ok ->
        send_json(Req, 200, {[{ok, true}]});
    Error ->
        throw(Error)
    end.

db_req(#httpd{method='GET',
              path_parts=[_DbName],
              db_frontend=DbFrontend}=Req, Db) ->
    {ok, DbInfo} = DbFrontend:get_db_info(Db),
    send_json(Req, {DbInfo});

db_req(#httpd{method='POST',
              path_parts=[DbName],
              db_frontend=DbFrontend}=Req, Db) ->
    
    case couch_httpd:is_ctype(Req, "application/json") of
    true ->
        Doc = couch_doc:from_json_obj(couch_httpd:json_body(Req));
    false ->
        Doc = #doc{binary=couch_httpd:body(Req)}
    end,
    Doc2 = case Doc#doc.id of
        <<"">> ->
            Doc#doc{id=couch_uuids:new(), rev={0, <<>>}};
        _ ->
            Doc
    end,
    DocId = Doc2#doc.id,
    case couch_httpd:qs_value(Req, "batch") of
    "ok" ->
        % async_batching
        spawn(fun() ->
                case catch(DbFrontend:update_doc(Db, Doc2, [])) of
                ok -> ok;
                Error ->
                    ?LOG_INFO("Batch doc error (~s): ~p",[DocId, Error])
                end
            end),
            
        send_json(Req, 202, [], {[
            {ok, true},
            {id, DocId}
        ]});
    _Normal ->
        % normal
        ok = DbFrontend:update_doc(Db, Doc2, []),
        DocUrl = absolute_uri(
            Req, binary_to_list(<<"/",DbName/binary,"/", DocId/binary>>)),
        send_json(Req, 201, [{"Location", DocUrl}], {[
            {ok, true},
            {id, DocId}
        ]})
    end;


db_req(#httpd{path_parts=[_DbName]}=Req, _Db) ->
    send_method_not_allowed(Req, "DELETE,GET,HEAD,POST");

db_req(#httpd{method='POST',
              path_parts=[_,<<"_ensure_full_commit">>],
              db_frontend=DbFrontend}=Req, Db) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    RequiredSeq = case couch_httpd:qs_value(Req, "seq") of
                      undefined -> undefined;
                      RequiredStr -> list_to_integer(RequiredStr)
                  end,
    {ok, StartTime} = DbFrontend:ensure_full_commit(Db, RequiredSeq),
    send_json(Req, 201, {[
        {ok, true},
        {instance_start_time, StartTime}
    ]});

db_req(#httpd{path_parts=[_,<<"_ensure_full_commit">>]}=Req, _Db) ->
    send_method_not_allowed(Req, "POST");

db_req(#httpd{method='POST',
              path_parts=[_,<<"_bulk_docs">>],
              db_frontend=DbFrontend}=Req, Db) ->
    couch_stats_collector:increment({httpd, bulk_requests}),
    couch_httpd:validate_ctype(Req, "application/json"),
    {JsonProps} = couch_httpd:json_body_obj(Req),
    case couch_util:get_value(<<"docs">>, JsonProps) of
    undefined ->
        send_error(Req, 400, <<"bad_request">>, <<"Missing JSON list of 'docs'">>);
    DocsArray ->
        case couch_httpd:header_value(Req, "X-Couch-Full-Commit") of
        "true" ->
            Options = [full_commit];
        "false" ->
            Options = [delay_commit];
        _ ->
            Options = []
        end,
        case couch_util:get_value(<<"new_edits">>, JsonProps, true) of
        true ->
            Docs = lists:map(
                fun({ObjProps} = JsonObj) ->
                    Doc = couch_doc:from_json_obj(JsonObj),
                    Id = case Doc#doc.id of
                        <<>> -> couch_uuids:new();
                        Id0 -> Id0
                    end,
                    case couch_util:get_value(<<"_rev">>, ObjProps) of
                    undefined ->
                       Rev2 = {0, <<>>};
                    Rev  ->
                        {Pos, RevId} = couch_doc:parse_rev(Rev),
                        Rev2 = {Pos, RevId}
                    end,
                    Doc#doc{id=Id,rev=Rev2}
                end,
                DocsArray),
            Options2 =
            case couch_util:get_value(<<"all_or_nothing">>, JsonProps) of
            true  -> [all_or_nothing|Options];
            _ -> Options
            end,
            case DbFrontend:update_docs(Db, Docs, [sort_docs | Options2]) of
            ok ->
                send_json(Req, 201, {[{ok, true}]});
            {aborted, Errors} ->
                ErrorsJson =
                    lists:map(fun update_doc_result_to_json/1, Errors),
                send_json(Req, 417, ErrorsJson)
            end;
        false ->
            Docs = lists:map(fun(JsonObj) ->
                    Doc = couch_doc:from_json_obj(JsonObj),
                    Doc
                end, DocsArray),
            ok = DbFrontend:update_docs(Db, Docs, [sort_docs | Options], replicated_changes),
            send_json(Req, 201, {[{ok, true}]})
        end
    end;
db_req(#httpd{path_parts=[_,<<"_bulk_docs">>]}=Req, _Db) ->
    send_method_not_allowed(Req, "POST");

db_req(#httpd{method='POST',
              path_parts=[_,<<"_purge">>],
              db_frontend=DbFrontend}=Req, Db) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {IdsRevs} = couch_httpd:json_body_obj(Req),
    IdsRevs2 = [{Id, couch_doc:parse_revs(Revs)} || {Id, Revs} <- IdsRevs],

    case DbFrontend:purge_docs(Db, IdsRevs2) of
    {ok, PurgeSeq, PurgedIdsRevs} ->
        PurgedIdsRevs2 = [{Id, couch_doc:revs_to_strs(Revs)} || {Id, Revs} <- PurgedIdsRevs],
        send_json(Req, 200, {[{<<"purge_seq">>, PurgeSeq}, {<<"purged">>, {PurgedIdsRevs2}}]});
    Error ->
        throw(Error)
    end;

db_req(#httpd{path_parts=[_,<<"_purge">>]}=Req, _Db) ->
    send_method_not_allowed(Req, "POST");

db_req(#httpd{method='GET',path_parts=[_,<<"_all_docs">>]}=Req, Db) ->
    Keys = couch_httpd:qs_json_value(Req, "keys", nil),
    all_docs_view(Req, Db, Keys);

db_req(#httpd{method='POST',path_parts=[_,<<"_all_docs">>]}=Req, Db) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {Fields} = couch_httpd:json_body_obj(Req),
    case couch_util:get_value(<<"keys">>, Fields, nil) of
    nil ->
        ?LOG_DEBUG("POST to _all_docs with no keys member.", []),
        all_docs_view(Req, Db, nil);
    Keys when is_list(Keys) ->
        all_docs_view(Req, Db, Keys);
    _ ->
        throw({bad_request, "`keys` member must be a array."})
    end;

db_req(#httpd{path_parts=[_,<<"_all_docs">>]}=Req, _Db) ->
    send_method_not_allowed(Req, "GET,HEAD,POST");

db_req(#httpd{method='POST',
              path_parts=[_,<<"_missing_revs">>],
              db_frontend=DbFrontend}=Req, Db) ->
    {JsonDocIdRevs} = couch_httpd:json_body_obj(Req),
    JsonDocIdRevs2 = [{Id, [couch_doc:parse_rev(RevStr) || RevStr <- RevStrs]} || {Id, RevStrs} <- JsonDocIdRevs],
    {ok, Results} = DbFrontend:get_missing_revs(Db, JsonDocIdRevs2),
    Results2 = [{Id, couch_doc:revs_to_strs(Revs)} || {Id, Revs, _} <- Results],
    send_json(Req, {[
        {missing_revs, {Results2}}
    ]});

db_req(#httpd{path_parts=[_,<<"_missing_revs">>]}=Req, _Db) ->
    send_method_not_allowed(Req, "POST");

db_req(#httpd{method='POST',
              path_parts=[_,<<"_revs_diff">>],
              db_frontend=DbFrontend}=Req, Db) ->
    {JsonDocIdRevs} = couch_httpd:json_body_obj(Req),
    JsonDocIdRevs2 =
        [{Id, couch_doc:parse_rev(RevStr)} || {Id, RevStr} <- JsonDocIdRevs],
    {ok, Results} = DbFrontend:get_missing_revs(Db, JsonDocIdRevs2),
    Results2 =
    lists:map(fun({Id, MissingRevs}) ->
        {Id,
            {[{missing, couch_doc:revs_to_strs(MissingRevs)}]}}
    end, Results),
    send_json(Req, {Results2});

db_req(#httpd{path_parts=[_,<<"_revs_diff">>]}=Req, _Db) ->
    send_method_not_allowed(Req, "POST");

db_req(#httpd{method='PUT',
              path_parts=[_,<<"_security">>],
              db_frontend=DbFrontend}=Req, Db) ->
    SecObj = couch_httpd:json_body(Req),
    ok = DbFrontend:set_security(Db, SecObj),
    send_json(Req, {[{<<"ok">>, true}]});

db_req(#httpd{method='GET',
              path_parts=[_,<<"_security">>],
              db_frontend=DbFrontend}=Req, Db) ->
    send_json(Req, DbFrontend:get_security(Db));

db_req(#httpd{path_parts=[_,<<"_security">>]}=Req, _Db) ->
    send_method_not_allowed(Req, "PUT,GET");

db_req(#httpd{method='PUT',
              path_parts=[_,<<"_revs_limit">>],
              db_frontend=DbFrontend}=Req,
        Db) ->
    Limit = couch_httpd:json_body(Req),
    ok = DbFrontend:set_revs_limit(Db, Limit),
    send_json(Req, {[{<<"ok">>, true}]});

db_req(#httpd{method='GET',
              path_parts=[_,<<"_revs_limit">>],
              db_frontend=DbFrontend}=Req, Db) ->
    send_json(Req, DbFrontend:get_revs_limit(Db));

db_req(#httpd{path_parts=[_,<<"_revs_limit">>]}=Req, _Db) ->
    send_method_not_allowed(Req, "PUT,GET");

% Special case to enable using an unencoded slash in the URL of design docs,
% as slashes in document IDs must otherwise be URL encoded.
db_req(#httpd{method='GET',mochi_req=MochiReq, path_parts=[DbName,<<"_design/",_/binary>>|_]}=Req, _Db) ->
    PathFront = "/" ++ couch_httpd:quote(binary_to_list(DbName)) ++ "/",
    [_|PathTail] = re:split(MochiReq:get(raw_path), "_design%2F",
        [{return, list}]),
    couch_httpd:send_redirect(Req, PathFront ++ "_design/" ++
        mochiweb_util:join(PathTail, "_design%2F"));

db_req(#httpd{path_parts=[_DbName,<<"_design">>,Name]}=Req, Db) ->
    db_doc_req(Req, Db, <<"_design/",Name/binary>>);


% Special case to allow for accessing local documents without %2F
% encoding the docid. Throws out requests that don't have the second
% path part or that specify an attachment name.
db_req(#httpd{path_parts=[_DbName, <<"_local">>]}, _Db) ->
    throw({bad_request, <<"Invalid _local document id.">>});

db_req(#httpd{path_parts=[_DbName, <<"_local/">>]}, _Db) ->
    throw({bad_request, <<"Invalid _local document id.">>});

db_req(#httpd{path_parts=[_DbName, <<"_local">>, Name]}=Req, Db) ->
    db_doc_req(Req, Db, <<"_local/", Name/binary>>);

db_req(#httpd{path_parts=[_DbName, <<"_local">> | _Rest]}, _Db) ->
    throw({bad_request, <<"_local documents do not accept attachments.">>});

db_req(#httpd{path_parts=[_, DocId]}=Req, Db) ->
    db_doc_req(Req, Db, DocId).

all_docs_view(Req, Db, Keys) ->
    #view_query_args{
        start_key = StartKey,
        start_docid = StartDocId,
        end_key = EndKey,
        end_docid = EndDocId,
        limit = Limit,
        skip = SkipCount,
        direction = Dir,
        inclusive_end = Inclusive
    } = QueryArgs = couch_httpd_view:parse_view_params(Req, Keys, map),
    {ok, Info} = couch_db:get_db_info(Db),
    CurrentEtag = couch_httpd:make_etag(Info),
    couch_httpd:etag_respond(Req, CurrentEtag, fun() ->

        TotalRowCount = couch_util:get_value(doc_count, Info),
        StartId = if is_binary(StartKey) -> StartKey;
        true -> StartDocId
        end,
        EndId = if is_binary(EndKey) -> EndKey;
        true -> EndDocId
        end,
        FoldAccInit = {Limit, SkipCount, undefined, []},
        UpdateSeq = couch_db:get_update_seq(Db),
        JsonParams = case couch_httpd:qs_value(Req, "update_seq") of
        "true" ->
            [{update_seq, UpdateSeq}];
        _Else ->
            []
        end,
        case Keys of
        nil ->
            FoldlFun = couch_httpd_view:make_view_fold_fun(Req, QueryArgs, CurrentEtag, Db, UpdateSeq,
                TotalRowCount, #view_fold_helper_funs{
                    reduce_count = fun couch_db:enum_docs_reduce_to_count/1,
                    send_row = fun all_docs_send_json_view_row/6
                }),
            AdapterFun = fun(#doc_info{id=Id,deleted=Deleted}=DocInfo, Offset, Acc) ->
                case Deleted of
                false ->
                    FoldlFun({{Id, Id}, DocInfo}, Offset, Acc);
                true ->
                    {ok, Acc}
                end
            end,
            {ok, LastOffset, FoldResult} = couch_db:enum_docs(Db,
                AdapterFun, FoldAccInit, [{start_key, StartId}, {dir, Dir},
                    {if Inclusive -> end_key; true -> end_key_gt end, EndId}]),
            couch_httpd_view:finish_view_fold(Req, TotalRowCount, LastOffset, FoldResult, JsonParams);
        _ ->
            FoldlFun = couch_httpd_view:make_view_fold_fun(Req, QueryArgs, CurrentEtag, Db, UpdateSeq,
                TotalRowCount, #view_fold_helper_funs{
                    reduce_count = fun(Offset) -> Offset end,
                    send_row = fun all_docs_send_json_view_row/6
                }),
            KeyFoldFun = case Dir of
            fwd ->
                fun lists:foldl/3;
            rev ->
                fun lists:foldr/3
            end,
            FoldResult = KeyFoldFun(
                fun(Key, FoldAcc) ->
                    DocInfo = (catch couch_db:get_doc_info(Db, Key)),
                    Doc = case DocInfo of
                    {ok, #doc_info{id = Id} = Di} ->
                        {{Id, Id}, Di};
                    not_found ->
                        {{Key, error}, not_found};
                    _ ->
                        ?LOG_ERROR("Invalid DocInfo: ~p", [DocInfo]),
                        throw({error, invalid_doc_info})
                    end,
                    {_, FoldAcc2} = FoldlFun(Doc, 0, FoldAcc),
                    FoldAcc2
                end, FoldAccInit, Keys),
            couch_httpd_view:finish_view_fold(Req, TotalRowCount, 0, FoldResult, JsonParams)
        end
    end).

all_docs_send_json_view_row(Resp, Db, KV, IncludeDocs, Conflicts, RowFront) ->
    JsonRow = all_docs_view_row_obj(Db, KV, IncludeDocs, Conflicts),
    send_chunk(Resp, RowFront ++ ?JSON_ENCODE(JsonRow)),
    {ok, ",\r\n"}.

all_docs_view_row_obj(_Db, {{DocId, error}, Value}, _IncludeDocs, _Conflicts) ->
    {[{key, DocId}, {error, Value}]};
all_docs_view_row_obj(Db, {_KeyDocId, DocInfo}, true, Conflicts) ->
    case DocInfo of
    #doc_info{deleted = true} ->
        {all_docs_row(DocInfo) ++ [{doc, null}]};
    _ ->
        {all_docs_row(DocInfo) ++ couch_httpd_view:doc_member(
            Db, DocInfo, if Conflicts -> [conflicts]; true -> [] end)}
    end;
all_docs_view_row_obj(_Db, {_KeyDocId, DocInfo}, _IncludeDocs, _Conflicts) ->
    {all_docs_row(DocInfo)}.

all_docs_row(#doc_info{id = Id, rev = Rev, deleted = Del}) ->
    [ {id, Id}, {key, Id},
        {value, {[{rev, couch_doc:rev_to_str(Rev)}] ++ case Del of
            true -> [{deleted, true}];
            false -> []
            end}} ].


db_doc_req(#httpd{method='DELETE',db_frontend=DbFrontend}=Req, Db, DocId) ->
    % check for the existence of the doc to handle the 404 case.
    DbFrontend:open_doc(Db, DocId, []),
    case couch_httpd:qs_value(Req, "rev") of
    undefined ->
        update_doc(Req, Db, DocId,
                couch_doc_from_req(Req, DocId, {[{<<"_deleted">>,true}]}));
    Rev ->
        update_doc(Req, Db, DocId,
                couch_doc_from_req(Req, DocId,
                    {[{<<"_rev">>, ?l2b(Rev)},{<<"_deleted">>,true}]}))
    end;

db_doc_req(#httpd{method = 'GET',
                  db_frontend=DbFrontend} = Req, Db, DocId) ->
    #doc_query_args{
        options = Options
    } = parse_doc_query(Req),
    Doc = DbFrontend:couch_doc_open(Db, DocId, Options),
    send_doc(Req, Doc, Options);


db_doc_req(#httpd{method='PUT'}=Req, Db, DocId) ->
    couch_doc:validate_docid(DocId),
    Loc = absolute_uri(Req, "/" ++ ?b2l(Db#db.name) ++ "/" ++ ?b2l(DocId)),
    RespHeaders = [{"Location", Loc}],
    case couch_httpd:is_ctype(Req, "application/json") of
    true ->
        Body = couch_httpd:json_body(Req);
    false ->
        Body = couch_doc:from_binary(DocId, couch_httpd:body(Req), true)
    end,
    Doc = couch_doc_from_req(Req, DocId, Body),
    update_doc(Req, Db, DocId, Doc, RespHeaders);

db_doc_req(Req, _Db, _DocId) ->
    send_method_not_allowed(Req, "DELETE,GET,HEAD,POST,PUT").


send_doc(Req, Doc, Options) ->
    case Doc#doc.meta of
    [] ->
        DiskEtag = couch_httpd:doc_etag(Doc),
        % output etag only when we have no meta
        couch_httpd:etag_respond(Req, DiskEtag, fun() ->
            send_doc_efficiently(Req, Doc, [{"Etag", DiskEtag}], Options)
        end);
    _ ->
        send_doc_efficiently(Req, Doc, [], Options)
    end.


send_doc_efficiently(Req,
        #doc{binary=nil} = Doc, Headers, Options) ->
    send_json(Req, 200, Headers, couch_doc:to_json_obj(Doc, Options));
send_doc_efficiently(Req,
        #doc{binary=Binary}, Headers, _Options) ->
    Headers2 = Headers ++ [
        {"Content-Type", "application/content-stream"},
        {"Cache-Control", "must-revalidate"}
    ],
    send_response(Req, 200, Headers2, Binary).

update_doc_result_to_json({{Id, Rev}, Error}) ->
        {_Code, Err, Msg} = couch_httpd:error_info(Error),
        {[{id, Id}, {rev, couch_doc:rev_to_str(Rev)},
            {error, Err}, {reason, Msg}]}.

update_doc_result_to_json(#doc{id=DocId}, Result) ->
    update_doc_result_to_json(DocId, Result);
update_doc_result_to_json(DocId, {ok, NewRev}) ->
    {[{ok, true}, {id, DocId}, {rev, couch_doc:rev_to_str(NewRev)}]};
update_doc_result_to_json(DocId, Error) ->
    {_Code, ErrorStr, Reason} = couch_httpd:error_info(Error),
    {[{id, DocId}, {error, ErrorStr}, {reason, Reason}]}.


update_doc(Req, Db, DocId, Doc) ->
    update_doc(Req, Db, DocId, Doc, []).

update_doc(Req, Db, DocId, #doc{deleted=Deleted}=Doc, Headers) ->
    DbFrontend = Req#httpd.db_frontend,
    case couch_httpd:header_value(Req, "X-Couch-Full-Commit") of
    "true" ->
        Options = [full_commit];
    "false" ->
        Options = [delay_commit];
    _ ->
        Options = []
    end,
    ok = DbFrontend:update_doc(Db, Doc, Options),
    send_json(Req, if Deleted -> 200; true -> 201 end,
        Headers, {[
            {ok, true},
            {id, DocId}]}).

couch_doc_from_req(_Req, DocId, #doc{} = Doc) ->
    Doc#doc{id=DocId};
couch_doc_from_req(Req, DocId, Json) ->
    couch_doc_from_req(Req, DocId, couch_doc:from_json_obj(Json)).


parse_doc_query(Req) ->
    lists:foldl(fun({Key,Value}, Args) ->
        case {Key, Value} of
        {"attachments", "true"} ->
            Options = [attachments | Args#doc_query_args.options],
            Args#doc_query_args{options=Options};
        {"meta", "true"} ->
            Options = [revs_info, conflicts, deleted_conflicts | Args#doc_query_args.options],
            Args#doc_query_args{options=Options};
        {"revs", "true"} ->
            Options = [revs | Args#doc_query_args.options],
            Args#doc_query_args{options=Options};
        {"local_seq", "true"} ->
            Options = [local_seq | Args#doc_query_args.options],
            Args#doc_query_args{options=Options};
        {"revs_info", "true"} ->
            Options = [revs_info | Args#doc_query_args.options],
            Args#doc_query_args{options=Options};
        {"conflicts", "true"} ->
            Options = [conflicts | Args#doc_query_args.options],
            Args#doc_query_args{options=Options};
        {"deleted_conflicts", "true"} ->
            Options = [deleted_conflicts | Args#doc_query_args.options],
            Args#doc_query_args{options=Options};
        {"rev", Rev} ->
            Args#doc_query_args{rev=couch_doc:parse_rev(Rev)};
        {"open_revs", "all"} ->
            Args#doc_query_args{open_revs=all};
        {"open_revs", RevsJsonStr} ->
            JsonArray = ?JSON_DECODE(RevsJsonStr),
            Args#doc_query_args{open_revs=couch_doc:parse_revs(JsonArray)};
        {"latest", "true"} ->
            Options = [latest | Args#doc_query_args.options],
            Args#doc_query_args{options=Options};
        {"atts_since", RevsJsonStr} ->
            JsonArray = ?JSON_DECODE(RevsJsonStr),
            Args#doc_query_args{atts_since = couch_doc:parse_revs(JsonArray)};
        {"new_edits", "false"} ->
            Args#doc_query_args{update_type=replicated_changes};
        {"new_edits", "true"} ->
            Args#doc_query_args{update_type=interactive_edit};
        {"att_encoding_info", "true"} ->
            Options = [att_encoding_info | Args#doc_query_args.options],
            Args#doc_query_args{options=Options};
        _Else -> % unknown key value pair, ignore.
            Args
        end
    end, #doc_query_args{}, couch_httpd:qs(Req)).

parse_changes_query(Req) ->
    lists:foldl(fun({Key, Value}, Args) ->
        case {Key, Value} of
        {"feed", _} ->
            Args#changes_args{feed=Value};
        {"descending", "true"} ->
            Args#changes_args{dir=rev};
        {"since", _} ->
            Args#changes_args{since=list_to_integer(Value)};
        {"limit", _} ->
            Args#changes_args{limit=list_to_integer(Value)};
        {"style", _} ->
            Args#changes_args{style=list_to_existing_atom(Value)};
        {"heartbeat", "true"} ->
            Args#changes_args{heartbeat=true};
        {"heartbeat", _} ->
            Args#changes_args{heartbeat=list_to_integer(Value)};
        {"timeout", _} ->
            Args#changes_args{timeout=list_to_integer(Value)};
        {"include_docs", "true"} ->
            Args#changes_args{include_docs=true};
        {"conflicts", "true"} ->
            Args#changes_args{conflicts=true};
        {"filter", _} ->
            Args#changes_args{filter=Value};
        _Else -> % unknown key value pair, ignore.
            Args
        end
    end, #changes_args{}, couch_httpd:qs(Req)).



