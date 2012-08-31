%% -*- erlang -*-

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

-module(couch_set_view_test_util).

-export([start_server/0, start_server/1, stop_server/0]).
-export([create_set_dbs/2, delete_set_dbs/2, doc_count/2]).
-export([open_set_db/2, get_db_main_pid/1, delete_set_db/2]).
-export([populate_set_alternated/3, populate_set_sequentially/3, update_ddoc/2, delete_ddoc/2]).
-export([define_set_view/5]).
-export([query_view/3, query_view/4]).
-export([are_view_keys_sorted/2]).
-export([get_db_ref_counters/2, compact_set_dbs/3]).
-export([get_db_seqs/2]).
-export([full_reduce_id_btree/2]).
-export([full_reduce_view_btree/2]).
-export([fold_id_btree/5]).
-export([fold_view_btree/5]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").


start_server() ->
    couch_server_sup:start_link(test_util:config_files()),
    put(addr, couch_config:get("httpd", "bind_address", "127.0.0.1")),
    put(port, integer_to_list(mochiweb_socket_server:get(couch_httpd, port))).


start_server(SetName) ->
    couch_config:start_link(test_util:config_files()),
    DbDir = couch_config:get("couchdb", "database_dir"),
    IndexDir = couch_config:get("couchdb", "view_index_dir"),
    NewDbDir = filename:join([DbDir, ?b2l(SetName)]),
    NewIndexDir = filename:join([IndexDir, ?b2l(SetName)]),
    case file:make_dir(NewDbDir) of
    ok ->
        ok;
    {error, eexist} ->
        ok;
    Error ->
        throw(Error)
    end,
    case file:make_dir(NewIndexDir) of
    ok ->
        ok;
    {error, eexist} ->
        ok;
    Error2 ->
        throw(Error2)
    end,
    ok = couch_config:set("couchdb", "database_dir", NewDbDir, false),
    ok = couch_config:set("couchdb", "view_index_dir", NewIndexDir, false),
    start_server(),
    ok.


stop_server() ->
    ok = timer:sleep(1000),
    couch_server_sup:stop().


admin_user_ctx() ->
    {user_ctx, #user_ctx{roles = [<<"_admin">>]}}.


create_set_dbs(SetName, NumPartitions) ->
    lists:foreach(
        fun(PartId) ->
            DbName = iolist_to_binary([
                SetName, $/, integer_to_list(PartId)
            ]),
            {ok, Db} = couch_db:create(DbName, [admin_user_ctx()]),
            ok = couch_db:close(Db)
        end,
        lists:seq(0, NumPartitions - 1)),
    MasterDbName = iolist_to_binary([SetName, "/master"]),
    {ok, MasterDb} = couch_db:create(MasterDbName, [admin_user_ctx()]),
    ok = couch_db:close(MasterDb).


delete_set_dbs(SetName, NumPartitions) ->
    lists:foreach(
        fun(PartId) ->
            DbName = iolist_to_binary([
                SetName, $/, integer_to_list(PartId)
            ]),
            couch_server:delete(DbName, [admin_user_ctx()])
        end,
        lists:seq(0, NumPartitions - 1)),
    MasterDbName = iolist_to_binary([SetName, "/master"]),
    couch_server:delete(MasterDbName, [admin_user_ctx()]).


compact_set_dbs(SetName, Partitions, BlockUntilFinished) ->
    Dbs0 = open_set_dbs(SetName, Partitions),
    {ok, MasterDb} = couch_db:open_int(
        <<SetName/binary, "/master">>, [admin_user_ctx()]),
    Dbs = [MasterDb | Dbs0],
    MonRefs = lists:map(
        fun(Db) ->
            {ok, Pid} = couch_db:start_compact(Db),
            ok = couch_db:close(Db),
            {couch_db:name(Db), erlang:monitor(process, Pid)}
        end,
        Dbs),
    case BlockUntilFinished of
    true ->
        lists:foreach(
            fun({DbName, Ref}) ->
                receive
                {'DOWN', Ref, _, _, normal} ->
                    ok;
                {'DOWN', Ref, _, _, Reason} ->
                    etap:bail("Compaction for database " ++ ?b2l(DbName) ++
                        " failed: " ++ couch_util:to_list(Reason))
                after 90000 ->
                    etap:bail("Timeout waiting for compaction to finish for " ++
                        "database " ++ ?b2l(DbName))
                end
            end,
            MonRefs);
    false ->
        MonRefs
    end.


get_db_ref_counters(SetName, Partitions) ->
    Dbs0 = open_set_dbs(SetName, Partitions),
    {ok, MasterDb} = couch_db:open_int(
        <<SetName/binary, "/master">>, [admin_user_ctx()]),
    lists:map(
        fun(Db) ->
            ok = couch_db:close(Db),
            {couch_db:name(Db), Db#db.fd_ref_counter}
        end,
        [MasterDb | Dbs0]).


populate_set_alternated(SetName, Partitions, DocList) ->
    Dbs = open_set_dbs(SetName, Partitions),
    lists:foldl(
        fun(DocJson, I) ->
            Db = lists:nth(I + 1, Dbs),
            Doc = couch_doc:from_json_obj(DocJson),
            ok = couch_db:update_doc(Db, Doc, []),
            (I + 1) rem length(Dbs)
        end,
        0,
        DocList),
    lists:foreach(fun couch_db:close/1, Dbs).


populate_set_sequentially(SetName, Partitions, DocList) ->
    N = round(length(DocList) / length(Partitions)),
    populate_set_sequentially(SetName, Partitions, DocList, N).

populate_set_sequentially(_SetName, _Partitions, [], _N) ->
    ok;
populate_set_sequentially(SetName, [PartId | Rest], DocList, N) when N > 0 ->
    NumDocs = erlang:min(N, length(DocList)),
    case Rest of
    [] ->
        Docs = DocList,
        DocList2 = [];
    _ ->
        {Docs, DocList2} = lists:split(NumDocs, DocList)
    end,
    {ok, Db} = open_set_db(SetName, PartId),
    ok = couch_db:update_docs(Db, [couch_doc:from_json_obj(Doc) || Doc <- Docs], [sort_docs]),
    ok = couch_db:close(Db),
    populate_set_sequentially(SetName, Rest, DocList2, N).


update_ddoc(SetName, DDoc) ->
    DbName = iolist_to_binary([SetName, "/master"]),
    {ok, Db} = couch_db:open_int(DbName, [admin_user_ctx()]),
    ok = couch_db:update_doc(Db, couch_doc:from_json_obj(DDoc), []),
    ok = couch_db:close(Db).


delete_ddoc(SetName, DDocId) ->
    DbName = iolist_to_binary([SetName, "/master"]),
    {ok, Db} = couch_db:open_int(DbName, [admin_user_ctx()]),
    ok = couch_db:update_doc(Db, #doc{id = DDocId, deleted = true}, []),
    ok = couch_db:close(Db).


define_set_view(SetName, DDocId, NumPartitions, Active, Passive) ->
    Url = set_url(SetName, DDocId) ++ "_define",
    Def = {[
        {<<"number_partitions">>, NumPartitions},
        {<<"active_partitions">>, Active},
        {<<"passive_partitions">>, Passive},
        {<<"cleanup_partitions">>, []}
    ]},
    {ok, Code, _Headers, _Body} = test_util:request(
        Url, [{"Content-Type", "application/json"}],
        post, ejson:encode(Def)),
    case Code of
    201 ->
        ok;
    _ ->
        etap:bail("Error defining set view: " ++ integer_to_list(Code))
    end.


query_view(SetName, DDocId, ViewName) ->
    query_view(SetName, DDocId, ViewName, []).

query_view(SetName, DDocId, ViewName, QueryString) ->
    QueryUrl = set_view_url(SetName, DDocId, ViewName) ++
        case QueryString of
        [] ->
            [];
        _ ->
            "?" ++ QueryString
        end,
    {ok, Code, _Headers, Body} = test_util:request(QueryUrl, [], get),
    case Code of
    200 ->
        ok;
    _ ->
        io:format(standard_error, "~nView response body: ~p~n~n", [Body]),
        etap:bail("View response status is not 200 (got " ++
            integer_to_list(Code) ++ ")")
    end,
    {ok, ejson:decode(Body)}.


are_view_keys_sorted({ViewResult}, KeyCompFun) ->
    Rows = couch_util:get_value(<<"rows">>, ViewResult),
    case Rows of
    [] ->
        true;
    [First | Rest] ->
        {AreSorted, _} = lists:foldl(
           fun({Row}, {Sorted, {Prev}}) ->
              Key = couch_util:get_value(<<"key">>, Row),
              PrevKey = couch_util:get_value(<<"key">>, Prev),
              {Sorted andalso KeyCompFun(PrevKey, Key), {Row}}
           end,
           {true, First},
           Rest),
        AreSorted
    end.


set_url(SetName, DDocId) ->
    ?b2l(iolist_to_binary([
        "http://", get(addr), ":", get(port), "/", "_set_view", "/",
        SetName, "/", DDocId, "/"
    ])).


set_view_url(SetName, DDocId, ViewName) ->
    ?b2l(iolist_to_binary([
        "http://", get(addr), ":", get(port), "/", "_set_view", "/",
        SetName, "/", DDocId, "/", "_view", "/", ViewName
    ])).


open_set_dbs(SetName, Partitions) ->
    lists:map(
        fun(PartId) ->
            DbName = iolist_to_binary([
                SetName, $/, integer_to_list(PartId)
            ]),
            {ok, Db} = couch_db:open_int(DbName, [admin_user_ctx()]),
            Db
        end,
        Partitions).


open_set_db(SetName, master) ->
    {ok, _} = couch_db:open_int(?master_dbname(SetName), [admin_user_ctx()]);
open_set_db(SetName, PartId) ->
    {ok, _} = couch_db:open_int(?dbname(SetName, PartId), [admin_user_ctx()]).


get_db_main_pid(#db{main_pid = Pid}) ->
    Pid.


delete_set_db(SetName, master) ->
    ok = couch_server:delete(?master_dbname(SetName), [admin_user_ctx()]);
delete_set_db(SetName, PartId) ->
    ok = couch_server:delete(?dbname(SetName, PartId), [admin_user_ctx()]).


doc_count(SetName, Partitions) ->
    Dbs = open_set_dbs(SetName, Partitions),
    Count = lists:foldl(
        fun(#db{docinfo_by_id_btree = IdBtree}, Acc) ->
            {ok, <<Count:40, _DelCount:40, _Size:48>>} =
                    couch_btree:full_reduce(IdBtree),
            Acc + Count
        end,
        0, Dbs),
    lists:foreach(fun couch_db:close/1, Dbs),
    Count.


get_db_seqs(SetName, Partitions) ->
    Dbs = open_set_dbs(SetName, Partitions),
    {Seqs, []} = lists:foldl(
        fun(Db, {SeqsAcc, [PartId | Rest]}) ->
            { [{PartId, couch_db:get_update_seq(Db)} | SeqsAcc], Rest }
        end,
        {[], Partitions}, Dbs),
    lists:foreach(fun couch_db:close/1, Dbs),
    lists:reverse(Seqs).


full_reduce_id_btree(_Group, Btree) ->
    {ok, <<Count:40, Bitmap:?MAX_NUM_PARTITIONS, _/binary>>} = couch_btree:full_reduce(Btree),
    {ok, {Count, Bitmap}}.

full_reduce_view_btree(_Group, Btree) ->
    {ok, <<Count:40, Bitmap:?MAX_NUM_PARTITIONS, RedsBin/binary>>} = couch_btree:full_reduce(Btree),
    Reds = couch_set_view_util:parse_reductions(RedsBin),
    {ok, {Count, [?JSON_DECODE(R) || R <- Reds], Bitmap}}.

fold_id_btree(_Group, Btree, Fun, Acc, Args) ->
    FunWrap = fun ({Id, Value}, AccRed, Acc0) ->
        <<PartId:16, ValuesBin/binary>> = Value,
        Vals = couch_set_view_util:parse_view_id_keys(ValuesBin),
        ExpandedVals = lists:sort(lists:flatten([[{ViewId, ?JSON_DECODE(KeyVal)} || KeyVal <- KeysVal] || {ViewId,KeysVal} <- Vals])),
        Fun({Id, {PartId, ExpandedVals}}, AccRed, Acc0)
    end,
    couch_btree:fold(Btree, FunWrap, Acc, Args).

fold_view_btree(_Group, Btree, Fun, Acc, Args) ->
    FunWrap = fun({KeyDocId, <<PartId:16, ValsBin/binary>>}, AccRed, Acc0) ->
        case couch_set_view_util:parse_values(ValsBin) of
        [Val0] ->
            Val = ?JSON_DECODE(Val0);
        Vals ->
            Val = {dups, lists:sort([?JSON_DECODE(V) || V <- Vals])}
        end,
        {Key, Id} = couch_set_view_util:decode_key_docid(KeyDocId),
        Fun({{Key, Id}, {PartId, Val}}, AccRed, Acc0)
    end,
    couch_btree:fold(Btree, FunWrap, Acc, Args).
