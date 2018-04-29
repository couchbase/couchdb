#!/usr/bin/env escript
%% -*- Mode: Erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
%%! -smp enable

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

-include("../../couchdb/couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").


test_set_name() -> <<"couch_test_ryow_query">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 5000.

-define(TIMEOUT, 600000).

main(_) ->
    test_util:init_code_path(),

    etap:plan(1),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.

test() ->
    etap:diag("Testing stale=false query for RYOW property"),
    couch_set_view_test_util:start_server(test_set_name()),
    etap:diag("Adding documents with value = 0"),
    create_set(),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    add_documents(0, num_docs()),
    couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, ["stale=false"]),

    etap:diag("Updating documents with value = 1"),
    update_documents(0, num_docs(), fun(_I) -> 1 end),

    {ok, UpdaterPid} = gen_server:call(GroupPid, {start_updater, [pause]}, ?TIMEOUT),
    Parent = self(),
    QueryPid = spawn(fun() ->
        setup_query_env(),
        {ok, {ViewResults}} = couch_set_view_test_util:query_view(
            test_set_name(), ddoc_id(), <<"test">>, ["stale=false"]),
        Parent ! {query_response, self(), ViewResults}
        end),


    etap:diag("Updating same documents subsequently multiple times"),
    update_documents(0, num_docs(), fun(_I) -> 2 end),
    update_documents(0, num_docs(), fun(_I) -> 3 end),
    update_documents(0, num_docs(), fun(_I) -> 4 end),


    % Atleast two snapshots are required for updater to do checkpointing
    couch_dcp_fake_server:set_items_per_snapshot(2),

    case UpdaterPid of
    nil ->
        ok;
    _ ->
        UpdaterPid ! continue
    end,

    receive
    {query_response, QueryPid, ViewResults} ->
        verify_view_results(ViewResults)
    after ?TIMEOUT ->
        etap:bail("Timed out waiting for stale=false query results")
    end,

    shutdown_group(),
    couch_set_view_test_util:stop_server(),
    ok.


setup_query_env() ->
    case misc:is_ipv6() of
        false ->
            put(addr, couch_config:get("httpd", "ip4_bind_address", "127.0.0.1"));
        true ->
            Ip6Addr = couch_config:get("httpd", "ip6_bind_address", "::1"),
            put(addr, "[" ++ Ip6Addr ++ "]")
    end,
    put(port, integer_to_list(mochiweb_socket_server:get(couch_httpd, port))),
    ok.


verify_view_results(ViewResults) ->
    Rows = couch_util:get_value(<<"rows">>, ViewResults),
    FoundOldValue = lists:any(fun({Row}) ->
        Val = couch_util:get_value(<<"value">>, Row),
        Val =:= 0
    end, Rows),
    etap:is(FoundOldValue, false,
        "Expected each row value in stale=false query to be 1,2,3 or 4").


create_set() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view:cleanup_index_files(mapreduce_view, test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>}
                ]}}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag(io_lib:format(
        "Configuring set view with partitions [0 .. ~p] as active",
        [num_set_partitions() - 1])),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, num_set_partitions() - 1),
        passive_partitions = [],
        use_replica_index = false
    },
    ok = couch_set_view:define_group(
        mapreduce_view, test_set_name(), ddoc_id(), Params).


add_documents(StartId, Count) ->
    add_documents(StartId, Count, fun(_I) -> 0 end).

add_documents(StartId, Count, ValueGenFun) ->
    etap:diag("Adding " ++ integer_to_list(Count) ++ " new documents"),
    DocList0 = lists:map(
        fun(I) ->
            {I rem num_set_partitions(), {[
                {<<"meta">>, {[{<<"id">>, doc_id(I)}]}},
                {<<"json">>, {[
                    {<<"value">>, ValueGenFun(I)}
                ]}}
            ]}}
        end,
        lists:seq(StartId, StartId + Count - 1)),
    DocList = [Doc || {_, Doc} <- lists:keysort(1, DocList0)],
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


update_documents(StartId, NumDocs, ValueGenFun) ->
    etap:diag("About to update " ++ integer_to_list(NumDocs) ++ " documents"),
    Dbs = dict:from_list(lists:map(
        fun(I) ->
            {ok, Db} = couch_set_view_test_util:open_set_db(test_set_name(), I),
            {I, Db}
        end,
        lists:seq(0, num_set_partitions() - 1))),
    Docs = lists:foldl(
        fun(I, Acc) ->
            Doc = couch_doc:from_json_obj({[
                {<<"meta">>, {[{<<"id">>, doc_id(I)}]}},
                {<<"json">>, {[
                    {<<"value">>, ValueGenFun(I)}
                ]}}
            ]}),
            DocList = case orddict:find(I rem num_set_partitions(), Acc) of
            {ok, L} ->
                L;
            error ->
                []
            end,
            orddict:store(I rem num_set_partitions(), [Doc | DocList], Acc)
        end,
        orddict:new(), lists:seq(StartId, StartId + NumDocs - 1)),
    [] = orddict:fold(
        fun(I, DocList, Acc) ->
            Db = dict:fetch(I, Dbs),
            ok = couch_db:update_docs(Db, DocList, [sort_docs]),
            Acc
        end,
        [], Docs),
    etap:diag("Updated " ++ integer_to_list(NumDocs) ++ " documents"),
    ok = lists:foreach(fun({_, Db}) -> ok = couch_db:close(Db) end, dict:to_list(Dbs)).


doc_id(I) ->
    iolist_to_binary(io_lib:format("doc_~8..0b", [I])).


shutdown_group() ->
    couch_dcp_fake_server:reset(),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    MonRef = erlang:monitor(process, GroupPid),
    receive
    {'DOWN', MonRef, _, _, _} ->
        ok
    after 10000 ->
        etap:bail("Timeout waiting for group shutdown")
    end.
