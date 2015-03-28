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

-include_lib("couch_set_view/include/couch_set_view.hrl").


test_set_name() -> <<"couch_test_dcp_duplicates">>.
num_set_partitions() -> 1.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 2000.

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
    %init:stop(),
    %receive after infinity -> ok end,
    ok.

test() ->
    etap:diag("Testing that there are no duplicates in the final output "
        "(MB-13160)"),
    couch_set_view_test_util:start_server(test_set_name()),
    create_set(),

    etap:diag("Insert documents within two large snapshots without any "
        "overlapping document IDs"),
    update_documents(0, num_docs(), fun(_I) -> 1 end),
    {ok, {ViewResults1}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, ["stale=false"]),

    etap:diag("Update documents within several snapshots, where each "
        "snapshot contains some document IDs from a previous one"),
    couch_dcp_fake_server:set_items_per_snapshot(1000),
    couch_dcp_fake_server:set_dups_per_snapshot(10),
    % Use the same value as previously, so that the view results stay
    % the same
    update_documents(0, num_docs(), fun(_I) -> 1 end),
    {ok, {ViewResults2}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, ["stale=false"]),

    etap:is(ViewResults2, ViewResults1,
        "The results of disjoint snapshots is the same as with multiple "
        "snapshots containing duplicates"),

    shutdown_group(),
    couch_set_view_test_util:stop_server(),
    ok.

create_set() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(),
        num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(),
        num_set_partitions()),
    couch_set_view:cleanup_index_files(mapreduce_view, test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>,
                        <<"function(doc, meta) {emit(meta.id, doc.value);}">>}
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


update_documents(StartId, NumDocs, ValueGenFun) ->
    etap:diag("About to update " ++ integer_to_list(NumDocs) ++ " documents"),
    Dbs = dict:from_list(lists:map(
        fun(I) ->
            {ok, Db} = couch_set_view_test_util:open_set_db(
                test_set_name(), I),
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
    ok = lists:foreach(fun({_, Db}) ->
        ok = couch_db:close(Db)
    end, dict:to_list(Dbs)).


doc_id(I) ->
    iolist_to_binary(io_lib:format("doc_~8..0b", [I])).


shutdown_group() ->
    couch_dcp_fake_server:reset(),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    couch_set_view_test_util:delete_set_dbs(test_set_name(),
        num_set_partitions()),
    MonRef = erlang:monitor(process, GroupPid),
    receive
    {'DOWN', MonRef, _, _, _} ->
        ok
    after 10000 ->
        etap:bail("Timeout waiting for group shutdown")
    end.
