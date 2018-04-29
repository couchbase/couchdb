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

-define(MAX_WAIT_TIME, 60000).

test_set_name() -> <<"couch_test_set_index_query_fdleaks">>.
num_set_partitions() -> 8.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 8000.


main(_) ->
    test_util:init_code_path(),

    etap:plan(6),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    couch_set_view_test_util:start_server(test_set_name()),
    etap:diag("Checking view with only map function for leaks"),
    test_group_refleaks(map),
    etap:diag("Checking view with reducer function for leaks"),
    test_group_refleaks(reduce),
    couch_set_view_test_util:stop_server(),
    ok.

test_group_refleaks(ViewType) ->
    create_set(lists:seq(0, num_set_partitions() - 1), [], ViewType),
    ValueGenFun1 = fun(I) -> I end,
    update_documents(0, num_docs(), ValueGenFun1),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    trigger_initial_build(),
    Msg = case ViewType of
    reduce ->
        io_lib:format("Group with reducer function is alive" ,[]);
    map ->
        io_lib:format("Group with only map function is alive", [])
    end,
    etap:is(is_process_alive(GroupPid), true, Msg),
    check_refcount_leaks(GroupPid),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


trigger_initial_build() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, _, _} = gen_server:call(
        GroupPid, #set_view_group_req{stale = false, debug = true}, ?MAX_WAIT_TIME).


check_refcount_leaks(GroupPid) ->
    RefCounter = get_group_refcounter(GroupPid),
    RefCount = couch_ref_counter:count(RefCounter),
    etap:is(is_process_alive(RefCounter), true, "RefCounter is alive"),

    etap:diag("Querying view"),
    query_view(),

    RefCount2 = couch_ref_counter:count(RefCounter),
    etap:is(RefCount, RefCount2, "Reference count for view group as expected after query").


query_view() ->
    {ok, _} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>).


create_set(ActiveParts, PassiveParts, ViewType) ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view:cleanup_index_files(mapreduce_view, test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    ViewFns = [{<<"map">>, <<"function(doc, meta) { emit(doc.value, meta.id); }">>}],
    ViewFns2 = case ViewType of
    reduce ->
        ViewFns ++ [{<<"reduce">>, <<"_count">>}];
    map ->
        ViewFns
    end,

    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"language">>, <<"javascript">>},
            {<<"views">>, {[
                {<<"test">>, {ViewFns2}}
            ]}}
        ]}}
    ]},

    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag("Configuring set view with partitions [0 .. 31]"
              " as active and [32 .. 47] as passive"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = ActiveParts,
        passive_partitions = PassiveParts,
        use_replica_index = true
    },
    ok = couch_set_view:define_group(
        mapreduce_view, test_set_name(), ddoc_id(), Params).


update_documents(StartId, Count, ValueGenFun) ->
    etap:diag("Updating " ++ integer_to_list(Count) ++ " new documents"),
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


doc_id(I) ->
    iolist_to_binary(io_lib:format("doc_~8..0b", [I])).


get_group_refcounter(GroupPid) ->
    {ok, Group} = gen_server:call(GroupPid, request_group, infinity),
    Group#set_view_group.ref_counter.
