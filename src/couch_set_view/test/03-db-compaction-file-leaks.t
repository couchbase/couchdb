#!/usr/bin/env escript
%% -*- erlang -*-
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

-define(b2l(B), binary_to_list(B)).

test_set_name() -> <<"couch_test_set_index_compaction">>.
num_set_partitions() -> 8.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 8000.


main(_) ->
    test_util:init_code_path(),

    etap:plan(25),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    couch_set_view_test_util:start_server(),
    ok = couch_config:set("set_views", "update_interval", "0", false),

    create_set(lists:seq(0, num_set_partitions() - 1), []),
    ValueGenFun1 = fun(I) -> I end,
    update_documents(0, num_docs(), ValueGenFun1),

    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    etap:is(is_process_alive(GroupPid), true, "Group is alive"),

    etap:diag("Querying view before database compactions"),
    {ok, ViewResults} = query_view(num_docs()),

    DbRefCounters = couch_set_view_test_util:get_db_ref_counters(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1)),

    ok = couch_set_view_test_util:compact_set_dbs(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        true),

    ok = timer:sleep(3000),

    etap:is(is_process_alive(GroupPid), true,
        "Group is alive after database compactions"),

    DbRefCountersAfter = couch_set_view_test_util:get_db_ref_counters(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1)),

    lists:foreach(
        fun({{DbName, OldRefCounter}, {DbName, NewRefCounter}}) ->
            etap:isnt(NewRefCounter, OldRefCounter,
                "Database " ++ ?b2l(DbName) ++ " has a new ref counter"),
            etap:is(is_process_alive(OldRefCounter), false,
                "Database " ++ ?b2l(DbName) ++ " old ref counter is dead")
        end,
        lists:zip(DbRefCounters, DbRefCountersAfter)),

    etap:diag("Querying view after database compactions"),
    {ok, ViewResults2} = query_view(num_docs()),

    etap:is(ViewResults2, ViewResults,
        "Same view results after database compactions"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:stop_server(),
    ok.


query_view(ExpectedRowCount) ->
    {ok, {ViewResults}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>),
    etap:is(
        length(couch_util:get_value(<<"rows">>, ViewResults)),
        ExpectedRowCount,
        "Got " ++ integer_to_list(ExpectedRowCount) ++ " view rows"),
    SortedKeys =  couch_set_view_test_util:are_view_keys_sorted(
        {ViewResults}, fun(A, B) -> A < B end),
    etap:is(SortedKeys, true, "View result keys are sorted"),
    {ok, {ViewResults}}.


create_set(ActiveParts, PassiveParts) ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view:cleanup_index_files(test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    DDoc = {[
        {<<"_id">>, ddoc_id()},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc.value, doc._id); }">>}
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
    ok = couch_set_view:define_group(test_set_name(), ddoc_id(), Params).


update_documents(StartId, Count, ValueGenFun) ->
    etap:diag("Updating " ++ integer_to_list(Count) ++ " new documents"),
    DocList0 = lists:map(
        fun(I) ->
            {I rem num_set_partitions(), {[
                {<<"_id">>, doc_id(I)},
                {<<"value">>, ValueGenFun(I)}
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
