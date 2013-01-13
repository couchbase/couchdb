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

-define(JSON_ENCODE(V), ejson:encode(V)). % couch_db.hrl
-define(MAX_WAIT_TIME, 900 * 1000).

-include_lib("couch_set_view/include/couch_set_view.hrl").

test_set_name() -> <<"couch_test_set_index_util_stats">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs_0() -> 78144.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(24),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            io:format(standard_error, "Test died abnormally: ~p", [Other]),
            etap:bail(Other)
    end,
    ok.


test() ->
    couch_set_view_test_util:start_server(test_set_name()),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    create_set(),

    ValueGenFun1 = fun(I) -> I end,
    update_documents(0, num_docs_0(), ValueGenFun1),

    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),

    {ok, Stats0} = couch_set_view:get_utilization_stats(test_set_name(), ddoc_id()),
    etap:is(couch_util:get_value(updates, Stats0), 0,
            "Right number of updates"),
    etap:is(couch_util:get_value(updater_interruptions, Stats0), 0,
            "Right number of updater interruptions"),
    etap:is(couch_util:get_value(useful_indexing_time, Stats0), 0.0,
            "Right useful indexing time"),
    etap:is(couch_util:get_value(wasted_indexing_time, Stats0), 0.0,
            "Right wasted indexing time"),
    etap:is(couch_util:get_value(compactions, Stats0), 0,
            "Right number of compactions"),
    etap:is(couch_util:get_value(compactor_interruptions, Stats0), 0,
            "Right number of compactor interruptions"),
    etap:is(couch_util:get_value(compaction_time, Stats0), 0.0,
            "Right compaction time"),

    {ok, UpdaterPid1} = gen_server:call(GroupPid, {start_updater, []}, infinity),
    Ref1 = erlang:monitor(process, UpdaterPid1),
    etap:diag("Waiting for updater to finish"),
    receive
    {'DOWN', Ref1, _, _, {updater_finished, _}} ->
        etap:diag("Updater finished");
    {'DOWN', Ref1, _, _, Reason1} ->
        etap:bail("Updater finished with unexpected reason: " ++ couch_util:to_list(Reason1))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for updater to finish")
    end,

    {ok, CompactPid1} = couch_set_view_compactor:start_compact(test_set_name(), ddoc_id(), main),
    Ref2 = erlang:monitor(process, CompactPid1),
    etap:diag("Waiting for main group compaction to finish"),
    receive
    {'DOWN', Ref2, _, _, normal} ->
        ok;
    {'DOWN', Ref2, _, _, noproc} ->
        ok;
    {'DOWN', Ref2, _, _, Reason2} ->
        etap:bail("Failure compacting main group: " ++ couch_util:to_list(Reason2))
    after ?MAX_WAIT_TIME ->
        etap:bail("Timeout waiting for main group compaction to finish")
    end,

    update_documents(num_docs_0(), 64, ValueGenFun1),

    {ok, UpdaterPid2} = gen_server:call(GroupPid, {start_updater, [pause]}, infinity),
    Ref3 = erlang:monitor(process, UpdaterPid2),

    etap:diag("Marking partition 0 for cleanup while updater is running"),
    ok = couch_set_view:set_partition_states(test_set_name(), ddoc_id(), [], [], [0]),

    etap:diag("Waiting for updater to be shutdown"),
    receive
    {'DOWN', Ref3, _, _, {updater_error, shutdown}} ->
        etap:diag("Updater shutdown")
    after 5000 ->
        etap:bail("Timeout waiting for updater to shutdown")
    end,

    {ok, UpdaterPid3} = gen_server:call(GroupPid, {start_updater, []}, infinity),
    case is_pid(UpdaterPid3) of
    true ->
        Ref4 = erlang:monitor(process, UpdaterPid3),
        etap:diag("Waiting for new updater to finish"),
        receive
        {'DOWN', Ref4, _, _, {updater_finished, _}} ->
            etap:diag("Updater finished");
        {'DOWN', Ref4, _, _, Reason4} ->
            etap:bail("Updater finished with unexpected reason: " ++ couch_util:to_list(Reason4))
        after ?MAX_WAIT_TIME ->
            etap:bail("Timeout waiting for new updater to finish")
        end;
    false ->
        ok
    end,

    {ok, CompactPid2} = couch_set_view_compactor:start_compact(test_set_name(), ddoc_id(), main),
    CompactPid2 ! pause,
    Ref5 = erlang:monitor(process, CompactPid2),
    etap:diag("Marking partition 1 for cleanup while compactor is running"),
    ok = couch_set_view:set_partition_states(test_set_name(), ddoc_id(), [], [], [1]),

    etap:diag("Waiting for main group compaction to shutdown"),
    receive
    {'DOWN', Ref5, _, _, shutdown} ->
        ok;
    {'DOWN', Ref5, _, _, Reason5} ->
        etap:bail("Failure compacting main group: " ++ couch_util:to_list(Reason5))
    after 5000 ->
        etap:bail("Timeout waiting for main group compaction to shutdown")
    end,

    {ok, Stats2} = couch_set_view:get_utilization_stats(test_set_name(), ddoc_id()),
    etap:is(couch_util:get_value(updates, Stats2), 2,
            "Right number of updates"),
    etap:is(couch_util:get_value(updater_interruptions, Stats2), 1,
            "Right number of updater interruptions"),
    etap:is(is_float(couch_util:get_value(useful_indexing_time, Stats2)), true,
            "Useful indexing time is a float"),
    etap:is(couch_util:get_value(useful_indexing_time, Stats2) > 0, true,
            "Useful indexing time greater than zero"),
    etap:is(is_float(couch_util:get_value(wasted_indexing_time, Stats2)), true,
            "Wasted indexing time is a float"),
    etap:is(couch_util:get_value(wasted_indexing_time, Stats2) > 0, true,
            "Wasted indexing time greater than zero"),
    etap:is(couch_util:get_value(compactions, Stats2), 1,
            "Right number of compactions"),
    etap:is(couch_util:get_value(compactor_interruptions, Stats2), 1,
            "Right number of compactor interruptions"),
    etap:is(is_float(couch_util:get_value(compaction_time, Stats2)), true,
            "Compaction time is a float"),
    etap:is(couch_util:get_value(compaction_time, Stats2) > 0, true,
            "Compaction time is greater than zero"),

    etap:diag("Reseting stats"),
    ok = couch_set_view:reset_utilization_stats(test_set_name(), ddoc_id()),
    {ok, Stats3} = couch_set_view:get_utilization_stats(test_set_name(), ddoc_id()),
    etap:is(couch_util:get_value(updates, Stats3), 0,
            "Right number of updates"),
    etap:is(couch_util:get_value(updater_interruptions, Stats3), 0,
            "Right number of updater interruptions"),
    etap:is(couch_util:get_value(useful_indexing_time, Stats3), 0.0,
            "Right useful indexing time"),
    etap:is(couch_util:get_value(wasted_indexing_time, Stats3), 0.0,
            "Right wasted indexing time"),
    etap:is(couch_util:get_value(compactions, Stats3), 0,
            "Right number of compactions"),
    etap:is(couch_util:get_value(compactor_interruptions, Stats3), 0,
            "Right number of compactor interruptions"),
    etap:is(couch_util:get_value(compaction_time, Stats3), 0.0,
            "Right compaction time"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


create_set() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view:cleanup_index_files(test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
        {<<"views">>, {[
            {<<"view_1">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                {<<"reduce">>, <<"_count">>}
            ]}}
        ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag("Configuring set view with partitions [0 .. 63] as active"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, num_set_partitions() - 1),
        passive_partitions = [],
        use_replica_index = false
    },
    ok = couch_set_view:define_group(test_set_name(), ddoc_id(), Params).


update_documents(StartId, Count, ValueGenFun) ->
    etap:diag("Updating " ++ integer_to_list(Count) ++ " documents"),
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
