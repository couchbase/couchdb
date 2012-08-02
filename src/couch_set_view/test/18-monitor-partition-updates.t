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

test_set_name() -> <<"couch_test_set_monitor_partition_updates">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 8000.  % keep it a multiple of num_set_partitions()

-define(etap_match(Got, Expected, Desc),
        etap:fun_is(fun(XXXXXX) ->
            case XXXXXX of Expected -> true; _ -> false end
        end, Got, Desc)).


main(_) ->
    test_util:init_code_path(),

    etap:plan(8),
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

    create_set(),
    ValueGenFun1 = fun(I) -> I end,
    update_documents(0, num_docs(), ValueGenFun1),

    Ref1 = couch_set_view:monitor_partition_update(test_set_name(), ddoc_id(), 10),
    Msg1 = receive
    {Ref1, _} ->
        ok
    after 1000 ->
        undefined
    end,
    etap:is(Msg1, undefined, "Didn't got any partition 10 updated notification"),

    Ref2 = couch_set_view:monitor_partition_update(test_set_name(), ddoc_id(), 33),
    Msg2 = receive
    {Ref2, _} ->
        ok
    after 1000 ->
        undefined
    end,
    etap:is(Msg2, undefined, "Didn't got any partition 33 updated notification"),

    Ref3 = try
        couch_set_view:monitor_partition_update(test_set_name(), ddoc_id(), 60)
    catch throw:Error ->
        Error
    end,

    ?etap_match(Ref3, {error, _},
                "Got error when asking to monitor partition 60 update"
                " (not in active nor passive set)"),

    % build index
    _ = get_group_snapshot(),

    GotPart10Notify = receive
    {Ref1, updated} ->
        true
    after 5000 ->
        false
    end,
    etap:is(GotPart10Notify, true, "Got update notification for partition 10"),

    GotPart33Notify = receive
    {Ref2, updated} ->
        true
    after 5000 ->
        false
    end,
    etap:is(GotPart33Notify, true, "Got update notification for partition 33"),

    Ref4 = try
        couch_set_view:monitor_partition_update(test_set_name(), ddoc_id(), 127)
    catch throw:Error2 ->
        Error2
    end,

    wait_updater_finished(),

    ?etap_match(Ref4, {error, _},
                "Got error when asking to monitor partition 127 update"
                " (not in range 0 .. 63)"),

    Ref5 = couch_set_view:monitor_partition_update(test_set_name(), ddoc_id(), 12),
    GotPart12ImmediateNotification = receive
    {Ref5, updated} ->
        true
    after 0 ->
        false
    end,
    etap:is(GotPart12ImmediateNotification, true,
            "Got immediate notification for partition 12"),

    update_documents(num_docs(), num_set_partitions(), ValueGenFun1),

    Ref6 = couch_set_view:monitor_partition_update(test_set_name(), ddoc_id(), 20),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 20),

    GotPart20CleanupNotify = receive
    {Ref6, marked_for_cleanup} ->
        true
    after 3000 ->
        false
    end,
    etap:is(GotPart20CleanupNotify, true, "Got cleanup notification for partition 20"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


get_group_snapshot() ->
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    {ok, Group, 0} = gen_server:call(
        GroupPid, #set_view_group_req{stale = false}, infinity),
    Group.


create_set() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view:cleanup_index_files(test_set_name()),
    etap:diag("Creating the set databases (# of partitions: " ++
        integer_to_list(num_set_partitions()) ++ ")"),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"view_1">>, {[
                {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                {<<"reduce">>, <<"_sum">>}
            ]}}
        ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    etap:diag("Configuring set view with partitions [0 .. 31]"
              " as active and [32 .. 47] as passive"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, 31),
        passive_partitions = lists:seq(32, 47),
        use_replica_index = false
    },
    ok = couch_set_view:define_group(test_set_name(), ddoc_id(), Params).


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


wait_updater_finished() ->
    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    {ok, UpPid} = gen_server:call(GroupPid, updater_pid),
    case is_pid(UpPid) of
    true ->
        Ref = erlang:monitor(process, UpPid),
        receive
        {'DOWN', Ref, process, UpPid, _} ->
            ok
        after 320000 ->
            etap:diag("Timeout waiting for updater to finish")
        end;
    false ->
        ok
    end.
