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


-define(MAX_WAIT_TIME, 10 * 1000).
test_set_name() -> <<"couch_test_dcp_rollback">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 1024.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(16),
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
    test_index_post_rollback(),
    couch_set_view_test_util:stop_server(),
    ok.

test_index_post_rollback() ->
    etap:diag("Testing initial index build post rollback to sequence no 0"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    setup_test(),
    {auth, User, Passwd} = cb_auth_info:get(),
    {ok, _Pid} = couch_dcp_client:start(
        test_set_name(), test_set_name(), User, Passwd, 1024, 0),

    {ok, MainGroupInfo} = couch_set_view:get_group_info(
        mapreduce_view, test_set_name(), ddoc_id(), prod),

    {RepGroupInfo} = couch_util:get_value(replica_group_info, MainGroupInfo),

    etap:is(
        couch_util:get_value(active_partitions, MainGroupInfo),
        lists:seq(0, num_set_partitions() div 2 - 1),
        "Main group has [ 0 .. 7 ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, MainGroupInfo),
        [],
        "Main group has [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, MainGroupInfo),
        [],
        "Main group has [ ] as cleanup partitions"),
    etap:is(
        couch_util:get_value(replica_partitions, MainGroupInfo),
        [],
        "Main group has [ ] as replica partitions"),
    etap:is(
        couch_util:get_value(replicas_on_transfer, MainGroupInfo),
        [],
        "Main group has [ ] as replicas on transfer"),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo),
        [],
        "Replica group has [ ] as passive partitions"),

    etap:diag("Marking partitions [ 8 .. 15 ] as replicas"),
    ok = couch_set_view:add_replica_partitions(
        mapreduce_view, test_set_name(), ddoc_id(),
        lists:seq(num_set_partitions() div 2, num_set_partitions() - 1)),

    MainGroupInfo1 = get_group_info(),
    {RepGroupInfo1} = couch_util:get_value(replica_group_info, MainGroupInfo1),

    etap:diag("Verifying main and replica group infos again"),
    etap:is(
        couch_util:get_value(active_partitions, MainGroupInfo1),
        lists:seq(0, num_set_partitions() div 2 - 1),
        "Main group has [ 0 .. 7 ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, MainGroupInfo1),
        [],
        "Main group has [ ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, MainGroupInfo1),
        [],
        "Main group has [ ] as cleanup partitions"),
    etap:is(
        couch_util:get_value(replica_partitions, MainGroupInfo1),
        lists:seq(num_set_partitions() div 2, num_set_partitions() - 1),
        "Main group has [ 8 .. 15] as replica partitions"),
    etap:is(
        couch_util:get_value(replicas_on_transfer, MainGroupInfo1),
        [],
        "Main group has [ ] as replicas on transfer"),
    etap:is(
        couch_util:get_value(active_partitions, RepGroupInfo1),
        [],
        "Replica group has [ ] as active partitions"),
    etap:is(
        couch_util:get_value(passive_partitions, RepGroupInfo1),
        lists:seq(num_set_partitions() div 2, num_set_partitions() - 1),
        "Replica group has [ 8 .. 15 ] as passive partitions"),
    etap:is(
        couch_util:get_value(cleanup_partitions, RepGroupInfo1),
        [],
        "Replica group has [ ] as cleanup partitions"),

    % To allow loading of non-zero persisted sequence numbers
    couch_dcp_fake_server:set_persisted_items_fun(
        fun(Seq) -> max(Seq - 1, 1) end),

    % Update the index, so that there are header to roll back to
    {ok, {_ViewResults1}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, []),

    etap:diag("Marking partitions [ 8 .. 15 ] as active"),
    lists:foreach(
       fun(I) ->
             ok = couch_set_view:set_partition_states(
                 mapreduce_view, test_set_name(), ddoc_id(), [I], [], [])
         end,
         lists:seq(num_set_partitions() div 2, num_set_partitions() - 1)),

    etap:diag("Rolling back partition 0 to seq no 0"),
    rollback_different_heads(0),

    shutdown_group(),
    ok.


rollback_different_heads(PartId) ->
    FailoverLog = get_group_failover_log(PartId),

    etap:diag(io_lib:format("PartId: ~w FailoverLog Dump: ~w~n",
        [PartId, FailoverLog])),

    FailoverLog2 = [{1000 + 100 * PartId + 1, 0}] ++ FailoverLog,
    couch_dcp_fake_server:set_failover_log(PartId, FailoverLog2),

    populate_set(1, 4 * num_docs()),
    {ok, {_ViewResults3}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, []),

    FailoverLog3 = get_group_failover_log(PartId),

    etap:is(FailoverLog3, FailoverLog2, "Failover log looks good post rollback").

configure_view_group() ->
    etap:diag("Configuring view group"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, num_set_partitions() div 2 - 1),
        passive_partitions = [],
        use_replica_index = true
    },

    try
        couch_set_view:define_group(
            mapreduce_view, test_set_name(), ddoc_id(), Params)
    catch _:Error ->
        Error
    end.

shutdown_group() ->
    couch_dcp_fake_server:reset(),
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    couch_set_view_test_util:wait_for_updater_to_finish(GroupPid, ?MAX_WAIT_TIME),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    MonRef = erlang:monitor(process, GroupPid),
    receive
    {'DOWN', MonRef, _, _, _} ->
        ok
    after 10000 ->
        etap:bail("Timeout waiting for group shutdown")
    end.

setup_test() ->
    populate_set(1, 4 * num_docs()),

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
    ok = configure_view_group().

populate_set(From, To) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    DocList = create_docs(From, To),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).

doc_id(I) ->
    iolist_to_binary(io_lib:format("doc_~8..0b", [I])).

create_docs(From, To) ->
    lists:map(
        fun(I) ->
            Cas = I,
            ExpireTime = 0,
            Flags = 0,
            RevMeta1 = <<Cas:64/native, ExpireTime:32/native, Flags:32/native>>,
            RevMeta2 = [[io_lib:format("~2.16.0b",[X]) || <<X:8>> <= RevMeta1 ]],
            RevMeta3 = iolist_to_binary(RevMeta2),
            {[
              {<<"meta">>, {[
                             {<<"id">>, doc_id(I)},
                             {<<"rev">>, <<"1-", RevMeta3/binary>>}
                            ]}},
              {<<"json">>, {[{<<"value">>, I}]}}
            ]}
        end,
        lists:seq(From, To)).

get_group_info() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, GroupInfo} = couch_set_view_group:request_group_info(GroupPid),
    GroupInfo.

get_group_failover_log(PartId) ->
    GroupInfo = get_group_info(),
    {partition_versions, {PartVersions0}} = lists:keyfind(
        partition_versions, 1, GroupInfo),
    PartVersions = lists:map(fun({PartId0, PartVersion}) ->
        {list_to_integer(binary_to_list(PartId0)),
            [list_to_tuple(V) || V <- PartVersion]}
    end, PartVersions0),
    {PartId, FailoverLog} = lists:keyfind(PartId, 1, PartVersions),
    FailoverLog.
