#!/usr/bin/env escript
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


% from couch_set_view.hrl
-record(set_view_params, {
    max_partitions = 0,
    active_partitions = [],
    passive_partitions = [],
    use_replica_index = false
}).

-define(etap_match(Got, Expected, Desc),
        etap:fun_is(fun(XXXXXX) ->
            case XXXXXX of Expected -> true; _ -> false end
        end, Got, Desc)).

test_set_name() -> <<"couch_test_set_index_shutdown">>.
num_set_partitions() -> 8.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 8000.


main(_) ->
    test_util:init_code_path(),

    etap:plan(26),
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

    test_partition_deletes_when_group_is_alive(),
    test_partition_not_found_when_group_is_configured(),
    test_partition_not_found_when_group_starts(),

    couch_set_view_test_util:stop_server(),
    ok.


test_partition_deletes_when_group_is_alive() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    populate_set(),
    configure_view_group([0, 1, 2, 3], [4, 5]),

    GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),

    IndexFile = group_index_file(),
    etap:is(filelib:is_file(IndexFile), true, "Index file exists"),

    query_view(4000, []),
    etap:is(is_process_alive(GroupPid), true, "Group alive after query"),

    etap:is(filelib:is_file(IndexFile), true, "Index file exists"),

    {ok, Db6} = couch_set_view_test_util:open_set_db(test_set_name(), 6),
    {ok, Db7} = couch_set_view_test_util:open_set_db(test_set_name(), 7),
    ok = couch_db:close(Db6),
    ok = couch_db:close(Db7),
    Db6Pid = couch_set_view_test_util:get_db_main_pid(Db6),
    Db7Pid = couch_set_view_test_util:get_db_main_pid(Db7),
    etap:is(is_process_alive(Db6Pid), true, "Partition 7 database is alive"),
    etap:is(is_process_alive(Db7Pid), true, "Partition 8 database is alive"),

    etap:diag("Deleting databases of partitions 7 and 8"),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 6),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 7),
    ok = timer:sleep(1000),
    etap:is(is_process_alive(Db6Pid), false, "Partition 7 database is not alive"),
    etap:is(is_process_alive(Db7Pid), false, "Partition 8 database is not alive"),

    etap:is(is_process_alive(GroupPid), true, "Group is still alive"),
    etap:is(filelib:is_file(IndexFile), true, "Index file exists"),

    etap:diag("Marking set view partition 4 for cleanup"),
    ok = couch_set_view_group:set_state(GroupPid, [], [], [3]),
    query_view(3000, []),

    {ok, Db3} = couch_set_view_test_util:open_set_db(test_set_name(), 3),
    ok = couch_db:close(Db3),
    Db3Pid = couch_set_view_test_util:get_db_main_pid(Db3),
    etap:is(is_process_alive(Db3Pid), true, "Partition 4 database is alive"),

    etap:diag("Deleting database of partition 4"),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 3),
    ok = timer:sleep(1000),
    etap:is(is_process_alive(Db3Pid), false, "Partition 4 database is not alive"),

    etap:is(is_process_alive(GroupPid), true, "Group is still alive"),
    etap:is(filelib:is_file(IndexFile), true, "Index file exists"),

    query_view(3000, []),

    {ok, Db1} = couch_set_view_test_util:open_set_db(test_set_name(), 1),
    ok = couch_db:close(Db1),
    Db1Pid = couch_set_view_test_util:get_db_main_pid(Db1),
    etap:is(is_process_alive(Db1Pid), true, "Partition 2 database is alive"),

    etap:diag("Deleting database of partition 2"),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 1),
    ok = timer:sleep(1000),
    etap:is(is_process_alive(Db1Pid), false, "Partition 2 database is not alive"),

    etap:is(is_process_alive(GroupPid), false, "Group is not alive anymore"),
    etap:is(filelib:is_file(IndexFile), false, "Index file does not exist anymore"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_partition_not_found_when_group_is_configured() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    populate_set(),

    etap:diag("Deleting database of partition 1 before configuring view group"),
    ok = couch_set_view_test_util:delete_set_db(test_set_name(), 0),

    ConfigError = configure_view_group([0, 1, 2, 3], [4, 5]),
    ?etap_match(
        ConfigError,
        {error, {db_open_error, _DbName, {not_found, no_db_file}, _Text}},
        "Got an error when configuring view group"),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_partition_not_found_when_group_starts() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    populate_set(),

    ok = configure_view_group([0, 1, 2, 3], [4, 5]),
    GroupPid1 = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
    couch_util:shutdown_sync(GroupPid1),
    couch_util:shutdown_sync(whereis(couch_server)),

    etap:diag("Deleting database of active partition 1 after view group shutdown"),
    DbFile = iolist_to_binary([test_set_name(), "/0.couch.1"]),
    DbDir = couch_config:get("couchdb", "database_dir"),
    ok = file:delete(filename:join([DbDir, DbFile])),
    ok = timer:sleep(1000),

    SetViewServerBefore = whereis(couch_set_view),
    MonRef = erlang:monitor(process, SetViewServerBefore),

    try
        couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
        etap:bail("No failure opening view group after deleting an active partition database")
    catch _:Error ->
        DbName = iolist_to_binary([test_set_name(), "/0"]),
        ?etap_match(
            Error,
            {error, {db_open_error, DbName, {not_found, no_db_file}, _Text}},
            "Got an error when opening view group")
    end,

    receive
    {'DOWN', MonRef, _, _, _} ->
        etap:bail("set_view server died")
    after 5000 ->
        ok
    end,

    SetViewServerAfter = whereis(couch_set_view),
    etap:is(SetViewServerAfter, SetViewServerBefore, "couch_set_view server didn't die"),
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


query_view(ExpectedRowCount, QueryString) ->
    {ok, {ViewResults}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, QueryString),
    etap:is(
        length(couch_util:get_value(<<"rows">>, ViewResults)),
        ExpectedRowCount,
        "Got " ++ integer_to_list(ExpectedRowCount) ++ " view rows"),
    SortedKeys =  couch_set_view_test_util:are_view_keys_sorted(
        {ViewResults}, fun(A, B) -> A < B end),
    etap:is(SortedKeys, true, "View result keys are sorted").


populate_set() ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
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
    DocList = lists:map(
        fun(I) ->
            {[
                {<<"_id">>, iolist_to_binary(["doc", integer_to_list(I)])},
                {<<"value">>, I}
            ]}
        end,
        lists:seq(1, num_docs())),
    ok = couch_set_view_test_util:populate_set_alternated(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


configure_view_group(Active, Passive) ->
    etap:diag("Configuring view group"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = Active,
        passive_partitions = Passive
    },
    try
        couch_set_view:define_group(test_set_name(), ddoc_id(), Params)
    catch _:Error ->
        Error
    end.


group_index_file() ->
    {ok, Info} = couch_set_view:get_group_info(test_set_name(), ddoc_id()),
    binary_to_list(iolist_to_binary(
        [
            filename:join([
                couch_config:get("couchdb", "view_index_dir"),
                "set_view_" ++ binary_to_list(test_set_name()) ++ "_design",
                "main_" ++ binary_to_list(couch_util:get_value(signature, Info))
            ]),
            ".view"
        ])).
