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

test_set_name() -> <<"couch_test_set_index_replica_index_missing">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.


main(_) ->
    test_util:init_code_path(),

    etap:plan(13),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            io:format(standard_error, "Test died abnormally: ~p", [Other]),
            etap:bail(Other)
    end,
    ok.


test() ->
    couch_set_view_test_util:start_server(test_set_name()),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    configure_group(),

    MainPid = get_main_pid(),
    etap:is(true, is_process_alive(MainPid), "Main group is alive"),
    etap:is(true, couch_set_view_group:is_view_defined(MainPid), "Main group is configured"),

    RepPid = get_replica_pid(MainPid),
    etap:is(true, is_process_alive(RepPid), "Replica group is alive"),
    etap:is(true, couch_set_view_group:is_view_defined(RepPid), "Replica group is configured"),

    RepIndexFile = replica_index_file(),
    etap:is(true, filelib:is_file(RepIndexFile), "Replica index file found"),

    etap:diag("Shutting down view group"),
    couch_util:shutdown_sync(MainPid),

    etap:is(true, filelib:is_file(RepIndexFile), "Replica index file not deleted"),
    etap:diag("Deleting index file"),
    ok = file:delete(RepIndexFile),
    etap:is(false, filelib:is_file(RepIndexFile), "Replica index file deleted"),

    wait_group_shutdown(MainPid),

    etap:diag("Opening view groups again"),
    MainPid2 = get_main_pid(),
    etap:is(true, is_process_alive(MainPid2), "Main group is alive"),
    etap:is(true, couch_set_view_group:is_view_defined(MainPid2), "Main group is configured"),
    RepPid2 = get_replica_pid(MainPid2),
    etap:is(true, is_process_alive(RepPid2), "Replica group is alive"),
    etap:is(true, couch_set_view_group:is_view_defined(RepPid2), "Replica group is configured"),

    {ok, MainGroup} = gen_server:call(MainPid2, request_group, infinity),
    {ok, RepGroup} = gen_server:call(RepPid2, request_group, infinity),

    etap:is(?set_num_partitions(RepGroup),
            ?set_num_partitions(MainGroup),
            "Replica group has same number of max partitions as main group"),

    etap:is(true, filelib:is_file(RepIndexFile), "Replica index file recreated"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    ok = timer:sleep(1000),
    couch_set_view_test_util:stop_server(),
    ok.


configure_group() ->
    couch_set_view:cleanup_index_files(test_set_name()),
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
    etap:diag("Configuring set view with partitions [0 .. 31] as active"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, 31),
        passive_partitions = [],
        use_replica_index = true
    },
    ok = couch_set_view:define_group(test_set_name(), ddoc_id(), Params).


get_main_pid() ->
    couch_set_view:get_group_pid(test_set_name(), ddoc_id()).


get_replica_pid(MainPid) ->
    {ok, Group} = gen_server:call(MainPid, request_group, infinity),
    Group#set_view_group.replica_pid.


replica_index_file() ->
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    IndexDir = couch_set_view:set_index_dir(RootDir, test_set_name()),
    {ok, GroupSig} = couch_set_view:get_group_signature(test_set_name(), ddoc_id()),
    filename:join([IndexDir, "replica_" ++ binary_to_list(GroupSig) ++ ".view.1"]).


wait_group_shutdown(OldPid) ->
    NewPid = get_main_pid(),
    case NewPid of
    OldPid ->
        ok = timer:sleep(50),
        wait_group_shutdown(OldPid);
    _ ->
        ok
    end.
