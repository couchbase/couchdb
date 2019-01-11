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

% This test is about truncating a view file so that the only valid header is
% the first one in the view file. In such a case the system should just
% recreate the view.

-include("../../couchdb/couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

test_set_name() -> <<"couch_test_set_view_truncate">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 128.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(5),
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

    etap:diag("Testing truncating a view file after compaction"),

    setup_test(),
    populate_set(1, num_docs()),
    {ok, {ViewResults1}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, ["stale=false"]),
    [{<<"total_rows">>, Rows1}, _, _] = ViewResults1,
    etap:is(Rows1, 128, "View contains data"),

    % Compact the view so that it contains only two headers. One right at
    % the beginning of the file and one after the actual data
    trigger_compaction(),

    Fd1 = get_fd(),
    {ok, HeaderBin1, Pos1} = couch_file:find_header_bin(Fd1, eof),
    etap:is(Pos1 > 0, true, "View contains more than one valid header"),

    % Truncate the last byte of the second header so that it is corrupted
    ok = couch_file:truncate(Fd1, Pos1 + byte_size(HeaderBin1) - 1),
    couch_file:sync(Fd1),

    % Restart with the corrupted file
    restart_server(),

    % Check if truncation was successful
    Fd2 = get_fd(),
    {ok, HeaderBin2, Pos2} = couch_file:find_header_bin(Fd2, eof),
    etap:is(Pos2, 0, "View contains only one valud header after truncation"),

    % A query with `stale=ok` should return an empty set as the only
    % valid header is the first one in the file that doesn't point to
    % any data.
    {ok, {ViewResults2}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, ["stale=ok"]),
    [{<<"total_rows">>, Rows2}, _, _] = ViewResults2,
    etap:is(Rows2, 0, "View is empty"),

    % A query with `stale=false` should rebuild the index and return the
    % original result
    {ok, {ViewResults3}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, ["stale=false"]),
    etap:is(ViewResults3, ViewResults1,
        "View got recreated and contains the expected data"),

    couch_set_view_test_util:stop_server(),

    ok.


restart_server() ->
    timer:sleep(1000),
    couch_util:shutdown_sync(get(test_util_dcp_pid)),
    couch_util:shutdown_sync(whereis(couch_server_sup)),
    couch_set_view_test_util:start_server(test_set_name()),
    ok.


trigger_compaction() ->
    etap:diag("Triggering compaction"),
    {ok, CompactPid} = couch_set_view_compactor:start_compact(
        mapreduce_view, test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, CompactPid),
    receive
    {'DOWN', Ref, process, CompactPid, _Reason} ->
        ok
    after 5000 ->
        etap:bail("Compaction took to long")
    end.


setup_test() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

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
    ok = configure_view_group(num_set_partitions()).


populate_set(From, To) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    DocList = generate_docs(From, To),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


generate_docs(From, To) ->
    lists:map(
        fun(I) ->
            Doc = iolist_to_binary(["doc", integer_to_list(I)]),
            {[
                {<<"meta">>, {[{<<"id">>, Doc}]}},
                {<<"json">>, {[{<<"value">>, I}]}}
            ]}
        end,
        lists:seq(From, To)).


configure_view_group(NumViewPartitions) ->
    etap:diag("Configuring view group"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, NumViewPartitions-1),
        passive_partitions = [],
        use_replica_index = false
    },
    try
        couch_set_view:define_group(
            mapreduce_view, test_set_name(), ddoc_id(), Params)
    catch _:Error ->
        Error
    end.


get_fd() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, Group, 0} = gen_server:call(
        GroupPid, #set_view_group_req{stale = ok, debug = true}, infinity),
    Group#set_view_group.fd.
