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

% This test is about a corrupted file on group startup. This lead to
% dangling references in the couch_file_write_guard.

-include("../../couchdb/couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

test_set_name() -> <<"couch_test_set_view_write_guard">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.


main(_) ->
    test_util:init_code_path(),

    etap:plan(3),
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

    etap:diag("Testing startup when header is invalid"),

    setup_test(),
    % Make sure there's a view created
    {ok, {ViewResults}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, ["stale=false"]),
    etap:is(ViewResults,
        [{<<"total_rows">>,0},{<<"offset">>,0},{<<"rows">>,[]}],
        "View created"),

    % Create and append invalid header
    Fd = get_fd(),
    {ok, HeaderBin, _Pos} = couch_file:find_header_bin(Fd, eof),
    <<Signature:16/binary, _HeaderBaseCompressed/binary>> = HeaderBin,
    InvalidBase = couch_compress:compress(<<"this_is_not_a_valid_header">>),
    InvalidHeaderBin = <<Signature:16/binary, InvalidBase/binary>>,
    couch_file:write_header_bin(Fd, InvalidHeaderBin),

    % Shutdown the group, so that it fails when it starts up again
    couch_util:shutdown_sync(whereis(couch_setview_server_name_prod)),
    % Wait for server to restart
    timer:sleep(1000),

    % The first query leads to an error due to the invalid header
    {ok, Body1} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, ["stale=false"], 500),
    etap:is(Body1, {[{<<"error">>, <<"badmatch">>},
                     {<<"reason">>, <<"this_is_not_a_valid_header">>}]},
           "First query leads to an error due to the invalid header"),
    {ok, Body2} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"test">>, ["stale=false"], 500),
    % In case of MB-17044 it would be an `file_already_opened` error
    etap:is(Body2, Body1,
           "Second query leads to an error due to the invalid header as well"),

    couch_set_view_test_util:stop_server(),
    ok.


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


configure_view_group(NumViewPartitions) ->
    etap:diag("Configuring view group"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, NumViewPartitions div 2),
        passive_partitions = lists:seq(NumViewPartitions div 2 + 1, NumViewPartitions-1),
        use_replica_index = true
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
