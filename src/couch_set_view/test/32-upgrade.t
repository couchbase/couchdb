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

test_set_name() -> <<"couch_test_set_upgrade">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 128.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(9),
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

    etap:diag("Testing the upgrade of the group header"),

    test_upgrade_header_2x_incremental(),
    test_upgrade_header_2x_no_seqs(),

    couch_set_view_test_util:stop_server(),
    ok.


test_upgrade_header_2x_incremental() ->
    etap:diag("Testing upgrade header from 2.x to 3.x with "
        "incremental indexing"),

    setup_test(),

    Fd = get_fd(),
    {ok, HeaderBin1, HeaderPos1} = couch_file:read_header_bin(Fd),

    % Downgrade the header to a 2.x one
    HeaderWithoutPartVersionBin = downgrade_header_to_2x(HeaderBin1),
    HeaderWithoutPartVersion = couch_set_view_util:header_bin_to_term(
        HeaderWithoutPartVersionBin),
    etap:is(HeaderWithoutPartVersion#set_view_index_header.partition_versions,
        nil, "The header was downgraded and can be parsed"),
    {ok, HeaderPos2} = couch_file:write_header_bin(
        Fd, HeaderWithoutPartVersionBin),
    couch_file:flush(Fd),
    etap:isnt(HeaderPos2, HeaderPos1, "The downgraded header was written"),
    {ok, HeaderBin2, HeaderPos2} = couch_file:read_header_bin(Fd),
    Header2 = couch_set_view_util:header_bin_to_term(HeaderBin2),
    etap:is(HeaderBin2, HeaderWithoutPartVersionBin,
         "The downgraded header can be read"),

    % Kill the group, it will respawn itself by `populate_set/2`
    couch_util:shutdown_sync(get_group_pid()),

    % In the logs you can also see a message of the upgrade:
    % `Upgrading index header from Couchbase 2.x to 3.x`
    populate_set(1, num_set_partitions()),
    Fd2 = get_fd(),
    {ok, HeaderBin3, HeaderPos3} = couch_file:read_header_bin(Fd2),
    Header3 = couch_set_view_util:header_bin_to_term(HeaderBin3),

    etap:ok(HeaderPos3 > HeaderPos2, "New header was written"),
    etap:is(Header3#set_view_index_header.partition_versions,
        [{0,[{1000,0}]},{1,[{1001,0}]},{2,[{1002,0}]},{3,[{1003,0}]}],
        "Header got upgraded, and now contains proper partition versions"),

    shutdown_group().


test_upgrade_header_2x_no_seqs() ->
    etap:diag("Testing upgrade header from 2.x to 3.x when no seqs "
        "are set"),

     setup_test(),

    Fd = get_fd(),
    {ok, HeaderBin1, HeaderPos1} = couch_file:read_header_bin(Fd),
    Header1 = couch_set_view_util:header_bin_to_term(HeaderBin1),
    HeaderSig1 = couch_set_view_util:header_bin_sig(HeaderBin1),

    Header1NoSeqs = Header1#set_view_index_header{seqs = []},
    Header1NoSeqsBin = couch_set_view_util:group_to_header_bin(
        #set_view_group{index_header = Header1NoSeqs, sig = HeaderSig1}),

    % Downgrade the header to a 2.x one
    HeaderWithoutPartVersionBin = downgrade_header_to_2x(Header1NoSeqsBin),
    HeaderWithoutPartVersion = couch_set_view_util:header_bin_to_term(
    HeaderWithoutPartVersionBin),
    etap:is(HeaderWithoutPartVersion#set_view_index_header.partition_versions,
        nil, "The header was downgraded and can be parsed"),

    {ok, HeaderPos2} = couch_file:write_header_bin(
    Fd, HeaderWithoutPartVersionBin),
    couch_file:flush(Fd),
    etap:isnt(HeaderPos2, HeaderPos1, "The downgraded header was written"),
    {ok, HeaderBin2, HeaderPos2} = couch_file:read_header_bin(Fd),
    Header2 = couch_set_view_util:header_bin_to_term(HeaderBin2),
    etap:is(HeaderBin2, HeaderWithoutPartVersionBin,
        "The downgraded header can be read"),

    % Kill the group, it will respawn itself by `populate_set/2`
    couch_util:shutdown_sync(get_group_pid()),

    % In the logs you can also see a message of the upgrade:
    % `Upgrading index header from Couchbase 2.x to 3.x`
    populate_set(1, num_set_partitions()),
    Fd2 = get_fd(),
    {ok, HeaderBin3, _HeaderPos3} = couch_file:read_header_bin(Fd2),
    Header3 = couch_set_view_util:header_bin_to_term(HeaderBin3),
    etap:is(Header3#set_view_index_header.partition_versions, [],
        "Header got upgraded, and now contains proper partition versions"),
    shutdown_group().

setup_test() ->
    couch_set_view_test_util:delete_set_dbs(
        test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(
        test_set_name(), num_set_partitions()),

    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta) { "
                                  "emit(meta.id, doc.value); }">>}
                ]}}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    ok = configure_view_group(num_set_partitions()).


shutdown_group() ->
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


populate_set(From, To) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    DocList = generate_docs(From, To),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).

generate_docs(From, To) ->
    DocList = lists:map(
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


get_group_pid() ->
    couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod).

get_fd() ->
    GroupPid = get_group_pid(),
    {ok, Group, 0} = gen_server:call(
        GroupPid, #set_view_group_req{stale = false, debug = true}, infinity),
    Group#set_view_group.fd.


% Take a 3.x header and make it a 2.x header with file format version 1
% without partition versions
downgrade_header_to_2x(HeaderBin) ->
    <<Signature:16/binary, HeaderBaseCompressed/binary>> = HeaderBin,
    Base = couch_compress:decompress(HeaderBaseCompressed),
    <<
      Version0:8,
      Rest/binary
    >> = Base,
    % The partition versions are at the end of the file. As they just got
    % initialized, they contain only one partition version per partition.
    % The number of partitions is 2 bytes, one partition version is 16 byte,
    % the number of partition versions is 2 bytes, the partition ID is 2 bytes.
    % I.e. 20 bytes per partition + 2 bytes for the number of partitions
    PartVersionsSize = 20 * num_set_partitions() + 2,
    NewRest = erlang:binary_part(
        Base, 1, byte_size(Base) - 1 - PartVersionsSize),
    % Assemble the header with version 1 again
    NewBase = <<1:8, NewRest/binary>>,
    NewHeaderBaseCompressed = couch_compress:compress(NewBase),
    <<Signature:16/binary, NewHeaderBaseCompressed/binary>>.
