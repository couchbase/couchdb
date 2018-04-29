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

-include_lib("couch_dcp/include/couch_dcp_typespecs.hrl").
-include_lib("couch_dcp/include/couch_dcp.hrl").

test_set_name() -> <<"couch_test_couch_dcp_client">>.
num_set_partitions() -> 4.
num_docs() -> 1000.
num_docs_pp() -> num_docs() div num_set_partitions().

-define(DCP_MSG_SIZE_MUTATION, 55).
-define(DCP_MSG_SIZE_DELETION, 42).
-define(DCP_MSG_SIZE_SNAPSHOT , 44).
-define(DCP_MSG_SIZE_STREAM_END, 28).

main(_) ->
    test_util:init_code_path(),

    etap:plan(56),
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
    setup_test(),

    tests(),
    test_close_during_request(),

    couch_set_view_test_util:stop_server(),
    ok.

tests() ->
    % Populate failover log
    FailoverLogs = lists:map(fun(PartId) ->
        FailoverLog = [
            {10001, PartId + 3}, {10002, PartId + 2}, {10003, 0}],
        couch_dcp_fake_server:set_failover_log(PartId, FailoverLog),
        FailoverLog
    end, lists:seq(0, num_set_partitions() - 1)),

    TestFun = fun(Item, Acc) ->
        case Item of
        {snapshot_marker, _} ->
            Acc;
        {part_versions, _} ->
            Acc;
        _ ->
            Acc ++ [Item]
        end
    end,

    AddStreamFun = fun(Pid, PartId, PartUuid, StartSeq, EndSeq, Flags) ->
        couch_dcp_client:add_stream(Pid, PartId, PartUuid, StartSeq, EndSeq, Flags)
    end,

    {auth, User, Passwd} = cb_auth_info:get(),
    {ok, Pid} = couch_dcp_client:start(
        test_set_name(), test_set_name(), User, Passwd, 1024, 0),

    % Get the latest partition version first
    {ok, InitialFailoverLog0} = couch_dcp_client:get_failover_log(Pid, 0),
    etap:is(InitialFailoverLog0, hd(FailoverLogs), "Failover log is correct"),

    % First parameter is the partition, the second is the sequence number
    % to start at.
    {ok, Docs1, FailoverLog1} = couch_dcp_client:enum_docs_since(
        Pid, 0, InitialFailoverLog0, 4, 10, ?DCP_FLAG_NOFLAG, TestFun, [], AddStreamFun),
    etap:is(length(Docs1), 6, "Correct number of docs (6) in partition 0"),
    etap:is(FailoverLog1, lists:nth(1, FailoverLogs),
        "Failoverlog from partition 0 is correct"),

    {ok, InitialFailoverLog1} = couch_dcp_client:get_failover_log(Pid, 1),
    {ok, Docs2, FailoverLog2} = couch_dcp_client:enum_docs_since(
        Pid, 1, InitialFailoverLog1, 46, 165, ?DCP_FLAG_NOFLAG, TestFun, [], AddStreamFun),
    etap:is(length(Docs2), 119, "Correct number of docs (109) partition 1"),
    etap:is(FailoverLog2, lists:nth(2, FailoverLogs),
        "Failoverlog from partition 1 is correct"),

    {ok, InitialFailoverLog2} = couch_dcp_client:get_failover_log(Pid, 2),
    {ok, Docs3, FailoverLog3} = couch_dcp_client:enum_docs_since(
        Pid, 2, InitialFailoverLog2, 80, num_docs() div num_set_partitions(),
        ?DCP_FLAG_NOFLAG, TestFun, [], AddStreamFun),
    Expected3 = (num_docs() div num_set_partitions()) - 80,
    etap:is(length(Docs3), Expected3,
        io_lib:format("Correct number of docs (~p) partition 2", [Expected3])),
    etap:is(FailoverLog3, lists:nth(3, FailoverLogs),
        "Failoverlog from partition 2 is correct"),

    {ok, InitialFailoverLog3} = couch_dcp_client:get_failover_log(Pid, 3),
    {ok, Docs4, FailoverLog4} = couch_dcp_client:enum_docs_since(
        Pid, 3, InitialFailoverLog3, 0, 5, ?DCP_FLAG_NOFLAG, TestFun, [], AddStreamFun),
    etap:is(length(Docs4), 5, "Correct number of docs (5) partition 3"),
    etap:is(FailoverLog4, lists:nth(4, FailoverLogs),
        "Failoverlog from partition 3 is correct"),

    % Try a too high sequence number to get a erange error response
    {error, ErangeError} = couch_dcp_client:enum_docs_since(
        Pid, 0, InitialFailoverLog0, 400, 450, ?DCP_FLAG_NOFLAG, TestFun, [], AddStreamFun),
    etap:is(ErangeError, wrong_start_sequence_number,
        "Correct error message for too high sequence number"),

    % Start sequence is bigger than end sequence
    {_Request, ErangeError2} =
        couch_dcp_client:add_stream(
            Pid, 0, first_uuid(InitialFailoverLog0), 5, 2,
            ?DCP_FLAG_NOFLAG),
    etap:is(ErangeError2, {error, wrong_start_sequence_number},
        "Correct error message for start sequence > end sequence"),

    Error = couch_dcp_client:enum_docs_since(
        Pid, 1, [{4455667788, 1243}], 46, 165, ?DCP_FLAG_NOFLAG, TestFun, [], AddStreamFun),
    etap:is(Error, {rollback, 0},
        "Correct error for wrong failover log"),

    {ok, [Seq0]} = couch_dcp_client:get_seqs(Pid, [0]),
    etap:is(Seq0, {0, num_docs() div num_set_partitions()},
        "Sequence number of partition 0 is correct"),
    {ok, [Seq1]} = couch_dcp_client:get_seqs(Pid, [1]),
    etap:is(Seq1, {1, num_docs() div num_set_partitions()},
        "Sequence number of partition 1 is correct"),
    {ok, [Seq2]} = couch_dcp_client:get_seqs(Pid, [2]),
    etap:is(Seq2, {2, num_docs() div num_set_partitions()},
         "Sequence number of partition 2 is correct"),
    {ok, [Seq3]} = couch_dcp_client:get_seqs(Pid, [3]),
    etap:is(Seq3, {3, num_docs() div num_set_partitions()},
        "Sequence number of partition 3 is correct"),
    SeqMissing = couch_dcp_client:get_seqs(Pid, [100000]),
    etap:is(SeqMissing, {ok, []},
        "Too high partition number returns correct error"),
    SeqAll = couch_dcp_client:get_seqs(Pid, nil),
    etap:is(SeqAll, {ok, [Seq0, Seq1, Seq2, Seq3]},
        "Returned all partition seqs correctly"),


    % Test snapshot markers types

    TestSnapshotFun = fun(Item, Acc) ->
        case Item of
        {snapshot_marker, Marker} ->
            Acc ++ [Marker];
        {part_versions, _} ->
            Acc;
        _ ->
            Acc
        end
    end,

    SnapshotStart1 = 0,
    SnapshotEnd1 = num_docs() div (num_set_partitions() * 2),
    {ok, Markers1, SnapshotFailoverLog1} = couch_dcp_client:enum_docs_since(
        Pid, 2, [{0, 0}], SnapshotStart1, SnapshotEnd1, ?DCP_FLAG_NOFLAG,
        TestSnapshotFun, [], AddStreamFun),
    ExpectedMarkers1 = [{SnapshotStart1, SnapshotEnd1,
        ?DCP_SNAPSHOT_TYPE_DISK}],
    etap:is(Markers1, ExpectedMarkers1,
        "Received one on-disk snapshot marker"),

    SnapshotStart2 = num_docs() div (num_set_partitions() * 2),
    SnapshotEnd2 = num_docs() div num_set_partitions(),
    {ok, Markers2, SnapshotFailoverLog2} = couch_dcp_client:enum_docs_since(
        Pid, 2, SnapshotFailoverLog1, SnapshotStart2, SnapshotEnd2,
        ?DCP_FLAG_NOFLAG, TestSnapshotFun, [], AddStreamFun),
    ExpectedMarkers2 = [{SnapshotStart2, SnapshotEnd2,
        ?DCP_SNAPSHOT_TYPE_MEMORY}],
    etap:is(Markers2, ExpectedMarkers2,
        "Received one in-memory snapshot marker"),


    % Test multiple snapshots

    TestAllFun = fun(Item, Acc) -> Acc ++ [Item] end,

    ItemsPerSnapshot = 30,
    couch_dcp_fake_server:set_items_per_snapshot(ItemsPerSnapshot),
    SnapshotStart3 = 0,
    SnapshotEnd3 = num_docs() div num_set_partitions(),
    {ok, All3, SnapshotFailoverLog3} = couch_dcp_client:enum_docs_since(
        Pid, 2, [{0, 0}], SnapshotStart3, SnapshotEnd3, ?DCP_FLAG_NOFLAG,
        TestAllFun, [], AddStreamFun),
    Markers3 = [M || {snapshot_marker, M} <- All3],
    ExpectedMarkers3 = [{0, ItemsPerSnapshot, ?DCP_SNAPSHOT_TYPE_DISK}] ++
        lists:map(fun(I) ->
            {I, min(I + ItemsPerSnapshot, SnapshotEnd3),
                ?DCP_SNAPSHOT_TYPE_MEMORY}
        end, lists:seq(ItemsPerSnapshot, SnapshotEnd3 - 1, ItemsPerSnapshot)),
    etap:is(Markers3, ExpectedMarkers3,
        "Received the expected snapshot markers"),

    Mutations3 = [M || #dcp_doc{} = M <- All3],
    couch_dcp_fake_server:set_items_per_snapshot(0),
    {ok, ExpectedMutations3, SnapshotFailoverLog3} =
            couch_dcp_client:enum_docs_since(
        Pid, 2, [{0, 0}], SnapshotStart3, SnapshotEnd3, ?DCP_FLAG_NOFLAG,
        TestFun, [], AddStreamFun),
    etap:is(Mutations3, ExpectedMutations3,
        "Received the expected mutations within the several snapshots"),


    % Test duplicated items in multiple snapshots

    ItemsPerSnapshot2 = 30,
    DupsPerSnapshot2 = 4,
    couch_dcp_fake_server:set_items_per_snapshot(ItemsPerSnapshot2),
    couch_dcp_fake_server:set_dups_per_snapshot(DupsPerSnapshot2),
    DupsStart2 = 0,
    DupsEnd2 = couch_dcp_fake_server:num_items_with_dups(
        num_docs_pp(), ItemsPerSnapshot2, DupsPerSnapshot2),

    {ok, AllDups2, DupsFailoverLog2} = couch_dcp_client:enum_docs_since(
        Pid, 2, [{0, 0}], DupsStart2, DupsEnd2, ?DCP_FLAG_NOFLAG,
        TestAllFun, [], AddStreamFun),
    DupsMutations2 = [M || #dcp_doc{} = M <- AllDups2],
    DupsMarkers2 = [M || {snapshot_marker, M} <- AllDups2],
    etap:is(length(DupsMutations2), DupsEnd2,
        "received the expected number of mutations (incl. duplicates)"),
    etap:is(length(DupsMarkers2),
        couch_dcp_fake_server:ceil_div(DupsEnd2, ItemsPerSnapshot2),
        "received the expected number of snapshots"),
    DupsUnique2 = lists:ukeysort(#dcp_doc.id, DupsMutations2),
    DupsUniqueIds2 = [Id || #dcp_doc{id = Id} <- DupsUnique2],
    MutationsIds2 = [Id || #dcp_doc{id = Id} <- Mutations3],
    etap:is(DupsUniqueIds2, MutationsIds2,
        "received the expected mutations when de-duplicated"),
    couch_dcp_fake_server:set_items_per_snapshot(0),
    couch_dcp_fake_server:set_dups_per_snapshot(0),


    % Test multiple streams in parallel
    {StreamReq0, {failoverlog, InitialFailoverLog0}} =
        couch_dcp_client:add_stream(
            Pid, 0, first_uuid(InitialFailoverLog0), 10, 100,
            ?DCP_FLAG_NOFLAG),

    {StreamReq1, {failoverlog, InitialFailoverLog1}} =
        couch_dcp_client:add_stream(
            Pid, 1, first_uuid(InitialFailoverLog1), 100, 200,
            ?DCP_FLAG_NOFLAG),

    {StreamReq2, {failoverlog, InitialFailoverLog2}} =
        couch_dcp_client:add_stream(
            Pid, 2, first_uuid(InitialFailoverLog2), 0, 10, ?DCP_FLAG_NOFLAG),

    [MutationsPart0, MutationsPart1, MutationsPart2] = read_mutations(
                    Pid, [StreamReq0, StreamReq1, StreamReq2], [[], [], []]),


    etap:is(is_same_partition(0, MutationsPart0), true,
        "Stream0 has only partition0 mutations"),
    etap:is(is_same_partition(1, MutationsPart1), true,
        "Stream1 has only partition1 mutations"),
    etap:is(is_same_partition(2, MutationsPart2), true,
        "Stream2 has only partition2 mutations"),

    etap:is(length(MutationsPart0), 90,
        "Stream0 has 90 mutations"),
    etap:is(length(MutationsPart1), 100,
        "Stream1 has 100 mutations"),
    etap:is(length(MutationsPart2), 10,
        "Stream2 has 10 mutations"),

    {active_list_streams, StreamList0} = couch_dcp_client:list_streams(Pid),
    etap:is(StreamList0, [], "Stream list is empty"),

    couch_dcp_fake_server:pause_mutations(),
    {StreamReq0_2, {failoverlog, InitialFailoverLog0}} =
        couch_dcp_client:add_stream(
            Pid, 0, first_uuid(InitialFailoverLog0), 1, 100, ?DCP_FLAG_NOFLAG),

    {_, StreamResp0_3} =
        couch_dcp_client:add_stream(
            Pid, 0, first_uuid(InitialFailoverLog0), 10, 100,
            ?DCP_FLAG_NOFLAG),
    etap:is(StreamResp0_3, {error,vbucket_stream_already_exists},
        "Stream for vbucket 0 already exists"),
    couch_dcp_fake_server:continue_mutations(),

    % Drain all mutations
    read_mutations(Pid, [StreamReq0_2], [[]]),

    couch_dcp_fake_server:pause_mutations(),
    couch_dcp_client:add_stream(
        Pid, 1, first_uuid(InitialFailoverLog1), 10, 300, ?DCP_FLAG_NOFLAG),

    couch_dcp_client:add_stream(
        Pid, 2, first_uuid(InitialFailoverLog2), 100, 200, ?DCP_FLAG_NOFLAG),

    {active_list_streams, StreamList1} = couch_dcp_client:list_streams(Pid),
    etap:is(StreamList1, [1,2], "Stream list contains parititon 1,2"),

    StreamRemoveResp0 = couch_dcp_client:remove_stream(Pid, 1),
    {active_list_streams, StreamList2} = couch_dcp_client:list_streams(Pid),
    etap:is({StreamRemoveResp0, StreamList2}, {ok, [2]},
        "Removed parititon stream 1 and parition stream 2 is left"),

    StreamRemoveResp1 = couch_dcp_client:remove_stream(Pid, 1),
    etap:is(StreamRemoveResp1, {error, vbucket_stream_not_found},
        "Correct error on trying to remove non-existing stream"),
    couch_dcp_fake_server:continue_mutations(),

    % Test with too large failover log
    TooLargeFailoverLog = [{I, I} ||
        I <- lists:seq(0, ?DCP_MAX_FAILOVER_LOG_SIZE)],
    PartId = 1,
    couch_dcp_fake_server:set_failover_log(PartId, TooLargeFailoverLog),
    TooLargeError = couch_dcp_client:enum_docs_since(
          Pid, PartId, [{0, 0}], 0, 100, ?DCP_FLAG_NOFLAG, TestFun, [], AddStreamFun),
    etap:is(TooLargeError, {error, too_large_failover_log},
        "Too large failover log returns correct error"),

    gen_server:call(Pid, reset_buffer_size),
    % Remove existing streams
    couch_dcp_client:remove_stream(Pid, 0),
    couch_dcp_client:remove_stream(Pid, 1),
    couch_dcp_client:remove_stream(Pid, 2),


    % Tests for flow control
    couch_dcp_fake_server:pause_mutations(),

    {StreamReq0_4, _} = couch_dcp_client:add_stream(
        Pid, 0, first_uuid(InitialFailoverLog0), 0, 500, ?DCP_FLAG_NOFLAG),

    gen_server:call(Pid, reset_buffer_size),
    Ret = couch_dcp_fake_server:is_control_req(),

    etap:is(Ret, true, "Buffer control request sent"),

    NumBufferAck = couch_dcp_fake_server:get_num_buffer_acks(),

    % Fill the client buffer by asking server to send 1024 bytes data
    try_until_throttled(Pid, StreamReq0_4, 1000, 1024),

    % Consume 200 bytes from the client
    try_until_unthrottled(Pid, StreamReq0_4, 0, 200),

    NumBufferAck2 = couch_dcp_fake_server:get_num_buffer_acks(),
    etap:is(NumBufferAck2, NumBufferAck, "Not sent the buffer ack"),

    % Consume More data so that is greater then 20 % of 1024 i.e.204.
    % when data is 20% consumed, client sends the buffer ack to increase
    % the flow control buffer.
    try_until_unthrottled(Pid, StreamReq0_4, 0, 210),
    timer:sleep(500),
    NumBufferAck3 = couch_dcp_fake_server:get_num_buffer_acks(),
    etap:is(NumBufferAck3, NumBufferAck + 1, "Got the buffer ack"),
    couch_dcp_client:remove_stream(Pid, 0),

    couch_dcp_fake_server:pause_mutations(),
    {StreamReq1_2, _} = couch_dcp_client:add_stream(
        Pid, 1, first_uuid(InitialFailoverLog1), 0, 500, ?DCP_FLAG_NOFLAG),

    ReqPid = spawn(fun() ->
        couch_dcp_client:get_stream_event(Pid, StreamReq1_2)
        end),

    EvResponse = couch_dcp_client:get_stream_event(Pid, StreamReq1_2),
    etap:is(EvResponse, {error, event_request_already_exists},
        "Error message received when requested multiple times for same stream's event"),

    exit(ReqPid, shutdown),
    couch_dcp_client:remove_stream(Pid, 1),
    gen_server:call(Pid, flush_old_streams_meta),

    {StreamReq0_5, _} = couch_dcp_client:add_stream(Pid, 2,
        first_uuid(InitialFailoverLog2), 0, 500, ?DCP_FLAG_NOFLAG),

    % Connection close and reconnection tests
    ok = couch_dcp_fake_server:send_single_mutation(),
    {snapshot_mutation, Mutation1} = couch_dcp_client:get_stream_event(Pid, StreamReq0_5),
    #dcp_doc{seq = SeqNo1} = Mutation1,
    OldSocket = gen_server:call(Pid, get_socket),
    ok = couch_dcp_fake_server:close_connection(2),
    % wait for sometime to do reconnect from couch_dcp client
    timer:sleep(4000),
    NewSocket = gen_server:call(Pid, get_socket),
    etap:isnt(OldSocket, NewSocket, "Socket changed"),
    ok = couch_dcp_fake_server:send_single_mutation(),
    ErrorResp = couch_dcp_client:get_stream_event(Pid, StreamReq0_5),
    etap:is({error, dcp_conn_closed}, ErrorResp, "Got the error response after connection close"),
    {active_list_streams, EmptyStreamList} = couch_dcp_client:list_streams(Pid),
    etap:is([], EmptyStreamList, "Stream is correctly removed after connection close"),

    ok = couch_dcp_fake_server:close_connection(nil),
    % wait for sometime to do reconnect from couch_dcp client
    timer:sleep(4000),
    couch_dcp_client:remove_stream(Pid, 2),
    couch_dcp_fake_server:continue_mutations(),

    % Test get_num_items

    {ok, NumItems0} = couch_dcp_client:get_num_items(Pid, 0),
    etap:is(NumItems0, num_docs() div num_set_partitions(),
        "Number of items of partition 0 is correct"),
    {ok, NumItems1} = couch_dcp_client:get_num_items(Pid, 1),
    etap:is(NumItems1, num_docs() div num_set_partitions(),
        "Number of items of partition 1 is correct"),
    {ok, NumItems2} = couch_dcp_client:get_num_items(Pid, 2),
    etap:is(NumItems2, num_docs() div num_set_partitions(),
         "Number of items of partition 2 is correct"),
    {ok, NumItems3} = couch_dcp_client:get_num_items(Pid, 3),
    etap:is(NumItems3, num_docs() div num_set_partitions(),
        "Number of items of partition 3 is correct"),
    NumItemsError = couch_dcp_client:get_num_items(Pid, 100000),
    etap:is(NumItemsError, {error, not_my_vbucket},
        "Too high partition number returns correct error"),

    DocsPerPartition = num_docs() div num_set_partitions(),
    NumDelDocs0 = num_docs() div (num_set_partitions() * 2),
    delete_docs(1, NumDelDocs0),
    {ok, NumItemsDel0} = couch_dcp_client:get_num_items(Pid, 0),
    etap:is(NumItemsDel0, DocsPerPartition - NumItemsDel0,
        "Number of items of partition 0 after some deletions is correct"),
    NumDelDocs3 = num_docs() div (num_set_partitions() * 4),
    delete_docs(3 * DocsPerPartition + 1, NumDelDocs3),
    {ok, NumItemsDel3} = couch_dcp_client:get_num_items(Pid, 3),
    etap:is(NumItemsDel3, DocsPerPartition - NumDelDocs3,
        "Number of items of partition 3 after some deletions is correct"),

    % Tests for requesting persisted items only

    couch_dcp_fake_server:set_persisted_items_fun(fun(Seq) -> Seq  end),
    {ok, [{1, HighSeq1}]} = couch_dcp_client:get_seqs(Pid, [1]),
    {ok, ExpectedDocs1, _} =
        couch_dcp_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq1, ?DCP_FLAG_NOFLAG,
            TestFun, [], AddStreamFun),
    {ok, PersistedDocs1, _} =
        couch_dcp_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq1, ?DCP_FLAG_DISKONLY,
            TestFun, [], AddStreamFun),
    etap:is(PersistedDocs1, ExpectedDocs1,
        "The persisted sequence number is correct, seq"),

    couch_dcp_fake_server:set_persisted_items_fun(
       fun(Seq) -> Seq div 2 end),
    {ok, [{1, HighSeq2}]} = couch_dcp_client:get_seqs(Pid, [1]),
    {ok, ExpectedDocs2, _} =
        couch_dcp_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq2 div 2, ?DCP_FLAG_NOFLAG,
            TestFun, [], AddStreamFun),
    {ok, PersistedDocs2, _} =
        couch_dcp_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq2, ?DCP_FLAG_DISKONLY,
            TestFun, [], AddStreamFun),
    etap:is(PersistedDocs2, ExpectedDocs2,
        "The persisted sequence number is correct, seq/2"),

    couch_dcp_fake_server:set_persisted_items_fun(fun(Seq) -> Seq - 1 end),
    {ok, [{1, HighSeq3}]} = couch_dcp_client:get_seqs(Pid, [1]),
    {ok, ExpectedDocs3, _} =
        couch_dcp_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq3 - 1, ?DCP_FLAG_NOFLAG,
            TestFun, [], AddStreamFun),
    {ok, PersistedDocs3, _} =
        couch_dcp_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq3, ?DCP_FLAG_DISKONLY,
            TestFun, [], AddStreamFun),
    etap:is(PersistedDocs3, ExpectedDocs3,
        "The persisted sequence number is correct, seq - 1"),
    ok.

% Test robustness when the connection is dropped during a get all sequence
% numbers or stats request
% This is a regression test for MB-15922 and MB-17026
test_close_during_request() ->
    %timer:sleep(1),
    %couch_set_view_test_util:start_server(test_set_name()),
    %setup_test(),

    {auth, User, Passwd} = cb_auth_info:get(),
    {ok, Pid} = couch_dcp_client:start(
        test_set_name(), test_set_name(), User, Passwd, 1024, 0),

    ParentPid = self(),

    % Get sequencenumbers request
    spawn(fun() ->
        couch_dcp_fake_server:close_on_next(),
        Closed = couch_dcp_client:get_seqs(Pid, [0]),
        ParentPid ! {ok, Closed}
    end),
    receive
    {ok, Closed} ->
        etap:is(Closed, {error, dcp_conn_closed},
            "The connection got (as expected) closed "
            "during the get sequence numbers request")
    after 10000 ->
         etap:bail("Cannot get sequence number on time, the DCP client hangs")
    end,

    % Stats request
    spawn(fun() ->
        couch_dcp_fake_server:close_on_next(),
        catch couch_dcp_client:get_num_items(Pid, 0),
        ParentPid ! ok
    end),
    receive
    ok ->
        etap:ok(true,
            "The connection got (as expected) closed during the stats request")
    after 10000 ->
         etap:bail("Cannot get stats on time, the DCP client hangs")
    end,
    ok.


try_until_throttled(Pid, ReqId, N, MaxSize) when N > 0 ->
    ok = couch_dcp_fake_server:send_single_mutation(),
    timer:sleep(1),
    Size2 = gen_server:call(Pid, {get_buffer_size, ReqId}),
    if 
    MaxSize > Size2 ->
        try_until_throttled(Pid, ReqId, N - 1, MaxSize);
    true ->
        ok
    end.

try_until_unthrottled(Pid, ReqId, Size, MaxSize) ->
    Data = couch_dcp_client:get_stream_event(Pid, ReqId),
    Size2 = Size + get_event_size(Data), 
    if
    Size2 < MaxSize ->
        try_until_unthrottled(Pid, ReqId, Size2, MaxSize);
    true ->
        ok
    end.

setup_test() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    populate_set().

is_same_partition(PartId, Docs) ->
    lists:all(fun({_, #dcp_doc{partition = P}}) ->
        P =:= PartId
    end, Docs).

read_streams(Pid, StreamReqs) ->
    lists:foldr(fun(Request, Acc) ->
        case couch_dcp_client:get_stream_event(Pid, Request) of
        {error, vbucket_stream_not_found} ->
            [drained | Acc];
        {Optype, _} = Item ->
            case Optype of
            stream_end ->
                [[] | Acc];
            snapshot_marker ->
                [[] | Acc];
            _ ->
                [[Item] | Acc]
            end;
        _ ->
            [[] | Acc]
        end
    end, [], StreamReqs).

read_mutations(Pid, StreamReqs, Acc) ->
    Items = read_streams(Pid, StreamReqs),
    case lists:all(fun(Item) -> Item =:= drained end, Items) of
    true ->
        Acc;
    false ->
        Acc2 = lists:zipwith(fun(Item, Acc0) ->
            case Item of
            drained ->
                Acc0;
            _ ->
                Item ++ Acc0
            end
        end, Items, Acc),
        read_mutations(Pid, StreamReqs, Acc2)
    end.

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

populate_set() ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    DocList = create_docs(1, num_docs()),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).

delete_docs(StartId, NumDocs) ->
    Dbs = dict:from_list(lists:map(
        fun(I) ->
            {ok, Db} = couch_set_view_test_util:open_set_db(
                test_set_name(), I),
            {I, Db}
        end,
        lists:seq(0, num_set_partitions() - 1))),
    Docs = lists:foldl(
        fun(I, Acc) ->
            Doc = couch_doc:from_json_obj({[
                {<<"meta">>, {[{<<"deleted">>, true}, {<<"id">>, doc_id(I)}]}},
                {<<"json">>, {[]}}
            ]}),
            DocsPerPartition = num_docs() div num_set_partitions(),
            DocList = case orddict:find(I div DocsPerPartition, Acc) of
            {ok, L} ->
                L;
            error ->
                []
            end,
            orddict:store(I div DocsPerPartition, [Doc | DocList], Acc)
        end,
        orddict:new(), lists:seq(StartId, StartId + NumDocs - 1)),
    [] = orddict:fold(
        fun(I, DocList, Acc) ->
            Db = dict:fetch(I, Dbs),
            etap:diag("Deleting " ++ integer_to_list(length(DocList)) ++
                " documents from partition " ++ integer_to_list(I)),
            ok = couch_db:update_docs(Db, DocList, [sort_docs]),
            Acc
        end,
        [], Docs),
    ok = lists:foreach(fun({_, Db}) ->
        ok = couch_db:close(Db)
    end, dict:to_list(Dbs)).


first_uuid(FailoverLog) ->
    element(1, hd(FailoverLog)).

receive_mutation(0, _, _) ->
    ok;
receive_mutation(Count, Pid, Stream) ->
    couch_dcp_fake_server:send_single_mutation(),
    couch_dcp_client:get_stream_event(Pid, Stream),
    receive_mutation(Count - 1, Pid, Stream).

get_event_size({Type, Doc}) ->
    case Type of
        snapshot_mutation ->
            #dcp_doc {
                id = Key,
                body = Value
            } = Doc,
            ?DCP_MSG_SIZE_MUTATION + erlang:external_size(Key) + erlang:external_size(Value);
        stream_end ->
            ?DCP_MSG_SIZE_STREAM_END;
        snapshot_marker ->
            ?DCP_MSG_SIZE_SNAPSHOT;
        snapshot_deletion ->
            #dcp_doc {
                id = Key
            } = Doc,
           ?DCP_MSG_SIZE_DELETION + erlang:external_size(Key)
    end.
