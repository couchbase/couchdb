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

-include_lib("couch_upr/include/couch_upr.hrl").

test_set_name() -> <<"couch_test_couch_upr_client">>.
num_set_partitions() -> 4.
num_docs() -> 1000.


main(_) ->
    test_util:init_code_path(),

    etap:plan(44),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    %init:stop(),
    %receive after infinity -> ok end,
    ok.


test() ->
    couch_set_view_test_util:start_server(test_set_name()),
    setup_test(),
    % Populate failover log
    FailoverLogs = lists:map(fun(PartId) ->
        FailoverLog = [
            {10001, PartId + 3}, {10002, PartId + 2}, {10003, 0}],
        couch_upr_fake_server:set_failover_log(PartId, FailoverLog),
        FailoverLog
    end, lists:seq(0, num_set_partitions() - 1)),

    TestFun = fun(Item, Acc) ->
        case Item of
        {snapshot_marker, _} ->
            Acc;
        _ ->
            Acc ++ [Item]
        end
    end,

    {auth, User, Passwd} = cb_auth_info:get(),
    {ok, Pid} = couch_upr_client:start(
        test_set_name(), test_set_name(), User, Passwd),

    % Get the latest partition version first
    {ok, InitialFailoverLog0} = couch_upr_client:get_failover_log(Pid, 0),
    etap:is(InitialFailoverLog0, hd(FailoverLogs), "Failover log is correct"),

    % First parameter is the partition, the second is the sequence number
    % to start at.
    {ok, Docs1, FailoverLog1} = couch_upr_client:enum_docs_since(
        Pid, 0, InitialFailoverLog0, 4, 10, ?UPR_FLAG_NOFLAG, TestFun, []),
    etap:is(length(Docs1), 6, "Correct number of docs (6) in partition 0"),
    etap:is(FailoverLog1, lists:nth(1, FailoverLogs),
        "Failoverlog from partition 0 is correct"),

    {ok, InitialFailoverLog1} = couch_upr_client:get_failover_log(Pid, 1),
    {ok, Docs2, FailoverLog2} = couch_upr_client:enum_docs_since(
        Pid, 1, InitialFailoverLog1, 46, 165, ?UPR_FLAG_NOFLAG, TestFun, []),
    etap:is(length(Docs2), 119, "Correct number of docs (109) partition 1"),
    etap:is(FailoverLog2, lists:nth(2, FailoverLogs),
        "Failoverlog from partition 1 is correct"),

    {ok, InitialFailoverLog2} = couch_upr_client:get_failover_log(Pid, 2),
    {ok, Docs3, FailoverLog3} = couch_upr_client:enum_docs_since(
        Pid, 2, InitialFailoverLog2, 80, num_docs() div num_set_partitions(),
        ?UPR_FLAG_NOFLAG, TestFun, []),
    Expected3 = (num_docs() div num_set_partitions()) - 80,
    etap:is(length(Docs3), Expected3,
        io_lib:format("Correct number of docs (~p) partition 2", [Expected3])),
    etap:is(FailoverLog3, lists:nth(3, FailoverLogs),
        "Failoverlog from partition 2 is correct"),

    {ok, InitialFailoverLog3} = couch_upr_client:get_failover_log(Pid, 3),
    {ok, Docs4, FailoverLog4} = couch_upr_client:enum_docs_since(
        Pid, 3, InitialFailoverLog3, 0, 5, ?UPR_FLAG_NOFLAG, TestFun, []),
    etap:is(length(Docs4), 5, "Correct number of docs (5) partition 3"),
    etap:is(FailoverLog4, lists:nth(4, FailoverLogs),
        "Failoverlog from partition 3 is correct"),

    % Try a too high sequence number to get a erange error response
    {error, ErangeError} = couch_upr_client:enum_docs_since(
        Pid, 0, InitialFailoverLog0, 400, 450, ?UPR_FLAG_NOFLAG, TestFun, []),
    etap:is(ErangeError, wrong_start_sequence_number,
        "Correct error message for too high sequence number"),
    % Start sequence is bigger than end sequence
    {error, ErangeError2} = couch_upr_client:enum_docs_since(
        Pid, 0, InitialFailoverLog0, 5, 2, ?UPR_FLAG_NOFLAG, TestFun, []),
    etap:is(ErangeError2, wrong_start_sequence_number,
        "Correct error message for start sequence > end sequence"),


    Error = couch_upr_client:enum_docs_since(
        Pid, 1, [{4455667788, 1243}], 46, 165, ?UPR_FLAG_NOFLAG, TestFun, []),
    etap:is(Error, {rollback, 0},
        "Correct error for wrong failover log"),

    {ok, Seq0} = couch_upr_client:get_sequence_number(Pid, 0),
    etap:is(Seq0, num_docs() div num_set_partitions(),
        "Sequence number of partition 0 is correct"),
    {ok, Seq1} = couch_upr_client:get_sequence_number(Pid, 1),
    etap:is(Seq1, num_docs() div num_set_partitions(),
        "Sequence number of partition 1 is correct"),
    {ok, Seq2} = couch_upr_client:get_sequence_number(Pid, 2),
    etap:is(Seq2, num_docs() div num_set_partitions(),
         "Sequence number of partition 2 is correct"),
    {ok, Seq3} = couch_upr_client:get_sequence_number(Pid, 3),
    etap:is(Seq3, num_docs() div num_set_partitions(),
        "Sequence number of partition 3 is correct"),
    SeqError = couch_upr_client:get_sequence_number(Pid, 100000),
    etap:is(SeqError, {error, not_my_vbucket},
        "Too high partition number returns correct error"),


    % Test snapshot markers types

    TestSnapshotFun = fun(Item, Acc) ->
        case Item of
        {snapshot_marker, Marker} ->
            Acc ++ [Marker];
        _ ->
            Acc
        end
    end,

    SnapshotStart1 = 0,
    SnapshotEnd1 = num_docs() div (num_set_partitions() * 2),
    {ok, Markers1, SnapshotFailoverLog1} = couch_upr_client:enum_docs_since(
        Pid, 2, [{0, 0}], SnapshotStart1, SnapshotEnd1, ?UPR_FLAG_NOFLAG,
        TestSnapshotFun, []),
    ExpectedMarkers1 = [{SnapshotStart1, SnapshotEnd1,
        ?UPR_SNAPSHOT_TYPE_DISK}],
    etap:is(Markers1, ExpectedMarkers1,
        "Received one on-disk snapshot marker"),

    SnapshotStart2 = num_docs() div (num_set_partitions() * 2),
    SnapshotEnd2 = num_docs() div num_set_partitions(),
    {ok, Markers2, SnapshotFailoverLog2} = couch_upr_client:enum_docs_since(
        Pid, 2, SnapshotFailoverLog1, SnapshotStart2, SnapshotEnd2,
        ?UPR_FLAG_NOFLAG, TestSnapshotFun, []),
    ExpectedMarkers2 = [{SnapshotStart2, SnapshotEnd2,
        ?UPR_SNAPSHOT_TYPE_MEMORY}],
    etap:is(Markers2, ExpectedMarkers2,
        "Received one in-memory snapshot marker"),


    % Test multiple streams in parallel
    {StreamReq0, {failoverlog, InitialFailoverLog0}} =
        couch_upr_client:add_stream(
            Pid, 0, first_uuid(InitialFailoverLog0), 10, 100,
            ?UPR_FLAG_NOFLAG),

    {StreamReq1, {failoverlog, InitialFailoverLog1}} =
        couch_upr_client:add_stream(
            Pid, 1, first_uuid(InitialFailoverLog1), 100, 200,
            ?UPR_FLAG_NOFLAG),

    {StreamReq2, {failoverlog, InitialFailoverLog2}} =
        couch_upr_client:add_stream(
            Pid, 2, first_uuid(InitialFailoverLog2), 0, 10, ?UPR_FLAG_NOFLAG),

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

    StreamList0 = couch_upr_client:list_streams(Pid),
    etap:is(StreamList0, [], "Stream list is empty"),

    couch_upr_fake_server:pause_mutations(),
    {StreamReq0_2, {failoverlog, InitialFailoverLog0}} =
        couch_upr_client:add_stream(
            Pid, 0, first_uuid(InitialFailoverLog0), 1, 100, ?UPR_FLAG_NOFLAG),

    {_, StreamResp0_3} =
        couch_upr_client:add_stream(
            Pid, 0, first_uuid(InitialFailoverLog0), 10, 100,
            ?UPR_FLAG_NOFLAG),
    etap:is(StreamResp0_3, {error,vbucket_stream_already_exists},
        "Stream for vbucket 0 already exists"),
    couch_upr_fake_server:continue_mutations(),

    % Drain all mutations
    read_mutations(Pid, [StreamReq0_2], [[]]),

    couch_upr_fake_server:pause_mutations(),
    couch_upr_client:add_stream(
        Pid, 1, first_uuid(InitialFailoverLog1), 10, 300, ?UPR_FLAG_NOFLAG),

    couch_upr_client:add_stream(
        Pid, 2, first_uuid(InitialFailoverLog2), 100, 200, ?UPR_FLAG_NOFLAG),

    StreamList1 = couch_upr_client:list_streams(Pid),
    etap:is(StreamList1, [1,2], "Stream list contains parititon 1,2"),

    StreamRemoveResp0 = couch_upr_client:remove_stream(Pid, 1),
    StreamList2 = couch_upr_client:list_streams(Pid),
    etap:is({StreamRemoveResp0, StreamList2}, {ok, [2]},
        "Removed parititon stream 1 and parition stream 2 is left"),

    StreamRemoveResp1 = couch_upr_client:remove_stream(Pid, 1),
    etap:is(StreamRemoveResp1, {error, vbucket_stream_not_found},
        "Correct error on trying to remove non-existing stream"),
    couch_upr_fake_server:continue_mutations(),

    % Test with too large failover log
    TooLargeFailoverLog = [{I, I} ||
        I <- lists:seq(0, ?UPR_MAX_FAILOVER_LOG_SIZE)],
    PartId = 1,
    couch_upr_fake_server:set_failover_log(PartId, TooLargeFailoverLog),
    TooLargeError = couch_upr_client:enum_docs_since(
          Pid, PartId, [{0, 0}], 0, 100, ?UPR_FLAG_NOFLAG, TestFun, []),
    etap:is(TooLargeError, {error, too_large_failover_log},
        "Too large failover log returns correct error"),

    % Remove existing streams
    couch_upr_client:remove_stream(Pid, 1),
    couch_upr_client:remove_stream(Pid, 2),

    % Tests for flow control
    couch_upr_fake_server:pause_mutations(),
    couch_upr_client:set_buffer_size(Pid, 50),

    {StreamReq0_4, _} = couch_upr_client:add_stream(
        Pid, 0, first_uuid(InitialFailoverLog0), 0, 500, ?UPR_FLAG_NOFLAG),

    Throttled0 = try_until_throttled(Pid, 100),
    etap:is(Throttled0, true, "Throttled stream events queue when buffer became full"),

    Throttled1 = try_until_unthrottled(Pid, StreamReq0_4, 25),
    etap:is(Throttled1, false, "Throttling disabled when drained buffered events queue"),
    couch_upr_client:remove_stream(Pid, 0),

    couch_upr_fake_server:pause_mutations(),
    {StreamReq1_2, _} = couch_upr_client:add_stream(
        Pid, 1, first_uuid(InitialFailoverLog1), 0, 500, ?UPR_FLAG_NOFLAG),

    ReqPid = spawn(fun() ->
        couch_upr_client:get_stream_event(Pid, StreamReq1_2)
        end),

    EvResponse = couch_upr_client:get_stream_event(Pid, StreamReq1_2),
    etap:is(EvResponse, {error, event_request_already_exists},
        "Error message received when requested multiple times for same stream's event"),

    exit(ReqPid, shutdown),
    couch_upr_client:remove_stream(Pid, 1),
    couch_upr_fake_server:continue_mutations(),

    % Test get_num_items

    {ok, NumItems0} = couch_upr_client:get_num_items(Pid, 0),
    etap:is(NumItems0, num_docs() div num_set_partitions(),
        "Number of items of partition 0 is correct"),
    {ok, NumItems1} = couch_upr_client:get_num_items(Pid, 1),
    etap:is(NumItems1, num_docs() div num_set_partitions(),
        "Number of items of partition 1 is correct"),
    {ok, NumItems2} = couch_upr_client:get_num_items(Pid, 2),
    etap:is(NumItems2, num_docs() div num_set_partitions(),
         "Number of items of partition 2 is correct"),
    {ok, NumItems3} = couch_upr_client:get_num_items(Pid, 3),
    etap:is(NumItems3, num_docs() div num_set_partitions(),
        "Number of items of partition 3 is correct"),
    NumItemsError = couch_upr_client:get_num_items(Pid, 100000),
    etap:is(NumItemsError, {error, not_my_vbucket},
        "Too high partition number returns correct error"),

    DocsPerPartition = num_docs() div num_set_partitions(),
    NumDelDocs0 = num_docs() div (num_set_partitions() * 2),
    delete_docs(1, NumDelDocs0),
    {ok, NumItemsDel0} = couch_upr_client:get_num_items(Pid, 0),
    etap:is(NumItemsDel0, DocsPerPartition - NumItemsDel0,
        "Number of items of partition 0 after some deletions is correct"),
    NumDelDocs3 = num_docs() div (num_set_partitions() * 4),
    delete_docs(3 * DocsPerPartition + 1, NumDelDocs3),
    {ok, NumItemsDel3} = couch_upr_client:get_num_items(Pid, 3),
    etap:is(NumItemsDel3, DocsPerPartition - NumDelDocs3,
        "Number of items of partition 3 after some deletions is correct"),

    % Tests for requesting persisted items only

    couch_upr_fake_server:set_persisted_items_fun(fun(Seq) -> Seq  end),
    {ok, HighSeq1} = couch_upr_client:get_sequence_number(Pid, 1),
    {ok, ExpectedDocs1, _} =
        couch_upr_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq1, ?UPR_FLAG_NOFLAG,
            TestFun, []),
    {ok, PersistedDocs1, _} =
        couch_upr_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq1, ?UPR_FLAG_DISKONLY,
            TestFun, []),
    etap:is(PersistedDocs1, ExpectedDocs1,
        "The persisted sequence number is correct, seq"),

    couch_upr_fake_server:set_persisted_items_fun(
       fun(Seq) -> Seq div 2 end),
    {ok, HighSeq2} = couch_upr_client:get_sequence_number(Pid, 1),
    {ok, ExpectedDocs2, _} =
        couch_upr_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq2 div 2, ?UPR_FLAG_NOFLAG,
            TestFun, []),
    {ok, PersistedDocs2, _} =
        couch_upr_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq2, ?UPR_FLAG_DISKONLY,
            TestFun, []),
    etap:is(PersistedDocs2, ExpectedDocs2,
        "The persisted sequence number is correct, seq/2"),

    couch_upr_fake_server:set_persisted_items_fun(fun(Seq) -> Seq - 1 end),
    {ok, HighSeq3} = couch_upr_client:get_sequence_number(Pid, 1),
    {ok, ExpectedDocs3, _} =
        couch_upr_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq3 - 1, ?UPR_FLAG_NOFLAG,
            TestFun, []),
    {ok, PersistedDocs3, _} =
        couch_upr_client:enum_docs_since(
            Pid, 0, InitialFailoverLog0, 0, HighSeq3, ?UPR_FLAG_DISKONLY,
            TestFun, []),
    etap:is(PersistedDocs3, ExpectedDocs3,
        "The persisted sequence number is correct, seq - 1"),

    couch_set_view_test_util:stop_server(),
    ok.


try_until_throttled(_Pid, 0) ->
    false;
try_until_throttled(Pid, N) ->
    Throttled = gen_server:call(Pid, throttled),
    case Throttled of
    true ->
        true;
    false ->
        ok = couch_upr_fake_server:send_single_mutation(),
        timer:sleep(1),
        try_until_throttled(Pid, N-1)
    end.

try_until_unthrottled(_Pid, _ReqId, 0) ->
    true;
try_until_unthrottled(Pid, ReqId, N) ->
    Throttled = gen_server:call(Pid, throttled),
    case Throttled of
    false ->
        false;
    true ->
        couch_upr_client:get_stream_event(Pid, ReqId),
        try_until_unthrottled(Pid, ReqId, N-1)
    end.


setup_test() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    populate_set().

is_same_partition(PartId, Docs) ->
    lists:all(fun({_, #upr_doc{partition = P}}) ->
        P =:= PartId
    end, Docs).

read_streams(Pid, StreamReqs) ->
    lists:foldr(fun(Request, Acc) ->
        case couch_upr_client:get_stream_event(Pid, Request) of
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
