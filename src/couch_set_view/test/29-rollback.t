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

-include_lib("couch_set_view/include/couch_set_view.hrl").


test_set_name() -> <<"couch_test_set_index_rollback">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 128.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(63),
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

    etap:diag("Testing rollback of indexes"),

    test_rollback_once(10),
    %% Test with a header > 4k
    test_rollback_once(5000),
    test_rollback_multiple(3, 5),
    test_rollback_multiple(2, 9),
    test_rollback_different_seqs(5, 8),
    test_rollback_different_seqs(1, 6),
    test_rollback_not_exactly(),
    test_rollback_not_possible(),
    test_rollback_during_compaction(),
    test_rollback_unindexable_seqs(),
    test_rollback_nonexistent(),
    test_rollback_never_existed(),
    test_rollback_mark_for_cleanup(),
    test_rollback_multiple_partitions(),
    test_rollback_multiple_partitions_missing(),
    test_rollback_unindexable_now_indexable_seqs(),

    couch_set_view_test_util:stop_server(),
    ok.


test_rollback_once(ReduceSize) ->
    etap:diag("Testing rollback to a previous header"),

    setup_test(ReduceSize),

    % Populate and build the index with headers

    populate_set(1, num_docs()),
    {ok, {ViewResults1}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, []),
    GroupSeqs1 = get_seq_from_group(),

    populate_set(num_docs() + 1, 2 * num_docs()),
    {ok, {ViewResults2}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, []),
    etap:isnt(ViewResults1, ViewResults2, "Results are different"),
    GroupSeqs2 = get_seq_from_group(),

    % Verify that the latest stored header matches the group
    Fd = get_fd(),
    {ok, HeaderBin1, _Pos1} = couch_file:read_header_bin(Fd),
    Header1 = couch_set_view_util:header_bin_to_term(HeaderBin1),
    HeaderSeqs1 = Header1#set_view_index_header.seqs,
    etap:is(HeaderSeqs1, GroupSeqs2,
        "Latest stored header matches group header"),

    % Rollback the index
    PartId = 0,
    PartSeq = couch_set_view_util:get_part_seq(PartId, GroupSeqs1),
    GroupPid = get_group_pid(),
    ok = couch_set_view_group:rollback(GroupPid, [{PartId, PartSeq}]),
    {ok, HeaderBin3, _Pos3} = couch_file:read_header_bin(Fd),
    Header3 = couch_set_view_util:header_bin_to_term(HeaderBin3),
    HeaderSeqs3 = Header3#set_view_index_header.seqs,
    etap:is(HeaderSeqs3, GroupSeqs1,
        "The most recent header of the truncated file has the correct "
        "sequence numbers"),

    % Query the group (which will make it startup) to see if the truncation
    % was successful
    {ok, {ViewResults3}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, ["stale=ok"]),
    etap:is(ViewResults3, ViewResults1,
            "View returns the correct value after trunction"),
    shutdown_group().


test_rollback_multiple(From, NumRollback) ->
    etap:diag("Testing rollback to a previous header (longer ago)"),
    setup_test(25),

    Inserted = insert_data(From + NumRollback),
    rollback(Inserted, From),
    shutdown_group().


test_rollback_different_seqs(From, NumRollback) ->
    etap:diag("Testing rollback when sequence numbers are different"),
    setup_test(25),

    random:seed({4, 5, 6}),
    Inserted = insert_data_randomly(From + NumRollback),
    rollback(Inserted, From),
    shutdown_group().


test_rollback_not_exactly() ->
    etap:diag("Testing rollback to a previous header which doesn't "
      "match the given sequence number exactly"),
    setup_test(30),

    Inserted = insert_data(3),
    % Get the file descriptor right here as it triggers the updater
    Fd = get_fd(),

    % Pick a sequence number that is between the first and the second batch
    % that was written. The index should than be rolled back to the first one.
    {Seqs1, ViewResult1} = lists:nth(1, Inserted),
    {Seqs2, _} = lists:nth(2, Inserted),
    Seqs = lists:zipwith(fun({PartId, Seq1}, {PartId, Seq2}) ->
        {PartId, Seq1 + ((Seq2 - Seq1) div 2)}
    end, Seqs1, Seqs2),

    % Rollback the index
    PartId = 0,
    PartSeq = couch_set_view_util:get_part_seq(PartId, Seqs),
    GroupPid = get_group_pid(),
    ok = couch_set_view_group:rollback(GroupPid, [{PartId, PartSeq}]),
    {ok, HeaderBin, _Pos3} = couch_file:read_header_bin(Fd),
    Header = couch_set_view_util:header_bin_to_term(HeaderBin),
    HeaderSeqs = Header#set_view_index_header.seqs,
    etap:is(HeaderSeqs, Seqs1,
        "The header of the truncated file has the correct sequence numbers "
        "of the first batch"),

    % Query the group (which will make it startup) to see if the truncation
    % was successful
    {ok, {ViewResult}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, ["stale=ok"]),
    etap:is(ViewResult, ViewResult1,
            "View returns the correct value after trunction"),
    shutdown_group().


test_rollback_not_possible() ->
    etap:diag("Testing rollback to a previous header which doesn't "
      "exist anymore due to compaction"),
    setup_test(30),

    Inserted = insert_data(3),
    Fd1 = get_fd(),
    {ok, HeaderBin1, Pos1} = couch_file:read_header_bin(Fd1),
    Header1 = couch_set_view_util:header_bin_to_term(HeaderBin1),
    HeaderSeqs1 = Header1#set_view_index_header.seqs,

    etap:diag("Triggering compaction"),
    {ok, CompactPid} = couch_set_view_compactor:start_compact(
        mapreduce_view, test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, CompactPid),
    etap:diag("Waiting for main group compaction to finish"),
    receive
    {'DOWN', Ref, process, CompactPid, normal} ->
        ok;
    {'DOWN', Ref, process, CompactPid, noproc} ->
        ok;
    {'DOWN', Ref, process, CompactPid, Reason} ->
        etap:bail("Failure compacting main group: " ++ couch_util:to_list(Reason))
    after 5000 ->
        etap:bail("Timeout waiting for main group compaction to finish")
    end,

    Fd = get_fd(),

    % Verify that the file got compacted
    {ok, HeaderBin2, Pos2} = couch_file:read_header_bin(Fd),
    Header2 = couch_set_view_util:header_bin_to_term(HeaderBin2),
    HeaderSeqs2 = Header2#set_view_index_header.seqs,
    etap:is(HeaderSeqs2, HeaderSeqs1, "The sequence numbers in the "
        "latest header are the same pre and post compaction"),
    etap:ok(Pos2 < Pos1, "Header is further at the front of the file"),

    %  Verify that are no previous headers left
    {ok, HeaderBin3, Pos3} = couch_file:find_header_bin(Fd, Pos2 - 1),
    Header3 = couch_set_view_util:header_bin_to_term(HeaderBin3),
    HeaderSeqs3 = Header3#set_view_index_header.seqs,
    etap:is(Pos3, 0,
        "There are no headers other than the one the file starts with"),
    etap:is(HeaderSeqs3, HeaderSeqs2,
        "The file opening header has the same sequence number "
        "as the current one"),

    % Pick a sequence number that is lower than the one from the header,
    % hence doesn't exist in the file
    PartId = 0,
    PartSeq = couch_set_view_util:get_part_seq(PartId, HeaderSeqs2) - 1,
    GroupPid = get_group_pid(),

    % Rollback the index
    RollbackResult = couch_set_view_group:rollback(
        GroupPid, [{PartId, PartSeq}]),
    etap:is(RollbackResult, {error, cannot_rollback},
        "The header wasn't found as expected"),
    shutdown_group().


test_rollback_during_compaction() ->
    etap:diag("Testing rollback during compaction"),
    setup_test(25),

    Inserted = insert_data(10),

    etap:diag("Triggering compaction"),
    {ok, CompactPid} = couch_set_view_compactor:start_compact(
        mapreduce_view, test_set_name(), ddoc_id(), main),
    Ref = erlang:monitor(process, CompactPid),
    CompactPid ! pause,
    etap:diag("Waiting for main group compaction to finish"),
    receive
    {'DOWN', Ref, process, CompactPid, _Reason} ->
        etap:bail("Compaction finished before it got paused")
    after 0 ->
        ok
    end,
    etap:is(is_process_alive(CompactPid), true, "Compactor is still running"),

    rollback(Inserted, 5),
    receive
    {'DOWN', Ref, process, CompactPid, shutdown} ->
        etap:ok(true, "Compaction was shutdown properly");
    {'DOWN', Ref, process, CompactPid, Reason} ->
        etap:bail("Compaction unexpectedly stopped")
    after 0 ->
        etap:bail("Compaction should have been stopped")
    end,
    etap:is(is_process_alive(CompactPid), false,
        "Compactor is not running after the rollback"),
    shutdown_group().


test_rollback_unindexable_seqs() ->
    etap:diag("Testing rollback with header that contains unindexable "
        "partitions"),
    setup_test(30),

    populate_set(1, num_docs()),
    {ok, {_ViewResults1}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, []),

    % Mark a few partitions as unindexable, insert more data and make
    % sure a new header is written.

    Fd = get_fd(),
    Unindexable = lists:seq(num_set_partitions() div 2, num_set_partitions() - 1),
    ok = couch_set_view:mark_partitions_unindexable(
        mapreduce_view, test_set_name(), ddoc_id(), Unindexable),
    populate_set(num_docs() + 1, 2 * num_docs()),
    trigger_updater(),
    GroupSeqs = get_seq_from_group(),
    GroupSeqsUnindexable = get_unindexable_seq_from_group(),
    etap:is([PartId || {PartId, _} <- GroupSeqsUnindexable], Unindexable,
        "Partitions were set to unindexable"),
    {ok, HeaderBin1, _Pos1} = couch_file:read_header_bin(Fd),
    Header1 = couch_set_view_util:header_bin_to_term(HeaderBin1),
    HeaderSeqs1 = Header1#set_view_index_header.seqs,
    HeaderSeqsUnindexable1 = Header1#set_view_index_header.unindexable_seqs,
    etap:is(HeaderSeqs1, GroupSeqs,
        "The on-disk header has the same indexable sequence numbers as the "
        "group header"),
    etap:is(HeaderSeqsUnindexable1, GroupSeqsUnindexable,
        "The on-disk header has the same unindexable sequence numbers as the "
        "group header"),

    % The most current header now contains unindexable partitions with the
    % same sequence number as the header before it. If it works correctly and
    % takes the unindexable partitions into account, a rollback to that
    % sequence number should keep the file as it is and not actually doing
    % a roll back.

    PartId = num_set_partitions() - 1,
    PartSeq = couch_set_view_util:get_part_seq(PartId, GroupSeqsUnindexable),
    GroupPid = get_group_pid(),
    ok = couch_set_view_group:rollback(GroupPid, [{PartId, PartSeq}]),
    {ok, HeaderBin2, _Pos2} = couch_file:read_header_bin(Fd),
    Header2 = couch_set_view_util:header_bin_to_term(HeaderBin2),
    HeaderSeqs2 = Header2#set_view_index_header.seqs,
    etap:is(HeaderSeqs2, GroupSeqs,
        "The most recent header of the truncated file has the correct "
        "sequence numbers"),
    shutdown_group().


test_rollback_nonexistent() ->
    etap:diag("Testing rollback with old headers not containing the "
      "requested partition (the most recent does)"),

    % Create a view with the last partition missing

    setup_test(30, num_set_partitions() - 1),
    populate_set(1, num_docs()),
    {ok, {_ViewResults1}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, []),
    GroupSeqs1 = get_seq_from_group(),

    PartId = num_set_partitions() - 1,
    etap:is(couch_set_view_util:has_part_seq(PartId, GroupSeqs1), false,
        "Last Partition is currently not part of the index"),

    % Add the last partition, insert more data and make sure a new header
    % is written.

    Fd = get_fd(),
    AllPartitions = lists:seq(0, num_set_partitions() - 1),
    ok = couch_set_view:set_partition_states(
        mapreduce_view, test_set_name(), ddoc_id(), AllPartitions, [], []),
    populate_set(num_docs() + 1, 2 * num_docs()),
    trigger_updater(),
    GroupSeqs2 = get_seq_from_group(),

    % Rollback to a sequence number that is smaller than the current
    % sequence number of the last partition. Verify that it got rolled back
    % to the first header that doesn't contain the partition.

    PartSeq = couch_set_view_util:get_part_seq(PartId, GroupSeqs2) - 1,
    GroupPid = get_group_pid(),
    Rollback = couch_set_view_group:rollback(GroupPid, [{PartId, PartSeq}]),
    {ok, HeaderBin, _Pos} = couch_file:read_header_bin(Fd),
    Header = couch_set_view_util:header_bin_to_term(HeaderBin),
    HeaderSeqs = Header#set_view_index_header.seqs,
    % NOTE vmx 2013-11-05: Having the newly added partition as part of the
    % header it was rolled back to, leverages an implementation detail of the
    % the indexer. Whenever a new partition is added a new header with the
    % same sequence numbers as before plus the new partition with sequence
    % number 0 is written. In case that changes in the future this test fail.
    Expected = GroupSeqs1 ++ [{3, 0}],
    etap:is(HeaderSeqs, Expected, "Rollback contains all partitions"),
    GroupSeqs3 = get_seq_from_group(),
    etap:is(HeaderSeqs, GroupSeqs3,
        "On-disk header has the same sequence numbers as the group"),
    GroupInfo = get_group_info(),
    {active_partitions, ActivePartitions} = lists:keyfind(
        active_partitions, 1, GroupInfo),
    etap:is(ActivePartitions, AllPartitions,
        "The partition the on-disk header is missing is part of the active "
        "partitions as expected"),
    shutdown_group().


test_rollback_never_existed() ->
    etap:diag("Testing rollback with request a partition that the index"
       "never contained"),

    % Create a view with the last partition missing
    setup_test(30, num_set_partitions() - 1),

    Inserted = insert_data(3),
    GroupSeqs = get_seq_from_group(),
    PartId = num_set_partitions() - 1,
    etap:is(couch_set_view_util:has_part_seq(PartId, GroupSeqs), false,
        "Last Partition is not part of the index"),

    % Get header information before the rollback

    Fd = get_fd(),
    {ok, HeaderBin0, _Pos0} = couch_file:read_header_bin(Fd),

    % Rollback

    GroupPid = get_group_pid(),
    % Get a valid sequence number
    Seq = couch_set_view_util:get_part_seq(PartId - 1, GroupSeqs),
    Rollback = couch_set_view_group:rollback(GroupPid, [{PartId, Seq}]),

    % Verify that no rollback happened

    {ok, HeaderBin, _Pos} = couch_file:read_header_bin(Fd),
    Header = couch_set_view_util:header_bin_to_term(HeaderBin),
    HeaderSeqs = Header#set_view_index_header.seqs,
    etap:is(HeaderBin, HeaderBin0, "Header is equal to the old one"),
    etap:is(HeaderSeqs, GroupSeqs,
        "On-disk header has the same sequence numbers as the group"),
    shutdown_group().


test_rollback_mark_for_cleanup() ->
    etap:diag("Testing rollback with old headers containing more partitions "
      "than the current one where the rollback starts"),

    setup_test(30),
    Fd = get_fd(),
    MissingA = num_set_partitions() - 2,
    MissingB = num_set_partitions() - 1,
    MissingPartitions = [MissingA, MissingB],

    populate_set(1, num_docs() div 2),
    trigger_updater(),
    GroupSeqs1 = get_seq_from_group(),
    etap:is(length(GroupSeqs1), num_set_partitions(),
        "All partitions are indexable"),

    % Mark last partition as unindexable
    ok = couch_set_view:mark_partitions_unindexable(
        mapreduce_view, test_set_name(), ddoc_id(), [MissingB]),

    % Several updates need to be happen in order to have a way to roll back
    % to a header where there are unindexable partitions

    populate_set(
        (num_docs() div 2) + 1, (num_docs() div 2) + (num_docs() div 4)),
    trigger_updater(),
    GroupSeqs2 = get_seq_from_group(),
    GroupSeqUnindexable2 = get_unindexable_seq_from_group(),
    etap:is(length(GroupSeqs2), length(GroupSeqs1) - 1,
        "One partitions is not indexable"),
    etap:is(length(GroupSeqUnindexable2), 1, "One partitions is unindexable"),
    populate_set((num_docs() div 2) + (num_docs() div 4) + 1, num_docs()),
    trigger_updater(),
    GroupSeqUnindexable3 = get_unindexable_seq_from_group(),
    etap:is(length(GroupSeqUnindexable3), 1,
        "One partitions is still unindexable"),

    % Remove the two last partitions, insert more data and make sure a
    % new header is written.

    % A partition can only be removed (cleaned up) when it is indexable
    ok = couch_set_view:mark_partitions_indexable(
        mapreduce_view, test_set_name(), ddoc_id(),
        [MissingB]),

    ok = couch_set_view:set_partition_states(
        mapreduce_view, test_set_name(), ddoc_id(), [], [],
        MissingPartitions),

    populate_set(num_docs() + 1, 2 * num_docs()),
    trigger_updater(),
    GroupSeqs4 = get_seq_from_group(),
    etap:is(couch_set_view_util:has_part_seq(MissingA, GroupSeqs4),
        false,
        "The second to last partition is currently not part of the index"),
    etap:is(couch_set_view_util:has_part_seq(MissingB, GroupSeqs4),
        false,
        "Last partition is currently not part of the index"),

    % Rollback to a sequence where the last partition is part of and
    % verify that the additional partition is marked for cleanup and
    % not part of the sequences

    PartId = 0,
    PartSeq = couch_set_view_util:get_part_seq(PartId, GroupSeqs2),
    GroupPid = get_group_pid(),
    ok = couch_set_view_group:rollback(GroupPid, [{PartId, PartSeq}]),

    GroupInfo = get_group_info(),
    {cleanup_partitions, CleanupPartitions} = lists:keyfind(
        cleanup_partitions, 1, GroupInfo),
    etap:is(CleanupPartitions, MissingPartitions,
        "Last partitions were correctly marked for cleanup"),
    {update_seqs, {UpdateSeqs}} = lists:keyfind(update_seqs, 1, GroupInfo),
    etap:is(couch_set_view_util:has_part_seq(MissingA, UpdateSeqs),
        false,
        "Second to last partition is currently not part of the index"),
    etap:is(couch_set_view_util:has_part_seq(MissingB, UpdateSeqs),
        false,
        "Last Partition is currently not part of the index"),
    shutdown_group().


test_rollback_multiple_partitions() ->
    etap:diag("Testing rollback with multiple partitions"),
    setup_test(25),

    random:seed({4, 5, 6}),
    Inserted = insert_data_randomly(8),

    {PartSeqs3, ViewResult3} = lists:nth(3, Inserted),
    {PartSeqs5, ViewResult5} = lists:nth(5, Inserted),
    {PartSeqs6, ViewResult6} = lists:nth(6, Inserted),
    GroupPid = get_group_pid(),
    PartSeqs = [
        {0, couch_set_view_util:get_part_seq(0, PartSeqs5)},
        {1, couch_set_view_util:get_part_seq(1, PartSeqs6)},
        {2, couch_set_view_util:get_part_seq(2, PartSeqs3)}
    ],

    ok = couch_set_view_group:rollback(GroupPid, PartSeqs),

    {ok, {ViewResultTruncated}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, ["stale=ok"]),
    etap:is(ViewResultTruncated, ViewResult3,
            "View returns the correct value after trunction"),
    GroupSeqs = get_seq_from_group(),
    Found = lists:member(GroupSeqs, [Seqs || {Seqs, _} <- Inserted]),
    etap:ok(Found, "The current header matches a previous state"),

    shutdown_group().


test_rollback_multiple_partitions_missing() ->
    etap:diag("Testing rollback with multiple partitions where one partition "
      "was not always part of the index"),

    setup_test(30),
    populate_set(1, num_docs()),
    {ok, {_ViewResults1}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, []),
    GroupSeqs1 = get_seq_from_group(),
    etap:is(length(GroupSeqs1), num_set_partitions(),
        "All partitions are there"),

    % Remove last partition, insert more data and make sure a new header
    % is written.

    MissingPartition = num_set_partitions() - 1,
    ok = couch_set_view:set_partition_states(
        mapreduce_view, test_set_name(), ddoc_id(), [], [],
        [MissingPartition]),
    populate_set(num_docs() + 1, 2 * num_docs()),
    trigger_updater(),
    GroupSeqs2 = get_seq_from_group(),
    etap:is(couch_set_view_util:has_part_seq(MissingPartition, GroupSeqs2),
        false,
        "Last Partition is currently not part of the index"),

    % Add last partition again, insert more data and make sure a new header
    % is written.

    ok = couch_set_view:set_partition_states(
        mapreduce_view, test_set_name(), ddoc_id(),
        [MissingPartition], [], []),
    populate_set(num_docs() + 1, 2 * num_docs()),
    trigger_updater(),
    GroupSeqs3 = get_seq_from_group(),
    etap:is(couch_set_view_util:has_part_seq(MissingPartition, GroupSeqs3),
        true,
        "Last Partition is again part of the index"),

    % Rollback to a sequence that was prior to the removal of the last
    % partition and verify that it did rollback

    GroupPid = get_group_pid(),
    ok = couch_set_view_group:rollback(GroupPid, GroupSeqs1),
    % As the last partition is part of the group, it will be added
    % after the rollback with sequence number 0.
    Expected = lists:keydelete(MissingPartition, 1, GroupSeqs1) ++
        [{MissingPartition, 0}],
    etap:is(get_seq_from_group(), Expected, "Rollback is correct"),
    shutdown_group().


test_rollback_unindexable_now_indexable_seqs() ->
    etap:diag("Testing rollback with header that contains unindexable "
        "partitions which are then made indexable"),
    setup_test(30),

    % Mark a few partitions as unindexable, insert data and make
    % sure a new header is written.

    populate_set(1, num_docs() div 2),
    {ok, {_ViewResults1}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, []),
    Fd = get_fd(),
    Unindexable = lists:seq(num_set_partitions() div 2, num_set_partitions() - 1),
    ok = couch_set_view:mark_partitions_unindexable(
        mapreduce_view, test_set_name(), ddoc_id(), Unindexable),

    populate_set((num_docs() div 2) + 1, num_docs()),
    trigger_updater(),

    GroupSeqs = get_seq_from_group(),
    GroupSeqsUnindexable = get_unindexable_seq_from_group(),
    etap:is([PartId || {PartId, _} <- GroupSeqsUnindexable], Unindexable,
        "Partitions were set to unindexable"),

    % Mark one partition indexable that was previously marked as unindexable,
    % insert more data and make sure a new header is written.

    Indexable = lists:seq(0, num_set_partitions() div 2),
    ok = couch_set_view:mark_partitions_indexable(
        mapreduce_view, test_set_name(), ddoc_id(), Indexable),
    populate_set(num_docs() + 1, 2 * num_docs()),
    trigger_updater(),
    GroupSeqs2 = get_seq_from_group(),
    GroupSeqsUnindexable2 = get_unindexable_seq_from_group(),
    etap:is([PartId || {PartId, _} <- GroupSeqs2], Indexable,
        "The expected partitions are indexable"),
    etap:is(GroupSeqsUnindexable2, tl(GroupSeqsUnindexable),
        "The expected partitions are unindexable"),

    {ok, HeaderBin1, _Pos1} = couch_file:read_header_bin(Fd),
    Header1 = couch_set_view_util:header_bin_to_term(HeaderBin1),
    HeaderSeqs1 = Header1#set_view_index_header.seqs,
    HeaderSeqsUnindexable1 = Header1#set_view_index_header.unindexable_seqs,
    etap:is(HeaderSeqs1, GroupSeqs2,
        "The on-disk header has the same indexable sequence numbers as the "
        "group header"),
    etap:is(HeaderSeqsUnindexable1, GroupSeqsUnindexable2,
        "The on-disk header has the same unindexable sequence numbers as the "
        "group header"),

    % Rollback to the state when more unindexable partitions existed

    PartId = num_set_partitions() div 2,
    PartSeq = couch_set_view_util:get_part_seq(PartId, GroupSeqsUnindexable),
    GroupPid = get_group_pid(),
    ok = couch_set_view_group:rollback(GroupPid, [{PartId, PartSeq}]),

    % Check that the new header contains the same indexable and unindexable
    % partitions as before the rollback. Only their sequence numbers should
    % be different.

    {ok, HeaderBin2, _Pos2} = couch_file:read_header_bin(Fd),
    Header2 = couch_set_view_util:header_bin_to_term(HeaderBin2),
    HeaderSeqs2 = Header2#set_view_index_header.seqs,
    HeaderSeqsUnindexable2 = Header2#set_view_index_header.unindexable_seqs,
    etap:is([PartId || {PartId, _} <- HeaderSeqs2],
        [PartId || {PartId, _} <- GroupSeqs2],
        "The most recent header of the truncated file has the correct "
        "indexable partition IDs"),
    etap:is([PartId || {PartId, _} <- HeaderSeqsUnindexable2],
        [PartId || {PartId, _} <- GroupSeqsUnindexable2],
        "The most recent header of the truncated file has the correct "
        "unindexable partition IDs"),
    etap:isnt([Seq || {_, Seq} <- HeaderSeqs2],
        [Seq || {_, Seq} <- GroupSeqs2],
        "The most recent header of the truncated file has different "
        "indexable partition sequence numbers than the original header"),
    etap:is([Seq || {_, Seq} <- HeaderSeqsUnindexable2],
        [Seq || {_, Seq} <- GroupSeqsUnindexable2],
        "The most recent header of the truncated file has the same "
        "unindexable partition sequence numbers than the original header"),
    shutdown_group().


insert_data(NumBatches) ->
    insert_data(NumBatches, 1, []).
insert_data(SameNum, SameNum, Acc) ->
    lists:reverse(Acc);
insert_data(NumBatches, Count, Acc0) ->
    From = (Count * num_docs()) + 1,
    populate_set(From, From + num_docs()),
    {ok, {ViewResult}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, []),
    GroupSeqs = get_seq_from_group(),
    Acc = [{GroupSeqs, ViewResult}|Acc0],
    insert_data(NumBatches, Count + 1, Acc).

insert_data_randomly(NumBatches) ->
    insert_data_randomly(NumBatches, 1, []).
insert_data_randomly(SameNum, SameNum, Acc) ->
    lists:reverse(Acc);
insert_data_randomly(NumBatches, Count, Acc0) ->
    From = (Count * num_docs()) + 1,
    populate_set_randomly(From, From + num_docs()),
    {ok, {ViewResult}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, []),
    GroupSeqs = get_seq_from_group(),
    Acc = [{GroupSeqs, ViewResult} | Acc0],
    insert_data_randomly(NumBatches, Count + 1, Acc).

rollback(Inserted, From) ->
    {PartSeqs, ViewResult} = lists:nth(From, Inserted),
    PartId = 1,
    PartSeq = couch_set_view_util:get_part_seq(PartId, PartSeqs),
    GroupPid = get_group_pid(),
    ok = couch_set_view_group:rollback(GroupPid, [{PartId, PartSeq}]),

    {ok, {ViewResultTruncated}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, ["stale=ok"]),
    etap:is(ViewResultTruncated, ViewResult,
            "View returns the correct value after trunction"),
    GroupSeqs = get_seq_from_group(),
    Found = lists:member(GroupSeqs, [Seqs || {Seqs, _} <- Inserted]),
    etap:ok(Found, "The current header matches a previous state").


setup_test(ReduceSize) ->
    setup_test(ReduceSize, num_set_partitions()).
setup_test(ReduceSize, NumViewPartitions) ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    ReduceValue = random_binary(ReduceSize),
    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>}
                ]}},
                {<<"testred">>, {[
                    {<<"map">>, <<"function(doc, meta) { emit(meta.id, doc.value); }">>},
                    {<<"reduce">>, <<"function(key, values, rereduce) {\n"
                        "if (rereduce) values = values.map("
                        "function(elem) {return elem[1];});"
                        "var max = Math.max.apply(this, values);"
                        "return [\"", ReduceValue/binary, "\", max];}">>}
                ]}}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    ok = configure_view_group(NumViewPartitions).

random_binary(N) ->
    random:seed({1, 2, 3}),
    << <<(random:uniform(20) + 100):8>> ||  _ <- lists:seq(1, N) >>.


shutdown_group() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
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

populate_set_randomly(From, To) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    DocList = generate_docs(From, To),
    ok = couch_set_view_test_util:populate_set_randomly(
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


get_group_snapshot() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, Group, 0} = gen_server:call(
        GroupPid, #set_view_group_req{stale = false, debug = true}, infinity),
    Group.


get_group_info() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), prod),
    {ok, GroupInfo} = couch_set_view_group:request_group_info(GroupPid),
    GroupInfo.


get_seq_from_group() ->
    GroupInfo = get_group_info(),
    {update_seqs, {Seqs}} = lists:keyfind(update_seqs, 1, GroupInfo),
    [{couch_util:to_integer(PartId), Seq} || {PartId, Seq} <- Seqs].


get_unindexable_seq_from_group() ->
    GroupInfo = get_group_info(),
    {unindexable_partitions, {Seqs}} = lists:keyfind(
        unindexable_partitions, 1, GroupInfo),
    [{couch_util:to_integer(PartId), Seq} || {PartId, Seq} <- Seqs].


get_fd() ->
    Group = get_group_snapshot(),
    Group#set_view_group.fd.


trigger_updater() ->
    GroupPid = get_group_pid(),
    {ok, UpPid} = gen_server:call(GroupPid, {start_updater, []}, infinity),
    UpRef = erlang:monitor(process, UpPid),
    receive
    {'DOWN', UpRef, process, UpPid, {updater_finished, _}} ->
        ok;
    {'DOWN', UpRef, process, UpPid, Reason} ->
        etap:bail("Updater died with unexpected reason: " ++
            couch_util:to_list(Reason))
    after 5000 ->
        etap:bail("Timeout waiting for updater to finish")
    end.
