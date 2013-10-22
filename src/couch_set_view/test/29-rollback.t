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

    etap:plan(23),
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
    GroupPid = get_group_pid(),
    ok = couch_set_view_group:rollback(
        GroupPid, PartId, couch_set_view_util:get_part_seq(PartId, GroupSeqs1)),
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
    GroupPid = get_group_pid(),
    ok = couch_set_view_group:rollback(
        GroupPid, PartId, couch_set_view_util:get_part_seq(PartId, Seqs)),
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
    RollbackResult = couch_set_view_group:rollback(GroupPid, PartId, PartSeq),
    etap:is(RollbackResult, {error, cannot_rollback},
        "The header wasn't found as expected"),
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
    ok = couch_set_view_group:rollback(GroupPid, PartId, PartSeq),

    {ok, {ViewResultTruncated}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, ["stale=ok"]),
    etap:is(ViewResultTruncated, ViewResult,
            "View returns the correct value after trunction"),
    GroupSeqs = get_seq_from_group(),
    Found = lists:member(GroupSeqs, [Seqs || {Seqs, _} <- Inserted]),
    etap:ok(Found, "The current header matches a previous state").


setup_test(ReduceSize) ->
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
    ok = configure_view_group().

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


configure_view_group() ->
    etap:diag("Configuring view group"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        active_partitions = lists:seq(0, num_set_partitions()-1),
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


get_seq_from_group() ->
    Group = get_group_snapshot(),
    (Group#set_view_group.index_header)#set_view_index_header.seqs.


get_fd() ->
    Group = get_group_snapshot(),
    Group#set_view_group.fd.
