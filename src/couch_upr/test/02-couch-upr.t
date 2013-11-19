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

test_set_name() -> <<"couch_test_couch_upr">>.
num_set_partitions() -> 4.
num_docs() -> 1000.


main(_) ->
    test_util:init_code_path(),

    etap:plan(11),
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
            {<<"abcdefgh">>, PartId + 3}, {<<"b123xfqw">>, PartId + 2},
            {<<"ccddeeff">>, 0}],
        couch_upr_fake_server:set_failover_log(PartId, FailoverLog),
        FailoverLog
    end, lists:seq(0, num_set_partitions() - 1)),

    TestFun = fun(Item, Acc) ->
        [Item|Acc]
    end,

    {ok, Pid} = couch_upr:start(test_set_name()),

    % Get the latest partition version first
    {ok, InitialFailoverLog0} = couch_upr:get_failover_log(Pid, 0),
    PartVersion0 = hd(InitialFailoverLog0),
    etap:is(PartVersion0, {<<"abcdefgh">>, 3}, "Partition version is correct"),

    % First parameter is the partition, the second is the sequence number
    % to start at.
    {ok, Docs1, FailoverLog1} = couch_upr:enum_docs_since(
        Pid, 0, PartVersion0, 4, 10, TestFun, []),
    etap:is(length(Docs1), 6, "Correct number of docs (6) in partition 0"),
    etap:is(FailoverLog1, lists:nth(1, FailoverLogs),
        "Failoverlog from partition 0 is correct"),

    {ok, InitialFailoverLog1} = couch_upr:get_failover_log(Pid, 1),
    PartVersion1 = hd(InitialFailoverLog1),
    {ok, Docs2, FailoverLog2} = couch_upr:enum_docs_since(
        Pid, 1, PartVersion1, 46, 165, TestFun, []),
    etap:is(length(Docs2), 119, "Correct number of docs (109) partition 1"),
    etap:is(FailoverLog2, lists:nth(2, FailoverLogs),
        "Failoverlog from partition 1 is correct"),

    {ok, InitialFailoverLog2} = couch_upr:get_failover_log(Pid, 2),
    PartVersion2 = hd(InitialFailoverLog2),
    {ok, Docs3, FailoverLog3} = couch_upr:enum_docs_since(
        Pid, 2, PartVersion2, 80, num_docs() div num_set_partitions(),
        TestFun, []),
    Expected3 = (num_docs() div num_set_partitions()) - 80,
    etap:is(length(Docs3), Expected3,
        io_lib:format("Correct number of docs (~p) partition 2", [Expected3])),
    etap:is(FailoverLog3, lists:nth(3, FailoverLogs),
        "Failoverlog from partition 2 is correct"),

    {ok, InitialFailoverLog3} = couch_upr:get_failover_log(Pid, 3),
    PartVersion3 = hd(InitialFailoverLog3),
    {ok, Docs4, FailoverLog4} = couch_upr:enum_docs_since(
        Pid, 3, PartVersion3, 0, 5, TestFun, []),
    etap:is(length(Docs4), 5, "Correct number of docs (5) partition 3"),
    etap:is(FailoverLog4, lists:nth(4, FailoverLogs),
        "Failoverlog from partition 3 is correct"),

    % Try a too high sequence number to get a rollback response
    {rollback, RollbackSeq} = couch_upr:enum_docs_since(
        Pid, 0, PartVersion0, 400, 450, TestFun, []),
    etap:is(RollbackSeq, num_docs() div num_set_partitions(),
        "Correct rollback sequence number"),

    {error, Error} = couch_upr:enum_docs_since(
        Pid, 1, {<<"wrong123">>, 1243}, 46, 165, TestFun, []),
    etap:is(Error, wrong_partition_version,
        "Correct error for wrong partition version"),

    couch_set_view_test_util:stop_server(),
    ok.

setup_test() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),
    populate_set().


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
