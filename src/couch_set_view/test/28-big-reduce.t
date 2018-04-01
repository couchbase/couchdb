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


test_set_name() -> <<"couch_test_set_big_reduce">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 1024.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(1),
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
    ReduceValue = setup_test(5000),

    populate_set(1, num_docs()),
    {ok, {ViewResults}} = couch_set_view_test_util:query_view(
        test_set_name(), ddoc_id(), <<"testred">>, []),
    [{<<"rows">>,[{[{<<"key">>,null},{<<"value">>, Result}]}]}] = ViewResults,
    etap:is(Result, ReduceValue, "Reduce value is correct"),

    shutdown_group(),
    couch_set_view_test_util:stop_server().


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
                    {<<"reduce">>, <<"function() {return \"",
                        ReduceValue/binary, "\";}">>}
                ]}}
            ]}}
        ]}}
    ]},
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    ok = configure_view_group(),
    ReduceValue.

random_binary(N) ->
    rand:seed(exrop, {1, 2, 3}),
    << <<(rand:uniform(20) + 100):8>> ||  _ <- lists:seq(1, N) >>.


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
    DocList = lists:map(
        fun(I) ->
            Doc = iolist_to_binary(["doc", integer_to_list(I)]),
            {[
                {<<"meta">>, {[{<<"id">>, Doc}]}},
                {<<"json">>, {[{<<"value">>, I}]}}
            ]}
        end,
        lists:seq(From, To)),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


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
