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

% from couch_db.hrl
-define(MIN_STR, <<>>).
-define(MAX_STR, <<255>>).

-record(view_query_args, {
    start_key,
    end_key,
    start_docid = ?MIN_STR,
    end_docid = ?MAX_STR,
    direction = fwd,
    inclusive_end = true,
    limit = 10000000000,
    skip = 0,
    group_level = 0,
    view_type = nil,
    include_docs = false,
    conflicts = false,
    stale = false,
    multi_get = false,
    callback = nil,
    list = nil,
    run_reduce = true,
    keys = nil,
    view_name = nil,
    debug = false,
    filter = true,
    type = main
}).

-define(etap_match(Got, Expected, Desc),
        etap:fun_is(fun(XXXXXX) ->
            case XXXXXX of Expected -> true; _ -> false end
        end, Got, Desc)).

test_set_name() -> <<"couch_test_set_index_debug_params">>.
num_set_partitions() -> 64.
ddoc_id() -> <<"_design/test">>.
num_docs() -> 1024.  % keep it a multiple of num_set_partitions()


main(_) ->
    test_util:init_code_path(),

    etap:plan(8),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    couch_set_view_test_util:start_server(),
    ok = couch_config:set("set_views", "update_interval", "0", false),

    etap:diag("Testing debug parameters"),
    test_filter_parameter_main(),
    test_filter_parameter_replica(),

    couch_set_view_test_util:stop_server(),
    ok.


setup_test() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(), num_set_partitions()),

    DDoc = {[
        {<<"_id">>, ddoc_id()},
        {<<"language">>, <<"javascript">>},
        {<<"views">>, {[
            {<<"test">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, doc.value); }">>}
            ]}},
            {<<"testred">>, {[
                {<<"map">>, <<"function(doc) { emit(doc._id, doc.value); }">>},
                {<<"reduce">>, <<"_count">>}
            ]}}
        ]}}
    ]},
    populate_set(DDoc).


test_filter_parameter_main() ->
    setup_test(),

    % There are 75% active, 12.5% passive and 12.5% cleanup partitions
    ok = configure_view_group(ddoc_id(), lists:seq(0, 47), lists:seq(48, 55),
        lists:seq(56, 63)),

    {ok, Rows} = (catch query_map_view(<<"test">>, main, true)),
    etap:is(length(Rows), (num_docs())*0.75,
            "Map view query returned all rows from active partitions"),
    {ok, Rows2} = (catch query_map_view(<<"test">>, main, false)),
    etap:is(length(Rows2), num_docs(),
            "Map view query queried with _filter=false returns all "
            "partitions"),

    % The same, but with a reduce function
    {ok, RowsRed} = (catch query_reduce_view(<<"testred">>, main, true)),
    etap:is(RowsRed, (num_docs())*0.75,
            "Reduce view query returned all rows from active partitions"),
    {ok, RowsRed2} = (catch query_reduce_view(<<"testred">>, main, false)),
    etap:is(RowsRed2, num_docs(),
            "Reduce view query queried with _filter=false returns all "
            "partitions"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


test_filter_parameter_replica() ->
    setup_test(),

    ok = configure_view_group(ddoc_id(), lists:seq(0, 47), [], []),
    ok = couch_set_view:add_replica_partitions(test_set_name(), ddoc_id(),
        lists:seq(48, 63)),

    % There are 75% main and 25% replica partitions, from the replica
    % partitions are 87.5% active and 12.5% passive
    ok = configure_replica_group(ddoc_id(), lists:seq(48, 61),
        lists:seq(62, 63)),

    {ok, Rows} = (catch query_map_view(<<"test">>, replica, true)),
    etap:is(length(Rows), (num_docs()*0.25)*0.125,
            "Replica map view query returned all rows from "
            "passive partitions"),

    {ok, Rows2} = (catch query_map_view(<<"test">>, replica, false)),
    % There are 75% main and 25% replica partitions
    etap:is(length(Rows2), (num_docs())*0.25,
            "Replica map view query queried with _filter=false returns all "
            "partitions"),

    % The same, but with a reduce function
    {ok, RowsRed} = (catch query_reduce_view(<<"testred">>, replica, true)),
    etap:is(RowsRed, (num_docs()*0.25)*0.125,
        "Replica reduce view query returned all rows from passive partitions"),
    {ok, RowsRed2} = (catch query_reduce_view(<<"testred">>, replica, false)),
    % There are 75% active and 25% replica partitions
    etap:is(RowsRed2, (num_docs())*0.25,
        "Replica reduce view query queried with _filter=false returns all "
        "partitions"),

    couch_set_view_test_util:delete_set_dbs(test_set_name(), num_set_partitions()).


query_map_view(ViewName, Type, Filter) ->
    etap:diag("Querying map view " ++ binary_to_list(ddoc_id()) ++ "/" ++
        binary_to_list(ViewName)),
    Req = #set_view_group_req{
        stale = false,
        type = Type
    },
    {ok, View, Group, _} = couch_set_view:get_map_view(
        test_set_name(), ddoc_id(), ViewName, Req),

    FoldFun = fun({{Key, DocId}, {_PartId, Value}}, _, Acc) ->
        {ok, [{{Key, DocId}, Value} | Acc]}
    end,
    ViewArgs = #view_query_args{
        run_reduce = true,
        view_name = ViewName,
        filter = Filter
    },
    {ok, _, Rows} = couch_set_view:fold(Group, View, FoldFun, [], ViewArgs),
    couch_set_view:release_group(Group),
    {ok, Rows}.


query_reduce_view(ViewName, Type, Filter) ->
    etap:diag("Querying reduce view " ++ binary_to_list(ddoc_id()) ++ "/" ++
        binary_to_list(ViewName) ++ "with ?group=true"),
    Req = #set_view_group_req{
        stale = false,
        type = Type
    },
    {ok, View, Group, _} = couch_set_view:get_reduce_view(
        test_set_name(), ddoc_id(), ViewName, Req),

    KeyGroupFun = fun({_Key1, _}, {_Key2, _}) -> true end,
    FoldFun = fun(Key, Red, Acc) -> {ok, [{Key, Red} | Acc]} end,
    ViewArgs = #view_query_args{
        run_reduce = true,
        view_name = ViewName,
        filter = Filter
    },
    {ok, Rows} = couch_set_view:fold_reduce(Group, View, FoldFun, [],
        KeyGroupFun, ViewArgs),
    couch_set_view:release_group(Group),
    case Rows of
    [{_Key, RedValue}] ->
        {ok, RedValue};
    [] ->
        empty
    end.


populate_set(DDoc) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    DocList = lists:map(
        fun(I) ->
            {[
                {<<"_id">>, iolist_to_binary(["doc", integer_to_list(I)])},
                {<<"value">>, I}
            ]}
        end,
        lists:seq(1, num_docs())),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


configure_view_group(DDocId, Active, Passive, Cleanup) ->
    etap:diag("Configuring view group"),
    Params = #set_view_params{
        max_partitions = num_set_partitions(),
        % Set all partitions to active an later on passive/cleanup
        active_partitions = lists:seq(0, num_set_partitions()-1),
        passive_partitions = [],
        use_replica_index = true
    },
    try
        couch_set_view:define_group(test_set_name(), DDocId, Params),

        GroupPid = couch_set_view:get_group_pid(test_set_name(), ddoc_id()),
        ok = gen_server:call(GroupPid, {set_auto_cleanup, false}, infinity),
        % Somehow the set_auto_cleanup doesn't get set when I don't request
        % the group afterwards
        Req = #set_view_group_req{stale = false},
        {ok, Group} = couch_set_view:get_group(test_set_name(), DDocId, Req),

        couch_set_view_group:set_state(GroupPid, Active, Passive, Cleanup)
    catch _:Error ->
        Error
    end.


configure_replica_group(DDocId, Active, Passive) ->
    etap:diag("Configuring replica group"),
    Req = #set_view_group_req{stale = false},
    {ok, Group} = couch_set_view:get_group(test_set_name(), DDocId, Req),

    couch_set_view_group:set_state(
        Group#set_view_group.replica_pid, Active, Passive, []),
    couch_set_view:release_group(Group).
