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

test_set_name() -> <<"couch_test_set_index_meta_params">>.
num_set_partitions() -> 4.
ddoc_id() -> <<"_design/dev_test">>.
num_docs() -> 1024.  % keep it a multiple of num_set_partitions()
docs_per_partition() -> num_docs() div num_set_partitions().


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

    etap:diag("Testing development views with meta params"),

    % Test for PartId (a.k.a vbucket) to which document hash into
    test_map_query_vb(0),
    test_map_query_vb(1),
    test_map_query_vb(2),
    test_map_query_vb(3),

    % Test for current seq number of documents
    test_map_query_seq(0),
    test_map_query_seq(1),
    test_map_query_seq(2),
    test_map_query_seq(3),

    % Test if seq number changes after documents are updated
    test_map_query_updated(0),
    test_map_query_updated(1),
    test_map_query_updated(2),
    test_map_query_updated(3),

    % Test xattrs when document does not contain extra attribute
    test_map_query_xattrs(0, false, false, false),
    test_map_query_xattrs(1, false, false, false),
    test_map_query_xattrs(2, false, false, false),
    test_map_query_xattrs(3, false, false, false),

    % Test xattrs when document contains extra attribute
    test_map_query_xattrs(0, true, false, true),
    test_map_query_xattrs(1, true, false, true),
    test_map_query_xattrs(2, true, false, true),
    test_map_query_xattrs(3, true, false, true),

    % Test xattrs when document is deleted
    % and does not contain extra attribute
    test_map_query_xattrs(0, false, true, false),
    test_map_query_xattrs(1, false, true, false),
    test_map_query_xattrs(2, false, true, false),
    test_map_query_xattrs(3, false, true, false),

    % Test xattrs when document is deleted
    % and contains extra attribute that are indexed
    test_map_query_xattrs(0, true, true, true),
    test_map_query_xattrs(1, true, true, true),
    test_map_query_xattrs(2, true, true, true),
    test_map_query_xattrs(3, true, true, true),

    % Test xattrs when document is deleted
    % and contains extra attribute that are not to be indexed
    test_map_query_xattrs(0, true, true, false),
    test_map_query_xattrs(1, true, true, false),
    test_map_query_xattrs(2, true, true, false),
    test_map_query_xattrs(3, true, true, false),



    couch_set_view_test_util:delete_set_dbs(test_set_name(),
        num_set_partitions()),
    couch_set_view_test_util:stop_server(),
    ok.


test_map_query_vb(PartitionId) ->
    setup_test_vb(),
    ok = configure_view_group(ddoc_id(), PartitionId),

    {ok, Rows} = (catch query_map_view(<<"test">>)),
    etap:is(length(Rows), docs_per_partition(),
        "Got " ++ integer_to_list(docs_per_partition()) ++ " view rows"),
    verify_rows_vb(Rows, PartitionId),

    shutdown_group().

test_map_query_seq(PartitionId) ->
    setup_test_seq(),
    ok = configure_view_group(ddoc_id(), PartitionId),

    {ok, Rows} = (catch query_map_view(<<"test">>)),
    etap:is(length(Rows), docs_per_partition(),
        "Got " ++ integer_to_list(docs_per_partition()) ++ " view rows"),
    verify_rows_seq(Rows, PartitionId, 1, docs_per_partition()),

    shutdown_group().

test_map_query_updated(PartitionId) ->
    setup_test_seq(),
    update_docs(),
    ok = configure_view_group(ddoc_id(), PartitionId),

    {ok, Rows} = (catch query_map_view(<<"test">>)),
    etap:is(length(Rows), docs_per_partition(),
        "Got " ++ integer_to_list(docs_per_partition()) ++ " view rows"),
    verify_rows_seq(Rows, PartitionId, (1 + docs_per_partition()),
        (2 * docs_per_partition())),

    shutdown_group().

verify_zero_rows(Rows) ->
    etap:is(length(Rows), 0,
        "Got " ++ integer_to_list(0) ++ " view rows").

verify_nonzero_rows(Rows, PartitionId, HasXattrs) ->
    etap:is(length(Rows), docs_per_partition(),
        "Got " ++ integer_to_list(docs_per_partition()) ++ " view rows"),
    verify_rows_xattrs(Rows, PartitionId, HasXattrs).

test_map_query_xattrs(PartitionId, HasXattrs, Deleted, IndexXattrs) ->
    setup_test_xattrs(HasXattrs, Deleted, IndexXattrs),
    ok = configure_view_group(ddoc_id(), PartitionId),

    {ok, Rows} = (catch query_map_view(<<"test">>)),
    case HasXattrs of
    true ->
        case Deleted of
        false ->
            verify_nonzero_rows(Rows, PartitionId, HasXattrs);
        true ->
            case IndexXattrs of
            true ->
                verify_nonzero_rows(Rows, PartitionId, HasXattrs);
            false ->
                verify_zero_rows(Rows)
            end
        end;
    false ->
        case Deleted of
        true ->
            verify_zero_rows(Rows);
        false ->
            verify_nonzero_rows(Rows, PartitionId, HasXattrs)
        end
    end,
    shutdown_group().

% As the partitions are populated sequentially we can easily verify them
verify_rows_vb(Rows, PartitionId) ->
    Offset = (PartitionId * docs_per_partition()),
    PartId = list_to_binary(integer_to_list(PartitionId)),
    DocList = lists:map(fun(Doc) ->
        {[{<<"meta">>, {[{<<"deleted">>, false}, {<<"id">>, DocId}]}},
          {<<"json">>, {[{<<"value">>, _Value}]}}]} = Doc,
        {<<"\"", DocId/binary, "\"">>, DocId,
            <<"\"", PartId/binary, "\"">>}
    end, create_docs(1 + Offset, Offset + docs_per_partition(), false)),
    etap:is(Rows, lists:sort(DocList), "Returned correct rows").

verify_rows_seq(Rows, PartitionId, From, To) ->
    Offset = (PartitionId * docs_per_partition()),
    DocList = lists:zipwith(fun(Doc, I) ->
        {[{<<"meta">>, {[{<<"deleted">>, false}, {<<"id">>, DocId}]}},
          {<<"json">>, {[{<<"value">>, _Value}]}}]} = Doc,
          Seq = list_to_binary(integer_to_list(I)),
        {<<"\"", DocId/binary, "\"">>, DocId,
            <<"\"", Seq/binary, "\"">>}
    end, lists:sort(create_docs(1 + Offset, Offset + docs_per_partition(), false)),
         lists:seq(From, To)),
    etap:is(Rows, lists:sort(DocList), "Returned correct rows").

verify_rows_xattrs(Rows, PartitionId, HasXattrs) ->
    Offset = (PartitionId * docs_per_partition()),
    DocList = lists:zipwith(fun(Doc, I) ->
        {[{<<"meta">>, {[{<<"deleted">>, false}, {<<"id">>, DocId}]}},
          {<<"json">>, {[{<<"value">>, _Value}]}}]} = Doc,
        Id = list_to_binary(integer_to_list(I)),
        case HasXattrs of
        true ->
            {<<"\"", DocId/binary, "\"">>, DocId, <<"{\"xattr_key\":",Id/binary, "}">>};
        false ->
            {<<"\"", DocId/binary, "\"">>, DocId, <<"{}">>}
        end
    end, create_docs(1 + Offset, Offset + docs_per_partition(), false),
         lists:seq(1+Offset, Offset + docs_per_partition())),
    etap:is(Rows, lists:sort(DocList), "Returned correct rows").


query_map_view(ViewName) ->
    etap:diag("Querying map view " ++ binary_to_list(ddoc_id()) ++ "/" ++
        binary_to_list(ViewName)),
    Req = #set_view_group_req{
        stale = false,
        category = dev
    },
    {ok, View, Group, _} = couch_set_view:get_map_view(
        test_set_name(), ddoc_id(), ViewName, Req),

    FoldFun = fun({{{json, Key}, DocId}, {_PartId, {json, Value}}}, _, Acc) ->
        {ok, [{Key, DocId, Value} | Acc]}
    end,
    ViewArgs = #view_query_args{
        run_reduce = false,
        view_name = ViewName
    },
    {ok, _, Rows} = couch_set_view:fold(Group, View, FoldFun, [], ViewArgs),
    couch_set_view:release_group(Group),
    {ok, lists:reverse(Rows)}.

setup_test_vb() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(),
        num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(),
        num_set_partitions()),

    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta)
                        { emit(meta.id, meta.vb); }">>}
                ]}}
            ]}}
        ]}}
    ]},
    populate_set(DDoc, false).

setup_test_seq() ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(),
        num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(),
        num_set_partitions()),

    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta)
                        { emit(meta.id, meta.seq); }">>}
                ]}}
            ]}}
        ]}}
    ]},
    populate_set(DDoc, false).

setup_test_xattrs(HasXattrs, Deleted, IndexXattrs) ->
    couch_set_view_test_util:delete_set_dbs(test_set_name(),
        num_set_partitions()),
    couch_set_view_test_util:create_set_dbs(test_set_name(),
        num_set_partitions()),

    DDoc = {[
        {<<"meta">>, {[{<<"id">>, ddoc_id()}]}},
        {<<"json">>, {[
            {<<"views">>, {[
                {<<"test">>, {[
                    {<<"map">>, <<"function(doc, meta)
                        { emit(meta.id, meta.xattrs); }">>}
                ]}}
            ]}},
            {<<"index_xattr_on_deleted_docs">>, IndexXattrs}
        ]}}
    ]},
    populate_set_xattrs(DDoc, HasXattrs, Deleted).

create_docs(From, To, Deleted) ->
    lists:map(
        fun(I) ->
            {[
                {<<"meta">>, {[{<<"deleted">>, Deleted}, {<<"id">>, iolist_to_binary(["doc",
                    integer_to_list(I)])}]}},
                {<<"json">>, {[{<<"value">>, I}]}}
            ]}
        end,
        lists:seq(From, To)).

create_docs_xattrs(From, To, HasXattrs, Deleted) ->
    lists:map(
        fun(I) ->
            {
                case HasXattrs of
                true ->
                [{<<"meta">>, {[{<<"id">>, iolist_to_binary(["doc",
                    integer_to_list(I)])}]}},
                    {<<"json">>, {[{<<"xattrs">>, I}, {<<"deleted">>, Deleted}, {<<"value">>, I}]}}];
                false ->
                [{<<"meta">>, {[{<<"deleted">>, Deleted}, {<<"id">>, iolist_to_binary(["doc",
                    integer_to_list(I)])}]}},
                    {<<"json">>, {[{<<"value">>, I}]}}]
                end
            }
        end,
        lists:seq(From, To)).


update_docs(From, To) ->
    lists:map(
        fun(I) ->
            {[
                {<<"meta">>, {[{<<"id">>, iolist_to_binary(["doc",
                    integer_to_list(I)])}]}},
                {<<"json">>, {[{<<"value">>, 2*I}]}}
            ]}
        end,
        lists:seq(From, To)).

update_docs() ->
    DocList = update_docs(1, num_docs()),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).

populate_set(DDoc, Deleted) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    DocList = create_docs(1, num_docs(), Deleted),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).

populate_set_xattrs(DDoc, HasXattrs, Deleted) ->
    etap:diag("Populating the " ++ integer_to_list(num_set_partitions()) ++
        " databases with " ++ integer_to_list(num_docs()) ++ " documents"),
    ok = couch_set_view_test_util:update_ddoc(test_set_name(), DDoc),
    DocList = create_docs_xattrs(1, num_docs(), HasXattrs, Deleted),
    ok = couch_set_view_test_util:populate_set_sequentially(
        test_set_name(),
        lists:seq(0, num_set_partitions() - 1),
        DocList).


configure_view_group(DDocId, PartitionId) ->
    etap:diag("Configuring view group"),
    try
        ok = couch_set_view_dev:define_group(
            mapreduce_view, test_set_name(), DDocId, PartitionId)
    catch _:Error ->
        Error
    end.

shutdown_group() ->
    GroupPid = couch_set_view:get_group_pid(
        mapreduce_view, test_set_name(), ddoc_id(), dev),
    couch_set_view_test_util:delete_set_dbs(test_set_name(),
        num_set_partitions()),
    MonRef = erlang:monitor(process, GroupPid),
    receive
    {'DOWN', MonRef, _, _, _} ->
        ok
    after 10000 ->
        etap:bail("Timeout waiting for group shutdown")
    end.
