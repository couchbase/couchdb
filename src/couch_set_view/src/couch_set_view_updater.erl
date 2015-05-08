% -*- Mode: Erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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

-module(couch_set_view_updater).

-export([update/6]).
% Exported for the MapReduce specific stuff
-export([new_sort_file_name/1]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").
-include_lib("couch_dcp/include/couch_dcp.hrl").

-define(MAP_QUEUE_SIZE, 256 * 1024).
-define(WRITE_QUEUE_SIZE, 512 * 1024).
% The size of the accumulator the emitted key-values are queued up in before
% they get actually queued. The bigger the value, the less the lock contention,
% but the higher the possible memory consumption is.
-define(QUEUE_ACC_BATCH_SIZE, 256 * 1024).

% incremental updates
-define(INC_MAX_TMP_FILE_SIZE, 31457280).
-define(MIN_BATCH_SIZE_PER_VIEW, 65536).

% For file sorter and file merger commands.
-define(PORT_OPTS,
        [exit_status, use_stdio, stderr_to_stdout, {line, 4096}, binary]).

-record(writer_acc, {
    parent,
    owner,
    group,
    last_seqs = orddict:new(),
    part_versions = orddict:new(),
    compactor_running,
    write_queue,
    initial_build,
    view_empty_kvs,
    kvs = [],
    kvs_size = 0,
    state = updating_active,
    final_batch = false,
    max_seqs,
    stats = #set_view_updater_stats{},
    tmp_dir = nil,
    initial_seqs,
    max_insert_batch_size,
    tmp_files = dict:new(),
    throttle = 0,
    force_flush = false
}).


-spec update(pid(), #set_view_group{},
             partition_seqs(), boolean(), string(), [term()]) -> no_return().
update(Owner, Group, CurSeqs, CompactorRunning, TmpDir, Options) ->
    #set_view_group{
        set_name = SetName,
        type = Type,
        name = DDocId
    } = Group,

    ActiveParts = couch_set_view_util:decode_bitmask(?set_abitmask(Group)),
    PassiveParts = couch_set_view_util:decode_bitmask(?set_pbitmask(Group)),
    NumChanges = couch_set_view_util:missing_changes_count(CurSeqs, ?set_seqs(Group)),

    process_flag(trap_exit, true),

    BeforeEnterTs = os:timestamp(),
    Parent = self(),
    BarrierEntryPid = spawn_link(fun() ->
        DDocIds = couch_set_view_util:get_ddoc_ids_with_sig(SetName, Group),
        couch_task_status:add_task([
            {type, blocked_indexer},
            {set, SetName},
            {signature, ?l2b(couch_util:to_hex(Group#set_view_group.sig))},
            {design_documents, DDocIds},
            {indexer_type, Type}
        ]),
        case Type of
        main ->
            ok = couch_index_barrier:enter(couch_main_index_barrier, Parent);
        replica ->
            ok = couch_index_barrier:enter(couch_replica_index_barrier, Parent)
        end,
        Parent ! {done, self(), (timer:now_diff(os:timestamp(), BeforeEnterTs) / 1000000)},
        receive shutdown -> ok end
    end),

    BlockedTime = receive
    {done, BarrierEntryPid, Duration} ->
        Duration;
    {'EXIT', _, Reason} ->
        exit({updater_error, Reason})
    end,

    CleanupParts = couch_set_view_util:decode_bitmask(?set_cbitmask(Group)),
    InitialBuild = couch_set_view_util:is_initial_build(Group),
    ?LOG_INFO("Updater for set view `~s`, ~s group `~s` started~n"
              "Active partitions:    ~w~n"
              "Passive partitions:   ~w~n"
              "Cleanup partitions:   ~w~n"
              "Replicas to transfer: ~w~n"
              "Pending transition:   ~n"
              "    active:           ~w~n"
              "    passive:          ~w~n"
              "    unindexable:      ~w~n"
              "Initial build:        ~s~n"
              "Compactor running:    ~s~n"
              "Min # changes:        ~p~n"
              "Partition versions:   ~w~n",
              [SetName, Type, DDocId,
               ActiveParts,
               PassiveParts,
               CleanupParts,
               ?set_replicas_on_transfer(Group),
               ?pending_transition_active(?set_pending_transition(Group)),
               ?pending_transition_passive(?set_pending_transition(Group)),
               ?pending_transition_unindexable(?set_pending_transition(Group)),
               InitialBuild,
               CompactorRunning,
               NumChanges,
               ?set_partition_versions(Group)
              ]),

    WriterAcc0 = #writer_acc{
        parent = self(),
        owner = Owner,
        group = Group,
        initial_build = InitialBuild,
        max_seqs = CurSeqs,
        part_versions = ?set_partition_versions(Group),
        tmp_dir = TmpDir,
        max_insert_batch_size = list_to_integer(
            couch_config:get("set_views", "indexer_max_insert_batch_size", "1048576"))
    },
    update(WriterAcc0, ActiveParts, PassiveParts,
            BlockedTime, BarrierEntryPid, NumChanges, CompactorRunning, Options).


update(WriterAcc, ActiveParts, PassiveParts, BlockedTime,
       BarrierEntryPid, NumChanges, CompactorRunning, Options) ->
    #writer_acc{
        owner = Owner,
        group = Group
    } = WriterAcc,
    #set_view_group{
        set_name = SetName,
        type = Type,
        name = DDocId,
        sig = GroupSig,
        mod = Mod
    } = Group,

    StartTime = os:timestamp(),

    MapQueueOptions = [{max_size, ?MAP_QUEUE_SIZE}, {max_items, infinity}],
    WriteQueueOptions = [{max_size, ?WRITE_QUEUE_SIZE}, {max_items, infinity}],
    {ok, MapQueue} = couch_work_queue:new(MapQueueOptions),
    {ok, WriteQueue} = couch_work_queue:new(WriteQueueOptions),

    Mapper = spawn_link(fun() ->
        try
            couch_set_view_mapreduce:start_map_context(Group),
            try
                do_maps(Group, MapQueue, WriteQueue)
            after
                couch_set_view_mapreduce:end_map_context()
            end
        catch _:Error ->
            Stacktrace = erlang:get_stacktrace(),
            ?LOG_ERROR("Set view `~s`, ~s group `~s`, mapper error~n"
                "error:      ~p~n"
                "stacktrace: ~p~n",
                [SetName, Type, DDocId, Error, Stacktrace]),
            exit(Error)
        end
    end),

    Parent = self(),
    Writer = spawn_link(fun() ->
        BarrierEntryPid ! shutdown,
        ViewEmptyKVs = [{View, []} || View <- Group#set_view_group.views],
        WriterAcc2 = init_tmp_files(WriterAcc#writer_acc{
            parent = Parent,
            group = Group,
            write_queue = WriteQueue,
            view_empty_kvs = ViewEmptyKVs,
            compactor_running = CompactorRunning,
            initial_seqs = ?set_seqs(Group),
            throttle = case lists:member(throttle, Options) of
                true ->
                    list_to_integer(
                        couch_config:get("set_views", "throttle_period", "100"));
                false ->
                    0
                end
        }),
        ok = couch_set_view_util:open_raw_read_fd(Group),
        try
            Mod:start_reduce_context(Group),
            try
                WriterAcc3 = do_writes(WriterAcc2),
                receive
                {doc_loader_finished, {PartVersions0, RealMaxSeqs}} ->
                    WriterAccStats = WriterAcc3#writer_acc.stats,
                    WriterAccGroup = WriterAcc3#writer_acc.group,
                    WriterAccHeader = WriterAccGroup#set_view_group.index_header,
                    PartVersions = lists:ukeymerge(1, PartVersions0,
                        WriterAccHeader#set_view_index_header.partition_versions),
                    case WriterAcc3#writer_acc.initial_build of
                    true ->
                        % The doc loader might not load the mutations up to the
                        % most recent one, but only to a lower one. Update the
                        % group header and stats with the correct information.
                        MaxSeqs = lists:ukeymerge(
                            1, RealMaxSeqs, WriterAccHeader#set_view_index_header.seqs),
                        Stats = WriterAccStats#set_view_updater_stats{
                            seqs = lists:sum([S || {_, S} <- RealMaxSeqs])
                        };
                    false ->
                        MaxSeqs = WriterAccHeader#set_view_index_header.seqs,
                        Stats = WriterAcc3#writer_acc.stats
                    end,
                    FinalWriterAcc = WriterAcc3#writer_acc{
                        stats = Stats,
                        group = WriterAccGroup#set_view_group{
                            index_header = WriterAccHeader#set_view_index_header{
                                partition_versions = PartVersions,
                                seqs = MaxSeqs
                            }
                        }
                    }
                end,
                Parent ! {writer_finished, FinalWriterAcc}
            after
                Mod:end_reduce_context(Group)
            end
        catch _:Error ->
            Stacktrace = erlang:get_stacktrace(),
            ?LOG_ERROR("Set view `~s`, ~s group `~s`, writer error~n"
                "error:      ~p~n"
                "stacktrace: ~p~n",
                [SetName, Type, DDocId, Error, Stacktrace]),
            exit(Error)
        after
            ok = couch_set_view_util:close_raw_read_fd(Group)
        end
    end),

    InitialBuild = WriterAcc#writer_acc.initial_build,
    NumChanges2 = case InitialBuild of
        true ->
            couch_set_view_updater_helper:count_items_from_set(Group, ActiveParts ++ PassiveParts);
        false ->
            NumChanges
    end,

    DocLoader = spawn_link(fun() ->
        DDocIds = couch_set_view_util:get_ddoc_ids_with_sig(SetName, Group),
        couch_task_status:add_task([
            {type, indexer},
            {set, SetName},
            {signature, ?l2b(couch_util:to_hex(GroupSig))},
            {design_documents, DDocIds},
            {indexer_type, Type},
            {progress, 0},
            {changes_done, 0},
            {initial_build, InitialBuild},
            {total_changes, NumChanges2}
        ]),
        couch_task_status:set_update_frequency(5000),
        case lists:member(pause, Options) of
        true ->
            % For reliable unit testing, to verify that adding new partitions
            % to the passive state doesn't restart the updater and the updater
            % can be aware of it and index these new partitions in the same run.
            receive continue -> ok end;
        false ->
            ok
        end,
        try
            {PartVersions, MaxSeqs} = load_changes(
                Owner, Parent, Group, MapQueue, ActiveParts, PassiveParts,
                WriterAcc#writer_acc.max_seqs, WriterAcc#writer_acc.initial_build),
            Parent ! {doc_loader_finished, {PartVersions, MaxSeqs}}
        catch
        throw:purge ->
            exit(purge);
        throw:{rollback, RollbackSeqs} ->
            exit({rollback, RollbackSeqs});
        _:Error ->
            Stacktrace = erlang:get_stacktrace(),
            ?LOG_ERROR("Set view `~s`, ~s group `~s`, doc loader error~n"
                "error:      ~p~n"
                "stacktrace: ~p~n",
                [SetName, Type, DDocId, Error, Stacktrace]),
            exit(Error)
        end,
        % Since updater progress stats is added from docloader,
        % this process has to stay till updater has completed.
        receive
        updater_finished ->
            ok
        end
    end),

    Result = wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime, Group),
    case Type of
    main ->
        ok = couch_index_barrier:leave(couch_main_index_barrier);
    replica ->
        ok = couch_index_barrier:leave(couch_replica_index_barrier)
    end,
    case Result of
    {updater_finished, #set_view_updater_result{group = NewGroup}} ->
        ?LOG_DEBUG("Updater for ~s set view group `~s`, set `~s`, writer finished:~n"
                   "  start seqs: ~w~n"
                   "  end seqs:   ~w~n",
                   [Type, DDocId, SetName, ?set_seqs(Group), ?set_seqs(NewGroup)]);
    _ ->
        ok
    end,
    DocLoader ! updater_finished,
    exit(Result).


wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime, OldGroup) ->
    #set_view_group{set_name = SetName, name = DDocId, type = Type} = OldGroup,
    receive
    {new_passive_partitions, _} = NewPassivePartitions ->
        Writer ! NewPassivePartitions,
        DocLoader ! NewPassivePartitions,
        wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime, OldGroup);
    continue ->
        % Used by unit tests.
        DocLoader ! continue,
        wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime, OldGroup);
    {doc_loader_finished, {PartVersions, MaxSeqs}} ->
        Writer ! {doc_loader_finished, {PartVersions, MaxSeqs}},
        wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime, OldGroup);
    {writer_finished, WriterAcc} ->
        Stats0 = WriterAcc#writer_acc.stats,
        Result = #set_view_updater_result{
            group = WriterAcc#writer_acc.group,
            state = WriterAcc#writer_acc.state,
            stats = Stats0#set_view_updater_stats{
                indexing_time = timer:now_diff(os:timestamp(), StartTime) / 1000000,
                blocked_time = BlockedTime
            },
            tmp_file = case WriterAcc#writer_acc.initial_build of
                true ->
                    dict:fetch(build_file, WriterAcc#writer_acc.tmp_files);
                false ->
                    ""
                end
        },
        {updater_finished, Result};
    {compactor_started, Pid, Ref} ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, updater received "
                  "compactor ~p notification, ref ~p, writer ~p",
                   [SetName, Type, DDocId, Pid, Ref, Writer]),
        Writer ! {compactor_started, self()},
        erlang:put(compactor_pid, {Pid, Ref}),
        wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime, OldGroup);
    {compactor_started_ack, Writer, GroupSnapshot} ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, updater received compaction ack"
                  " from writer ~p", [SetName, Type, DDocId, Writer]),
        case erlang:erase(compactor_pid) of
        {Pid, Ref} ->
            Pid ! {Ref, {ok, GroupSnapshot}};
        undefined ->
            ok
        end,
        wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime, OldGroup);
    {'EXIT', _, Reason} when Reason =/= normal ->
        couch_util:shutdown_sync(DocLoader),
        couch_util:shutdown_sync(Mapper),
        couch_util:shutdown_sync(Writer),
        {updater_error, Reason};
    {native_updater_start, Writer} ->
        % We need control over spawning native updater process
        % This helps to terminate native os processes correctly
        Writer ! {ok, native_updater_start},
        receive
        {native_updater_pid, NativeUpdater} ->
            erlang:put(native_updater, NativeUpdater)
        end,
        wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime, OldGroup);
    stop ->
        case erlang:erase(native_updater) of
        undefined ->
            couch_util:shutdown_sync(DocLoader),
            couch_util:shutdown_sync(Mapper),
            couch_util:shutdown_sync(Writer);
        NativeUpdater ->
            MRef = erlang:monitor(process, NativeUpdater),
            NativeUpdater ! stop,
            receive
            {'DOWN', MRef, process, NativeUpdater, _} ->
                couch_util:shutdown_sync(DocLoader),
                couch_util:shutdown_sync(Mapper),
                couch_util:shutdown_sync(Writer)
            end
        end,
        exit({updater_error, shutdown})
    end.


dcp_marker_to_string(Type) ->
    case Type band ?DCP_SNAPSHOT_TYPE_MASK of
    ?DCP_SNAPSHOT_TYPE_DISK ->
        "on-disk";
    ?DCP_SNAPSHOT_TYPE_MEMORY ->
        "in-memory";
    _ ->
        io_lib:format("unknown (~w)", [Type])
    end.


load_changes(Owner, Updater, Group, MapQueue, ActiveParts, PassiveParts,
        EndSeqs, InitialBuild) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = GroupType,
        index_header = #set_view_index_header{
            seqs = SinceSeqs,
            partition_versions = PartVersions0
        },
        dcp_pid = DcpPid,
        category = Category
    } = Group,

    MaxDocSize = list_to_integer(
        couch_config:get("set_views", "indexer_max_doc_size", "0")),
    FoldFun = fun({PartId, EndSeq}, {AccCount, AccSeqs, AccVersions, AccRollbacks}) ->
        case couch_set_view_util:has_part_seq(PartId, ?set_unindexable_seqs(Group))
            andalso not lists:member(PartId, ?set_replicas_on_transfer(Group)) of
        true ->
            {AccCount, AccSeqs, AccVersions, AccRollbacks};
        false ->
            Since = couch_util:get_value(PartId, SinceSeqs, 0),
            PartVersions = couch_util:get_value(PartId, AccVersions),
            Flags = case InitialBuild of
            true ->
                ?DCP_FLAG_DISKONLY;
            false ->
                ?DCP_FLAG_NOFLAG
            end,
            % For stream request from 0, If a vbucket got reset in the window
            % of time between seqno was obtained from stats and stream request
            % was made, the end_seqno may be higher than current vb high seqno.
            % Use a special flag to tell server to set end_seqno.
            Flags2 = case PartVersions of
            [{0, 0}] ->
                Flags bor ?DCP_FLAG_USELATEST_ENDSEQNO;
            _ ->
                Flags
            end,
            case AccRollbacks of
            [] ->
                case EndSeq =:= Since of
                true ->
                    {AccCount, AccSeqs, AccVersions, AccRollbacks};
                false ->
                    ChangesWrapper = fun
                        ({part_versions, _} = NewVersions, Acc) ->
                            queue_doc(
                                NewVersions, MapQueue, Group,
                                MaxDocSize, InitialBuild),
                            Acc;
                        ({snapshot_marker, {MarkerStartSeq, MarkerEndSeq, MarkerType}}, {Count, _})
                            when MarkerType band ?DCP_SNAPSHOT_TYPE_MASK =/= 0 ->
                            ?LOG_INFO(
                                "set view `~s`, ~s (~s) group `~s`: received "
                                "a snapshot marker (~s) for partition ~p from "
                                "sequence ~p to ~p",
                                [SetName, GroupType, Category, DDocId,
                                    dcp_marker_to_string(MarkerType),
                                    PartId, MarkerStartSeq, MarkerEndSeq]),
                            case Count of
                            % Ignore the snapshot marker that is at the
                            % beginning of the stream.
                            % If it wasn't ignored, it would lead to an
                            % additional forced flush which isn't needed. A
                            % flush is needed if there are several mutations
                            % of the same document within one batch. As two
                            % different partitions can't contain the same
                            % document ID, we are safe to not force flushing
                            % between two partitions.
                            0 ->
                                {Count, MarkerEndSeq};
                            _ ->
                                queue_doc(
                                    snapshot_marker, MapQueue, Group,
                                    MaxDocSize, InitialBuild),
                                {Count, MarkerEndSeq}
                            end;
                        ({snapshot_marker, {MarkerStartSeq, MarkerEndSeq, MarkerType}}, Acc) ->
                            ?LOG_ERROR(
                                "set view `~s`, ~s (~s) group `~s`: received "
                                "a snapshot marker (~s) for partition ~p from "
                                "sequence ~p to ~p",
                                [SetName, GroupType, Category, DDocId,
                                    dcp_marker_to_string(MarkerType),
                                    PartId, MarkerStartSeq, MarkerEndSeq]),
                            throw({error, unknown_snapshot_marker, MarkerType}),
                            Acc;
                        (#dcp_doc{} = Item, {Count, AccEndSeq}) ->
                            queue_doc(
                                Item, MapQueue, Group,
                                MaxDocSize, InitialBuild),
                            {Count + 1, AccEndSeq}
                        end,
                    Result = couch_dcp_client:enum_docs_since(
                        DcpPid, PartId, PartVersions, Since, EndSeq, Flags2,
                        ChangesWrapper, {0, 0}),
                    case Result of
                    {ok, {AccCount2, AccEndSeq}, NewPartVersions} ->
                        AccSeqs2 = orddict:store(PartId, AccEndSeq, AccSeqs),
                        AccVersions2 = lists:ukeymerge(
                            1, [{PartId, NewPartVersions}], AccVersions),
                        AccRollbacks2 = AccRollbacks;
                    {rollback, RollbackSeq} ->
                        AccCount2 = AccCount,
                        AccSeqs2 = AccSeqs,
                        AccVersions2 = AccVersions,
                        AccRollbacks2 = ordsets:add_element(
                            {PartId, RollbackSeq}, AccRollbacks);
                    Error ->
                        AccCount2 = AccCount,
                        AccSeqs2 = AccSeqs,
                        AccVersions2 = AccVersions,
                        AccRollbacks2 = AccRollbacks,
                        ?LOG_ERROR("set view `~s`, ~s (~s) group `~s` error"
                            "while loading changes for partition ~p:~n~p~n",
                            [SetName, GroupType, Category, DDocId, PartId,
                                Error]),
                        throw(Error)
                    end,
                    {AccCount2, AccSeqs2, AccVersions2, AccRollbacks2}
                end;
            _ ->
                % If there is a rollback needed, don't store any new documents
                % in the index, but just check for a rollback of another
                % partition (i.e. a request with start seq == end seq)
                ChangesWrapper = fun(_, _) -> ok end,
                Result = couch_dcp_client:enum_docs_since(
                    DcpPid, PartId, PartVersions, Since, Since, Flags2,
                    ChangesWrapper, ok),
                case Result of
                {ok, _, _} ->
                    AccRollbacks2 = AccRollbacks;
                {rollback, RollbackSeq} ->
                    AccRollbacks2 = ordsets:add_element(
                        {PartId, RollbackSeq}, AccRollbacks)
                end,
                {AccCount, AccSeqs, AccVersions, AccRollbacks2}
            end
        end
    end,

    notify_owner(Owner, {state, updating_active}, Updater),
    case ActiveParts of
    [] ->
        ActiveChangesCount = 0,
        MaxSeqs = orddict:new(),
        PartVersions = PartVersions0,
        Rollbacks = [];
    _ ->
        ?LOG_INFO("Updater reading changes from active partitions to "
                  "update ~s set view group `~s` from set `~s`",
                  [GroupType, DDocId, SetName]),
        {ActiveChangesCount, MaxSeqs, PartVersions, Rollbacks} = lists:foldl(
            FoldFun, {0, orddict:new(), PartVersions0, ordsets:new()},
            couch_set_view_util:filter_seqs(ActiveParts, EndSeqs))
    end,
    case PassiveParts of
    [] ->
        FinalChangesCount = ActiveChangesCount,
        MaxSeqs2 = MaxSeqs,
        PartVersions2 = PartVersions,
        Rollbacks2 = Rollbacks;
    _ ->
        ?LOG_INFO("Updater reading changes from passive partitions to "
                  "update ~s set view group `~s` from set `~s`",
                  [GroupType, DDocId, SetName]),
        {FinalChangesCount, MaxSeqs2, PartVersions2, Rollbacks2} = lists:foldl(
            FoldFun, {ActiveChangesCount, MaxSeqs, PartVersions, Rollbacks},
            couch_set_view_util:filter_seqs(PassiveParts, EndSeqs))
    end,
    {FinalChangesCount3, MaxSeqs3, PartVersions3, Rollbacks3} =
        load_changes_from_passive_parts_in_mailbox(DcpPid,
            Group, FoldFun, FinalChangesCount, MaxSeqs2, PartVersions2, Rollbacks2),

    case Rollbacks3 of
    [] ->
        ok;
    _ ->
        throw({rollback, Rollbacks3})
    end,

    couch_work_queue:close(MapQueue),
    ?LOG_INFO("Updater for ~s set view group `~s`, set `~s`, read a total of ~p changes",
              [GroupType, DDocId, SetName, FinalChangesCount3]),
    ?LOG_DEBUG("Updater for ~s set view group `~s`, set `~s`, max partition seqs found:~n~w",
               [GroupType, DDocId, SetName, MaxSeqs3]),
    {PartVersions3, MaxSeqs3}.


load_changes_from_passive_parts_in_mailbox(DcpPid,
        Group, FoldFun, ChangesCount, MaxSeqs0, PartVersions0, Rollbacks) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = GroupType
    } = Group,
    receive
    {new_passive_partitions, Parts0} ->
        Parts = get_more_passive_partitions(Parts0),
        AddPartVersions = [{P, [{0, 0}]} || P <- Parts],
        {ok, AddMaxSeqs} = couch_dcp_client:get_seqs(DcpPid, Parts),
        PartVersions = lists:ukeymerge(1, AddPartVersions, PartVersions0),

        MaxSeqs = lists:ukeymerge(1, AddMaxSeqs, MaxSeqs0),
        ?LOG_INFO("Updater reading changes from new passive partitions ~w to "
                  "update ~s set view group `~s` from set `~s`",
                  [Parts, GroupType, DDocId, SetName]),
        {ChangesCount2, MaxSeqs2, PartVersions2, Rollbacks2} = lists:foldl(
            FoldFun, {ChangesCount, MaxSeqs, PartVersions, Rollbacks}, AddMaxSeqs),
        load_changes_from_passive_parts_in_mailbox(DcpPid,
            Group, FoldFun, ChangesCount2, MaxSeqs2, PartVersions2, Rollbacks2)
    after 0 ->
        {ChangesCount, MaxSeqs0, PartVersions0, Rollbacks}
    end.


get_more_passive_partitions(Parts) ->
    receive
    {new_passive_partitions, Parts2} ->
        get_more_passive_partitions(Parts ++ Parts2)
    after 0 ->
        lists:sort(Parts)
    end.


notify_owner(Owner, Msg, UpdaterPid) ->
    Owner ! {updater_info, UpdaterPid, Msg}.


queue_doc(snapshot_marker, MapQueue, _Group, _MaxDocSize, _InitialBuild) ->
    couch_work_queue:queue(MapQueue, snapshot_marker);
queue_doc({part_versions, _} = PartVersions, MapQueue, _Group, _MaxDocSize,
    _InitialBuild) ->
    couch_work_queue:queue(MapQueue, PartVersions);
queue_doc(Doc, MapQueue, Group, MaxDocSize, InitialBuild) ->
    case Doc#dcp_doc.deleted of
    true when InitialBuild ->
        Entry = nil;
    true ->
        Entry = Doc;
    false ->
        #set_view_group{
           set_name = SetName,
           name = DDocId,
           type = GroupType
        } = Group,
        case couch_util:validate_utf8(Doc#dcp_doc.id) of
        true ->
            case (MaxDocSize > 0) andalso
                (iolist_size(Doc#dcp_doc.body) > MaxDocSize) of
            true ->
                ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, skipping "
                    "document with ID `~s`: too large body (~p bytes)",
                    [SetName, GroupType, DDocId,
                     ?b2l(Doc#dcp_doc.id), iolist_size(Doc#dcp_doc.body)]),
                Entry = Doc#dcp_doc{deleted = true};
            false ->
                Entry = Doc
            end;
        false ->
            % If the id isn't utf8 (memcached allows it), then log an error
            % message and skip the doc. Send it through the queue anyway
            % so we record the high seq num in case there are a bunch of
            % these at the end, we want to keep track of the high seq and
            % not reprocess again.
            ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, skipping "
                "document with non-utf8 id. Doc id bytes: ~w",
                [SetName, GroupType, DDocId, ?b2l(Doc#dcp_doc.id)]),
            Entry = Doc#dcp_doc{deleted = true}
        end
    end,
    case Entry of
    nil ->
        ok;
    _ ->
        couch_work_queue:queue(MapQueue, Entry),
        update_task(1)
    end.


do_maps(Group, MapQueue, WriteQueue) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = Type,
        mod = Mod
    } = Group,
    case couch_work_queue:dequeue(MapQueue) of
    closed ->
        couch_work_queue:close(WriteQueue);
    {ok, Queue, _QueueSize} ->
        ViewCount = length(Group#set_view_group.views),
        {Items, _} = lists:foldl(
            fun(#dcp_doc{deleted = true} = DcpDoc, {Acc, Size}) ->
                #dcp_doc{
                    id = Id,
                    partition = PartId,
                    seq = Seq
                } = DcpDoc,
                Item = {Seq, Id, PartId, []},
                {[Item | Acc], Size};
            (#dcp_doc{deleted = false} = DcpDoc, {Acc0, Size0}) ->
                % When there are a lot of emits per document the memory can
                % grow almost indefinitely as the queue size is only limited
                % by the number of documents and not their emits.
                % In case the accumulator grows huge, queue the items early
                % into the writer queue. Only take emits into account, the
                % other cases in this `foldl` won't ever have an significant
                % size.
                {Acc, Size} = case Size0 > ?QUEUE_ACC_BATCH_SIZE of
                true ->
                    couch_work_queue:queue(WriteQueue, lists:reverse(Acc0)),
                    {[], 0};
                false ->
                    {Acc0, Size0}
                end,
                #dcp_doc{
                    id = Id,
                    body = Body,
                    partition = PartId,
                    rev_seq = RevSeq,
                    seq = Seq,
                    cas = Cas,
                    expiration = Expiration,
                    flags = Flags,
                    data_type = DcpDataType
                } = DcpDoc,
                DataType = case DcpDataType of
                ?DCP_DATA_TYPE_RAW ->
                    ?CONTENT_META_NON_JSON_MODE;
                ?DCP_DATA_TYPE_JSON ->
                    ?CONTENT_META_JSON
                end,
                Doc = #doc{
                    id = Id,
                    rev = {RevSeq, <<Cas:64, Expiration:32, Flags:32>>},
                    body = Body,
                    content_meta = DataType,
                    deleted = false
                },
                try
                    {ok, Result, LogList} = couch_set_view_mapreduce:map(Doc),
                    {Result2, _} = lists:foldr(
                        fun({error, Reason}, {AccRes, Pos}) ->
                            ErrorMsg = "Bucket `~s`, ~s group `~s`, error mapping"
                                    " document `~s` for view `~s`: ~s",
                            ViewName = Mod:view_name(Group, Pos),
                            Args = [SetName, Type, DDocId, Id, ViewName,
                                    couch_util:to_binary(Reason)],
                            ?LOG_MAPREDUCE_ERROR(ErrorMsg, Args),
                            {[[] | AccRes], Pos - 1};
                        (KVs, {AccRes, Pos}) ->
                            {[KVs | AccRes], Pos - 1}
                        end,
                        {[], ViewCount}, Result),
                    lists:foreach(
                        fun(Msg) ->
                            DebugMsg = "Bucket `~s`, ~s group `~s`, map function"
                                " log for document `~s`: ~s",
                            Args = [SetName, Type, DDocId, Id, binary_to_list(Msg)],
                            ?LOG_MAPREDUCE_ERROR(DebugMsg, Args)
                        end, LogList),
                    Item = {Seq, Id, PartId, Result2},
                    {[Item | Acc], Size + erlang:external_size(Result2)}
                catch _:{error, Reason} ->
                    ErrorMsg = "Bucket `~s`, ~s group `~s`, error mapping document `~s`: ~s",
                    Args = [SetName, Type, DDocId, Id, couch_util:to_binary(Reason)],
                    ?LOG_MAPREDUCE_ERROR(ErrorMsg, Args),
                    {[{Seq, Id, PartId, []} | Acc], Size}
                end;
            (snapshot_marker, {Acc, Size}) ->
                {[snapshot_marker | Acc], Size};
            ({part_versions, _} = PartVersions, {Acc, Size}) ->
                {[PartVersions | Acc], Size}
            end,
            {[], 0}, Queue),
        ok = couch_work_queue:queue(WriteQueue, lists:reverse(Items)),
        do_maps(Group, MapQueue, WriteQueue)
    end.


do_writes(Acc) ->
    #writer_acc{
        kvs = Kvs,
        kvs_size = KvsSize,
        write_queue = WriteQueue,
        throttle = Throttle
    } = Acc,
    ok = timer:sleep(Throttle),
    case couch_work_queue:dequeue(WriteQueue) of
    closed ->
        flush_writes(Acc#writer_acc{final_batch = true});
    {ok, Queue0, QueueSize} ->
        Queue = lists:flatten(Queue0),
        Kvs2 = Kvs ++ Queue,
        KvsSize2 = KvsSize + QueueSize,

        Acc2 = Acc#writer_acc{
            kvs = Kvs2,
            kvs_size = KvsSize2
        },
        case should_flush_writes(Acc2) of
        true ->
            Acc3 = flush_writes(Acc2),
            Acc4 = Acc3#writer_acc{kvs = [], kvs_size = 0};
        false ->
            Acc4 = Acc2
        end,
        do_writes(Acc4)
    end.

should_flush_writes(Acc) ->
    #writer_acc{
        view_empty_kvs = ViewEmptyKvs,
        kvs_size = KvsSize
    } = Acc,
    KvsSize >= (?MIN_BATCH_SIZE_PER_VIEW * length(ViewEmptyKvs)).


flush_writes(#writer_acc{kvs = [], initial_build = false} = Acc) ->
    Acc2 = maybe_update_btrees(Acc),
    checkpoint(Acc2);

flush_writes(#writer_acc{initial_build = false} = Acc0) ->
    #writer_acc{
        kvs = Kvs,
        view_empty_kvs = ViewEmptyKVs,
        group = Group,
        parent = Parent,
        owner = Owner,
        last_seqs = LastSeqs,
        final_batch = IsFinalBatch,
        part_versions = PartVersions
    } = Acc0,
    Mod = Group#set_view_group.mod,
    % Only incremental updates can contain multiple snapshots
    {MultipleSnapshots, Kvs2} = merge_snapshots(Kvs),
    Acc1 = case MultipleSnapshots of
    true ->
        Acc2 = maybe_update_btrees(Acc0#writer_acc{force_flush = true}),
        checkpoint(Acc2);
    false ->
        Acc0
    end,
    {ViewKVs, DocIdViewIdKeys, NewLastSeqs, NewPartVersions} =
        process_map_results(Mod, Kvs2, ViewEmptyKVs, LastSeqs, PartVersions),
    Acc3 = Acc1#writer_acc{last_seqs = NewLastSeqs, part_versions = NewPartVersions},
    Acc4 = write_to_tmp_batch_files(ViewKVs, DocIdViewIdKeys, Acc3),
    #writer_acc{group = NewGroup} = Acc4,
    case ?set_seqs(NewGroup) =/= ?set_seqs(Group) of
    true ->
        Acc5 = checkpoint(Acc4),
        case (Acc4#writer_acc.state =:= updating_active) andalso
            lists:any(fun({PartId, _}) ->
                ((1 bsl PartId) band ?set_pbitmask(Group) =/= 0)
            end, NewLastSeqs) of
        true ->
            notify_owner(Owner, {state, updating_passive}, Parent),
            Acc5#writer_acc{state = updating_passive};
        false ->
            Acc5
        end;
    false ->
        case IsFinalBatch of
        true ->
            checkpoint(Acc4);
        false ->
            Acc4
        end
    end;

flush_writes(#writer_acc{initial_build = true} = WriterAcc) ->
    #writer_acc{
        kvs = Kvs,
        view_empty_kvs = ViewEmptyKVs,
        tmp_files = TmpFiles,
        tmp_dir = TmpDir,
        group = Group,
        final_batch = IsFinalBatch,
        max_seqs = MaxSeqs,
        part_versions = PartVersions,
        stats = Stats
    } = WriterAcc,
    #set_view_group{
        set_name = SetName,
        type = Type,
        name = DDocId,
        mod = Mod
    } = Group,
    {ViewKVs, DocIdViewIdKeys, MaxSeqs2, PartVersions2} = process_map_results(
        Mod, Kvs, ViewEmptyKVs, MaxSeqs, PartVersions),

    IdRecords = lists:foldr(
        fun({_DocId, {_PartId, []}}, Acc) ->
                Acc;
            (Kv, Acc) ->
                [{KeyBin, ValBin}] = Mod:convert_back_index_kvs_to_binary([Kv], []),
                KvBin = [<<(byte_size(KeyBin)):16>>, KeyBin, ValBin],
                [[<<(iolist_size(KvBin)):32/native>>, KvBin] | Acc]
        end,
        [], DocIdViewIdKeys),
    #set_view_tmp_file_info{fd = IdFd} = dict:fetch(ids_index, TmpFiles),
    ok = file:write(IdFd, IdRecords),

    {InsertKVCount, TmpFiles2} = Mod:write_kvs(Group, TmpFiles, ViewKVs),

    Stats2 = Stats#set_view_updater_stats{
        inserted_kvs = Stats#set_view_updater_stats.inserted_kvs + InsertKVCount,
        inserted_ids = Stats#set_view_updater_stats.inserted_ids + length(DocIdViewIdKeys)
    },
    case IsFinalBatch of
    false ->
        WriterAcc#writer_acc{
            max_seqs = MaxSeqs2,
            stats = Stats2
        };
    true ->
        ?LOG_INFO("Updater for set view `~s`, ~s group `~s`, starting btree "
                  "build phase" , [SetName, Type, DDocId]),
        {Group2, BuildFd} = Mod:finish_build(Group, TmpFiles2, TmpDir),
        WriterAcc#writer_acc{
            tmp_files = dict:store(build_file, BuildFd, TmpFiles2),
            max_seqs = MaxSeqs2,
            part_versions = PartVersions2,
            stats = Stats2,
            group = Group2
        }
    end.


process_map_results(Mod, Kvs, ViewEmptyKVs, PartSeqs, PartVersions) ->
    lists:foldl(
        fun({Seq, DocId, PartId, []}, {ViewKVsAcc, DocIdViewIdKeysAcc, PartIdSeqs, PartIdVersions}) ->
            PartIdSeqs2 = update_part_seq(Seq, PartId, PartIdSeqs),
            {ViewKVsAcc, [{DocId, {PartId, []}} | DocIdViewIdKeysAcc], PartIdSeqs2, PartIdVersions};
        ({Seq, DocId, PartId, QueryResults}, {ViewKVsAcc, DocIdViewIdKeysAcc, PartIdSeqs, PartIdVersions}) ->
            {NewViewKVs, NewViewIdKeys} = Mod:view_insert_doc_query_results(
                    DocId, PartId, QueryResults, ViewKVsAcc, [], []),
            PartIdSeqs2 = update_part_seq(Seq, PartId, PartIdSeqs),
            {NewViewKVs, [{DocId, {PartId, NewViewIdKeys}} | DocIdViewIdKeysAcc], PartIdSeqs2, PartIdVersions};
        (snapshot_marker, _Acc) ->
            throw({error,
                <<"The multiple snapshots should have been merged, "
                  "but theu weren't">>});
        ({part_versions, {PartId, NewVersions}}, {ViewKVsAcc, DocIdViewIdKeysAcc, PartIdSeqs, PartIdVersions}) ->
            PartIdVersions2 = update_part_versions(NewVersions, PartId, PartIdVersions),
            {ViewKVsAcc, DocIdViewIdKeysAcc, PartIdSeqs, PartIdVersions2}
        end,
        {ViewEmptyKVs, [], PartSeqs, PartVersions}, Kvs).


-spec update_transferred_replicas(#set_view_group{},
                                  partition_seqs(),
                                  partition_seqs()) -> #set_view_group{}.
update_transferred_replicas(Group, _MaxSeqs, _PartIdSeqs) when ?set_replicas_on_transfer(Group) =:= [] ->
    Group;
update_transferred_replicas(Group, MaxSeqs, PartIdSeqs) ->
    #set_view_group{index_header = Header} = Group,
    RepsTransferred = lists:foldl(
        fun({PartId, Seq}, A) ->
            case lists:member(PartId, ?set_replicas_on_transfer(Group))
                andalso (Seq >= couch_set_view_util:get_part_seq(PartId, MaxSeqs)) of
            true ->
                ordsets:add_element(PartId, A);
            false ->
                A
            end
        end,
        ordsets:new(), PartIdSeqs),
    ReplicasOnTransfer2 = ordsets:subtract(?set_replicas_on_transfer(Group), RepsTransferred),
    {Abitmask2, Pbitmask2} = lists:foldl(
        fun(Id, {A, P}) ->
            Mask = 1 bsl Id,
            Mask = ?set_pbitmask(Group) band Mask,
            0 = ?set_abitmask(Group) band Mask,
            {A bor Mask, P bxor Mask}
        end,
        {?set_abitmask(Group), ?set_pbitmask(Group)},
        RepsTransferred),
    Group#set_view_group{
        index_header = Header#set_view_index_header{
            abitmask = Abitmask2,
            pbitmask = Pbitmask2,
            replicas_on_transfer = ReplicasOnTransfer2
        }
    }.


-spec update_part_seq(update_seq(), partition_id(), partition_seqs()) -> partition_seqs().
update_part_seq(Seq, PartId, Acc) ->
    case couch_set_view_util:find_part_seq(PartId, Acc) of
    {ok, Max} when Max >= Seq ->
        Acc;
    _ ->
        orddict:store(PartId, Seq, Acc)
    end.


-spec update_part_versions(partition_version(), partition_id(), partition_versions()) ->
    partition_versions().
update_part_versions(NewVersions, PartId, PartVersions) ->
    orddict:store(PartId, NewVersions, PartVersions).


% Incremental updates.
write_to_tmp_batch_files(ViewKeyValuesToAdd, DocIdViewIdKeys, WriterAcc) ->
    #writer_acc{
        tmp_files = TmpFiles,
        group = #set_view_group{
            id_btree = IdBtree,
            mod = Mod
        }
    } = WriterAcc,

    {AddDocIdViewIdKeys0, RemoveDocIds, LookupDocIds} = lists:foldr(
        fun({DocId, {PartId, [] = _ViewIdKeys}}, {A, B, C}) ->
                BackKey = make_back_index_key(DocId, PartId),
                case is_new_partition(PartId, WriterAcc) of
                true ->
                    {A, [BackKey | B], C};
                false ->
                    {A, [BackKey | B], [BackKey | C]}
                end;
            ({DocId, {PartId, _ViewIdKeys}} = KvPairs, {A, B, C}) ->
                BackKey = make_back_index_key(DocId, PartId),
                case is_new_partition(PartId, WriterAcc) of
                true ->
                    {[KvPairs | A], B, C};
                false ->
                    {[KvPairs | A], B, [BackKey | C]}
                end
        end,
        {[], [], []}, DocIdViewIdKeys),

    AddDocIdViewIdKeys = Mod:convert_back_index_kvs_to_binary(
        AddDocIdViewIdKeys0, []),

    IdsData1 = lists:map(
        fun(K) -> couch_set_view_updater_helper:encode_op(remove, K) end,
        RemoveDocIds),

    IdsData2 = lists:foldl(
        fun({K, V}, Acc) ->
            Bin = couch_set_view_updater_helper:encode_op(insert, K, V),
            [Bin | Acc]
        end,
        IdsData1,
        AddDocIdViewIdKeys),

    IdTmpFileInfo = dict:fetch(ids_index, TmpFiles),
    case IdTmpFileInfo of
    #set_view_tmp_file_info{fd = nil} ->
        0 = IdTmpFileInfo#set_view_tmp_file_info.size,
        IdTmpFilePath = new_sort_file_name(WriterAcc),
        {ok, IdTmpFileFd} = file2:open(IdTmpFilePath, [raw, append, binary]),
        IdTmpFileSize = 0;
    #set_view_tmp_file_info{
            fd = IdTmpFileFd, name = IdTmpFilePath, size = IdTmpFileSize} ->
        ok
    end,

    ok = file:write(IdTmpFileFd, IdsData2),

    IdTmpFileInfo2 = IdTmpFileInfo#set_view_tmp_file_info{
        fd = IdTmpFileFd,
        name = IdTmpFilePath,
        size = IdTmpFileSize + iolist_size(IdsData2)
    },
    TmpFiles2 = dict:store(ids_index, IdTmpFileInfo2, TmpFiles),

    case LookupDocIds of
    [] ->
        LookupResults = [];
    _ ->
        {ok, LookupResults, IdBtree} =
            couch_btree:query_modify(IdBtree, LookupDocIds, [], [])
    end,
    KeysToRemoveByView = lists:foldl(
        fun(LookupResult, KeysToRemoveByViewAcc) ->
            case LookupResult of
            {ok, {<<_Part:16, DocId/binary>>, <<_Part:16, ViewIdKeys/binary>>}} ->
                lists:foldl(
                    fun({ViewId, Keys}, KeysToRemoveByViewAcc2) ->
                        RemoveKeysDict = lists:foldl(
                            fun(Key, RemoveKeysDictAcc) ->
                                EncodedKey = Mod:encode_key_docid(Key, DocId),
                                dict:store(EncodedKey, nil, RemoveKeysDictAcc)
                            end,
                        couch_util:dict_find(ViewId, KeysToRemoveByViewAcc2, dict:new()), Keys),
                        dict:store(ViewId, RemoveKeysDict, KeysToRemoveByViewAcc2)
                    end,
                    KeysToRemoveByViewAcc, couch_set_view_util:parse_view_id_keys(ViewIdKeys));
            {not_found, _} ->
                KeysToRemoveByViewAcc
            end
        end,
        dict:new(), LookupResults),

    WriterAcc2 = update_tmp_files(
        Mod, WriterAcc#writer_acc{tmp_files = TmpFiles2}, ViewKeyValuesToAdd,
        KeysToRemoveByView),
    maybe_update_btrees(WriterAcc2).


% Update the temporary files with the key-values from the indexer. Return
% the updated writer accumulator.
-spec update_tmp_files(atom(), #writer_acc{},
                       [{#set_view{}, [set_view_key_value()]}],
                       dict:dict(non_neg_integer(), dict:dict(binary(), nil)))
                      -> #writer_acc{}.
update_tmp_files(Mod, WriterAcc, ViewKeyValues, KeysToRemoveByView) ->
    #writer_acc{
       group = Group,
       tmp_files = TmpFiles
    } = WriterAcc,
    TmpFiles2 = lists:foldl(
        fun({#set_view{id_num = ViewId}, AddKeyValues}, AccTmpFiles) ->
            AddKeyValuesBinaries = Mod:convert_primary_index_kvs_to_binary(
                AddKeyValues, Group, []),
            KeysToRemoveDict = couch_util:dict_find(
                ViewId, KeysToRemoveByView, dict:new()),

            case Mod of
            % The b-tree replaces nodes with the same key, hence we don't
            % need to delete a node that gets updated anyway
            mapreduce_view ->
                {KeysToRemoveDict2, BatchData} = lists:foldl(
                    fun({K, V}, {KeysToRemoveAcc, BinOpAcc}) ->
                        Bin = couch_set_view_updater_helper:encode_op(
                            insert, K, V),
                        BinOpAcc2 = [Bin | BinOpAcc],
                        case dict:find(K, KeysToRemoveAcc) of
                        {ok, _} ->
                            {dict:erase(K, KeysToRemoveAcc), BinOpAcc2};
                        _ ->
                            {KeysToRemoveAcc, BinOpAcc2}
                        end
                    end,
                    {KeysToRemoveDict, []}, AddKeyValuesBinaries);
            % In r-trees there are multiple possible paths to a key. Hence it's
            % not easily possible to replace an existing node with a new one,
            % as the insertion could happen in a subtree that is different
            % from the subtree of the old value.
            spatial_view ->
                BatchData = [
                    couch_set_view_updater_helper:encode_op(insert, K, V) ||
                        {K, V} <- AddKeyValuesBinaries],
                KeysToRemoveDict2 = KeysToRemoveDict
            end,

            BatchData2 = dict:fold(
                fun(K, _V, BatchAcc) ->
                    Bin = couch_set_view_updater_helper:encode_op(remove, K),
                    [Bin | BatchAcc]
                end,
                BatchData, KeysToRemoveDict2),

            ViewTmpFileInfo = dict:fetch(ViewId, TmpFiles),
            case ViewTmpFileInfo of
            #set_view_tmp_file_info{fd = nil} ->
                0 = ViewTmpFileInfo#set_view_tmp_file_info.size,
                ViewTmpFilePath = new_sort_file_name(WriterAcc),
                {ok, ViewTmpFileFd} = file2:open(
                    ViewTmpFilePath, [raw, append, binary]),
                ViewTmpFileSize = 0;
            #set_view_tmp_file_info{fd = ViewTmpFileFd,
                                    size = ViewTmpFileSize,
                                    name = ViewTmpFilePath} ->
                ok
            end,
            ok = file:write(ViewTmpFileFd, BatchData2),
            ViewTmpFileInfo2 = ViewTmpFileInfo#set_view_tmp_file_info{
                fd = ViewTmpFileFd,
                name = ViewTmpFilePath,
                size = ViewTmpFileSize + iolist_size(BatchData2)
            },
            dict:store(ViewId, ViewTmpFileInfo2, AccTmpFiles)
        end,
    TmpFiles, ViewKeyValues),
    WriterAcc#writer_acc{
        tmp_files = TmpFiles2
    }.


is_new_partition(PartId, #writer_acc{initial_seqs = InitialSeqs}) ->
    couch_util:get_value(PartId, InitialSeqs, 0) == 0.


% For incremental index updates.
maybe_update_btrees(WriterAcc0) ->
    #writer_acc{
        view_empty_kvs = ViewEmptyKVs,
        tmp_files = TmpFiles,
        group = Group0,
        final_batch = IsFinalBatch,
        owner = Owner,
        last_seqs = LastSeqs,
        part_versions = PartVersions,
        force_flush = ForceFlush
    } = WriterAcc0,
    IdTmpFileInfo = dict:fetch(ids_index, TmpFiles),
    ShouldFlushViews =
        lists:any(
            fun({#set_view{id_num = Id}, _}) ->
                ViewTmpFileInfo = dict:fetch(Id, TmpFiles),
                ViewTmpFileInfo#set_view_tmp_file_info.size >= ?INC_MAX_TMP_FILE_SIZE
            end, ViewEmptyKVs),
    ShouldFlush = IsFinalBatch orelse
        ForceFlush orelse
        ((IdTmpFileInfo#set_view_tmp_file_info.size >= ?INC_MAX_TMP_FILE_SIZE) andalso
        ShouldFlushViews),
    case ShouldFlush of
    false ->
        NewLastSeqs1 = LastSeqs,
        case erlang:get(updater_worker) of
        undefined ->
            WriterAcc = WriterAcc0;
        UpdaterWorker when is_reference(UpdaterWorker) ->
            receive
            {UpdaterWorker, UpGroup, UpStats, CompactFiles} ->
                send_log_compact_files(Owner, CompactFiles, ?set_seqs(UpGroup),
                    ?set_partition_versions(UpGroup)),
                erlang:erase(updater_worker),
                WriterAcc = check_if_compactor_started(
                    WriterAcc0#writer_acc{group = UpGroup, stats = UpStats})
            after 0 ->
                WriterAcc = WriterAcc0
            end
        end;
    true ->
        ok = close_tmp_fd(IdTmpFileInfo),
        ok = lists:foreach(
            fun(#set_view{id_num = Id}) ->
                ViewTmpFileInfo = dict:fetch(Id, TmpFiles),
                ok = close_tmp_fd(ViewTmpFileInfo)
            end,
            Group0#set_view_group.views),
        case erlang:erase(updater_worker) of
        undefined ->
            WriterAcc1 = WriterAcc0;
        UpdaterWorker when is_reference(UpdaterWorker) ->
            receive
            {UpdaterWorker, UpGroup2, UpStats2, CompactFiles2} ->
                send_log_compact_files(Owner, CompactFiles2, ?set_seqs(UpGroup2),
                    ?set_partition_versions(UpGroup2)),
                WriterAcc1 = check_if_compactor_started(
                    WriterAcc0#writer_acc{
                        group = UpGroup2,
                        stats = UpStats2
                    })
            end
        end,


        % MB-11472: There is no id sortfile present and hence this is the final batch
        % and nothing left to be updated to the btree
        case IdTmpFileInfo#set_view_tmp_file_info.name of
        nil ->
            WriterAcc = WriterAcc1;
        _ ->
            WriterAcc2 = check_if_compactor_started(WriterAcc1),
            NewUpdaterWorker = spawn_updater_worker(WriterAcc2, LastSeqs, PartVersions),
            erlang:put(updater_worker, NewUpdaterWorker),

            TmpFiles2 = dict:map(
                fun(_, _) -> #set_view_tmp_file_info{} end, TmpFiles),
            WriterAcc = WriterAcc2#writer_acc{tmp_files = TmpFiles2}
        end,
        NewLastSeqs1 = orddict:new()
    end,
    #writer_acc{
        stats = NewStats0,
        group = NewGroup0
    } = WriterAcc,
    case IsFinalBatch of
    true ->
        case erlang:erase(updater_worker) of
        undefined ->
            NewGroup = NewGroup0,
            NewStats = NewStats0;
        UpdaterWorker2 when is_reference(UpdaterWorker2) ->
            receive
            {UpdaterWorker2, NewGroup, NewStats, CompactFiles3} ->
                send_log_compact_files(Owner, CompactFiles3, ?set_seqs(NewGroup),
                    ?set_partition_versions(NewGroup))
            end
        end,
        NewLastSeqs = orddict:new();
    false ->
        NewGroup = NewGroup0,
        NewStats = NewStats0,
        NewLastSeqs = NewLastSeqs1
    end,
    NewWriterAcc = WriterAcc#writer_acc{
        stats = NewStats,
        group = NewGroup,
        last_seqs = NewLastSeqs
    },
    NewWriterAcc.


send_log_compact_files(_Owner, [], _Seqs, _PartVersions) ->
    ok;
send_log_compact_files(Owner, Files, Seqs, PartVersions) ->
    Init = case erlang:erase(new_compactor) of
    true ->
        true;
    undefined ->
        false
    end,
    ok = gen_server:cast(Owner, {compact_log_files, Files, Seqs, PartVersions, Init}).


-spec spawn_updater_worker(#writer_acc{}, partition_seqs(),
                           partition_versions()) -> reference().
spawn_updater_worker(WriterAcc, PartIdSeqs, PartVersions) ->
    Parent = self(),
    Ref = make_ref(),
    #writer_acc{
        group = Group,
        parent = UpdaterPid,
        max_seqs = MaxSeqs
    } = WriterAcc,
    % Wait for main updater process to ack
    UpdaterPid ! {native_updater_start, self()},
    receive
    {ok, native_updater_start} ->
        ok
    end,
    Pid = spawn_link(fun() ->
        case ?set_cbitmask(Group) of
        0 ->
            CleanupStart = 0;
        _ ->
            CleanupStart = os:timestamp()
        end,
        {ok, NewGroup0, CleanupCount, NewStats, NewCompactFiles} = update_btrees(WriterAcc),
        case ?set_cbitmask(Group) of
        0 ->
            CleanupTime = 0.0;
        _ ->
            CleanupTime = timer:now_diff(os:timestamp(), CleanupStart) / 1000000,
            #set_view_group{
                set_name = SetName,
                name = DDocId,
                type = GroupType
            } = Group,
            ?LOG_INFO("Updater for set view `~s`, ~s group `~s`, performed cleanup "
                      "of ~p key/value pairs in ~.3f seconds",
                      [SetName, GroupType, DDocId, CleanupCount, CleanupTime])
        end,
        NewSeqs = update_seqs(PartIdSeqs, ?set_seqs(Group)),
        NewPartVersions = update_versions(PartVersions, ?set_partition_versions(Group)),
        Header = NewGroup0#set_view_group.index_header,
        NewHeader = Header#set_view_index_header{
            seqs = NewSeqs,
            partition_versions = NewPartVersions
        },
        NewGroup = NewGroup0#set_view_group{
            index_header = NewHeader
        },
        NewGroup2 = update_transferred_replicas(NewGroup, MaxSeqs, PartIdSeqs),
        NumChanges = count_seqs_done(Group, NewSeqs),
        NewStats2 = NewStats#set_view_updater_stats{
           seqs = NewStats#set_view_updater_stats.seqs + NumChanges,
           cleanup_time = NewStats#set_view_updater_stats.seqs + CleanupTime,
           cleanup_kv_count = NewStats#set_view_updater_stats.cleanup_kv_count + CleanupCount
        },
        Parent ! {Ref, NewGroup2, NewStats2, NewCompactFiles}
    end),
    UpdaterPid ! {native_updater_pid, Pid},
    Ref.

% Update id btree and view btrees with current batch of changes
update_btrees(WriterAcc) ->
    #writer_acc{
        stats = Stats,
        group = Group0,
        tmp_dir = TmpDir,
        tmp_files = TmpFiles,
        compactor_running = CompactorRunning,
        max_insert_batch_size = MaxBatchSize
    } = WriterAcc,

    % Prepare list of operation logs for each btree
    #set_view_tmp_file_info{name = IdFile} = dict:fetch(ids_index, TmpFiles),
    ViewFiles = lists:map(
        fun(#set_view{id_num = Id}) ->
            #set_view_tmp_file_info{
                name = ViewFile
            } = dict:fetch(Id, TmpFiles),
            ViewFile
        end, Group0#set_view_group.views),
    % `LogFiles` is supplied to the native updater. For spatial views only the
    % ID b-tree is updated.
    LogFiles = case Group0#set_view_group.mod of
    mapreduce_view ->
        [IdFile | ViewFiles];
    spatial_view ->
        [IdFile]
    end,

    % Remove spatial views from group
    % The native updater can currently handle mapreduce views only, but it
    % handles the ID b-tree of the spatial view
    Group = couch_set_view_util:remove_group_views(Group0, spatial_view),

    {ok, NewGroup0, Stats2} = couch_set_view_updater_helper:update_btrees(
        Group, TmpDir, LogFiles, MaxBatchSize, false),
    {IdsInserted, IdsDeleted, KVsInserted, KVsDeleted, CleanupCount} = Stats2,

    % Add back spatial views
    NewGroup = couch_set_view_util:update_group_views(
        NewGroup0, Group0, spatial_view),

    % The native update for the Id b-tree was run, now it's time to run the
    % Erlang updater for the spatial views
    NewGroup2 = case NewGroup#set_view_group.mod of
    mapreduce_view ->
        NewGroup;
    spatial_view = Mod ->
        ok = couch_file:refresh_eof(NewGroup#set_view_group.fd),
        Views = Mod:update_spatial(NewGroup#set_view_group.views, ViewFiles,
            MaxBatchSize),
        NewGroup#set_view_group{
            views = Views,
            index_header = (NewGroup#set_view_group.index_header)#set_view_index_header{
                view_states = [Mod:get_state(V#set_view.indexer) || V <- Views]
            }
        }
    end,

    NewStats = Stats#set_view_updater_stats{
     inserted_ids = Stats#set_view_updater_stats.inserted_ids + IdsInserted,
     deleted_ids = Stats#set_view_updater_stats.deleted_ids + IdsDeleted,
     inserted_kvs = Stats#set_view_updater_stats.inserted_kvs + KVsInserted,
     deleted_kvs = Stats#set_view_updater_stats.deleted_kvs + KVsDeleted
    },

    % Remove files if compactor is not running
    % Otherwise send them to compactor to apply deltas
    CompactFiles = lists:foldr(
        fun(SortedFile, AccCompactFiles) ->
            case CompactorRunning of
            true ->
                case filename:extension(SortedFile) of
                ".compact" ->
                     [SortedFile | AccCompactFiles];
                _ ->
                    SortedFile2 = new_sort_file_name(TmpDir, true),
                    ok = file2:rename(SortedFile, SortedFile2),
                    [SortedFile2 | AccCompactFiles]
                end;
            false ->
                ok = file2:delete(SortedFile),
                AccCompactFiles
            end
        end, [], [IdFile | ViewFiles]),
    {ok, NewGroup2, CleanupCount, NewStats, CompactFiles}.


update_seqs(PartIdSeqs, Seqs) ->
    orddict:fold(
        fun(PartId, NewSeq, Acc) ->
            OldSeq = couch_util:get_value(PartId, Acc, 0),
            case NewSeq > OldSeq of
            true ->
                ok;
            false ->
                exit({error, <<"New seq smaller or equal than old seq.">>, PartId, OldSeq, NewSeq})
            end,
            orddict:store(PartId, NewSeq, Acc)
        end,
        Seqs, PartIdSeqs).

update_versions(PartVersions, AllPartVersions) ->
    lists:ukeymerge(1, PartVersions, AllPartVersions).


update_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Total2 = erlang:max(Total, Changes2),
    Progress = (Changes2 * 100) div Total2,
    couch_task_status:update([
        {progress, Progress},
        {changes_done, Changes2},
        {total_changes, Total2}
    ]).


checkpoint(#writer_acc{owner = Owner, parent = Parent, group = Group} = Acc) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = Type
    } = Group,
    ?LOG_INFO("Updater checkpointing set view `~s` update for ~s group `~s`",
              [SetName, Type, DDocId]),
    NewGroup = maybe_fix_group(Group),
    ok = couch_file:refresh_eof(NewGroup#set_view_group.fd),
    Owner ! {partial_update, Parent, self(), NewGroup},
    receive
    update_processed ->
        ok;
    stop ->
        exit(shutdown)
    end,
    Acc#writer_acc{group = NewGroup}.


maybe_fix_group(#set_view_group{index_header = Header} = Group) ->
    receive
    {new_passive_partitions, Parts} ->
        Bitmask = couch_set_view_util:build_bitmask(Parts),
        {Seqs, PartVersions} = lists:foldl(
            fun(PartId, {SeqAcc, PartVersionsAcc} = Acc) ->
                case couch_set_view_util:has_part_seq(PartId, SeqAcc) of
                true ->
                    Acc;
                false ->
                    {ordsets:add_element({PartId, 0}, SeqAcc),
                        ordsets:add_element({PartId, [{0, 0}]},
                            PartVersionsAcc)}
                end
            end,
            {?set_seqs(Group), ?set_partition_versions(Group)}, Parts),
        Group#set_view_group{
            index_header = Header#set_view_index_header{
                seqs = Seqs,
                pbitmask = ?set_pbitmask(Group) bor Bitmask,
                partition_versions = PartVersions
            }
        }
    after 0 ->
        Group
    end.


check_if_compactor_started(#writer_acc{group = Group0} = Acc) ->
    receive
    {compactor_started, Pid} ->
        erlang:put(new_compactor, true),
        Group = maybe_fix_group(Group0),
        Pid ! {compactor_started_ack, self(), Group},
        Acc#writer_acc{compactor_running = true, group = Group}
    after 0 ->
        Acc
    end.


init_tmp_files(WriterAcc) ->
    #writer_acc{
        group = Group, initial_build = Init, tmp_dir = TmpDir
    } = WriterAcc,
    case WriterAcc#writer_acc.compactor_running of
    true ->
        ok = couch_set_view_util:delete_sort_files(TmpDir, updater);
    false ->
        ok = couch_set_view_util:delete_sort_files(TmpDir, all)
    end,
    Ids = [ids_index | [V#set_view.id_num || V <- Group#set_view_group.views]],
    Files = case Init of
    true ->
        [begin
             FileName = new_sort_file_name(WriterAcc),
             {ok, Fd} = file2:open(FileName, [raw, append, binary]),
             {Id, #set_view_tmp_file_info{fd = Fd, name = FileName}}
         end || Id <- Ids];
    false ->
         [{Id, #set_view_tmp_file_info{}} || Id <- Ids]
    end,
    WriterAcc#writer_acc{tmp_files = dict:from_list(Files)}.


new_sort_file_name(#writer_acc{tmp_dir = TmpDir, compactor_running = Cr}) ->
    new_sort_file_name(TmpDir, Cr).

new_sort_file_name(TmpDir, true) ->
    couch_set_view_util:new_sort_file_path(TmpDir, compactor);
new_sort_file_name(TmpDir, false) ->
    couch_set_view_util:new_sort_file_path(TmpDir, updater).


make_back_index_key(DocId, PartId) ->
    <<PartId:16, DocId/binary>>.


count_seqs_done(Group, NewSeqs) ->
    % NewSeqs might have new passive partitions that Group's seqs doesn't
    % have yet (will get them after a checkpoint period).
    lists:foldl(
        fun({PartId, SeqDone}, Acc) ->
            SeqBefore = couch_util:get_value(PartId, ?set_seqs(Group), 0),
            Acc + (SeqDone - SeqBefore)
        end,
        0, NewSeqs).


close_tmp_fd(#set_view_tmp_file_info{fd = nil}) ->
    ok;
close_tmp_fd(#set_view_tmp_file_info{fd = Fd}) ->
    ok = file:close(Fd).


% DCP introduces the concept of snapshots, where a document mutation is only
% guaranteed to be unique within a single snapshot. But the flusher expects
% unique mutations within the full batch. Merge multiple snapshots (if there
% are any) into a single one. The latest mutation wins.
merge_snapshots(KVs) ->
    merge_snapshots(KVs, false, []).

merge_snapshots([], true, Acc) ->
    {true, Acc};
merge_snapshots([], false, Acc) ->
    % The order of the KVs doesn't really matter, but having them sorted the
    % same way will make life easier when debugging
    {false, lists:reverse(Acc)};
merge_snapshots([snapshot_marker | KVs], _MultipleSnapshots, Acc) ->
    merge_snapshots(KVs, true, Acc);
merge_snapshots([{part_versions, _} = PartVersions | KVs], MultipleSnapshots, Acc) ->
    merge_snapshots(KVs, MultipleSnapshots, [PartVersions | Acc]);
merge_snapshots([KV | KVs], true, Acc0) ->
    {_Seq, DocId, _PartId, _QueryResults} = KV,
    Acc = lists:keystore(DocId, 2, Acc0, KV),
    merge_snapshots(KVs, true, Acc);
merge_snapshots([KV | KVs], false, Acc) ->
    merge_snapshots(KVs, false, [KV | Acc]).
