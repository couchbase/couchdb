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
% Exported for unit tests only.
-export([convert_back_index_kvs_to_binary/2]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-define(MAP_QUEUE_SIZE, 256 * 1024).
-define(WRITE_QUEUE_SIZE, 512 * 1024).

% initial index build
-define(MAX_SORT_BUFFER_SIZE, 1048576).

% incremental updates
-define(CHECKPOINT_WRITE_INTERVAL, 60000000).

% Same as in couch_btree.erl
-define(KEY_BITS,       12).
-define(MAX_KEY_SIZE,   ((1 bsl ?KEY_BITS) - 1)).

-record(writer_acc, {
    parent,
    owner,
    group,
    last_seqs = orddict:new(),
    compactor_running,
    write_queue,
    initial_build,
    view_empty_kvs,
    kvs = [],
    kvs_size = 0,
    kvs_length = 0,
    state = updating_active,
    final_batch = false,
    max_seqs,
    stats = #set_view_updater_stats{},
    merge_buffers = nil,
    tmp_dir = nil,
    sort_files = nil,
    sort_file_workers = [],
    new_partitions = [],
    write_queue_size,
    max_tmp_files,
    max_insert_batch_size,
    min_batch_size_per_view
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
    InitialBuild = couch_set_view_util:is_group_empty(Group),
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
              "Min # changes:        ~p~n",
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
               NumChanges
              ]),

    WriterAcc0 = #writer_acc{
        parent = self(),
        owner = Owner,
        group = Group,
        initial_build = InitialBuild,
        max_seqs = CurSeqs,
        tmp_dir = TmpDir,
        max_tmp_files = list_to_integer(
            couch_config:get("set_views", "indexer_max_tmp_files", "16")),
        max_insert_batch_size = list_to_integer(
            couch_config:get("set_views", "indexer_max_insert_batch_size", "1048576")),
        min_batch_size_per_view = list_to_integer(
            couch_config:get("set_views", "indexer_min_batch_size_per_view", "1048576"))
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
        sig = GroupSig
    } = Group,

    StartTime = os:timestamp(),

    MapQueueOptions = [{max_size, ?MAP_QUEUE_SIZE}, {max_items, infinity}],
    WriteQueueOptions = case WriterAcc#writer_acc.initial_build of
    true ->
        [{max_size, ?MAX_SORT_BUFFER_SIZE div 2}, {max_items, infinity}];
    false ->
        [{max_size, ?WRITE_QUEUE_SIZE}, {max_items, infinity}]
    end,
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
    unlink(BarrierEntryPid),
    Writer = spawn_link(fun() ->
        DDocIds = couch_set_view_util:get_ddoc_ids_with_sig(SetName, Group),
        couch_task_status:add_task([
            {type, indexer},
            {set, SetName},
            {signature, ?l2b(couch_util:to_hex(GroupSig))},
            {design_documents, DDocIds},
            {indexer_type, Type},
            {progress, 0},
            {changes_done, 0},
            {initial_build, WriterAcc#writer_acc.initial_build},
            {total_changes, NumChanges}
        ]),
        BarrierEntryPid ! shutdown,
        couch_task_status:set_update_frequency(5000),

        ViewEmptyKVs = [{View, []} || View <- Group#set_view_group.views],
        WriterAcc2 = init_view_merge_params(WriterAcc#writer_acc{
            parent = Parent,
            group = Group,
            write_queue = WriteQueue,
            view_empty_kvs = ViewEmptyKVs,
            compactor_running = CompactorRunning,
            write_queue_size = couch_util:get_value(max_size, WriteQueueOptions),
            new_partitions = [P || {P, Seq} <- ?set_seqs(Group), Seq == 0]
        }),
        delete_prev_sort_files(WriterAcc2),
        ok = couch_set_view_util:open_raw_read_fd(Group),
        try
            couch_set_view_mapreduce:start_reduce_context(Group),
            try
                FinalWriterAcc = do_writes(WriterAcc2),
                Parent ! {writer_finished, FinalWriterAcc}
            after
                couch_set_view_mapreduce:end_reduce_context(Group)
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

    DocLoader = spawn_link(fun() ->
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
            load_changes(Owner, Parent, Group, MapQueue,
                         ActiveParts, PassiveParts,
                         WriterAcc#writer_acc.initial_build)
        catch _:Error ->
            Stacktrace = erlang:get_stacktrace(),
            ?LOG_ERROR("Set view `~s`, ~s group `~s`, doc loader error~n"
                "error:      ~p~n"
                "stacktrace: ~p~n",
                [SetName, Type, DDocId, Error, Stacktrace]),
            exit(Error)
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
    {writer_finished, WriterAcc} ->
        Stats0 = WriterAcc#writer_acc.stats,
        Result = #set_view_updater_result{
            group = WriterAcc#writer_acc.group,
            state = WriterAcc#writer_acc.state,
            stats = Stats0#set_view_updater_stats{
                indexing_time = timer:now_diff(os:timestamp(), StartTime) / 1000000,
                blocked_time = BlockedTime
            }
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
        {Pid, Ref} = erlang:erase(compactor_pid),
        Pid ! {Ref, {ok, GroupSnapshot}},
        wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime, OldGroup);
    {'EXIT', _, Reason} when Reason =/= normal ->
        couch_util:shutdown_sync(DocLoader),
        couch_util:shutdown_sync(Mapper),
        couch_util:shutdown_sync(Writer),
        {updater_error, Reason}
    end.


load_changes(Owner, Updater, Group, MapQueue, ActiveParts, PassiveParts, InitialBuild) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = GroupType,
        index_header = #set_view_index_header{seqs = SinceSeqs}
    } = Group,

    FoldFun = fun(PartId, {AccCount, AccSeqs}) ->
        case couch_set_view_util:has_part_seq(PartId, ?set_unindexable_seqs(Group)) of
        true ->
            {AccCount, AccSeqs};
        false ->
            Db = case couch_db:open_int(?dbname(SetName, PartId), []) of
            {ok, PartDb} ->
                PartDb;
            Error ->
                ErrorMsg = io_lib:format("Updater error opening database `~s': ~w",
                                         [?dbname(SetName, PartId), Error]),
                throw({error, iolist_to_binary(ErrorMsg)})
            end,
            try
                Since = couch_util:get_value(PartId, SinceSeqs, 0),
                ChangesWrapper = fun(DocInfo, _, AccCount2) ->
                    load_doc(Db, PartId, DocInfo, MapQueue, Group, InitialBuild),
                    {ok, AccCount2 + 1}
                end,
                {ok, _, AccCount3} = couch_db:fast_reads(Db, fun() ->
                    couch_db:enum_docs_since(Db, Since, ChangesWrapper, AccCount, [])
                end),
                {AccCount3, orddict:store(PartId, Db#db.update_seq, AccSeqs)}
            after
                ok = couch_db:close(Db)
            end
        end
    end,

    notify_owner(Owner, {state, updating_active}, Updater),
    case ActiveParts of
    [] ->
        ActiveChangesCount = 0,
        MaxSeqs = orddict:new();
    _ ->
        ?LOG_INFO("Updater reading changes from active partitions to "
                  "update ~s set view group `~s` from set `~s`",
                  [GroupType, DDocId, SetName]),
        {ActiveChangesCount, MaxSeqs} = lists:foldl(
            FoldFun, {0, orddict:new()}, ActiveParts)
    end,
    case PassiveParts of
    [] ->
        FinalChangesCount = ActiveChangesCount,
        MaxSeqs2 = MaxSeqs;
    _ ->
        ?LOG_INFO("Updater reading changes from passive partitions to "
                  "update ~s set view group `~s` from set `~s`",
                  [GroupType, DDocId, SetName]),
        {FinalChangesCount, MaxSeqs2} = lists:foldl(
            FoldFun, {ActiveChangesCount, MaxSeqs}, PassiveParts)
    end,
    {FinalChangesCount3, MaxSeqs3} = load_changes_from_passive_parts_in_mailbox(
        Group, FoldFun, FinalChangesCount, MaxSeqs2),
    couch_work_queue:close(MapQueue),
    ?LOG_INFO("Updater for ~s set view group `~s`, set `~s`, read a total of ~p changes",
              [GroupType, DDocId, SetName, FinalChangesCount3]),
    ?LOG_DEBUG("Updater for ~s set view group `~s`, set `~s`, max partition seqs found:~n~w",
               [GroupType, DDocId, SetName, MaxSeqs3]).


load_changes_from_passive_parts_in_mailbox(Group, FoldFun, ChangesCount, MaxSeqs) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = GroupType
    } = Group,
    receive
    {new_passive_partitions, Parts0} ->
        Parts = get_more_passive_partitions(Parts0),
        ?LOG_INFO("Updater reading changes from new passive partitions ~w to "
                  "update ~s set view group `~s` from set `~s`",
                  [Parts, GroupType, DDocId, SetName]),
        {ChangesCount2, MaxSeqs2} = lists:foldl(FoldFun, {ChangesCount, MaxSeqs}, Parts),
        load_changes_from_passive_parts_in_mailbox(Group, FoldFun, ChangesCount2, MaxSeqs2)
    after 0 ->
        {ChangesCount, MaxSeqs}
    end.


get_more_passive_partitions(Parts) ->
    receive
    {new_passive_partitions, Parts2} ->
        get_more_passive_partitions(Parts ++ Parts2)
    after 0 ->
        Parts
    end.


notify_owner(Owner, Msg, UpdaterPid) ->
    Owner ! {updater_info, UpdaterPid, Msg}.


load_doc(Db, PartitionId, DocInfo, MapQueue, Group, InitialBuild) ->
    #doc_info{id=DocId, local_seq=Seq, deleted=Deleted} = DocInfo,
    case DocId of
    <<?DESIGN_DOC_PREFIX, _/binary>> ->
        ok;
    _ ->
        case Deleted of
        true when InitialBuild ->
            ok;
        true ->
            Entry = {Seq, #doc{id = DocId, deleted = true}, PartitionId},
            couch_work_queue:queue(MapQueue, Entry);
        false ->
            case couch_util:validate_utf8(DocId) of
            true ->
                {ok, Doc} = couch_db:open_doc_int(Db, DocInfo, []),
                couch_work_queue:queue(MapQueue, {Seq, Doc, PartitionId});
            false ->
                #set_view_group{
                   set_name = SetName,
                   name = DDocId,
                   type = GroupType
               } = Group,
                % If the id isn't utf8 (memcached allows it), then log an error
                % message and skip the doc. Send it through the queue anyway
                % so we record the high seq num in case there are a bunch of
                % these at the end, we want to keep track of the high seq and
                % not reprocess again.
                ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, skipping "
                    "document with non-utf8 id. Doc id bytes: ~w",
                    [SetName, GroupType, DDocId, ?b2l(DocId)]),
                Entry = {Seq, #doc{id = DocId, deleted = true}, PartitionId},
                couch_work_queue:queue(MapQueue, Entry)
            end
        end
    end.


do_maps(Group, MapQueue, WriteQueue) ->
    case couch_work_queue:dequeue(MapQueue) of
    closed ->
        couch_work_queue:close(WriteQueue);
    {ok, Queue, _QueueSize} ->
        Items = lists:foldr(
            fun({Seq, #doc{id = Id, deleted = true}, PartitionId}, Acc) ->
                Item = {Seq, Id, PartitionId, []},
                [Item | Acc];
            ({Seq, #doc{id = Id, deleted = false} = Doc, PartitionId}, Acc) ->
                try
                    {ok, Result} = couch_set_view_mapreduce:map(Doc),
                    Item = {Seq, Id, PartitionId, Result},
                    [Item | Acc]
                catch _:{error, Reason} ->
                    #set_view_group{
                        set_name = SetName,
                        name = DDocId,
                        type = Type
                    } = Group,
                    ErrorMsg = "Bucket `~s`, ~s group `~s`, error mapping document `~s`: ~s",
                    Args = [SetName, Type, DDocId, Id, couch_util:to_binary(Reason)],
                    ?LOG_MAPREDUCE_ERROR(ErrorMsg, Args),
                    [{Seq, Id, PartitionId, []} | Acc]
                end
            end,
            [], Queue),
        ok = couch_work_queue:queue(WriteQueue, Items),
        do_maps(Group, MapQueue, WriteQueue)
    end.


do_writes(Acc) ->
    #writer_acc{
        kvs = Kvs,
        kvs_size = KvsSize,
        kvs_length = KvsLength,
        write_queue = WriteQueue
    } = Acc,
    case couch_work_queue:dequeue(WriteQueue) of
    closed ->
        flush_writes(Acc#writer_acc{final_batch = true});
    {ok, Queue0, QueueSize} ->
        Queue = lists:flatten(Queue0),
        Kvs2 = Kvs ++ Queue,
        KvsSize2 = KvsSize + QueueSize,
        KvsLength2 = KvsLength + length(Queue),
        Acc2 = Acc#writer_acc{
            kvs = Kvs2,
            kvs_size = KvsSize2,
            kvs_length = KvsLength2
        },
        case should_flush_writes(Acc2) of
        true ->
            Acc3 = flush_writes(Acc2),
            Acc4 = Acc3#writer_acc{kvs = [], kvs_size = 0, kvs_length = 0};
        false ->
            Acc4 = Acc2
        end,
        do_writes(Acc4)
    end.


should_flush_writes(#writer_acc{initial_build = true} = Acc) ->
    #writer_acc{kvs_size = KvsSize, write_queue_size = Wqsz} = Acc,
    KvsSize >= Wqsz;
should_flush_writes(#writer_acc{initial_build = false} = Acc) ->
    #writer_acc{
        view_empty_kvs = ViewEmptyKvs,
        kvs_size = KvsSize,
       min_batch_size_per_view = MinBatchSizePerView
    } = Acc,
    KvsSize >= (MinBatchSizePerView * length(ViewEmptyKvs)).


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
        last_seqs = LastSeqs
    } = Acc0,
    {ViewKVs, DocIdViewIdKeys, NewPartIdSeqs} =
        process_map_results(Kvs, ViewEmptyKVs, orddict:new()),
    NewLastSeqs = orddict:merge(
        fun(_, S1, S2) -> erlang:max(S1, S2) end,
        LastSeqs,
        NewPartIdSeqs),
    Acc1 = Acc0#writer_acc{last_seqs = NewLastSeqs},
    Acc = write_to_tmp_batch_files(ViewKVs, DocIdViewIdKeys, Acc1),
    #writer_acc{group = NewGroup} = Acc,
    case ?set_seqs(NewGroup) =/= ?set_seqs(Group) of
    true ->
        case (Acc#writer_acc.state =:= updating_active) andalso
        lists:any(fun({PartId, _}) ->
            ((1 bsl PartId) band ?set_pbitmask(Group) =/= 0)
        end, NewLastSeqs) of
        true ->
            Acc2 = checkpoint(Acc),
            notify_owner(Owner, {state, updating_passive}, Parent),
            Acc2#writer_acc{state = updating_passive};
        false ->
            case Acc#writer_acc.final_batch orelse
                (?set_cbitmask(Acc#writer_acc.group) /= ?set_cbitmask(Group)) of
            true ->
                checkpoint(Acc);
            false ->
                maybe_checkpoint(Acc)
            end
        end;
    false ->
        Acc
    end;

flush_writes(#writer_acc{initial_build = true} = WriterAcc) ->
    #writer_acc{
        kvs = Kvs,
        kvs_length = KvsLength,
        view_empty_kvs = ViewEmptyKVs,
        merge_buffers = Buffers,
        group = Group,
        final_batch = IsFinalBatch,
        max_seqs = MaxSeqs,
        stats = Stats
    } = WriterAcc,
    #set_view_group{
        id_btree = IdBtree,
        set_name = SetName,
        type = Type,
        name = DDocId,
        fd = GroupFd
    } = Group,
    {ViewKVs, DocIdViewIdKeys, MaxSeqs2} = process_map_results(Kvs, ViewEmptyKVs, MaxSeqs),
    {IdBuffer, IdBufferSize} = dict:fetch(ids_index, Buffers),
    {NewIdBuffer0, NewIdBufferSize} = lists:foldr(
        fun({_DocId, {_PartId, []}}, Acc) ->
            Acc;
        (Kv, {AccBuf, AccSize}) ->
            [{KeyBin, ValBin}] = convert_back_index_kvs_to_binary([Kv], []),
            KvBin = [<<(byte_size(KeyBin)):16>>, KeyBin, ValBin],
            KvBinSize = iolist_size(KvBin),
            {[[<<KvBinSize:32>>, KvBin] | AccBuf], AccSize + KvBinSize}
        end,
        {IdBuffer, IdBufferSize},
        DocIdViewIdKeys),
    NewIdBuffer = case (NewIdBufferSize >= ?MAX_SORT_BUFFER_SIZE) orelse IsFinalBatch of
    true ->
        IdLessFun = fun([_, [_, A, _]], [_, [_, B, _]]) -> (IdBtree#btree.less)(A, B) end,
        lists:sort(IdLessFun, NewIdBuffer0);
    false ->
        NewIdBuffer0
    end,
    {NewBuffers0, InsertKVCount} = lists:foldl(
        fun({#set_view{id_num = Id}, KvList}, {AccBuffers, AccCount}) ->
            {Buf, BufSize} = dict:fetch(Id, AccBuffers),
            {NewBuf, NewBufSize, AccCount3} = lists:foldr(
                fun({KeyBin, ValBin}, {AccBuf, AccBufSize, AccCount2}) ->
                    KvBin = [<<(byte_size(KeyBin)):16>>, KeyBin, ValBin],
                    KvBinSize = iolist_size(KvBin),
                    AccBuf2 = [[<<KvBinSize:32>>, KvBin] | AccBuf],
                    {AccBuf2, AccBufSize + KvBinSize, AccCount2 + 1}
                end,
                {Buf, BufSize, AccCount},
                convert_primary_index_kvs_to_binary(KvList, Group, [])),
            {dict:store(Id, {NewBuf, NewBufSize}, AccBuffers), AccCount3}
        end,
        {dict:store(ids_index, {NewIdBuffer, NewIdBufferSize}, Buffers), 0},
        ViewKVs),

    NewBuffers = lists:foldl(
        fun(#set_view{id_num = Id, btree = Bt}, AccBuffers) ->
            {Buf, BufSize} = dict:fetch(Id, AccBuffers),
            case (BufSize >= ?MAX_SORT_BUFFER_SIZE) orelse IsFinalBatch of
            true ->
                ViewLessFun = fun([_, [_, A, _]], [_, [_, B, _]]) -> (Bt#btree.less)(A, B) end,
                Buf2 = lists:sort(ViewLessFun, Buf),
                dict:store(Id, {Buf2, BufSize}, AccBuffers);
            false ->
                AccBuffers
            end
        end,
        NewBuffers0,
        Group#set_view_group.views),

    {NewBuffers2, NewSortFiles2, SortFileWorkers2} =
        maybe_flush_merge_buffers(NewBuffers, WriterAcc),
    update_task(KvsLength),
    case IsFinalBatch of
    false ->
        WriterAcc#writer_acc{
            merge_buffers = NewBuffers2,
            sort_files = NewSortFiles2,
            sort_file_workers = SortFileWorkers2,
            max_seqs = MaxSeqs2,
            stats = Stats#set_view_updater_stats{
                inserted_kvs = Stats#set_view_updater_stats.inserted_kvs + InsertKVCount,
                inserted_ids = Stats#set_view_updater_stats.inserted_ids + length(DocIdViewIdKeys)
            }
        };
    true ->
        ?LOG_INFO("Updater for set view `~s`, ~s group `~s`, performing final "
                  "btree build phase", [SetName, Type, DDocId]),
        wait_for_workers(SortFileWorkers2),
        [{_, IdsSortedFile}] = dict:fetch(ids_index, NewSortFiles2),
        {ok, NewIdBtreeRoot} = couch_btree_copy:from_sorted_file(
            IdBtree, IdsSortedFile, GroupFd, fun file_sorter_initial_build_format_fun/1),
        NewIdBtree = IdBtree#btree{root = NewIdBtreeRoot},
        ok = file2:delete(IdsSortedFile),
        NewViews = lists:map(
            fun(#set_view{id_num = Id, btree = Bt} = View) ->
               [{_, KvSortedFile}] = dict:fetch(Id, NewSortFiles2),
               {ok, NewBtRoot} = couch_btree_copy:from_sorted_file(
                   Bt, KvSortedFile, GroupFd, fun file_sorter_initial_build_format_fun/1),
               ok = file2:delete(KvSortedFile),
               View#set_view{
                   btree = Bt#btree{root = NewBtRoot}
               }
            end,
            Group#set_view_group.views),
        Header = Group#set_view_group.index_header,
        NewHeader = Header#set_view_index_header{
            id_btree_state = couch_btree:get_state(NewIdBtree),
            view_states = [couch_btree:get_state(V#set_view.btree) || V <- NewViews],
            seqs = MaxSeqs2
        },
        update_task(1),
        WriterAcc#writer_acc{
            sort_files = nil,
            sort_file_workers = [],
            max_seqs = MaxSeqs2,
            stats = Stats#set_view_updater_stats{
                inserted_kvs = Stats#set_view_updater_stats.inserted_kvs + InsertKVCount,
                inserted_ids = Stats#set_view_updater_stats.inserted_ids + length(DocIdViewIdKeys)
            },
            group = Group#set_view_group{
                id_btree = NewIdBtree,
                views = NewViews,
                index_header = NewHeader
            }
        }
    end.


process_map_results(Kvs, ViewEmptyKVs, PartSeqs) ->
    lists:foldl(
        fun({Seq, DocId, PartId, []}, {ViewKVsAcc, DocIdViewIdKeysAcc, PartIdSeqs}) ->
            PartIdSeqs2 = update_part_seq(Seq, PartId, PartIdSeqs),
            {ViewKVsAcc, [{DocId, {PartId, []}} | DocIdViewIdKeysAcc], PartIdSeqs2};
        ({Seq, DocId, PartId, QueryResults}, {ViewKVsAcc, DocIdViewIdKeysAcc, PartIdSeqs}) ->
            {NewViewKVs, NewViewIdKeys} = view_insert_doc_query_results(
                    DocId, PartId, QueryResults, ViewKVsAcc, [], []),
            PartIdSeqs2 = update_part_seq(Seq, PartId, PartIdSeqs),
            {NewViewKVs, [{DocId, {PartId, NewViewIdKeys}} | DocIdViewIdKeysAcc], PartIdSeqs2}
        end,
        {ViewEmptyKVs, [], PartSeqs}, Kvs).


% initial build
maybe_flush_merge_buffers(BuffersDict, WriterAcc) ->
    #writer_acc{
        view_empty_kvs = ViewEmptyKVs,
        sort_files = SortFilesDict,
        sort_file_workers = Workers,
        tmp_dir = TmpDir,
        group = #set_view_group{id_btree = IdBtree},
        final_batch = IsFinalBatch,
        max_tmp_files = MaxTmpFiles
    } = WriterAcc,
    ViewInfos = [
        {ids_index, IdBtree#btree.less} |
        [{V#set_view.id_num, (V#set_view.btree)#btree.less} || {V, _} <- ViewEmptyKVs]
    ],
    NumBtrees = length(ViewEmptyKVs) + 1,
    lists:foldl(
        fun({Id, Less}, {AccBuffers, AccFiles, AccWorkers}) ->
            {Buf, BufSize} = dict:fetch(Id, AccBuffers),
            case (BufSize >= ?MAX_SORT_BUFFER_SIZE) orelse IsFinalBatch of
            true ->
                SortFiles = dict:fetch(Id, AccFiles),
                FileName = new_sort_file_name(WriterAcc),
                {ok, Fd} = file2:open(FileName, [raw, append, binary]),
                ok = file:write(Fd, Buf),
                ok = file:close(Fd),
                AccBuffers2 = dict:store(Id, {[], 0}, AccBuffers),
                SortFiles2 = ordsets:add_element({0, FileName}, SortFiles),
                case (ordsets:size(SortFiles2) >= MaxTmpFiles) orelse IsFinalBatch of
                true ->
                    LessFun = fun({A, _}, {B, _}) -> Less(A, B) end,
                    MergeSortFile = new_sort_file_name(WriterAcc),
                    {FilesToMerge, NextGen, RestSortFiles} =
                        split_files_to_merge(SortFiles2, IsFinalBatch),
                    case FilesToMerge of
                    [_] ->
                        {AccBuffers2, dict:store(Id, SortFiles2, AccFiles), AccWorkers};
                    [_, _ | _] ->
                        case length(AccWorkers) >= NumBtrees of
                        true ->
                            [OldestWorker | RestWorkersRev] = lists:reverse(AccWorkers),
                            wait_for_workers([OldestWorker]),
                            AccWorkers2 = lists:reverse(RestWorkersRev);
                        false ->
                            AccWorkers2 = AccWorkers
                        end,
                        MergeWorker = spawn_merge_worker(
                            LessFun, TmpDir, AccWorkers2, FilesToMerge, MergeSortFile),
                        SortFiles3 = ordsets:add_element({NextGen, MergeSortFile}, RestSortFiles),
                        {AccBuffers2, dict:store(Id, SortFiles3, AccFiles), [MergeWorker | AccWorkers2]}
                    end;
                false ->
                    {AccBuffers2, dict:store(Id, SortFiles2, AccFiles), AccWorkers}
                end;
            false ->
                {AccBuffers, AccFiles, AccWorkers}
            end
        end,
        {BuffersDict, SortFilesDict, Workers},
        ViewInfos).


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


update_part_seq(Seq, PartId, Acc) ->
    case couch_set_view_util:find_part_seq(PartId, Acc) of
    {ok, Max} when Max >= Seq ->
        Acc;
    _ ->
        orddict:store(PartId, Seq, Acc)
    end.


view_insert_doc_query_results(_DocId, _PartitionId, [], [], ViewKVsAcc, ViewIdKeysAcc) ->
    {lists:reverse(ViewKVsAcc), lists:reverse(ViewIdKeysAcc)};
view_insert_doc_query_results(DocId, PartitionId, [ResultKVs | RestResults],
        [{View, KVs} | RestViewKVs], ViewKVsAcc, ViewIdKeysAcc) ->
    % Take any identical keys and combine the values
    {NewKVs, NewViewIdKeysAcc} = lists:foldl(
        fun({Key, Val}, {[{{Key, PrevDocId} = Kd, PrevVal} | AccRest], AccVid}) when PrevDocId =:= DocId ->
            AccKv2 = case PrevVal of
            {PartitionId, {dups, Dups}} ->
                [{Kd, {PartitionId, {dups, [Val | Dups]}}} | AccRest];
            {PartitionId, UserPrevVal} ->
                [{Kd, {PartitionId, {dups, [Val, UserPrevVal]}}} | AccRest]
            end,
            {AccKv2, AccVid};
        ({Key, Val}, {AccKv, AccVid}) ->
            {[{{Key, DocId}, {PartitionId, Val}} | AccKv], [Key | AccVid]}
        end,
        {KVs, []}, lists:sort(ResultKVs)),
    NewViewKVsAcc = [{View, NewKVs} | ViewKVsAcc],
    case NewViewIdKeysAcc of
    [] ->
        NewViewIdKeysAcc2 = ViewIdKeysAcc;
    _ ->
        NewViewIdKeysAcc2 = [{View#set_view.id_num, NewViewIdKeysAcc} | ViewIdKeysAcc]
    end,
    view_insert_doc_query_results(
        DocId, PartitionId, RestResults, RestViewKVs, NewViewKVsAcc, NewViewIdKeysAcc2).


% Incremental updates.
write_to_tmp_batch_files(ViewKeyValuesToAdd, DocIdViewIdKeys, WriterAcc) ->
    #writer_acc{
        sort_files = SortFiles,
        group = #set_view_group{id_btree = IdBtree} = Group,
        new_partitions = NewParts
    } = WriterAcc,

    {AddDocIdViewIdKeys0, RemoveDocIds, LookupDocIds} = lists:foldr(
        fun({DocId, {PartId, [] = _ViewIdKeys}}, {A, B, C}) ->
                BackKey = make_back_index_key(DocId, PartId),
                case lists:member(PartId, NewParts) of
                true ->
                    {A, [BackKey | B], C};
                false ->
                    {A, [BackKey | B], [BackKey | C]}
                end;
            ({DocId, {PartId, _ViewIdKeys}} = KvPairs, {A, B, C}) ->
                BackKey = make_back_index_key(DocId, PartId),
                case lists:member(PartId, NewParts) of
                true ->
                    {[KvPairs | A], B, C};
                false ->
                    {[KvPairs | A], B, [BackKey | C]}
                end
        end,
        {[], [], []}, DocIdViewIdKeys),

    AddDocIdViewIdKeys = convert_back_index_kvs_to_binary(AddDocIdViewIdKeys0, []),

    IdsData1 = lists:map(
        fun(K) -> couch_set_view_updater_helper:encode_btree_op(remove, K) end,
        RemoveDocIds),

    IdsData2 = lists:foldl(
        fun({K, V}, Acc) ->
            Bin = couch_set_view_updater_helper:encode_btree_op(insert, K, V),
            [Bin | Acc]
        end,
        IdsData1,
        AddDocIdViewIdKeys),

    BaseIdLess = couch_set_view_updater_helper:batch_sort_fun(IdBtree#btree.less),
    IdsData3 = lists:sort(
       fun(<<_:32, A/binary>>, <<_:32, B/binary>>) -> BaseIdLess(A, B) end,
       IdsData2),
    NewIdsIndexFile = new_sort_file_name(WriterAcc),
    {ok, IdsIndexFd} = file2:open(NewIdsIndexFile, [raw, append, binary]),
    ok = file:write(IdsIndexFd, IdsData3),
    ok = file:close(IdsIndexFd),

    IdsIndexFiles = dict:fetch(ids_index, SortFiles),
    SortFiles2 = dict:store(ids_index, [NewIdsIndexFile | IdsIndexFiles], SortFiles),

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
                        EncodedKeys = [couch_set_view_util:encode_key_docid(Key, DocId) || Key <- Keys],
                        dict:append_list(ViewId, EncodedKeys, KeysToRemoveByViewAcc2)
                    end,
                    KeysToRemoveByViewAcc, couch_set_view_util:parse_view_id_keys(ViewIdKeys));
            {not_found, _} ->
                KeysToRemoveByViewAcc
            end
        end,
        dict:new(), LookupResults),

    SortFiles3 = lists:foldl(
        fun({#set_view{id_num = ViewId, btree = Bt}, AddKeyValues}, AccSortFiles) ->
            AddKeyValuesBinaries = convert_primary_index_kvs_to_binary(AddKeyValues, Group, []),
            KeysToRemove = couch_util:dict_find(ViewId, KeysToRemoveByView, []),
            BatchData = lists:map(
                fun(K) -> couch_set_view_updater_helper:encode_btree_op(remove, K) end,
                KeysToRemove),
            BatchData2 = lists:foldl(
                fun({K, V}, Acc) ->
                    Bin = couch_set_view_updater_helper:encode_btree_op(insert, K, V),
                    [Bin | Acc]
                end,
                BatchData, AddKeyValuesBinaries),
            BaseLess = couch_set_view_updater_helper:batch_sort_fun(Bt#btree.less),
            BatchData3 = lists:sort(
                fun(<<_:32, A/binary>>, <<_:32, B/binary>>) -> BaseLess(A, B) end,
                BatchData2),
            NewBatchFile = new_sort_file_name(WriterAcc),
            {ok, Fd} = file2:open(NewBatchFile, [raw, append, binary]),
            ok = file:write(Fd, BatchData3),
            ok = file:close(Fd),
            ViewBatchFiles = dict:fetch(ViewId, AccSortFiles),
            dict:store(ViewId, [NewBatchFile | ViewBatchFiles], AccSortFiles)
        end,
        SortFiles2,
        ViewKeyValuesToAdd),

    WriterAcc2 = WriterAcc#writer_acc{
        sort_files = SortFiles3
    },
    maybe_update_btrees(WriterAcc2).


maybe_update_btrees(WriterAcc0) ->
    #writer_acc{
        view_empty_kvs = ViewEmptyKVs,
        sort_files = SortFiles,
        group = Group0,
        stats = #set_view_updater_stats{seqs = SeqsDoneBefore},
        final_batch = IsFinalBatch,
        owner = Owner,
        last_seqs = LastSeqs,
        max_tmp_files = MaxTmpFiles,
        tmp_dir = TmpDir,
        compactor_running = CompactorRunning
    } = WriterAcc0,
    ShouldFlush = IsFinalBatch orelse
        length(dict:fetch(ids_index, SortFiles)) >= MaxTmpFiles orelse
        lists:any(
            fun({#set_view{id_num = Id}, _}) ->
                length(dict:fetch(Id, SortFiles)) >= MaxTmpFiles
            end, ViewEmptyKVs),
    case ShouldFlush of
    false ->
        NewLastSeqs1 = LastSeqs,
        case erlang:get(updater_worker) of
        undefined ->
            WriterAcc = WriterAcc0;
        UpdaterWorker when is_reference(UpdaterWorker) ->
            receive
            {UpdaterWorker, UpGroup, UpStats, CompactFiles} ->
                send_log_compact_files(Owner, CompactFiles, ?set_seqs(UpGroup)),
                erlang:erase(updater_worker),
                WriterAcc = check_if_compactor_started(
                    WriterAcc0#writer_acc{group = UpGroup, stats = UpStats})
            after 0 ->
                WriterAcc = WriterAcc0
            end
        end;
    true ->
        ViewInfos = [
            {ids_index, Group0#set_view_group.id_btree} |
            [{V#set_view.id_num, V#set_view.btree} || V <- Group0#set_view_group.views]
        ],
        SortFiles1 = lists:foldl(
            fun({Id, Bt}, A) ->
                Files = dict:fetch(Id, SortFiles),
                SortedFile = merge_files(Files, Bt, TmpDir, CompactorRunning),
                dict:store(Id, [SortedFile], A)
            end,
            SortFiles, ViewInfos),
        case erlang:erase(updater_worker) of
        undefined ->
            WriterAcc1 = WriterAcc0#writer_acc{sort_files = SortFiles1};
        UpdaterWorker when is_reference(UpdaterWorker) ->
            receive
            {UpdaterWorker, UpGroup2, UpStats2, CompactFiles2} ->
                send_log_compact_files(Owner, CompactFiles2, ?set_seqs(UpGroup2)),
                WriterAcc1 = check_if_compactor_started(
                    WriterAcc0#writer_acc{
                        group = UpGroup2,
                        stats = UpStats2,
                        sort_files = SortFiles1
                    })
            end
        end,
        WriterAcc2 = check_if_compactor_started(WriterAcc1),
        NewUpdaterWorker = spawn_updater_worker(WriterAcc2, LastSeqs),
        NewLastSeqs1 = orddict:new(),
        erlang:put(updater_worker, NewUpdaterWorker),
        SortFiles2 = dict:map(fun(_, _) -> [] end, SortFiles),
        WriterAcc = WriterAcc2#writer_acc{sort_files = SortFiles2}
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
                send_log_compact_files(Owner, CompactFiles3, ?set_seqs(NewGroup))
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
    case (not IsFinalBatch) andalso (NewGroup =/= Group0) of
    true ->
        NewWriterAcc2 = maybe_checkpoint(NewWriterAcc);
    false ->
        NewWriterAcc2 = NewWriterAcc
    end,
    SeqsDone = NewStats#set_view_updater_stats.seqs - SeqsDoneBefore,
    case SeqsDone of
    _ when SeqsDone > 0 ->
        update_task(SeqsDone);
    0 ->
        ok
    end,
    NewWriterAcc2.


send_log_compact_files(_Owner, [], _Seqs) ->
    ok;
send_log_compact_files(Owner, Files, Seqs) ->
    ok = gen_server:cast(Owner, {compact_log_files, Files, Seqs}).


spawn_updater_worker(WriterAcc, PartIdSeqs) ->
    Parent = self(),
    Ref = make_ref(),
    #writer_acc{
        group = Group,
        max_seqs = MaxSeqs
    } = WriterAcc,
    _Pid = spawn_link(fun() ->
        case ?set_cbitmask(Group) of
        0 ->
            CleanupStart = 0;
        _ ->
            CleanupStart = os:timestamp()
        end,
        couch_set_view_mapreduce:start_reduce_context(Group),
        {ok, NewBtrees, CleanupCount, NewStats, NewCompactFiles} = update_btrees(WriterAcc),
        couch_set_view_mapreduce:end_reduce_context(Group),
        [IdBtree2 | ViewBtrees2] = NewBtrees,
        case ?set_cbitmask(Group) of
        0 ->
            NewCbitmask = 0,
            CleanupTime = 0.0;
        _ ->
            {ok, <<_:40, IdBitmap:?MAX_NUM_PARTITIONS>>} = couch_btree:full_reduce(IdBtree2),
            CombinedBitmap = lists:foldl(
                fun(Bt, AccMap) ->
                    {ok, <<_:40, Bm:?MAX_NUM_PARTITIONS, _/binary>>} = couch_btree:full_reduce(Bt),
                    AccMap bor Bm
                end,
                IdBitmap, ViewBtrees2),
            NewCbitmask = ?set_cbitmask(Group) band CombinedBitmap,
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
        Header = Group#set_view_group.index_header,
        NewHeader = Header#set_view_index_header{
            id_btree_state = couch_btree:get_state(IdBtree2),
            view_states = lists:map(fun couch_btree:get_state/1, ViewBtrees2),
            seqs = NewSeqs,
            cbitmask = NewCbitmask
        },
        NewGroup0 = Group#set_view_group{
            views = lists:zipwith(
                fun(V, Bt) -> V#set_view{btree = Bt} end,
                Group#set_view_group.views,
                ViewBtrees2),
            id_btree = IdBtree2,
            index_header = NewHeader
        },
        NewGroup = update_transferred_replicas(NewGroup0, MaxSeqs, PartIdSeqs),
        NumChanges = count_seqs_done(Group, NewSeqs),
        NewStats2 = NewStats#set_view_updater_stats{
           seqs = NewStats#set_view_updater_stats.seqs + NumChanges,
           cleanup_time = NewStats#set_view_updater_stats.seqs + CleanupTime,
           cleanup_kv_count = NewStats#set_view_updater_stats.cleanup_kv_count + CleanupCount
        },
        Parent ! {Ref, NewGroup, NewStats2, NewCompactFiles}
    end),
    Ref.


update_btrees(WriterAcc) ->
    #writer_acc{
        stats = Stats,
        group = Group,
        tmp_dir = TmpDir,
        sort_files = SortFiles,
        compactor_running = CompactorRunning,
        max_insert_batch_size = MaxBatchSize
    } = WriterAcc,
    ViewInfos = [
        {ids_index, Group#set_view_group.id_btree} |
        [{V#set_view.id_num, V#set_view.btree} || V <- Group#set_view_group.views]
    ],
    CleanupAcc0 = {go, 0},
    case ?set_cbitmask(Group) of
    0 ->
        CleanupFun = nil;
    _ ->
        CleanupFun = couch_set_view_util:make_btree_purge_fun(Group)
    end,
    ok = couch_set_view_util:open_raw_read_fd(Group),
    {NewBtrees, {{_, CleanupCount}, NewStats, CompactFiles}} = lists:mapfoldl(
        fun({ViewId, Bt}, {CleanupAcc, StatsAcc, AccCompactFiles}) ->
            [SortedFile] = dict:fetch(ViewId, SortFiles),
            {ok, CleanupAcc2, Bt2, Inserted, Deleted} =
                couch_set_view_updater_helper:update_btree(
                    Bt, SortedFile, MaxBatchSize, CleanupFun, CleanupAcc),
            StatsAcc2 = case ViewId of
            ids_index ->
                StatsAcc#set_view_updater_stats{
                    inserted_ids = StatsAcc#set_view_updater_stats.inserted_ids + Inserted,
                    deleted_ids = StatsAcc#set_view_updater_stats.deleted_ids + Deleted
                };
            _ ->
                StatsAcc#set_view_updater_stats{
                    inserted_kvs = StatsAcc#set_view_updater_stats.inserted_kvs + Inserted,
                    deleted_kvs = StatsAcc#set_view_updater_stats.deleted_kvs + Deleted
                }
            end,
            case CompactorRunning of
            true ->
                SortedFile2 = merge_files([SortedFile], Bt, TmpDir, true),
                AccCompactFiles2 = [SortedFile2 | AccCompactFiles];
            false ->
                ok = file2:delete(SortedFile),
                AccCompactFiles2 = AccCompactFiles
            end,
            {Bt2, {CleanupAcc2, StatsAcc2, AccCompactFiles2}}
        end,
        {CleanupAcc0, Stats, []}, ViewInfos),
    ok = couch_set_view_util:close_raw_read_fd(Group),
    {ok, NewBtrees, CleanupCount, NewStats, lists:reverse(CompactFiles)}.


merge_files([F], _Bt, _TmpDir, false) ->
    F;
merge_files([F], _Bt, TmpDir, true) ->
    case filename:extension(F) of
    ".compact" ->
        F;
    _ ->
        NewName = new_sort_file_name(TmpDir, true),
        ok = file2:rename(F, NewName),
        NewName
    end;
merge_files(Files, #btree{less = Less}, TmpDir, CompactorRunning) ->
    NewFile = new_sort_file_name(TmpDir, CompactorRunning),
    SortOptions = [
        {order, couch_set_view_updater_helper:batch_sort_fun(Less)},
        {tmpdir, TmpDir},
        {format, binary}
    ],
    case file_sorter_2:merge(Files, NewFile, SortOptions) of
    ok ->
        ok;
    {error, Reason} ->
        exit({batch_sort_failed, Reason})
    end,
    ok = lists:foreach(fun(F) -> ok = file2:delete(F) end, Files),
    NewFile.


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


maybe_checkpoint(WriterAcc) ->
    Before = get(last_header_commit_ts),
    Now = os:timestamp(),
    case (Before == undefined) orelse
        (timer:now_diff(Now, Before) >= ?CHECKPOINT_WRITE_INTERVAL) of
    true ->
        NewWriterAcc = checkpoint(WriterAcc),
        put(last_header_commit_ts, Now),
        NewWriterAcc;
    false ->
        NewWriterAcc = maybe_fix_group(WriterAcc),
        #writer_acc{owner = Owner, parent = Parent, group = Group} = NewWriterAcc,
        Owner ! {partial_update, Parent, Group},
        NewWriterAcc
    end.


checkpoint(#writer_acc{owner = Owner, parent = Parent, group = Group} = Acc) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = Type
    } = Group,
    ?LOG_INFO("Updater checkpointing set view `~s` update for ~s group `~s`",
              [SetName, Type, DDocId]),
    NewGroup = maybe_fix_group(Group),
    Owner ! {partial_update, Parent, NewGroup},
    Acc#writer_acc{group = NewGroup}.


maybe_fix_group(#writer_acc{group = Group} = Acc) ->
    NewGroup = maybe_fix_group(Group),
    Acc#writer_acc{group = NewGroup};
maybe_fix_group(#set_view_group{index_header = Header} = Group) ->
    receive
    {new_passive_partitions, Parts} ->
        Bitmask = couch_set_view_util:build_bitmask(Parts),
        Seqs2 = lists:foldl(
            fun(PartId, Acc) ->
                case couch_set_view_util:has_part_seq(PartId, Acc) of
                true ->
                    Acc;
                false ->
                    ordsets:add_element({PartId, 0}, Acc)
                end
            end,
            ?set_seqs(Group), Parts),
        Group#set_view_group{
            index_header = Header#set_view_index_header{
                seqs = Seqs2,
                pbitmask = ?set_pbitmask(Group) bor Bitmask
            }
        }
    after 0 ->
        Group
    end.


check_if_compactor_started(Acc) ->
    receive
    {compactor_started, Pid} ->
        Pid ! {compactor_started_ack, self(), Acc#writer_acc.group},
        Acc#writer_acc{compactor_running = true}
    after 0 ->
        Acc
    end.


init_view_merge_params(#writer_acc{group = Group, initial_build = false} = WriterAcc) ->
    SortFiles = [{View#set_view.id_num, []} || View <- Group#set_view_group.views],
    WriterAcc#writer_acc{
        sort_files = dict:from_list([{ids_index, []} | SortFiles])
    };
init_view_merge_params(#writer_acc{group = Group} = WriterAcc) ->
    SortFiles = [{View#set_view.id_num, []} || View <- Group#set_view_group.views],
    Buffers = [{View#set_view.id_num, {[], 0}} || View <- Group#set_view_group.views],
    WriterAcc#writer_acc{
        merge_buffers = dict:from_list([{ids_index, {[], 0}} | Buffers]),
        sort_files = dict:from_list([{ids_index, []} | SortFiles])
    }.


new_sort_file_name(#writer_acc{tmp_dir = TmpDir, compactor_running = Cr}) ->
    new_sort_file_name(TmpDir, Cr).

new_sort_file_name(TmpDir, true) ->
    couch_set_view_util:new_sort_file_path(TmpDir, compactor);
new_sort_file_name(TmpDir, false) ->
    couch_set_view_util:new_sort_file_path(TmpDir, updater).


delete_prev_sort_files(#writer_acc{tmp_dir = TmpDir}) ->
    ok = couch_set_view_util:delete_sort_files(TmpDir, updater).


wait_for_workers(Pids) ->
    ok = lists:foldr(fun(W, ok) ->
        Ref = erlang:monitor(process, W),
        receive
        {'DOWN', Ref, process, W, {sort_worker_died, _} = Reason} ->
            exit(Reason);
        {'DOWN', Ref, process, W, _Reason} ->
            ok
        end
    end, ok, Pids).


convert_primary_index_kvs_to_binary([], _Group, Acc) ->
    lists:reverse(Acc);
convert_primary_index_kvs_to_binary([{{Key, DocId}, {PartId, V0}} | Rest], Group, Acc)->
    V = case V0 of
    {dups, Values} ->
        ValueListBinary = lists:foldl(
            fun(V, Acc2) ->
                <<Acc2/binary, (byte_size(V)):24, V/binary>>
            end,
            <<>>, Values),
        <<PartId:16, ValueListBinary/binary>>;
    _ ->
        <<PartId:16, (byte_size(V0)):24, V0/binary>>
    end,
    KeyBin = couch_set_view_util:encode_key_docid(Key, DocId),
    case byte_size(KeyBin) > ?MAX_KEY_SIZE of
    true ->
        #set_view_group{set_name = SetName, name = DDocId, type = Type} = Group,
        KeyPrefix = lists:sublist(unicode:characters_to_list(Key), 100),
        Error = iolist_to_binary(
            io_lib:format("key emitted for document `~s` is too long: ~s...", [DocId, KeyPrefix])),
        ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, ~s", [SetName, Type, DDocId, Error]),
        throw({error, Error});
    false ->
        convert_primary_index_kvs_to_binary(Rest, Group, [{KeyBin, V} | Acc])
    end.


convert_back_index_kvs_to_binary([], Acc)->
    lists:reverse(Acc);
convert_back_index_kvs_to_binary([{DocId, {PartId, ViewIdKeys}} | Rest], Acc) ->
    ViewIdKeysBinary = lists:foldl(
        fun({ViewId, Keys}, Acc2) ->
            KeyListBinary = lists:foldl(
                fun(Key, AccKeys) ->
                    <<AccKeys/binary, (byte_size(Key)):16, Key/binary>>
                end,
                <<>>, Keys),
            <<Acc2/binary, ViewId:8, (length(Keys)):16, KeyListBinary/binary>>
        end,
        <<>>, ViewIdKeys),
    KvBin = {make_back_index_key(DocId, PartId), <<PartId:16, ViewIdKeysBinary/binary>>},
    convert_back_index_kvs_to_binary(Rest, [KvBin | Acc]).


make_back_index_key(DocId, PartId) ->
    <<PartId:16, DocId/binary>>.


% initial build
file_sorter_initial_build_format_fun(<<KeyLen:16, Key:KeyLen/binary, Value/binary>>) ->
    {Key, Value}.


split_files_to_merge(SortFiles, true) ->
    {LastGen, _} = lists:last(SortFiles),
    {[F || {_, F} <- SortFiles], LastGen + 1, []};

split_files_to_merge([{MinGen, FirstFile} | Rest], false) ->
    {ChosenGen, ToMerge, NotToMerge} =
        split_files_by_gen(MinGen, Rest, ordsets:new(), [FirstFile]),
    {ToMerge, ChosenGen + length(ToMerge), NotToMerge}.


split_files_by_gen(Gen, [{Gen, F} | Rest], AccNot, Acc) ->
    split_files_by_gen(Gen, Rest, AccNot, [F | Acc]);
split_files_by_gen(Gen, [], AccNot, Acc) ->
    {Gen, Acc, AccNot};
split_files_by_gen(Gen, [{Gen2, F} | Rest], AccNot, [FAcc]) when Gen2 > Gen ->
    split_files_by_gen(Gen2, Rest, ordsets:add_element({Gen, FAcc}, AccNot), [F]);
split_files_by_gen(Gen, [{Gen2, _} | _] = Rest, AccNot, Acc) when Gen2 > Gen ->
    {Gen, Acc, ordsets:union(AccNot, Rest)}.


spawn_merge_worker(LessFun, TmpDir, Workers, FilesToMerge, DestFile) ->
    spawn_link(fun() ->
        SortOptions = [
            {order, LessFun},
            {tmpdir, TmpDir},
            {format, fun file_sorter_initial_build_format_fun/1}
        ],
        wait_for_workers(Workers),
        case file_sorter_2:merge(FilesToMerge, DestFile, SortOptions) of
        ok ->
            ok;
        {error, Reason} ->
            exit({sort_worker_died, Reason})
        end,
        lists:foreach(fun(F) ->
            case file2:delete(F) of
            ok ->
                ok;
            {error, Reason2} ->
                exit({sort_worker_died, Reason2})
            end
        end, FilesToMerge)
    end).


count_seqs_done(Group, NewSeqs) ->
    % NewSeqs might have new passive partitions that Group's seqs doesn't
    % have yet (will get them after a checkpoint period).
    lists:foldl(
        fun({PartId, SeqDone}, Acc) ->
            SeqBefore = couch_util:get_value(PartId, ?set_seqs(Group), 0),
            Acc + (SeqDone - SeqBefore)
        end,
        0, NewSeqs).
