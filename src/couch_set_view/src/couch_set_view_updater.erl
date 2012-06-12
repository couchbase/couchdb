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

-export([update/4]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-define(QUEUE_MAX_ITEMS, 5000).
-define(QUEUE_MAX_SIZE, 500 * 1024).
-define(MIN_WRITER_NUM_ITEMS, 1000).
-define(MIN_WRITER_BATCH_SIZE, 100 * 1024).
-define(CHECKPOINT_WRITE_INTERVAL, 5000000).
-define(replace(L, K, V), lists:keystore(K, 1, L, {K, V})).

-record(writer_acc, {
    parent,
    owner,
    group,
    log_fd = nil,
    write_queue,
    initial_build,
    view_empty_kvs,
    kvs = [],
    kvs_size = 0,
    state = updating_active,
    final_batch = false,
    max_seqs,
    replicas_transferred = [],
    cleanup_kv_count = 0,
    cleanup_time = 0,
    inserted_kvs = 0,
    deleted_kvs = 0,
    inserted_ids = 0,
    deleted_ids = 0
}).


-spec update(pid(), #set_view_group{}, partition_seqs(), string() | 'nil') -> no_return().
update(Owner, Group, CurSeqs, LogFilePath) ->
    #set_view_group{
        set_name = SetName,
        type = Type,
        name = DDocId
    } = Group,
    ActiveParts = couch_set_view_util:decode_bitmask(?set_abitmask(Group)),
    PassiveParts = couch_set_view_util:decode_bitmask(?set_pbitmask(Group)),
    SinceSeqs = ?set_seqs(Group),
    NumChanges = lists:foldl(
        fun({{PartId, NewSeq}, {PartId, OldSeq}}, Acc) when NewSeq >= OldSeq ->
            Acc + (NewSeq - OldSeq)
        end,
        0, lists:zip(CurSeqs, SinceSeqs)),

    case ?set_pending_transition(Group) of
    nil ->
        PendingActive = [],
        PendingPassive = [];
    #set_view_transition{active = PendingActive, passive = PendingPassive} ->
        ok
    end,

    process_flag(trap_exit, true),

    BeforeEnterTs = os:timestamp(),
    Parent = self(),
    Pid = spawn_link(fun() ->
        case Type of
        main ->
            ok = couch_index_barrier:enter(couch_main_index_barrier, Parent);
        replica ->
            ok = couch_index_barrier:enter(couch_replica_index_barrier, Parent)
        end,
        exit({done, (timer:now_diff(os:timestamp(), BeforeEnterTs) / 1000000)})
    end),

    BlockedTime = receive
    {'EXIT', Pid, {done, Duration}} ->
        Duration;
    {'EXIT', _, Reason} ->
        exit({updater_error, Reason});
    stop_immediately ->
        EmptyResult = #set_view_updater_result{
            group = Group,
            indexing_time = 0.0,
            blocked_time = timer:now_diff(os:timestamp(), BeforeEnterTs) / 1000000,
            state = updating_active,
            cleanup_kv_count = 0,
            cleanup_time = 0.0,
            inserted_ids = 0,
            deleted_ids = 0,
            inserted_kvs = 0,
            deleted_kvs = 0
        },
        exit({updater_finished, EmptyResult})
    end,

    CleanupParts = couch_set_view_util:decode_bitmask(?set_cbitmask(Group)),
    ?LOG_INFO("Updater for set view `~s`, ~s group `~s` started~n"
              "Active partitions:    ~w~n"
              "Passive partitions:   ~w~n"
              "Cleanup partitions:   ~w~n"
              "Replicas to transfer: ~w~n"
              "Pending transition:   ~n"
              "    active:           ~w~n"
              "    passive:          ~w~n",
              [SetName, Type, DDocId,
               ActiveParts,
               PassiveParts,
               CleanupParts,
               ?set_replicas_on_transfer(Group),
               PendingActive,
               PendingPassive
              ]),

    WriterAcc0 = #writer_acc{
        parent = self(),
        owner = Owner,
        group = Group,
        max_seqs = CurSeqs
    },
    update(WriterAcc0, ActiveParts, PassiveParts, BlockedTime, NumChanges, LogFilePath).


update(WriterAcc, ActiveParts, PassiveParts, BlockedTime, NumChanges, LogFilePath) ->
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

    {ok, MapQueue} = couch_work_queue:new(
        [{max_size, ?QUEUE_MAX_SIZE}, {max_items, ?QUEUE_MAX_ITEMS}]),
    {ok, WriteQueue} = couch_work_queue:new(
        [{max_size, ?QUEUE_MAX_SIZE}, {max_items, ?QUEUE_MAX_ITEMS}]),

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
        ok = couch_set_view_util:open_raw_read_fd(Group),

        DDocIds = couch_set_view_util:get_ddoc_ids_with_sig(SetName, GroupSig),
        couch_task_status:add_task([
            {type, indexer},
            {set, SetName},
            {design_documents, DDocIds},
            {indexer_type, Type},
            {progress, 0},
            {changes_done, 0},
            {total_changes, NumChanges}
        ]),
        couch_task_status:set_update_frequency(1000),

        InitialBuild = lists:all(fun({_, Seq}) -> Seq == 0 end, ?set_seqs(Group)) andalso
                lists:all(fun({_, Seq}) -> Seq == 0 end, ?set_unindexable_seqs(Group)),
        ViewEmptyKVs = [{View, []} || View <- Group#set_view_group.views],
        WriterAcc2 = WriterAcc#writer_acc{
            parent = Parent,
            group = Group,
            write_queue = WriteQueue,
            initial_build = InitialBuild,
            view_empty_kvs = ViewEmptyKVs,
            log_fd = open_log_file(LogFilePath)
        },
        try
            couch_set_view_mapreduce:start_reduce_context(Group),
            try
                FinalWriterAcc = do_writes(WriterAcc2),
                case FinalWriterAcc#writer_acc.log_fd of
                nil ->
                    ok;
                _ ->
                    ok = file:close(FinalWriterAcc#writer_acc.log_fd)
                end,
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
        try
            load_changes(Owner, Parent, Group, MapQueue, Writer, ActiveParts, PassiveParts)
        catch _:Error ->
            Stacktrace = erlang:get_stacktrace(),
            ?LOG_ERROR("Set view `~s`, ~s group `~s`, doc loader error~n"
                "error:      ~p~n"
                "stacktrace: ~p~n",
                [SetName, Type, DDocId, Error, Stacktrace]),
            exit(Error)
        end
    end),

    Result = wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime),
    case Type of
    main ->
        ok = couch_index_barrier:leave(couch_main_index_barrier);
    replica ->
        ok = couch_index_barrier:leave(couch_replica_index_barrier)
    end,
    exit(Result).


wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime) ->
    receive
    {writer_finished, WriterAcc} ->
        Result = #set_view_updater_result{
            group = WriterAcc#writer_acc.group,
            indexing_time = timer:now_diff(os:timestamp(), StartTime) / 1000000,
            blocked_time = BlockedTime,
            state = WriterAcc#writer_acc.state,
            cleanup_kv_count = WriterAcc#writer_acc.cleanup_kv_count,
            cleanup_time = WriterAcc#writer_acc.cleanup_time,
            inserted_ids = WriterAcc#writer_acc.inserted_ids,
            deleted_ids = WriterAcc#writer_acc.deleted_ids,
            inserted_kvs = WriterAcc#writer_acc.inserted_kvs,
            deleted_kvs = WriterAcc#writer_acc.deleted_kvs
        },
        {updater_finished, Result};
    stop_immediately ->
        DocLoader ! stop_immediately,
        wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime);
    {log_new_changes, Pid, Ref, LogFilePath} ->
        Writer ! {log_new_changes, self(), LogFilePath},
        erlang:put(log_request, {Pid, Ref}),
        wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime);
    {log_started, Writer, GroupSnapshot} ->
        {Pid, Ref} = erlang:erase(log_request),
        Pid ! {Ref, {ok, GroupSnapshot}},
        wait_result_loop(StartTime, DocLoader, Mapper, Writer, BlockedTime);
    {'EXIT', _, Reason} when Reason =/= normal ->
        couch_util:shutdown_sync(DocLoader),
        couch_util:shutdown_sync(Mapper),
        couch_util:shutdown_sync(Writer),
        {updater_error, Reason}
    end.


load_changes(Owner, Updater, Group, MapQueue, Writer, ActiveParts, PassiveParts) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = GroupType,
        index_header = #set_view_index_header{seqs = SinceSeqs}
    } = Group,

    FoldFun = fun(PartId, Acc) ->
        case orddict:is_key(PartId, ?set_unindexable_seqs(Group)) of
        true ->
            Acc;
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
                maybe_stop(),
                Since = couch_util:get_value(PartId, SinceSeqs),
                ChangesWrapper = fun(DocInfo, _, Acc2) ->
                    maybe_stop(),
                    load_doc(Db, PartId, DocInfo, MapQueue),
                    maybe_stop(),
                    {ok, Acc2 + 1}
                end,
                {ok, _, Acc3} = couch_db:fast_reads(Db, fun() ->
                    couch_db:enum_docs_since(Db, Since, ChangesWrapper, Acc, [])
                end),
                Acc3
            after
                ok = couch_db:close(Db)
            end
        end
    end,

    notify_owner(Owner, {state, updating_active}, Updater),
    try
        case ActiveParts of
        [] ->
            ActiveChangesCount = 0;
        _ ->
            ?LOG_INFO("Updater reading changes from active partitions to "
                      "update ~s set view group `~s` from set `~s`",
                      [GroupType, DDocId, SetName]),
            ActiveChangesCount = lists:foldl(FoldFun, 0, ActiveParts)
        end,
        case PassiveParts of
        [] ->
            FinalChangesCount = ActiveChangesCount;
        _ ->
            ?LOG_INFO("Updater reading changes from passive partitions to "
                      "update ~s set view group `~s` from set `~s`",
                      [GroupType, DDocId, SetName]),
            FinalChangesCount = lists:foldl(FoldFun, ActiveChangesCount, PassiveParts)
        end,
        ?LOG_INFO("Updater for ~s set view group `~s`, set `~s`, read a total of ~p changes",
                  [GroupType, DDocId, SetName, FinalChangesCount])
    catch throw:stop ->
        Writer ! stop
    end,
    couch_work_queue:close(MapQueue).


maybe_stop() ->
    receive
    stop_immediately ->
        throw(stop)
    after 0 ->
        ok
    end.

notify_owner(Owner, Msg, UpdaterPid) ->
    Owner ! {updater_info, UpdaterPid, Msg}.


load_doc(Db, PartitionId, DocInfo, MapQueue) ->
    #doc_info{id=DocId, local_seq=Seq, deleted=Deleted} = DocInfo,
    case DocId of
    <<?DESIGN_DOC_PREFIX, _/binary>> ->
        ok;
    _ ->
        if Deleted ->
            couch_work_queue:queue(MapQueue, {Seq, #doc{id=DocId, deleted=true}, PartitionId});
        true ->
            {ok, Doc} = couch_db:open_doc_int(Db, DocInfo, []),
            couch_work_queue:queue(MapQueue, {Seq, Doc, PartitionId})
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
                    ?LOG_ERROR("Set view `~s`, ~s group `~s`, error mapping "
                               "document `~s`: ~s~n",
                               [SetName, Type, DDocId, Id, couch_util:to_binary(Reason)]),
                    Acc
                end
            end,
            [], Queue),
        ok = couch_work_queue:queue(WriteQueue, Items),
        do_maps(Group, MapQueue, WriteQueue)
    end.


do_writes(#writer_acc{kvs = Kvs, kvs_size = KvsSize, write_queue = WriteQueue} = Acc) ->
    Acc2 = maybe_open_log_file(Acc),
    case couch_work_queue:dequeue(WriteQueue) of
    closed ->
        FinalAcc = flush_writes(Acc2#writer_acc{final_batch = true}),
        write_header(FinalAcc#writer_acc.group, false),
        FinalAcc;
    {ok, Queue, QueueSize} ->
        Acc3 = maybe_open_log_file(Acc2),
        Kvs2 = Kvs ++ lists:flatten(Queue),
        KvsSize2 = KvsSize + QueueSize,
        case (KvsSize2 >= ?MIN_WRITER_BATCH_SIZE) orelse (length(Kvs2) >= ?MIN_WRITER_NUM_ITEMS) of
        true ->
            Acc4 = flush_writes(Acc3#writer_acc{kvs = Kvs2, kvs_size = KvsSize2}),
            Acc5 = Acc4#writer_acc{kvs = [], kvs_size = 0};
        false ->
            Acc5 = Acc3#writer_acc{kvs = Kvs2, kvs_size = KvsSize2}
        end,
        do_writes(Acc5)
    end.


flush_writes(#writer_acc{kvs = []} = Acc) ->
    {Acc2, ReplicasTransferred} = update_transferred_replicas(Acc, []),
    case ReplicasTransferred of
    true ->
        checkpoint(Acc2, true);
    false ->
        ok
    end,
    Acc2;
flush_writes(Acc) ->
    #writer_acc{
        kvs = Queue,
        view_empty_kvs = ViewEmptyKVs,
        group = Group,
        parent = Parent,
        owner = Owner
    } = Acc,
    {ViewKVs, DocIdViewIdKeys, PartIdSeqs} = lists:foldl(
        fun({Seq, DocId, PartId, []}, {ViewKVsAcc, DocIdViewIdKeysAcc, PartIdSeqs}) ->
            PartIdSeqs2 = update_part_seq(Seq, PartId, PartIdSeqs),
            {ViewKVsAcc, [{DocId, {PartId, []}} | DocIdViewIdKeysAcc], PartIdSeqs2};
        ({Seq, DocId, PartId, QueryResults}, {ViewKVsAcc, DocIdViewIdKeysAcc, PartIdSeqs}) ->
            {NewViewKVs, NewViewIdKeys} = view_insert_doc_query_results(
                    DocId, PartId, QueryResults, ViewKVsAcc, [], []),
            PartIdSeqs2 = update_part_seq(Seq, PartId, PartIdSeqs),
            {NewViewKVs, [{DocId, {PartId, NewViewIdKeys}} | DocIdViewIdKeysAcc], PartIdSeqs2}
        end,
        {ViewEmptyKVs, [], orddict:new()}, Queue),
    Acc2 = write_changes(Acc, ViewKVs, DocIdViewIdKeys, PartIdSeqs),
    {Acc3, ReplicasTransferred} = update_transferred_replicas(Acc2, PartIdSeqs),
    update_task(length(Queue)),
    case (Acc3#writer_acc.state =:= updating_active) andalso
        lists:any(fun({PartId, _}) ->
            ((1 bsl PartId) band ?set_pbitmask(Group) =/= 0)
        end, PartIdSeqs) of
    true ->
        checkpoint(Acc3, false),
        notify_owner(Owner, {state, updating_passive}, Parent),
        Acc3#writer_acc{state = updating_passive};
    false when ReplicasTransferred ->
        checkpoint(Acc3, true),
        Acc3;
    false ->
        maybe_checkpoint(Acc3),
        Acc3
    end.


update_transferred_replicas(#writer_acc{group = Group} = Acc, _PartIdSeqs) when ?set_replicas_on_transfer(Group) =:= [] ->
    {Acc, false};
update_transferred_replicas(Acc, PartIdSeqs) ->
    #writer_acc{
        group = #set_view_group{index_header = Header} = Group,
        max_seqs = MaxSeqs,
        replicas_transferred = RepsTransferred,
        final_batch = FinalBatch
    } = Acc,
    RepsTransferred2 = lists:foldl(
        fun({PartId, Seq}, A) ->
            case ordsets:is_element(PartId, ?set_replicas_on_transfer(Group))
                andalso (Seq >= orddict:fetch(PartId, MaxSeqs)) of
            true ->
                ordsets:add_element(PartId, A);
            false ->
                A
            end
        end,
        RepsTransferred, PartIdSeqs),
    % Only update the group's list of replicas on transfer when the updater is finishing
    % or when all the replicas were transferred (indexed). This is to make the cleanup
    % of the replica index much more efficient (less partition state transitions, less
    % cleanup process interruptions/restarts).
    ReplicasOnTransfer = ordsets:subtract(?set_replicas_on_transfer(Group), RepsTransferred2),
    case FinalBatch orelse (ReplicasOnTransfer =:= []) of
    false ->
        {Acc#writer_acc{replicas_transferred = RepsTransferred2}, false};
    true ->
        {Abitmask2, Pbitmask2} = lists:foldl(
            fun(Id, {A, P}) ->
                Mask = 1 bsl Id,
                Mask = ?set_pbitmask(Group) band Mask,
                0 = ?set_abitmask(Group) band Mask,
                {A bor Mask, P bxor Mask}
            end,
            {?set_abitmask(Group), ?set_pbitmask(Group)},
            RepsTransferred2),
        Group2 = Group#set_view_group{
            index_header = Header#set_view_index_header{
                abitmask = Abitmask2,
                pbitmask = Pbitmask2,
                replicas_on_transfer = ReplicasOnTransfer
            }
        },
        {Acc#writer_acc{group = Group2}, ReplicasOnTransfer /= []}
    end.


update_part_seq(Seq, PartId, Acc) ->
    case orddict:find(PartId, Acc) of
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
            {AccKv2, [{View#set_view.id_num, Key} | AccVid]};
        ({Key, Val}, {AccKv, AccVid}) ->
            {[{{Key, DocId}, {PartitionId, Val}} | AccKv], [{View#set_view.id_num, Key} | AccVid]}
        end,
        {KVs, ViewIdKeysAcc}, lists:sort(ResultKVs)),
    NewViewKVsAcc = [{View, NewKVs} | ViewKVsAcc],
    view_insert_doc_query_results(
        DocId, PartitionId, RestResults, RestViewKVs, NewViewKVsAcc, NewViewIdKeysAcc).


write_changes(WriterAcc, ViewKeyValuesToAdd, DocIdViewIdKeys, PartIdSeqs) ->
    #writer_acc{
        log_fd = LogFd,
        owner = Owner,
        group = Group,
        initial_build = InitialBuild,
        cleanup_kv_count = CleanupKvCount0,
        cleanup_time = CleanupTime0
    } = WriterAcc,
    #set_view_group{
        id_btree = IdBtree,
        fd = Fd,
        set_name = SetName,
        name = GroupName,
        type = GroupType
    } = Group,

    AddDocIdViewIdKeys = [{DocId, {PartId, ViewIdKeys}} || {DocId, {PartId, ViewIdKeys}} <- DocIdViewIdKeys, ViewIdKeys /= []],
    if InitialBuild ->
        RemoveDocIds = [],
        LookupDocIds = [];
    true ->
        RemoveDocIds = [DocId || {DocId, {_PartId, ViewIdKeys}} <- DocIdViewIdKeys, ViewIdKeys == []],
        LookupDocIds = [DocId || {DocId, _ViewIdKeys} <- DocIdViewIdKeys]
    end,
    CleanupFun = case ?set_cbitmask(Group) of
    0 ->
        nil;
    _ ->
        couch_set_view_util:make_btree_purge_fun(Group)
    end,
    case ?set_cbitmask(Group) of
    0 ->
        IdBtreePurgedKeyCount = 0,
        CleanupStart = 0,
        {ok, LookupResults, IdBtree2} =
            couch_btree:query_modify(IdBtree, LookupDocIds, AddDocIdViewIdKeys, RemoveDocIds);
    _ ->
        CleanupStart = os:timestamp(),
        {ok, LookupResults, {Go, IdBtreePurgedKeyCount}, IdBtree2} =
            couch_btree:query_modify(
                IdBtree, LookupDocIds, AddDocIdViewIdKeys, RemoveDocIds, CleanupFun, {go, 0}),
        case Go of
        stop ->
            self() ! stop;
        go ->
            ok
        end
    end,
    KeysToRemoveByView = lists:foldl(
        fun(LookupResult, KeysToRemoveByViewAcc) ->
            case LookupResult of
            {ok, {DocId, {_Part, ViewIdKeys}}} ->
                lists:foldl(
                    fun({ViewId, Key}, KeysToRemoveByViewAcc2) ->
                        dict:append(ViewId, {Key, DocId}, KeysToRemoveByViewAcc2)
                    end,
                    KeysToRemoveByViewAcc, ViewIdKeys);
            {not_found, _} ->
                KeysToRemoveByViewAcc
            end
        end,
        dict:new(), LookupResults),
    {Views2, {CleanupKvCount, InsertedKvCount, DeletedKvCount}} =
        lists:mapfoldl(fun({View, {_View, AddKeyValues}}, {AccC, AccI, AccD}) ->
            KeysToRemove = couch_util:dict_find(View#set_view.id_num, KeysToRemoveByView, []),
            case ?set_cbitmask(Group) of
            0 ->
                CleanupCount = 0,
                {ok, ViewBtree2} = couch_btree:add_remove(
                    View#set_view.btree, AddKeyValues, KeysToRemove);
            _ ->
                {ok, {Go2, CleanupCount}, ViewBtree2} = couch_btree:add_remove(
                    View#set_view.btree, AddKeyValues, KeysToRemove, CleanupFun, {go, 0}),
                case Go2 of
                stop ->
                    self() ! stop;
                go ->
                    ok
                end
            end,
            NewView = View#set_view{btree = ViewBtree2},
            {NewView, {AccC + CleanupCount, AccI + length(AddKeyValues), AccD + length(KeysToRemove)}}
        end,
        {IdBtreePurgedKeyCount, 0, 0}, lists:zip(Group#set_view_group.views, ViewKeyValuesToAdd)),

    case ?set_cbitmask(Group) of
    0 ->
        NewCbitmask = 0,
        CleanupTime = 0;
    _ ->
        {ok, {_, IdBitmap}} = couch_btree:full_reduce(IdBtree2),
        CombinedBitmap = lists:foldl(
            fun(#set_view{btree = Bt}, AccMap) ->
                {ok, {_, _, Bm}} = couch_btree:full_reduce(Bt),
                AccMap bor Bm
            end,
            IdBitmap, Views2),
        NewCbitmask = ?set_cbitmask(Group) band CombinedBitmap,
        CleanupTime = timer:now_diff(os:timestamp(), CleanupStart) / 1000000,
        ?LOG_INFO("Updater for set view `~s`, ~s group `~s`, performed cleanup "
            "of ~p key/value pairs in ~.3f seconds",
            [SetName, GroupType, GroupName, CleanupKvCount, CleanupTime])
    end,
    NewSeqs = update_seqs(PartIdSeqs, ?set_seqs(Group)),
    case LogFd of
    nil ->
        ok;
    _ ->
        LogViewsAddRemoveKvs = lists:map(
            fun({#set_view{id_num = ViewId}, AddKeyValues}) ->
                KeysToRemove = couch_util:dict_find(ViewId, KeysToRemoveByView, []),
                {AddKeyValues, KeysToRemove}
            end,
            ViewKeyValuesToAdd),
        LogEntry = {NewSeqs, AddDocIdViewIdKeys, RemoveDocIds, LogViewsAddRemoveKvs},
        LogEntryBin = couch_compress:compress(?term_to_bin(LogEntry)),
        ok = file:write(LogFd, [<<(byte_size(LogEntryBin)):32>>, LogEntryBin]),
        {ok, LogEof} = file:position(LogFd, eof),
        ok = gen_server:cast(Owner, {log_eof, LogEof})
    end,
    Header = Group#set_view_group.index_header,
    NewHeader = Header#set_view_index_header{
        id_btree_state = couch_btree:get_state(IdBtree2),
        view_states = [couch_btree:get_state(V#set_view.btree) || V <- Views2],
        seqs = NewSeqs,
        cbitmask = NewCbitmask
    },
    NewGroup = Group#set_view_group{
        views = Views2,
        id_btree = IdBtree2,
        index_header = NewHeader
    },
    couch_file:flush(Fd),
    WriterAcc#writer_acc{
        group = NewGroup,
        cleanup_kv_count = CleanupKvCount0 + CleanupKvCount,
        cleanup_time = CleanupTime0 + CleanupTime,
        inserted_kvs = WriterAcc#writer_acc.inserted_kvs + InsertedKvCount,
        deleted_kvs = WriterAcc#writer_acc.deleted_kvs + DeletedKvCount,
        inserted_ids = WriterAcc#writer_acc.inserted_ids + length(AddDocIdViewIdKeys),
        deleted_ids = WriterAcc#writer_acc.deleted_ids + length(RemoveDocIds)
    }.


update_seqs(PartIdSeqs, Seqs) ->
    orddict:fold(
        fun(PartId, NewSeq, Acc) ->
            OldSeq = couch_util:get_value(PartId, Acc),
            case is_integer(OldSeq) of
            true ->
                ok;
            false ->
                exit({error, <<"Old seq is not an integer.">>, PartId, OldSeq, NewSeq})
            end,
            case NewSeq > OldSeq of
            true ->
                ok;
            false ->
                exit({error, <<"New seq smaller or equal than old seq.">>, PartId, OldSeq, NewSeq})
            end,
            ?replace(Acc, PartId, NewSeq)
        end,
        Seqs, PartIdSeqs).


update_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress = (Changes2 * 100) div Total,
    couch_task_status:update([{progress, Progress}, {changes_done, Changes2}]).


maybe_checkpoint(WriterAcc) ->
    Before = get(last_header_commit_ts),
    Now = os:timestamp(),
    case (Before == undefined) orelse
        (timer:now_diff(Now, Before) >= ?CHECKPOINT_WRITE_INTERVAL) of
    true ->
        checkpoint(WriterAcc, false),
        put(last_header_commit_ts, Now);
    false ->
        ok
    end.


checkpoint(#writer_acc{owner = Owner, parent = Parent, group = Group}, DoFsync) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = Type
    } = Group,
    ?LOG_INFO("Updater checkpointing set view `~s` update for ~s group `~s`",
              [SetName, Type, DDocId]),
    write_header(Group, DoFsync),
    ok = gen_server:cast(Owner, {partial_update, Parent, Group}).


write_header(#set_view_group{fd = Fd} = Group, DoFsync) ->
    DiskHeader = couch_set_view_util:make_disk_header(Group),
    ok = couch_file:write_header(Fd, DiskHeader),
    case DoFsync of
    true ->
        ok = couch_file:sync(Fd);
    false ->
        ok
    end.


open_log_file(nil) ->
    nil;
open_log_file(Path) when is_list(Path) ->
    {ok, LogFd} = file:open(Path, [raw, binary, append]),
    LogFd.


maybe_open_log_file(Acc) ->
    receive
    {log_new_changes, Pid, LogFilePath} ->
        case Acc#writer_acc.log_fd of
        nil ->
            ok;
        OldLogFd ->
            % Compactor died and just restarted, close the current
            % log and open a new one.
            file:close(OldLogFd)
        end,
        LogFd = open_log_file(LogFilePath),
        Pid ! {log_started, self(), Acc#writer_acc.group},
        Acc#writer_acc{log_fd = LogFd}
    after 0 ->
        Acc
    end.
