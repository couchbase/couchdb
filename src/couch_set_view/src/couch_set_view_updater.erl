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

-export([update/2]).

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


-spec update(pid() | 'nil', #set_view_group{}) -> no_return().
update(Owner, Group) ->
    #set_view_group{
        set_name = SetName,
        type = Type,
        name = DDocId
    } = Group,
    ActiveParts = ordsets:from_list(
        couch_set_view_util:decode_bitmask(?set_abitmask(Group))),
    PassiveParts = ordsets:from_list(
        couch_set_view_util:decode_bitmask(?set_pbitmask(Group))),

    FoldFun = fun(P, {A1, A2, A3}) ->
        {ok, Db} = couch_db:open_int(?dbname(SetName, P), []),
        {ok, <<NotDel:40, Del:40, _Size:48>>} =
                couch_btree:full_reduce(Db#db.docinfo_by_id_btree),
        Seq = couch_db:get_update_seq(Db),
        ok = couch_db:close(Db),
        {[{P, Del} | A1], [{P, NotDel} | A2], [{P, Seq} | A3]}
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

    {ActiveDelCounts, ActiveNotDelCounts, ActiveSeqs} =
        lists:foldl(FoldFun, {[], [], []}, ActiveParts),
    {PassiveDelCounts, PassiveNotDelCounts, PassiveSeqs} =
        lists:foldl(FoldFun, {[], [], []}, PassiveParts),

    MaxSeqs = dict:from_list(ActiveSeqs ++ PassiveSeqs),
    IndexedActiveSeqs =  [{P, S} || {P, S} <- ?set_seqs(Group), ordsets:is_element(P, ActiveParts)],
    IndexedPassiveSeqs = [{P, S} || {P, S} <- ?set_seqs(Group), ordsets:is_element(P, PassiveParts)],

    case is_pid(Owner) of
    true ->
        ?LOG_INFO("Updater for set view `~s`, ~s group `~s` started~n"
                  "Active partitions:                      ~w~n"
                  "Passive partitions:                     ~w~n"
                  "Active partitions update seqs:          ~w~n"
                  "Active partitions indexed update seqs:  ~w~n"
                  "Passive partitions update seqs:         ~w~n"
                  "Passive partitions indexed update seqs: ~w~n"
                  "Active partitions # docs:               ~w~n"
                  "Active partitions # deleted docs:       ~w~n"
                  "Passive partitions # docs:              ~w~n"
                  "Passive partitions # deleted docs:      ~w~n"
                  "Replicas to transfer:                   ~w~n",
                  [SetName, Type, DDocId,
                   ActiveParts,
                   PassiveParts,
                   lists:reverse(ActiveSeqs),
                   IndexedActiveSeqs,
                   lists:reverse(PassiveSeqs),
                   IndexedPassiveSeqs,
                   lists:reverse(ActiveNotDelCounts),
                   lists:reverse(ActiveDelCounts),
                   lists:reverse(PassiveNotDelCounts),
                   lists:reverse(PassiveDelCounts),
                   ?set_replicas_on_transfer(Group)
                  ]);
    false ->
        ok
    end,

    update(Owner, Group, ActiveParts, PassiveParts, MaxSeqs, BlockedTime).


update(Owner, Group, ActiveParts, PassiveParts, MaxSeqs, BlockedTime) ->
    #set_view_group{
        set_name = SetName,
        type = Type,
        name = DDocId,
        index_header = #set_view_index_header{seqs = SinceSeqs},
        sig = GroupSig
    } = Group,

    StartTime = os:timestamp(),
    NumChanges = lists:foldl(
        fun({{PartId, NewSeq}, {PartId, OldSeq}}, Acc) ->
             Acc + (NewSeq - OldSeq)
        end,
        0, lists:zip(lists:keysort(1, dict:to_list(MaxSeqs)), SinceSeqs)),

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

        Group2 = lists:foldl(
            fun({PartId, PurgeSeq}, GroupAcc) ->
                {ok, Db} = couch_db:open_int(?dbname(SetName, PartId), []),
                DbPurgeSeq = couch_db:get_purge_seq(Db),
                GroupAcc2 =
                if DbPurgeSeq == PurgeSeq + 1 ->
                    purge_index(GroupAcc, Db, PartId);
                true ->
                    GroupAcc
                end,
                couch_db:close(Db),
                GroupAcc2
            end,
            Group, ?set_purge_seqs(Group)),

        InitialBuild = (0 =:= lists:sum([S || {_, S} <- ?set_seqs(Group)])),
        ViewEmptyKVs = [{View, []} || View <- Group2#set_view_group.views],
        WriterAcc = #writer_acc{
            parent = Parent,
            owner = Owner,
            group = Group2,
            write_queue = WriteQueue,
            initial_build = InitialBuild,
            view_empty_kvs = ViewEmptyKVs,
            max_seqs = MaxSeqs
        },
        try
            couch_set_view_mapreduce:start_reduce_context(Group2),
            try
                FinalWriterAcc = do_writes(WriterAcc),
                Parent ! {writer_finished, FinalWriterAcc}
            after
                couch_set_view_mapreduce:end_reduce_context(Group2)
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

    FoldFun = fun(PartId, PartType) ->
        {ok, Db} = couch_db:open_int(?dbname(SetName, PartId), []),
        maybe_stop(),
        Since = couch_util:get_value(PartId, SinceSeqs),
        ?LOG_INFO("Reading changes (since sequence ~p) from ~s partition ~s to"
                  " update ~s set view group `~s` from set `~s`",
                  [Since, PartType, couch_db:name(Db), GroupType, DDocId, SetName]),
        ChangesWrapper = fun(DocInfo, _, ok) ->
            maybe_stop(),
            load_doc(Db, PartId, DocInfo, MapQueue),
            maybe_stop(),
            {ok, ok}
        end,
        {ok, _, ok} = couch_db:fast_reads(Db, fun() ->
            couch_db:enum_docs_since(Db, Since, ChangesWrapper, ok, [])
        end),
        ok = couch_db:close(Db),
        PartType
    end,

    notify_owner(Owner, {state, updating_active}, Updater),
    try
        active = lists:foldl(FoldFun, active, ActiveParts),
        passive = lists:foldl(FoldFun, passive, PassiveParts)
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

notify_owner(nil, _Msg, _UpdaterPid) ->
    ok;
notify_owner(Owner, Msg, UpdaterPid) when is_pid(Owner) ->
    Owner ! {updater_info, UpdaterPid, Msg}.


purge_index(#set_view_group{fd=Fd, views=Views, id_btree=IdBtree}=Group, Db, PartitionId) ->
    {ok, PurgedIdsRevs} = couch_db:get_last_purged(Db),
    Ids = [Id || {Id, _Revs} <- PurgedIdsRevs],
    {ok, Lookups, IdBtree2} = couch_btree:query_modify(IdBtree, Ids, [], Ids),

    % now populate the dictionary with all the keys to delete
    ViewKeysToRemoveDict = lists:foldl(
        fun({ok, {DocId, {_Part, ViewNumRowKeys}}}, ViewDictAcc) ->
            lists:foldl(
                fun({ViewNum, RowKey}, ViewDictAcc2) ->
                    dict:append(ViewNum, {RowKey, DocId}, ViewDictAcc2)
                end, ViewDictAcc, ViewNumRowKeys);
        ({not_found, _}, ViewDictAcc) ->
            ViewDictAcc
        end, dict:new(), Lookups),

    % Now remove the values from the btrees
    PurgeSeq = couch_db:get_purge_seq(Db),
    Views2 = lists:map(
        fun(#set_view{id_num=Num,btree=Btree}=View) ->
            case dict:find(Num, ViewKeysToRemoveDict) of
            {ok, RemoveKeys} ->
                {ok, ViewBtree2} = couch_btree:add_remove(Btree, [], RemoveKeys),
                View#set_view{btree = ViewBtree2};
            error -> % no keys to remove in this view
                View
            end
        end, Views),
    ok = couch_file:flush(Fd),
    NewPurgeSeqs = ?replace(?set_purge_seqs(Group), PartitionId, PurgeSeq),
    Header = Group#set_view_group.index_header,
    NewHeader = Header#set_view_index_header{purge_seqs = NewPurgeSeqs},
    Group#set_view_group{
        id_btree = IdBtree2,
        views = Views2,
        index_header = NewHeader
    }.


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
    case couch_work_queue:dequeue(WriteQueue) of
    closed ->
        FinalAcc = flush_writes(Acc#writer_acc{final_batch = true}),
        write_header(FinalAcc#writer_acc.group, false),
        FinalAcc;
    {ok, Queue, QueueSize} ->
        Kvs2 = Kvs ++ lists:flatten(Queue),
        KvsSize2 = KvsSize + QueueSize,
        case (KvsSize2 >= ?MIN_WRITER_BATCH_SIZE) orelse (length(Kvs2) >= ?MIN_WRITER_NUM_ITEMS) of
        true ->
            Acc1 = flush_writes(Acc#writer_acc{kvs = Kvs2, kvs_size = KvsSize2}),
            Acc2 = Acc1#writer_acc{kvs = [], kvs_size = 0};
        false ->
            Acc2 = Acc#writer_acc{kvs = Kvs2, kvs_size = KvsSize2}
        end,
        do_writes(Acc2)
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
                andalso (Seq >= dict:fetch(PartId, MaxSeqs)) of
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


maybe_checkpoint(#writer_acc{owner = nil}) ->
    ok;
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


checkpoint(#writer_acc{owner = nil}, _DoFsync) ->
    ok;
checkpoint(#writer_acc{owner = Owner, parent = Parent, group = Group}, DoFsync) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = Type
    } = Group,
    ?LOG_INFO("Checkpointing set view `~s` update for ~s group `~s`",
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
