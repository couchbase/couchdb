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

-export([update/3]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-define(QUEUE_MAX_ITEMS, 1000).
-define(QUEUE_MAX_SIZE, 100 * 1024).
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
    replicas_transferred = []
}).

update(Owner, Group, FileName) ->
    #set_view_group{
        set_name = SetName,
        type = Type,
        name = DDocId
    } = Group,
    ActiveParts = ordsets:from_list(
        couch_set_view_util:decode_bitmask(?set_abitmask(Group))),
    PassiveParts = ordsets:from_list(
        couch_set_view_util:decode_bitmask(?set_pbitmask(Group))),

    FoldFun = fun(P, {A1, A2, A3, A4}) ->
        {ok, Db} = couch_db:open_int(?dbname(SetName, P), []),
        {ok, {NotDel, Del, _}} = couch_btree:full_reduce(Db#db.docinfo_by_id_btree),
        Seq = couch_db:get_update_seq(Db),
        {[{P, Db} | A1], [{P, Del} | A2], [{P, NotDel} | A3], [{P, Seq} | A4]}
    end,

    {ActiveDbs0, ActiveDelCounts, ActiveNotDelCounts, ActiveSeqs} =
        lists:foldl(FoldFun, {[], [], [], []}, ActiveParts),
    {PassiveDbs0, PassiveDelCounts, PassiveNotDelCounts, PassiveSeqs} =
        lists:foldl(FoldFun, {[], [], [], []}, PassiveParts),

    ActiveDbs = lists:reverse(ActiveDbs0),
    PassiveDbs = lists:reverse(PassiveDbs0),
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

    update(Owner, Group, FileName, ActiveDbs, PassiveDbs, MaxSeqs).


update(Owner, Group, FileName, ActiveDbs, PassiveDbs, MaxSeqs) ->
    #set_view_group{
        set_name = SetName,
        type = Type,
        index_header = #set_view_index_header{seqs = SinceSeqs},
        fd = GroupFd,
        sig = GroupSig
    } = Group,

    process_flag(trap_exit, true),
    StartTime = now(),
    NumChanges = lists:foldl(
        fun({{PartId, NewSeq}, {PartId, OldSeq}}, Acc) ->
             Acc + (NewSeq - OldSeq)
        end,
        0, lists:zip(lists:keysort(1, dict:to_list(MaxSeqs)), SinceSeqs)),

    {ok, MapQueue} = couch_work_queue:new(
        [{max_size, ?QUEUE_MAX_SIZE}, {max_items, ?QUEUE_MAX_ITEMS}]),
    {ok, WriteQueue} = couch_work_queue:new(
        [{max_size, ?QUEUE_MAX_SIZE}, {max_items, ?QUEUE_MAX_ITEMS}]),

    ok = couch_indexer_manager:enter(),

    Mapper = spawn_link(fun() ->
        try
            QsGroup = add_query_server(Group),
            case can_do_batched_maps(Group) of
            true ->
                do_batched_maps(QsGroup, MapQueue, WriteQueue, [], 0);
            false->
                do_maps(QsGroup, MapQueue, WriteQueue)
            end
        catch _:Error ->
            exit(Error)
        end
    end),

    Parent = self(),
    Writer = spawn_link(fun() ->
        {ok, RawReadFd} = file:open(FileName, [read, raw, binary]),
        erlang:put({GroupFd, fast_fd_read}, RawReadFd),

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
                {ok, Db} = couch_db:open_int(?dbname(SetName, PartId), [sys_db]),
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
            #writer_acc{group = NewGroup, state = UpState} = do_writes(WriterAcc),
            Parent ! {new_group, NewGroup, UpState}
        catch _:Error ->
            exit(Error)
        after
            ok = file:close(RawReadFd)
        end
    end),

    DocLoader = spawn_link(fun() ->
        load_changes(Owner, Parent, Group, MapQueue, Writer, ActiveDbs, PassiveDbs)
    end),

    Result = wait_result_loop(StartTime, DocLoader, Mapper, Writer),
    ok = couch_indexer_manager:leave(),
    exit(Result).


wait_result_loop(StartTime, DocLoader, Mapper, Writer) ->
    receive
    {new_group, NewGroup, UpState} ->
        Duration = timer:now_diff(now(), StartTime) / 1000000,
        {updater_finished, NewGroup, UpState, Duration};
    stop_after_active ->
        DocLoader ! stop_after_active,
        wait_result_loop(StartTime, DocLoader, Mapper, Writer);
    stop_immediately ->
        DocLoader ! stop_immediately,
        wait_result_loop(StartTime, DocLoader, Mapper, Writer);
    {'EXIT', _, Reason} when Reason =/= normal ->
        couch_util:shutdown_sync(DocLoader),
        couch_util:shutdown_sync(Mapper),
        couch_util:shutdown_sync(Writer),
        {updater_error, Reason}
    end.


load_changes(Owner, Updater, Group, MapQueue, Writer, ActiveDbs, PassiveDbs) ->
    #set_view_group{
        set_name = SetName,
        type = GroupType,
        index_header = #set_view_index_header{seqs = SinceSeqs}
    } = Group,

    FoldFun = fun({PartId, Db}, PartType) ->
        maybe_stop(PartType),
        Since = couch_util:get_value(PartId, SinceSeqs),
        ?LOG_INFO("Reading changes (since sequence ~p) from ~s partition ~s to"
                  " update ~s set view group `~s`",
                  [Since, PartType, couch_db:name(Db), GroupType, SetName]),
        ChangesWrapper = fun(DocInfo, _, ok) ->
            maybe_stop(PartType),
            load_doc(Db, PartId, DocInfo, MapQueue),
            maybe_stop(PartType),
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
        active = lists:foldl(FoldFun, active, ActiveDbs),
        passive = lists:foldl(FoldFun, passive, PassiveDbs)
    catch throw:stop ->
        Writer ! stop
    end,
    couch_work_queue:close(MapQueue).


maybe_stop(active) ->
    receive
    stop_immediately ->
        throw(stop)
    after 0 ->
        ok
    end;
maybe_stop(passive) ->
    receive
    stop_immediately ->
        throw(stop);
    stop_after_active ->
        throw(stop)
    after 0 ->
        ok
    end.

notify_owner(nil, _Msg, _UpdaterPid) ->
    ok;
notify_owner(Owner, Msg, UpdaterPid) when is_pid(Owner) ->
    Owner ! {updater_info, UpdaterPid, Msg}.


add_query_server(#set_view_group{query_server = nil} = Group) ->
    {ok, Qs} = couch_query_servers:start_doc_map(
        Group#set_view_group.def_lang,
        [View#set_view.def || View <- Group#set_view_group.views],
        Group#set_view_group.lib),
    Group#set_view_group{query_server = Qs};
add_query_server(Group) ->
    Group.


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
                case ViewBtree2 =/= Btree of
                    true ->
                        PSeqs = ?replace(View#set_view.purge_seqs, PartitionId, PurgeSeq),
                        View#set_view{btree=ViewBtree2, purge_seqs=PSeqs};
                    _ ->
                        View#set_view{btree=ViewBtree2}
                end;
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


can_do_batched_maps(#set_view_group{def_lang = <<"erlang">>}) ->
    true;
can_do_batched_maps(_Group) ->
    case os:type() of
    {win32, _} ->
        false;
    _ ->
        true
    end.


do_maps(#set_view_group{query_server = Qs} = Group, MapQueue, WriteQueue) ->
    case couch_work_queue:dequeue(MapQueue) of
    closed ->
        couch_work_queue:close(WriteQueue),
        couch_query_servers:stop_doc_map(Group#set_view_group.query_server);
    {ok, Queue, _QueueSize} ->
        lists:foreach(
            fun({Seq, #doc{id = Id, deleted = true}, PartitionId}) ->
                Item = {Seq, Id, PartitionId, []},
                ok = couch_work_queue:queue(WriteQueue, Item);
            ({Seq, #doc{id = Id, deleted = false} = Doc, PartitionId}) ->
                {ok, Result} = couch_query_servers:map_doc_raw(Qs, Doc),
                Item = {Seq, Id, PartitionId, Result},
                ok = couch_work_queue:queue(WriteQueue, Item)
            end,
            Queue),
        do_maps(Group, MapQueue, WriteQueue)
    end.


% TODO: batch by byte size as well, not just changes #
do_batched_maps(#set_view_group{query_server = Qs} = Group, MapQueue, WriteQueue, Acc, AccSize) ->
    case couch_work_queue:dequeue(MapQueue) of
    closed ->
        compute_map_results(Group, WriteQueue, Acc),
        couch_work_queue:close(WriteQueue),
        couch_query_servers:stop_doc_map(Qs);
    {ok, Queue, QueueSize} ->
        Acc2 = Acc ++ Queue,
        AccSize2 = AccSize + QueueSize,
        case (AccSize2 >= ?QUEUE_MAX_SIZE) orelse (length(Acc2) >= ?QUEUE_MAX_ITEMS) of
        true ->
            compute_map_results(Group, WriteQueue, Acc2),
            do_batched_maps(Group, MapQueue, WriteQueue, [], 0);
        false ->
            do_batched_maps(Group, MapQueue, WriteQueue, Acc2, AccSize2)
        end
    end.


compute_map_results(_Group, _WriteQueue, []) ->
    ok;
compute_map_results(#set_view_group{query_server = Qs}, WriteQueue, Queue) ->
    {Deleted, NotDeleted} = lists:partition(
        fun({_Seq, Doc, _PartId}) -> Doc#doc.deleted end,
        Queue),
    NotDeletedDocs = [Doc || {_Seq, Doc, _PartId} <- NotDeleted],
    {ok, MapResultList} = couch_query_servers:map_docs_raw(Qs, NotDeletedDocs),
    lists:foreach(
        fun({MapResults, {Seq, Doc, PartId}}) ->
            Item = {Seq, Doc#doc.id, PartId, MapResults},
            ok = couch_work_queue:queue(WriteQueue, Item)
        end,
        lists:zip(MapResultList, NotDeleted)),
    lists:foreach(
        fun({Seq, #doc{id = Id, deleted = true}, PartId}) ->
            Item = {Seq, Id, PartId, []},
            ok = couch_work_queue:queue(WriteQueue, Item)
        end,
        Deleted).


do_writes(#writer_acc{kvs = Kvs, kvs_size = KvsSize, write_queue = WriteQueue} = Acc) ->
    case couch_work_queue:dequeue(WriteQueue) of
    closed ->
        flush_writes(Acc#writer_acc{final_batch = true});
    {ok, Queue, QueueSize} ->
        Kvs2 = Kvs ++ Queue,
        KvsSize2 = KvsSize + QueueSize,
        case (KvsSize2 >= ?QUEUE_MAX_SIZE) orelse (length(Kvs2) >= ?QUEUE_MAX_ITEMS) of
        true ->
            Acc1 = flush_writes(Acc#writer_acc{kvs = Kvs2, kvs_size = KvsSize2}),
            Acc2 = Acc1#writer_acc{kvs = [], kvs_size = 0};
        false ->
            Acc2 = Acc#writer_acc{kvs = Kvs2, kvs_size = KvsSize2}
        end,
        do_writes(Acc2)
    end.


flush_writes(#writer_acc{kvs = [], owner = Owner, parent = Parent, group = Group} = Acc) ->
    #writer_acc{group = NewGroup} = Acc2 = update_transferred_replicas(Acc, []),
    case NewGroup of
    Group ->
        ok;
    _ ->
        ok = gen_server:cast(Owner, {partial_update, Parent, NewGroup})
    end,
    Acc2;
flush_writes(Acc) ->
    #writer_acc{
        kvs = Queue,
        initial_build = InitialBuild,
        view_empty_kvs = ViewEmptyKVs,
        group = Group,
        parent = Parent,
        owner = Owner
    } = Acc,
    {ViewKVs, DocIdViewIdKeys, PartIdSeqs} = lists:foldl(
        fun({Seq, DocId, PartId, []}, {ViewKVsAcc, DocIdViewIdKeysAcc, PartIdSeqs}) ->
            PartIdSeqs2 = update_part_seq(Seq, PartId, PartIdSeqs),
            {ViewKVsAcc, [{DocId, {PartId, []}} | DocIdViewIdKeysAcc], PartIdSeqs2};
        ({Seq, DocId, PartId, RawQueryResults}, {ViewKVsAcc, DocIdViewIdKeysAcc, PartIdSeqs}) ->
            QueryResults = [
                [list_to_tuple(FunResult) || FunResult <- FunRs] || FunRs <-
                    couch_query_servers:raw_to_ejson(RawQueryResults)
            ],
            {NewViewKVs, NewViewIdKeys} = view_insert_doc_query_results(
                    DocId, PartId, QueryResults, ViewKVsAcc, [], []),
            PartIdSeqs2 = update_part_seq(Seq, PartId, PartIdSeqs),
            {NewViewKVs, [{DocId, {PartId, NewViewIdKeys}} | DocIdViewIdKeysAcc], PartIdSeqs2}
        end,
        {ViewEmptyKVs, [], orddict:new()}, Queue),
    {Group2, CleanupTime, CleanupKVCount} = write_changes(
        Group, ViewKVs, DocIdViewIdKeys, PartIdSeqs, InitialBuild),
    #writer_acc{group = Group3} = Acc2 =
        update_transferred_replicas(Acc#writer_acc{group = Group2}, PartIdSeqs),
    case Owner of
    nil ->
        ok;
    _ ->
        ok = gen_server:cast(Owner, {partial_update, Parent, Group3}),
        case ?set_cbitmask(Group) of
        0 ->
            ok;
        _ ->
            ok = gen_server:cast(Owner, {cleanup_done, CleanupTime, CleanupKVCount})
        end
    end,
    update_task(length(Queue)),
    case (Acc2#writer_acc.state =:= updating_active) andalso
        lists:any(fun({PartId, _}) ->
            ((1 bsl PartId) band ?set_pbitmask(Group) =/= 0)
        end, PartIdSeqs) of
    true ->
        notify_owner(Owner, {state, updating_passive}, Parent),
        Acc2#writer_acc{state = updating_passive};
    false ->
        Acc2
    end.


update_transferred_replicas(#writer_acc{group = Group} = Acc, _PartIdSeqs) when ?set_replicas_on_transfer(Group) =:= [] ->
    Acc;
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
        Acc#writer_acc{replicas_transferred = RepsTransferred2};
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
        Acc#writer_acc{group = Group2}
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
        fun({Key, Val}, {[{{Key, _DocId} = Kd, PrevVal} | AccRest], AccVid}) ->
            AccKv2 = case PrevVal of
            {dups, Dups} ->
                [{Kd, {dups, [{PartitionId, Val} | Dups]}} | AccRest];
            _ ->
                [{Kd, {dups, [{PartitionId, Val}, PrevVal]}} | AccRest]
            end,
            {AccKv2, [{View#set_view.id_num, Key} | AccVid]};
        ({Key, Val}, {AccKv, AccVid}) ->
            {[{{Key, DocId}, {PartitionId, Val}} | AccKv], [{View#set_view.id_num, Key} | AccVid]}
        end,
        {KVs, ViewIdKeysAcc}, lists:sort(ResultKVs)),
    NewViewKVsAcc = [{View, NewKVs} | ViewKVsAcc],
    view_insert_doc_query_results(
        DocId, PartitionId, RestResults, RestViewKVs, NewViewKVsAcc, NewViewIdKeysAcc).


write_changes(Group, ViewKeyValuesToAdd, DocIdViewIdKeys, PartIdSeqs, InitialBuild) ->
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
        CleanupStart = now(),
        {ok, LookupResults, {_, IdBtreePurgedKeyCount}, IdBtree2} =
            couch_btree:query_modify(
                IdBtree, LookupDocIds, AddDocIdViewIdKeys, RemoveDocIds, CleanupFun, {go, 0})
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
    {Views2, CleanupKvCount} = lists:mapfoldl(fun({View, {_View, AddKeyValues}}, Acc) ->
            KeysToRemove = couch_util:dict_find(View#set_view.id_num, KeysToRemoveByView, []),
            case ?set_cbitmask(Group) of
            0 ->
                CleanupCount = 0,
                {ok, ViewBtree2} = couch_btree:add_remove(
                    View#set_view.btree, AddKeyValues, KeysToRemove);
            _ ->
                {ok, {_, CleanupCount}, ViewBtree2} = couch_btree:add_remove(
                    View#set_view.btree, AddKeyValues, KeysToRemove, CleanupFun, {go, 0})
            end,
            NewView = case ViewBtree2 =/= View#set_view.btree of
                true ->
                    NewUpSeqs = update_seqs(PartIdSeqs, View#set_view.update_seqs),
                    View#set_view{btree=ViewBtree2, update_seqs=NewUpSeqs};
                _ ->
                    View#set_view{btree=ViewBtree2}
            end,
            {NewView, Acc + CleanupCount}
        end,
        IdBtreePurgedKeyCount, lists:zip(Group#set_view_group.views, ViewKeyValuesToAdd)),
    couch_file:flush(Fd),
    NewSeqs = update_seqs(PartIdSeqs, ?set_seqs(Group)),
    Header = Group#set_view_group.index_header,
    NewHeader = Header#set_view_index_header{seqs = NewSeqs, cbitmask = 0},
    case ?set_cbitmask(Group) of
    0 ->
        CleanupTime = nil;
    _ ->
        CleanupTime = timer:now_diff(now(), CleanupStart) / 1000000,
        ?LOG_INFO("Updater for set view `~s`, ~s group `~s`, performed cleanup "
            "of ~p key/value pairs in ~.3f seconds",
            [SetName, GroupType, GroupName, CleanupKvCount, CleanupTime])
    end,
    NewGroup = Group#set_view_group{
        views = Views2,
        id_btree = IdBtree2,
        index_header = NewHeader
    },
    {NewGroup, CleanupTime, CleanupKvCount}.


update_seqs(PartIdSeqs, Seqs) ->
    orddict:fold(
        fun(PartId, S, Acc) -> ?replace(Acc, PartId, S) end,
        Seqs, PartIdSeqs).


update_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress = erlang:min((Changes2 * 100) div Total, 100),
    couch_task_status:update([{progress, Progress}, {changes_done, Changes2}]).
