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

-export([update/2, update/3]).

-include("couch_db.hrl").
-include("couch_set_view.hrl").

-define(QUEUE_ITEMS, 500).
-define(replace(L, K, V), lists:keystore(K, 1, L, {K, V})).


update(Owner, #set_view_group{db_set = DbSet} = Group) ->
    {ok, NewSeqs} = couch_db_set:get_seqs(DbSet),
    update(Owner, Group, NewSeqs).

update(Owner, Group, NewSeqs) ->
    #set_view_group{
        set_name = SetName,
        name = GroupName,
        index_header = #set_view_index_header{seqs = SinceSeqs}
    } = Group,

    NumChanges = lists:foldl(
        fun({{PartId, NewSeq}, {PartId, OldSeq}}, Acc) ->
             Acc + (NewSeq - OldSeq)
        end,
        0, lists:zip(NewSeqs, SinceSeqs)),

    {ok, MapQueue} = couch_work_queue:new(
        [{max_size, 100000}, {max_items, ?QUEUE_ITEMS}]),
    {ok, WriteQueue} = couch_work_queue:new(
        [{max_size, 100000}, {max_items, ?QUEUE_ITEMS}]),

    ok = couch_indexer_manager:enter(),

    spawn_link(fun() ->
        do_maps(add_query_server(Group), MapQueue, WriteQueue)
    end),

    Parent = self(),
    Writer = spawn_link(fun() ->
        couch_task_status:add_task([
            {type, indexer},
            {set, SetName},
            {design_document, GroupName},
            {progress, 0},
            {changes_done, 0},
            {total_changes, NumChanges}
        ]),
        couch_task_status:set_update_frequency(500),

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
        NewGroup = do_writes(
            Parent, Owner, Group2, WriteQueue, InitialBuild, ViewEmptyKVs, []),
        Parent ! {new_group, NewGroup}
    end),

    load_changes(Owner, Group, SinceSeqs, MapQueue, Writer),
    receive
    {new_group, _} = NewGroup ->
        ok = couch_indexer_manager:leave(),
        exit(NewGroup)
    end.


load_changes(Owner, Group, SinceSeqs, MapQueue, Writer) ->
    #set_view_group{
        set_name = SetName,
        db_set = DbSet,
        design_options = DesignOptions
    } = Group,

    IncludeDesign = couch_util:get_value(<<"include_design">>,
        DesignOptions, false),
    LocalSeq = couch_util:get_value(<<"local_seq">>, DesignOptions, false),
    DocOpts =
    case LocalSeq of
    true -> [conflicts, deleted_conflicts, local_seq];
    _ -> [conflicts, deleted_conflicts]
    end,
    FoldFun = fun({partition, Id, Since}, Type) ->
            maybe_stop(Type),
            ?LOG_INFO("Reading changes (since sequence ~p) from ~p partition ~s to"
                " update set view `~s`", [Since, Type, ?dbname(SetName, Id), SetName]),
            {ok, Type};
        (starting_active, _) ->
            notify_owner(Owner, updating_active),
            {ok, active};
        (starting_passive, _) ->
            maybe_stop(passive),
            notify_owner(Owner, updating_passive),
            {ok, passive};
        ({doc_info, DocInfo, PartId, Db}, Type) ->
            maybe_stop(Type),
            load_doc(Db, PartId, DocInfo, MapQueue, DocOpts, IncludeDesign),
            {ok, Type}
    end,
    try
        {ok, _} = couch_db_set:enum_docs_since(DbSet, SinceSeqs, FoldFun, active)
    catch
    throw:stop ->
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


notify_owner(nil, _State) ->
    ok;
notify_owner(Owner, State) when is_pid(Owner) ->
    Owner ! {updater_state, self(), State}.


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


load_doc(Db, PartitionId, DocInfo, MapQueue, DocOpts, IncludeDesign) ->
    #doc_info{id=DocId, high_seq=Seq, revs=[#rev_info{deleted=Deleted}|_]} = DocInfo,
    case {IncludeDesign, DocId} of
    {false, <<?DESIGN_DOC_PREFIX, _/binary>>} -> % we skip design docs
        ok;
    _ ->
        if Deleted ->
            couch_work_queue:queue(MapQueue, {Seq, #doc{id=DocId, deleted=true}, PartitionId});
        true ->
            {ok, Doc} = couch_db:open_doc_int(Db, DocInfo, DocOpts),
            couch_work_queue:queue(MapQueue, {Seq, Doc, PartitionId})
        end
    end.

do_maps(#set_view_group{query_server = Qs} = Group, MapQueue, WriteQueue) ->
    case couch_work_queue:dequeue(MapQueue) of
    closed ->
        couch_work_queue:close(WriteQueue),
        couch_query_servers:stop_doc_map(Group#set_view_group.query_server);
    {ok, Queue} ->
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


do_writes(Parent, Owner, Group, WriteQueue, InitialBuild, ViewEmptyKVs, Acc) ->
    case couch_work_queue:dequeue(WriteQueue) of
    closed ->
        flush_writes(Parent, Owner, Group, InitialBuild, ViewEmptyKVs, Acc);
    {ok, Queue} ->
        Acc2 = Acc ++ Queue,
        case length(Acc2) >= ?QUEUE_ITEMS of
        true ->
            Group2 = flush_writes(Parent, Owner, Group, InitialBuild, ViewEmptyKVs, Acc2),
            NewAcc = [];
        false ->
            Group2 = Group,
            NewAcc = Acc2
        end,
        do_writes(Parent, Owner, Group2, WriteQueue, InitialBuild, ViewEmptyKVs, NewAcc)
    end.


flush_writes(_Parent, _Owner, Group, _InitialBuild, _ViewEmptyKVs, []) ->
    Group;
flush_writes(Parent, Owner, Group, InitialBuild, ViewEmptyKVs, Queue) ->
    {ViewKVs, DocIdViewIdKeys, PartIdSeqs} = lists:foldr(
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
        {ViewEmptyKVs, [], dict:new()}, Queue),
    {Group2, CleanupTime, CleanupKVCount} = write_changes(
        Group, ViewKVs, DocIdViewIdKeys, PartIdSeqs, InitialBuild),
    case Owner of
    nil ->
        ok;
    _ ->
        ok = gen_server:cast(Owner, {partial_update, Parent, Group2}),
        case ?set_cbitmask(Group) of
        0 ->
            ok;
        _ ->
            ok = gen_server:cast(Owner, {cleanup_done, CleanupTime, CleanupKVCount})
        end
    end,
    update_task(length(Queue)),
    Group2.


update_part_seq(Seq, PartId, Acc) ->
    case dict:find(PartId, Acc) of
    {ok, Max} when Max >= Seq ->
        Acc;
    _ ->
        dict:store(PartId, Seq, Acc)
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
        name = GroupName
    } = Group,

    AddDocIdViewIdKeys = [{DocId, ViewIdKeys} || {DocId, ViewIdKeys} <- DocIdViewIdKeys, ViewIdKeys /= []],
    if InitialBuild ->
        RemoveDocIds = [],
        LookupDocIds = [];
    true ->
        RemoveDocIds = [DocId || {DocId, ViewIdKeys} <- DocIdViewIdKeys, ViewIdKeys == []],
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
        CleanupTime = timer:now_diff(now(), CleanupStart),
        ?LOG_INFO("Updater for set view `~s`, group `~s`, performed cleanup "
            "of ~p key/value pairs in ~.3f seconds",
            [SetName, GroupName, CleanupKvCount, CleanupTime / 1000000])
    end,
    NewGroup = Group#set_view_group{
        views = Views2,
        id_btree = IdBtree2,
        index_header = NewHeader
    },
    {NewGroup, CleanupTime, CleanupKvCount}.


update_seqs(PartIdSeqs, Seqs) ->
    dict:fold(
        fun(PartId, S, Acc) -> ?replace(Acc, PartId, S) end,
        Seqs, PartIdSeqs).


update_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress = erlang:min((Changes2 * 100) div Total, 100),
    couch_task_status:update([{progress, Progress}, {changes_done, Changes2}]).
