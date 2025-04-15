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

-module(couch_view_updater).

-export([update/3]).

-include("couch_db.hrl").

-define(QUEUE_MAX_ITEMS, 500).
-define(QUEUE_MAX_SIZE, 100000).
-define(MIN_FLUSH_BATCH_SIZE, 250).

-spec update(_, #group{}, Dbname::binary()) -> no_return().

update(Owner, Group, DbName) when is_binary(DbName) ->
    {ok, Db} = couch_db:open_int(DbName, []),
    try
        update(Owner, Group, Db)
    after
        couch_db:close(Db)
    end;

update(Owner, Group, #db{name = DbName} = Db) ->
    ok = couch_index_barrier:enter(couch_main_index_barrier),
    #group{
        name = GroupName,
        current_seq = Seq,
        purge_seq = PurgeSeq
    } = Group,

    DbPurgeSeq = couch_db:get_purge_seq(Db),
    if DbPurgeSeq == PurgeSeq ->
        ok;
    true ->
        exit(reset)
    end,
    {ok, MapQueue} = couch_work_queue:new(
        [{max_size, ?QUEUE_MAX_SIZE}, {max_items, ?QUEUE_MAX_ITEMS}]),
    {ok, WriteQueue} = couch_work_queue:new(
        [{max_size, ?QUEUE_MAX_SIZE}, {max_items, ?QUEUE_MAX_ITEMS}]),
    Self = self(),
    spawn_link(fun() ->
        couch_view_mapreduce:start_map_context(Group),
        try
            do_maps(Group, MapQueue, WriteQueue, [], 0)
        after
            couch_view_mapreduce:end_map_context()
        end
    end),
    TotalChanges = couch_db:count_changes_since(Db, Seq),
    spawn_link(fun() ->
        couch_task_status:add_task([
            {type, indexer},
            {database, DbName},
            {design_document, GroupName},
            {progress, 0},
            {changes_done, 0},
            {total_changes, TotalChanges}
        ]),
        couch_task_status:set_update_frequency(500),
        ViewEmptyKVs = [{View, []} || View <- Group#group.views],
        couch_view_mapreduce:start_reduce_context(Group),
        try
            do_writes(Self, Owner, Group, WriteQueue, Seq == 0, ViewEmptyKVs, [])
        after
            couch_view_mapreduce:end_reduce_context(Group)
        end
    end),
    % compute on all docs modified since we last computed.
    #group{ design_options = DesignOptions } = Group,
    IncludeDesign = couch_util:get_value(<<"include_design">>,
        DesignOptions, false),
    LocalSeq = couch_util:get_value(<<"local_seq">>, DesignOptions, false),
    DocOpts =
    case LocalSeq of
    true -> [conflicts, deleted_conflicts, local_seq];
    _ -> [conflicts, deleted_conflicts]
    end,
    {ok, _, _}
        = couch_db:enum_docs_since(
            Db,
            Seq,
            fun(DocInfo, _, Acc) ->
                load_doc(Db, DocInfo, MapQueue, DocOpts, IncludeDesign),
                {ok, Acc}
            end,
            ok, []),
    couch_work_queue:close(MapQueue),
    receive {new_group, NewGroup} ->
        ok = couch_index_barrier:leave(couch_main_index_barrier),
        exit({new_group,
                NewGroup#group{current_seq=couch_db:get_update_seq(Db)}})
    end.


load_doc(Db, DocInfo, MapQueue, DocOpts, IncludeDesign) ->
    #doc_info{id=DocId, local_seq=Seq, deleted=Deleted} = DocInfo,
    case {IncludeDesign, DocId} of
    {false, <<?DESIGN_DOC_PREFIX, _/binary>>} -> % we skip design docs
        ok;
    _ ->
        if Deleted ->
            couch_work_queue:queue(MapQueue, {Seq, #doc{id=DocId, deleted=true}});
        true ->
            {ok, Doc} = couch_db:open_doc_int(Db, DocInfo, DocOpts),
            couch_work_queue:queue(MapQueue, {Seq, Doc})
        end
    end.

do_maps(Group, MapQueue, WriteQueue, AccItems, AccItemsSize) ->
    case couch_work_queue:dequeue(MapQueue) of
    closed ->
        case AccItems of
        [] ->
            ok;
        _ ->
            ok = couch_work_queue:queue(WriteQueue, AccItems)
        end,
        couch_work_queue:close(WriteQueue);
    {ok, Queue, QueueSize} ->
        ViewCount = length(Group#group.views),
        Items = lists:foldr(
            fun({Seq, #doc{id = Id, deleted = true}}, Acc) ->
                Item = {Seq, Id, []},
                [Item | Acc];
            ({Seq, #doc{id = Id, deleted = false} = Doc}, Acc) ->
                try
                    {ok, Result} = couch_view_mapreduce:map(Doc),
                    {Result2, _} = lists:foldr(
                        fun({error, Reason}, {AccRes, Pos}) ->
                            ErrorMsg = "View group `~s`, error mapping document "
                                    " `~s` for view `~s`: ~s",
                            Args = [Group#group.name, Id,
                                    view_name(Group, Pos), couch_util:to_binary(Reason)],
                            ?LOG_MAPREDUCE_ERROR(ErrorMsg, Args),
                            {[[] | AccRes], Pos - 1};
                        (KVs, {AccRes, Pos}) ->
                            {[KVs | AccRes], Pos - 1}
                        end,
                        {[], ViewCount}, Result),
                    Item = {Seq, Id, Result2},
                    [Item | Acc]
                catch _:{error, Reason} ->
                    ErrorMsg = "View group `~s`, error mapping document `~s`: ~s",
                    Args = [Group#group.name, Id, couch_util:to_binary(Reason)],
                    ?LOG_MAPREDUCE_ERROR(ErrorMsg, Args),
                    [{Seq, Id, []} | Acc]
                end
            end,
            [], Queue),
        AccItems2 = AccItems ++ Items,
        AccItemsSize2 = AccItemsSize + QueueSize,
        case (AccItemsSize2 >= ?QUEUE_MAX_SIZE) orelse
            (length(AccItems2) >= ?QUEUE_MAX_ITEMS) of
        true ->
            ok = couch_work_queue:queue(WriteQueue, AccItems2),
            do_maps(Group, MapQueue, WriteQueue, [], 0);
        false ->
            do_maps(Group, MapQueue, WriteQueue, AccItems2, AccItemsSize2)
        end
    end.


view_name(#group{views = Views}, ViewPos) ->
    V = lists:nth(ViewPos, Views),
    case V#view.map_names of
    [] ->
        [{Name, _} | _] = V#view.reduce_funs;
    [Name | _] ->
        ok
    end,
    Name.


do_writes(Parent, Owner, Group, WriteQueue, InitialBuild, ViewEmptyKVs, Acc) ->
    case couch_work_queue:dequeue(WriteQueue) of
    closed ->
         Group2 = flush_writes(
             Parent, Owner, Group, InitialBuild, ViewEmptyKVs, Acc),
         Parent ! {new_group, Group2};
    {ok, Queue, _QueueSize} ->
        Acc2 = Acc ++ lists:flatten(Queue),
        case length(Acc2) >= ?MIN_FLUSH_BATCH_SIZE of
        true ->
            Group2 = flush_writes(Parent, Owner, Group, InitialBuild, ViewEmptyKVs, Acc2),
            do_writes(Parent, Owner, Group2, WriteQueue, InitialBuild, ViewEmptyKVs, []);
        false ->
            do_writes(Parent, Owner, Group, WriteQueue, InitialBuild, ViewEmptyKVs, Acc2)
        end
    end.


flush_writes(_Parent, _Owner, Group, _InitialBuild, _ViewEmptyKVs, []) ->
    Group;
flush_writes(Parent, Owner, Group, InitialBuild, ViewEmptyKVs, Queue) ->
    {ViewKVs, DocIdViewIdKeys} = lists:foldr(
        fun({_Seq, Id, []}, {ViewKVsAcc, DocIdViewIdKeysAcc}) ->
            {ViewKVsAcc, [{Id, []} | DocIdViewIdKeysAcc]};
        ({_Seq, Id, QueryResults}, {ViewKVsAcc, DocIdViewIdKeysAcc}) ->
            {NewViewKVs, NewViewIdKeys} = view_insert_doc_query_results(
                Id, QueryResults, ViewKVsAcc, [], []),
            {NewViewKVs, [{Id, NewViewIdKeys} | DocIdViewIdKeysAcc]}
        end,
        {ViewEmptyKVs, []}, Queue),
    {NewSeq, _, _} = lists:last(Queue),
    Group2 = write_changes(
        Group, ViewKVs, DocIdViewIdKeys, NewSeq, InitialBuild),
    case Owner of
    nil ->
        ok;
    _ ->
        ok = gen_server:cast(Owner, {partial_update, Parent, Group2})
    end,
    update_task(length(Queue)),
    Group2.


view_insert_doc_query_results(_DocId, [], [], ViewKVsAcc, ViewIdKeysAcc) ->
    {lists:reverse(ViewKVsAcc), lists:reverse(ViewIdKeysAcc)};
view_insert_doc_query_results(DocId, [ResultKVs | RestResults],
        [{View, KVs} | RestViewKVs], ViewKVsAcc, ViewIdKeysAcc) ->
    % Take any identical keys and combine the values
    {NewKVs, NewViewIdKeys} = lists:foldl(
        fun({Key, Val}, {[{{Key, _DocId} = Kd, PrevVal} | AccRest], AccVid}) ->
            AccKv2 = case PrevVal of
            {dups, Dups} ->
                [{Kd, {dups, [Val | Dups]}} | AccRest];
            _ ->
                [{Kd, {dups, [Val, PrevVal]}} | AccRest]
            end,
            {AccKv2, [{View#view.id_num, Key} | AccVid]};
        ({Key, Val}, {AccKv, AccVid}) ->
            {[{{Key, DocId}, Val} | AccKv], [{View#view.id_num, Key} | AccVid]}
        end,
        {[], []}, lists:sort(ResultKVs)),
    NewViewKVsAcc = [{View, NewKVs ++ KVs} | ViewKVsAcc],
    NewViewIdKeysAcc = NewViewIdKeys ++ ViewIdKeysAcc,
    view_insert_doc_query_results(
        DocId, RestResults, RestViewKVs, NewViewKVsAcc, NewViewIdKeysAcc).


write_changes(Group, ViewKeyValuesToAdd, DocIdViewIdKeys, NewSeq, InitialBuild) ->
    #group{id_btree=IdBtree,fd=Fd} = Group,

    AddDocIdViewIdKeys = [{DocId, ViewIdKeys} || {DocId, ViewIdKeys} <- DocIdViewIdKeys, ViewIdKeys /= []],
    if InitialBuild ->
        RemoveDocIds = [],
        LookupDocIds = [];
    true ->
        RemoveDocIds = [DocId || {DocId, ViewIdKeys} <- DocIdViewIdKeys, ViewIdKeys == []],
        LookupDocIds = [DocId || {DocId, _ViewIdKeys} <- DocIdViewIdKeys]
    end,
    {ok, LookupResults, IdBtree2}
        = couch_btree:query_modify(IdBtree, LookupDocIds, AddDocIdViewIdKeys, RemoveDocIds),
    KeysToRemoveByView = lists:foldl(
        fun(LookupResult, KeysToRemoveByViewAcc) ->
            case LookupResult of
            {ok, {DocId, ViewIdKeys}} ->
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
    Views2 = lists:zipwith(fun(View, {_View, AddKeyValues}) ->
            KeysToRemove = couch_util:dict_find(View#view.id_num, KeysToRemoveByView, []),
            {ok, ViewBtree2} = couch_btree:add_remove(View#view.btree, AddKeyValues, KeysToRemove),
            case ViewBtree2 =/= View#view.btree of
                true ->
                    View#view{btree=ViewBtree2, update_seq=NewSeq};
                _ ->
                    View#view{btree=ViewBtree2}
            end
        end,    Group#group.views, ViewKeyValuesToAdd),
    couch_file:flush(Fd),
    Group#group{views=Views2, current_seq=NewSeq, id_btree=IdBtree2}.

update_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress = (Changes2 * 100) div Total,
    couch_task_status:update([{progress, Progress}, {changes_done, Changes2}]).
