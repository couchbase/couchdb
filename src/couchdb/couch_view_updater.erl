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

-spec update(_, #group{}, Dbname::binary()) -> no_return().

update(Owner, Group, DbName) ->
    #group{
        name = GroupName,
        current_seq = Seq,
        purge_seq = PurgeSeq
    } = Group,

    {ok, Db} = couch_db:open_int(DbName, []),
    DbPurgeSeq = couch_db:get_purge_seq(Db),
    TimeToPurge = DbPurgeSeq == PurgeSeq + 1,
    if DbPurgeSeq == PurgeSeq ->
        ok;
    TimeToPurge ->
        ok;
    true ->
        exit(reset)
    end,
    {ok, MapQueue} = couch_work_queue:new(
        [{max_size, 100000}, {max_items, 500}]),
    {ok, WriteQueue} = couch_work_queue:new(
        [{max_size, 100000}, {max_items, 500}]),
    Self = self(),
    spawn_link(fun() -> start_maps(Group, MapQueue, WriteQueue) end),
    spawn_link(fun() ->
        couch_task_status:add_task(
            <<"View Group Indexer">>,
            <<DbName/binary, " ", GroupName/binary>>,
            <<"Starting index update">>),
        couch_task_status:set_update_frequency(500),
        TotalChanges = couch_db:count_changes_since(Db, Seq),
        start_writes(Self, Owner, WriteQueue, Seq == 0, TimeToPurge, Db, TotalChanges),
        couch_task_status:set_update_frequency(0),
        couch_task_status:update("Finishing.")
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
    couch_db:close(Db),
    receive {new_group, NewGroup} ->
        exit({new_group,
                NewGroup#group{current_seq=couch_db:get_update_seq(Db)}})
    end.



purge_index(#group{fd=Fd, views=Views, id_btree=IdBtree}=Group, Db) ->
    {ok, PurgedIdsRevs} = couch_db:get_last_purged(Db),
    Ids = [Id || {Id, _Revs} <- PurgedIdsRevs],
    {ok, Lookups, IdBtree2} = couch_btree:query_modify(IdBtree, Ids, [], Ids),

    % now populate the dictionary with all the keys to delete
    ViewKeysToRemoveDict = lists:foldl(
        fun({ok,{DocId,ViewNumRowKeys}}, ViewDictAcc) ->
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
        fun(#view{id_num=Num,btree=Btree}=View) ->
            case dict:find(Num, ViewKeysToRemoveDict) of
            {ok, RemoveKeys} ->
                {ok, ViewBtree2} = couch_btree:add_remove(Btree, [], RemoveKeys),
                case ViewBtree2 =/= Btree of
                    true ->
                        View#view{btree=ViewBtree2, purge_seq=PurgeSeq};
                    _ ->
                        View#view{btree=ViewBtree2}
                end;
            error -> % no keys to remove in this view
                View
            end
        end, Views),
    ok = couch_file:flush(Fd),
    Group#group{id_btree=IdBtree2,
            views=Views2,
            purge_seq=PurgeSeq}.


load_doc(Db, DocInfo, MapQueue, DocOpts, IncludeDesign) ->
    #doc_info{id=DocId, high_seq=Seq, revs=[#rev_info{deleted=Deleted}|_]} = DocInfo,
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

start_maps(Group, MapQueue, WriteQueue) ->
    {Group1, _Server} = get_view_server(Group),
    % We have to send the group after we open up the view server.
    couch_work_queue:queue(WriteQueue, {group, Group1}),
    do_maps(Group1, MapQueue, WriteQueue).
    
do_maps(#group{view_server=Server} = Group, MapQueue, WriteQueue) ->
    case couch_work_queue:dequeue(MapQueue) of
    closed ->
        couch_work_queue:close(WriteQueue);
    {ok, Queue} ->
        lists:foreach(
            fun({Seq, #doc{id = Id, deleted = true}}) ->
                Item = {Seq, Id, []},
                ok = couch_work_queue:queue(WriteQueue, Item);
            ({Seq, #doc{id = Id, deleted = false} = Doc}) ->
                {ok, [Result]} = couch_view_server:map(Server, [Doc]), % maybe refactor view server modules for Doc/Docs?
                Item = {Seq, Id, Result},
                ok = couch_work_queue:queue(WriteQueue, Item)
            end,
            Queue),
        do_maps(Group, MapQueue, WriteQueue)
    end.

    
% Wait for the mapper process to send us the group with the open
% query server.
start_writes(Parent, Owner, WriteQueue, InitialBuild, TimeToPurge, Db, TotalChanges) ->
    {ok, [{group, Group}]} = couch_work_queue:dequeue(WriteQueue, 1),
    % do purge
    Group2 =
    if TimeToPurge ->
        couch_task_status:update(<<"Removing purged entries from view index.">>),
        purge_index(Group, Db);
    true ->
        Group
    end,
    ViewEmptyKVs = [{View, []} || View <- Group2#group.views],
    do_writes(Parent, Owner, Group2, WriteQueue, InitialBuild, ViewEmptyKVs,
            0, TotalChanges).

do_writes(Parent, Owner, Group, WriteQueue, InitialBuild, ViewEmptyKVs,
        ChangesDone, TotalChanges) ->
% do_writes(Parent, Owner, Group, WriteQueue, InitialBuild) ->
    case couch_work_queue:dequeue(WriteQueue) of
    closed ->
        Parent ! {new_group, close_view_server(Group)};
    {ok, Queue} ->
        {ViewKVs, DocIdViewIdKeys} = lists:foldr(
            fun({_Seq, Id, []}, {ViewKVsAcc, DocIdViewIdKeysAcc}) ->
                {ViewKVsAcc, [{Id, []} | DocIdViewIdKeysAcc]};
            ({_Seq, Id, RawQueryResults}, {ViewKVsAcc, DocIdViewIdKeysAcc}) ->
                QueryResults = [
                    [list_to_tuple(FunResult) || FunResult <- FunRs] || FunRs <-
                        couch_query_servers:raw_to_ejson(RawQueryResults)
                ],
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
                % Strip view server references before sending to
                % the group server.
                Group3 = strip_view_server(Group2),
                ok = gen_server:cast(Owner, {partial_update, Parent, Group3})
        end,
        ChangesDone2 = ChangesDone + length(Queue),
        couch_task_status:update("Processed ~p of ~p changes (~p%)",
              [ChangesDone2, TotalChanges, (ChangesDone2 * 100) div TotalChanges]),
        do_writes(Parent, Owner, Group2, WriteQueue, InitialBuild, ViewEmptyKVs,
            ChangesDone2, TotalChanges)
    end.


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


% Prepare a view server for map/reduce work. Compile the necessary
% functions and update the btree's that we're going to be writing
% to. We store a copy of the reduce function that was generated in
% couch_view_group:init_group/4 so that we can replace this version
% when we send the group back. This way view readers aren't using
% the same view server process that we are.
get_view_server(#group{view_server=Server}=Group) when Server =/= nil ->
    {Group, Server};
get_view_server(Group) ->
    #group{
        def_lang=Lang,
        views=Views,
        view_server=nil
    } = Group,

    % Gather functions to compile.
    {MapFuns, RedFuns} = lists:foldl(fun(View, {MapAcc, RedAcc}) ->
        ViewId = View#view.id_num,
        MapFun = View#view.def,
        RedFuns = [FunSrc || {_Name, FunSrc} <- View#view.reduce_funs],
        {[MapFun | MapAcc], [[ViewId, RedFuns] | RedAcc]}
    end, {[], []}, Views),
    
    RevMapFuns = lists:reverse(MapFuns), % Order for maps does matter.
    
    {ok, Server} = couch_view_server:get_server(Lang, RevMapFuns, RedFuns),
    
    % Rebuild the reduce functions
    Views2 = lists:map(fun(View) ->
        #view{
            id_num=ViewId,
            reduce_funs=RedFuns2,
            btree=Btree
        } = View,
        
        FunSrcs = [FunSrc || {_Name, FunSrc} <- RedFuns2],
        ReduceFun = fun
            (reduce, KVs) ->
                KVs2 = couch_view:expand_dups(KVs,[]),
                KVs3 = couch_view:detuple_kvs(KVs2,[]),
                {ok, Reduced} = couch_view_server:reduce(Server, ViewId, FunSrcs, KVs3),
                {length(KVs3), Reduced};
            (rereduce, Reds) ->
                Count = lists:sum([Count0 || {Count0, _} <- Reds]),
                UserReds = [UserRedsList || {_, UserRedsList} <- Reds],
                {ok, Reduced} = couch_view_server:rereduce(Server, ViewId, FunSrcs, UserReds),
                {Count, Reduced}
        end,

        OldRed = couch_btree:get_reduce(Btree),
        Btree2 = couch_btree:set_options(Btree, [{reduce, ReduceFun}]),

        View#view{btree=Btree2, native_red_fun=OldRed}
    end, Views),

    {Group#group{views=Views2, view_server=Server}, Server}.
    
% Before sending a group back to the group server, we strip
% any references to the view server so we don't leak any
% handles to it.
strip_view_server(#group{views=Views}=Group) ->
    Views2 = lists:map(fun(View) ->
        #view{
            btree=Btree,
            native_red_fun=RedFun
        } = View,
        Btree2 = case RedFun of 
            undefined ->
                Btree;
            RedFun ->
                couch_btree:set_options(Btree, [{reduce, RedFun}])
        end,
        View#view{btree=Btree2}
    end, Views),
    Group#group{view_server=nil, views=Views2}.

% We finished updating the view at the end of do_writes. Now
% we close the view and strip references before sending back.
close_view_server(#group{view_server=nil}=Group) ->
    % We never started a view server, nothing to release.
    Group;
close_view_server(#group{view_server=Server}=Group) ->
    couch_view_server:ret_server(Server),
    strip_view_server(Group#group{view_server=nil}).
