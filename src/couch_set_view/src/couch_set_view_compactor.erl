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

-module(couch_set_view_compactor).

-include ("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-export([start_compact/2, start_compact/3, cancel_compact/2, cancel_compact/3]).

-record(acc, {
   last_id = nil,
   changes = 0,
   total_changes
}).

start_compact(SetName, DDocId) ->
    start_compact(SetName, DDocId, main).

start_compact(SetName, DDocId, Type) ->
    {ok, Pid} = get_group_pid(SetName, DDocId, Type),
    gen_server:call(Pid, {start_compact, fun compact_group/5}).


cancel_compact(SetName, DDocId) ->
    cancel_compact(SetName, DDocId, main).

cancel_compact(SetName, DDocId, Type) ->
    {ok, Pid} = get_group_pid(SetName, DDocId, Type),
    gen_server:call(Pid, cancel_compact).


%%=============================================================================
%% internal functions
%%=============================================================================

%% @spec compact_group(Group, NewGroup, SetName, FileName) -> ok
compact_group(Group, EmptyGroup, SetName, FileName, CompactFileName) ->
    #set_view_group{
        id_btree = IdBtree,
        views = Views,
        name = GroupId,
        type = Type,
        index_header = Header,
        fd = GroupFd,
        sig = GroupSig
    } = Group,
    StartTime = now(),

    #set_view_group{
        id_btree = EmptyIdBtree,
        views = EmptyViews,
        fd = Fd
    } = EmptyGroup,

    TotalChanges = total_kv_count(Group),
    Acc0 = #acc{total_changes = TotalChanges},

    DDocIds = couch_set_view_util:get_ddoc_ids_with_sig(SetName, GroupSig),
    couch_task_status:add_task([
        {type, view_compaction},
        {set, SetName},
        {design_documents, DDocIds},
        {changes_done, 0},
        {total_changes, TotalChanges},
        {indexer_type, Type},
        {progress, case TotalChanges of 0 -> 100; _ -> 0 end}
    ]),

    ok = couch_set_view_util:open_raw_read_fd(GroupFd, FileName),

    BeforeKVWriteFun = fun({DocId, _} = KV, #acc{last_id = LastDocId} = Acc) ->
        if DocId =:= LastDocId -> % COUCHDB-999
            ?LOG_ERROR("Duplicates of document `~s` detected in set view `~s`"
                ", group `~s` - view rebuild, from scratch, is required",
                [DocId, SetName, GroupId]),
            exit({view_duplicated_id, DocId});
        true ->
            ok
        end,
        {KV, update_task(Acc, 1)}
    end,

    FilterFun = fun({_Key, {PartId, _}}) ->
        ((1 bsl PartId) band ?set_cbitmask(Group)) =:= 0
    end,
    % First copy the id btree.
    {ok, NewIdBtreeRoot, Acc1} = couch_btree_copy:copy(
        IdBtree, Fd,
        [{before_kv_write, {BeforeKVWriteFun, Acc0}}, {filter, FilterFun}]),
    NewIdBtree = EmptyIdBtree#btree{root = NewIdBtreeRoot},

    {NewViews, _} = lists:mapfoldl(fun({View, EmptyView}, Acc) ->
        compact_view(Fd, View, EmptyView, FilterFun, Acc)
    end, Acc1, lists:zip(Views, EmptyViews)),

    NewGroup = EmptyGroup#set_view_group{
        id_btree = NewIdBtree,
        views = NewViews,
        index_header = Header#set_view_index_header{
            cbitmask = 0,
            id_btree_state = nil,
            view_states = nil
        }
    },
    CleanupKVCount = TotalChanges - total_kv_count(NewGroup),
    ok = couch_file:flush(NewGroup#set_view_group.fd),
    CompactResult = #set_view_compactor_result{
        group = NewGroup,
        compact_time = timer:now_diff(now(), StartTime) / 1000000,
        cleanup_kv_count = CleanupKVCount
    },
    maybe_retry_compact(CompactResult, SetName, StartTime, GroupFd, CompactFileName).

maybe_retry_compact(CompactResult0, SetName, StartTime, GroupFd, CompactFileName) ->
    NewGroup = CompactResult0#set_view_compactor_result.group,
    #set_view_group{
        name = DDocId,
        type = Type
    } = NewGroup,
    CompactResult = CompactResult0#set_view_compactor_result{
        compact_time = timer:now_diff(now(), StartTime) / 1000000
    },
    {ok, Pid} = get_group_pid(SetName, DDocId, Type),
    case gen_server:call(Pid, {compact_done, CompactResult}) of
    ok ->
        ok = couch_set_view_util:close_raw_read_fd(GroupFd);
    update ->
        {_, Ref} = erlang:spawn_monitor(fun() ->
            couch_set_view_updater:update(nil, NewGroup, CompactFileName)
        end),
        receive
        {'DOWN', Ref, _, _, {updater_finished, UpdaterResult}} ->
            CompactResult2 = CompactResult0#set_view_compactor_result{
                group = UpdaterResult#set_view_updater_result.group
            },
            maybe_retry_compact(
                CompactResult2, SetName, StartTime, GroupFd, CompactFileName);
        {'DOWN', Ref, _, _, Reason} ->
            exit(Reason)
        end
    end.


get_group_pid(SetName, DDocId, main) ->
    Pid = couch_set_view:get_group_pid(SetName, DDocId),
    {ok, Pid};
get_group_pid(SetName, DDocId, replica) ->
    Pid = couch_set_view:get_group_pid(SetName, DDocId),
    {ok, #set_view_group{replica_pid = RepPid}} = couch_set_view_group:request_group(Pid, ok),
    case is_pid(RepPid) of
    true ->
        {ok, RepPid};
    false ->
        no_replica_group_found
    end.


%% @spec compact_view(Fd, View, EmptyView, Acc) -> {CompactView, NewAcc}
compact_view(Fd, View, #set_view{btree = ViewBtree} = EmptyView, FilterFun, Acc0) ->
    BeforeKVWriteFun = fun(Item, Acc) ->
        {Item, update_task(Acc, 1)}
    end,

    % Copy each view btree.
    {ok, NewBtreeRoot, Acc2} = couch_btree_copy:copy(
        View#set_view.btree, Fd,
        [{before_kv_write, {BeforeKVWriteFun, Acc0}}, {filter, FilterFun}]),
    ViewBtree2 = ViewBtree#btree{root = NewBtreeRoot},
    NewView = EmptyView#set_view{
        btree = ViewBtree2,
        update_seqs = View#set_view.update_seqs,
        purge_seqs = View#set_view.purge_seqs
    },
    {NewView, Acc2}.


update_task(#acc{total_changes = 0} = Acc, _ChangesInc) ->
    Acc;
update_task(#acc{changes = Changes, total_changes = Total} = Acc, ChangesInc) ->
    Changes2 = Changes + ChangesInc,
    couch_task_status:update([
        {changes_done, Changes2},
        {progress, (Changes2 * 100) div Total}
    ]),
    Acc#acc{changes = Changes2}.


total_kv_count(#set_view_group{id_btree = IdBtree, views = Views}) ->
    {ok, {IdCount, _}} = couch_btree:full_reduce(IdBtree),
    lists:foldl(
        fun(#set_view{btree = Bt}, Acc) ->
            {ok, {Count, _, _}} = couch_btree:full_reduce(Bt),
            Acc + Count
        end,
        IdCount, Views).
