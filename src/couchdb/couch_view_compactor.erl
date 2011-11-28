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

-module(couch_view_compactor).

-include ("couch_db.hrl").

-export([start_compact/2, cancel_compact/2]).

-record(acc, {
   last_id = nil,
   changes = 0,
   total_changes
}).

%% @spec start_compact(DbName::binary(), GroupId:binary()) -> ok
%% @doc Compacts the views.  GroupId must not include the _design/ prefix
start_compact({DbName, GroupDbName}, GroupId) ->
    Pid = couch_view:get_group_server({DbName, GroupDbName}, GroupId),
    CompactFun = fun(Group, EmptyGroup, _) ->
        compact_group(Group, EmptyGroup, {DbName, GroupDbName})
    end,
    gen_server:call(Pid, {start_compact, CompactFun});
start_compact(DbName, GroupId) ->
    Pid = couch_view:get_group_server(DbName, <<"_design/",GroupId/binary>>),
    gen_server:call(Pid, {start_compact, fun compact_group/3}).

cancel_compact({DbName, GroupDbName}, GroupId) ->
    Pid = couch_view:get_group_server({DbName, GroupDbName}, GroupId),
    gen_server:call(Pid, cancel_compact);
cancel_compact(DbName, GroupId) ->
    Pid = couch_view:get_group_server(DbName, <<"_design/", GroupId/binary>>),
    gen_server:call(Pid, cancel_compact).

%%=============================================================================
%% internal functions
%%=============================================================================

docs_db_name({DocsDbName, _DDocDbName}) ->
    DocsDbName;
docs_db_name(DbName) when is_binary(DbName) ->
    DbName.

%% @spec compact_group(Group, NewGroup) -> ok
compact_group(Group, EmptyGroup, DbName) ->
    #group{
        current_seq = Seq,
        id_btree = IdBtree,
        name = GroupId,
        views = Views
    } = Group,

    #group{
        id_btree = EmptyIdBtree,
        views = EmptyViews,
        fd = Fd
    } = EmptyGroup,

    DbName1 = docs_db_name(DbName),
    {ok, Db} = couch_db:open_int(DbName1, []),
    {ok, DbReduce} = couch_btree:full_reduce(Db#db.docinfo_by_id_btree),
    Count = element(1, DbReduce),

    TotalChanges = lists:foldl(
        fun(View, Acc) ->
            {ok, Kvs} = couch_view:get_row_count(View),
            Acc + Kvs
        end,
        Count, Views),
    Acc0 = #acc{total_changes = TotalChanges},

    couch_task_status:add_task([
        {type, view_compaction},
        {database, DbName1},
        {design_document, GroupId},
        {progress, 0}
    ]),

    BeforeKVWriteFun = fun({DocId, _} = KV, #acc{last_id = LastDocId} = Acc) ->
        if DocId =:= LastDocId -> % COUCHDB-999
            Msg = "Duplicates of ~s detected in ~s ~s - rebuild required",
            exit(io_lib:format(Msg, [DocId, DbName1, GroupId]));
        true -> ok end,
        {KV, update_task(Acc, 1)}
    end,
    % First copy the id btree.
    {ok, NewIdBtreeRoot, Acc1} = couch_btree_copy:copy(IdBtree, Fd,
        [{before_kv_write, {BeforeKVWriteFun, Acc0}}]),
    NewIdBtree = EmptyIdBtree#btree{root = NewIdBtreeRoot},

    {NewViews, _} = lists:mapfoldl(fun({View, EmptyView}, Acc) ->
        compact_view(Fd, View, EmptyView, Acc)
    end, Acc1, lists:zip(Views, EmptyViews)),

    NewGroup = EmptyGroup#group{
        id_btree=NewIdBtree,
        views=NewViews,
        current_seq=Seq
    },
    maybe_retry_compact(Db, GroupId, NewGroup).

maybe_retry_compact(#db{name = DbName} = Db, GroupId, NewGroup) ->
    Pid = couch_view:get_group_server(DbName, GroupId),
    case gen_server:call(Pid, {compact_done, NewGroup}) of
    ok ->
        couch_db:close(Db);
    update ->
        {ok, Db2} = couch_db:reopen(Db),
        {_, Ref} = erlang:spawn_monitor(fun() ->
            couch_view_updater:update(nil, NewGroup, Db2)
        end),
        receive
        {'DOWN', Ref, _, _, {new_group, NewGroup2}} ->
            maybe_retry_compact(Db2, GroupId, NewGroup2)
        end
    end.

%% @spec compact_view(Fd, View, EmptyView, Acc) -> {CompactView, NewAcc}
compact_view(Fd, View, #view{btree=ViewBtree}=EmptyView, Acc0) ->
    BeforeKVWriteFun = fun(Item, Acc) ->
        {Item, update_task(Acc, 1)}
    end,

    % Copy each view btree.
    {ok, NewBtreeRoot, Acc2} = couch_btree_copy:copy(View#view.btree, Fd,
        [{before_kv_write, {BeforeKVWriteFun, Acc0}}]),
    ViewBtree2 = ViewBtree#btree{root = NewBtreeRoot},
    {EmptyView#view{btree = ViewBtree2}, Acc2}.

update_task(#acc{changes = Changes, total_changes = Total} = Acc, ChangesInc) ->
    Changes2 = Changes + ChangesInc,
    couch_task_status:update([{progress, (Changes2 * 100) div Total}]),
    Acc#acc{changes = Changes2}.
