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
        id_btree = #btree{extract_kv = Extract} = IdBtree,
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
    {ok, DbReduce} = couch_btree:full_reduce(Db#db.fulldocinfo_by_id_btree),
    Count = element(1, DbReduce),

    <<"_design", ShortName/binary>> = GroupId,
    TaskName = <<DbName1/binary, ShortName/binary>>,
    couch_task_status:add_task(<<"View Group Compaction">>, TaskName, <<"">>),

    BeforeKVWriteFun = fun(Item, {TotalCopied, LastDocId}) ->
        {DocId, _ViewIdKeys} = Extract(Item),
        if DocId =:= LastDocId -> % COUCHDB-999
            Msg = "Duplicates of ~s detected in ~s ~s - rebuild required",
            exit(io_lib:format(Msg, [DocId, DbName1, GroupId]));
        true -> ok end,
        couch_task_status:update("Copied ~p of ~p ids (~p%)",
            [TotalCopied, Count, (TotalCopied*100) div Count]),
        {Item, {TotalCopied + 1, DocId}}
    end,
    % First copy the id btree.
    {ok, NewIdBtreeRoot} = couch_btree_copy:copy(IdBtree, Fd,
        [{before_kv_write, {BeforeKVWriteFun, {0, nil}}}]),
    NewIdBtree = EmptyIdBtree#btree{root=NewIdBtreeRoot},

    NewViews = lists:map(fun({View, EmptyView}) ->
        compact_view(Fd, View, EmptyView)
    end, lists:zip(Views, EmptyViews)),

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

%% @spec compact_view(Fd, View, EmptyView, Retry) -> CompactView
compact_view(Fd, View, #view{btree=ViewBtree}=EmptyView) ->
    {ok, Count} = couch_view:get_row_count(View),

    BeforeKVWriteFun = fun(Item, TotalCopied) ->
        couch_task_status:update("View #~p: copied ~p of ~p kvs (~p%)",
            [View#view.id_num, TotalCopied, Count,
             (TotalCopied*100) div Count]),
        {Item, TotalCopied + 1}
    end,

    % Copy each view btree.
    {ok, NewBtreeRoot} = couch_btree_copy:copy(View#view.btree, Fd,
        [{before_kv_write, {BeforeKVWriteFun, 0}}]),
    ViewBtree2 = ViewBtree#btree{root=NewBtreeRoot},
    EmptyView#view{btree=ViewBtree2}.
