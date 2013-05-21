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

-export([start_compact/3, start_compact/4, start_compact/6, cancel_compact/5]).
-export([merge_files/4]).

-define(SORTED_CHUNK_SIZE, 1024 * 1024).

-record(acc, {
   changes = 0,
   total_changes
}).

-spec start_compact(atom(), binary(), binary()) -> {'ok', pid()} |
                                                   {'error', 'initial_build'}.
start_compact(Mod, SetName, DDocId) ->
    start_compact(Mod, SetName, DDocId, main).

-spec start_compact(atom(), binary(), binary(), set_view_group_type()) ->
                           {'ok', pid()} |
                           {'error', 'initial_build'}.
start_compact(Mod, SetName, DDocId, Type) ->
    start_compact(Mod, SetName, DDocId, Type, prod, []).

-spec start_compact(atom(), binary(), binary(),
                    set_view_group_type(), 'prod' | 'dev', list()) ->
                           {'ok', pid()} | {'error', 'initial_build'}.
start_compact(Mod, SetName, DDocId, Type, Category, UserTaskStatus) ->
    {ok, Pid} = get_group_pid(Mod, SetName, DDocId, Type, Category),
    gen_server:call(Pid, {start_compact, mk_compact_group(UserTaskStatus)}, infinity).



-spec cancel_compact(atom(), binary(), binary(), set_view_group_type(),
                     'prod' | 'dev') -> 'ok'.
cancel_compact(Mod, SetName, DDocId, Type, Category) ->
    {ok, Pid} = get_group_pid(Mod, SetName, DDocId, Type, Category),
    gen_server:call(Pid, cancel_compact).


%%=============================================================================
%% internal functions
%%=============================================================================

-spec mk_compact_group(list()) -> CompactGroupFun
  when CompactGroupFun :: fun((#set_view_group{},
                               #set_view_group{},
                               string(),
                               pid() | 'nil',
                               pid()) -> no_return()).
mk_compact_group(UserStatus) ->
    fun(Group, EmptyGroup, TmpDir, UpdaterPid, Owner) ->
        compact_group(Group, EmptyGroup, TmpDir, UpdaterPid, Owner, UserStatus)
    end.

-spec compact_group(#set_view_group{},
                    #set_view_group{},
                    string(),
                    pid() | 'nil',
                    pid(),
                    list()) -> no_return().
compact_group(Group0, EmptyGroup, TmpDir, UpdaterPid, Owner, UserStatus) ->
    #set_view_group{
        set_name = SetName,
        type = Type,
        mod = Mod
    } = Group0,
    StartTime = os:timestamp(),

    case is_pid(UpdaterPid) of
    true ->
        MonRef = erlang:monitor(process, UpdaterPid),
        Ref = make_ref(),
        UpdaterPid ! {compactor_started, self(), Ref},
        Group = receive
        {Ref, {ok, Group2}} ->
            erlang:demonitor(MonRef, [flush]),
            Group2;
        {'DOWN', MonRef, _, _, {updater_finished, UpResult}} ->
            UpResult#set_view_updater_result.group;
        {'DOWN', MonRef, _, _, noproc} ->
            % updater just finished
            {ok, Group2} = gen_server:call(Owner, request_group, infinity),
            Group2;
        {'DOWN', MonRef, _, _, Reason2} ->
            exit({updater_died, Reason2})
        end;
    false ->
        Group = Group0
    end,

    #set_view_group{
        id_btree = IdBtree,
        views = Views,
        index_header = Header,
        sig = GroupSig
    } = Group,

    #set_view_group{
        id_btree = EmptyIdBtree,
        views = EmptyViews,
        fd = Fd
    } = EmptyGroup,

    TotalChanges = total_kv_count(Group),
    Acc0 = #acc{total_changes = TotalChanges},

    DDocIds = couch_set_view_util:get_ddoc_ids_with_sig(SetName, Group),

    Status = merge_statuses(UserStatus, [
        {type, view_compaction},
        {set, SetName},
        {signature, ?l2b(couch_util:to_hex(GroupSig))},
        {design_documents, DDocIds},
        {changes_done, 0},
        {total_changes, TotalChanges},
        {indexer_type, Type},
        {progress, case TotalChanges of 0 -> 100; _ -> 0 end}
    ]),

    couch_task_status:add_task(Status),
    couch_task_status:set_update_frequency(5000),

    ok = couch_set_view_util:open_raw_read_fd(Group),

    BeforeKVWriteFun = fun(KV, Acc) ->
        {KV, update_task(Acc, 1)}
    end,

    FilterFun = case ?set_cbitmask(Group) of
    0 ->
        fun(_Kv) -> true end;
    _ ->
        fun({_Key, <<PartId:16, _/binary>>}) ->
            ((1 bsl PartId) band ?set_cbitmask(Group)) =:= 0
        end
    end,

    {ok, NewIdBtreeRoot, Acc1} = couch_btree_copy:copy(
        IdBtree, Fd,
        [{before_kv_write, {BeforeKVWriteFun, Acc0}}, {filter, FilterFun}]),
    NewIdBtree = EmptyIdBtree#btree{root = NewIdBtreeRoot},

    {NewViews, _} = lists:mapfoldl(fun({View, EmptyView}, Acc) ->
        Mod:compact_view(Fd, View, EmptyView, FilterFun, BeforeKVWriteFun, Acc)
    end, Acc1, lists:zip(Views, EmptyViews)),

    ok = couch_set_view_util:close_raw_read_fd(Group),

    NewGroup = EmptyGroup#set_view_group{
        id_btree = NewIdBtree,
        views = NewViews,
        index_header = Header#set_view_index_header{
            cbitmask = 0,
            id_btree_state = couch_btree:get_state(NewIdBtree),
            view_states = [Mod:get_state(V#set_view.indexer) || V <- NewViews]
        }
    },

    CleanupKVCount = TotalChanges - total_kv_count(NewGroup),
    CompactResult = #set_view_compactor_result{
        group = NewGroup,
        cleanup_kv_count = CleanupKVCount
    },
    maybe_retry_compact(CompactResult, StartTime, TmpDir, Owner, 1).

merge_statuses(UserStatus, OurStatus) ->
    UserStatus0 =
        lists:filter(
            fun ({Key, _}) ->
                not lists:keymember(Key, 1, OurStatus)
            end, UserStatus),
    UserStatus0 ++ OurStatus.

maybe_retry_compact(CompactResult0, StartTime, TmpDir, Owner, Retries) ->
    NewGroup = CompactResult0#set_view_compactor_result.group,
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = Type,
        fd = Fd
    } = NewGroup,
    CompactResult = CompactResult0#set_view_compactor_result{
        compact_time = timer:now_diff(os:timestamp(), StartTime) / 1000000
    },
    % For compaction retry testing purposes
    receive
    pause ->
        receive unpause -> ok end
    after 0 ->
        ok
    end,
    ok = couch_file:flush(Fd),
    case gen_server:call(Owner, {compact_done, CompactResult}, infinity) of
    ok ->
        ok;
    {update, MissingCount} ->
        {ok, {LogFiles, NewSeqs}} = gen_server:call(
            Owner, compact_log_files, infinity),
        ?LOG_INFO("Compactor for set view `~s`, ~s group `~s`, "
                  "applying delta of ~p changes (retry number ~p)",
                  [SetName, Type, DDocId, MissingCount, Retries]),
        [TotalChanges] = couch_task_status:get([total_changes]),
        TotalChanges2 = TotalChanges + MissingCount,
        couch_task_status:update([
            {total_changes, TotalChanges2},
            {changes_done, TotalChanges},
            {progress, (TotalChanges * 100) div TotalChanges2},
            {retry_number, Retries}
        ]),
        ok = couch_set_view_util:open_raw_read_fd(NewGroup),
        NewGroup2 = apply_log(NewGroup, LogFiles, NewSeqs, TmpDir),
        ok = couch_set_view_util:close_raw_read_fd(NewGroup),
        CompactResult2 = CompactResult0#set_view_compactor_result{
            group = NewGroup2
        },
        maybe_retry_compact(CompactResult2, StartTime, TmpDir, Owner, Retries + 1)
    end.


get_group_pid(Mod, SetName, DDocId, main, Category) ->
    Pid = couch_set_view:get_group_pid(Mod, SetName, DDocId, Category),
    {ok, Pid};
get_group_pid(Mod, SetName, DDocId, replica, Category) ->
    Pid = couch_set_view:get_group_pid(Mod, SetName, DDocId, Category),
    {ok, Group} = gen_server:call(Pid, request_group, infinity),
    case is_pid(Group#set_view_group.replica_pid) of
    true ->
        {ok, Group#set_view_group.replica_pid};
    false ->
        no_replica_group_found
    end.


update_task(#acc{total_changes = 0} = Acc, _ChangesInc) ->
    Acc;
update_task(#acc{changes = Changes, total_changes = Total} = Acc, ChangesInc) ->
    Changes2 = Changes + ChangesInc,
    case (Changes2 rem 10000) == 0 of
    true ->
        couch_task_status:update([
            {changes_done, Changes2},
            {progress, (Changes2 * 100) div Total}
        ]);
    false ->
        ok
    end,
    Acc#acc{changes = Changes2}.


total_kv_count(#set_view_group{id_btree = IdBtree, views = Views, mod = Mod}) ->
    {ok, <<IdCount:40, _/binary>>} = couch_btree:full_reduce(IdBtree),
    lists:foldl(
        fun(View, Acc) ->
            Acc + Mod:get_row_count(View)
        end,
        IdCount, Views).


apply_log(Group, LogFiles, NewSeqs, TmpDir) ->
    #set_view_group{
        id_btree = IdBtree,
        index_header = Header,
        mod = Mod
    } = Group,

    [IdLogFiles | ViewLogFiles] = LogFiles,
    IdMergeFile = merge_files(IdLogFiles, TmpDir, Group, "i"),
    {ok, NewIdBtree, _, _} = couch_set_view_updater_helper:update_btree(
        IdBtree, IdMergeFile, ?SORTED_CHUNK_SIZE),
    ok = file2:delete(IdMergeFile),

    NewViews = Mod:apply_log(Group, ViewLogFiles, TmpDir),

    Group#set_view_group{
        id_btree = NewIdBtree,
        views = NewViews,
        index_header = Header#set_view_index_header{
            seqs = NewSeqs,
            id_btree_state = couch_btree:get_state(NewIdBtree),
            view_states = [Mod:get_state(V#set_view.indexer) || V <- NewViews]
        }
    }.


merge_files([F], _TmpDir, _Group, _ViewFileType) ->
    F;
merge_files(Files, TmpDir, Group, ViewFileType) ->
    NewFile = couch_set_view_util:new_sort_file_path(TmpDir, compactor),
    FileMergerCmd = case os:find_executable("couch_view_file_merger") of
    false ->
        throw(<<"couch_view_file_merger command not found">>);
    Cmd ->
        Cmd
    end,
    PortOpts = [exit_status, use_stdio, stderr_to_stdout, {line, 4096}, binary],
    Merger = open_port({spawn_executable, FileMergerCmd}, PortOpts),
    MergerInput = [
        ViewFileType, $\n,
        integer_to_list(length(Files)), $\n,
        string:join(Files, "\n"), $\n,
        NewFile, $\n
    ],
    true = port_command(Merger, MergerInput),
    try
        file_merger_wait_loop(Merger, Group, []),
        NewFile
    after
        catch port_close(Merger)
    end.


file_merger_wait_loop(Port, Group, Acc) ->
    receive
    {Port, {exit_status, 0}} ->
        ok;
    {Port, {exit_status, Status}} ->
        throw({file_merger_exit, Status});
    {Port, {data, {noeol, Data}}} ->
        file_merger_wait_loop(Port, Group, [Data | Acc]);
    {Port, {data, {eol, Data}}} ->
        #set_view_group{
            set_name = SetName,
            name = DDocId,
            type = Type
        } = Group,
        Msg = lists:reverse([Data | Acc]),
        ?LOG_ERROR("Set view `~s`, ~s group `~s`, received error from file merger: ~s",
                   [SetName, Type, DDocId, Msg]),
        file_merger_wait_loop(Port, Group, []);
    {Port, Error} ->
        throw({file_merger_error, Error})
    end.
