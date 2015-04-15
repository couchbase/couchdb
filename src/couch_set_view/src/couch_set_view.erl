% -*- Mode: Erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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

-module(couch_set_view).
-behaviour(gen_server).

% public API
-export([start_link/2]).
-export([define_group/4]).
-export([cleanup_index_files/2, set_index_dir/3]).
-export([get_group_data_size/3, get_group_signature/3]).
-export([reset_utilization_stats/3, get_utilization_stats/3]).
-export([set_partition_states/6, add_replica_partitions/4, remove_replica_partitions/4]).
-export([mark_partitions_unindexable/4, mark_partitions_indexable/4]).
-export([monitor_partition_update/4, demonitor_partition_update/4]).
-export([trigger_update/4, trigger_replica_update/4]).
% Exported for ns_server
-export([delete_index_dir/2]).
-export([get_indexed_seqs/4]).

% Internal, not meant to be used by components other than the view engine.
-export([get_group_pid/4, get_group/4, release_group/1, get_group_info/4]).
-export([get_map_view/4, get_reduce_view/4]).
-export([fold/5, fold_reduce/5]).
-export([get_row_count/2, reduce_to_count/1, extract_map_view/1]).
-export([map_view_key_compare/2, reduce_view_key_compare/2]).
-export([get_map_view0/2, get_reduce_view0/2]).
-export([inc_group_access_stat/1]).

% Exported for spatial index
-export([modify_bitmasks/2]).

% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

-include("couch_db.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").
-include_lib("couch_index_merger/include/couch_view_merger.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").


-record(server, {
    root_dir = [],
    db_notifier,
    stats_ets       :: atom(),
    name_to_sig_ets :: atom(),
    sig_to_pid_ets  :: atom(),
    pid_to_sig_ets  :: atom(),
    name            :: atom(),
    indexer         :: mapreduce_view | spatial_view
}).


% For a "set view" we have multiple databases which are indexed.
% The set has a name which is a prefix common to all source databases.
% Each database is designated as a "partition" and internally identified
% with an integer between 0 and N - 1 (N is total number of partitions).
% For example, if the set name is "myset", and the number of partitions
% is 4, then the "set view" indexer will index the following 4 databases:
%
%    "myset/0", "myset/1", "myset/2" and "myset/3"
%
% Not all paritions are necessarily indexed, so when the set view is created,
% the caller should specify not only the set name but also:
% 1) Total number of partitions
% 2) A list of active partition IDs
%
% Once a view is created, the caller can (via other APIs):
% 1) Change the list of active partitions (add or remove)
% 2) Add several "passive" partitions - these are partitions that are
%    indexed but whose results are not included in queries
% 3) Define a list of partitions to cleanup from the index. All
%    the view key/values that originated from any of these
%    partitions will eventually be removed from the index
%
-spec get_group(atom(),
                binary(),
                binary() | #doc{},
                #set_view_group_req{}) -> {'ok', #set_view_group{}}.
get_group(Mod, SetName, DDoc, #set_view_group_req{type = main} = Req) ->
    GroupPid = get_group_pid(Mod, SetName, DDoc,
        Req#set_view_group_req.category),
    case couch_set_view_group:request_group(GroupPid, Req) of
    {ok, Group} ->
        {ok, Group};
    {error, view_undefined} ->
        % caller must call ?MODULE:define_group/3
        throw(view_undefined);
    Error ->
        throw(Error)
    end;
get_group(Mod, SetName, DDoc, #set_view_group_req{type = replica} = Req) ->
    {ok, MainGroup} = get_group(
        Mod, SetName, DDoc, Req#set_view_group_req{type = main, stale = ok}),
    release_group(MainGroup),
    case MainGroup#set_view_group.replica_pid of
    nil ->
        throw({error, <<"Requested replica group doesn't exist">>});
    ReplicaPid ->
        case couch_set_view_group:request_group(ReplicaPid, Req) of
        {ok, Group} ->
            {ok, Group};
        {error, view_undefined} ->
            % caller must call ?MODULE:define_group/3
            throw(view_undefined);
        Error ->
            throw(Error)
        end
    end.


-spec get_group_pid(atom(), binary(), binary() | #doc{}, dev | prod) -> pid().
get_group_pid(Mod, SetName, #doc{} = DDoc, Category) ->
    Group = Mod:design_doc_to_set_view_group(SetName, DDoc),
    StatsEts = Mod:stats_ets(Category),
    get_group_server(SetName, Group#set_view_group{
        category = Category,
        stats_ets = StatsEts
    });
get_group_pid(Mod, SetName, DDocId, Category) when is_binary(DDocId) ->
    get_group_server(SetName, open_set_group(Mod, SetName, DDocId, Category)).


-spec release_group(#set_view_group{}) -> ok.
release_group(Group) ->
    couch_set_view_group:release_group(Group).


-spec define_group(atom(), binary(), binary(), #set_view_params{}) -> 'ok'.
define_group(Mod, SetName, DDocId, #set_view_params{} = Params) ->
    try
        GroupPid = get_group_pid(Mod, SetName, DDocId, prod),
        case couch_set_view_group:define_view(GroupPid, Params) of
        ok ->
            ok;
        Error ->
            throw(Error)
        end
    catch
        throw:{error, empty_group} ->
            ok;
        exit:{normal, _} ->
            ?LOG_INFO("Group process normally exited. So retrying..", []),
            timer:sleep(100),
            define_group(Mod, SetName, DDocId, Params)
    end.


% This is an incremental operation. That is, the following sequence of calls:
%
% set_partitions_states(<<"myset">>, <<"_design/foo">>, [0, 1], [5], [8])
% set_partitions_states(<<"myset">>, <<"_design/foo">>, [2, 3], [6, 7], [9])
% set_partitions_states(<<"myset">>, <<"_design/foo">>, [], [], [10])
%
% Will cause the set view index to have the following state:
%
%   active partitions:   [0, 1, 2, 3]
%   passive partitions:  [5, 6, 7]
%   cleanup partitions:  [8, 9, 10]
%
% Also, to move partition(s) from one state to another, simply do a call
% where that partition(s) is listed in the new desired state. Example:
%
% set_partitions_states(<<"myset">>, <<"_design/foo">>, [0, 1, 2], [3], [4])
% set_partitions_states(<<"myset">>, <<"_design/foo">>, [], [2], [])
%
% This will result in the following set view index state:
%
%   active partitions:   [0, 1]
%   passive_partitions:  [2, 3]
%   cleanup_partitions:  [4]
%
% (partition 2 was first set to active state and then moved into the passive state)
%
% New partitions are added by specifying them for the first time in the active
% or passive state lists.
%
% If a request asks to set to active a partition that is currently marked as a
% replica partition, data from that partition will start to be transfered from
% the replica index into the main index.
%
-spec set_partition_states(atom(),
                           binary(),
                           binary(),
                           ordsets:ordset(partition_id()),
                           ordsets:ordset(partition_id()),
                           ordsets:ordset(partition_id())) -> 'ok'.
set_partition_states(Mod, SetName, DDocId, ActivePartitions, PassivePartitions, CleanupPartitions) ->
    try
        GroupPid = get_group_pid(Mod, SetName, DDocId, prod),
        case couch_set_view_group:set_state(
            GroupPid, ActivePartitions, PassivePartitions, CleanupPartitions) of
        ok ->
            ok;
        Error ->
            throw(Error)
        end
    catch throw:{error, empty_group} ->
        ok
    end.


% Mark a set of partitions as replicas. They will be indexed in the replica index.
% This will only work if the view was defined with the option "use_replica_index".
%
% All the given partitions must not be in the active nor passive state.
% Like set_partition_states, this is an incremental operation.
%
-spec add_replica_partitions(atom(), binary(), binary(), ordsets:ordset(partition_id())) -> 'ok'.
add_replica_partitions(Mod, SetName, DDocId, Partitions) ->
    try
        GroupPid = get_group_pid(Mod, SetName, DDocId, prod),
        case couch_set_view_group:add_replica_partitions(GroupPid, Partitions) of
        ok ->
            ok;
        Error ->
            throw(Error)
        end
    catch throw:{error, empty_group} ->
        ok
    end.


% Unmark a set of partitions as replicas. Their data will be cleaned from the
% replica index. This will only work if the view was defined with the option
% "use_replica_index".
%
% This is a no-op for partitions not currently marked as replicas.
% Like set_partition_states, this is an incremental operation.
%
-spec remove_replica_partitions(atom(), binary(), binary(), ordsets:ordset(partition_id())) -> 'ok'.
remove_replica_partitions(Mod, SetName, DDocId, Partitions) ->
    try
        GroupPid = get_group_pid(Mod, SetName, DDocId, prod),
        case couch_set_view_group:remove_replica_partitions(GroupPid, Partitions) of
        ok ->
            ok;
        Error ->
            throw(Error)
        end
    catch throw:{error, empty_group} ->
        ok
    end.


% Mark a set of partitions, currently either in the active or passive states, as
% unindexable. This means future index updates will ignore new changes found in the
% corresponding partition databases. This operation doesn't remove any data from
% the index, nor does it start any cleanup operation. Queries will still see
% and get data from the corresponding partitions.
-spec mark_partitions_unindexable(atom(), binary(), binary(), ordsets:ordset(partition_id())) -> 'ok'.
mark_partitions_unindexable(_Mod, _SetName, _DDocId, []) ->
    ok;
mark_partitions_unindexable(Mod, SetName, DDocId, Partitions) ->
    try
        Pid = get_group_pid(Mod, SetName, DDocId, prod),
        case couch_set_view_group:mark_as_unindexable(Pid, Partitions) of
        ok ->
            ok;
        Error ->
            throw(Error)
        end
    catch throw:{error, empty_group} ->
        ok
    end.


% This is the counterpart of mark_partitions_unindexable/3. It marks a set of partitions
% as indexable again, meaning future index updates will process all new partition database
% changes (changes that happened since the last index update prior to the
% mark_partitions_unindexable/3 call). The given partitions are currently in either the
% active or passive states and were marked as unindexable before.
-spec mark_partitions_indexable(atom(), binary(), binary(), ordsets:ordset(partition_id())) -> 'ok'.
mark_partitions_indexable(_Mod, _SetName, _DDocId, []) ->
    ok;
mark_partitions_indexable(Mod, SetName, DDocId, Partitions) ->
    try
        Pid = get_group_pid(Mod, SetName, DDocId, prod),
        case couch_set_view_group:mark_as_indexable(Pid, Partitions) of
        ok ->
            ok;
        Error ->
            throw(Error)
        end
    catch throw:{error, empty_group} ->
        ok
    end.


% Allow a caller to be notified, via a message, when a particular partition is
% up to date in the index (its current database sequence number matches the
% one in the index for that partition).
% When the partition is up to date, the caller will receive a message with the
% following shape:
%
%    {Ref::reference(), updated}
%
% Where the reference is the one returned when this function is called.
% If the underlying view group process dies before the partition is up to date,
% the caller will receive a message with the following shape:
%
%    {Ref::reference(), {shutdown, Reason::term()}}
%
% If the requested partition is marked for cleanup (because some process asked
% for that or the partition's database was deleted), the caller will receive a
% message with the following shape:
%
%    {Ref::reference(), marked_for_cleanup}
%
% If an error happens in the index updater, preventing the caller from ever getting
% a notification, the caller will receive a message with the following shape:
%
%    {Ref::reference(), {updater_error, Reason::term()}}
%
% The target partition must be either an active or passive partition.
% Replica partitions are not supported at the moment.
-spec monitor_partition_update(atom(), binary(), binary(), partition_id()) -> reference().
monitor_partition_update(Mod, SetName, DDocId, PartitionId) ->
    Ref = make_ref(),
    try
        Pid = get_group_pid(Mod, SetName, DDocId, prod),
        case couch_set_view_group:monitor_partition_update(Pid, PartitionId, Ref, self()) of
        ok ->
            Ref;
        Error ->
            throw(Error)
        end
    catch throw:{error, empty_group} ->
        self() ! {Ref, updated},
        Ref
    end.


% Stop monitoring for notification of when a partition is fully indexed.
% This is a counter part to monitor_partition_update/3. This call flushes
% any monitor messsages from the callers mailbox.
-spec demonitor_partition_update(atom(), binary(), binary(), reference()) -> 'ok'.
demonitor_partition_update(Mod, SetName, DDocId, Ref) ->
    receive
    {Ref, _} ->
        ok
    after 0 ->
        try
            Pid = get_group_pid(Mod, SetName, DDocId, prod),
            ok = couch_set_view_group:demonitor_partition_update(Pid, Ref),
            receive
            {Ref, _} ->
                ok
            after 0 ->
                ok
            end
        catch throw:{error, empty_group} ->
            ok
        end
    end.


% Trigger a view group index update if there are at least N new changes
% (from all the active/passive partitions) to index.
-spec trigger_update(atom(), binary(), binary(), non_neg_integer()) -> ok.
trigger_update(Mod, SetName, DDocId, MinNumChanges) ->
    try
        Pid = get_group_pid(Mod, SetName, DDocId, prod),
        ok = gen_server:cast(Pid, {update, MinNumChanges})
    catch throw:{error, empty_group} ->
        ok
    end.


% Trigger a replica view group index update if there are at least N new
% changes (from all the currently defined replica partitions) to index.
-spec trigger_replica_update(atom(), binary(), binary(), non_neg_integer()) -> ok.
trigger_replica_update(Mod, SetName, DDocId, MinNumChanges) ->
    try
        Pid = get_group_pid(Mod, SetName, DDocId, prod),
        ok = gen_server:cast(Pid, {update_replica, MinNumChanges})
    catch throw:{error, empty_group} ->
        ok
    end.


-spec get_indexed_seqs(atom(), binary(), binary(), dev | prod) ->
                              {ok, PartSeqs::partition_seqs()}.
get_indexed_seqs(Mod, SetName, DDocId, Category) ->
    Pid = couch_set_view:get_group_pid(Mod, SetName, DDocId, Category),
    {ok, Group} = gen_server:call(Pid, request_group, infinity),
    {ok, RepPid} = gen_server:call(Pid, replica_pid, infinity),
    MainSeqs = ordsets:union(?set_seqs(Group), ?set_unindexable_seqs(Group)),
    case is_pid(RepPid) of
    false ->
        {ok, MainSeqs};
    true ->
        {ok, RepGroup} = gen_server:call(RepPid, request_group, infinity),
        RepSeqs0 = ordsets:union(?set_seqs(RepGroup),
                                 ?set_unindexable_seqs(RepGroup)),
        RepSeqs = case ?set_replicas_on_transfer(Group) of
        [] ->
            RepSeqs0;
        OnTransfer ->
            [{P, S} || {P, S} <- RepSeqs0, not lists:member(P, OnTransfer)]
        end,
        {ok, ordsets:union(MainSeqs, RepSeqs)}
    end.


-spec get_group_server(binary(), #set_view_group{}) -> pid().
get_group_server(_SetName, #set_view_group{views = []}) ->
    throw({error, empty_group});
get_group_server(SetName, Group) ->
    #set_view_group{
        sig = Sig,
        category = Category,
        mod = Mod
    } = Group,
    ServerName = Mod:server_name(Category),
    SigToPidEts = Mod:sig_to_pid_ets(Category),
    case ets:lookup(SigToPidEts, {SetName, Sig}) of
    [{_, Pid}] when is_pid(Pid) ->
        Pid;
    _ ->
        case gen_server:call(ServerName, {get_group_server, SetName, Group},
                infinity) of
        {ok, Pid} ->
            Pid;
        Error ->
            throw(Error)
        end
    end.


-spec open_set_group(atom(), binary(), binary(), dev | prod) ->
                            #set_view_group{}.
open_set_group(Mod, SetName, GroupId, Category) ->
    case couch_set_view_group:open_set_group(Mod, SetName, GroupId) of
    {ok, Group} ->
        StatsEts = Mod:stats_ets(Category),
        Group#set_view_group{
            category = Category,
            stats_ets = StatsEts
        };
    Error ->
        throw(Error)
    end.

-spec start_link(dev | prod, mapreduce_view | spatial_view) ->
                        {ok, pid()} | ignore |
                        {error, {already_started, pid()} | term()}.
start_link(Category, Indexer) ->
    ServerName = Indexer:server_name(Category),
    gen_server:start_link({local, ServerName}, ?MODULE,
        {Category, Indexer}, []).


% To be used only for debugging. This is a very expensive call.
get_group_info(Mod, SetName, DDocId, Category) ->
    GroupPid = get_group_pid(Mod, SetName, DDocId, Category),
    {ok, _Info} = couch_set_view_group:request_group_info(GroupPid).


get_group_data_size(Mod, SetName, DDocId) ->
    try
        GroupPid = get_group_pid(Mod, SetName, DDocId, prod),
        {ok, _Info} = couch_set_view_group:get_data_size(GroupPid)
    catch throw:{error, empty_group} ->
        {ok, Sig} = get_group_signature(Mod, SetName, DDocId),
        EmptyInfo = [
            {signature, ?b2l(Sig)},
            {disk_size, 0},
            {data_size, 0},
            {accesses, 0},
            {updater_running, false},
            {initial_build, false}
        ],
        {ok, EmptyInfo}
    end.


-spec reset_utilization_stats(atom(), binary(), binary()) -> 'ok'.
reset_utilization_stats(Mod, SetName, DDocId) ->
    GroupPid = get_group_pid(Mod, SetName, DDocId, prod),
    ok = couch_set_view_group:reset_utilization_stats(GroupPid).


-spec get_utilization_stats(atom(), binary(), binary()) ->
                                   {'ok', [{atom() | binary(), term()}]}.
get_utilization_stats(Mod, SetName, DDocId) ->
    GroupPid = get_group_pid(Mod, SetName, DDocId, prod),
    {ok, _} = couch_set_view_group:get_utilization_stats(GroupPid).


-spec get_group_signature(atom(), binary(), binary()) -> {'ok', binary()}.
get_group_signature(Mod, SetName, DDocId) ->
    case couch_set_view_ddoc_cache:get_ddoc(SetName, DDocId) of
    {ok, DDoc} ->
        Group = Mod:design_doc_to_set_view_group(SetName, DDoc),
        {ok, ?l2b(couch_util:to_hex(Group#set_view_group.sig))};
    Error ->
        throw(Error)
    end.


cleanup_index_files(Mod, SetName) ->
    % load all ddocs
    {ok, Db} = couch_db:open_int(?master_dbname(SetName), []),
    {ok, DesignDocs} = couch_db:get_design_docs(Db),
    couch_db:close(Db),

    % make unique list of group sigs and get the file
    % extension (which is the same for all groups)
    Sigs = lists:map(fun(DDoc) ->
            #set_view_group{sig = Sig} =
                Mod:design_doc_to_set_view_group(SetName, DDoc),
            couch_util:to_hex(Sig)
        end,
        [DD || DD <- DesignDocs, not DD#doc.deleted]),

    Extension = Mod:index_extension(),
    FileList = list_index_files(SetName, Extension),

    % regex that matches all ddocs
    RegExp = "("++ string:join(Sigs, "|") ++")",

    % filter out the ones in use
    DeleteFiles = case Sigs of
    [] ->
        FileList;
    _ ->
        [FilePath || FilePath <- FileList,
            re:run(FilePath, RegExp, [{capture, none}]) =:= nomatch]
    end,
    % delete unused files
    case DeleteFiles of
    [] ->
        ok;
    _ ->
        ?LOG_INFO("Deleting unused (old) set view `~s` index files:~n~n~s",
            [SetName, string:join(DeleteFiles, "\n")])
    end,
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    lists:foreach(
        fun(File) -> couch_file:delete(RootDir, File, false) end,
        DeleteFiles).

list_index_files(SetName, Extension) ->
    % call server to fetch the index files
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    Wildcard = "*" ++ Extension ++ "*",
    ProdIndexDir = filename:join(
        set_index_dir(RootDir, SetName, prod), Wildcard),
    DevIndexDir = filename:join(
        set_index_dir(RootDir, SetName, dev), Wildcard),
    filelib:wildcard(ProdIndexDir) ++ filelib:wildcard(DevIndexDir).


-spec get_row_count(#set_view_group{}, #set_view{}) -> non_neg_integer().
get_row_count(#set_view_group{replica_group = nil} = Group, View) ->
    Mod = Group#set_view_group.mod,
    Mod:get_row_count(View);
get_row_count(#set_view_group{replica_group = RepGroup} = Group, View) ->
    Mod = Group#set_view_group.mod,
    RepView = lists:nth(View#set_view.id_num + 1, RepGroup#set_view_group.views),
    CountMain = Mod:get_row_count(View),
    CountRep = Mod:get_row_count(RepView),
    CountMain + CountRep.


extract_map_view({reduce, _N, View}) ->
    View.


% This case is triggered when at least one partition of the replica group is
% active. This happens during failover, when a replica index is transferred
% to the main index
-spec fold_reduce(#set_view_group{},
                  {'reduce', non_neg_integer(), #set_view{}},
                  set_view_fold_reduce_fun(),
                  term(),
                  #view_query_args{}) -> {'ok', term()}.
fold_reduce(#set_view_group{replica_group = #set_view_group{} = RepGroup} = Group, View, FoldFun, FoldAcc, ViewQueryArgs) ->
    {reduce, NthRed, #set_view{id_num = Id}} = View,
    RepView = {reduce, NthRed, lists:nth(Id + 1, RepGroup#set_view_group.views)},
    ViewSpecs = [
        #set_view_spec{
            name = Group#set_view_group.set_name,
            ddoc_id = Group#set_view_group.name,
            view_name = ViewQueryArgs#view_query_args.view_name,
            partitions = [],  % not needed in this context
            group = Group#set_view_group{replica_group = nil},
            view = View
        },
        #set_view_spec{
            name = RepGroup#set_view_group.set_name,
            ddoc_id = RepGroup#set_view_group.name,
            view_name = ViewQueryArgs#view_query_args.view_name,
            partitions = [],  % not needed in this context
            % We want the partitions filtered like it would be a main group
            group = RepGroup#set_view_group{type = main},
            view = RepView
        }
    ],
    MergeParams = #index_merge{
        indexes = ViewSpecs,
        callback = fun couch_view_merger:reduce_view_merge_callback/2,
        user_acc = #merge_acc{fold_fun = FoldFun, acc = FoldAcc},
        user_ctx = #user_ctx{roles = [<<"_admin">>]},
        http_params = ViewQueryArgs,
        make_row_fun = fun(RowData) -> RowData end,
        extra = #view_merge{
            keys = ViewQueryArgs#view_query_args.keys
        }
    },
    #merge_acc{acc = FinalAcc} = couch_index_merger:query_index(couch_view_merger, MergeParams),
    {ok, FinalAcc};

fold_reduce(Group, View, FoldFun, FoldAcc, #view_query_args{keys = nil} = ViewQueryArgs) ->
    KeyGroupFun = make_reduce_group_keys_fun(ViewQueryArgs#view_query_args.group_level),
    Options = [{key_group_fun, KeyGroupFun} | mapreduce_view:make_key_options(ViewQueryArgs)],
    do_fold_reduce(Group, View, FoldFun, FoldAcc, Options, ViewQueryArgs);

fold_reduce(Group, View, FoldFun, FoldAcc, #view_query_args{keys = Keys} = ViewQueryArgs0) ->
    KeyGroupFun = make_reduce_group_keys_fun(ViewQueryArgs0#view_query_args.group_level),
    {_, FinalAcc} = lists:foldl(
        fun(Key, {_, Acc}) ->
            ViewQueryArgs = ViewQueryArgs0#view_query_args{start_key = Key, end_key = Key},
            Options = [{key_group_fun, KeyGroupFun} | mapreduce_view:make_key_options(ViewQueryArgs)],
            do_fold_reduce(Group, View, FoldFun, Acc, Options, ViewQueryArgs)
        end,
        {ok, FoldAcc},
        Keys),
    {ok, FinalAcc}.


do_fold_reduce(Group, ViewInfo, Fun, Acc, Options0, ViewQueryArgs) ->
    {reduce, NthRed, View} = ViewInfo,
    #mapreduce_view{
        btree = Bt,
        reduce_funs = RedFuns
    } = View#set_view.indexer,
    #view_query_args{
        filter = DoFilter,
        group_level = GroupLevel
    } = ViewQueryArgs,
    Filter = case DoFilter of
        false ->
            false;
        true ->
            filter(Group)
    end,

    Options = case Filter of
    false ->
        Options0;
    {true, ExcludeBitmask, IncludeBitmask} ->
        FilterFun = fun(value, {_K, <<PartId:16, _/binary>>}) ->
            ((1 bsl PartId) band IncludeBitmask) =/= 0;
        (branch, <<_Count:40, PartsBitmap:?MAX_NUM_PARTITIONS, _/binary>>) ->
            case PartsBitmap band ExcludeBitmask of
            0 ->
                all;
            PartsBitmap ->
                none;
            _ ->
                partial
            end
        end,
        lists:keystore(filter_fun, 1, Options0, {filter_fun, FilterFun})
    end,
    PreResultPadding = lists:duplicate(NthRed - 1, <<>>),
    PostResultPadding = lists:duplicate(length(RedFuns) - NthRed, <<>>),
    couch_set_view_mapreduce:start_reduce_context(View),
    ReduceFun =
        fun(reduce, KVs) ->
            KVs2 = couch_set_view_util:expand_dups(KVs, []),
            {ok, Reduced} = couch_set_view_mapreduce:reduce(View, NthRed, KVs2),
            Reduced2 = PreResultPadding ++ Reduced ++ PostResultPadding,
            LenReductions = [<<(size(R)):16, R/binary>> || R <- Reduced2],
            iolist_to_binary([<<0:40, 0:?MAX_NUM_PARTITIONS>> | LenReductions]);
        (rereduce, Reds) ->
            UserReds = lists:map(
                fun(<<_Count:40, _BitMap:?MAX_NUM_PARTITIONS, UserRedsList/binary>>) ->
                    [lists:nth(NthRed, couch_set_view_util:parse_reductions(UserRedsList))]
                end,
                Reds),
            {ok, Reduced} = couch_set_view_mapreduce:rereduce(View, NthRed, UserReds),
            Reduced2 = PreResultPadding ++ Reduced ++ PostResultPadding,
            LenReductions = [<<(size(R)):16, R/binary>> || R <- Reduced2],
            iolist_to_binary([<<0:40, 0:?MAX_NUM_PARTITIONS>> | LenReductions])
        end,
    WrapperFun = fun(KeyDocId, PartialReds, Acc0) ->
            GroupedKey = case GroupLevel of
            0 ->
                <<"null">>;
            _ when is_integer(GroupLevel) ->
                {KeyJson, _DocId} = mapreduce_view:decode_key_docid(KeyDocId),
                case is_array_key(KeyJson) of
                true ->
                    ?JSON_ENCODE(lists:sublist(?JSON_DECODE(KeyJson), GroupLevel));
                false ->
                    KeyJson
                end;
            _ ->
                {KeyJson, _DocId} = mapreduce_view:decode_key_docid(KeyDocId),
                KeyJson
            end,
            <<_Count:40, _BitMap:?MAX_NUM_PARTITIONS, Reds/binary>> =
                couch_btree:final_reduce(ReduceFun, PartialReds),
            UserRed = lists:nth(NthRed, couch_set_view_util:parse_reductions(Reds)),
            Fun({json, GroupedKey}, {json, UserRed}, Acc0)
        end,
    couch_set_view_util:open_raw_read_fd(Group),
    try
        couch_btree:fold_reduce(Bt, WrapperFun, Acc, Options)
    after
        couch_set_view_util:close_raw_read_fd(Group),
        couch_set_view_mapreduce:end_reduce_context(View)
    end.


get_key_pos(_Key, [], _N) ->
    0;
get_key_pos(Key, [{Key1,_Value}|_], N) when Key == Key1 ->
    N + 1;
get_key_pos(Key, [_|Rest], N) ->
    get_key_pos(Key, Rest, N+1).


get_map_view(SetName, DDoc, ViewName, Req) ->
    #set_view_group_req{wanted_partitions = WantedPartitions} = Req,
    try
        {ok, Group0} = get_group(mapreduce_view, SetName, DDoc, Req),
        {Group, Unindexed} = modify_bitmasks(Group0, WantedPartitions),
        case get_map_view0(ViewName, Group#set_view_group.views) of
        {ok, View} ->
            {ok, View, Group, Unindexed};
        Else ->
            release_group(Group0),
            Else
        end
    catch
    throw:{error, empty_group} ->
        {not_found, missing_named_view}
    end.

get_map_view0(_Name, []) ->
    {not_found, missing_named_view};
get_map_view0(Name, [#set_view{} = View|Rest]) ->
    MapNames = (View#set_view.indexer)#mapreduce_view.map_names,
    case lists:member(Name, MapNames) of
        true -> {ok, View};
        false -> get_map_view0(Name, Rest)
    end.


get_reduce_view(SetName, DDoc, ViewName, Req) ->
    #set_view_group_req{wanted_partitions = WantedPartitions} = Req,
    try
        {ok, Group0} = get_group(mapreduce_view, SetName, DDoc, Req),
        {Group, Unindexed} = modify_bitmasks(Group0, WantedPartitions),
        #set_view_group{
            views = Views
        } = Group,
        case get_reduce_view0(ViewName, Views) of
        {ok, View} ->
            {ok, View, Group, Unindexed};
        Else ->
            release_group(Group0),
            Else
        end
    catch
    throw:{error, empty_group} ->
        {not_found, missing_named_view}
    end.

get_reduce_view0(_Name, []) ->
    {not_found, missing_named_view};
get_reduce_view0(Name, [#set_view{} = View|Rest]) ->
    RedFuns = (View#set_view.indexer)#mapreduce_view.reduce_funs,
    case get_key_pos(Name, RedFuns, 0) of
        0 -> get_reduce_view0(Name, Rest);
        N -> {ok, {reduce, N, View}}
    end.


reduce_to_count(Reductions) ->
    <<Count:40, _/binary>> =
    couch_btree:final_reduce(
        fun(reduce, KVs) ->
            Count = length(couch_set_view_util:expand_dups(KVs, [])),
            <<Count:40>>;
        (rereduce, Reds) ->
            Count = lists:foldl(fun(<<C:40, _/binary>>, Acc) -> Acc + C end, 0, Reds),
            <<Count:40>>
        end, Reductions),
    Count.

-spec inc_group_access_stat(#set_view_group{}) -> 'ok'.
inc_group_access_stat(Group) ->
    GroupPid = get_group_server(Group#set_view_group.set_name, Group),
    ok = couch_set_view_group:inc_access_stat(GroupPid).

% This case is triggered when at least one partition of the replica group is
% active. This happens during failover, when a replica index is transferred
% to the main index
% The "ors" in the spec are for the spatial views
-spec fold(#set_view_group{},
           #set_view{},
           set_view_fold_fun() | fun((tuple(), term()) -> {ok, term()}),
           term(),
           #view_query_args{} | tuple()) -> {'ok', term(), term()}.
fold(#set_view_group{replica_group = #set_view_group{} = RepGroup} = Group, View, Fun, Acc, ViewQueryArgs) ->
    RepView = lists:nth(View#set_view.id_num + 1, RepGroup#set_view_group.views),
    case ViewQueryArgs of
    #view_query_args{keys = Keys} ->
        Extra = #view_merge{keys = Keys},
        Merger = couch_view_merger;
    _ ->
        Extra = nil,
        Merger = spatial_merger
    end,
    Mod = Group#set_view_group.mod,
    ViewName = Mod:query_args_view_name(ViewQueryArgs),
    ViewSpecs = [
        #set_view_spec{
            name = Group#set_view_group.set_name,
            ddoc_id = Group#set_view_group.name,
            view_name = ViewName,
            partitions = [],  % not needed in this context
            group = Group#set_view_group{replica_group = nil},
            view = View
        },
        #set_view_spec{
            name = RepGroup#set_view_group.set_name,
            ddoc_id = RepGroup#set_view_group.name,
            view_name = ViewName,
            partitions = [],  % not needed in this context
            % We want the partitions filtered like it would be a main group
            group = RepGroup#set_view_group{type = main},
            view = RepView
        }
    ],
    MergeParams = #index_merge{
        indexes = ViewSpecs,
        callback = fun Merger:map_view_merge_callback/2,
        user_acc = #merge_acc{fold_fun = Fun, acc = Acc},
        user_ctx = #user_ctx{roles = [<<"_admin">>]},
        http_params = ViewQueryArgs,
        make_row_fun = fun(RowData) -> RowData end,
        extra = Extra
    },
    #merge_acc{acc = FinalAcc} = couch_index_merger:query_index(
        Merger, MergeParams),
    {ok, nil, FinalAcc};

fold(Group, View, Fun, Acc, #view_query_args{keys = Keys} = ViewQueryArgs0)
        when Keys =/= nil ->
    lists:foldl(
        fun(Key, {ok, _, FoldAcc}) ->
            ViewQueryArgs = ViewQueryArgs0#view_query_args{start_key = Key, end_key = Key},
            do_fold(Group, View, Fun, FoldAcc, ViewQueryArgs)
        end,
        {ok, {[], []}, Acc},
        Keys);
fold(Group, View, Fun, Acc, ViewQueryArgs) ->
    do_fold(Group, View, Fun, Acc, ViewQueryArgs).


do_fold(Group, SetView, Fun, Acc, ViewQueryArgs) ->
    View = SetView#set_view.indexer,
    Mod = Group#set_view_group.mod,

    Filter = case Mod:should_filter(ViewQueryArgs) of
        false ->
            false;
        true ->
            filter(Group)
    end,

    WrapperFun = Mod:make_wrapper_fun(Fun, Filter),
    couch_set_view_util:open_raw_read_fd(Group),
    try
        Options = Mod:make_key_options(ViewQueryArgs),
        {ok, _LastReduce, _AccResult} =
            Mod:fold(View, WrapperFun, Acc, Options)
    after
        couch_set_view_util:close_raw_read_fd(Group)
    end.


-spec init({prod | dev, mapreduce_view | spatial_view}) -> {ok, #server{}}.
init({Category, Indexer}) ->
    % read configuration settings and register for configuration changes
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    ok = couch_config:register(
        fun("mapreduce", "function_timeout", NewTimeout) ->
                ok = mapreduce:set_timeout(list_to_integer(NewTimeout));
            ("mapreduce", "max_kv_size_per_doc", NewMax) ->
                ok = mapreduce:set_max_kv_size_per_doc(list_to_integer(NewMax))
        end),

    ok = mapreduce:set_timeout(list_to_integer(
        couch_config:get("mapreduce", "function_timeout", "10000"))),

    Server = init_server(Category, Indexer),
    % {SetName, {DDocId, Signature}}
    ets:new(Server#server.name_to_sig_ets,
            [bag, protected, named_table, {read_concurrency, true}]),
    % {{SetName, Signature}, Pid | WaitListPids}
    ets:new(Server#server.sig_to_pid_ets,
            [set, protected, named_table, {read_concurrency, true}]),
    % {Pid, {SetName, Sig, DDocId}}
    ets:new(Server#server.pid_to_sig_ets, [set, private, named_table]),

    ets:new(Server#server.stats_ets,
        [set, public, named_table, {keypos, #set_view_group_stats.ets_key}]),

    {ok, Notifier} = couch_db_update_notifier:start_link(
        make_handle_db_event_fun(
            Indexer, Server#server.name, Server#server.sig_to_pid_ets,
            Server#server.name_to_sig_ets)),

    process_flag(trap_exit, true),
    ok = couch_file:init_delete_dir(RootDir),
    {ok, Server#server{root_dir = RootDir, db_notifier = Notifier}}.

init_server(Category, Indexer) ->
    #server{
        stats_ets = Indexer:stats_ets(Category),
        name_to_sig_ets = Indexer:name_to_sig_ets(Category),
        sig_to_pid_ets = Indexer:sig_to_pid_ets(Category),
        pid_to_sig_ets = Indexer:pid_to_sig_ets(Category),
        name = Indexer:server_name(Category),
        indexer = Indexer
    }.


terminate(_Reason, Server) ->
    [couch_util:shutdown_sync(Pid) || {Pid, _} <-
            ets:tab2list(Server#server.pid_to_sig_ets)],
    ok.


handle_call({get_group_server, SetName, Group}, From, Server) ->
    #set_view_group{sig = Sig} = Group,
    case ets:lookup(Server#server.sig_to_pid_ets, {SetName, Sig}) of
    [] ->
        WaitList = [From],
        _ = spawn_monitor(fun() ->
            exit(new_group(Server#server.root_dir, SetName, Group))
        end),
        ets:insert(Server#server.sig_to_pid_ets, {{SetName, Sig}, WaitList}),
        {noreply, Server};
    [{_, WaitList}] when is_list(WaitList) ->
        WaitList2 = [From | WaitList],
        ets:insert(Server#server.sig_to_pid_ets, {{SetName, Sig}, WaitList2}),
        {noreply, Server};
    [{_, ExistingPid}] ->
        {reply, {ok, ExistingPid}, Server}
    end;

handle_call({before_database_delete, SetName}, _From, Server) ->
    #server{root_dir = RootDir} = Server,
    lists:foreach(
        fun({_SetName, {_DDocId, Sig}}) ->
            case ets:lookup(Server#server.sig_to_pid_ets, {SetName, Sig}) of
            [{_, Pid}] when is_pid(Pid) ->
                gen_server:cast(Pid, before_master_delete);
            _ ->
                ok
            end
        end,
        ets:lookup(Server#server.name_to_sig_ets, SetName)),
    true = ets:delete(Server#server.name_to_sig_ets, SetName),
    ?LOG_INFO("Deleting index files for set `~s` because master database "
              "is about to deleted", [SetName]),
    try
        delete_index_dir(RootDir, SetName)
    catch _:Error ->
        Stack = erlang:get_stacktrace(),
        ?LOG_ERROR("Error deleting index files for set `~s`:~n"
                   "  error: ~p~n  stacktrace: ~p~n",
                   [SetName, Error, Stack])
    end,
    {reply, ok, Server};

handle_call({ddoc_updated, SetName, #doc{deleted = false} = DDoc0}, _From, Server) ->
    #doc{id = DDocId} = DDoc = couch_doc:with_ejson_body(DDoc0),
    Indexer = Server#server.indexer,
    #set_view_group{sig = Sig} = Indexer:design_doc_to_set_view_group(SetName, DDoc),
    true = ets:insert(Server#server.name_to_sig_ets, {SetName, {DDocId, Sig}}),
    {reply, ok, Server};

handle_call({ddoc_updated, SetName, #doc{id = DDocId, deleted = true}}, _From, Server) ->
    true = ets:match_delete(Server#server.name_to_sig_ets, {SetName, {DDocId, '$1'}}),
    {reply, ok, Server}.


handle_cast(Msg, Server) ->
    {stop, {unexpected_cast, Msg}, Server}.

new_group(Root, SetName, Group) ->
    #set_view_group{
        name = DDocId,
        sig = Sig,
        mod = Mod
    } = Group,
    process_flag(trap_exit, true),
    Reply = case (catch couch_set_view_group:start_link({Root, SetName, Group})) of
    {ok, NewPid} ->
        Aliases = get_ddoc_ids_with_sig(Mod, SetName, Sig),
        unlink(NewPid),
        {ok, NewPid, Aliases};
    {error, Reason} ->
        Reason;
    Error ->
        Error
    end,
    {SetName, DDocId, Sig, Reply}.

handle_info({'EXIT', Pid, Reason}, #server{db_notifier = Pid} = Server) ->
    ?LOG_ERROR("Database update notifer died with reason: ~p", [Reason]),
    {stop, Reason, Server};

handle_info({'EXIT', FromPid, Reason}, Server) ->
    case ets:lookup(Server#server.pid_to_sig_ets, FromPid) of
    [] ->
        if Reason /= normal ->
            % non-updater linked process died, we propagate the error
            ?LOG_ERROR("Exit on non-updater process: ~p", [Reason]),
            exit(Reason);
        true -> ok
        end;
    [{_, {SetName, Sig, DDocId}}] ->
        delete_from_ets(FromPid, SetName, DDocId, Sig, Server),
        true = ets:match_delete(Server#server.name_to_sig_ets, {SetName, {'$1', Sig}})
    end,
    {noreply, Server};

handle_info({'DOWN', _MonRef, _, _Pid, {SetName, DDocId, Sig, Reply}}, Server) ->
    Key = {SetName, Sig},
    [{_, WaitList}] = ets:lookup(Server#server.sig_to_pid_ets, Key),
    case Reply of
    {ok, NewPid, Aliases} ->
        lists:foreach(fun(From) -> gen_server:reply(From, {ok, NewPid}) end, WaitList),
        true = link(NewPid),
        add_to_ets(NewPid, SetName, DDocId, Sig, Server),
        lists:foreach(fun(AliasDDocId) ->
            true = ets:insert(Server#server.name_to_sig_ets, {SetName, {AliasDDocId, Sig}})
        end, Aliases);
    _ ->
        lists:foreach(fun(From) -> gen_server:reply(From, Reply) end, WaitList),
        ets:delete(Server#server.sig_to_pid_ets, Key)
    end,
    {noreply, Server}.

add_to_ets(Pid, SetName, DDocId, Sig, Server) ->
    true = ets:insert(Server#server.pid_to_sig_ets, {Pid, {SetName, Sig, DDocId}}),
    true = ets:insert(Server#server.sig_to_pid_ets, {{SetName, Sig}, Pid}),
    true = ets:insert(Server#server.name_to_sig_ets, {SetName, {DDocId, Sig}}).

delete_from_ets(Pid, SetName, DDocId, Sig, Server) ->
    true = ets:delete(Server#server.pid_to_sig_ets, Pid),
    true = ets:delete(Server#server.sig_to_pid_ets, {SetName, Sig}),
    true = ets:delete_object(Server#server.name_to_sig_ets, {SetName, {DDocId, Sig}}),
    true = ets:delete(Server#server.stats_ets, {SetName, DDocId, Sig, main}),
    true = ets:delete(Server#server.stats_ets, {SetName, DDocId, Sig, replica}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


delete_index_dir(RootDir, SetName) ->
    ProdDirName = set_index_dir(RootDir, SetName, prod),
    DevDirName = set_index_dir(RootDir, SetName, dev),
    nuke_dir(RootDir, ProdDirName),
    nuke_dir(RootDir, DevDirName).

set_index_dir(RootDir, SetName, prod) ->
    filename:join([RootDir, "@indexes", ?b2l(SetName)]);
% development views must be stored in a different directory as they have
% the same signature/filename as the production views
set_index_dir(RootDir, SetName, dev) ->
    filename:join([RootDir, "@indexes_dev", ?b2l(SetName)]).


nuke_dir(RootDelDir, Dir) ->
    case file:list_dir(Dir) of
    {error, enoent} -> ok; % doesn't exist
    {ok, Files} ->
        lists:foreach(
            fun(File)->
                Full = Dir ++ "/" ++ File,
                case couch_file:delete(RootDelDir, Full, false) of
                ok -> ok;
                {error, enoent} -> ok;
                {error, eperm} ->
                    ok = nuke_dir(RootDelDir, Full)
                end
            end,
            Files),
        ok = file:del_dir(Dir)
    end.


map_view_key_compare({Key1, DocId1}, {Key2, DocId2}) ->
    case couch_ejson_compare:less_json(Key1, Key2) of
    0 ->
        DocId1 < DocId2;
    LessResult ->
        LessResult < 0
    end.


reduce_view_key_compare(A, B) ->
    couch_ejson_compare:less_json(A, B) < 0.


modify_bitmasks(Group, []) ->
    {Group, []};

modify_bitmasks(#set_view_group{replica_group = nil} = Group, Partitions) ->
    IndexedBitmask = ?set_abitmask(Group) bor ?set_pbitmask(Group),
    WantedBitmask = couch_set_view_util:build_bitmask(Partitions),
    UnindexedBitmask = WantedBitmask band (bnot IndexedBitmask),
    ABitmask2 = WantedBitmask band IndexedBitmask,
    PBitmask2 = (bnot ABitmask2) band IndexedBitmask,
    Header = (Group#set_view_group.index_header)#set_view_index_header{
        abitmask = ABitmask2 band (bnot ?set_cbitmask(Group)),
        pbitmask = PBitmask2
    },
    Unindexed = couch_set_view_util:decode_bitmask(UnindexedBitmask),
    {Group#set_view_group{index_header = Header}, Unindexed};

modify_bitmasks(#set_view_group{replica_group = RepGroup} = Group, Partitions) ->
    IndexedBitmaskMain = ?set_abitmask(Group) bor ?set_pbitmask(Group),
    IndexedBitmaskRep = ?set_abitmask(RepGroup) bor ?set_pbitmask(RepGroup),
    WantedBitmask = couch_set_view_util:build_bitmask(Partitions),

    UnindexedBitmaskMain = (WantedBitmask band (bnot IndexedBitmaskMain)) band (bnot IndexedBitmaskRep),
    UnindexedBitmaskRep = (WantedBitmask band (bnot IndexedBitmaskRep)) band (bnot IndexedBitmaskMain),

    ABitmaskRep2 = WantedBitmask band IndexedBitmaskRep,
    ABitmaskMain2 = (WantedBitmask band IndexedBitmaskMain) band (bnot ABitmaskRep2),

    PBitmaskMain2 = (bnot ABitmaskMain2) band IndexedBitmaskMain,
    PBitmaskRep2 = (bnot ABitmaskRep2) band IndexedBitmaskRep,

    HeaderMain = (Group#set_view_group.index_header)#set_view_index_header{
        abitmask = ABitmaskMain2 band (bnot ?set_cbitmask(Group)),
        pbitmask = PBitmaskMain2
    },
    HeaderRep = (RepGroup#set_view_group.index_header)#set_view_index_header{
        abitmask = ABitmaskRep2 band (bnot ?set_cbitmask(RepGroup)),
        pbitmask = PBitmaskRep2
    },
    Unindexed = couch_set_view_util:decode_bitmask(UnindexedBitmaskMain bor UnindexedBitmaskRep),
    Group2 = Group#set_view_group{
        index_header = HeaderMain,
        replica_group = RepGroup#set_view_group{index_header = HeaderRep}
    },
    {Group2, Unindexed}.


% Wrap the handle_db_event handler so that it works independent of the
% indexer and production/development views.
make_handle_db_event_fun(Mod, ServerName, SigToPidEts, NameToSigEts) ->
    fun
    ({before_delete, DbName}) ->
        case couch_set_view_util:split_set_db_name(DbName) of
        {ok, SetName, master} ->
            ok = gen_server:call(
                ServerName, {before_database_delete, SetName}, infinity);
        _ ->
            ok
        end;
    ({ddoc_updated, {DbName, #doc{id = DDocId} = DDoc}}) ->
        case couch_set_view_util:split_set_db_name(DbName) of
        {ok, SetName, master} ->
            case DDoc#doc.deleted of
            false ->
                DDoc2 = couch_doc:with_ejson_body(DDoc),
                #set_view_group{sig = NewSig} = Mod:design_doc_to_set_view_group(
                    SetName, DDoc2);
            true ->
                NewSig = <<>>
            end,

            lists:foreach(
                fun({_SetName, {_DDocId, Sig}}) ->
                    case ets:lookup(SigToPidEts, {SetName, Sig}) of
                    [{_, GroupPid}] when is_pid(GroupPid), Sig =/= NewSig->
                        Aliases = [
                            AliasDDocId || {_SetName2, {AliasDDocId, _Sig2}} <-
                                ets:match_object(NameToSigEts, {SetName, {'$1', Sig}}),
                                AliasDDocId =/= DDocId
                        ],
                        ok = gen_server:cast(GroupPid, {ddoc_updated, NewSig, Aliases});
                    _ ->
                        ok
                    end
                end,
                ets:match_object(NameToSigEts, {SetName, {DDocId, '$1'}})),
            ok = gen_server:call(ServerName, {ddoc_updated, SetName, DDoc}, infinity);

        _ ->
            ok
        end;
    (_) ->
        ok
    end.


% Returns whether the results should be filtered based on a bitmask or not
-spec filter(#set_view_group{}) -> false | {true, bitmask(), bitmask()}.
filter(#set_view_group{type = main} = Group) ->
    case ?set_pbitmask(Group) bor ?set_cbitmask(Group) of
    0 ->
        false;
    ExcludeBitmask ->
        {true, ExcludeBitmask, ?set_abitmask(Group)}
    end;
filter(#set_view_group{type = replica} = Group) ->
    case ?set_abitmask(Group) bor ?set_cbitmask(Group) of
    0 ->
        false;
    ExcludeBitmask ->
        {true, ExcludeBitmask, ?set_pbitmask(Group)}
    end.


make_reduce_group_keys_fun(0) ->
    fun(_, _) -> true end;
make_reduce_group_keys_fun(GroupLevel) when is_integer(GroupLevel) ->
    fun(KeyDocId1, KeyDocId2) ->
        {Key1, _DocId1} = mapreduce_view:decode_key_docid(KeyDocId1),
        {Key2, _DocId2} = mapreduce_view:decode_key_docid(KeyDocId2),
        case is_array_key(Key1) andalso is_array_key(Key2) of
        true ->
            lists:sublist(?JSON_DECODE(Key1), GroupLevel) ==
                    lists:sublist(?JSON_DECODE(Key2), GroupLevel);
        false ->
            couch_ejson_compare:less_json(Key1, Key2) == 0
        end
    end;
make_reduce_group_keys_fun(_) ->
    fun(KeyDocId1, KeyDocId2) ->
        {Key1, _DocId1} = mapreduce_view:decode_key_docid(KeyDocId1),
        {Key2, _DocId2} = mapreduce_view:decode_key_docid(KeyDocId2),
        couch_ejson_compare:less_json(Key1, Key2) == 0
    end.

is_array_key(<<"[", _/binary>>) ->
    true;
is_array_key(K) when is_binary(K) ->
    false.


-spec get_ddoc_ids_with_sig(atom(), binary(), binary()) -> [binary()].
get_ddoc_ids_with_sig(Mod, SetName, ViewGroupSig) ->
    {ok, Db} = couch_db:open_int(?master_dbname(SetName), []),
    {ok, DDocList} = couch_db:get_design_docs(Db, no_deletes),
    ok = couch_db:close(Db),
    lists:foldl(
        fun(#doc{id = Id} = DDoc, Acc) ->
            case Mod:design_doc_to_set_view_group(SetName, DDoc) of
            #set_view_group{sig = ViewGroupSig} ->
                [Id | Acc];
            #set_view_group{sig = _OtherSig} ->
                Acc
            end
        end,
        [], DDocList).
