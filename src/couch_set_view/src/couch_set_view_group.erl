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

-module(couch_set_view_group).
-behaviour(gen_server).

%% API
-export([start_link/1, request_group_info/1]).
-export([open_set_group/2]).
-export([request_group/2, release_group/1]).
-export([is_view_defined/1, define_view/2]).
-export([set_state/4]).
-export([partition_deleted/2]).
-export([add_replica_partitions/2, remove_replica_partitions/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-define(TIMEOUT, 3000).
-define(DELAYED_COMMIT_PERIOD, 5000).
-define(MIN_CHANGES_AUTO_UPDATE, 20000).
-define(BTREE_CHUNK_THRESHOLD, 5120).

-define(root_dir(State), element(1, State#state.init_args)).
-define(set_name(State), element(2, State#state.init_args)).
-define(type(State), (element(3, State#state.init_args))#set_view_group.type).
-define(group_sig(State), (element(3, State#state.init_args))#set_view_group.sig).
-define(group_id(State), (State#state.group)#set_view_group.name).
-define(db_set(State), (State#state.group)#set_view_group.db_set).
-define(is_defined(State),
    is_integer(((State#state.group)#set_view_group.index_header)#set_view_index_header.num_partitions)).
-define(replicas_on_transfer(State),
        ((State#state.group)#set_view_group.index_header)#set_view_index_header.replicas_on_transfer).

-define(MAX_HIST_SIZE, 10).

-record(state, {
    init_args,
    replica_group = nil,
    group,
    updater_pid = nil,
    % 'not_running' | 'starting' | 'updating_active' | 'updating_passive'
    updater_state = not_running,
    compactor_pid = nil,
    compactor_file = nil,
    compactor_fun = nil,
    commit_ref = nil,
    waiting_list = [],
    cleaner_pid = nil,
    cleanup_waiters = [],
    stats = #set_view_group_stats{},
    shutdown = false,
    replica_partitions = []
}).

-record(cleanup_waiter, {
    from,
    active_list,
    passive_list,
    cleanup_list
}).

-define(inc_stat(S, Stats), setelement(S, Stats, element(S, Stats) + 1)).
-define(inc_partial_updates(Stats), ?inc_stat(#set_view_group_stats.partial_updates, Stats)).
-define(inc_updater_stops(Stats), ?inc_stat(#set_view_group_stats.updater_stops, Stats)).
-define(inc_cleanup_stops(Stats), ?inc_stat(#set_view_group_stats.cleanup_stops, Stats)).


% api methods
request_group(Pid, StaleType) ->
    request_group(Pid, StaleType, 1).

request_group(Pid, StaleType, Retries) ->
    case gen_server:call(Pid, {request_group, StaleType}, infinity) of
    {ok, Group, ActiveReplicasBitmask} ->
        #set_view_group{
            ref_counter = RefCounter,
            replica_pid = RepPid,
            name = GroupName,
            set_name = SetName
        } = Group,
        case request_replica_group(RepPid, ActiveReplicasBitmask, StaleType) of
        {ok, RepGroup} ->
            {ok, Group#set_view_group{replica_group = RepGroup}};
        retry ->
            ?LOG_INFO("Retrying group `~s` request, stale=~s,"
                  " set `~s`, retry attempt #~p",
                  [GroupName, StaleType, SetName, Retries]),
            couch_ref_counter:drop(RefCounter),
            request_group(Pid, StaleType, Retries + 1)
        end;
    Error ->
        Error
    end.


request_replica_group(_RepPid, 0, _Staleness) ->
    {ok, nil};
request_replica_group(RepPid, ActiveReplicasBitmask, Staleness) ->
    {ok, RepGroup, 0} = gen_server:call(RepPid, {request_group, Staleness}, infinity),
    case ?set_abitmask(RepGroup) =:= ActiveReplicasBitmask of
    true ->
        {ok, RepGroup};
    false ->
        couch_ref_counter:drop(RepGroup#set_view_group.ref_counter),
        retry
    end.


release_group(#set_view_group{ref_counter = RefCounter, replica_group = RepGroup}) ->
    couch_ref_counter:drop(RefCounter),
    case RepGroup of
    #set_view_group{ref_counter = RepRefCounter} ->
        couch_ref_counter:drop(RepRefCounter);
    nil ->
        ok
    end.


request_group_info(Pid) ->
    case gen_server:call(Pid, request_group_info) of
    {ok, GroupInfoList} ->
        {ok, GroupInfoList};
    Error ->
        throw(Error)
    end.


% Returns 'ignore' or 'shutdown'.
partition_deleted(Pid, PartId) ->
    try
        gen_server:call(Pid, {partition_deleted, PartId}, infinity)
    catch
    _:_ ->
        % May have stopped already, because partition was part of the
        % group's db set (active or passive partition).
        shutdown
    end.


define_view(Pid, Params) ->
    #set_view_params{
        max_partitions = NumPartitions,
        active_partitions = ActivePartitionsList,
        passive_partitions = PassivePartitionsList,
        use_replica_index = UseReplicaIndex
    } = Params,
    ActiveList = lists:usort(ActivePartitionsList),
    ActiveBitmask = couch_set_view_util:build_bitmask(ActiveList),
    PassiveList = lists:usort(PassivePartitionsList),
    PassiveBitmask = couch_set_view_util:build_bitmask(PassiveList),
    case (ActiveBitmask band PassiveBitmask) /= 0 of
    true ->
        throw({bad_view_definition,
            <<"Intersection between active and passive bitmasks">>});
    false ->
        ok
    end,
    gen_server:call(
        Pid, {define_view, NumPartitions, ActiveList, ActiveBitmask,
            PassiveList, PassiveBitmask, UseReplicaIndex}, infinity).


is_view_defined(Pid) ->
    gen_server:call(Pid, is_view_defined, infinity).


set_state(_Pid, [], [], []) ->
    ok;
set_state(Pid, ActivePartitions, PassivePartitions, CleanupPartitions) ->
    Active = ordsets:from_list(ActivePartitions),
    Passive = ordsets:from_list(PassivePartitions),
    case ordsets:intersection(Active, Passive) of
    [] ->
        Cleanup = ordsets:from_list(CleanupPartitions),
        case ordsets:intersection(Active, Cleanup) of
        [] ->
            case ordsets:intersection(Passive, Cleanup) of
            [] ->
                gen_server:call(
                    Pid, {set_state, Active, Passive, Cleanup}, infinity);
            _ ->
                {error,
                    <<"Intersection between passive and cleanup partition lists">>}
            end;
        _ ->
            {error, <<"Intersection between active and cleanup partition lists">>}
        end;
    _ ->
        {error, <<"Intersection between active and passive partition lists">>}
    end.


add_replica_partitions(_Pid, []) ->
    ok;
add_replica_partitions(Pid, Partitions) ->
    BitMask = couch_set_view_util:build_bitmask(Partitions),
    gen_server:call(Pid, {add_replicas, BitMask}, infinity).


remove_replica_partitions(_Pid, []) ->
    ok;
remove_replica_partitions(Pid, Partitions) ->
    gen_server:call(Pid, {remove_replicas, ordsets:from_list(Partitions)}, infinity).


start_link({RootDir, SetName, Group}) ->
    Args = {RootDir, SetName, Group#set_view_group{type = main}},
    proc_lib:start_link(?MODULE, init, [Args]).


init({_, _, Group} = InitArgs) ->
    process_flag(trap_exit, true),
    try
        {ok, State} = do_init(InitArgs),
        proc_lib:init_ack({ok, self()}),
        gen_server:enter_loop(?MODULE, [], State, 1)
    catch
    _:Error ->
        ?LOG_ERROR("~s error opening set view group `~s` from set `~s`: ~p",
            [?MODULE, Group#set_view_group.name, Group#set_view_group.set_name, Error]),
        exit(Error)
    end.

do_init({_, SetName, _} = InitArgs) ->
    case prepare_group(InitArgs, false) of
    {ok, #set_view_group{fd = Fd, index_header = Header, type = Type} = Group} ->
        {ok, RefCounter} = couch_ref_counter:start([Fd]),
        case Header#set_view_index_header.has_replica of
        false ->
            ReplicaPid = nil,
            ReplicaParts = [];
        true ->
            ReplicaPid = open_replica_group(InitArgs),
            maybe_fix_replica_group(ReplicaPid, Group),
            ReplicaParts = get_replica_partitions(ReplicaPid)
        end,
        case is_integer(Header#set_view_index_header.num_partitions) of
        false ->
            DbSet = nil,
            ?LOG_INFO("Started undefined ~s set view group `~s`, group `~s`",
                      [Type, SetName, Group#set_view_group.name]);
        true ->
            {ActiveList, PassiveList} = make_partition_lists(Group),
            DbSet = case (catch couch_db_set:open(SetName, ActiveList, PassiveList, [])) of
            {ok, SetPid} ->
                SetPid;
            Error ->
                throw(Error)
            end,
            ?LOG_INFO("Started ~s set view group `~s`, group `~s`~n"
                      "active partitions:  ~w~n"
                      "passive partitions: ~w~n"
                      "cleanup partitions: ~w~n"
                      "~sreplica support~n" ++
                      case Header#set_view_index_header.has_replica of
                      true ->
                          "replica partitions: ~w~n"
                          "replica partitions on transfer: ~w~n";
                      false ->
                          ""
                      end,
                      [Type, SetName, Group#set_view_group.name,
                       couch_set_view_util:decode_bitmask(Header#set_view_index_header.abitmask),
                       couch_set_view_util:decode_bitmask(Header#set_view_index_header.pbitmask),
                       couch_set_view_util:decode_bitmask(Header#set_view_index_header.cbitmask),
                       case Header#set_view_index_header.has_replica of
                       true ->
                           "";
                       false ->
                           "no "
                       end] ++
                       case Header#set_view_index_header.has_replica of
                       true ->
                           [ReplicaParts, ?set_replicas_on_transfer(Group)];
                       false ->
                           []
                       end)
        end,
        InitState = #state{
            init_args = InitArgs,
            replica_group = ReplicaPid,
            replica_partitions = ReplicaParts,
            group = Group#set_view_group{
                ref_counter = RefCounter,
                db_set = DbSet,
                replica_pid = ReplicaPid
            }
        },
        {ok, InitState};
    Error ->
        Error
    end.

handle_call({define_view, NumPartitions, ActiveList, ActiveBitmask,
        PassiveList, PassiveBitmask, UseReplicaIndex}, _From, State) when not ?is_defined(State) ->
    #state{init_args = InitArgs, group = Group} = State,
    Seqs = lists:map(
        fun(PartId) -> {PartId, 0} end, lists:usort(ActiveList ++ PassiveList)),
    #set_view_group{
        name = DDocId,
        index_header = Header,
        views = Views
    } = Group,
    NewHeader = Header#set_view_index_header{
        num_partitions = NumPartitions,
        abitmask = ActiveBitmask,
        pbitmask = PassiveBitmask,
        seqs = Seqs,
        purge_seqs = Seqs,
        has_replica = UseReplicaIndex
    },
    case (catch couch_db_set:open(?set_name(State), ActiveList, PassiveList, [])) of
    {ok, DbSet} ->
        case (?type(State) =:= main) andalso UseReplicaIndex of
        false ->
            ReplicaPid = nil;
        true ->
            ReplicaPid = open_replica_group(InitArgs),
            ok = gen_server:call(ReplicaPid, {define_view, NumPartitions, [], 0, [], 0, false}, infinity)
        end,
        NewGroup = Group#set_view_group{
            db_set = DbSet,
            index_header = NewHeader,
            replica_pid = ReplicaPid,
            views = lists:map(
                fun(V) -> V#set_view{update_seqs = Seqs, purge_seqs = Seqs} end, Views)
        },
        ok = commit_header(NewGroup, true),
        NewState = State#state{
            group = NewGroup,
            replica_group = ReplicaPid
        },
        ?LOG_INFO("Set view `~s`, ~s group `~s`, configured with:~n"
            "~p partitions~n"
            "~sreplica support~n"
            "initial active partitions ~w~n"
            "initial passive partitions ~w",
            [?set_name(State), ?type(State), DDocId, NumPartitions,
            case UseReplicaIndex of
            true ->  "";
            false -> "no "
            end,
            ActiveList, PassiveList]),
        {reply, ok, NewState, ?TIMEOUT};
    Error ->
        {reply, Error, State, ?TIMEOUT}
    end;

handle_call({define_view, _, _, _, _, _, _}, _From, State) ->
    {reply, view_already_defined, State, ?TIMEOUT};

handle_call(is_view_defined, _From, #state{group = Group} = State) ->
    {reply, is_integer(?set_num_partitions(Group)), State, ?TIMEOUT};

handle_call({partition_deleted, master}, _From, State) ->
    {stop, shutdown, shutdown, State};
handle_call({partition_deleted, PartId}, _From, #state{group = Group} = State) ->
    Mask = 1 bsl PartId,
    case ((?set_abitmask(Group) band Mask) =/= 0) orelse
        ((?set_pbitmask(Group) band Mask) =/= 0) of
    true ->
        {stop, shutdown, shutdown, State};
    false ->
        {reply, ignore, State, ?TIMEOUT}
    end;

handle_call(_Msg, _From, State) when not ?is_defined(State) ->
    {reply, view_undefined, State};

handle_call({set_state, ActiveList, PassiveList, CleanupList}, From, State) ->
    try
        NewState = update_partition_states(
            ActiveList, PassiveList, CleanupList, From, State),
        {noreply, NewState, ?TIMEOUT}
    catch
    throw:Error ->
        {reply, Error, State}
    end;

handle_call({add_replicas, BitMask}, _From, #state{replica_group = ReplicaPid} = State) when is_pid(ReplicaPid) ->
    #state{
        group = Group,
        replica_partitions = ReplicaParts
    } = State,
    BitMask2 = case BitMask band ?set_abitmask(Group) of
    0 ->
        BitMask;
    Common1 ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, ignoring request to set partitions"
                  " ~w to replica state because they are currently marked as active",
                  [?set_name(State), ?type(State), ?group_id(State),
                   couch_set_view_util:decode_bitmask(Common1)]),
        BitMask bxor Common1
    end,
    BitMask3 = case BitMask2 band ?set_pbitmask(Group) of
    0 ->
        BitMask2;
    Common2 ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, ignoring request to set partitions"
                  " ~w to replica state because they are currently marked as passive",
                  [?set_name(State), ?type(State), ?group_id(State),
                   couch_set_view_util:decode_bitmask(Common2)]),
        BitMask2 bxor Common2
    end,
    Parts = ordsets:from_list(couch_set_view_util:decode_bitmask(BitMask3)),
    % TODO: Improve this. Technically the set_state on the replica group
    % can block us until its cleanup is finished. This happens when we
    % request it to add a group of partitions that are still marked for cleanup.
    ok = set_state(ReplicaPid, [], Parts, []),
    NewReplicaParts = ordsets:union(ReplicaParts, Parts),
    ?LOG_INFO("Set view `~s`, ~s group `~s`, defined new replica partitions: ~w~n"
              "New full set of replica partitions is: ~w~n",
              [?set_name(State), ?type(State), ?group_id(State), Parts, NewReplicaParts]),
    {reply, ok, State#state{replica_partitions = NewReplicaParts}, ?TIMEOUT};

handle_call({remove_replicas, Partitions}, _From, #state{replica_group = ReplicaPid} = State) when is_pid(ReplicaPid) ->
    #state{
        replica_partitions = ReplicaParts,
        group = Group
    } = State,
    case ordsets:intersection(?set_replicas_on_transfer(Group), Partitions) of
    [] ->
        ok = set_state(ReplicaPid, [], [], Partitions),
        NewState = State#state{
            replica_partitions = ordsets:subtract(ReplicaParts, Partitions)
        };
    Common ->
        UpdaterWasRunning = is_pid(State#state.updater_pid),
        State2 = stop_cleaner(State),
        #state{group = Group3} = State3 = stop_updater(State2, immediately),
        {ok, NewAbitmask, NewPbitmask, NewCbitmask, NewSeqs, NewPurgeSeqs} =
            set_cleanup_partitions(
                Common,
                ?set_abitmask(Group3),
                ?set_pbitmask(Group3),
                ?set_cbitmask(Group3),
                ?set_seqs(Group3),
                ?set_purge_seqs(Group3)),
        case NewCbitmask =/= ?set_cbitmask(Group3) of
        true ->
             State4 = restart_compactor(State3, "partition states were updated");
        false ->
             State4 = State3
        end,
        ok = couch_db_set:remove_partitions(?db_set(State4), Common),
        ReplicaPartitions2 = ordsets:subtract(ReplicaParts, Common),
        ReplicasOnTransfer2 = ordsets:subtract(?set_replicas_on_transfer(Group3), Common),
        State5 = update_header(
            State4,
            NewAbitmask,
            NewPbitmask,
            NewCbitmask,
            NewSeqs,
            NewPurgeSeqs,
            ReplicasOnTransfer2,
            ReplicaPartitions2),
        ok = set_state(ReplicaPid, [], [], Partitions),
        case UpdaterWasRunning of
        true ->
            State6 = start_updater(State5);
        false ->
            State6 = State5
        end,
        NewState = maybe_start_cleaner(State6)
    end,
    ?LOG_INFO("Set view `~s`, ~s group `~s`, marked the following replica partitions for removal: ~w",
              [?set_name(State), ?type(State), ?group_id(State), Partitions]),
    {reply, ok, NewState, ?TIMEOUT};

% {request_group, StaleType}
handle_call({request_group, false}, From,
        #state{
            group = Group,
            updater_pid = UpPid,
            updater_state = UpState,
            waiting_list = WaitList
        } = State) ->
    case UpPid of
    nil ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, blocking client ~w on group request~n",
                  [?set_name(State), ?type(State), ?group_id(State), From]),
        State2 = start_updater(State#state{waiting_list = [From | WaitList]}),
        {noreply, State2, ?TIMEOUT};
    _ when is_pid(UpPid), UpState =:= updating_passive ->
        reply_with_group(Group, State#state.stats, [From]),
        {noreply, State, ?TIMEOUT};
    _ when is_pid(UpPid) ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, blocking client ~w on group request~n",
                  [?set_name(State), ?type(State), ?group_id(State), From]),
        State2 = State#state{waiting_list = [From | WaitList]},
        {noreply, State2, ?TIMEOUT}
    end;

handle_call({request_group, ok}, From, #state{group = Group} = State) ->
    reply_with_group(Group, State#state.stats, [From]),
    {noreply, State, ?TIMEOUT};

handle_call({request_group, update_after}, From, #state{group = Group} = State) ->
    reply_with_group(Group, State#state.stats, [From]),
    case State#state.updater_pid of
    Pid when is_pid(Pid) ->
        {noreply, State};
    nil ->
        State2 = start_updater(State),
        {noreply, State2, ?TIMEOUT}
    end;

handle_call(request_group_info, _From, State) ->
    GroupInfo = get_group_info(State),
    {reply, {ok, GroupInfo}, State, ?TIMEOUT};

handle_call({start_compact, CompactFun}, _From, #state{compactor_pid = nil} = State) ->
    #state{compactor_pid = Pid} = State2 = start_compactor(State, CompactFun),
    {reply, {ok, Pid}, State2};
handle_call({start_compact, _}, _From, State) ->
    %% compact already running, this is a no-op
    {reply, {ok, State#state.compactor_pid}, State};

handle_call({compact_done, NewGroup0, Duration, CleanupKVCount}, {Pid, _}, #state{compactor_pid = Pid} = State) ->
    #state{
        group = Group,
        updater_pid = UpdaterPid,
        compactor_pid = CompactorPid
    } = State,
    #set_view_group{
        fd = OldFd, sig = GroupSig, ref_counter = RefCounter
    } = Group,

    case group_up_to_date(NewGroup0, State#state.group) of
    true ->
        NewGroup = NewGroup0#set_view_group{
            index_header = get_index_header_data(NewGroup0)
        },
        if is_pid(UpdaterPid) ->
            couch_util:shutdown_sync(UpdaterPid);
        true ->
            ok
        end,
        ok = commit_header(NewGroup, true),
        ?LOG_INFO("Set view `~s`, ~s group `~s`, compaction complete in ~.3f seconds,"
            " filtered ~p key-value pairs",
            [?set_name(State), ?type(State), ?group_id(State), Duration, CleanupKVCount]),
        FileName = index_file_name(
            ?root_dir(State), ?set_name(State), ?type(State), GroupSig),
        ok = couch_file:only_snapshot_reads(OldFd),
        ok = couch_file:delete(?root_dir(State), FileName),
        ok = couch_file:rename(NewGroup#set_view_group.fd, FileName),

        NewUpdaterPid =
        if is_pid(UpdaterPid) ->
            Owner = self(),
            spawn_link(fun() ->
                couch_set_view_updater:update(Owner, NewGroup, index_file_name(State))
            end);
        true ->
            nil
        end,

        %% cleanup old group
        unlink(CompactorPid),
        receive {'EXIT', CompactorPid, normal} -> ok after 0 -> ok end,
        unlink(OldFd),
        couch_ref_counter:drop(RefCounter),
        {ok, NewRefCounter} = couch_ref_counter:start([NewGroup#set_view_group.fd]),

        State2 = State#state{
            compactor_pid = nil,
            compactor_file = nil,
            compactor_fun = nil,
            updater_pid = NewUpdaterPid,
            updater_state = case is_pid(NewUpdaterPid) of
                true -> starting;
                false -> not_running
            end,
            group = NewGroup#set_view_group{
                ref_counter = NewRefCounter,
                index_header = (NewGroup#set_view_group.index_header)#set_view_index_header{
                    replicas_on_transfer = ?set_replicas_on_transfer(Group)
                }
            },
            stats = inc_compactions(State#state.stats, Duration, CleanupKVCount)
        },
        State3 = notify_cleanup_waiters(State2),
        {reply, ok, State3, ?TIMEOUT};
    false ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, compaction still behind, retrying",
            [?set_name(State), ?type(State), ?group_id(State)]),
        {reply, update, State}
    end;
handle_call({compact_done, _NewGroup, _Duration, _CleanupKVCount}, {OldPid, _}, State) ->
    % From a previous compactor that was killed/stopped, ignore.
    false = is_process_alive(OldPid),
    {noreply, State, ?TIMEOUT};

handle_call(cancel_compact, _From, #state{compactor_pid = nil} = State) ->
    {reply, ok, State, ?TIMEOUT};
handle_call(cancel_compact, _From, #state{compactor_pid = Pid, compactor_file = CompactFd} = State) ->
    couch_util:shutdown_sync(Pid),
    couch_util:shutdown_sync(CompactFd),
    CompactFile = index_file_name(State, compact),
    ok = couch_file:delete(?root_dir(State), CompactFile),
    State2 = maybe_start_cleaner(State#state{compactor_pid = nil, compactor_file = nil}),
    {reply, ok, State2, ?TIMEOUT}.


handle_cast({partial_update, Pid, NewGroup}, #state{updater_pid = Pid} = State) ->
    NewState = process_partial_update(State, NewGroup),
    {noreply, NewState};
handle_cast({partial_update, _, _}, State) ->
    %% message from an old (probably pre-compaction) updater; ignore
    {noreply, State, ?TIMEOUT};

handle_cast({cleanup_done, CleanupTime, CleanupKVCount}, State) ->
    NewStats = inc_cleanups(State#state.stats, CleanupTime, CleanupKVCount, true),
    {noreply, State#state{stats = NewStats}};

handle_cast(ddoc_updated, State) ->
    #state{
        waiting_list = Waiters,
        group = #set_view_group{name = DDocId, sig = CurSig}
    } = State,
    DbName = ?master_dbname((?set_name(State))),
    {ok, Db} = couch_db:open_int(DbName, []),
    case couch_db:open_doc(Db, DDocId, [ejson_body]) of
    {not_found, deleted} ->
        NewSig = nil;
    {ok, DDoc} ->
        #set_view_group{sig = NewSig} =
            couch_set_view_util:design_doc_to_set_view_group(?set_name(State), DDoc)
    end,
    couch_db:close(Db),
    case NewSig of
    CurSig ->
        {noreply, State#state{shutdown = false}, ?TIMEOUT};
    _ ->
        case Waiters of
        [] ->
            {stop, normal, State};
        _ ->
            {noreply, State#state{shutdown = true}}
        end
    end.


handle_info(timeout, State) when not ?is_defined(State) ->
    {noreply, State};

handle_info(timeout, State) ->
    case ?type(State) of
    main ->
        {noreply, maybe_start_cleaner(State)};
    replica ->
        {noreply, maybe_update_replica_index(State)}
    end;

handle_info({updater_info, Pid, {state, UpdaterState}}, #state{updater_pid = Pid} = State) ->
    #state{waiting_list = WaitList, cleanup_waiters = CleanupWaiters} = State,
    State2 = State#state{updater_state = UpdaterState},
    case UpdaterState of
    updating_passive when WaitList =/= [] andalso CleanupWaiters =/= [] ->
        State3 = stop_updater(State2),
        case State#state.shutdown of
        true ->
            {stop, normal, State3};
        false ->
            {noreply, start_updater(State3)}
        end;
    updating_passive when WaitList =/= [] ->
        reply_with_group(State2#state.group, State#state.stats, WaitList),
        {noreply, State2#state{waiting_list = []}};
    _ ->
        {noreply, State2}
    end;

handle_info({updater_info, _Pid, {state, _UpdaterState}}, State) ->
    % Message from an old updater, ignore.
    {noreply, State, ?TIMEOUT};

handle_info(delayed_commit, #state{group = Group} = State) ->
    ?LOG_INFO("Checkpointing set view `~s` update for ~s group `~s`",
        [?set_name(State), ?type(State), ?group_id(State)]),
    commit_header(Group, false),
    {noreply, State#state{commit_ref = nil}, ?TIMEOUT};

handle_info({'EXIT', Pid, {clean_group, NewGroup, Count, Time}}, #state{cleaner_pid = Pid} = State) ->
    #state{group = OldGroup, stats = Stats} = State,
    ?LOG_INFO("Cleanup finished for set view `~s`, ~s group `~s`~n"
              "Removed ~p values from the index in ~.3f seconds~n"
              "active partitions before:  ~w~n"
              "active partitions after:   ~w~n"
              "passive partitions before: ~w~n"
              "passive partitions after:  ~w~n"
              "cleanup partitions before: ~w~n"
              "cleanup partitions after:  ~w~n" ++
          case is_pid(State#state.replica_group) of
          true ->
              "Current set of replica partitions: ~w~n"
              "Current set of replicas on transfer: ~w~n";
          false ->
               []
          end,
          [?set_name(State), ?type(State), ?group_id(State), Count, Time,
           couch_set_view_util:decode_bitmask(?set_abitmask(OldGroup)),
           couch_set_view_util:decode_bitmask(?set_abitmask(NewGroup)),
           couch_set_view_util:decode_bitmask(?set_pbitmask(OldGroup)),
           couch_set_view_util:decode_bitmask(?set_pbitmask(NewGroup)),
           couch_set_view_util:decode_bitmask(?set_cbitmask(OldGroup)),
           couch_set_view_util:decode_bitmask(?set_cbitmask(NewGroup))] ++
              case is_pid(State#state.replica_group) of
              true ->
                  [State#state.replica_partitions, ?set_replicas_on_transfer(NewGroup)];
              false ->
                  []
              end),
    State2 = State#state{
        cleaner_pid = nil,
        group = NewGroup,
        stats = inc_cleanups(Stats, Time, Count)
    },
    {noreply, notify_cleanup_waiters(State2)};

handle_info({'EXIT', Pid, Reason}, #state{cleaner_pid = Pid} = State) ->
    {stop, {cleaner_died, Reason}, State};

handle_info({'EXIT', Pid, shutdown},
    #state{group = #set_view_group{db_set = Pid}} = State) ->
    ?LOG_INFO("Set view `~s`, ~s group `~s`, terminating because database set "
              "was shutdown", [?set_name(State), ?type(State), ?group_id(State)]),
    {stop, normal, State};

handle_info({'EXIT', Pid, {updater_finished, NewGroup, _UpState, Duration}}, #state{updater_pid = Pid} = State) ->
    #state{
        waiting_list = WaitList,
        shutdown = Shutdown
    } = State,
    ok = commit_header(NewGroup, false),
    reply_with_group(NewGroup, State#state.stats, WaitList),
    ?LOG_INFO("Set view `~s`, ~s group `~s`, updater finished, ran for ~.3f seconds",
        [?set_name(State), ?type(State), ?group_id(State), Duration]),
    case Shutdown of
    true ->
        {stop, normal, State};
    false ->
        cancel_commit(State),
        State2 = State#state{
            updater_pid = nil,
            updater_state = not_running,
            commit_ref = nil,
            waiting_list = [],
            group = NewGroup,
            stats = inc_updates(State#state.stats, Duration)
        },
        State3 = maybe_start_cleaner(State2),
        {noreply, State3, ?TIMEOUT}
    end;

handle_info({'EXIT', Pid, {updater_error, Error}}, #state{updater_pid = Pid} = State) ->
    ?LOG_ERROR("Set view `~s`, ~s group `~s`, received error from updater: ~p",
        [?set_name(State), ?type(State), ?group_id(State), Error]),
    case State#state.shutdown of
    true ->
        {stop, normal, reply_all(State, {error, Error})};
    false ->
        State2 = State#state{
            updater_pid = nil,
            updater_state = not_running
        },
        State3 = reply_all(State2, {error, Error}),
        {noreply, maybe_start_cleaner(State3), ?TIMEOUT}
    end;

handle_info({'EXIT', _Pid, {updater_error, _Error}}, State) ->
    % from old, shutdown updater, ignore
    {noreply, State, ?TIMEOUT};

handle_info({'EXIT', UpPid, reset}, #state{updater_pid = UpPid} = State) ->
    State2 = stop_cleaner(State),
    case prepare_group(State#state.init_args, true) of
    {ok, ResetGroup} ->
        {ok, start_updater(State2#state{group = ResetGroup})};
    Error ->
        {stop, normal, reply_all(State2, Error), ?TIMEOUT}
    end;

handle_info({'EXIT', Pid, normal}, State) ->
    ?LOG_INFO("Set view `~s`, ~s group `~s`, linked PID ~p stopped normally",
              [?set_name(State), ?type(State), ?group_id(State), Pid]),
    {noreply, State, ?TIMEOUT};

handle_info({'EXIT', Pid, Reason}, #state{compactor_pid = Pid} = State) ->
    couch_util:shutdown_sync(State#state.compactor_file),
    {stop, {compactor_died, Reason}, State};

handle_info({'EXIT', Pid, Reason}, #state{group = #set_view_group{db_set = Pid}} = State) ->
    {stop, {db_set_died, Reason}, State};

handle_info({'EXIT', Pid, Reason}, State) ->
    ?LOG_ERROR("Set view `~s`, ~s group `~s`, terminating because linked PID ~p "
              "died with reason: ~p",
              [?set_name(State), ?type(State), ?group_id(State), Pid, Reason]),
    {stop, Reason, State}.


terminate(Reason, #state{updater_pid=Update, compactor_pid=Compact}=S) ->
    ?LOG_INFO("Set view `~s`, ~s group `~s`, terminating with reason: ~p",
        [?set_name(S), ?type(S), ?group_id(S), Reason]),
    State2 = stop_cleaner(S),
    reply_all(State2, Reason),
    case is_pid(?db_set(S)) andalso is_process_alive(?db_set(S)) of
    true ->
        couch_db_set:close(?db_set(S));
    false ->
        ok
    end,
    couch_util:shutdown_sync(Update),
    couch_util:shutdown_sync(Compact),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Local Functions

reply_with_group(Group0, Stats, WaitList) ->
    #set_view_group{
        ref_counter = RefCnt,
        set_name = SetName,
        type = Type,
        name = GroupId,
        debug_info = DebugInfo
    } = Group0,
    ActiveReplicasBitmask = couch_set_view_util:build_bitmask(
        ?set_replicas_on_transfer(Group0)),
    Group = Group0#set_view_group{
        debug_info = DebugInfo#set_view_debug_info{
            stats = Stats,
            original_abitmask = ?set_abitmask(Group0),
            original_pbitmask = ?set_pbitmask(Group0)
        }
    },
    lists:foreach(fun({Pid, _} = From) ->
        couch_ref_counter:add(RefCnt, Pid),
        gen_server:reply(From, {ok, Group, ActiveReplicasBitmask}),
        ?LOG_INFO("Set view `~s`, ~s group `~s`, replied to client ~w",
                  [SetName, Type, GroupId, From])
    end, WaitList).

reply_all(#state{waiting_list=WaitList}=State, Reply) ->
    [catch gen_server:reply(From, Reply) || From <- WaitList],
    State#state{waiting_list=[]}.

prepare_group({RootDir, SetName, #set_view_group{sig = Sig, type = Type} = Group}, ForceReset)->
    case open_index_file(RootDir, SetName, Type, Sig) of
    {ok, Fd} ->
        if ForceReset ->
            % this can happen if we missed a purge
            {ok, reset_file(Fd, SetName, Group)};
        true ->
            case (catch couch_file:read_header(Fd)) of
            {ok, {Sig, HeaderInfo}} ->
                % sigs match!
                {ok, init_group(Fd, Group, HeaderInfo)};
            _ ->
                % this happens on a new file
                {ok, reset_file(Fd, SetName, Group)}
            end
        end;
    Error ->
        catch delete_index_file(RootDir, SetName, Type, Sig),
        Error
    end.

get_index_header_data(#set_view_group{id_btree = IdBtree, views = Views, index_header = Header}) ->
    ViewStates = [
        {couch_btree:get_state(V#set_view.btree), V#set_view.update_seqs, V#set_view.purge_seqs} || V <- Views
    ],
    Header#set_view_index_header{
        id_btree_state = couch_btree:get_state(IdBtree),
        view_states = ViewStates
    }.

hex_sig(GroupSig) ->
    couch_util:to_hex(?b2l(GroupSig)).

design_root(RootDir, SetName) ->
    RootDir ++ "/set_view_" ++ ?b2l(SetName) ++ "_design/".

index_file_name(State) ->
    index_file_name(?root_dir(State), ?set_name(State), ?type(State), ?group_sig(State)).
index_file_name(State, compact) ->
    index_file_name(compact, ?root_dir(State), ?set_name(State), ?type(State), ?group_sig(State)).

index_file_name(RootDir, SetName, main, GroupSig) ->
    design_root(RootDir, SetName) ++ "main_" ++ hex_sig(GroupSig) ++".view";
index_file_name(RootDir, SetName, replica, GroupSig) ->
    design_root(RootDir, SetName) ++ "replica_" ++ hex_sig(GroupSig) ++".view".

index_file_name(compact, RootDir, SetName, main, GroupSig) ->
    design_root(RootDir, SetName) ++ "main_" ++ hex_sig(GroupSig) ++".compact.view";
index_file_name(compact, RootDir, SetName, replica, GroupSig) ->
    design_root(RootDir, SetName) ++ "replica_" ++ hex_sig(GroupSig) ++".compact.view".


open_index_file(RootDir, SetName, Type, GroupSig) ->
    FileName = index_file_name(RootDir, SetName, Type, GroupSig),
    case couch_file:open(FileName) of
    {ok, Fd}        -> {ok, Fd};
    {error, enoent} -> couch_file:open(FileName, [create]);
    Error           -> Error
    end.

open_index_file(compact, RootDir, SetName, Type, GroupSig) ->
    FileName = index_file_name(compact, RootDir, SetName, Type, GroupSig),
    case couch_file:open(FileName) of
    {ok, Fd}        -> {ok, Fd};
    {error, enoent} -> couch_file:open(FileName, [create]);
    Error           -> Error
    end.


open_set_group(SetName, GroupId) ->
    case couch_db:open_int(?master_dbname(SetName), []) of
    {ok, Db} ->
        case couch_db:open_doc(Db, GroupId, [ejson_body]) of
        {ok, Doc} ->
            couch_db:close(Db),
            {ok, couch_set_view_util:design_doc_to_set_view_group(SetName, Doc)};
        Else ->
            couch_db:close(Db),
            Else
        end;
    Else ->
        Else
    end.

get_group_info(State) ->
    #state{
        group = Group,
        replica_group = ReplicaPid,
        updater_pid = UpdaterPid,
        updater_state = UpdaterState,
        compactor_pid = CompactorPid,
        commit_ref = CommitRef,
        waiting_list = WaitersList,
        cleaner_pid = CleanerPid,
        replica_partitions = ReplicaParts,
        stats = Stats
    } = State,
    #set_view_group{
        fd = Fd,
        sig = GroupSig,
        id_btree = Btree,
        def_lang = Lang,
        views = Views
    } = Group,
    JsonStats = {[
        {updates, Stats#set_view_group_stats.updates},
        {partial_updates, Stats#set_view_group_stats.partial_updates},
        {updater_interruptions, Stats#set_view_group_stats.updater_stops},
        {updater_cleanups, Stats#set_view_group_stats.updater_cleanups},
        {compactions, Stats#set_view_group_stats.compactions},
        {cleanups, Stats#set_view_group_stats.cleanups},
        {waiting_clients, length(WaitersList)},
        {cleanup_interruptions, Stats#set_view_group_stats.cleanup_stops},
        {cleanup_blocked_processes, length(State#state.cleanup_waiters)},
        {update_history, Stats#set_view_group_stats.update_history},
        {compaction_history, Stats#set_view_group_stats.compaction_history},
        {cleanup_history, Stats#set_view_group_stats.cleanup_history}
    ]},
    {ok, Size} = couch_file:bytes(Fd),
    [
        {signature, ?l2b(hex_sig(GroupSig))},
        {language, Lang},
        {disk_size, Size},
        {data_size, view_group_data_size(Btree, Views)},
        {updater_running, UpdaterPid /= nil},
        {updater_state, couch_util:to_binary(UpdaterState)},
        {compact_running, CompactorPid /= nil},
        {cleanup_running, (CleanerPid /= nil) orelse
            ((CompactorPid /= nil) andalso (?set_cbitmask(Group) =/= 0))},
        {waiting_commit, is_reference(CommitRef)},
        {max_number_partitions, ?set_num_partitions(Group)},
        {update_seqs, {[{couch_util:to_binary(P), S} || {P, S} <- ?set_seqs(Group)]}},
        {purge_seqs, {[{couch_util:to_binary(P), S} || {P, S} <- ?set_purge_seqs(Group)]}},
        {active_partitions, couch_set_view_util:decode_bitmask(?set_abitmask(Group))},
        {passive_partitions, couch_set_view_util:decode_bitmask(?set_pbitmask(Group))},
        {cleanup_partitions, couch_set_view_util:decode_bitmask(?set_cbitmask(Group))},
        {stats, JsonStats}
    ] ++
    case (?type(State) =:= main) andalso is_pid(ReplicaPid) of
    true ->
        [{replica_partitions, ReplicaParts}, {replicas_on_transfer, ?set_replicas_on_transfer(Group)}];
    false ->
        []
    end ++
    get_replica_group_info(ReplicaPid).

get_replica_group_info(ReplicaPid) when is_pid(ReplicaPid) ->
    {ok, RepGroupInfo} = gen_server:call(ReplicaPid, request_group_info, infinity),
    [{replica_group_info, {RepGroupInfo}}];
get_replica_group_info(_) ->
    [].


view_group_data_size(IdBtree, Views) ->
    lists:foldl(
        fun(#set_view{btree = Btree}, Acc) ->
            Acc + couch_btree:size(Btree)
        end,
        couch_btree:size(IdBtree),
        Views).


reset_group(#set_view_group{views = Views} = Group) ->
    Views2 = [View#set_view{btree = nil} || View <- Views],
    Group#set_view_group{
        fd = nil,
        index_header = nil,
        query_server = nil,
        id_btree = nil,
        views = Views2
    }.

reset_file(Fd, SetName, #set_view_group{
        sig = Sig, name = Name, index_header = Header} = Group) ->
    ?LOG_DEBUG("Resetting group index `~s` in set `~s`", [Name, SetName]),
    ok = couch_file:truncate(Fd, 0),
    ok = couch_file:write_header(Fd, {Sig, nil}),
    init_group(Fd, reset_group(Group), Header).

delete_index_file(RootDir, SetName, Type, GroupSig) ->
    couch_file:delete(
        RootDir, index_file_name(RootDir, SetName, Type, GroupSig)).

init_group(Fd, #set_view_group{views = Views}=Group, nil) ->
    EmptyHeader = #set_view_index_header{
        view_states = [{nil, [], []} || _ <- Views]
    },
    init_group(Fd, Group, EmptyHeader);
init_group(Fd, #set_view_group{def_lang = Lang, views = Views} = Group, IndexHeader) ->
    #set_view_index_header{
        id_btree_state = IdBtreeState,
        view_states = ViewStates
    } = IndexHeader,
    StateUpdate = fun
        ({_, _, _}=State) -> State;
        (State) -> {State, [], []}
    end,
    ViewStates2 = lists:map(StateUpdate, ViewStates),
    IdTreeReduce = fun(reduce, KVs) ->
        {length(KVs), couch_set_view_util:partitions_map(KVs, 0)};
    (rereduce, [First | Rest]) ->
        lists:foldl(
            fun({S, M}, {T, A}) -> {S + T, M bor A} end,
            First, Rest)
    end,
    BtreeOptions = [
        {chunk_threshold, ?BTREE_CHUNK_THRESHOLD},
        {compression, snappy}
    ],
    {ok, IdBtree} = couch_btree:open(
        IdBtreeState, Fd, [{reduce, IdTreeReduce} | BtreeOptions]),
    Views2 = lists:zipwith(
        fun({BTState, USeqs, PSeqs}, #set_view{reduce_funs=RedFuns,options=Options}=View) ->
            FunSrcs = [FunSrc || {_Name, FunSrc} <- RedFuns],
            ReduceFun =
                fun(reduce, KVs) ->
                    AllPartitionsBitMap = couch_set_view_util:partitions_map(KVs, 0),
                    KVs2 = couch_set_view_util:expand_dups(KVs, []),
                    KVs3 = couch_set_view_util:detuple_kvs(KVs2, []),
                    {ok, Reduced} = couch_query_servers:reduce(Lang, FunSrcs,
                        KVs3),
                    {length(KVs3), Reduced, AllPartitionsBitMap};
                (rereduce, [{Count0, Red0, AllPartitionsBitMap0} | Reds]) ->
                    {Count, UserReds, AllPartitionsBitMap} = lists:foldl(
                        fun({C, R, Apbm}, {CountAcc, RedAcc, ApbmAcc}) ->
                            {C + CountAcc, [R | RedAcc], Apbm bor ApbmAcc}
                        end,
                        {Count0, [Red0], AllPartitionsBitMap0},
                        Reds),
                    {ok, Reduced} = couch_query_servers:rereduce(
                        Lang, FunSrcs, UserReds),
                    {Count, Reduced, AllPartitionsBitMap}
                end,
            
            case couch_util:get_value(<<"collation">>, Options, <<"default">>) of
            <<"default">> ->
                Less = fun couch_set_view:less_json_ids/2;
            <<"raw">> ->
                Less = fun(A,B) -> A < B end
            end,
            {ok, Btree} = couch_btree:open(
                BTState, Fd, [{less, Less}, {reduce, ReduceFun} | BtreeOptions]),
            View#set_view{btree=Btree, update_seqs=USeqs, purge_seqs=PSeqs}
        end,
        ViewStates2, Views),
    Group#set_view_group{
        fd = Fd,
        id_btree = IdBtree,
        views = Views2,
        index_header = IndexHeader
    }.


commit_header(Group, Sync) ->
    Header = {Group#set_view_group.sig, get_index_header_data(Group)},
    ok = couch_file:write_header(Group#set_view_group.fd, Header),
    case Sync of
    true ->
        ok = couch_file:flush(Group#set_view_group.fd),
        ok = couch_file:sync(Group#set_view_group.fd);
    false ->
        ok
    end.


group_up_to_date(#set_view_group{} = NewGroup, #set_view_group{} = CurGroup) ->
    compare_seqs(?set_seqs(NewGroup), ?set_seqs(CurGroup)).


compare_seqs([], []) ->
    true;
compare_seqs([{PartId, SeqA} | RestA], [{PartId, SeqB} | RestB]) ->
    case SeqA - SeqB of
    Greater when Greater >= 0 ->
        compare_seqs(RestA, RestB);
    _Smaller ->
        false
    end.


update_partition_states(ActiveList, PassiveList, CleanupList, From, State) ->
    #state{group = Group} = State,
    ActiveMask = couch_set_view_util:build_bitmask(ActiveList),
    case ActiveMask >= (1 bsl ?set_num_partitions(Group)) of
    true ->
        throw({error, <<"Invalid active partitions list">>});
    false ->
        ok
    end,
    PassiveMask = couch_set_view_util:build_bitmask(PassiveList),
    case PassiveMask >= (1 bsl ?set_num_partitions(Group)) of
    true ->
        throw({error, <<"Invalid passive partitions list">>});
    false ->
        ok
    end,
    CleanupMask = couch_set_view_util:build_bitmask(CleanupList),
    case CleanupMask >= (1 bsl ?set_num_partitions(Group)) of
    true ->
        throw({error, <<"Invalid cleanup partitions list">>});
    false ->
        ok
    end,
    case (ActiveMask bor ?set_abitmask(Group)) =:= ?set_abitmask(Group) andalso
        (PassiveMask bor ?set_pbitmask(Group)) =:= ?set_pbitmask(Group) andalso
        (CleanupMask bor ?set_cbitmask(Group)) =:= ?set_cbitmask(Group) of
    true ->
        gen_server:reply(From, ok),
        State;
    false ->
        do_update_partition_states(ActiveList, PassiveList, CleanupList, From, State)
    end.


do_update_partition_states(ActiveList, PassiveList, CleanupList, From, State) ->
    #state{cleanup_waiters = CleanupWaiters} = State2 = stop_cleaner(State),
    UpdaterRunning = is_pid(State2#state.updater_pid),
    #state{group = Group3} = State3 = stop_updater(State2, immediately),
    {InCleanup, _NotInCleanup} =
        partitions_still_in_cleanup(ActiveList ++ PassiveList, Group3),
    case InCleanup of
    [] ->
        State4 = persist_partition_states(State3, ActiveList, PassiveList, CleanupList),
        gen_server:reply(From, ok);
    _ ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, blocking client ~p, "
            "requesting partition state change because the following "
            "partitions are still in cleanup: ~w",
            [?set_name(State), ?type(State), ?group_id(State), element(1, From), InCleanup]),
        Waiter = #cleanup_waiter{
            from = From,
            active_list = ActiveList,
            passive_list = PassiveList,
            cleanup_list = CleanupList
        },
        State4 = State3#state{cleanup_waiters = CleanupWaiters ++ [Waiter]}
    end,
    case ?type(State) of
    main ->
        State5 = case UpdaterRunning of
        true ->
            % Updater was running, we stopped it, updated the group we received
            % from the updater, updated that group's bitmasks and update/purge
            % seqs, and now restart the updater with this modified group.
            start_updater(State4);
        false ->
            State4
        end,
        State6 = restart_compactor(State5, "partition states were updated"),
        maybe_start_cleaner(State6);
    replica ->
        State5 = restart_compactor(State4, "partition states were updated"),
        case is_pid(State5#state.compactor_pid) of
        true ->
            State5;
        false ->
            maybe_update_replica_index(State5)
        end
    end.


persist_partition_states(State, ActiveList, PassiveList, CleanupList) ->
    #state{
        group = Group,
        replica_partitions = ReplicaParts,
        replica_group = ReplicaPid
    } = State,
    case ordsets:intersection(ActiveList, ReplicaParts) of
    [] ->
         ActiveList2 = ActiveList,
         PassiveList2 = PassiveList,
         ReplicasOnTransfer2 = ?set_replicas_on_transfer(Group),
         ReplicasToMarkActive = [];
    CommonRep ->
         PassiveList2 = ordsets:union(PassiveList, CommonRep),
         ActiveList2 = ordsets:subtract(ActiveList, CommonRep),
         ReplicasOnTransfer2 = ordsets:union(?set_replicas_on_transfer(Group), CommonRep),
         ReplicasToMarkActive = CommonRep
    end,
    case ordsets:intersection(PassiveList, ReplicasOnTransfer2) of
    [] ->
        PassiveList3 = PassiveList2,
        ReplicasOnTransfer3 = ReplicasOnTransfer2;
    CommonRep2 ->
        PassiveList3 = ordsets:subtract(PassiveList2, CommonRep2),
        ReplicasOnTransfer3 = ordsets:subtract(ReplicasOnTransfer2, CommonRep2)
    end,
    case ordsets:intersection(CleanupList, ReplicasOnTransfer3) of
    [] ->
        ReplicaParts2 = ReplicaParts,
        ReplicasOnTransfer4 = ReplicasOnTransfer3,
        ReplicasToCleanup = [];
    CommonRep3 ->
        ReplicaParts2 = ordsets:subtract(ReplicaParts, CommonRep3),
        ReplicasOnTransfer4 = ordsets:subtract(ReplicasOnTransfer3, CommonRep3),
        ReplicasToCleanup = CommonRep3
    end,
    {ok, NewAbitmask1, NewPbitmask1, NewSeqs1, NewPurgeSeqs1} =
        set_active_partitions(
            ActiveList2,
            ?set_abitmask(Group),
            ?set_pbitmask(Group),
            ?set_seqs(Group),
            ?set_purge_seqs(Group)),
    {ok, NewAbitmask2, NewPbitmask2, NewSeqs2, NewPurgeSeqs2} =
        set_passive_partitions(
            PassiveList3,
            NewAbitmask1,
            NewPbitmask1,
            NewSeqs1,
            NewPurgeSeqs1),
    {ok, NewAbitmask3, NewPbitmask3, NewCbitmask3, NewSeqs3, NewPurgeSeqs3} =
        set_cleanup_partitions(
            CleanupList,
            NewAbitmask2,
            NewPbitmask2,
            ?set_cbitmask(Group),
            NewSeqs2,
            NewPurgeSeqs2),
    ok = couch_db_set:remove_partitions(?db_set(State), CleanupList),
    State2 = update_header(
        State,
        NewAbitmask3,
        NewPbitmask3,
        NewCbitmask3,
        NewSeqs3,
        NewPurgeSeqs3,
        ReplicasOnTransfer4,
        ReplicaParts2),
    % A crash might happen between updating our header and updating the state of
    % replica view group. The init function must detect and correct this.
    ok = set_state(ReplicaPid, ReplicasToMarkActive, [], ReplicasToCleanup),
    State2.


set_passive_partitions([], Abitmask, Pbitmask, Seqs, PurgeSeqs) ->
    {ok, Abitmask, Pbitmask, Seqs, PurgeSeqs};

set_passive_partitions([PartId | Rest], Abitmask, Pbitmask, Seqs, PurgeSeqs) ->
    PartMask = 1 bsl PartId,
    case PartMask band Abitmask of
    0 ->
        case PartMask band Pbitmask of
        PartMask ->
            set_passive_partitions(Rest, Abitmask, Pbitmask, Seqs, PurgeSeqs);
        0 ->
            NewSeqs = lists:ukeymerge(1, [{PartId, 0}], Seqs),
            NewPurgeSeqs = lists:ukeymerge(1, [{PartId, 0}], PurgeSeqs),
            set_passive_partitions(
                Rest, Abitmask, Pbitmask bor PartMask, NewSeqs, NewPurgeSeqs)
        end;
    PartMask ->
        set_passive_partitions(
            Rest, Abitmask bxor PartMask, Pbitmask bor PartMask, Seqs, PurgeSeqs)
    end.


set_active_partitions([], Abitmask, Pbitmask, Seqs, PurgeSeqs) ->
    {ok, Abitmask, Pbitmask, Seqs, PurgeSeqs};

set_active_partitions([PartId | Rest], Abitmask, Pbitmask, Seqs, PurgeSeqs) ->
    PartMask = 1 bsl PartId,
    case PartMask band Pbitmask of
    0 ->
        case PartMask band Abitmask of
        PartMask ->
            set_active_partitions(Rest, Abitmask, Pbitmask, Seqs, PurgeSeqs);
        0 ->
            NewSeqs = lists:ukeymerge(1, Seqs, [{PartId, 0}]),
            NewPurgeSeqs = lists:ukeymerge(1, PurgeSeqs, [{PartId, 0}]),
            set_active_partitions(
                Rest, Abitmask bor PartMask, Pbitmask, NewSeqs, NewPurgeSeqs)
        end;
    PartMask ->
        set_active_partitions(
            Rest, Abitmask bor PartMask, Pbitmask bxor PartMask, Seqs, PurgeSeqs)
    end.


set_cleanup_partitions([], Abitmask, Pbitmask, Cbitmask, Seqs, PurgeSeqs) ->
    {ok, Abitmask, Pbitmask, Cbitmask, Seqs, PurgeSeqs};

set_cleanup_partitions([PartId | Rest], Abitmask, Pbitmask, Cbitmask, Seqs, PurgeSeqs) ->
    PartMask = 1 bsl PartId,
    case PartMask band Cbitmask of
    PartMask ->
        set_cleanup_partitions(Rest, Abitmask, Pbitmask, Cbitmask, Seqs, PurgeSeqs);
    0 ->
        Seqs2 = lists:keydelete(PartId, 1, Seqs),
        PurgeSeqs2 = lists:keydelete(PartId, 1, PurgeSeqs),
        Cbitmask2 = Cbitmask bor PartMask,
        case PartMask band Abitmask of
        PartMask ->
            set_cleanup_partitions(
                Rest, Abitmask bxor PartMask, Pbitmask, Cbitmask2, Seqs2, PurgeSeqs2);
        0 ->
            case (PartMask band Pbitmask) of
            PartMask ->
                set_cleanup_partitions(
                    Rest, Abitmask, Pbitmask bxor PartMask, Cbitmask2, Seqs2, PurgeSeqs2);
            0 ->
                set_cleanup_partitions(
                    Rest, Abitmask, Pbitmask, Cbitmask, Seqs, PurgeSeqs)
            end
        end
    end.


update_header(State, NewAbitmask, NewPbitmask, NewCbitmask, NewSeqs, NewPurgeSeqs, NewRelicasOnTransfer, NewReplicaParts) ->
    #state{
        group = #set_view_group{
            index_header =
                #set_view_index_header{
                    abitmask = Abitmask,
                    pbitmask = Pbitmask,
                    cbitmask = Cbitmask,
                    replicas_on_transfer = ReplicasOnTransfer
                } = Header,
            views = Views
        } = Group,
        replica_partitions = ReplicaParts
    } = State,
    NewState = State#state{
        group = Group#set_view_group{
            index_header = Header#set_view_index_header{
                abitmask = NewAbitmask,
                pbitmask = NewPbitmask,
                cbitmask = NewCbitmask,
                seqs = NewSeqs,
                purge_seqs = NewPurgeSeqs,
                replicas_on_transfer = NewRelicasOnTransfer
            },
            views = lists:map(
                fun(V) ->
                    V#set_view{update_seqs = NewSeqs, purge_seqs = NewPurgeSeqs}
                end, Views)
        },
        replica_partitions = NewReplicaParts
    },
    ok = commit_header(NewState#state.group, true),
    case (NewAbitmask =:= Abitmask) andalso (NewPbitmask =:= Pbitmask) of
    true ->
        ok;
    false ->
        {ActiveList, PassiveList} = make_partition_lists(NewState#state.group),
        ok = couch_db_set:set_active(?db_set(NewState), ActiveList),
        ok = couch_db_set:set_passive(?db_set(NewState), PassiveList)
    end,
    ?LOG_INFO("Set view `~s`, ~s group `~s`, partition states updated~n"
        "active partitions before:  ~w~n"
        "active partitions after:   ~w~n"
        "passive partitions before: ~w~n"
        "passive partitions after:  ~w~n"
        "cleanup partitions before: ~w~n"
        "cleanup partitions after:  ~w~n" ++
        case is_pid(State#state.replica_group) of
        true ->
            "replica partitions before:   ~w~n"
            "replica partitions after:    ~w~n"
            "replicas on transfer before: ~w~n"
            "replicas on transfer after:  ~w~n";
        false ->
            ""
        end,
        [?set_name(State), ?type(State), ?group_id(State),
         couch_set_view_util:decode_bitmask(Abitmask),
         couch_set_view_util:decode_bitmask(NewAbitmask),
         couch_set_view_util:decode_bitmask(Pbitmask),
         couch_set_view_util:decode_bitmask(NewPbitmask),
         couch_set_view_util:decode_bitmask(Cbitmask),
         couch_set_view_util:decode_bitmask(NewCbitmask)] ++
         case is_pid(State#state.replica_group) of
         true ->
             [ReplicaParts, NewReplicaParts, ReplicasOnTransfer, NewRelicasOnTransfer];
         false ->
             []
         end),
    NewState.


maybe_start_cleaner(#state{cleaner_pid = Pid} = State) when is_pid(Pid) ->
    State;
maybe_start_cleaner(#state{group = Group} = State) ->
    case is_pid(State#state.compactor_pid) orelse
        is_pid(State#state.updater_pid) orelse (?set_cbitmask(Group) == 0) of
    true ->
        State;
    false ->
        Cleaner = spawn_link(fun() -> cleaner(State) end),
        ?LOG_INFO("Started cleanup process ~p for set view `~s`, ~s group `~s`",
                  [Cleaner, ?set_name(State), ?type(State), ?group_id(State)]),
        State#state{cleaner_pid = Cleaner}
    end.


stop_cleaner(#state{cleaner_pid = nil} = State) ->
    State;
stop_cleaner(#state{cleaner_pid = Pid, group = OldGroup} = State) when is_pid(Pid) ->
    ?LOG_INFO("Stopping cleanup process for set view `~s`, group `~s`",
        [?set_name(State), ?group_id(State)]),
    Pid ! stop,
    receive
    {'EXIT', Pid, {clean_group, NewGroup, Count, Time}} ->
        ?LOG_INFO("Stopped cleanup process for set view `~s`, ~s group `~s`.~n"
             "Removed ~p values from the index in ~.3f seconds~n"
             "New set of partitions to cleanup: ~w~n"
             "Old set of partitions to cleanup: ~w~n",
             [?set_name(State), ?type(State), ?group_id(State), Count, Time,
                 couch_set_view_util:decode_bitmask(?set_cbitmask(NewGroup)),
                 couch_set_view_util:decode_bitmask(?set_cbitmask(OldGroup))]),
        case ?set_cbitmask(NewGroup) of
        0 ->
            NewStats = inc_cleanups(State#state.stats, Time, Count);
        _ ->
            NewStats = ?inc_cleanup_stops(State#state.stats)
        end,
        State2 = State#state{
            group = NewGroup,
            cleaner_pid = nil,
            stats = NewStats,
            commit_ref = schedule_commit(State)
        },
        notify_cleanup_waiters(State2);
    {'EXIT', Pid, Reason} ->
        exit({cleanup_process_died, Reason})
    end.


cleaner(#state{group = Group} = State) ->
    #set_view_group{
        index_header = Header,
        views = Views,
        id_btree = IdBtree,
        fd = Fd,
        sig = Sig
    } = Group,
    FileName = index_file_name(?root_dir(State), ?set_name(State), ?type(State), Sig),
    {ok, RawReadFd} = file:open(FileName, [binary, read, raw]),
    erlang:put({Fd, fast_fd_read}, RawReadFd),
    StartTime = now(),
    PurgeFun = couch_set_view_util:make_btree_purge_fun(Group),
    {ok, NewIdBtree, {Go, IdPurgedCount}} =
        couch_btree:guided_purge(IdBtree, PurgeFun, {go, 0}),
    {TotalPurgedCount, NewViews} = case Go of
    go ->
        clean_views(go, PurgeFun, Views, IdPurgedCount, []);
    stop ->
        {IdPurgedCount, Views}
    end,
    ok = file:close(RawReadFd),
    erlang:erase({Fd, fast_fd_read}),
    {ok, {_, IdBitmap}} = couch_btree:full_reduce(NewIdBtree),
    CombinedBitmap = lists:foldl(
        fun(#set_view{btree = Bt}, AccMap) ->
            {ok, {_, _, Bm}} = couch_btree:full_reduce(Bt),
            AccMap bor Bm
        end,
        IdBitmap, NewViews),
    NewCbitmask = ?set_cbitmask(Group) band CombinedBitmap,
    NewGroup = Group#set_view_group{
        id_btree = NewIdBtree,
        views = NewViews,
        index_header = Header#set_view_index_header{cbitmask = NewCbitmask}
    },
    Duration = timer:now_diff(now(), StartTime) / 1000000,
    commit_header(NewGroup, true),
    exit({clean_group, NewGroup, TotalPurgedCount, Duration}).


clean_views(_, _, [], Count, Acc) ->
    {Count, lists:reverse(Acc)};
clean_views(stop, _, Rest, Count, Acc) ->
    {Count, lists:reverse(Acc, Rest)};
clean_views(go, PurgeFun, [#set_view{btree = Btree} = View | Rest], Count, Acc) ->
    {ok, NewBtree, {Go, PurgedCount}} =
        couch_btree:guided_purge(Btree, PurgeFun, {go, Count}),
    NewAcc = [View#set_view{btree = NewBtree} | Acc],
    clean_views(Go, PurgeFun, Rest, PurgedCount, NewAcc).


index_needs_update(#state{group = Group} = State) ->
    {ok, CurSeqs} = couch_db_set:get_seqs(?db_set(State)),
    CurSeqs > ?set_seqs(Group).


make_partition_lists(Group) ->
    make_partition_lists(?set_seqs(Group), ?set_abitmask(Group), ?set_pbitmask(Group), [], []).

make_partition_lists([], _Abitmask, _Pbitmask, Active, Passive) ->
    {lists:reverse(Active), lists:reverse(Passive)};
make_partition_lists([{PartId, _} | Rest], Abitmask, Pbitmask, Active, Passive) ->
    Mask = 1 bsl PartId,
    case Mask band Abitmask of
    0 ->
        Mask = Mask band Pbitmask,
        make_partition_lists(Rest, Abitmask, Pbitmask, Active, [PartId | Passive]);
    Mask ->
        make_partition_lists(Rest, Abitmask, Pbitmask, [PartId | Active], Passive)
    end.


start_compactor(State, CompactFun) ->
    #state{group = Group} = State2 = stop_cleaner(State),
    ?LOG_INFO("Set view `~s`, ~s group `~s`, compaction starting",
              [?set_name(State2), ?type(State), ?group_id(State2)]),
    #set_view_group{
        fd = CompactFd
    } = NewGroup = compact_group(State2),
    unlink(CompactFd),
    Pid = spawn_link(fun() ->
        link(CompactFd),
        FileName = index_file_name(State2),
        CompactFileName = index_file_name(State2, compact),
        CompactFun(Group, NewGroup, ?set_name(State2), FileName, CompactFileName),
        unlink(CompactFd)
    end),
    State2#state{
        compactor_pid = Pid,
        compactor_fun = CompactFun,
        compactor_file = CompactFd
    }.


restart_compactor(#state{compactor_pid = nil} = State, _Reason) ->
    State;
restart_compactor(#state{compactor_pid = Pid, compactor_file = CompactFd} = State, Reason) ->
    true = is_process_alive(Pid),
    ?LOG_INFO("Restarting compaction for ~s group `~s`, set view `~s`. Reason: ~s",
        [?type(State), ?group_id(State), ?set_name(State), Reason]),
    couch_util:shutdown_sync(Pid),
    couch_util:shutdown_sync(CompactFd),
    State2 = case ?set_cbitmask(State#state.group) of
    0 ->
        State;
    _ ->
        State#state{stats = ?inc_cleanup_stops(State#state.stats)}
    end,
    start_compactor(State2, State2#state.compactor_fun).


compact_group(State) ->
    #state{
        group = #set_view_group{sig = GroupSig} = Group
    } = State,
    {ok, Fd} = open_index_file(
        compact, ?root_dir(State), ?set_name(State), ?type(State), GroupSig),
    reset_file(Fd, ?set_name(State), Group).


stop_updater(State) ->
    stop_updater(State, after_active_indexed).

stop_updater(#state{updater_pid = nil} = State, _When) ->
    State;
stop_updater(#state{updater_pid = Pid} = State, When) ->
    case When of
    after_active_indexed ->
        Pid ! stop_after_active,
        ?LOG_INFO("Stopping updater for set view `~s`, ~s group `~s`, as soon "
            "as all active partitions are processed",
            [?set_name(State), ?type(State), ?group_id(State)]);
    immediately ->
        Pid ! stop_immediately,
        ?LOG_INFO("Stopping updater for set view `~s`, ~s group `~s`, immediately",
            [?set_name(State), ?type(State), ?group_id(State)])
    end,
    receive
    {'EXIT', Pid, {updater_finished, NewGroup, UpdaterFinishState, Duration}} ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, updater stopped, ran for ~.3f seconds",
            [?set_name(State), ?type(State), ?group_id(State), Duration]),
        State2 = process_partial_update(State, NewGroup),
        case UpdaterFinishState of
        updating_active ->
            NewStats = ?inc_updater_stops(State2#state.stats),
            WaitingList2 = State2#state.waiting_list;
        updating_passive ->
            NewStats = case ?set_pbitmask(NewGroup) of
            0 ->
                inc_updates(State2#state.stats, Duration);
            _ ->
                ?inc_partial_updates(State2#state.stats)
            end,
            reply_with_group(NewGroup, NewStats, State2#state.waiting_list),
            WaitingList2 = []
        end,
        NewState = State2#state{
            updater_pid = nil,
            updater_state = not_running,
            waiting_list = WaitingList2,
            stats = NewStats
        },
        notify_cleanup_waiters(NewState);
    {'EXIT', Pid, Reason} ->
        ?LOG_ERROR("Updater, set view `~s`, ~s group `~s`, died with "
            "unexpected reason: ~p",
            [?set_name(State), ?type(State), ?group_id(State), Reason]),
        State#state{updater_pid = nil, updater_state = not_running}
    end.


start_updater(#state{updater_pid = Pid} = State) when is_pid(Pid) ->
    State;
start_updater(#state{updater_pid = nil, updater_state = not_running} = State) ->
    case index_needs_update(State) of
    true ->
        do_start_updater(State);
    false ->
        case State#state.waiting_list of
        [] ->
            State;
        _ ->
            reply_with_group(State#state.group, State#state.stats, State#state.waiting_list),
            State#state{waiting_list = []}
        end
    end.


do_start_updater(State) ->
    #state{group = Group, stats = Stats} = State2 = stop_cleaner(State),
    ?LOG_INFO("Starting updater for set view `~s`, ~s group `~s`",
        [?set_name(State), ?type(State), ?group_id(State)]),
    Owner = self(),
    Pid = spawn_link(fun() ->
        couch_set_view_updater:update(Owner, Group, index_file_name(State))
    end),
    State2#state{
        updater_pid = Pid,
        updater_state = starting,
        stats = Stats#set_view_group_stats{
            current_updater_kv_cleanup_count = 0
        }
    }.


partitions_still_in_cleanup(Parts, Group) ->
    partitions_still_in_cleanup(Parts, Group, [], []).

partitions_still_in_cleanup([], _Group, AccStill, AccNot) ->
    {lists:reverse(AccStill), lists:reverse(AccNot)};
partitions_still_in_cleanup([PartId | Rest], Group, AccStill, AccNot) ->
    Mask = 1 bsl PartId,
    case Mask band ?set_cbitmask(Group) of
    Mask ->
        partitions_still_in_cleanup(Rest, Group, [PartId | AccStill], AccNot);
    0 ->
        partitions_still_in_cleanup(Rest, Group, AccStill, [PartId | AccNot])
    end.


% TODO: instead of applying a group of state updates one by one and unblocking cleanup
% waiters one by one, these state updates should be collapsed as soon they arrive and
% applied all at once. This would also avoids the need to block clients when they ask
% to mark partitions as active/passive when they're still in cleanup.
notify_cleanup_waiters(#state{cleanup_waiters = []} = State) ->
    State;
notify_cleanup_waiters(#state{group = Group} = State) when ?set_cbitmask(Group) =/= 0 ->
    State;
notify_cleanup_waiters(State) ->
    #state{group = Group, cleanup_waiters = [Waiter | RestWaiters]} = State,
    #cleanup_waiter{
        from = From,
        active_list = Active,
        passive_list = Passive,
        cleanup_list = Cleanup
    } = Waiter,
    {InCleanup, _NotInCleanup} =
        partitions_still_in_cleanup(Active ++ Passive, Group),
    case InCleanup of
    [] ->
        State2 = persist_partition_states(State, Active, Passive, Cleanup),
        % TODO: track how much time a cleanup waiter is blocked and log it
        ?LOG_INFO("Set view `~s`, ~s group `~s`, unblocking cleanup waiter ~p",
            [?set_name(State2), ?type(State), ?group_id(State2), element(1, From)]),
        gen_server:reply(From, ok),
        State2#state{cleanup_waiters = RestWaiters};
    _ ->
        State
    end.


open_replica_group({RootDir, SetName, Group} = _InitArgs) ->
    ReplicaArgs = {RootDir, SetName, Group#set_view_group{type = replica}},
    {ok, Pid} = proc_lib:start_link(?MODULE, init, [ReplicaArgs]),
    Pid.


get_replica_partitions(ReplicaPid) ->
    {ok, Group, 0} = gen_server:call(ReplicaPid, {request_group, ok}, infinity),
    ordsets:from_list(couch_set_view_util:decode_bitmask(
        ?set_abitmask(Group) bor ?set_pbitmask(Group))).


maybe_update_replica_index(#state{updater_pid = Pid} = State) when is_pid(Pid) ->
    State;
maybe_update_replica_index(#state{group = Group, updater_state = not_running} = State) ->
    {ok, CurSeqs} = couch_db_set:get_seqs(?db_set(State)),
    ChangesCount = lists:foldl(
        fun({{PartId, CurSeq}, {PartId, UpSeq}}, Acc) when CurSeq >= UpSeq ->
            Acc + (CurSeq - UpSeq)
        end,
        0, lists:zip(CurSeqs, ?set_seqs(Group))),
    case (ChangesCount >= ?MIN_CHANGES_AUTO_UPDATE) orelse
        (ChangesCount > 0 andalso ?set_cbitmask(Group) =/= 0) of
    true ->
        do_start_updater(State);
    false ->
        maybe_start_cleaner(State)
    end.


maybe_fix_replica_group(ReplicaPid, Group) ->
    {ok, RepGroup, 0} = gen_server:call(ReplicaPid, {request_group, ok}, infinity),
    RepGroupActive = couch_set_view_util:decode_bitmask(?set_abitmask(RepGroup)),
    RepGroupPassive = couch_set_view_util:decode_bitmask(?set_pbitmask(RepGroup)),
    CleanupList = lists:foldl(
        fun(PartId, Acc) ->
            case ordsets:is_element(PartId, ?set_replicas_on_transfer(Group)) of
            true ->
                Acc;
            false ->
                [PartId | Acc]
            end
        end,
        [], RepGroupActive),
    ActiveList = lists:foldl(
        fun(PartId, Acc) ->
            case ordsets:is_element(PartId, ?set_replicas_on_transfer(Group)) of
            true ->
                [PartId | Acc];
            false ->
                Acc
            end
        end,
        [], RepGroupPassive),
    ok = set_state(ReplicaPid, ActiveList, [], CleanupList).


schedule_commit(#state{commit_ref = Ref}) when is_reference(Ref) ->
    Ref;
schedule_commit(_State) ->
    erlang:send_after(?DELAYED_COMMIT_PERIOD, self(), delayed_commit).


cancel_commit(#state{commit_ref = Ref}) when is_reference(Ref) ->
    erlang:cancel_timer(Ref);
cancel_commit(_State) ->
    ok.


process_partial_update(#state{group = Group} = State, NewGroup) ->
    ReplicasTransferred = ordsets:subtract(
        ?set_replicas_on_transfer(Group), ?set_replicas_on_transfer(NewGroup)),
    case ReplicasTransferred of
    [] ->
        CommitRef2 = schedule_commit(State);
    _ ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, completed transferral of replica partitions ~w~n"
                  "New group of replica partitions to transfer is ~w~n",
                  [?set_name(State), ?type(State), ?group_id(State),
                   ReplicasTransferred, ?set_replicas_on_transfer(NewGroup)]),
        commit_header(NewGroup, true),
        ok = set_state(State#state.replica_group, [], [], ReplicasTransferred),
        cancel_commit(State),
        CommitRef2 = nil
    end,
    State#state{
        group = NewGroup,
        commit_ref = CommitRef2,
        replica_partitions = ordsets:subtract(State#state.replica_partitions, ReplicasTransferred)
    }.


inc_updates(Stats, Duration) ->
    #set_view_group_stats{
        update_history = Hist,
        current_updater_kv_cleanup_count = CleanupKvCount
    } = Stats,
    Entry = {[
        {<<"duration">>, Duration},
        {<<"cleanup_kv_count">>, CleanupKvCount}
    ]},
    Stats#set_view_group_stats{
        updates = Stats#set_view_group_stats.updates + 1,
        update_history = lists:sublist([Entry | Hist], ?MAX_HIST_SIZE)
    }.


inc_cleanups(Stats, Duration, Count) ->
    inc_cleanups(Stats, Duration, Count, false).

inc_cleanups(#set_view_group_stats{cleanup_history = Hist} = Stats, Duration, Count, ByUpdater) ->
    Entry = {[
        {<<"duration">>, Duration},
        {<<"kv_count">>, Count}
    ]},
    Stats#set_view_group_stats{
        cleanups = Stats#set_view_group_stats.cleanups + 1,
        cleanup_history = lists:sublist([Entry | Hist], ?MAX_HIST_SIZE),
        updater_cleanups = case ByUpdater of
            true ->
                Stats#set_view_group_stats.updater_cleanups + 1;
            false ->
                Stats#set_view_group_stats.updater_cleanups
            end
    }.

inc_compactions(#set_view_group_stats{compaction_history = Hist} = Stats, Duration, CleanupKVCount) ->
    Entry = {[
        {<<"duration">>, Duration},
        {<<"cleanup_kv_count">>, CleanupKVCount}
    ]},
    Stats#set_view_group_stats{
        compactions = Stats#set_view_group_stats.compactions + 1,
        compaction_history = lists:sublist([Entry | Hist], ?MAX_HIST_SIZE),
        cleanups = case CleanupKVCount of
            0 ->
                Stats#set_view_group_stats.cleanups;
            _ ->
                Stats#set_view_group_stats.cleanups + 1
        end
    }.
