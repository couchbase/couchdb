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
-export([start_link/1, request_group_info/1, get_data_size/1]).
-export([open_set_group/2]).
-export([request_group/2, release_group/1]).
-export([is_view_defined/1, define_view/2]).
-export([set_state/4]).
-export([add_replica_partitions/2, remove_replica_partitions/2]).
-export([mark_as_unindexable/2, mark_as_indexable/2]).
-export([monitor_partition_update/4, demonitor_partition_update/2]).
-export([reset_utilization_stats/1, get_utilization_stats/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

-type init_args()   :: {string(), binary(), #set_view_group{}}.
-type compact_fun() :: fun((#set_view_group{},
                            #set_view_group{},
                            string(),
                            pid() | 'nil',
                            pid()) -> no_return()).
-type monitor_error() :: {'shutdown', any()} |
                         'marked_for_cleanup' |
                         {'updater_error', any()}.

-define(TIMEOUT, 3000).

-define(root_dir(State), element(1, State#state.init_args)).
-define(set_name(State), element(2, State#state.init_args)).
-define(type(State), (element(3, State#state.init_args))#set_view_group.type).
-define(group_sig(State), (element(3, State#state.init_args))#set_view_group.sig).
-define(group_id(State), (State#state.group)#set_view_group.name).
-define(db_set(State), (State#state.group)#set_view_group.db_set).
-define(is_defined(State),
    (((State#state.group)#set_view_group.index_header)#set_view_index_header.num_partitions > 0)).
-define(replicas_on_transfer(State),
        ((State#state.group)#set_view_group.index_header)#set_view_index_header.replicas_on_transfer).
-define(have_pending_transition(State),
        ((((State#state.group)#set_view_group.index_header)
          #set_view_index_header.pending_transition) /= nil)).

-define(MAX_HIST_SIZE, 10).

-record(util_stats, {
    useful_indexing_time = 0.0  :: float(),
    wasted_indexing_time = 0.0  :: float(),
    updates = 0                 :: non_neg_integer(),
    updater_interruptions = 0   :: non_neg_integer(),
    compaction_time = 0.0       :: float(),
    compactions = 0             :: non_neg_integer(),
    compactor_interruptions = 0 :: non_neg_integer()
}).

-record(up_listener, {
    pid,
    monref,
    partition,
    seq
}).

-record(waiter, {
    from,
    debug = false :: boolean(),
    % seqs for active partitions only
    seqs = []     :: partition_seqs()
}).

-record(state, {
    init_args                          :: init_args(),
    replica_group = nil                :: 'nil' | pid(),
    group = #set_view_group{}          :: #set_view_group{},
    updater_pid = nil                  :: 'nil' | pid(),
    initial_build = false              :: boolean(),
    updater_state = not_running        :: set_view_updater_state() | 'not_running' | 'starting',
    compactor_pid = nil                :: 'nil' | pid(),
    compactor_file = nil               :: 'nil' | pid(),
    compactor_fun = nil                :: 'nil' | compact_fun(),
    waiting_list = []                  :: [#waiter{}],
    cleaner_pid = nil                  :: 'nil' | pid(),
    shutdown = false                   :: boolean(),
    shutdown_aliases                   :: [binary()],
    auto_cleanup = true                :: boolean(),
    replica_partitions = []            :: ordsets:ordset(partition_id()),
    pending_transition_waiters = []    :: [{From::{pid(), reference()}, #set_view_group_req{}}],
    update_listeners = dict:new()      :: dict(),
    compact_log_files = nil            :: 'nil' | {[[string()]], partition_seqs()},
    % Monitor references for active, passive and replica partitions.
    % Applies to main group only, replica group must always have an empty dict.
    db_refs = dict:new()               :: dict()
}).

-define(inc_stat(Group, S),
    ets:update_counter(
        ?SET_VIEW_STATS_ETS,
        ?set_view_group_stats_key(Group),
        {S, 1})).
-define(inc_cleanup_stops(Group), ?inc_stat(Group, #set_view_group_stats.cleanup_stops)).
-define(inc_updater_errors(Group), ?inc_stat(Group, #set_view_group_stats.update_errors)).
-define(inc_accesses(Group), ?inc_stat(Group, #set_view_group_stats.accesses)).


% api methods
-spec request_group(pid(), #set_view_group_req{}) ->
                   {'ok', #set_view_group{}} | {'error', term()}.
request_group(Pid, Req) ->
    #set_view_group_req{wanted_partitions = WantedPartitions} = Req,
    Req2 = Req#set_view_group_req{
        wanted_partitions = ordsets:from_list(WantedPartitions)
    },
    request_group(Pid, Req2, 1).

-spec request_group(pid(), #set_view_group_req{}, non_neg_integer()) ->
                   {'ok', #set_view_group{}} | {'error', term()}.
request_group(Pid, Req, Retries) ->
    case gen_server:call(Pid, Req, infinity) of
    {ok, Group, ActiveReplicasBitmask} ->
        #set_view_group{
            ref_counter = RefCounter,
            replica_pid = RepPid,
            name = GroupName,
            set_name = SetName
        } = Group,
        case request_replica_group(RepPid, ActiveReplicasBitmask, Req) of
        {ok, RepGroup} ->
            {ok, Group#set_view_group{replica_group = RepGroup}};
        retry ->
            couch_ref_counter:drop(RefCounter),
            ?LOG_INFO("Retrying group `~s` request, stale=~s,"
                  " set `~s`, retry attempt #~p",
                  [GroupName, Req#set_view_group_req.stale, SetName, Retries]),
            request_group(Pid, Req, Retries + 1)
        end;
    Error ->
        Error
    end.


-spec request_replica_group(pid(), bitmask(), #set_view_group_req{}) ->
                           {'ok', #set_view_group{} | 'nil'} | 'retry'.
request_replica_group(_RepPid, 0, _Req) ->
    {ok, nil};
request_replica_group(RepPid, ActiveReplicasBitmask, Req) ->
    {ok, RepGroup, 0} = gen_server:call(RepPid, Req, infinity),
    case ?set_abitmask(RepGroup) =:= ActiveReplicasBitmask of
    true ->
        {ok, RepGroup};
    false ->
        couch_ref_counter:drop(RepGroup#set_view_group.ref_counter),
        retry
    end.


-spec release_group(#set_view_group{}) -> no_return().
release_group(#set_view_group{ref_counter = RefCounter, replica_group = RepGroup}) ->
    couch_ref_counter:drop(RefCounter),
    case RepGroup of
    #set_view_group{ref_counter = RepRefCounter} ->
        couch_ref_counter:drop(RepRefCounter);
    nil ->
        ok
    end.


-spec request_group_info(pid()) -> {'ok', [{term(), term()}]}.
request_group_info(Pid) ->
    case gen_server:call(Pid, request_group_info, infinity) of
    {ok, GroupInfoList} ->
        {ok, GroupInfoList};
    Error ->
        throw(Error)
    end.


-spec get_data_size(pid()) -> {'ok', [{term(), term()}]}.
get_data_size(Pid) ->
    case gen_server:call(Pid, get_data_size, infinity) of
    {ok, _Info} = Ok ->
        Ok;
    Error ->
        throw(Error)
    end.


-spec define_view(pid(), #set_view_params{}) -> 'ok' | {'error', term()}.
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


-spec is_view_defined(pid()) -> boolean().
is_view_defined(Pid) ->
    gen_server:call(Pid, is_view_defined, infinity).


-spec set_state(pid(),
                ordsets:ordset(partition_id()),
                ordsets:ordset(partition_id()),
                ordsets:ordset(partition_id())) -> 'ok' | {'error', term()}.
set_state(_Pid, [], [], []) ->
    ok;
set_state(Pid, Active, Passive, Cleanup) ->
    ordsets:is_set(Active) orelse throw({error, <<"Active list is not an ordset">>}),
    ordsets:is_set(Passive) orelse throw({error, <<"Passive list is not an ordset">>}),
    case ordsets:intersection(Active, Passive) of
    [] ->
        ordsets:is_set(Cleanup) orelse throw({error, <<"Cleanup list is not an ordset">>}),
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


-spec add_replica_partitions(pid(), ordsets:ordset(partition_id())) -> 'ok' | {'error', term()}.
add_replica_partitions(_Pid, []) ->
    ok;
add_replica_partitions(Pid, Partitions) ->
    BitMask = couch_set_view_util:build_bitmask(Partitions),
    gen_server:call(Pid, {add_replicas, BitMask}, infinity).


-spec remove_replica_partitions(pid(), ordsets:ordset(partition_id())) -> 'ok' | {'error', term()}.
remove_replica_partitions(_Pid, []) ->
    ok;
remove_replica_partitions(Pid, Partitions) ->
    ordsets:is_set(Partitions) orelse throw({error, <<"List is not an ordset">>}),
    gen_server:call(Pid, {remove_replicas, Partitions}, infinity).


-spec mark_as_unindexable(pid(), ordsets:ordset(partition_id())) ->
                                 'ok' | {'error', term()}.
mark_as_unindexable(Pid, Partitions) ->
    ordsets:is_set(Partitions) orelse throw({error, <<"List is not an ordset">>}),
    gen_server:call(Pid, {mark_as_unindexable, Partitions}, infinity).


-spec mark_as_indexable(pid(), ordsets:ordset(partition_id())) ->
                               'ok' | {'error', term()}.
mark_as_indexable(Pid, Partitions) ->
    ordsets:is_set(Partitions) orelse throw({error, <<"List is not an ordset">>}),
    gen_server:call(Pid, {mark_as_indexable, Partitions}, infinity).


-spec monitor_partition_update(pid(), partition_id(), reference(), pid()) ->
                               'ok' | {'error', term()}.
monitor_partition_update(Pid, PartitionId, Ref, CallerPid) ->
    gen_server:call(
        Pid, {monitor_partition_update, PartitionId, Ref, CallerPid}, infinity).


-spec demonitor_partition_update(pid(), reference()) -> 'ok'.
demonitor_partition_update(Pid, Ref) ->
    ok = gen_server:call(Pid, {demonitor_partition_update, Ref}, infinity).


-spec reset_utilization_stats(pid()) -> 'ok'.
reset_utilization_stats(Pid) ->
    ok = gen_server:call(Pid, reset_utilization_stats, infinity).


-spec get_utilization_stats(pid()) -> {'ok', [{atom() | binary(), term()}]}.
get_utilization_stats(Pid) ->
    gen_server:call(Pid, get_utilization_stats, infinity).


start_link({RootDir, SetName, Group}) ->
    Args = {RootDir, SetName, Group#set_view_group{type = main}},
    proc_lib:start_link(?MODULE, init, [Args]).


init({_, _, Group} = InitArgs) ->
    process_flag(trap_exit, true),
    {ok, State} = try
        do_init(InitArgs)
    catch
    _:Error ->
        ?LOG_ERROR("~s error opening set view group `~s`, signature `~s', from set `~s`: ~p",
                   [?MODULE, Group#set_view_group.name, hex_sig(Group),
                    Group#set_view_group.set_name, Error]),
        exit(Error)
    end,
    proc_lib:init_ack({ok, self()}),
    gen_server:enter_loop(?MODULE, [], State, 1).


do_init({_, SetName, _} = InitArgs) ->
    case prepare_group(InitArgs, false) of
    {ok, #set_view_group{fd = Fd, index_header = Header, type = Type} = Group} ->
        RefCounter = new_fd_ref_counter(Fd),
        PartitionsList = make_partitions_list(Group),
        case Header#set_view_index_header.has_replica of
        false ->
            ReplicaPid = nil,
            ReplicaParts = [];
        true ->
            ReplicaPid = open_replica_group(InitArgs),
            maybe_fix_replica_group(ReplicaPid, Group),
            ReplicaParts = get_replica_partitions(ReplicaPid)
        end,
        ViewCount = length(Group#set_view_group.views),
        case Header#set_view_index_header.num_partitions > 0 of
        false ->
            DbSet = nil,
            ?LOG_INFO("Started undefined ~s set view group `~s`, group `~s`,"
                      " signature `~s', view count: ~p",
                      [Type, SetName, Group#set_view_group.name, hex_sig(Group), ViewCount]);
        true ->
            DbSet = case (catch couch_db_set:open(SetName, PartitionsList)) of
            {ok, SetPid} ->
                SetPid;
            Error ->
                throw(Error)
            end,
            ?LOG_INFO("Started ~s set view group `~s`, group `~s`, signature `~s', view count ~p~n"
                      "active partitions:      ~w~n"
                      "passive partitions:     ~w~n"
                      "cleanup partitions:     ~w~n"
                      "unindexable partitions: ~w~n"
                      "~sreplica support~n" ++
                      case Header#set_view_index_header.has_replica of
                      true ->
                          "replica partitions: ~w~n"
                          "replica partitions on transfer: ~w~n";
                      false ->
                          ""
                      end,
                      [Type, SetName, Group#set_view_group.name, hex_sig(Group), ViewCount,
                       couch_set_view_util:decode_bitmask(Header#set_view_index_header.abitmask),
                       couch_set_view_util:decode_bitmask(Header#set_view_index_header.pbitmask),
                       couch_set_view_util:decode_bitmask(Header#set_view_index_header.cbitmask),
                       Header#set_view_index_header.unindexable_seqs,
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
        State = #state{
            init_args = InitArgs,
            replica_group = ReplicaPid,
            replica_partitions = ReplicaParts,
            group = Group#set_view_group{
                ref_counter = RefCounter,
                db_set = DbSet,
                replica_pid = ReplicaPid
            }
        },
        State2 = monitor_partitions(State, [master | PartitionsList]),
        State3 = monitor_partitions(State2, ReplicaParts),
        true = ets:insert(
             ?SET_VIEW_STATS_ETS,
             #set_view_group_stats{ets_key = ?set_view_group_stats_key(Group)}),
        TmpDir = updater_tmp_dir(State),
        ok = couch_set_view_util:delete_sort_files(TmpDir, all),
        reset_util_stats(),
        {ok, maybe_apply_pending_transition(State3)};
    Error ->
        throw(Error)
    end.

handle_call(get_sig, _From, #state{group = Group} = State) ->
    {reply, {ok, Group#set_view_group.sig}, State, ?TIMEOUT};

handle_call({set_auto_cleanup, Enabled}, _From, State) ->
    % To be used only by unit tests.
    {reply, ok, State#state{auto_cleanup = Enabled}, ?TIMEOUT};

handle_call({define_view, NumPartitions, _, _, _, _, _}, _From, State)
        when (not ?is_defined(State)), NumPartitions > ?MAX_NUM_PARTITIONS ->
    {reply, {error, <<"Too high value for number of partitions">>}, State};

handle_call({define_view, NumPartitions, ActiveList, ActiveBitmask,
        PassiveList, PassiveBitmask, UseReplicaIndex}, _From, State) when not ?is_defined(State) ->
    #state{init_args = InitArgs, group = Group} = State,
    Seqs = lists:map(
        fun(PartId) -> {PartId, 0} end, lists:usort(ActiveList ++ PassiveList)),
    #set_view_group{
        name = DDocId,
        index_header = Header
    } = Group,
    NewHeader = Header#set_view_index_header{
        num_partitions = NumPartitions,
        abitmask = ActiveBitmask,
        pbitmask = PassiveBitmask,
        seqs = Seqs,
        has_replica = UseReplicaIndex
    },
    case (catch couch_db_set:open(?set_name(State), ActiveList ++ PassiveList)) of
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
            replica_pid = ReplicaPid
        },
        State2 = State#state{
            group = NewGroup,
            replica_group = ReplicaPid
        },
        State3 = monitor_partitions(State2, ActiveList),
        State4 = monitor_partitions(State3, PassiveList),
        ok = commit_header(NewGroup),
        ?LOG_INFO("Set view `~s`, ~s group `~s`, signature `~s', configured with:~n"
            "~p partitions~n"
            "~sreplica support~n"
            "initial active partitions ~w~n"
            "initial passive partitions ~w",
            [?set_name(State), ?type(State), DDocId, hex_sig(Group), NumPartitions,
            case UseReplicaIndex of
            true ->  "";
            false -> "no "
            end,
            ActiveList, PassiveList]),
        {reply, ok, State4, ?TIMEOUT};
    Error ->
        {reply, Error, State, ?TIMEOUT}
    end;

handle_call({define_view, _, _, _, _, _, _}, _From, State) ->
    {reply, view_already_defined, State, ?TIMEOUT};

handle_call(is_view_defined, _From, State) ->
    {reply, ?is_defined(State), State, ?TIMEOUT};

handle_call(_Msg, _From, State) when not ?is_defined(State) ->
    {reply, {error, view_undefined}, State};

handle_call({set_state, ActiveList, PassiveList, CleanupList}, _From, State) ->
    try
        NewState = maybe_update_partition_states(
            ActiveList, PassiveList, CleanupList, State),
        {reply, ok, NewState, ?TIMEOUT}
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
    Parts = couch_set_view_util:decode_bitmask(BitMask3),
    ok = set_state(ReplicaPid, [], Parts, []),
    NewReplicaParts = ordsets:union(ReplicaParts, Parts),
    ?LOG_INFO("Set view `~s`, ~s group `~s`, defined new replica partitions: ~w~n"
              "New full set of replica partitions is: ~w~n",
              [?set_name(State), ?type(State), ?group_id(State), Parts, NewReplicaParts]),
    State2 = State#state{
        replica_partitions = NewReplicaParts
    },
    State3 = monitor_partitions(State2, Parts),
    {reply, ok, State3, ?TIMEOUT};

handle_call({remove_replicas, Partitions}, _From, #state{replica_group = ReplicaPid} = State) when is_pid(ReplicaPid) ->
    #state{
        replica_partitions = ReplicaParts,
        group = Group
    } = State,
    State0 = demonitor_partitions(State, Partitions),
    case ordsets:intersection(?set_replicas_on_transfer(Group), Partitions) of
    [] ->
        ok = set_state(ReplicaPid, [], [], Partitions),
        NewState = State0#state{
            replica_partitions = ordsets:subtract(ReplicaParts, Partitions)
        };
    Common ->
        UpdaterWasRunning = is_pid(State#state.updater_pid),
        State2 = stop_cleaner(State0),
        #state{group = Group3} = State3 = stop_updater(State2),
        {ok, NewAbitmask, NewPbitmask, NewCbitmask, NewSeqs} =
            set_cleanup_partitions(
                Common,
                ?set_abitmask(Group3),
                ?set_pbitmask(Group3),
                ?set_cbitmask(Group3),
                ?set_seqs(Group3)),
        case NewCbitmask =/= ?set_cbitmask(Group3) of
        true ->
             State4 = stop_compactor(State3);
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
            ?set_unindexable_seqs(State4#state.group),
            ReplicasOnTransfer2,
            ReplicaPartitions2,
            ?set_pending_transition(State4#state.group)),
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

handle_call({mark_as_unindexable, Partitions}, _From, State) ->
    try
        State2 = process_mark_as_unindexable(State, Partitions),
        {reply, ok, State2}
    catch
    throw:Error ->
        {reply, Error, State}
    end;

handle_call({mark_as_indexable, Partitions}, _From, State) ->
    try
        State2 = process_mark_as_indexable(State, Partitions, true),
        {reply, ok, State2}
    catch
    throw:Error ->
        {reply, Error, State}
    end;

handle_call(#set_view_group_req{} = Req, From, State) ->
    #state{
        group = Group,
        pending_transition_waiters = Waiters
    } = State,
    State2 = case is_any_partition_pending(Req, Group) of
    false ->
        process_view_group_request(Req, From, State);
    true ->
        State#state{pending_transition_waiters = [{From, Req} | Waiters]}
    end,
    inc_view_group_access_stats(Req, State2#state.group),
    {noreply, State2, ?TIMEOUT};

handle_call(request_group, _From, #state{group = Group} = State) ->
    % Meant to be called only by this module and the compactor module.
    % Callers aren't supposed to read from the group's fd, we don't
    % increment here the ref counter on behalf of the caller.
    {reply, {ok, Group}, State, ?TIMEOUT};

handle_call(replica_pid, _From, #state{replica_group = Pid} = State) ->
    % To be used only by unit tests.
    {reply, {ok, Pid}, State, ?TIMEOUT};

handle_call({start_updater, Options}, _From, State) ->
    % To be used only by unit tests.
    State2 = start_updater(State, Options),
    {reply, {ok, State2#state.updater_pid}, State2, ?TIMEOUT};

handle_call(start_cleaner, _From, State) ->
    % To be used only by unit tests.
    State2 = maybe_start_cleaner(State#state{auto_cleanup = true}),
    State3 = State2#state{auto_cleanup = State#state.auto_cleanup},
    {reply, {ok, State2#state.cleaner_pid}, State3, ?TIMEOUT};

handle_call(updater_pid, _From, #state{updater_pid = Pid} = State) ->
    % To be used only by unit tests.
    {reply, {ok, Pid}, State, ?TIMEOUT};

handle_call(cleaner_pid, _From, #state{cleaner_pid = Pid} = State) ->
    % To be used only by unit tests.
    {reply, {ok, Pid}, State, ?TIMEOUT};

handle_call(request_group_info, _From, State) ->
    GroupInfo = get_group_info(State),
    {reply, {ok, GroupInfo}, State, ?TIMEOUT};

handle_call(get_data_size, _From, State) ->
    DataSizeInfo = get_data_size_info(State),
    {reply, {ok, DataSizeInfo}, State, ?TIMEOUT};

handle_call({start_compact, _CompactFun}, _From,
            #state{updater_pid = UpPid, initial_build = true} = State) when is_pid(UpPid) ->
    {reply, {error, initial_build}, State};
handle_call({start_compact, CompactFun}, _From, #state{compactor_pid = nil} = State) ->
    #state{compactor_pid = Pid} = State2 = start_compactor(State, CompactFun),
    {reply, {ok, Pid}, State2};
handle_call({start_compact, _}, _From, State) ->
    %% compact already running, this is a no-op
    {reply, {ok, State#state.compactor_pid}, State};

handle_call({compact_done, Result}, {Pid, _}, #state{compactor_pid = Pid} = State) ->
    #state{
        update_listeners = Listeners,
        group = Group,
        updater_pid = UpdaterPid,
        compactor_pid = CompactorPid
    } = State,
    #set_view_group{
        fd = OldFd,
        ref_counter = RefCounter,
        filepath = OldFilepath
    } = Group,
    #set_view_compactor_result{
        group = NewGroup,
        compact_time = Duration,
        cleanup_kv_count = CleanupKVCount
    } = Result,

    MissingChangesCount = couch_set_view_util:missing_changes_count(
        ?set_seqs(Group), ?set_seqs(NewGroup)),
    case MissingChangesCount == 0 of
    true ->
        HeaderBin = couch_set_view_util:group_to_header_bin(NewGroup),
        ok = couch_file:write_header_bin(NewGroup#set_view_group.fd, HeaderBin),
        if is_pid(UpdaterPid) ->
            ?LOG_INFO("Set view `~s`, ~s group `~s`, compact group up to date - restarting updater",
                      [?set_name(State), ?type(State), ?group_id(State)]),
            couch_util:shutdown_sync(UpdaterPid);
        true ->
            ok
        end,
        NewFilepath = increment_filepath(Group),
        NewRefCounter = new_fd_ref_counter(NewGroup#set_view_group.fd),
        case ?set_replicas_on_transfer(Group) /= ?set_replicas_on_transfer(NewGroup) of
        true ->
            % Set of replicas on transfer changed while compaction was running.
            % Just write a new header with the new set of replicas on transfer and all the
            % metadata that is updated when that set changes (active and passive bitmasks).
            % This happens only during (or after, for a short period) a cluster rebalance or
            % failover. This shouldn't take too long, as we are writing and fsync'ing only
            % one header, all data was already fsync'ed by the compactor process.
            NewGroup2 = NewGroup#set_view_group{
                ref_counter = NewRefCounter,
                filepath = NewFilepath,
                index_header = (NewGroup#set_view_group.index_header)#set_view_index_header{
                    replicas_on_transfer = ?set_replicas_on_transfer(Group),
                    abitmask = ?set_abitmask(Group),
                    pbitmask = ?set_pbitmask(Group)
                }
            },
            ok = commit_header(NewGroup2);
        false ->
            % The compactor process committed an header with up to date state information and
            % did an fsync before calling us. No need to commit a new header here (and fsync).
            NewGroup2 = NewGroup#set_view_group{
                ref_counter = NewRefCounter,
                filepath = NewFilepath
            }
        end,
        ?LOG_INFO("Set view `~s`, ~s group `~s`, compaction complete in ~.3f seconds,"
            " filtered ~p key-value pairs",
            [?set_name(State), ?type(State), ?group_id(State), Duration, CleanupKVCount]),
        inc_util_stat(#util_stats.compaction_time, Duration),
        ok = couch_file:only_snapshot_reads(OldFd),
        ok = couch_file:delete(?root_dir(State), OldFilepath),
        %% After rename call we're sure the header was written to the file
        %% (no need for couch_file:flush/1 call).
        ok = couch_file:rename(NewGroup#set_view_group.fd, NewFilepath),

        %% cleanup old group
        unlink(CompactorPid),
        couch_ref_counter:drop(RefCounter),

        NewUpdaterPid =
        if is_pid(UpdaterPid) ->
            CurSeqs = indexable_partition_seqs(State),
            spawn_link(couch_set_view_updater,
                       update,
                       [self(), NewGroup2, CurSeqs, false, updater_tmp_dir(State), []]);
        true ->
            nil
        end,

        Listeners2 = notify_update_listeners(State, Listeners, NewGroup2),
        State2 = State#state{
            update_listeners = Listeners2,
            compactor_pid = nil,
            compactor_file = nil,
            compactor_fun = nil,
            updater_pid = NewUpdaterPid,
            initial_build = is_pid(NewUpdaterPid) andalso
                    couch_set_view_util:is_group_empty(NewGroup2),
            updater_state = case is_pid(NewUpdaterPid) of
                true -> starting;
                false -> not_running
            end,
            group = NewGroup2
        },
        inc_compactions(Result),
        {reply, ok, maybe_apply_pending_transition(State2), ?TIMEOUT};
    false ->
        {reply, {update, MissingChangesCount}, State}
    end;
handle_call({compact_done, _Result}, _From, State) ->
    % From a previous compactor that was killed/stopped, ignore.
    {noreply, State, ?TIMEOUT};

handle_call(cancel_compact, _From, #state{compactor_pid = nil} = State) ->
    {reply, ok, State, ?TIMEOUT};
handle_call(cancel_compact, _From, #state{compactor_pid = Pid} = State) ->
    ?LOG_INFO("Set view `~s`, ~s group `~s`, canceling compaction (pid ~p)",
              [?set_name(State), ?type(State), ?group_id(State), Pid]),
    State2 = stop_compactor(State),
    State3 = maybe_start_cleaner(State2),
    {reply, ok, State3, ?TIMEOUT};

handle_call({monitor_partition_update, PartId, _Ref, _Pid}, _From, State)
        when PartId >= ?set_num_partitions(State#state.group) ->
    Msg = io_lib:format("Invalid partition: ~p", [PartId]),
    {reply, {error, iolist_to_binary(Msg)}, State, ?TIMEOUT};

handle_call({monitor_partition_update, PartId, Ref, Pid}, _From, State) ->
    try
        State2 = process_monitor_partition_update(State, PartId, Ref, Pid),
        {reply, ok, State2, ?TIMEOUT}
    catch
    throw:Error ->
        {reply, Error, State, ?TIMEOUT}
    end;

handle_call({demonitor_partition_update, Ref}, _From, State) ->
    #state{update_listeners = Listeners} = State,
    case dict:find(Ref, Listeners) of
    error ->
        {reply, ok, State, ?TIMEOUT};
    {ok, #up_listener{monref = MonRef, partition = PartId}} ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, removing partition ~p"
                   "update monitor, reference ~p",
                   [?set_name(State), ?type(State), ?group_id(State),
                    PartId, Ref]),
        erlang:demonitor(MonRef, [flush]),
        State2 = State#state{update_listeners = dict:erase(Ref, Listeners)},
        {reply, ok, State2, ?TIMEOUT}
    end;

handle_call(compact_log_files, _From, State) ->
    NewState = State#state{compact_log_files = nil},
    {reply, {ok, State#state.compact_log_files}, NewState, ?TIMEOUT};

handle_call(reset_utilization_stats, _From, #state{replica_group = RepPid} = State) ->
    reset_util_stats(),
    case is_pid(RepPid) of
    true ->
        ok = gen_server:call(RepPid, reset_utilization_stats, infinity);
    false ->
        ok
    end,
    {reply, ok, State, ?TIMEOUT};

handle_call(get_utilization_stats, _From, #state{replica_group = RepPid} = State) ->
    Stats = erlang:get(util_stats),
    UsefulIndexing = Stats#util_stats.useful_indexing_time,
    WastedIndexing = Stats#util_stats.wasted_indexing_time,
    StatNames = record_info(fields, util_stats),
    StatPoses = lists:seq(2, record_info(size, util_stats)),
    StatsList0 = lists:foldr(
        fun({StatName, StatPos}, Acc) ->
            Val = element(StatPos, Stats),
            [{StatName, Val} | Acc]
        end,
        [], lists:zip(StatNames, StatPoses)),
    StatsList1 = [{total_indexing_time, UsefulIndexing + WastedIndexing} | StatsList0],
    case is_pid(RepPid) of
    true ->
        {ok, RepStats} = gen_server:call(RepPid, get_utilization_stats, infinity),
        StatsList = StatsList1 ++ [{replica_utilization_stats, {RepStats}}];
    false ->
        StatsList = StatsList1
    end,
    {reply, {ok, StatsList}, State, ?TIMEOUT}.


handle_cast(_Msg, State) when not ?is_defined(State) ->
    {noreply, State};

handle_cast({compact_log_files, Files, Seqs}, #state{compact_log_files = nil} = State) ->
    L = lists:map(fun(F) -> [F] end, Files),
    {noreply, State#state{compact_log_files = {L, Seqs}}, ?TIMEOUT};

handle_cast({compact_log_files, Files, NewSeqs}, State) ->
    {OldL, _OldSeqs} = State#state.compact_log_files,
    L = lists:zipwith(
        fun(F, Current) -> [F | Current] end,
        Files, OldL),
    {noreply, State#state{compact_log_files = {L, NewSeqs}}, ?TIMEOUT};

handle_cast({ddoc_updated, NewSig, Aliases}, State) ->
    #state{
        waiting_list = Waiters,
        group = #set_view_group{sig = CurSig}
    } = State,
    ?LOG_INFO("Set view `~s`, ~s group `~s`, signature `~s', design document was updated~n"
              "  new signature:   ~s~n"
              "  current aliases: ~p~n"
              "  shutdown flag:   ~s~n"
              "  waiting clients: ~p~n",
              [?set_name(State), ?type(State), ?group_id(State),
               hex_sig(CurSig), hex_sig(NewSig), Aliases,
               State#state.shutdown, length(Waiters)]),
    case NewSig of
    CurSig ->
        {noreply, State#state{shutdown = false, shutdown_aliases = undefined}, ?TIMEOUT};
    _ ->
        State2 = State#state{shutdown = true, shutdown_aliases = Aliases},
        case Waiters of
        [] ->
            {stop, normal, State2};
        _ ->
            {noreply, State2}
        end
    end;


handle_cast({before_partition_delete, master}, State) ->
    Error = {error, {db_deleted, ?master_dbname((?set_name(State)))}},
    State2 = reply_all(State, Error),
    ?LOG_INFO("Set view `~s`, ~s group `~s`, going to shutdown because "
              "master database is being deleted",
              [?set_name(State), ?type(State), ?group_id(State)]),
    {stop, shutdown, State2};

handle_cast({before_partition_delete, _PartId}, State) when not ?is_defined(State) ->
    {noreply, State};

handle_cast({before_partition_delete, PartId}, #state{group = Group} = State) ->
    #state{
        replica_partitions = ReplicaParts,
        replica_group = ReplicaPid
    } = State,
    case ?set_pending_transition(Group) of
    nil ->
        ActivePending = [],
        PassivePending = [];
    PendingTrans ->
        #set_view_transition{
            active = ActivePending,
            passive = PassivePending
        } = PendingTrans
    end,
    Mask = 1 bsl PartId,
    case ((?set_abitmask(Group) band Mask) /= 0) orelse
        ((?set_pbitmask(Group) band Mask) /= 0) orelse
        lists:member(PartId, ActivePending) orelse
        lists:member(PartId, PassivePending) of
    true ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, marking partition ~p for "
                  "cleanup because it's about to be deleted",
                  [?set_name(State), ?type(State), ?group_id(State), PartId]),
        case couch_set_view_util:has_part_seq(PartId, ?set_unindexable_seqs(State#state.group)) of
        true ->
            State2 = process_mark_as_indexable(State, [PartId], false);
        false ->
            State2 = State
        end,
        State3 = update_partition_states([], [], [PartId], State2, true),
        {noreply, State3, ?TIMEOUT};
    false ->
        case lists:member(PartId, ReplicaParts) of
        true ->
            % Can't be a replica on transfer, otherwise it would be part of the
            % set of passive partitions.
            ?LOG_INFO("Set view `~s`, ~s group `~s`, removing replica partition ~p"
                      " because it's about to be deleted",
                      [?set_name(State), ?type(State), ?group_id(State), PartId]),
            ok = set_state(ReplicaPid, [], [], [PartId]),
            State2 = State#state{
               replica_partitions = ordsets:del_element(PartId, ReplicaParts)
            },
            State3 = demonitor_partitions(State2, [PartId]),
            {noreply, State3, ?TIMEOUT};
        false ->
            {noreply, State, ?TIMEOUT}
        end
    end;

handle_cast({update, MinNumChanges}, #state{group = Group} = State) ->
    case is_pid(State#state.updater_pid) of
    true ->
        {noreply, State};
    false ->
        CurSeqs = indexable_partition_seqs(State),
        MissingCount = couch_set_view_util:missing_changes_count(CurSeqs, ?set_seqs(Group)),
        case (MissingCount >= MinNumChanges) andalso (MissingCount > 0) of
        true ->
            {noreply, do_start_updater(State, CurSeqs, [])};
        false ->
            {noreply, State}
        end
    end;

handle_cast({update_replica, _MinNumChanges}, #state{replica_group = nil} = State) ->
    {noreply, State};

handle_cast({update_replica, MinNumChanges}, #state{replica_group = Pid} = State) ->
    ok = gen_server:cast(Pid, {update, MinNumChanges}),
    {noreply, State}.


handle_info(timeout, State) when not ?is_defined(State) ->
    {noreply, State};

handle_info(timeout, #state{group = Group} = State) ->
    case (?set_replicas_on_transfer(Group) /= []) orelse
        (dict:size(State#state.update_listeners) > 0) of
    true ->
        {noreply, start_updater(State)};
    false ->
        {noreply, maybe_start_cleaner(State)}
    end;

handle_info({partial_update, Pid, NewGroup}, #state{updater_pid = Pid} = State) ->
    case ?have_pending_transition(State) andalso
        (?set_cbitmask(NewGroup) =:= 0) andalso
        (?set_cbitmask(State#state.group) =/= 0) andalso
        (State#state.waiting_list =:= []) of
    true ->
        State2 = process_partial_update(State, NewGroup),
        State3 = stop_updater(State2),
        NewState = maybe_apply_pending_transition(State3);
    false ->
        NewState = process_partial_update(State, NewGroup)
    end,
    {noreply, NewState};
handle_info({partial_update, _, _}, State) ->
    %% message from an old (probably pre-compaction) updater; ignore
    {noreply, State, ?TIMEOUT};

handle_info({updater_info, Pid, {state, UpdaterState}}, #state{updater_pid = Pid} = State) ->
    #state{
        group = Group,
        waiting_list = WaitList,
        replica_partitions = RepParts
    } = State,
    State2 = State#state{updater_state = UpdaterState},
    case UpdaterState of
    updating_passive ->
        WaitList2 = reply_with_group(Group, RepParts, WaitList),
        State3 = State2#state{waiting_list = WaitList2},
        case State#state.shutdown of
        true ->
            State4 = stop_updater(State3),
            {stop, normal, State4};
        false ->
            State4 = maybe_apply_pending_transition(State3),
            {noreply, State4}
        end;
    _ ->
        {noreply, State2}
    end;

handle_info({updater_info, _Pid, {state, _UpdaterState}}, State) ->
    % Message from an old updater, ignore.
    {noreply, State, ?TIMEOUT};

handle_info({'EXIT', Pid, {clean_group, NewGroup0, Count, Time}}, #state{cleaner_pid = Pid} = State) ->
    #state{group = OldGroup} = State,
    NewGroup = update_clean_group_seqs(OldGroup, NewGroup0),
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
        group = NewGroup
    },
    inc_cleanups(State2#state.group, Time, Count, false),
    {noreply, maybe_apply_pending_transition(State2)};

handle_info({'EXIT', Pid, Reason}, #state{cleaner_pid = Pid} = State) ->
    ?LOG_ERROR("Set view `~s`, ~s group `~s`, cleanup process ~p died with "
               "unexpected reason: ~p",
               [?set_name(State), ?type(State), ?group_id(State), Pid, Reason]),
    {noreply, State#state{cleaner_pid = nil}, ?TIMEOUT};

handle_info({'EXIT', Pid, Reason},
    #state{group = #set_view_group{db_set = Pid}} = State) ->
    ?LOG_INFO("Set view `~s`, ~s group `~s`, terminating because database set ~p"
              " exited with reason: ~p",
              [?set_name(State), ?type(State), ?group_id(State), Pid, Reason]),
    {stop, Reason, State};

handle_info({'EXIT', Pid, {updater_finished, Result}}, #state{updater_pid = Pid} = State) ->
    #set_view_updater_result{
        stats = Stats,
        group = NewGroup
    } = Result,
    #set_view_updater_stats{
        indexing_time = IndexingTime,
        blocked_time = BlockedTime,
        inserted_ids = InsertedIds,
        deleted_ids = DeletedIds,
        inserted_kvs = InsertedKVs,
        deleted_kvs = DeletedKVs,
        cleanup_kv_count = CleanupKVCount,
        seqs = SeqsDone
    } = Stats,
    State2 = process_partial_update(State, NewGroup),
    #state{
        waiting_list = WaitList,
        replica_partitions = ReplicaParts,
        shutdown = Shutdown,
        group = NewGroup2,
        update_listeners = UpdateListeners2
    } = State2,
    WaitList2 = reply_with_group(NewGroup2, ReplicaParts, WaitList),
    inc_updates(NewGroup2, Result, false, false),
    ?LOG_INFO("Set view `~s`, ~s group `~s`, updater finished~n"
        "Indexing time: ~.3f seconds~n"
        "Blocked time:  ~.3f seconds~n"
        "Inserted IDs:  ~p~n"
        "Deleted IDs:   ~p~n"
        "Inserted KVs:  ~p~n"
        "Deleted KVs:   ~p~n"
        "Cleaned KVs:   ~p~n"
        "# seqs done:   ~p~n",
        [?set_name(State), ?type(State), ?group_id(State), IndexingTime, BlockedTime,
            InsertedIds, DeletedIds, InsertedKVs, DeletedKVs, CleanupKVCount, SeqsDone]),
    case Shutdown andalso (WaitList2 == []) of
    true ->
        {stop, normal, State2#state{waiting_list = []}};
    false ->
        State3 = State2#state{
            updater_pid = nil,
            initial_build = false,
            updater_state = not_running,
            waiting_list = WaitList2
        },
        State4 = maybe_apply_pending_transition(State3),
        State5 = case (WaitList2 /= []) orelse (dict:size(UpdateListeners2) > 0) of
        true ->
            start_updater(State4);
        false ->
            State4
        end,
        State6 = maybe_start_cleaner(State5),
        {noreply, State6, ?TIMEOUT}
    end;

handle_info({'EXIT', Pid, {updater_error, Error}}, #state{updater_pid = Pid} = State) ->
    ?LOG_ERROR("Set view `~s`, ~s group `~s`, received error from updater: ~p",
        [?set_name(State), ?type(State), ?group_id(State), Error]),
    Listeners2 = error_notify_update_listeners(
        State, State#state.update_listeners, {updater_error, Error}),
    State2 = State#state{
        updater_pid = nil,
        initial_build = false,
        updater_state = not_running,
        update_listeners = Listeners2
    },
    ?inc_updater_errors(State2#state.group),
    case State#state.shutdown of
    true ->
        {stop, normal, reply_all(State2, {error, Error})};
    false ->
        case Error of
        {error, _Reason} ->
            State3 = reply_all(State2, Error);
        _ ->
            State3 = reply_all(State2, {error, Error})
        end,
        {noreply, maybe_start_cleaner(State3), ?TIMEOUT}
    end;

handle_info({'EXIT', _Pid, {updater_error, _Error}}, State) ->
    % from old, shutdown updater, ignore
    {noreply, State, ?TIMEOUT};

handle_info({'EXIT', UpPid, reset}, #state{updater_pid = UpPid} = State) ->
    % TODO: once purge support is properly added, this needs to take into
    % account the replica index.
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
    ?LOG_ERROR("Set view `~s`, ~s group `~s`, compactor process ~p died with "
               "unexpected reason: ~p",
               [?set_name(State), ?type(State), ?group_id(State), Pid, Reason]),
    couch_util:shutdown_sync(State#state.compactor_file),
    _ = couch_file:delete(?root_dir(State), compact_file_name(State)),
    State2 = State#state{
        compactor_pid = nil,
        compactor_file = nil,
        compact_log_files = nil
    },
    {noreply, State2, ?TIMEOUT};

handle_info({'EXIT', Pid, Reason}, #state{group = #set_view_group{db_set = Pid}} = State) ->
    {stop, {db_set_died, Reason}, State};

handle_info({'EXIT', Pid, Reason}, State) ->
    ?LOG_ERROR("Set view `~s`, ~s group `~s`, terminating because linked PID ~p "
              "died with reason: ~p",
              [?set_name(State), ?type(State), ?group_id(State), Pid, Reason]),
    {stop, Reason, State};

handle_info({'DOWN', Ref, process, Pid, Reason}, State) ->
    try
        _ = dict:fold(
            fun(Id, Ref0, _Acc) when Ref0 == Ref ->
                    throw({found, Id});
                (_Id, _Ref0, Acc) ->
                    Acc
            end,
            undefined,
            State#state.db_refs),
        UpdateListeners2 = dict:filter(
            fun(RefList, #up_listener{pid = Pid0, partition = PartId}) when Pid0 == Pid ->
                ?LOG_INFO("Set view `~s`, ~s group `~s`, removing partition ~p"
                           "update monitor, reference ~p (it died)",
                           [?set_name(State), ?type(State), ?group_id(State),
                            PartId, RefList]),
                false;
            (_ , _) ->
                true
            end,
            State#state.update_listeners),
        {noreply, State#state{update_listeners = UpdateListeners2}}
    catch throw:{found, PartId} ->
        ?LOG_ERROR("Set view `~s`, ~s group `~s`, terminating because "
                   "partition ~p died with reason: ~p",
                   [?set_name(State), ?type(State), ?group_id(State), PartId, Reason]),
        {stop, shutdown, State}
    end.


terminate(Reason, #state{group = #set_view_group{sig = Sig} = Group} = State) ->
    ?LOG_INFO("Set view `~s`, ~s group `~s`, signature `~s`, terminating with reason: ~p",
        [?set_name(State), ?type(State), ?group_id(State), hex_sig(Sig), Reason]),
    Listeners2 = error_notify_update_listeners(
        State, State#state.update_listeners, {shutdown, Reason}),
    State2 = reply_all(State#state{update_listeners = Listeners2}, Reason),
    State3 = notify_pending_transition_waiters(State2, {shutdown, Reason}),
    catch couch_db_set:close(?db_set(State3)),
    couch_util:shutdown_sync(State3#state.cleaner_pid),
    couch_util:shutdown_sync(State3#state.updater_pid),
    couch_util:shutdown_sync(State3#state.compactor_pid),
    couch_util:shutdown_sync(State3#state.compactor_file),
    couch_util:shutdown_sync(State3#state.replica_group),
    true = ets:delete(?SET_VIEW_STATS_ETS, ?set_view_group_stats_key(State#state.group)),
    catch couch_file:only_snapshot_reads((State3#state.group)#set_view_group.fd),
    case State#state.shutdown of
    true when ?type(State) == main ->
        % Important to delete files here. A quick succession of ddoc updates, updating
        % the ddoc back to a previous version (while we're here in terminate), may lead
        % us to a case where it should start clean but it doesn't, because
        % couch_set_view:cleanup_index_files/1 was not invoked right before the last
        % reverting ddoc update, or it was invoked but did not finish before the view
        % group got spawned again. Problem here is during rebalance, where one of the
        % databases might have been deleted after the last ddoc update and while or after
        % we're here in terminate, so we must ensure we don't leave old index files around.
        % MB-6415 and MB-6517
        case State#state.shutdown_aliases of
        [] ->
            delete_index_file(?root_dir(State), Group, main),
            delete_index_file(?root_dir(State), Group, replica);
        _ ->
            ok
        end;
    _ ->
        ok
    end,
    TmpDir = updater_tmp_dir(State),
    ok = couch_set_view_util:delete_sort_files(TmpDir, all),
    _ = file:del_dir(TmpDir),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


-spec reply_with_group(#set_view_group{},
                       ordsets:ordset(partition_id()),
                       [#waiter{}]) -> [#waiter{}].
reply_with_group(_Group, _ReplicaPartitions, []) ->
    [];
reply_with_group(Group, ReplicaPartitions, WaitList) ->
    ActiveReplicasBitmask = couch_set_view_util:build_bitmask(
        ?set_replicas_on_transfer(Group)),
    ActiveIndexable = [{P, S} || {P, S} <- ?set_seqs(Group),
                          ((1 bsl P) band ?set_abitmask(Group) =/= 0)],
    ActiveUnindexable = [{P, S} || {P, S} <- ?set_unindexable_seqs(Group),
                          ((1 bsl P) band ?set_abitmask(Group) =/= 0)],
    GroupSeqs = ordsets:union(ActiveIndexable, ActiveUnindexable),
    WaitList2 = lists:foldr(
        fun(#waiter{debug = false} = Waiter, Acc) ->
            case maybe_reply_with_group(Waiter, Group, GroupSeqs, ActiveReplicasBitmask) of
            true ->
                Acc;
            false ->
                [Waiter | Acc]
            end;
        (#waiter{debug = true} = Waiter, Acc) ->
            [Stats] = ets:lookup(?SET_VIEW_STATS_ETS, ?set_view_group_stats_key(Group)),
            DebugGroup = Group#set_view_group{
                debug_info = #set_view_debug_info{
                    stats = Stats,
                    original_abitmask = ?set_abitmask(Group),
                    original_pbitmask = ?set_pbitmask(Group),
                    replica_partitions = ReplicaPartitions,
                    wanted_seqs = Waiter#waiter.seqs
                }
            },
            case maybe_reply_with_group(Waiter, DebugGroup, GroupSeqs, ActiveReplicasBitmask) of
            true ->
                Acc;
            false ->
                [Waiter | Acc]
            end
        end,
        [], WaitList),
    WaitList2.


-spec maybe_reply_with_group(#waiter{}, #set_view_group{}, partition_seqs(), bitmask()) -> boolean().
maybe_reply_with_group(Waiter, Group, GroupSeqs, ActiveReplicasBitmask) ->
    #waiter{from = {Pid, _} = From, seqs = ClientSeqs} = Waiter,
    case (ClientSeqs == []) orelse (GroupSeqs >= ClientSeqs) of
    true ->
        couch_ref_counter:add(Group#set_view_group.ref_counter, Pid),
        gen_server:reply(From, {ok, Group, ActiveReplicasBitmask}),
        true;
    false ->
        false
    end.


-spec reply_all(#state{}, term()) -> #state{}.
reply_all(#state{waiting_list = []} = State, _Reply) ->
    State;
reply_all(#state{waiting_list = WaitList} = State, Reply) ->
    lists:foreach(fun(#waiter{from = From}) ->
        catch gen_server:reply(From, Reply)
    end, WaitList),
    State#state{waiting_list = []}.


-spec prepare_group(init_args(), boolean()) -> {'ok', #set_view_group{}} |
                                               {'error', atom()}.
prepare_group({RootDir, SetName, #set_view_group{sig = Sig, type = Type} = Group0}, ForceReset)->
    Filepath = find_index_file(RootDir, Group0),
    Group = Group0#set_view_group{filepath = Filepath},
    case open_index_file(Filepath) of
    {ok, Fd} ->
        if ForceReset ->
            % this can happen if we missed a purge
            {ok, reset_file(Fd, Group)};
        true ->
            case (catch couch_file:read_header_bin(Fd)) of
            {ok, HeaderBin} ->
                HeaderSig = couch_set_view_util:header_bin_sig(HeaderBin);
            _ ->
                HeaderSig = <<>>,
                HeaderBin = <<>>
            end,
            case HeaderSig == Sig of
            true ->
                HeaderInfo = couch_set_view_util:header_bin_to_term(HeaderBin),
                {ok, init_group(Fd, Group, HeaderInfo)};
            _ ->
                % this happens on a new file
                case (not ForceReset) andalso (Type =:= main) of
                true ->
                    % initializing main view group
                    ok = delete_index_file(RootDir, Group, replica);
                false ->
                    ok
                end,
                {ok, reset_file(Fd, Group)}
            end
        end;
    {error, emfile} = Error ->
        ?LOG_ERROR("Can't open set view `~s`, ~s group `~s`: too many files open",
            [SetName, Type, Group#set_view_group.name]),
        Error;
    Error ->
        ok = delete_index_file(RootDir, Group, Type),
        case (not ForceReset) andalso (Type =:= main) of
        true ->
            % initializing main view group
            ok = delete_index_file(RootDir, Group, replica);
        false ->
            ok
        end,
        Error
    end.


-spec hex_sig(#set_view_group{} | binary()) -> string().
hex_sig(#set_view_group{sig = Sig}) ->
    hex_sig(Sig);
hex_sig(GroupSig) ->
    couch_util:to_hex(GroupSig).


-spec base_index_file_name(#set_view_group{}, set_view_group_type()) -> string().
base_index_file_name(Group, Type) ->
    atom_to_list(Type) ++ "_" ++ hex_sig(Group#set_view_group.sig) ++ ".view".


-spec find_index_file(string(), #set_view_group{}) -> string().
find_index_file(RootDir, Group) ->
    find_index_file(RootDir, Group, Group#set_view_group.type).

-spec find_index_file(string(), #set_view_group{}, set_view_group_type()) -> string().
find_index_file(RootDir, Group, Type) ->
    DesignRoot = couch_set_view:set_index_dir(RootDir, Group#set_view_group.set_name),
    BaseName = base_index_file_name(Group, Type),
    FullPath = filename:join([DesignRoot, BaseName]),
    case filelib:wildcard(FullPath ++ ".[0-9]*") of
    [] ->
        FullPath ++ ".1";
    Matching ->
        BaseNameSplitted = string:tokens(BaseName, "."),
        Matching2 = lists:filter(
            fun(Match) ->
                MatchBase = filename:basename(Match),
                [Suffix | Rest] = lists:reverse(string:tokens(MatchBase, ".")),
                (lists:reverse(Rest) =:= BaseNameSplitted) andalso
                    is_integer((catch list_to_integer(Suffix)))
            end,
            Matching),
        case Matching2 of
        [] ->
            FullPath ++ ".1";
        _ ->
            GetSuffix = fun(FileName) ->
                list_to_integer(lists:last(string:tokens(FileName, ".")))
            end,
            Matching3 = lists:sort(
                fun(A, B) -> GetSuffix(A) > GetSuffix(B) end,
                Matching2),
            hd(Matching3)
        end
    end.


-spec delete_index_file(string(), #set_view_group{}, set_view_group_type()) -> no_return().
delete_index_file(RootDir, Group, Type) ->
    SetDir = couch_set_view:set_index_dir(RootDir, Group#set_view_group.set_name),
    BaseName = filename:join([SetDir, base_index_file_name(Group, Type)]),
    lists:foreach(
        fun(F) -> ok = couch_file:delete(RootDir, F) end,
        filelib:wildcard(BaseName ++ ".[0-9]*")).


-spec compact_file_name(#state{} | #set_view_group{}) -> string().
compact_file_name(#state{group = Group}) ->
    compact_file_name(Group);
compact_file_name(#set_view_group{filepath = CurFilepath}) ->
    CurFilepath ++ ".compact".


-spec increment_filepath(#set_view_group{}) -> string().
increment_filepath(#set_view_group{filepath = CurFilepath}) ->
    [Suffix | Rest] = lists:reverse(string:tokens(CurFilepath, ".")),
    NewSuffix = integer_to_list(list_to_integer(Suffix) + 1),
    string:join(lists:reverse(Rest), ".") ++ "." ++ NewSuffix.


-spec open_index_file(string()) -> {'ok', pid()} | {'error', atom()}.
open_index_file(Filepath) ->
    case do_open_index_file(Filepath) of
    {ok, Fd} ->
        unlink(Fd),
        {ok, Fd};
    Error ->
        Error
    end.

do_open_index_file(Filepath) ->
    case couch_file:open(Filepath) of
    {ok, Fd}        -> {ok, Fd};
    {error, enoent} -> couch_file:open(Filepath, [create]);
    Error           -> Error
    end.


open_set_group(SetName, GroupId) ->
    case couch_set_view_ddoc_cache:get_ddoc(SetName, GroupId) of
    {ok, DDoc} ->
        {ok, couch_set_view_util:design_doc_to_set_view_group(SetName, DDoc)};
    {doc_open_error, Error} ->
        Error;
    {db_open_error, Error} ->
        Error
    end.


% To be used for debug/troubleshooting only (accessible via REST/HTTP API)
get_group_info(State) ->
    #state{
        group = Group,
        replica_group = ReplicaPid,
        updater_pid = UpdaterPid,
        updater_state = UpdaterState,
        compactor_pid = CompactorPid,
        waiting_list = WaitersList,
        cleaner_pid = CleanerPid,
        replica_partitions = ReplicaParts
    } = State,
    #set_view_group{
        fd = Fd,
        sig = GroupSig,
        id_btree = Btree,
        views = Views
    } = Group,
    PendingTrans = get_pending_transition(State),
    [Stats] = ets:lookup(?SET_VIEW_STATS_ETS, ?set_view_group_stats_key(Group)),
    JsonStats = {[
        {full_updates, Stats#set_view_group_stats.full_updates},
        {partial_updates, Stats#set_view_group_stats.partial_updates},
        {stopped_updates, Stats#set_view_group_stats.stopped_updates},
        {updater_cleanups, Stats#set_view_group_stats.updater_cleanups},
        {compactions, Stats#set_view_group_stats.compactions},
        {cleanups, Stats#set_view_group_stats.cleanups},
        {waiting_clients, length(WaitersList)},
        {cleanup_interruptions, Stats#set_view_group_stats.cleanup_stops},
        {update_history, Stats#set_view_group_stats.update_history},
        {compaction_history, Stats#set_view_group_stats.compaction_history},
        {cleanup_history, Stats#set_view_group_stats.cleanup_history}
    ]},
    {ok, Size} = couch_file:bytes(Fd),
    {message_queue_len, DbSetMsgQueueLen} = process_info(?db_set(State), message_queue_len),
    {ok, DbSeqs, ExpectedDbSeqs} = gen_server:call(?db_set(State), get_seqs_debug, infinity),
    DbSetPartitions = ordsets:from_list([P || {P, _S} <- DbSeqs]),
    GroupPartitions = ordsets:from_list(
        couch_set_view_util:decode_bitmask(?set_abitmask(Group) bor ?set_pbitmask(Group))),
    [
        {signature, ?l2b(hex_sig(GroupSig))},
        {disk_size, Size},
        {data_size, view_group_data_size(Btree, Views)},
        {updater_running, is_pid(UpdaterPid)},
        {initial_build, is_pid(UpdaterPid) andalso State#state.initial_build},
        {updater_state, couch_util:to_binary(UpdaterState)},
        {compact_running, CompactorPid /= nil},
        {cleanup_running, (CleanerPid /= nil) orelse
            ((CompactorPid /= nil) andalso (?set_cbitmask(Group) =/= 0))},
        {max_number_partitions, ?set_num_partitions(Group)},
        {update_seqs, {[{couch_util:to_binary(P), S} || {P, S} <- ?set_seqs(Group)]}},
        {partition_seqs, {[{couch_util:to_binary(P), S} || {P, S} <- DbSeqs]}},
        {expected_partition_seqs, {[{couch_util:to_binary(P), S} || {P, S} <- ExpectedDbSeqs]}},
        {partition_seqs_up_to_date, DbSeqs == ExpectedDbSeqs},
        {out_of_sync_db_set_partitions, DbSetPartitions /= GroupPartitions},
        {db_set_message_queue_len, DbSetMsgQueueLen},
        {active_partitions, couch_set_view_util:decode_bitmask(?set_abitmask(Group))},
        {passive_partitions, couch_set_view_util:decode_bitmask(?set_pbitmask(Group))},
        {cleanup_partitions, couch_set_view_util:decode_bitmask(?set_cbitmask(Group))},
        {unindexable_partitions, {[{couch_util:to_binary(P), S} || {P, S} <- ?set_unindexable_seqs(Group)]}},
        {stats, JsonStats},
        {pending_transition, case PendingTrans of
            nil ->
                null;
            #set_view_transition{} ->
                {[
                    {active, PendingTrans#set_view_transition.active},
                    {passive, PendingTrans#set_view_transition.passive}
                ]}
            end
        }
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


get_data_size_info(State) ->
    #state{
        group = Group,
        replica_group = ReplicaPid,
        updater_pid = UpdaterPid
    } = State,
    #set_view_group{
        fd = Fd,
        id_btree = Btree,
        sig = GroupSig,
        views = Views
    } = Group,
    {ok, FileSize} = couch_file:bytes(Fd),
    DataSize = view_group_data_size(Btree, Views),
    [Stats] = ets:lookup(?SET_VIEW_STATS_ETS, ?set_view_group_stats_key(Group)),
    Info = [
        {signature, hex_sig(GroupSig)},
        {disk_size, FileSize},
        {data_size, DataSize},
        {accesses, Stats#set_view_group_stats.accesses},
        {updater_running, is_pid(UpdaterPid)},
        {initial_build, is_pid(UpdaterPid) andalso State#state.initial_build}
    ],
    case is_pid(ReplicaPid) of
    false ->
        Info;
    true ->
        {ok, RepInfo} = gen_server:call(ReplicaPid, get_data_size, infinity),
        [{replica_group_info, RepInfo} | Info]
    end.


-spec view_group_data_size(#btree{}, [#set_view{}]) -> non_neg_integer().
view_group_data_size(IdBtree, Views) ->
    lists:foldl(
        fun(#set_view{btree = Btree}, Acc) ->
            Acc + couch_btree:size(Btree)
        end,
        couch_btree:size(IdBtree),
        Views).


-spec reset_group(#set_view_group{}) -> #set_view_group{}.
reset_group(#set_view_group{views = Views} = Group) ->
    Views2 = [View#set_view{btree = nil} || View <- Views],
    Group#set_view_group{
        fd = nil,
        index_header = #set_view_index_header{},
        id_btree = nil,
        views = Views2
    }.


-spec reset_file(pid(), #set_view_group{}) -> #set_view_group{}.
reset_file(Fd, #set_view_group{views = Views, index_header = Header} = Group) ->
    ok = couch_file:truncate(Fd, 0),
    EmptyHeader = Header#set_view_index_header{
        view_states = [nil || _ <- Views],
        id_btree_state = nil
    },
    EmptyGroup = Group#set_view_group{index_header = EmptyHeader},
    EmptyHeaderBin = couch_set_view_util:group_to_header_bin(EmptyGroup),
    ok = couch_file:write_header_bin(Fd, EmptyHeaderBin),
    init_group(Fd, reset_group(EmptyGroup), EmptyHeader).


-spec init_group(pid(),
                 #set_view_group{},
                 #set_view_index_header{}) -> #set_view_group{}.
init_group(Fd, Group, IndexHeader) ->
    #set_view_group{
        views = Views0,
        set_name = SetName,
        name = DDocId,
        type = Type
    } = Group,
    Views = [V#set_view{ref = make_ref()} || V <- Views0],
    #set_view_index_header{
        id_btree_state = IdBtreeState,
        view_states = ViewStates
    } = IndexHeader,
    IdTreeReduce = fun(reduce, KVs) ->
        <<(length(KVs)):40, (couch_set_view_util:partitions_map(KVs, 0)):?MAX_NUM_PARTITIONS>>;
    (rereduce, [First | Rest]) ->
        lists:foldl(
            fun(<<S:40, M:?MAX_NUM_PARTITIONS>>, <<T:40, A:?MAX_NUM_PARTITIONS>>) ->
                <<(S + T):40, (M bor A):?MAX_NUM_PARTITIONS>>
            end,
            First, Rest)
    end,
    KvChunkThreshold = couch_config:get("set_views", "btree_kv_node_threshold", "7168"),
    KpChunkThreshold = couch_config:get("set_views", "btree_kp_node_threshold", "6144"),
    BtreeOptions = [
        {kv_chunk_threshold, list_to_integer(KvChunkThreshold)},
        {kp_chunk_threshold, list_to_integer(KpChunkThreshold)},
        {binary_mode, true}
    ],
    {ok, IdBtree} = couch_btree:open(
        IdBtreeState, Fd, [{reduce, IdTreeReduce} | BtreeOptions]),
    Views2 = lists:zipwith(
        fun(BTState, View) ->
            case View#set_view.reduce_funs of
            [{ViewName, _} | _] ->
                ok;
            [] ->
                [ViewName | _] = View#set_view.map_names
            end,
            ReduceFun =
                fun(reduce, KVs) ->
                    AllPartitionsBitMap = couch_set_view_util:partitions_map(KVs, 0),
                    KVs2 = couch_set_view_util:expand_dups(KVs, []),
                    {ok, Reduced} =
                        try
                             couch_set_view_mapreduce:reduce(View, KVs2)
                        catch throw:{error, Reason} = Error ->
                            ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, error executing"
                                                 " reduce function for view `~s'~n"
                                                 "  reason:                ~s~n"
                                                 "  input key-value pairs: ~p~n",
                                                 [SetName, Type, DDocId, ViewName,
                                                  couch_util:to_binary(Reason), KVs2]),
                            throw(Error)
                        end,
                    if length(Reduced) > 255 ->
                        throw({too_many_reductions, <<"Maximum reductions allowed is 255">>});
                    true -> ok
                    end,
                    LenReductions = [<<(size(R)):16, R/binary>> || R <- Reduced],
                    iolist_to_binary([<<(length(KVs2)):40, AllPartitionsBitMap:?MAX_NUM_PARTITIONS>> | LenReductions]);
                (rereduce, [<<Count0:40, AllPartitionsBitMap0:?MAX_NUM_PARTITIONS, Red0/binary>> | Reds]) ->
                    {Count, AllPartitionsBitMap, UserReds} = lists:foldl(
                        fun(<<C:40, Apbm:?MAX_NUM_PARTITIONS, R/binary>>, {CountAcc, ApbmAcc, RedAcc}) ->
                            {C + CountAcc, Apbm bor ApbmAcc, [couch_set_view_util:parse_reductions(R) | RedAcc]}
                        end,
                        {Count0, AllPartitionsBitMap0, [couch_set_view_util:parse_reductions(Red0)]},
                        Reds),
                    {ok, Reduced} =
                        try
                            couch_set_view_mapreduce:rereduce(View, UserReds)
                        catch throw:{error, Reason} = Error ->
                            ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, error executing"
                                                 " rereduce function for view `~s'~n"
                                                 "  reason:           ~s~n"
                                                 "  input reductions: ~p~n",
                                                 [SetName, Type, DDocId, ViewName,
                                                  couch_util:to_binary(Reason), UserReds]),
                            throw(Error)
                        end,
                    LenReductions = [<<(size(R1)):16, R1/binary>> || R1 <- Reduced],
                    iolist_to_binary([<<Count:40, AllPartitionsBitMap:?MAX_NUM_PARTITIONS>> | LenReductions])
                end,
            Less = fun(A, B) ->
                {Key1, DocId1} = couch_set_view_util:split_key_docid(A),
                {Key2, DocId2} = couch_set_view_util:split_key_docid(B),
                case couch_ejson_compare:less_json(Key1, Key2) of
                0 ->
                    DocId1 < DocId2;
                LessResult ->
                    LessResult < 0
                end
            end,
            {ok, Btree} = couch_btree:open(
                BTState, Fd, [{less, Less}, {reduce, ReduceFun} | BtreeOptions]),
            View#set_view{btree = Btree}
        end,
        ViewStates, Views),
    Group#set_view_group{
        fd = Fd,
        id_btree = IdBtree,
        views = Views2,
        index_header = IndexHeader
    }.


commit_header(Group) ->
    commit_header(Group, true).

-spec commit_header(#set_view_group{}, boolean()) -> 'ok'.
commit_header(Group, Fsync) ->
    HeaderBin = couch_set_view_util:group_to_header_bin(Group),
    ok = couch_file:write_header_bin(Group#set_view_group.fd, HeaderBin),
    case Fsync of
    true ->
        ok = couch_file:sync(Group#set_view_group.fd);
    false ->
        ok = couch_file:flush(Group#set_view_group.fd)
    end.

-spec filter_out_bitmask_partitions(ordsets:ordset(partition_id()),
                                    bitmask()) -> ordsets:ordset(partition_id()).
filter_out_bitmask_partitions(Partitions, BMask) ->
    [P || P <- Partitions, ((BMask bsr P) band 1) =/= 1].

-spec maybe_update_partition_states(ordsets:ordset(partition_id()),
                                    ordsets:ordset(partition_id()),
                                    ordsets:ordset(partition_id()),
                                    #state{}) -> #state{}.
maybe_update_partition_states(ActiveList0, PassiveList0, CleanupList0, State) ->
    #state{group = Group} = State,
    PendingTrans = ?set_pending_transition(Group),
    PendingUnindexable = ?pending_transition_unindexable(PendingTrans),
    case (?set_unindexable_seqs(Group) == []) andalso (PendingUnindexable == []) of
    true ->
        ActiveList = ActiveList0,
        PassiveList = PassiveList0,
        CleanupList = CleanupList0;
    false ->
        ActiveList = filter_out_bitmask_partitions(ActiveList0, ?set_abitmask(Group)),
        PassiveList = filter_out_bitmask_partitions(PassiveList0, ?set_pbitmask(Group)),
        CleanupList = filter_out_bitmask_partitions(CleanupList0, ?set_cbitmask(Group)),
        ActiveMarkedAsUnindexable = [
            P || P <- ActiveList, is_unindexable_part(P, Group)
        ],
        case ActiveMarkedAsUnindexable of
        [] ->
            ok;
        _ ->
            ErrorMsg1 = io_lib:format("Intersection between requested active list "
                "and current unindexable partitions: ~w", [ActiveMarkedAsUnindexable]),
            throw({error, iolist_to_binary(ErrorMsg1)})
        end,
        PassiveMarkedAsUnindexable = [
            P || P <- PassiveList, is_unindexable_part(P, Group)
        ],
        case PassiveMarkedAsUnindexable of
        [] ->
            ok;
        _ ->
            ErrorMsg2 = io_lib:format("Intersection between requested passive list "
                "and current unindexable partitions: ~w", [PassiveMarkedAsUnindexable]),
            throw({error, iolist_to_binary(ErrorMsg2)})
        end,
        CleanupMarkedAsUnindexable = [
            P || P <- CleanupList, is_unindexable_part(P, Group)
        ],
        case CleanupMarkedAsUnindexable of
        [] ->
            ok;
        _ ->
            ErrorMsg3 = io_lib:format("Intersection between requested cleanup list "
                "and current unindexable partitions: ~w", [CleanupMarkedAsUnindexable]),
            throw({error, iolist_to_binary(ErrorMsg3)})
        end
    end,
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

    ActivePending = ?pending_transition_active(PendingTrans),
    PassivePending = ?pending_transition_passive(PendingTrans),
    IsEffectlessTransition =
        (ActiveMask bor ?set_abitmask(Group)) == ?set_abitmask(Group) andalso
        (PassiveMask bor ?set_pbitmask(Group)) == ?set_pbitmask(Group) andalso
        ((CleanupMask band (?set_abitmask(Group) bor ?set_pbitmask(Group))) == 0) andalso
        ordsets:is_disjoint(CleanupList, ActivePending) andalso
        ordsets:is_disjoint(CleanupList, PassivePending) andalso
        ordsets:is_disjoint(ActiveList, PassivePending) andalso
        ordsets:is_disjoint(PassiveList, ActivePending),

    case IsEffectlessTransition of
    true ->
        State;
    false ->
        RestartUpdater = updater_needs_restart(
            Group, ActiveMask, PassiveMask, CleanupMask),
        NewState = update_partition_states(
            ActiveList, PassiveList, CleanupList, State, RestartUpdater),
        #state{group = NewGroup, updater_pid = UpdaterPid} = NewState,
        case RestartUpdater of
        false when is_pid(UpdaterPid) ->
            case missing_partitions(Group, NewGroup) of
            [] ->
                ok;
            MissingPassive ->
                UpdaterPid ! {new_passive_partitions, MissingPassive}
            end;
        _ ->
            ok
        end,
        NewState
    end.


-spec update_partition_states(ordsets:ordset(partition_id()),
                              ordsets:ordset(partition_id()),
                              ordsets:ordset(partition_id()),
                              #state{},
                              boolean()) -> #state{}.
update_partition_states(ActiveList, PassiveList, CleanupList, State, RestartUpdater) ->
    State2 = stop_cleaner(State),
    case RestartUpdater of
    true ->
        #state{group = Group3} = State3 = stop_updater(State2);
    false ->
        #state{group = Group3} = State3 = State2
    end,
    UpdaterWasRunning = is_pid(State#state.updater_pid),
    ActiveInCleanup = partitions_still_in_cleanup(ActiveList, Group3),
    PassiveInCleanup = partitions_still_in_cleanup(PassiveList, Group3),
    NewPendingTrans = merge_into_pending_transition(
        Group3, ActiveInCleanup, PassiveInCleanup, CleanupList),
    ApplyActiveList = ordsets:subtract(ActiveList, ActiveInCleanup),
    ApplyPassiveList = ordsets:subtract(PassiveList, PassiveInCleanup),
    ApplyCleanupList = CleanupList,
    State4 = persist_partition_states(
               State3, ApplyActiveList, ApplyPassiveList,
               ApplyCleanupList, NewPendingTrans, []),
    State5 = notify_pending_transition_waiters(State4),
    after_partition_states_updated(State5, UpdaterWasRunning).


-spec merge_into_pending_transition(#set_view_group{},
                                    ordsets:ordset(partition_id()),
                                    ordsets:ordset(partition_id()),
                                    ordsets:ordset(partition_id())) ->
                                           #set_view_transition{} | 'nil'.
merge_into_pending_transition(Group, ActiveInCleanup, PassiveInCleanup, CleanupList) ->
    PendingTrans = ?set_pending_transition(Group),
    ActivePending = ?pending_transition_active(PendingTrans),
    PassivePending = ?pending_transition_passive(PendingTrans),
    case ordsets:intersection(PassivePending, ActiveInCleanup) of
    [] ->
        PassivePending2 = PassivePending;
    Int ->
        PassivePending2 = ordsets:subtract(PassivePending, Int)
    end,
    case ordsets:intersection(ActivePending, PassiveInCleanup) of
    [] ->
        ActivePending2 = ActivePending;
    Int2 ->
        ActivePending2 = ordsets:subtract(ActivePending, Int2)
    end,
    ActivePending3 = ordsets:subtract(ActivePending2, CleanupList),
    PassivePending3 = ordsets:subtract(PassivePending2, CleanupList),
    ActivePending4 = ordsets:union(ActivePending3, ActiveInCleanup),
    PassivePending4 = ordsets:union(PassivePending3, PassiveInCleanup),
    case (ActivePending4 == []) andalso (PassivePending4 == []) of
    true ->
        nil;
    false ->
        #set_view_transition{
            active = ActivePending4,
            passive = PassivePending4
        }
    end.


-spec after_partition_states_updated(#state{}, boolean()) -> #state{}.
after_partition_states_updated(State, UpdaterWasRunning) ->
    State2 = case UpdaterWasRunning of
    true ->
        % Updater was running, we stopped it, updated the group we received
        % from the updater, updated that group's bitmasks and update seqs,
        % and now restart the updater with this modified group.
        start_updater(State);
    false ->
        State
    end,
    State3 = stop_compactor(State2),
    maybe_start_cleaner(State3).


-spec persist_partition_states(#state{},
                               ordsets:ordset(partition_id()),
                               ordsets:ordset(partition_id()),
                               ordsets:ordset(partition_id()),
                               #set_view_transition{} | 'nil',
                               ordsets:ordset(partition_id())) -> #state{}.
persist_partition_states(State, ActiveList, PassiveList, CleanupList, PendingTrans, ToBeUnindexable) ->
    % There can never be intersection between given active, passive and cleanup lists.
    % This check is performed elsewhere, outside the gen_server.
    #state{
        group = Group,
        replica_partitions = ReplicaParts,
        replica_group = ReplicaPid,
        update_listeners = Listeners,
        waiting_list = WaitList
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
        ReplicasToCleanup = [],
        PassiveList3 = PassiveList2,
        ReplicasOnTransfer3 = ReplicasOnTransfer2;
    CommonRep2 ->
        ReplicasToCleanup = CommonRep2,
        PassiveList3 = ordsets:subtract(PassiveList2, CommonRep2),
        ReplicasOnTransfer3 = ordsets:subtract(ReplicasOnTransfer2, CommonRep2)
    end,
    case ordsets:intersection(CleanupList, ReplicasOnTransfer3) of
    [] ->
        ReplicaParts2 = ReplicaParts,
        ReplicasOnTransfer4 = ReplicasOnTransfer3,
        ReplicasToCleanup2 = ReplicasToCleanup;
    CommonRep3 ->
        ReplicaParts2 = ordsets:subtract(ReplicaParts, CommonRep3),
        ReplicasOnTransfer4 = ordsets:subtract(ReplicasOnTransfer3, CommonRep3),
        ReplicasToCleanup2 = ordsets:union(ReplicasToCleanup, CommonRep3)
    end,
    {ok, NewAbitmask1, NewPbitmask1, NewSeqs1} =
        set_active_partitions(
            ActiveList2,
            ?set_abitmask(Group),
            ?set_pbitmask(Group),
            ?set_seqs(Group)),
    {ok, NewAbitmask2, NewPbitmask2, NewSeqs2} =
        set_passive_partitions(
            PassiveList3,
            NewAbitmask1,
            NewPbitmask1,
            NewSeqs1),
    {ok, NewAbitmask3, NewPbitmask3, NewCbitmask3, NewSeqs3} =
        set_cleanup_partitions(
            CleanupList,
            NewAbitmask2,
            NewPbitmask2,
            ?set_cbitmask(Group),
            NewSeqs2),
    {NewSeqs4, NewUnindexableSeqs} = lists:foldl(
        fun(PartId, {AccSeqs, AccUnSeqs}) ->
            PartSeq = couch_set_view_util:get_part_seq(PartId, AccSeqs),
            AccSeqs2 = orddict:erase(PartId, AccSeqs),
            AccUnSeqs2 = orddict:store(PartId, PartSeq, AccUnSeqs),
            {AccSeqs2, AccUnSeqs2}
        end,
        {NewSeqs3, ?set_unindexable_seqs(Group)},
        ToBeUnindexable),
    ok = couch_db_set:remove_partitions(?db_set(State), CleanupList),
    State2 = demonitor_partitions(State, CleanupList),
    State3 = update_header(
        State2,
        NewAbitmask3,
        NewPbitmask3,
        NewCbitmask3,
        NewSeqs4,
        NewUnindexableSeqs,
        ReplicasOnTransfer4,
        ReplicaParts2,
        PendingTrans),
    % A crash might happen between updating our header and updating the state of
    % replica view group. The init function must detect and correct this.
    ok = set_state(ReplicaPid, ReplicasToMarkActive, [], ReplicasToCleanup2),
    % Need to update list of active partition sequence numbers for every blocked client.
    WaitList2 = update_waiting_list(
        WaitList, ?db_set(State), ActiveList2, PassiveList3, CleanupList),
    State4 = State3#state{waiting_list = WaitList2},
    case (dict:size(Listeners) > 0) andalso (CleanupList /= []) of
    true ->
        Listeners2 = dict:filter(
            fun(Ref, Listener) ->
                #up_listener{
                    pid = Pid,
                    monref = MonRef,
                    partition = PartId
                } = Listener,
                case lists:member(PartId, CleanupList) of
                true ->
                    ?LOG_INFO("Set view `~s`, ~s group `~s`, replying to partition ~p"
                               "update monitor, reference ~p, error: marked_for_cleanup",
                               [?set_name(State), ?type(State), ?group_id(State),
                                Ref, PartId]),
                    Pid ! {Ref, marked_for_cleanup},
                    erlang:demonitor(MonRef, [flush]),
                    false;
                false ->
                    true
                end
            end,
            Listeners),
        State4#state{update_listeners = Listeners2};
    false ->
        State4
    end.


-spec update_waiting_list([#waiter{}],
                          pid(),
                          ordsets:ordset(partition_id()),
                          ordsets:ordset(partition_id()),
                          ordsets:ordset(partition_id())) -> [#waiter{}].
update_waiting_list([], _DbSet, _AddActiveList, _AddPassiveList, _AddCleanupList) ->
    [];
update_waiting_list(WaitList, DbSet, AddActiveList, AddPassiveList, AddCleanupList) ->
    {ok, AddActiveSeqs} = couch_db_set:get_seqs(DbSet, AddActiveList, false),
    RemoveSet = ordsets:union(AddPassiveList, AddCleanupList),
    MapFun = fun(W) -> update_waiter_seqs(W, AddActiveSeqs, RemoveSet) end,
    [MapFun(W) || W <- WaitList].


-spec update_waiter_seqs(#waiter{},
                         partition_seqs(),
                         ordsets:ordset(partition_id())) -> #waiter{}.
update_waiter_seqs(Waiter, AddActiveSeqs, ToRemove) ->
    Seqs2 = lists:foldl(
        fun({PartId, Seq}, Acc) ->
            case couch_set_view_util:has_part_seq(PartId, Acc) of
            true ->
                Acc;
            false ->
                orddict:store(PartId, Seq, Acc)
            end
        end,
        Waiter#waiter.seqs, AddActiveSeqs),
    Seqs3 = lists:foldl(
        fun(PartId, Acc) -> orddict:erase(PartId, Acc) end,
        Seqs2, ToRemove),
    Waiter#waiter{seqs = Seqs3}.


-spec maybe_apply_pending_transition(#state{}) -> #state{}.
maybe_apply_pending_transition(State) when not ?have_pending_transition(State) ->
    State;
maybe_apply_pending_transition(State) ->
    State2 = stop_cleaner(State),
    #state{group = Group3} = State3 = stop_updater(State2),
    UpdaterWasRunning = is_pid(State#state.updater_pid),
    #set_view_transition{
        active = ActivePending,
        passive = PassivePending,
        unindexable = UnindexablePending
    } = get_pending_transition(State),
    ActiveInCleanup = partitions_still_in_cleanup(ActivePending, Group3),
    PassiveInCleanup = partitions_still_in_cleanup(PassivePending, Group3),
    ApplyActiveList = ordsets:subtract(ActivePending, ActiveInCleanup),
    ApplyPassiveList = ordsets:subtract(PassivePending, PassiveInCleanup),
    {ApplyUnindexableList, NewUnindexablePending} = lists:partition(
        fun(P) ->
            lists:member(P, ApplyActiveList) orelse
            lists:member(P, ApplyPassiveList)
        end,
        UnindexablePending),
    case (ApplyActiveList /= []) orelse (ApplyPassiveList /= []) of
    true ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, applying state transitions "
                  "from pending transition:~n"
                  "  Active partitions:  ~w~n"
                  "  Passive partitions: ~w~n"
                  "  Unindexable:        ~w~n",
                  [?set_name(State), ?type(State), ?group_id(State),
                   ApplyActiveList, ApplyPassiveList, ApplyUnindexableList]),
        case (ActiveInCleanup == []) andalso (PassiveInCleanup == []) of
        true ->
            NewPendingTrans = nil;
        false ->
            NewPendingTrans = #set_view_transition{
                active = ActiveInCleanup,
                passive = PassiveInCleanup,
                unindexable = NewUnindexablePending
            }
        end,
        State4 = set_pending_transition(State3, NewPendingTrans),
        State5 = persist_partition_states(
            State4, ApplyActiveList, ApplyPassiveList, [], NewPendingTrans, ApplyUnindexableList),
        State6 = notify_pending_transition_waiters(State5),
        NewState = case dict:size(State6#state.update_listeners) > 0 of
        true ->
            start_updater(State6);
        false ->
            State6
        end;
    false ->
        NewState = State3
    end,
    after_partition_states_updated(NewState, UpdaterWasRunning).


-spec notify_pending_transition_waiters(#state{}) -> #state{}.
notify_pending_transition_waiters(#state{pending_transition_waiters = []} = State) ->
    State;
notify_pending_transition_waiters(State) ->
    #state{
        pending_transition_waiters = TransWaiters,
        group = Group,
        replica_partitions = RepParts,
        waiting_list = WaitList
    } = State,
    CurSeqs = active_partition_seqs(State, true),
    {TransWaiters2, WaitList2, GroupReplyList, TriggerGroupUpdate} =
        lists:foldr(
            fun({From, Req} = TransWaiter, {AccTrans, AccWait, ReplyAcc, AccTriggerUp}) ->
                #set_view_group_req{
                    stale = Stale,
                    debug = Debug
                } = Req,
                case is_any_partition_pending(Req, Group) of
                true ->
                    {[TransWaiter | AccTrans], AccWait, ReplyAcc, AccTriggerUp};
                false when Stale == ok ->
                    Waiter = #waiter{from = From, debug = Debug},
                    {AccTrans, AccWait, [Waiter | ReplyAcc], AccTriggerUp};
                false when Stale == update_after ->
                    Waiter = #waiter{from = From, debug = Debug},
                    {AccTrans, AccWait, [Waiter | ReplyAcc], true};
                false when Stale == false ->
                    Waiter = #waiter{from = From, debug = Debug, seqs = CurSeqs},
                    {AccTrans, [Waiter | AccWait], ReplyAcc, true}
                end
            end,
            {[], WaitList, [], false},
            TransWaiters),
    [] = reply_with_group(Group, RepParts, GroupReplyList),
    WaitList3 = reply_with_group(Group, RepParts, WaitList2),
    State2 = State#state{
        pending_transition_waiters = TransWaiters2,
        waiting_list = WaitList3
    },
    case TriggerGroupUpdate of
    true ->
        start_updater(State2);
    false ->
        State2
    end.


-spec notify_pending_transition_waiters(#state{}, term()) -> #state{}.
notify_pending_transition_waiters(#state{pending_transition_waiters = []} = State, _Reply) ->
    State;
notify_pending_transition_waiters(#state{pending_transition_waiters = Waiters} = State, Reply) ->
    lists:foreach(fun(F) -> catch gen_server:reply(F, Reply) end, Waiters),
    State#state{pending_transition_waiters = []}.


-spec set_passive_partitions(ordsets:ordset(partition_id()),
                             bitmask(),
                             bitmask(),
                             partition_seqs()) ->
                                    {'ok', bitmask(), bitmask(), partition_seqs()}.
set_passive_partitions([], Abitmask, Pbitmask, Seqs) ->
    {ok, Abitmask, Pbitmask, Seqs};

set_passive_partitions([PartId | Rest], Abitmask, Pbitmask, Seqs) ->
    PartMask = 1 bsl PartId,
    case PartMask band Abitmask of
    0 ->
        case PartMask band Pbitmask of
        PartMask ->
            set_passive_partitions(Rest, Abitmask, Pbitmask, Seqs);
        0 ->
            NewSeqs = lists:ukeymerge(1, [{PartId, 0}], Seqs),
            set_passive_partitions(Rest, Abitmask, Pbitmask bor PartMask, NewSeqs)
        end;
    PartMask ->
        set_passive_partitions(Rest, Abitmask bxor PartMask, Pbitmask bor PartMask, Seqs)
    end.


-spec set_active_partitions(ordsets:ordset(partition_id()),
                            bitmask(),
                            bitmask(),
                            partition_seqs()) ->
                                   {'ok', bitmask(), bitmask(), partition_seqs()}.
set_active_partitions([], Abitmask, Pbitmask, Seqs) ->
    {ok, Abitmask, Pbitmask, Seqs};

set_active_partitions([PartId | Rest], Abitmask, Pbitmask, Seqs) ->
    PartMask = 1 bsl PartId,
    case PartMask band Pbitmask of
    0 ->
        case PartMask band Abitmask of
        PartMask ->
            set_active_partitions(Rest, Abitmask, Pbitmask, Seqs);
        0 ->
            NewSeqs = lists:ukeymerge(1, Seqs, [{PartId, 0}]),
            set_active_partitions(Rest, Abitmask bor PartMask, Pbitmask, NewSeqs)
        end;
    PartMask ->
        set_active_partitions(Rest, Abitmask bor PartMask, Pbitmask bxor PartMask, Seqs)
    end.


-spec set_cleanup_partitions(ordsets:ordset(partition_id()),
                             bitmask(),
                             bitmask(),
                             bitmask(),
                             partition_seqs()) ->
                                    {'ok', bitmask(), bitmask(), bitmask(),
                                     partition_seqs()}.
set_cleanup_partitions([], Abitmask, Pbitmask, Cbitmask, Seqs) ->
    {ok, Abitmask, Pbitmask, Cbitmask, Seqs};

set_cleanup_partitions([PartId | Rest], Abitmask, Pbitmask, Cbitmask, Seqs) ->
    PartMask = 1 bsl PartId,
    case PartMask band Cbitmask of
    PartMask ->
        set_cleanup_partitions(Rest, Abitmask, Pbitmask, Cbitmask, Seqs);
    0 ->
        Seqs2 = lists:keydelete(PartId, 1, Seqs),
        Cbitmask2 = Cbitmask bor PartMask,
        case PartMask band Abitmask of
        PartMask ->
            set_cleanup_partitions(
                Rest, Abitmask bxor PartMask, Pbitmask, Cbitmask2, Seqs2);
        0 ->
            case (PartMask band Pbitmask) of
            PartMask ->
                set_cleanup_partitions(
                    Rest, Abitmask, Pbitmask bxor PartMask, Cbitmask2, Seqs2);
            0 ->
                set_cleanup_partitions(Rest, Abitmask, Pbitmask, Cbitmask, Seqs)
            end
        end
    end.


-spec update_header(#state{},
                    bitmask(),
                    bitmask(),
                    bitmask(),
                    partition_seqs(),
                    partition_seqs(),
                    ordsets:ordset(partition_id()),
                    ordsets:ordset(partition_id()),
                    #set_view_transition{} | 'nil') -> #state{}.
update_header(State, NewAbitmask, NewPbitmask, NewCbitmask, NewSeqs, NewUnindexableSeqs,
              NewRelicasOnTransfer, NewReplicaParts, NewPendingTrans) ->
    #state{
        group = #set_view_group{
            index_header =
                #set_view_index_header{
                    abitmask = Abitmask,
                    pbitmask = Pbitmask,
                    cbitmask = Cbitmask,
                    replicas_on_transfer = ReplicasOnTransfer,
                    unindexable_seqs = UnindexableSeqs,
                    pending_transition = PendingTrans
                } = Header
        } = Group,
        replica_partitions = ReplicaParts
    } = State,
    NewGroup = Group#set_view_group{
        index_header = Header#set_view_index_header{
            abitmask = NewAbitmask,
            pbitmask = NewPbitmask,
            cbitmask = NewCbitmask,
            seqs = NewSeqs,
            unindexable_seqs = NewUnindexableSeqs,
            replicas_on_transfer = NewRelicasOnTransfer,
            pending_transition = NewPendingTrans
        }
    },
    PartitionsList = make_partitions_list(NewGroup),
    ok = couch_db_set:add_partitions(?db_set(State), PartitionsList),
    NewState0 = State#state{
        group = NewGroup,
        replica_partitions = NewReplicaParts
    },
    NewState = monitor_partitions(NewState0, PartitionsList),
    FsyncHeader = (NewCbitmask /= Cbitmask),
    ok = commit_header(NewState#state.group, FsyncHeader),
    ?LOG_INFO("Set view `~s`, ~s group `~s`, partition states updated~n"
              "active partitions before:    ~w~n"
              "active partitions after:     ~w~n"
              "passive partitions before:   ~w~n"
              "passive partitions after:    ~w~n"
              "cleanup partitions before:   ~w~n"
              "cleanup partitions after:    ~w~n"
              "unindexable partitions:      ~w~n"
              "replica partitions before:   ~w~n"
              "replica partitions after:    ~w~n"
              "replicas on transfer before: ~w~n"
              "replicas on transfer after:  ~w~n"
              "pending transition before:~n"
              "  active:      ~w~n"
              "  passive:     ~w~n"
              "  unindexable: ~w~n"
              "pending transition after:~n"
              "  active:      ~w~n"
              "  passive:     ~w~n"
              "  unindexable: ~w~n",
              [?set_name(State), ?type(State), ?group_id(State),
               couch_set_view_util:decode_bitmask(Abitmask),
               couch_set_view_util:decode_bitmask(NewAbitmask),
               couch_set_view_util:decode_bitmask(Pbitmask),
               couch_set_view_util:decode_bitmask(NewPbitmask),
               couch_set_view_util:decode_bitmask(Cbitmask),
               couch_set_view_util:decode_bitmask(NewCbitmask),
               UnindexableSeqs,
               ReplicaParts,
               NewReplicaParts,
               ReplicasOnTransfer,
               NewRelicasOnTransfer,
               ?pending_transition_active(PendingTrans),
               ?pending_transition_passive(PendingTrans),
               ?pending_transition_unindexable(PendingTrans),
               ?pending_transition_active(NewPendingTrans),
               ?pending_transition_passive(NewPendingTrans),
               ?pending_transition_unindexable(NewPendingTrans)]),
    NewState.


-spec maybe_start_cleaner(#state{}) -> #state{}.
maybe_start_cleaner(#state{cleaner_pid = Pid} = State) when is_pid(Pid) ->
    State;
maybe_start_cleaner(#state{auto_cleanup = false} = State) ->
    State;
maybe_start_cleaner(#state{group = Group} = State) ->
    case is_pid(State#state.compactor_pid) orelse
        is_pid(State#state.updater_pid) orelse (?set_cbitmask(Group) == 0) of
    true ->
        State;
    false ->
        Cleaner = spawn_link(fun() -> exit(cleaner(State)) end),
        ?LOG_INFO("Started cleanup process ~p for set view `~s`, ~s group `~s`",
                  [Cleaner, ?set_name(State), ?type(State), ?group_id(State)]),
        State#state{cleaner_pid = Cleaner}
    end.


-spec stop_cleaner(#state{}) -> #state{}.
stop_cleaner(#state{cleaner_pid = nil} = State) ->
    State;
stop_cleaner(#state{cleaner_pid = Pid} = State) when is_pid(Pid) ->
    MRef = erlang:monitor(process, Pid),
    Pid ! stop,
    unlink(Pid),
    ?LOG_INFO("Stopping cleanup process for set view `~s`, group `~s`",
        [?set_name(State), ?group_id(State)]),
    NewState = receive
    {'EXIT', Pid, Reason} ->
        after_cleaner_stopped(State, Reason);
    {'DOWN', MRef, process, Pid, Reason} ->
        receive {'EXIT', Pid, _} -> ok after 0 -> ok end,
        after_cleaner_stopped(State, Reason)
    after 5000 ->
        couch_util:shutdown_sync(Pid),
        ?LOG_ERROR("Timeout stopping cleanup process ~p for set view `~s`, ~s group `~s`",
                   [Pid, ?set_name(State), ?type(State), ?group_id(State)]),
        State#state{cleaner_pid = nil}
    end,
    erlang:demonitor(MRef, [flush]),
    NewState.


after_cleaner_stopped(State, {clean_group, NewGroup0, Count, Time}) ->
    #state{group = OldGroup} = State,
    NewGroup = update_clean_group_seqs(OldGroup, NewGroup0),
    ?LOG_INFO("Stopped cleanup process for set view `~s`, ~s group `~s`.~n"
              "Removed ~p values from the index in ~.3f seconds~n"
              "New set of partitions to cleanup: ~w~n"
              "Old set of partitions to cleanup: ~w~n",
              [?set_name(State), ?type(State), ?group_id(State), Count, Time,
               couch_set_view_util:decode_bitmask(?set_cbitmask(NewGroup)),
               couch_set_view_util:decode_bitmask(?set_cbitmask(OldGroup))]),
    case ?set_cbitmask(NewGroup) of
    0 ->
        inc_cleanups(State#state.group, Time, Count, false);
    _ ->
        ?inc_cleanup_stops(State#state.group)
    end,
    State#state{
        group = NewGroup,
        cleaner_pid = nil
    };
after_cleaner_stopped(#state{cleaner_pid = Pid} = State, Reason) ->
    ?LOG_ERROR("Cleanup process ~p for set view `~s`, ~s group `~s`, died "
               "with reason: ~p",
               [Pid, ?set_name(State), ?type(State), ?group_id(State), Reason]),
    State#state{cleaner_pid = nil}.


-spec cleaner(#state{}) -> {'clean_group', #set_view_group{}, non_neg_integer(), float()}.
cleaner(#state{group = Group}) ->
    StartTime = os:timestamp(),
    {ok, NewGroup, TotalPurgedCount} = couch_set_view_util:cleanup_group(Group),
    Duration = timer:now_diff(os:timestamp(), StartTime) / 1000000,
    {clean_group, NewGroup, TotalPurgedCount, Duration}.


-spec indexable_partition_seqs(#state{}) -> partition_seqs().
indexable_partition_seqs(#state{group = Group} = State) ->
    Sync = (dict:size(State#state.update_listeners) > 0) orelse
        (State#state.waiting_list /= []),
    {ok, CurSeqs} = case ?set_unindexable_seqs(Group) of
    [] ->
        couch_db_set:get_seqs(?db_set(State), Sync);
    _ ->
        couch_db_set:get_seqs(?db_set(State), [P || {P, _} <- ?set_seqs(Group)], Sync)
    end,
    CurSeqs.


-spec active_partition_seqs(#state{}, boolean()) -> partition_seqs().
active_partition_seqs(#state{group = Group} = State, Sync) ->
    ActiveParts = couch_set_view_util:decode_bitmask(?set_abitmask(Group)),
    {ok, CurSeqs} = couch_db_set:get_seqs(?db_set(State), ActiveParts, Sync),
    CurSeqs.


-spec make_partitions_list(#set_view_group{}) -> ordsets:ordset(partition_id()).
make_partitions_list(Group) ->
    Indexable = lists:map(fun({P, _S}) -> P end, ?set_seqs(Group)),
    Unindexable = lists:map(fun({P, _S}) -> P end, ?set_unindexable_seqs(Group)),
    ordsets:union(Indexable, Unindexable).


-spec start_compactor(#state{}, compact_fun()) -> #state{}.
start_compactor(State, CompactFun) ->
    #state{group = Group} = State2 = stop_cleaner(State),
    ?LOG_INFO("Set view `~s`, ~s group `~s`, compaction starting",
              [?set_name(State2), ?type(State), ?group_id(State2)]),
    #set_view_group{
        fd = CompactFd
    } = NewGroup = compact_group(State2),
    Owner = self(),
    TmpDir = updater_tmp_dir(State2),
    Pid = spawn_link(fun() ->
        CompactFun(Group,
                   NewGroup,
                   TmpDir,
                   State#state.updater_pid,
                   Owner)
    end),
    State2#state{
        compactor_pid = Pid,
        compactor_fun = CompactFun,
        compactor_file = CompactFd
    }.


-spec stop_compactor(#state{}) -> #state{}.
stop_compactor(#state{compactor_pid = nil} = State) ->
    State;
stop_compactor(#state{compactor_pid = Pid, compactor_file = CompactFd} = State) ->
    couch_util:shutdown_sync(Pid),
    couch_util:shutdown_sync(CompactFd),
    CompactFile = compact_file_name(State),
    ok = couch_file:delete(?root_dir(State), CompactFile),
    case ?set_cbitmask(State#state.group) of
    0 ->
        ok;
    _ ->
        ?inc_cleanup_stops(State#state.group)
    end,
    inc_util_stat(#util_stats.compactor_interruptions, 1),
    State#state{compactor_pid = nil, compactor_file = nil, compact_log_files = nil}.


-spec compact_group(#state{}) -> #set_view_group{}.
compact_group(#state{group = Group} = State) ->
    CompactFilepath = compact_file_name(State),
    {ok, Fd} = open_index_file(CompactFilepath),
    reset_file(Fd, Group#set_view_group{filepath = CompactFilepath}).


-spec stop_updater(#state{}) -> #state{}.
stop_updater(#state{updater_pid = nil} = State) ->
    State;
stop_updater(#state{updater_pid = Pid, initial_build = true} = State) when is_pid(Pid) ->
    LostTime = updater_lost_time(),
    ?LOG_INFO("Stopping updater for set view `~s`, ~s group `~s` "
              "(doing initial index build), wasted indexing time ~.3f seconds.",
        [?set_name(State), ?type(State), ?group_id(State), LostTime]),
    couch_util:shutdown_sync(Pid),
    inc_util_stat(#util_stats.updater_interruptions, 1),
    inc_util_stat(#util_stats.wasted_indexing_time, LostTime),
    State#state{
        updater_pid = nil,
        initial_build = false,
        updater_state = not_running
    };
stop_updater(#state{updater_pid = Pid} = State) when is_pid(Pid) ->
    MRef = erlang:monitor(process, Pid),
    exit(Pid, shutdown),
    unlink(Pid),
    ?LOG_INFO("Stopping updater for set view `~s`, ~s group `~s`",
        [?set_name(State), ?type(State), ?group_id(State)]),
    State2 = process_last_updater_group(State, nil),
    NewState = receive
    {'EXIT', Pid, Reason} ->
        after_updater_stopped(State2, Reason);
    {'DOWN', MRef, process, Pid, Reason} ->
        receive {'EXIT', Pid, _} -> ok after 0 -> ok end,
        after_updater_stopped(State2, Reason)
    end,
    erlang:demonitor(MRef, [flush]),
    NewState.


after_updater_stopped(State, {updater_finished, Result}) ->
    #set_view_updater_result{
        stats = Stats,
        group = NewGroup,
        state = UpdaterFinishState
    } = Result,
    #set_view_updater_stats{
        indexing_time = IndexingTime,
        blocked_time = BlockedTime,
        inserted_ids = InsertedIds,
        deleted_ids = DeletedIds,
        inserted_kvs = InsertedKVs,
        deleted_kvs = DeletedKVs,
        cleanup_kv_count = CleanupKVCount,
        seqs = SeqsDone
    } = Stats,
    ?LOG_INFO("Set view `~s`, ~s group `~s`, updater stopped~n"
              "Indexing time: ~.3f seconds~n"
              "Blocked time:  ~.3f seconds~n"
              "Inserted IDs:  ~p~n"
              "Deleted IDs:   ~p~n"
              "Inserted KVs:  ~p~n"
              "Deleted KVs:   ~p~n"
              "Cleaned KVs:   ~p~n"
              "# seqs done:   ~p~n",
              [?set_name(State), ?type(State), ?group_id(State), IndexingTime, BlockedTime,
               InsertedIds, DeletedIds, InsertedKVs, DeletedKVs, CleanupKVCount, SeqsDone]),
    State2 = process_partial_update(State, NewGroup),
    case UpdaterFinishState of
    updating_active ->
        inc_updates(State2#state.group, Result, true, true),
        WaitingList2 = State2#state.waiting_list;
    updating_passive ->
        PartialUpdate = (?set_pbitmask(NewGroup) =/= 0),
        inc_updates(State2#state.group, Result, PartialUpdate, false),
        WaitingList2 = reply_with_group(
            NewGroup, State2#state.replica_partitions, State2#state.waiting_list)
    end,
    State2#state{
        updater_pid = nil,
        initial_build = false,
        updater_state = not_running,
        waiting_list = WaitingList2
     };
after_updater_stopped(State, _Reason) ->
    IndexingTime = updater_indexing_time(),
    LostTime = updater_lost_time(),
    ?LOG_INFO("Stopped updater, set view `~s`, ~s group `~s`, "
              "useful indexing time of ~.3f seconds, "
              "wasted indexing time of ~.3f seconds.",
              [?set_name(State), ?type(State), ?group_id(State), IndexingTime, LostTime]),
    inc_util_stat(#util_stats.updater_interruptions, 1),
    inc_util_stat(#util_stats.useful_indexing_time, IndexingTime),
    inc_util_stat(#util_stats.wasted_indexing_time, LostTime),
    State#state{
        updater_pid = nil,
        initial_build = false,
        updater_state = not_running
    }.


-spec process_last_updater_group(#state{}, 'nil' | #set_view_group{}) -> #state{}.
process_last_updater_group(#state{updater_pid = Pid} = State, Group) ->
    receive
    {partial_update, Pid, NewGroup} ->
         process_last_updater_group(State, NewGroup)
    after 0 ->
         case Group of
         nil ->
             State;
         _ ->
             process_partial_update(State, Group)
         end
    end.


start_updater(State) ->
    start_updater(State, []).

-spec start_updater(#state{}, [term()]) -> #state{}.
start_updater(#state{updater_pid = Pid} = State, _Options) when is_pid(Pid) ->
    State;
start_updater(#state{updater_pid = nil, updater_state = not_running} = State, Options) ->
    #state{
        group = Group,
        replica_partitions = ReplicaParts,
        waiting_list = WaitList
    } = State,
    CurSeqs = indexable_partition_seqs(State),
    case CurSeqs > ?set_seqs(Group) of
    true ->
        do_start_updater(State, CurSeqs, Options);
    false ->
        WaitList2 = reply_with_group(Group, ReplicaParts, WaitList),
        State#state{waiting_list = WaitList2}
    end.


-spec do_start_updater(#state{}, partition_seqs(), [term()]) -> #state{}.
do_start_updater(State, CurSeqs, Options) ->
    #state{
        group = Group,
        compactor_pid = CompactPid
    } = State2 = stop_cleaner(State),
    ?LOG_INFO("Starting updater for set view `~s`, ~s group `~s`",
              [?set_name(State), ?type(State), ?group_id(State)]),
    TmpDir = updater_tmp_dir(State),
    CompactRunning = is_pid(CompactPid) andalso is_process_alive(CompactPid),
    reset_updater_start_time(),
    Pid = spawn_link(couch_set_view_updater, update,
                     [self(), Group, CurSeqs, CompactRunning, TmpDir, Options]),
    State2#state{
        updater_pid = Pid,
        initial_build = couch_set_view_util:is_group_empty(Group),
        updater_state = starting
    }.


-spec partitions_still_in_cleanup(ordsets:ordset(partition_id()),
                                  #set_view_group{}) ->
                                         ordsets:ordset(partition_id()).
partitions_still_in_cleanup(Parts, Group) ->
    partitions_still_in_cleanup(Parts, Group, []).

-spec partitions_still_in_cleanup(ordsets:ordset(partition_id()),
                                  #set_view_group{},
                                  [partition_id()]) ->
                                         ordsets:ordset(partition_id()).
partitions_still_in_cleanup([], _Group, Acc) ->
    lists:reverse(Acc);
partitions_still_in_cleanup([PartId | Rest], Group, Acc) ->
    Mask = 1 bsl PartId,
    case Mask band ?set_cbitmask(Group) of
    Mask ->
        partitions_still_in_cleanup(Rest, Group, [PartId | Acc]);
    0 ->
        partitions_still_in_cleanup(Rest, Group, Acc)
    end.


-spec open_replica_group(init_args()) -> pid().
open_replica_group({RootDir, SetName, Group} = _InitArgs) ->
    ReplicaArgs = {RootDir, SetName, Group#set_view_group{type = replica}},
    {ok, Pid} = proc_lib:start_link(?MODULE, init, [ReplicaArgs]),
    Pid.


-spec get_replica_partitions(pid()) -> ordsets:ordset(partition_id()).
get_replica_partitions(ReplicaPid) ->
    {ok, Group} = gen_server:call(ReplicaPid, request_group, infinity),
    couch_set_view_util:decode_bitmask(?set_abitmask(Group) bor ?set_pbitmask(Group)).


-spec maybe_fix_replica_group(pid(), #set_view_group{}) -> 'ok'.
maybe_fix_replica_group(ReplicaPid, Group) ->
    case is_view_defined(ReplicaPid) of
    true ->
        ok;
    false ->
        Params = #set_view_params{
            max_partitions = ?set_num_partitions(Group),
            use_replica_index = false
        },
        ok = define_view(ReplicaPid, Params)
    end,
    {ok, RepGroup} = gen_server:call(ReplicaPid, request_group, infinity),
    RepGroupActive = couch_set_view_util:decode_bitmask(?set_abitmask(RepGroup)),
    RepGroupPassive = couch_set_view_util:decode_bitmask(?set_pbitmask(RepGroup)),
    CleanupList = lists:foldl(
        fun(PartId, Acc) ->
            case lists:member(PartId, ?set_replicas_on_transfer(Group)) of
            true ->
                Acc;
            false ->
                [PartId | Acc]
            end
        end,
        [], RepGroupActive),
    ActiveList = lists:foldl(
        fun(PartId, Acc) ->
            case lists:member(PartId, ?set_replicas_on_transfer(Group)) of
            true ->
                [PartId | Acc];
            false ->
                Acc
            end
        end,
        [], RepGroupPassive),
    case CleanupList of
    [] ->
        ok;
    _ ->
        ?LOG_INFO("Set view `~s`, main group `~s`, fixing replica group by marking "
                  " partitions ~w for cleanup because they were already transferred into "
                  " the main group",
                  [Group#set_view_group.set_name, Group#set_view_group.set_name, CleanupList])
    end,
    case ActiveList of
    [] ->
        ok;
    _ ->
        ?LOG_INFO("Set view `~s`, main group `~s`, fixing replica group by marking "
                  " partitions ~w as active because they are marked as on transfer in "
                  " the main group",
                  [Group#set_view_group.set_name, Group#set_view_group.set_name, ActiveList])
    end,
    ok = set_state(ReplicaPid, ActiveList, [], CleanupList).


-spec process_partial_update(#state{}, #set_view_group{}) -> #state{}.
process_partial_update(State, NewGroup0) ->
    #state{
        group = #set_view_group{fd = Fd} = Group,
        update_listeners = Listeners
    } = State,
    set_last_updater_checkpoint_ts(),
    ReplicasTransferred = ordsets:subtract(
        ?set_replicas_on_transfer(Group), ?set_replicas_on_transfer(NewGroup0)),
    NewState = case ReplicasTransferred of
    [] ->
        NewGroup1 = fix_updater_group(NewGroup0, Group),
        State#state{group = NewGroup1};
    _ ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, completed transferral of replica partitions ~w~n"
                  "New group of replica partitions to transfer is ~w~n",
                  [?set_name(State), ?type(State), ?group_id(State),
                   ReplicasTransferred, ?set_replicas_on_transfer(NewGroup0)]),
        ok = set_state(State#state.replica_group, [], [], ReplicasTransferred),
        State#state{
            group = NewGroup0,
            replica_partitions = ordsets:subtract(State#state.replica_partitions, ReplicasTransferred)
        }
    end,
    HeaderBin = couch_set_view_util:group_to_header_bin(NewState#state.group),
    ok = couch_file:write_header_bin(Fd, HeaderBin),
    Listeners2 = notify_update_listeners(NewState, Listeners, NewState#state.group),
    ok = couch_file:flush(Fd),
    NewState#state{update_listeners = Listeners2}.


-spec notify_update_listeners(#state{}, dict(), #set_view_group{}) -> dict().
notify_update_listeners(State, Listeners, NewGroup) ->
    case dict:size(Listeners) == 0 of
    true ->
        Listeners;
    false ->
        dict:filter(
            fun(Ref, Listener) ->
                #up_listener{
                    pid = Pid,
                    monref = MonRef,
                    seq = Seq,
                    partition = PartId
                } = Listener,
                case couch_set_view_util:find_part_seq(PartId, ?set_seqs(NewGroup)) of
                {ok, IndexedSeq} when IndexedSeq >= Seq ->
                   ?LOG_INFO("Set view `~s`, ~s group `~s`, replying to partition ~p"
                              " update monitor, reference ~p, desired indexed seq ~p,"
                              " indexed seq ~p",
                              [?set_name(State), ?type(State), ?group_id(State),
                               PartId, Ref, Seq, IndexedSeq]),
                    Pid ! {Ref, updated},
                    erlang:demonitor(MonRef, [flush]),
                    false;
                {ok, IndexedSeq} ->
                   ?LOG_INFO("Set view `~s`, ~s group `~s`, not replying yet to partition"
                             " ~p update monitor, reference ~p, desired indexed seq ~p,"
                             " indexed seq ~p",
                             [?set_name(State), ?type(State), ?group_id(State),
                              PartId, Ref, Seq, IndexedSeq]),
                    true
                end
            end,
            Listeners)
    end.


-spec error_notify_update_listeners(#state{}, dict(), monitor_error()) -> dict().
error_notify_update_listeners(State, Listeners, Error) ->
    _ = dict:fold(
        fun(Ref, #up_listener{pid = ListPid, partition = PartId}, _Acc) ->
            ?LOG_INFO("Set view `~s`, ~s group `~s`, replying to partition ~p"
                       "update monitor, reference ~p, error: ~p",
                       [?set_name(State), ?type(State), ?group_id(State),
                        Ref, PartId, Error]),
            ListPid ! {Ref, Error}
        end,
        ok, Listeners),
    dict:new().


-spec inc_updates(#set_view_group{},
                  #set_view_updater_result{},
                  boolean(),
                  boolean()) -> no_return().
inc_updates(Group, UpdaterResult, PartialUpdate, ForcedStop) ->
    [Stats] = ets:lookup(?SET_VIEW_STATS_ETS, ?set_view_group_stats_key(Group)),
    #set_view_group_stats{update_history = Hist} = Stats,
    #set_view_updater_stats{
        indexing_time = IndexingTime,
        blocked_time = BlockedTime,
        cleanup_kv_count = CleanupKvCount,
        cleanup_time = CleanupTime,
        inserted_ids = InsertedIds,
        deleted_ids = DeletedIds,
        inserted_kvs = InsertedKvs,
        deleted_kvs = DeletedKvs
    } = UpdaterResult#set_view_updater_result.stats,
    inc_util_stat(#util_stats.updates, 1),
    inc_util_stat(#util_stats.useful_indexing_time, IndexingTime),
    Entry = {
        case PartialUpdate of
        true ->
            [{<<"partial_update">>, true}];
        false ->
            []
        end ++
        case ForcedStop of
        true ->
            [{<<"forced_stop">>, true}];
        false ->
            []
        end ++ [
        {<<"indexing_time">>, IndexingTime},
        {<<"blocked_time">>, BlockedTime},
        {<<"cleanup_kv_count">>, CleanupKvCount},
        {<<"inserted_ids">>, InsertedIds},
        {<<"deleted_ids">>, DeletedIds},
        {<<"inserted_kvs">>, InsertedKvs},
        {<<"deleted_kvs">>, DeletedKvs}
    ]},
    Stats2 = Stats#set_view_group_stats{
        update_history = lists:sublist([Entry | Hist], ?MAX_HIST_SIZE),
        partial_updates = case PartialUpdate of
            true  -> Stats#set_view_group_stats.partial_updates + 1;
            false -> Stats#set_view_group_stats.partial_updates
            end,
        stopped_updates = case ForcedStop of
            true  -> Stats#set_view_group_stats.stopped_updates + 1;
            false -> Stats#set_view_group_stats.stopped_updates
            end,
        full_updates = case (not PartialUpdate) andalso (not ForcedStop) of
            true  -> Stats#set_view_group_stats.full_updates + 1;
            false -> Stats#set_view_group_stats.full_updates
            end
    },
    case CleanupKvCount > 0 of
    true ->
        inc_cleanups(Stats2, CleanupTime, CleanupKvCount, true);
    false ->
        true = ets:insert(?SET_VIEW_STATS_ETS, Stats2)
    end.


-spec inc_cleanups(#set_view_group{} | #set_view_group_stats{},
                   float(),
                   non_neg_integer(),
                   boolean()) -> no_return().
inc_cleanups(Group, Duration, Count, ByUpdater) when is_record(Group, set_view_group) ->
    [Stats] = ets:lookup(?SET_VIEW_STATS_ETS, ?set_view_group_stats_key(Group)),
    inc_cleanups(Stats, Duration, Count, ByUpdater);

inc_cleanups(#set_view_group_stats{cleanup_history = Hist} = Stats, Duration, Count, ByUpdater) ->
    Entry = {[
        {<<"duration">>, Duration},
        {<<"kv_count">>, Count}
    ]},
    Stats2 = Stats#set_view_group_stats{
        cleanups = Stats#set_view_group_stats.cleanups + 1,
        cleanup_history = lists:sublist([Entry | Hist], ?MAX_HIST_SIZE),
        updater_cleanups = case ByUpdater of
            true ->
                Stats#set_view_group_stats.updater_cleanups + 1;
            false ->
                Stats#set_view_group_stats.updater_cleanups
            end
    },
    true = ets:insert(?SET_VIEW_STATS_ETS, Stats2).


-spec inc_compactions(#set_view_compactor_result{}) -> no_return().
inc_compactions(Result) ->
    #set_view_compactor_result{
        group = Group,
        compact_time = Duration,
        cleanup_kv_count = CleanupKVCount
    } = Result,
    inc_util_stat(#util_stats.compactions, 1),
    [Stats] = ets:lookup(?SET_VIEW_STATS_ETS, ?set_view_group_stats_key(Group)),
    #set_view_group_stats{compaction_history = Hist} = Stats,
    Entry = {[
        {<<"duration">>, Duration},
        {<<"cleanup_kv_count">>, CleanupKVCount}
    ]},
    Stats2 = Stats#set_view_group_stats{
        compactions = Stats#set_view_group_stats.compactions + 1,
        compaction_history = lists:sublist([Entry | Hist], ?MAX_HIST_SIZE),
        cleanups = case CleanupKVCount of
            0 ->
                Stats#set_view_group_stats.cleanups;
            _ ->
                Stats#set_view_group_stats.cleanups + 1
        end
    },
    true = ets:insert(?SET_VIEW_STATS_ETS, Stats2).


-spec new_fd_ref_counter(pid()) -> pid().
new_fd_ref_counter(Fd) ->
    {ok, RefCounter} = couch_ref_counter:start([Fd]),
    RefCounter.


-spec inc_view_group_access_stats(#set_view_group_req{},
                                  #set_view_group{}) -> no_return().
inc_view_group_access_stats(#set_view_group_req{update_stats = true}, Group) ->
    ?inc_accesses(Group);
inc_view_group_access_stats(_Req, _Group) ->
    ok.


-spec get_pending_transition(#state{} | #set_view_group{}) ->
                                    #set_view_transition{} | 'nil'.
get_pending_transition(#state{group = Group}) ->
    get_pending_transition(Group);
get_pending_transition(#set_view_group{index_header = Header}) ->
    Header#set_view_index_header.pending_transition.


-spec set_pending_transition(#state{}, #set_view_transition{} | 'nil') -> #state{}.
set_pending_transition(#state{group = Group} = State, Transition) ->
    #set_view_group{index_header = IndexHeader} = Group,
    IndexHeader2 = IndexHeader#set_view_index_header{
        pending_transition = Transition
    },
    Group2 = Group#set_view_group{index_header = IndexHeader2},
    State#state{group = Group2}.


-spec is_any_partition_pending(#set_view_group_req{}, #set_view_group{}) -> boolean().
is_any_partition_pending(Req, Group) ->
    #set_view_group_req{wanted_partitions = WantedPartitions} = Req,
    case get_pending_transition(Group) of
    nil ->
        false;
    Trans ->
        #set_view_transition{
            active = ActivePending,
            passive = PassivePending
        } = Trans,
        (not ordsets:is_disjoint(WantedPartitions, ActivePending)) orelse
        (not ordsets:is_disjoint(WantedPartitions, PassivePending))
    end.


-spec process_view_group_request(#set_view_group_req{}, term(), #state{}) -> #state{}.
process_view_group_request(#set_view_group_req{stale = false} = Req, From, State) ->
    #state{
        group = Group,
        waiting_list = WaitList,
        replica_partitions = ReplicaParts
    } = State,
    #set_view_group_req{debug = Debug} = Req,
    CurSeqs = active_partition_seqs(State, true),
    Waiter = #waiter{from = From, debug = Debug, seqs = CurSeqs},
    case reply_with_group(Group, ReplicaParts, [Waiter]) of
    [] ->
        start_updater(State);
    _ ->
        start_updater(State#state{waiting_list = [Waiter | WaitList]})
    end;

process_view_group_request(#set_view_group_req{stale = ok} = Req, From, State) ->
    #state{
        group = Group,
        replica_partitions = ReplicaParts
    } = State,
    #set_view_group_req{debug = Debug} = Req,
    [] = reply_with_group(Group, ReplicaParts, [#waiter{from = From, debug = Debug}]),
    State;

process_view_group_request(#set_view_group_req{stale = update_after} = Req, From, State) ->
    #state{
        group = Group,
        replica_partitions = ReplicaParts
    } = State,
    #set_view_group_req{debug = Debug} = Req,
    [] = reply_with_group(Group, ReplicaParts, [#waiter{from = From, debug = Debug}]),
    case State#state.updater_pid of
    Pid when is_pid(Pid) ->
        State;
    nil ->
        start_updater(State)
    end.


-spec process_mark_as_unindexable(#state{},
                                  ordsets:ordset(partition_id())) -> #state{}.
process_mark_as_unindexable(#state{group = Group} = State, Partitions0) ->
    PendingTrans = ?set_pending_transition(Group),
    PendingActive = ?pending_transition_active(PendingTrans),
    PendingPassive = ?pending_transition_passive(PendingTrans),
    Partitions = lists:filter(
        fun(PartId) ->
            couch_set_view_util:has_part_seq(PartId, ?set_seqs(Group)) orelse
            lists:member(PartId, PendingActive) orelse
            lists:member(PartId, PendingPassive)
        end,
        Partitions0),
    do_process_mark_as_unindexable(State, Partitions).


-spec do_process_mark_as_unindexable(#state{},
                                     ordsets:ordset(partition_id())) -> #state{}.
do_process_mark_as_unindexable(State, []) ->
    State;
do_process_mark_as_unindexable(State0, Partitions) ->
    #state{
        group = #set_view_group{index_header = Header} = Group,
        replica_partitions = ReplicaParts
    } = State = stop_updater(State0),
    UpdaterWasRunning = is_pid(State0#state.updater_pid),
    ReplicasIntersection = [
        P || P <- Partitions, lists:member(P, ReplicaParts)
    ],
    case ReplicasIntersection of
    [] ->
        ok;
    _ ->
        ErrorMsg = io_lib:format("Intersection between requested unindexable list"
            " and current set of replica partitions: ~w", [ReplicasIntersection]),
        throw({error, iolist_to_binary(ErrorMsg)})
    end,

    PendingTrans = ?set_pending_transition(Group),
    PendingActive = ?pending_transition_active(PendingTrans),
    PendingPassive = ?pending_transition_passive(PendingTrans),
    PendingUnindexable = ?pending_transition_unindexable(PendingTrans),
    {Seqs2, UnindexableSeqs2, PendingUnindexable2} =
    lists:foldl(
        fun(PartId, {AccSeqs, AccUnSeqs, AccPendingUn}) ->
            PartMask = 1 bsl PartId,
            case (?set_abitmask(Group) band PartMask) == 0 andalso
                (?set_pbitmask(Group) band PartMask) == 0 andalso
                (not lists:member(PartId, PendingActive)) andalso
                (not lists:member(PartId, PendingPassive)) of
            true ->
                ErrorMsg2 = io_lib:format("Partition ~p is not in the active "
                    "nor passive state.", [PartId]),
                throw({error, iolist_to_binary(ErrorMsg2)});
            false ->
                ok
            end,
            case couch_set_view_util:has_part_seq(PartId, AccUnSeqs) of
            true ->
                {AccSeqs, AccUnSeqs, AccPendingUn};
            false ->
                case lists:member(PartId, PendingActive) orelse
                    lists:member(PartId, PendingPassive) of
                false ->
                    PartSeq = couch_set_view_util:get_part_seq(PartId, AccSeqs),
                    AccSeqs2 = orddict:erase(PartId, AccSeqs),
                    AccUnSeqs2 = orddict:store(PartId, PartSeq, AccUnSeqs),
                    {AccSeqs2, AccUnSeqs2, AccPendingUn};
                true ->
                    AccPendingUn2 = ordsets:add_element(PartId, AccPendingUn),
                    {AccSeqs, AccUnSeqs, AccPendingUn2}
                end
            end
        end,
        {?set_seqs(Group), ?set_unindexable_seqs(Group), PendingUnindexable},
        Partitions),

    PendingTrans2 = case PendingTrans of
    nil ->
        nil;
    _ ->
        PendingTrans#set_view_transition{unindexable = PendingUnindexable2}
    end,
    Group2 = Group#set_view_group{
        index_header = Header#set_view_index_header{
            seqs = Seqs2,
            unindexable_seqs = UnindexableSeqs2,
            pending_transition = PendingTrans2
        }
    },
    ?LOG_INFO("Set view `~s`, ~s group `~s`, unindexable partitions added.~n"
              "Previous set:         ~w~n"
              "New set:              ~w~n"
              "Previous pending set: ~w~n"
              "New pending set:      ~w~n",
              [?set_name(State), ?type(State), ?group_id(State),
               ?set_unindexable_seqs(Group), UnindexableSeqs2,
               PendingUnindexable, PendingUnindexable2]),
    NewState = State#state{group = Group2},
    NewState2 = stop_compactor(NewState),
    case UpdaterWasRunning of
    true ->
        start_updater(NewState2);
    false ->
        NewState2
    end.


-spec process_mark_as_indexable(#state{},
                                ordsets:ordset(partition_id()),
                                boolean()) -> #state{}.
process_mark_as_indexable(#state{group = Group} = State, Partitions0, CommitHeader) ->
    PendingTrans = ?set_pending_transition(Group),
    PendingUnindexable = ?pending_transition_unindexable(PendingTrans),
    Partitions = lists:filter(
        fun(PartId) ->
            couch_set_view_util:has_part_seq(PartId, ?set_unindexable_seqs(Group)) orelse
            lists:member(PartId, PendingUnindexable)
        end,
        Partitions0),
    do_process_mark_as_indexable(State, Partitions, CommitHeader).


-spec do_process_mark_as_indexable(#state{},
                                   ordsets:ordset(partition_id()),
                                   boolean()) -> #state{}.
do_process_mark_as_indexable(State, [], _LogTransition) ->
    State;
do_process_mark_as_indexable(State0, Partitions, LogTransition) ->
    #state{
        group = #set_view_group{index_header = Header} = Group,
        waiting_list = WaitList,
        update_listeners = Listeners
    } = State = stop_updater(State0),
    UpdaterWasRunning = is_pid(State0#state.updater_pid),
    PendingTrans = ?set_pending_transition(Group),
    PendingUnindexable = ?pending_transition_unindexable(PendingTrans),
    {Seqs2, UnindexableSeqs2, PendingUnindexable2} =
    lists:foldl(
        fun(PartId, {AccSeqs, AccUnSeqs, AccPendingUn}) ->
            case couch_set_view_util:has_part_seq(PartId, AccUnSeqs) of
            false ->
                case lists:member(PartId, AccPendingUn) of
                false ->
                    {AccSeqs, AccUnSeqs, AccPendingUn};
                true ->
                    {AccSeqs, AccUnSeqs, ordsets:del_element(PartId, AccPendingUn)}
                end;
            true ->
                Seq = couch_set_view_util:get_part_seq(PartId, AccUnSeqs),
                AccUnSeqs2 = orddict:erase(PartId, AccUnSeqs),
                AccSeqs2 = orddict:store(PartId, Seq, AccSeqs),
                {AccSeqs2, AccUnSeqs2, AccPendingUn}
            end
        end,
        {?set_seqs(Group), ?set_unindexable_seqs(Group), PendingUnindexable},
        Partitions),
    PendingTrans2 = case PendingTrans of
    nil ->
        nil;
    _ ->
        PendingTrans#set_view_transition{unindexable = PendingUnindexable2}
    end,
    Group2 = Group#set_view_group{
        index_header = Header#set_view_index_header{
            seqs = Seqs2,
            unindexable_seqs = UnindexableSeqs2,
            pending_transition = PendingTrans2
        }
    },
    case LogTransition of
    true ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, unindexable partitions removed.~n"
                  "Previous set:         ~w~n"
                  "New set:              ~w~n"
                  "Previous pending set: ~w~n"
                  "New pending set:      ~w~n",
                  [?set_name(State), ?type(State), ?group_id(State),
                   ?set_unindexable_seqs(Group), UnindexableSeqs2,
                   PendingUnindexable, PendingUnindexable2]);
    false ->
        ok
    end,
    NewState2 = stop_compactor(State#state{group = Group2}),
    case UpdaterWasRunning orelse (WaitList /= []) orelse (dict:size(Listeners) > 0) of
    true ->
        start_updater(NewState2);
    false ->
        NewState2
    end.


monitor_partitions(State, []) ->
    State;
monitor_partitions(State, _Partitions) when ?type(State) == replica ->
    State;
monitor_partitions(#state{db_refs = DbRefs} = State, Partitions) ->
    DbRefs2 = monitor_partitions(Partitions, ?set_name(State), DbRefs),
    State#state{db_refs = DbRefs2}.

monitor_partitions([], _SetName, Dict) ->
    Dict;
monitor_partitions([PartId | Rest], SetName, Dict) ->
    case dict:is_key(PartId, Dict) of
    true ->
        monitor_partitions(Rest, SetName, Dict);
    false ->
        DbName = case PartId of
        master ->
            ?master_dbname(SetName);
        _ when is_integer(PartId) ->
            ?dbname(SetName, PartId)
        end,
        case couch_db:open_int(DbName, []) of
        {ok, Db} ->
            Ref = couch_db:monitor(Db),
            ok = couch_db:close(Db),
            monitor_partitions(Rest, SetName, dict:store(PartId, Ref, Dict));
        Error ->
            throw({error, {db_open, DbName, Error}})
        end
    end.


demonitor_partitions(State, []) ->
    State;
demonitor_partitions(State, _Partitions) when ?type(State) == replica ->
    State;
demonitor_partitions(#state{db_refs = DbRefs} = State, Partitions) ->
    DbRefs2 = demonitor_partitions(Partitions, ?set_name(State), DbRefs),
    State#state{db_refs = DbRefs2}.

demonitor_partitions([], _SetName, Dict) ->
    Dict;
demonitor_partitions([PartId | Rest], SetName, Dict) ->
    case dict:find(PartId, Dict) of
    error ->
        demonitor_partitions(Rest, SetName, Dict);
    {ok, Ref} ->
        erlang:demonitor(Ref, [flush]),
        demonitor_partitions(Rest, SetName, dict:erase(PartId, Dict))
    end.


updater_tmp_dir(#state{group = Group} = State) ->
    #set_view_group{sig = Sig, type = Type} = Group,
    Base = couch_set_view:set_index_dir(?root_dir(State), ?set_name(State)),
    filename:join(
        [Base, "tmp_" ++ couch_util:to_hex(Sig) ++ "_" ++ atom_to_list(Type)]).


is_unindexable_part(PartId, Group) ->
    PendingTrans = ?set_pending_transition(Group),
    PendingUnindexable = ?pending_transition_unindexable(PendingTrans),
    couch_set_view_util:has_part_seq(PartId, ?set_unindexable_seqs(Group)) orelse
        lists:member(PartId, PendingUnindexable).


process_monitor_partition_update(#state{group = Group} = State, PartId, Ref, Pid) ->
    PendingTrans = ?set_pending_transition(Group),
    ActivePending = ?pending_transition_active(PendingTrans),
    PassivePending = ?pending_transition_passive(PendingTrans),
    IsPending = lists:member(PartId, ActivePending) orelse
        lists:member(PartId, PassivePending),
    Mask = 1 bsl PartId,
    IsDefined = (Mask band (?set_abitmask(Group) bor ?set_pbitmask(Group))) == Mask,
    case (not IsDefined) andalso (not IsPending) of
    true ->
        Msg = io_lib:format("Partition ~p not in active nor passive set", [PartId]),
        throw({error, iolist_to_binary(Msg)});
    false ->
        ok
    end,
    case IsPending of
    true ->
        State2 = monitor_partitions(State, [PartId]),
        Db = couch_set_view_util:open_db(?set_name(State2), PartId),
        ok = couch_db:close(Db),
        CurSeq = Db#db.update_seq,
        Seq = 0;
    false ->
        {ok, [{PartId, CurSeq}]} = couch_db_set:get_seqs(?db_set(State), [PartId], true),
        case couch_set_view_util:find_part_seq(PartId, ?set_seqs(Group)) of
        not_found ->
            Seq = couch_set_view_util:get_part_seq(PartId, ?set_unindexable_seqs(Group));
        {ok, Seq} ->
            ok
        end,
        State2 = State
    end,
    case CurSeq > Seq of
    true ->
        Listener = #up_listener{
            pid = Pid,
            monref = erlang:monitor(process, Pid),
            partition = PartId,
            seq = CurSeq
        },
        ?LOG_INFO("Set view `~s`, ~s group `~s`, blocking partition ~p update monitor,"
                   " reference ~p, desired indexed seq ~p, indexed seq ~p",
                   [?set_name(State), ?type(State), ?group_id(State),
                    PartId, Ref, CurSeq, Seq]),
        State2#state{
            update_listeners = dict:store(Ref, Listener, State2#state.update_listeners)
        };
    false ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, replying to partition ~p update monitor,"
                   " reference ~p, desired indexed seq ~p, indexed seq ~p",
                   [?set_name(State), ?type(State), ?group_id(State),
                    PartId, Ref, CurSeq, Seq]),
        Pid ! {Ref, updated},
        State2
    end.


-spec update_clean_group_seqs(#set_view_group{}, #set_view_group{}) -> #set_view_group{}.
update_clean_group_seqs(OldGroup, CleanGroup) ->
    % When toggling partitions between the indexable and unindexable states,
    % we don't restart the cleanup process. Therefore, when it finishes, set
    % the correct values for seqs (indexable) and unindexable_seqs. Whenever
    % the updater is started, the cleanup process is stopped, so all seqs are
    % correct always.
    CleanHeader = CleanGroup#set_view_group.index_header,
    NewCleanHeader = CleanHeader#set_view_index_header{
        seqs = ?set_seqs(OldGroup),
        unindexable_seqs = ?set_unindexable_seqs(OldGroup)
    },
    CleanGroup#set_view_group{index_header = NewCleanHeader}.


get_updater_start_time() ->
    erlang:get(updater_start_ts).


reset_updater_start_time() ->
    Now = os:timestamp(),
    set_last_updater_checkpoint_ts(Now),
    erlang:put(updater_start_ts, Now).


get_last_updater_checkpoint_ts() ->
    erlang:get(last_updater_checkpoint_ts).


set_last_updater_checkpoint_ts() ->
    set_last_updater_checkpoint_ts(os:timestamp()).


set_last_updater_checkpoint_ts(Ts) ->
    erlang:put(last_updater_checkpoint_ts, Ts).


updater_lost_time() ->
    Now = os:timestamp(),
    timer:now_diff(Now, get_last_updater_checkpoint_ts()) / 1000000.


updater_indexing_time() ->
    StartTs = get_updater_start_time(),
    LastCpTs = get_last_updater_checkpoint_ts(),
    timer:now_diff(LastCpTs, StartTs) / 1000000.


inc_util_stat(StatPos, Inc) ->
    Stats = erlang:get(util_stats),
    Stats2 = setelement(StatPos, Stats, element(StatPos, Stats) + Inc),
    erlang:put(util_stats, Stats2).


reset_util_stats() ->
    erlang:put(util_stats, #util_stats{}).


-spec updater_needs_restart(#set_view_group{}, bitmask(),
                            bitmask(), bitmask()) -> boolean().
updater_needs_restart(Group, _, _, _) when ?set_replicas_on_transfer(Group) /= [] ->
    true;
updater_needs_restart(Group, _, _, _) when ?set_pending_transition(Group) /= nil ->
    true;
updater_needs_restart(Group, ActiveMask, PassiveMask, CleanupMask) ->
    BeforeIndexable = ?set_abitmask(Group) bor ?set_pbitmask(Group),
    AfterActive = (?set_abitmask(Group) bor ActiveMask) band (bnot CleanupMask),
    case AfterActive == ?set_abitmask(Group) of
    true ->
        NewCleanup = CleanupMask band BeforeIndexable,
        AfterCleanup = ?set_cbitmask(Group) bor NewCleanup,
        % If this state transition only adds new passive partitions, don't restart
        % the updater. Send the updater the new set of passive partitions, and
        % hopefully it will still be on time to index them, otherwise it will see
        % them the next time it's started.
        AfterCleanup =/= ?set_cbitmask(Group);
    false ->
        AfterIndexable = (BeforeIndexable bor ActiveMask bor PassiveMask) band (bnot CleanupMask),
        % Don't restart updater when a state change request only transitions
        % partitions from passive to active state (or vice-versa). This speeds
        % up rebalance with consistent views enabled.
        AfterIndexable =/= BeforeIndexable
    end.


-spec missing_partitions(#set_view_group{}, #set_view_group{}) ->
                                        ordsets:ordset(partition_id()).
missing_partitions(UpdaterGroup, OurGroup) ->
    MissingMask = (?set_pbitmask(OurGroup) bor ?set_abitmask(OurGroup)) bxor
        (?set_pbitmask(UpdaterGroup) bor ?set_abitmask(UpdaterGroup)),
    couch_set_view_util:decode_bitmask(MissingMask).


-spec fix_updater_group(#set_view_group{}, #set_view_group{}) ->
                               #set_view_group{}.
fix_updater_group(UpdaterGroup, OurGroup) ->
    % Confront with logic in ?MODULE:updater_needs_restart/4.
    Missing = missing_partitions(UpdaterGroup, OurGroup),
    UpdaterHeader = UpdaterGroup#set_view_group.index_header,
    Seqs2 = lists:foldl(
        fun(PartId, Acc) ->
            case couch_set_view_util:has_part_seq(PartId, Acc) of
            true ->
                Acc;
            false ->
                ordsets:add_element({PartId, 0}, Acc)
            end
        end,
        ?set_seqs(UpdaterGroup), Missing),
    UpdaterGroup#set_view_group{
        index_header = UpdaterHeader#set_view_index_header{
            abitmask = ?set_abitmask(OurGroup),
            pbitmask = ?set_pbitmask(OurGroup),
            seqs = Seqs2
        }
    }.
