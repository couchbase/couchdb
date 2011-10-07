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
-export([set_passive_partitions/2, activate_partitions/2, cleanup_partitions/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("couch_db.hrl").
-include("couch_set_view.hrl").

-define(CLEANUP_TIMEOUT, 3000).
-define(set_name(State), element(2, State#group_state.init_args)).
-define(group_id(State), (State#group_state.group)#set_view_group.name).
-define(db_set(State), (State#group_state.group)#set_view_group.db_set).

-record(group_state, {
    init_args,
    group,
    updater_pid = nil,
    compactor_pid = nil,
    compactor_fun = nil,
    waiting_commit = false,
    waiting_list = [],
    cleaner_pid = nil
}).

% api methods
request_group(Pid, StaleType) ->
    case gen_server:call(Pid, {request_group, StaleType}, infinity) of
    {ok, #set_view_group{ref_counter = RefCounter} = Group} ->
        couch_ref_counter:add(RefCounter),
        {ok, Group};
    Error ->
        Error
    end.

release_group(#set_view_group{ref_counter = RefCounter}) ->
    couch_ref_counter:drop(RefCounter).

request_group_info(Pid) ->
    case gen_server:call(Pid, request_group_info) of
    {ok, GroupInfoList} ->
        {ok, GroupInfoList};
    Error ->
        throw(Error)
    end.

define_view(Pid, Params) ->
    #set_view_params{
        max_partitions = NumPartitions,
        active_partitions = ActivePartitionsList,
        passive_partitions = PassivePartitionsList
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
    ok = gen_server:call(
        Pid, {define_view, NumPartitions, ActiveList, ActiveBitmask,
            PassiveList, PassiveBitmask}, infinity).

is_view_defined(Pid) ->
    gen_server:call(Pid, is_view_defined, infinity).


set_passive_partitions(Pid, Partitions) ->
    gen_server:call(Pid, {set_passive_partitions, Partitions}, infinity).


activate_partitions(Pid, Partitions) ->
    gen_server:call(Pid, {activate_partitions, Partitions}, infinity).


cleanup_partitions(Pid, Partitions) ->
    gen_server:call(Pid, {cleanup_partitions, Partitions}, infinity).


% from template
start_link(InitArgs) ->
    case gen_server:start_link(?MODULE,
            {InitArgs, self(), Ref = make_ref()}, []) of
    {ok, Pid} ->
        {ok, Pid};
    ignore ->
        receive
        {Ref, Pid, Error} ->
            case process_info(self(), trap_exit) of
            {trap_exit, true} -> receive {'EXIT', Pid, _} -> ok end;
            {trap_exit, false} -> ok
            end,
            Error
        end;
    Error ->
        Error
    end.

% init creates a closure which spawns the appropriate view_updater.
init({{_, SetName, _} = InitArgs, ReturnPid, Ref}) ->
    process_flag(trap_exit, true),
    case prepare_group(InitArgs, false) of
    {ok, #set_view_group{fd = Fd, index_header = Header} = Group} ->
        case Header#set_view_index_header.num_partitions of
        nil ->
            DbSet = nil,
            ?LOG_INFO("Started undefined set view group `~s`, group `~s`",
                      [SetName, Group#set_view_group.name]);
        NumPartitions when is_integer(NumPartitions) ->
            {ActiveList, PassiveList} = make_partition_lists(Group),
            {ok, DbSet} = couch_db_set:open(SetName, ActiveList, PassiveList, []),
            ?LOG_INFO("Started set view group `~s`, group `~s`~n"
                      "abitmask ~*..0s~n"
                      "pbitmask ~*..0s~n"
                      "cbitmask ~*..0s~n",
                      [SetName, Group#set_view_group.name,
                       NumPartitions, integer_to_list(Header#set_view_index_header.abitmask, 2),
                       NumPartitions, integer_to_list(Header#set_view_index_header.pbitmask, 2),
                       NumPartitions, integer_to_list(Header#set_view_index_header.cbitmask, 2)])
        end,
        {ok, RefCounter} = couch_ref_counter:start([Fd]),
        {ok, #group_state{
            init_args = InitArgs,
            group = Group#set_view_group{
                ref_counter = RefCounter, db_set = DbSet
            }
        }, ?CLEANUP_TIMEOUT};
    Error ->
        ReturnPid ! {Ref, self(), Error},
        ignore
    end.

handle_call({define_view, NumPartitions, ActiveList, ActiveBitmask,
    PassiveList, PassiveBitmask}, _From,
    #group_state{
        group = #set_view_group{
            index_header = #set_view_index_header{num_partitions = nil}
        }} = State) ->
    Seqs = lists:map(
        fun(PartId) -> {PartId, 0} end, lists:usort(ActiveList ++ PassiveList)),
    #group_state{group = Group2} = State2 = stop_cleaner(State),
    #set_view_group{
        name = DDocId,
        index_header = Header,
        views = Views
    } = Group2,
    NewHeader = Header#set_view_index_header{
        num_partitions = NumPartitions,
        abitmask = ActiveBitmask,
        pbitmask = PassiveBitmask,
        seqs = Seqs,
        purge_seqs = Seqs
    },
    case is_pid(Group2#set_view_group.db_set) of
    false ->
        {ok, DbSet} = couch_db_set:open(?set_name(State2), ActiveList, PassiveList, []);
    true ->
        DbSet = Group2#set_view_group.db_set,
        ok = couch_db_set:set_active(DbSet, ActiveList),
        ok = couch_db_set:set_passive(DbSet, PassiveList)
    end,
    NewGroup = Group2#set_view_group{
        db_set = DbSet,
        index_header = NewHeader,
        views = lists:map(
            fun(V) -> V#set_view{update_seqs = Seqs, purge_seqs = Seqs} end, Views)
    },
    ok = commit_header(NewGroup),
    NewState = State2#group_state{group = NewGroup},
    ?LOG_INFO("Set view `~s`, group `~s`, configured with:~n"
              "~p partitions~n"
              "initial active partitions ~w~n"
              "initial passive partitions ~w",
              [?set_name(State), DDocId, NumPartitions, ActiveList, PassiveList]),
    {reply, ok, NewState, ?CLEANUP_TIMEOUT};

handle_call(is_view_defined, _From, #group_state{group = Group} = State) ->
    #set_view_group{
        index_header = #set_view_index_header{num_partitions = NumParts}
    } = Group,
    {reply, is_integer(NumParts), State, ?CLEANUP_TIMEOUT};

handle_call(_Msg, _From, #group_state{
        group = #set_view_group{
            index_header = #set_view_index_header{num_partitions = nil}
        }} = State) ->
    {reply, view_undefined, State};

handle_call({cleanup_partitions, Partitions}, _From, State) ->
    #group_state{group = Group2} = State2 = stop_cleaner(State),
    #set_view_group{
        index_header = #set_view_index_header{
            abitmask = Abitmask, pbitmask = Pbitmask, cbitmask = Cbitmask
        } = Header
    } = Group2,
    % TODO abort compaction and restart it
    case validate_partitions(Header, Partitions) of
    ok ->
        {ok, NewAbitmask, NewPbitmask, NewCbitmask, NewSeqs, NewPurgeSeqs} =
            set_cleanup_partitions(
                Partitions, Abitmask, Pbitmask, Cbitmask,
                ?set_seqs(Group2), ?set_purge_seqs(Group2)),
        State3 = update_header(
             cleanup_partitions, Partitions, State2,
             NewAbitmask, NewPbitmask, NewCbitmask, NewSeqs, NewPurgeSeqs),
        ok = couch_db_set:remove_partitions(?db_set(State3), Partitions),
        NewState = case is_pid(State3#group_state.compactor_pid) of
        true ->
            ?LOG_INFO("Restarting compaction for group `~s`, set view `~s`, "
                      "because the cleanup bitmask was updated.",
                      [?group_id(State3), ?set_name(State3)]),
            unlink(State3#group_state.compactor_pid),
            exit(State3#group_state.compactor_pid, kill),
            start_compactor(State3, State3#group_state.compactor_fun);
        false ->
            State3
        end,
        {reply, ok, maybe_start_cleaner(NewState), ?CLEANUP_TIMEOUT};
    Error ->
        {reply, Error, State2, ?CLEANUP_TIMEOUT}
    end;

handle_call({set_passive_partitions, Partitions}, _From, State) ->
    #group_state{group = Group2} = State2 = stop_cleaner(State),
    #set_view_group{
        index_header = #set_view_index_header{
            abitmask = Abitmask, pbitmask = Pbitmask, cbitmask = Cbitmask
        } = Header
    } = Group2,
    case validate_partitions(Header, Partitions) of
    ok ->
        {ok, NewAbitmask, NewPbitmask, NewSeqs, NewPurgeSeqs} =
            set_passive_partitions(
                Partitions, Abitmask, Pbitmask,
                ?set_seqs(Group2), ?set_purge_seqs(Group2)),
        NewState = update_header(
             set_passive_partitions, Partitions, State2,
             NewAbitmask, NewPbitmask, Cbitmask, NewSeqs, NewPurgeSeqs),
        {reply, ok, NewState, ?CLEANUP_TIMEOUT};
    Error ->
        {reply, Error, State2, ?CLEANUP_TIMEOUT}
    end;

handle_call({activate_partitions, Partitions}, _From, State) ->
    #group_state{group = Group2} = State2 = stop_cleaner(State),
    #set_view_group{
        index_header = #set_view_index_header{
            abitmask = Abitmask, pbitmask = Pbitmask, cbitmask = Cbitmask
        } = Header
    } = Group2,
    case validate_partitions(Header, Partitions) of
    ok ->
        {ok, NewAbitmask, NewPbitmask, NewSeqs, NewPurgeSeqs} =
            set_active_partitions(
                Partitions, Abitmask, Pbitmask,
                ?set_seqs(Group2), ?set_purge_seqs(Group2)),
        NewState = update_header(
             activate_partitions, Partitions, State2,
             NewAbitmask, NewPbitmask, Cbitmask, NewSeqs, NewPurgeSeqs),
        {reply, ok, NewState, ?CLEANUP_TIMEOUT};
    Error ->
        {reply, Error, State2, ?CLEANUP_TIMEOUT}
    end;


% {request_group, StaleType}
handle_call({request_group, false}, From,
        #group_state{
            updater_pid = UpPid,
            waiting_list = WaitList
        } = State) ->
    case UpPid of
    nil ->
        case index_needs_update(State) of
        {true, NewSeqs} ->
            #group_state{group = Group} = State2 = stop_cleaner(State),
            Owner = self(),
            Pid = spawn_link(fun() ->
                couch_set_view_updater:update(Owner, Group, ?set_name(State2), NewSeqs)
            end),
            {noreply, State2#group_state{
                updater_pid = Pid,
                waiting_list = [From | WaitList]}};
        {false, _} ->
            {reply, {ok, State#group_state.group}, State}
        end;
    _ when is_pid(UpPid) ->
        {noreply, State#group_state{waiting_list = [From | WaitList]}, ?CLEANUP_TIMEOUT}
    end;

handle_call({request_group, ok}, _From, #group_state{group = Group} = State) ->
    {reply, {ok, Group}, State, ?CLEANUP_TIMEOUT};

handle_call({request_group, update_after}, From, #group_state{group = Group} = State) ->
    gen_server:reply(From, {ok, Group}),
    case State#group_state.updater_pid of
    Pid when is_pid(Pid) ->
        {noreply, State};
    nil ->
        case index_needs_update(State) of
        {true, NewSeqs} ->
            #group_state{group = Group2} = State2 = stop_cleaner(State),
            Owner = self(),
            UpPid = spawn_link(fun() ->
                couch_set_view_updater:update(Owner, Group2, ?set_name(State2), NewSeqs)
            end),
            {noreply, State2#group_state{updater_pid = UpPid}};
        {false, _} ->
            {noreply, State}
        end
    end;

handle_call(request_group_info, _From, State) ->
    GroupInfo = get_group_info(State),
    {reply, {ok, GroupInfo}, State, ?CLEANUP_TIMEOUT};

handle_call({start_compact, CompactFun}, _From, #group_state{compactor_pid = nil} = State) ->
    #group_state{compactor_pid = Pid} = State2 = start_compactor(State, CompactFun),
    {reply, {ok, Pid}, State2};
handle_call({start_compact, _}, _From, State) ->
    %% compact already running, this is a no-op
    {reply, {ok, State#group_state.compactor_pid}, State};

handle_call({compact_done, NewGroup}, _From, State) ->
    #group_state{
        group = Group,
        init_args = {RootDir, _, _},
        updater_pid = UpdaterPid,
        compactor_pid = CompactorPid
    } = State,
    #set_view_group{
        fd = OldFd, sig = GroupSig, ref_counter = RefCounter
    } = Group,

    case accept_compact_group(NewGroup, Group) of
    false ->
        ?LOG_INFO("Recompacting group `~s`, set view `~s`, because the "
                  "cleanup bitmask was updated.",
                  [?group_id(State), ?set_name(State)]),
        {reply, {restart, Group, compact_group(State)}, State};
    true ->
        case group_up_to_date(NewGroup, State#group_state.group) of
        true ->
            ?LOG_INFO("Set view `~s`, group `~s`, compaction complete",
                      [?set_name(State), ?group_id(State)]),
            FileName = index_file_name(RootDir, ?set_name(State), GroupSig),
            CompactName = index_file_name(compact, RootDir, ?set_name(State), GroupSig),
            ok = couch_file:delete(RootDir, FileName),
            ok = file:rename(CompactName, FileName),

            %% if an updater is running, kill it and start a new one
            NewUpdaterPid =
            if is_pid(UpdaterPid) ->
                unlink(UpdaterPid),
                exit(UpdaterPid, view_compaction_complete),
                Owner = self(),
                {true, NewSeqs} = index_needs_update(State),
                spawn_link(fun() ->
                    couch_set_view_updater:update(Owner, NewGroup, ?set_name(State), NewSeqs)
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

            self() ! delayed_commit,
            State2 = State#group_state{
                compactor_pid = nil,
                compactor_fun = nil,
                updater_pid = NewUpdaterPid,
                group = NewGroup#set_view_group{
                    index_header = get_index_header_data(NewGroup),
                    ref_counter = NewRefCounter
                }
            },
            commit_header(State2#group_state.group),
            State3 = maybe_start_cleaner(State2),
            {reply, ok, State3, ?CLEANUP_TIMEOUT};
        false ->
            ?LOG_INFO("Set view `~s`, group `~s`, compaction still behind, retrying",
                      [?set_name(State), ?group_id(State)]),
            {reply, update, State}
        end
    end;

handle_call(cancel_compact, _From, #group_state{compactor_pid = nil} = State) ->
    {reply, ok, State, ?CLEANUP_TIMEOUT};
handle_call(cancel_compact, _From, #group_state{compactor_pid = Pid} = State) ->
    unlink(Pid),
    exit(Pid, kill),
    #group_state{
        group = #set_view_group{sig = GroupSig},
        init_args = {RootDir, _, _}
    } = State,
    CompactFile = index_file_name(compact, RootDir, ?set_name(State), GroupSig),
    ok = couch_file:delete(RootDir, CompactFile),
    State2 = maybe_start_cleaner(State#group_state{compactor_pid = nil}),
    {reply, ok, State2, ?CLEANUP_TIMEOUT}.


handle_cast({partial_update, Pid, NewGroup}, #group_state{updater_pid=Pid} = State) ->
    #group_state{
        waiting_commit = WaitingCommit
    } = State,
    ?LOG_INFO("Checkpointing set view `~s` update for group `~s`",
              [?set_name(State), NewGroup#set_view_group.name]),
    if not WaitingCommit ->
        erlang:send_after(1000, self(), delayed_commit);
    true -> ok
    end,
    {noreply, State#group_state{group = NewGroup, waiting_commit = true}};
handle_cast({partial_update, _, _}, State) ->
    %% message from an old (probably pre-compaction) updater; ignore
    {noreply, State, ?CLEANUP_TIMEOUT}.


handle_info(timeout, State) ->
    NewState = maybe_start_cleaner(State),
    {noreply, NewState};

handle_info(delayed_commit, #group_state{group = Group} = State) ->
    commit_header(Group),
    {noreply, State#group_state{waiting_commit = false}, ?CLEANUP_TIMEOUT};

handle_info({'EXIT', Pid, {clean_group, Group}}, #group_state{cleaner_pid = Pid} = State) ->
    #set_view_group{
        index_header = #set_view_index_header{
            cbitmask = Cbitmask,
            abitmask = Abitmask,
            pbitmask = Pbitmask,
            num_partitions = NumPartitions
        }
    } = Group,
    #set_view_group{
        index_header = #set_view_index_header{
            cbitmask = OldCbitmask,
            abitmask = OldAbitmask,
            pbitmask = OldPbitmask,
            num_partitions = NumPartitions
        }
    } = State#group_state.group,
    ?LOG_INFO("Cleanup finished for set view `~s`, group `~s`~n"
              "New abitmask ~*..0s, old abitmask ~*..0s~n"
              "New pbitmask ~*..0s, old pbitmask ~*..0s~n"
              "New cbitmask ~*..0s, old cbitmask ~*..0s~n",
              [?set_name(State), ?group_id(State),
               NumPartitions, integer_to_list(Abitmask, 2),
               NumPartitions, integer_to_list(OldAbitmask, 2),
               NumPartitions, integer_to_list(Pbitmask, 2),
               NumPartitions, integer_to_list(OldPbitmask, 2),
               NumPartitions, integer_to_list(Cbitmask, 2),
               NumPartitions, integer_to_list(OldCbitmask, 2)]),
    {noreply, State#group_state{cleaner_pid = nil, group = Group}};

handle_info({'EXIT', Pid, Reason}, #group_state{cleaner_pid = Pid} = State) ->
    {stop, {cleaner_died, Reason}, State};

handle_info({'EXIT', Pid, shutdown},
    #group_state{group = #set_view_group{db_set = Pid}} = State) ->
    ?LOG_INFO("Set view `~s`, group `~s`, terminating because database set "
              "was shutdown", [?set_name(State), ?group_id(State)]),
    {stop, normal, State};

handle_info({'EXIT', UpPid, {new_group, Group}},
        #group_state{
            updater_pid = UpPid,
            waiting_list = WaitList,
            waiting_commit = WaitingCommit} = State) ->
    if not WaitingCommit ->
        erlang:send_after(1000, self(), delayed_commit);
    true -> ok
    end,
    reply_with_group(Group, WaitList),
    State2 = State#group_state{
        updater_pid = nil,
        waiting_commit = true,
        waiting_list = [],
        group = Group
    },
    State3 = maybe_start_cleaner(State2),
    {noreply, State3, ?CLEANUP_TIMEOUT};

handle_info({'EXIT', _, {new_group, _}}, State) ->
    %% message from an old (probably pre-compaction) updater; ignore
    {noreply, State, ?CLEANUP_TIMEOUT};

handle_info({'EXIT', UpPid, reset}, #group_state{updater_pid = UpPid} = State) ->
    State2 = stop_cleaner(State),
    case prepare_group(State#group_state.init_args, true) of
    {ok, ResetGroup} ->
        Owner = self(),
        State3 = State2#group_state{group = ResetGroup},
        {true, NewSeqs} = index_needs_update(State3),
        Pid = spawn_link(fun() ->
            couch_set_view_updater:update(Owner, ResetGroup, ?set_name(State), NewSeqs)
        end),
        {ok, State3#group_state{updater_pid = Pid}};
    Error ->
        {stop, normal, reply_all(State2, Error), ?CLEANUP_TIMEOUT}
    end;
handle_info({'EXIT', _, reset}, State) ->
    %% message from an old (probably pre-compaction) updater; ignore
    {noreply, State, ?CLEANUP_TIMEOUT};
    
handle_info({'EXIT', _FromPid, normal}, State) ->
    {noreply, State, ?CLEANUP_TIMEOUT};

handle_info({'EXIT', FromPid, {{nocatch, Reason}, _Trace}}, State) ->
    State2 = stop_cleaner(State),
    ?LOG_DEBUG("Uncaught throw() in linked pid: ~p", [{FromPid, Reason}]),
    {stop, Reason, State2};

handle_info({'EXIT', FromPid, Reason}, State) ->
    ?LOG_ERROR("Exit from linked pid: ~p, State: ~p~n", [{FromPid, Reason}, State]),
    {stop, Reason, State}.


terminate(Reason, #group_state{updater_pid=Update, compactor_pid=Compact}=S) ->
    State2 = stop_cleaner(S),
    reply_all(State2, Reason),
    case is_process_alive(?db_set(S)) of
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

reply_with_group(Group, WaitList) ->
    lists:foreach(fun(From) -> gen_server:reply(From, {ok, Group}) end, WaitList).

reply_all(#group_state{waiting_list=WaitList}=State, Reply) ->
    [catch gen_server:reply(From, Reply) || From <- WaitList],
    State#group_state{waiting_list=[]}.

prepare_group({RootDir, SetName, #set_view_group{sig = Sig} = Group}, ForceReset)->
    case open_index_file(RootDir, SetName, Sig) of
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
        catch delete_index_file(RootDir, SetName, Sig),
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

index_file_name(RootDir, SetName, GroupSig) ->
    design_root(RootDir, SetName) ++ hex_sig(GroupSig) ++".view".

index_file_name(compact, RootDir, SetName, GroupSig) ->
    design_root(RootDir, SetName) ++ hex_sig(GroupSig) ++".compact.view".


open_index_file(RootDir, SetName, GroupSig) ->
    FileName = index_file_name(RootDir, SetName, GroupSig),
    case couch_file:open(FileName) of
    {ok, Fd}        -> {ok, Fd};
    {error, enoent} -> couch_file:open(FileName, [create]);
    Error           -> Error
    end.

open_index_file(compact, RootDir, SetName, GroupSig) ->
    FileName = index_file_name(compact, RootDir, SetName, GroupSig),
    case couch_file:open(FileName) of
    {ok, Fd}        -> {ok, Fd};
    {error, enoent} -> couch_file:open(FileName, [create]);
    Error           -> Error
    end.

set_view_sig(#set_view_group{
            views=Views,
            lib={[]},
            def_lang=Language,
            design_options=DesignOptions}=G) ->
    ViewInfo = [old_view_format(V) || V <- Views],
    G#set_view_group{sig=couch_util:md5(term_to_binary({ViewInfo, Language, DesignOptions}))};
set_view_sig(#set_view_group{
            views=Views,
            lib=Lib,
            def_lang=Language,
            design_options=DesignOptions}=G) ->
    ViewInfo = [old_view_format(V) || V <- Views],
    G#set_view_group{sig=couch_util:md5(term_to_binary({ViewInfo, Language, DesignOptions, sort_lib(Lib)}))}.

% Use the old view record format so group sig's don't change
old_view_format(View) ->
    {
        view,
        View#set_view.id_num,
        View#set_view.map_names,
        View#set_view.def,
        View#set_view.btree,
        View#set_view.reduce_funs,
        View#set_view.options
    }.

sort_lib({Lib}) ->
    sort_lib(Lib, []).
sort_lib([], LAcc) ->
    lists:keysort(1, LAcc);
sort_lib([{LName, {LObj}}|Rest], LAcc) ->
    LSorted = sort_lib(LObj, []), % descend into nested object
    sort_lib(Rest, [{LName, LSorted}|LAcc]);
sort_lib([{LName, LCode}|Rest], LAcc) ->
    sort_lib(Rest, [{LName, LCode}|LAcc]).

open_set_group(SetName, GroupId) ->
    case couch_db:open_int(?master_dbname(SetName), []) of
    {ok, Db} ->
        case couch_db:open_doc(Db, GroupId, [ejson_body]) of
        {ok, Doc} ->
            couch_db:close(Db),
            {ok, design_doc_to_set_view_group(SetName, Doc)};
        Else ->
            couch_db:close(Db),
            Else
        end;
    Else ->
        Else
    end.

get_group_info(State) ->
    #group_state{
        group=Group,
        updater_pid=UpdaterPid,
        compactor_pid=CompactorPid,
        waiting_commit=WaitingCommit,
        waiting_list=WaitersList
    } = State,
    #set_view_group{
        fd = Fd,
        sig = GroupSig,
        id_btree = Btree,
        def_lang = Lang,
        views = Views
    } = Group,
    {ok, Size} = couch_file:bytes(Fd),
    [
        {signature, ?l2b(hex_sig(GroupSig))},
        {language, Lang},
        {disk_size, Size},
        {data_size, view_group_data_size(Btree, Views)},
        {updater_running, UpdaterPid /= nil},
        {compact_running, CompactorPid /= nil},
        {waiting_commit, WaitingCommit},
        {waiting_clients, length(WaitersList)},
        {update_seqs, ?set_seqs(Group)},
        {purge_seqs, ?set_purge_seqs(Group)}
    ].

view_group_data_size(MainBtree, Views) ->
    lists:foldl(
        fun(#set_view{btree = Btree}, Acc) ->
            sum_btree_sizes(Acc, couch_btree:size(Btree))
        end,
        couch_btree:size(MainBtree),
        Views).

sum_btree_sizes(nil, _) ->
    null;
sum_btree_sizes(_, nil) ->
    null;
sum_btree_sizes(Size1, Size2) ->
    Size1 + Size2.

% maybe move to another module
design_doc_to_set_view_group(SetName, #doc{id=Id,body={Fields}}) ->
    Language = couch_util:get_value(<<"language">>, Fields, <<"javascript">>),
    {DesignOptions} = couch_util:get_value(<<"options">>, Fields, {[]}),
    {RawViews} = couch_util:get_value(<<"views">>, Fields, {[]}),
    Lib = couch_util:get_value(<<"lib">>, RawViews, {[]}),
    % add the views to a dictionary object, with the map source as the key
    DictBySrc =
    lists:foldl(
        fun({Name, {MRFuns}}, DictBySrcAcc) ->
            case couch_util:get_value(<<"map">>, MRFuns) of
            undefined -> DictBySrcAcc;
            MapSrc ->
                RedSrc = couch_util:get_value(<<"reduce">>, MRFuns, null),
                {ViewOptions} = couch_util:get_value(<<"options">>, MRFuns, {[]}),
                View =
                case dict:find({MapSrc, ViewOptions}, DictBySrcAcc) of
                    {ok, View0} -> View0;
                    error -> #set_view{def=MapSrc, options=ViewOptions} % create new view object
                end,
                View2 =
                if RedSrc == null ->
                    View#set_view{map_names=[Name|View#set_view.map_names]};
                true ->
                    View#set_view{reduce_funs=[{Name,RedSrc}|View#set_view.reduce_funs]}
                end,
                dict:store({MapSrc, ViewOptions}, View2, DictBySrcAcc)
            end
        end, dict:new(), RawViews),
    % number the views
    {Views, _N} = lists:mapfoldl(
        fun({_Src, View}, N) ->
            {View#set_view{id_num=N},N+1}
        end, 0, lists:sort(dict:to_list(DictBySrc))),
    SetViewGroup = #set_view_group{
        set_name = SetName,
        name = Id,
        lib = Lib,
        views = Views,
        def_lang = Language,
        design_options = DesignOptions
    },
    set_view_sig(SetViewGroup).

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

delete_index_file(RootDir, SetName, GroupSig) ->
    couch_file:delete(RootDir, index_file_name(RootDir, SetName, GroupSig)).

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
    Compression = couch_compress:get_compression_method(),
    IdTreeReduce = fun(reduce, KVs) ->
        couch_set_view_util:partitions_map(KVs, 0);
    (rereduce, [First | Rest]) ->
        lists:foldl(fun(M, A) -> M bor A end, First, Rest)
    end,
    {ok, IdBtree} = couch_btree:open(
        IdBtreeState, Fd, [{compression, Compression}, {reduce, IdTreeReduce}]),
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
            {ok, Btree} = couch_btree:open(BTState, Fd,
                    [{less, Less}, {reduce, ReduceFun},
                        {compression, Compression}]
            ),
            View#set_view{btree=Btree, update_seqs=USeqs, purge_seqs=PSeqs}
        end,
        ViewStates2, Views),
    Group#set_view_group{
        fd = Fd,
        id_btree = IdBtree,
        views = Views2,
        index_header = IndexHeader
    }.


commit_header(Group) ->
    Header = {Group#set_view_group.sig, get_index_header_data(Group)},
    ok = couch_file:write_header(Group#set_view_group.fd, Header).


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
                    Rest, Abitmask, Pbitmask, Cbitmask2, Seqs2, PurgeSeqs2)
            end
        end
    end.


validate_partitions(#set_view_index_header{num_partitions = N}, [PartId | _]) when PartId >= N ->
    {error, {invalid_partition, PartId}};
validate_partitions(Header, [_PartId | Rest]) ->
    validate_partitions(Header, Rest);
validate_partitions(_, []) ->
    ok.


update_header(OpName, Partitions, State,
              NewAbitmask, NewPbitmask, NewCbitmask, NewSeqs, NewPurgeSeqs) ->
    #group_state{
        group = #set_view_group{
            index_header =
                #set_view_index_header{
                    num_partitions = NumPartitions,
                    abitmask = Abitmask, pbitmask = Pbitmask, cbitmask = Cbitmask
                } = Header,
            views = Views
        } = Group
    } = State,
    NewState = State#group_state{
        group = Group#set_view_group{
            index_header = Header#set_view_index_header{
                abitmask = NewAbitmask,
                pbitmask = NewPbitmask,
                cbitmask = NewCbitmask,
                seqs = NewSeqs,
                purge_seqs = NewPurgeSeqs
            },
            views = lists:map(
                fun(V) ->
                    V#set_view{update_seqs = NewSeqs, purge_seqs = NewPurgeSeqs}
                end, Views)
        }
    },
    ok = commit_header(NewState#group_state.group),
    {ActiveList, PassiveList} = make_partition_lists(NewState#group_state.group),
    ok = couch_db_set:set_active(?db_set(NewState), ActiveList),
    ok = couch_db_set:set_passive(?db_set(NewState), PassiveList),
    % TODO maybe set to debug level
    ?LOG_INFO("Set view `~s`, group `~s`, ~p ~w~n"
        "abitmask before ~*..0s, abitmask after ~*..0s~n"
        "pbitmask before ~*..0s, pbitmask after ~*..0s~n"
        "cbitmask before ~*..0s, cbitmask after ~*..0s~n",
        [?set_name(State), ?group_id(State), OpName, Partitions,
         NumPartitions, integer_to_list(Abitmask, 2),
         NumPartitions, integer_to_list(NewAbitmask, 2),
         NumPartitions, integer_to_list(Pbitmask, 2),
         NumPartitions, integer_to_list(NewPbitmask, 2),
         NumPartitions, integer_to_list(Cbitmask, 2),
         NumPartitions, integer_to_list(NewCbitmask, 2)]),
    NewState.


maybe_start_cleaner(#group_state{cleaner_pid = Pid} = State) when is_pid(Pid) ->
    State;
maybe_start_cleaner(#group_state{group = Group} = State) ->
    #set_view_group{
        index_header = #set_view_index_header{cbitmask = Cbitmask}
    } = Group,
    case is_pid(State#group_state.compactor_pid) orelse
        is_pid(State#group_state.updater_pid) orelse (Cbitmask == 0) of
    true ->
        State;
    false ->
        Cleaner = spawn_link(fun() -> cleaner(Group) end),
        ?LOG_INFO("Started cleanup process ~p for set view `~s`, group `~s`",
                  [Cleaner, ?set_name(State), ?group_id(State)]),
        State#group_state{cleaner_pid = Cleaner}
    end.


stop_cleaner(#group_state{cleaner_pid = nil} = State) ->
    State;
stop_cleaner(#group_state{cleaner_pid = Pid} = State) when is_pid(Pid) ->
    ?LOG_INFO("Stopping cleanup process for set view `~s`, group `~s`",
              [?set_name(State), (State#group_state.group)#set_view_group.name]),
    Pid ! stop,
    receive
    {'EXIT', Pid, {clean_group, Group}} ->
        ?LOG_INFO("Stopped cleanup process for set view `~s`, group `~s`",
                  [?set_name(State), ?group_id(State)]),
        State#group_state{group = Group, cleaner_pid = nil};
    {'EXIT', Pid, Reason} ->
        exit({cleanup_process_died, Reason})
    end.


cleaner(Group) ->
    #set_view_group{
        index_header = #set_view_index_header{cbitmask = Cbitmask} = Header,
        views = Views,
        id_btree = IdBtree,
        fd = Fd
    } = Group,
    ok = couch_file:flush(Fd),
    {ok, NewIdBtree, Go} = couch_btree:guided_purge(
        IdBtree, make_btree_purge_fun(Cbitmask), go),
    NewViews = case Go of
    go ->
        clean_views(go, Cbitmask, Views, []);
    stop ->
        Views
    end,
    {ok, IdBitmap} = couch_btree:full_reduce(NewIdBtree),
    CombinedBitmap = lists:foldl(
        fun(#set_view{btree = Bt}, Acc) ->
            {ok, {_, _, Bm}} = couch_btree:full_reduce(Bt),
            Acc bor Bm
        end,
        IdBitmap, NewViews),
    NewCbitmask = Cbitmask band CombinedBitmap,
    NewGroup = Group#set_view_group{
        id_btree = NewIdBtree,
        views = NewViews,
        index_header = Header#set_view_index_header{cbitmask = NewCbitmask}
    },
    commit_header(NewGroup),
    ok = couch_file:flush(Fd),
    exit({clean_group, NewGroup}).


clean_views(_, _, [], Acc) ->
    lists:reverse(Acc);
clean_views(stop, _, Rest, Acc) ->
    lists:reverse(Acc, Rest);
clean_views(go, Cbitmask, [#set_view{btree = Btree} = View | Rest], Acc) ->
    {ok, NewBtree, Go} = couch_btree:guided_purge(
        Btree, make_btree_purge_fun(Cbitmask), go),
    clean_views(Go, Cbitmask, Rest, [View#set_view{btree = NewBtree} | Acc]).


btree_purge_fun(value, {_K, {PartId, _}}, Acc, Cbitmask) ->
    Mask = 1 bsl PartId,
    case (Cbitmask band Mask) of
    Mask ->
        {purge, Acc};
    0 ->
        {keep, Acc}
    end;
btree_purge_fun(branch, Red, Acc, Cbitmask) ->
    Bitmap = case is_tuple(Red) of
    true ->
        element(3, Red);
    false ->
        Red
    end,
    case Bitmap band Cbitmask of
    0 ->
        {keep, Acc};
    _ ->
        case Bitmap bxor Cbitmask of
        0 ->
            {purge, Acc};
        _ ->
            {partial_purge, Acc}
        end
    end.


make_btree_purge_fun(Cbitmask) ->
    fun(Type, Value, Acc) ->
        receive
        stop ->
            {stop, stop}
        after 0 ->
            btree_purge_fun(Type, Value, Acc, Cbitmask)
        end
    end.


index_needs_update(#group_state{group = Group} = State) ->
    {ok, CurSeqs} = couch_db_set:get_seqs(?db_set(State)),
    {CurSeqs > ?set_seqs(Group), CurSeqs}.


make_partition_lists(#set_view_group{index_header = Header}) ->
    #set_view_index_header{
        seqs = Seqs, abitmask = Abitmask, pbitmask = Pbitmask
    } = Header,
    make_partition_lists(Seqs, Abitmask, Pbitmask, [], []).

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
    State2 = stop_cleaner(State),
    ?LOG_INFO("Set view `~s`, group `~s`, compaction starting",
              [?set_name(State2), ?group_id(State2)]),
    NewGroup = compact_group(State2),
    Pid = spawn_link(fun() ->
        CompactFun(State2#group_state.group, NewGroup, ?set_name(State2))
    end),
    State2#group_state{compactor_pid = Pid, compactor_fun = CompactFun}.


compact_group(State) ->
    #group_state{
        group = #set_view_group{sig = GroupSig} = Group,
        init_args = {RootDir, SetName, _}
    } = State,
    {ok, Fd} = open_index_file(compact, RootDir, SetName, GroupSig),
    reset_file(Fd, SetName, Group).


accept_compact_group(NewGroup, CurrentGroup) ->
    #set_view_group{id_btree = NewIdBtree} = NewGroup,
    {ok, IdBitmap} = couch_btree:full_reduce(NewIdBtree),
    (IdBitmap band ?set_cbitmask(CurrentGroup)) =:= 0.
