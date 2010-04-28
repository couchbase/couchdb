 -module(couch_spatial_group).
-behaviour(gen_server).

%% API
-export([start_link/1, request_group/2, open_db_group/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("couch_db.hrl").
-include("couch_spatial.hrl").

-record(group_state, {
%    type,
    db_name,
    init_args,
    group,
    updater_pid=nil,
%    compactor_pid=nil,
    waiting_commit=false,
    waiting_list=[],
    ref_counter=nil
}).


% from template
start_link(InitArgs) ->
    case gen_server:start_link(couch_spatial_group,
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

% api methods
request_group(Pid, Seq) ->
    ?LOG_DEBUG("request_group {Pid, Seq} ~p", [{Pid, Seq}]),
    case gen_server:call(Pid, {request_group, Seq}, infinity) of
    {ok, Group, RefCounter} ->
?LOG_DEBUG("request_group:~n~p", [Group]),
        couch_ref_counter:add(RefCounter),
        {ok, Group};
    Error ->
        ?LOG_DEBUG("request_group Error ~p", [Error]),
        throw(Error)
    end.



init({InitArgs, ReturnPid, Ref}) ->
?LOG_DEBUG("(1) couch_spatial_group init", []),
    process_flag(trap_exit, true),
    case prepare_group(InitArgs, false) of
    {ok, #spatial_group{db=Db, fd=Fd, current_seq=Seq}=Group} ->
?LOG_DEBUG("(2) couch_spatial_group init", []),
        case Seq > couch_db:get_update_seq(Db) of
        true ->
?LOG_DEBUG("(2b) couch_spatial_group init", []),
            ReturnPid ! {Ref, self(), {error, invalid_view_seq}},
            ignore;
        _ ->
            couch_db:monitor(Db),
            Owner = self(),
?LOG_DEBUG("(3) couch_spatial_group init pre-spawn", []),
            Pid = spawn_link(
                fun()-> couch_spatial_updater:update(Owner, Group) end
            ),
?LOG_DEBUG("(4) couch_spatial_group init post-spawn", []),
            {ok, RefCounter} = couch_ref_counter:start([Fd]),
            {ok, #group_state{
                    db_name=couch_db:name(Db),
                    init_args=InitArgs,
                    updater_pid = Pid,
                    group=Group,
                    ref_counter=RefCounter}}
        end;
    Error ->
?LOG_DEBUG("(3) couch_spatial_group init: ~p", [Error]),
        ReturnPid ! {Ref, self(), Error},
        ignore
    end.

% NOTE vmx: There's a lenghy comment about this call in couch_view_group.erl
handle_call({request_group, RequestSeq}, From, #group_state{
            db_name=DbName,
            group=#spatial_group{current_seq=GroupSeq}=Group,
            updater_pid=nil,
            waiting_list=WaitList
            }=State) when RequestSeq > GroupSeq ->
    {ok, Db} = couch_db:open_int(DbName, []),
    Group2 = Group#spatial_group{db=Db},
    Owner = self(),
    Pid = spawn_link(fun()-> couch_spatial_updater:update(Owner, Group2) end),

    {noreply, State#group_state{
        updater_pid=Pid,
        group=Group2,
        waiting_list=[{From,RequestSeq}|WaitList]
        }, infinity};

% If the request seqence is less than or equal to the seq_id of a known Group,
% we respond with that Group.
handle_call({request_group, RequestSeq}, _From, #group_state{
            group = #spatial_group{current_seq=GroupSeq} = Group,
            ref_counter = RefCounter
        } = State) when RequestSeq =< GroupSeq  ->
?LOG_DEBUG("(2) request_group handler: seqs: req: ~p, group: ~p", [RequestSeq, GroupSeq]),
?LOG_DEBUG("(2b) request_group handler: Group:~n~p", [Group]),
    {reply, {ok, Group, RefCounter}, State};

% Otherwise: TargetSeq => RequestSeq > GroupSeq
% We've already initiated the appropriate action, so just hold the response until the group is up to the RequestSeq
handle_call({request_group, RequestSeq}, From,
        #group_state{waiting_list=WaitList}=State) ->
?LOG_DEBUG("(3) request_group handler: seqs: req: ~p", [RequestSeq]),
    {noreply, State#group_state{
        waiting_list=[{From, RequestSeq}|WaitList]
        }, infinity}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'EXIT', FromPid, {new_group, #spatial_group{db=Db}=Group}},
        #group_state{db_name=DbName,
            updater_pid=UpPid,
            ref_counter=RefCounter,
            waiting_list=WaitList,
            waiting_commit=WaitingCommit}=State) when UpPid == FromPid ->
    ok = couch_db:close(Db),
    if not WaitingCommit ->
        erlang:send_after(1000, self(), delayed_commit);
    true -> ok
    end,
    case reply_with_group(Group, WaitList, [], RefCounter) of
    [] ->
        {noreply, State#group_state{waiting_commit=true, waiting_list=[],
                group=Group#spatial_group{db=nil}, updater_pid=nil}};
    StillWaiting ->
        % we still have some waiters, reopen the database and reupdate the index
        {ok, Db2} = couch_db:open_int(DbName, []),
        Group2 = Group#spatial_group{db=Db2},
        Owner = self(),
        Pid = spawn_link(fun() -> couch_view_updater:update(Owner, Group2) end),
        {noreply, State#group_state{waiting_commit=true,
                waiting_list=StillWaiting, group=Group2, updater_pid=Pid}}
    end;

handle_info(_Msg, Server) ->
    {noreply, Server}.

terminate(_Reason, _Srv) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% reply_with_group/3
% for each item in the WaitingList {Pid, Seq}
% if the Seq is =< GroupSeq, reply
reply_with_group(Group=#spatial_group{current_seq=GroupSeq}, [{Pid, Seq}|WaitList],
        StillWaiting, RefCounter) when Seq =< GroupSeq ->
    gen_server:reply(Pid, {ok, Group, RefCounter}),
    reply_with_group(Group, WaitList, StillWaiting, RefCounter);

% else
% put it in the continuing waiting list
reply_with_group(Group, [{Pid, Seq}|WaitList], StillWaiting, RefCounter) ->
    reply_with_group(Group, WaitList, [{Pid, Seq}|StillWaiting], RefCounter);

% return the still waiting list
reply_with_group(_Group, [], StillWaiting, _RefCounter) ->
    StillWaiting.


open_db_group(DbName, DDocId) ->
?LOG_DEBUG("open_db_group: DbName: ~p, DDocId: ~p", [DbName, DDocId]),
    case couch_db:open_int(DbName, []) of
    {ok, Db} ->
?LOG_DEBUG("(2) open_db_group: DbName: ~p, DDocId: ~p", [DbName, DDocId]),
        case couch_db:open_doc(Db, DDocId) of
        {ok, Doc} ->
?LOG_DEBUG("(3) open_db_group: DbName: ~p, DDocId: ~p", [DbName, DDocId]),
            {ok, Db, design_doc_to_spatial_group(Doc)};
        Else ->
?LOG_DEBUG("(4) open_db_group: DbName: ~p, DDocId: ~p", [DbName, DDocId]),
            couch_db:close(Db),
            Else
        end;
    Else ->
?LOG_DEBUG("(5) open_db_group: DbName: ~p, DDocId: ~p", [DbName, DDocId]),
        Else
    end.


design_doc_to_spatial_group(#doc{id=Id,body={Fields}}) ->
?LOG_DEBUG("design_doc_to_spatial_group", []),
    Language = proplists:get_value(<<"language">>, Fields, <<"javascript">>),
    {DesignOptions} = proplists:get_value(<<"options">>, Fields, {[]}),
    {RawIndexes} = proplists:get_value(<<"spatial">>, Fields, {[]}),
?LOG_DEBUG("rawindexes:~p", [RawIndexes]),
    % add the views to a dictionary object, with the map source as the key
    DictBySrc =
    lists:foldl(fun({Name, IndexSrc}, DictBySrcAcc) ->
?LOG_DEBUG("inner fun: Name: ~p, IndexSrc: ~p", [Name, IndexSrc]),
        Index =
        case dict:find({IndexSrc}, DictBySrcAcc) of
            {ok, Index0} -> Index0;
            error -> #spatial{def=IndexSrc} % create new spatial index object
        end,
        Index2 = Index#spatial{index_names=[Name|Index#spatial.index_names]},
        dict:store({IndexSrc}, Index2, DictBySrcAcc)
    end, dict:new(), RawIndexes),
    % number the views
    {Indexes, _N} = lists:mapfoldl(
        fun({_Src, Index}, N) ->
            {Index#spatial{id_num=N},N+1}
        end, 0, lists:sort(dict:to_list(DictBySrc))),
?LOG_DEBUG("(2) design_doc_to_spatial_group: ~p", [Indexes]),
    set_index_sig(#spatial_group{name=Id, indexes=Indexes, def_lang=Language,
                                 design_options=DesignOptions}).

set_index_sig(#spatial_group{
            indexes=Indexes,
            def_lang=Language,
            design_options=DesignOptions}=G) ->
    G#spatial_group{sig=erlang:md5(term_to_binary({Indexes, Language,
                                                   DesignOptions}))}.



prepare_group({RootDir, DbName, #spatial_group{sig=Sig}=Group}, ForceReset)->
?LOG_DEBUG("(1) prepare group", []),
    case couch_db:open_int(DbName, []) of
    {ok, Db} ->
?LOG_DEBUG("(2) prepare group", []),
        case open_index_file(RootDir, DbName, Sig) of
        {ok, Fd} ->
?LOG_DEBUG("(3) prepare group", []),
            if ForceReset ->
?LOG_DEBUG("(6) prepare group", []),
                % this can happen if we missed a purge
                {ok, reset_file(Db, Fd, DbName, Group)};
            true ->
?LOG_DEBUG("(7) prepare group", []),
                % 09 UPGRADE CODE
                ok = couch_file:upgrade_old_header(Fd, <<$r, $c, $k, 0>>),
                case (catch couch_file:read_header(Fd)) of
                {ok, {Sig, HeaderInfo}} ->
?LOG_DEBUG("(8) prepare group", []),
                    % sigs match!
                    {ok, init_group(Db, Fd, Group, HeaderInfo)};
                _ ->
?LOG_DEBUG("(9) prepare group", []),
                    % this happens on a new file
                    {ok, reset_file(Db, Fd, DbName, Group)}
                end
            end;
        Error ->
?LOG_DEBUG("(4) prepare group", []),
            catch delete_index_file(RootDir, DbName, Sig),
            Error
        end;
    Else ->
?LOG_DEBUG("(5) prepare group", []),
        Else
    end.

delete_index_file(RootDir, DbName, GroupSig) ->
    file:delete(index_file_name(RootDir, DbName, GroupSig)).

index_file_name(RootDir, DbName, GroupSig) ->
    couch_view_group:design_root(RootDir, DbName) ++
        couch_view_group:hex_sig(GroupSig) ++".spatial".

open_index_file(RootDir, DbName, GroupSig) ->
    FileName = index_file_name(RootDir, DbName, GroupSig),
    case couch_file:open(FileName) of
    {ok, Fd}        -> {ok, Fd};
    {error, enoent} -> couch_file:open(FileName, [create]);
    Error           -> Error
    end.

reset_group(#spatial_group{indexes=Indexes}=Group) ->
    % XXX vmx: I should reset the Treepos in my #spatial
    %Views2 = [View#view{btree=nil} || View <- Views],
    Indexes2 = [Index#spatial{treepos=nil} || Index <- Indexes],
    Group#spatial_group{db=nil,fd=nil,query_server=nil,current_seq=0,
            indexes=Indexes2}.

reset_file(Db, Fd, DbName, #spatial_group{sig=Sig,name=Name} = Group) ->
    ?LOG_DEBUG("Resetting spatial group index \"~s\" in db ~s", [Name, DbName]),
    ok = couch_file:truncate(Fd, 0),
    ok = couch_file:write_header(Fd, {Sig, nil}),
    init_group(Db, Fd, reset_group(Group), nil).


init_group(Db, Fd, #spatial_group{indexes=Indexes}=Group, nil) ->
    init_group(Db, Fd, Group,
        #spatial_index_header{seq=0, purge_seq=couch_db:get_purge_seq(Db),
            id_btree_state=nil, index_states=[nil || _ <- Indexes]});
init_group(Db, Fd, #spatial_group{indexes=Indexes}=Group,
           IndexHeader) ->
    #spatial_index_header{seq=Seq, purge_seq=PurgeSeq,
            id_btree_state=IdBtreeState, index_states=IndexStates} = IndexHeader,
    {ok, IdBtree} = couch_btree:open(IdBtreeState, Fd),
    Indexes2 = lists:zipwith(
        fun(IndexTreePos, Index) ->
            %{ok, Btree} = couch_btree:open(BtreeState, Fd),
            Index#spatial{treepos=IndexTreePos}
        end,
        IndexStates, Indexes),
    Group#spatial_group{db=Db, fd=Fd, current_seq=Seq, purge_seq=PurgeSeq,
        id_btree=IdBtree, indexes=Indexes2}.
