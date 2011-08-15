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

-module(couch_db_updater).
-behaviour(gen_server).

-export([btree_by_id_reduce/2,btree_by_seq_reduce/2]).
-export([make_doc_summary/2]).
-export([init/1,terminate/2,handle_call/3,handle_cast/2,code_change/3,handle_info/2]).

-include("couch_db.hrl").


init({MainPid, DbName, Filepath, Fd, Options}) ->
    process_flag(trap_exit, true),
    case lists:member(create, Options) of
    true ->
        % create a new header and writes it to the file
        Header =  #db_header{},
        ok = couch_file:write_header(Fd, Header),
        % delete any old compaction files that might be hanging around
        RootDir = couch_config:get("couchdb", "database_dir", "."),
        couch_file:delete(RootDir, Filepath ++ ".compact");
    false ->
        case couch_file:read_header(Fd) of
        {ok, Header} ->
            ok;
        no_valid_header ->
            % create a new header and writes it to the file
            Header =  #db_header{},
            ok = couch_file:write_header(Fd, Header),
            % delete any old compaction files that might be hanging around
            file:delete(Filepath ++ ".compact")
        end
    end,

    Db = init_db(DbName, Filepath, Fd, Header, Options),
    Db2 = refresh_validate_doc_funs(Db),
    {ok, Db2#db{main_pid = MainPid}}.


terminate(_Reason, Db) ->
    couch_file:close(Db#db.fd),
    couch_util:shutdown_sync(Db#db.compactor_pid),
    couch_util:shutdown_sync(Db#db.fd_ref_counter),
    ok.

handle_call(get_db, _From, Db) ->
    {reply, {ok, Db}, Db};
handle_call(full_commit, _From, #db{waiting_delayed_commit=nil}=Db) ->
    {reply, ok, Db}; % no data waiting, return ok immediately
handle_call(full_commit, _From,  Db) ->
    {reply, ok, commit_data(Db)}; % commit the data and return ok
handle_call(increment_update_seq, _From, Db) ->
    Db2 = commit_data(Db#db{update_seq=Db#db.update_seq+1}),
    ok = gen_server:call(Db2#db.main_pid, {db_updated, Db2}),
    couch_db_update_notifier:notify({updated, Db#db.name}),
    {reply, {ok, Db2#db.update_seq}, Db2};

handle_call({set_security, NewSec}, _From, #db{compression = Comp} = Db) ->
    {ok, Ptr, _} = couch_file:append_term(
        Db#db.fd, NewSec, [{compression, Comp}]),
    Db2 = commit_data(Db#db{security=NewSec, security_ptr=Ptr,
            update_seq=Db#db.update_seq+1}),
    ok = gen_server:call(Db2#db.main_pid, {db_updated, Db2}),
    {reply, ok, Db2};

handle_call({set_revs_limit, Limit}, _From, Db) ->
    Db2 = commit_data(Db#db{revs_limit=Limit,
            update_seq=Db#db.update_seq+1}),
    ok = gen_server:call(Db2#db.main_pid, {db_updated, Db2}),
    {reply, ok, Db2};

handle_call({purge_docs, _IdRevs}, _From,
        #db{compactor_pid=Pid}=Db) when Pid /= nil ->
    {reply, {error, purge_during_compaction}, Db};
handle_call({purge_docs, IdRevs}, _From, Db) ->
    #db{
        fd = Fd,
        fulldocinfo_by_id_btree = DocInfoByIdBTree,
        docinfo_by_seq_btree = DocInfoBySeqBTree,
        update_seq = LastSeq,
        header = Header = #db_header{purge_seq=PurgeSeq},
        compression = Comp
        } = Db,
    DocLookups = couch_btree:lookup(DocInfoByIdBTree,
            [Id || {Id, _Revs} <- IdRevs]),

    NewDocInfos = lists:zipwith(
        fun({_Id, Revs}, {ok, #full_doc_info{rev_tree=Tree}=FullDocInfo}) ->
            case couch_key_tree:remove_leafs(Tree, Revs) of
            {_, []=_RemovedRevs} -> % no change
                nil;
            {NewTree, RemovedRevs} ->
                {FullDocInfo#full_doc_info{rev_tree=NewTree},RemovedRevs}
            end;
        (_, not_found) ->
            nil
        end,
        IdRevs, DocLookups),

    SeqsToRemove = [Seq
            || {#full_doc_info{update_seq=Seq},_} <- NewDocInfos],

    FullDocInfoToUpdate = [FullInfo
            || {#full_doc_info{rev_tree=Tree}=FullInfo,_}
            <- NewDocInfos, Tree /= []],

    IdRevsPurged = [{Id, Revs}
            || {#full_doc_info{id=Id}, Revs} <- NewDocInfos],

    {DocInfoToUpdate, NewSeq} = lists:mapfoldl(
        fun(#full_doc_info{rev_tree=Tree}=FullInfo, SeqAcc) ->
            Tree2 = couch_key_tree:map_leafs(
                fun(_RevId, LeafVal) ->
                    IsDeleted = element(1, LeafVal),
                    BodyPointer = element(2, LeafVal),
                    {IsDeleted, BodyPointer, SeqAcc + 1}
                end, Tree),
            {couch_doc:to_doc_info(FullInfo#full_doc_info{rev_tree=Tree2}),
                SeqAcc + 1}
        end, LastSeq, FullDocInfoToUpdate),

    IdsToRemove = [Id || {#full_doc_info{id=Id,rev_tree=[]},_}
            <- NewDocInfos],

    {ok, DocInfoBySeqBTree2} = couch_btree:add_remove(DocInfoBySeqBTree,
            DocInfoToUpdate, SeqsToRemove),
    {ok, DocInfoByIdBTree2} = couch_btree:add_remove(DocInfoByIdBTree,
            FullDocInfoToUpdate, IdsToRemove),
    {ok, Pointer, _} = couch_file:append_term(
            Fd, IdRevsPurged, [{compression, Comp}]),

    Db2 = commit_data(
        Db#db{
            fulldocinfo_by_id_btree = DocInfoByIdBTree2,
            docinfo_by_seq_btree = DocInfoBySeqBTree2,
            update_seq = NewSeq + 1,
            header=Header#db_header{purge_seq=PurgeSeq+1, purged_docs=Pointer}}),

    ok = gen_server:call(Db2#db.main_pid, {db_updated, Db2}),
    couch_db_update_notifier:notify({updated, Db#db.name}),
    {reply, {ok, (Db2#db.header)#db_header.purge_seq, IdRevsPurged}, Db2};
handle_call(start_compact, _From, Db) ->
    case Db#db.compactor_pid of
    nil ->
        ?LOG_INFO("Starting compaction for db \"~s\"", [Db#db.name]),
        Pid = spawn_link(fun() -> start_copy_compact(Db) end),
        Db2 = Db#db{compactor_pid=Pid},
        ok = gen_server:call(Db#db.main_pid, {db_updated, Db2}),
        {reply, {ok, Pid}, Db2};
    _ ->
        % compact currently running, this is a no-op
        {reply, {ok, Db#db.compactor_pid}, Db}
    end;
handle_call(cancel_compact, _From, #db{compactor_pid = nil} = Db) ->
    {reply, ok, Db};
handle_call(cancel_compact, _From, #db{compactor_pid = Pid} = Db) ->
    unlink(Pid),
    exit(Pid, kill),
    RootDir = couch_config:get("couchdb", "database_dir", "."),
    ok = couch_file:delete(RootDir, Db#db.filepath ++ ".compact"),
    {reply, ok, Db#db{compactor_pid = nil}};


handle_call({compact_done, CompactFilepath}, _From, #db{filepath=Filepath}=Db) ->
    {ok, NewFd} = couch_file:open(CompactFilepath),
    {ok, NewHeader} = couch_file:read_header(NewFd),
    #db{update_seq=NewSeq} = NewDb =
        init_db(Db#db.name, Filepath, NewFd, NewHeader, Db#db.options),
    unlink(NewFd),
    case Db#db.update_seq == NewSeq of
    true ->
        % suck up all the local docs into memory and write them to the new db
        {ok, _, LocalDocs} = couch_btree:foldl(Db#db.local_docs_btree,
                fun(Value, _Offset, Acc) -> {ok, [Value | Acc]} end, []),
        {ok, NewLocalBtree} = couch_btree:add(NewDb#db.local_docs_btree, LocalDocs),

        NewDb2 = commit_data(NewDb#db{
            local_docs_btree = NewLocalBtree,
            main_pid = Db#db.main_pid,
            filepath = Filepath,
            instance_start_time = Db#db.instance_start_time,
            revs_limit = Db#db.revs_limit
        }),

        ?LOG_DEBUG("CouchDB swapping files ~s and ~s.",
                [Filepath, CompactFilepath]),
        RootDir = couch_config:get("couchdb", "database_dir", "."),
        couch_file:delete(RootDir, Filepath),
        ok = file:rename(CompactFilepath, Filepath),
        close_db(Db),
        NewDb3 = refresh_validate_doc_funs(NewDb2),
        ok = gen_server:call(Db#db.main_pid, {db_updated, NewDb3}, infinity),
        couch_db_update_notifier:notify({compacted, NewDb3#db.name}),
        ?LOG_INFO("Compaction for db \"~s\" completed.", [Db#db.name]),
        {reply, ok, NewDb3#db{compactor_pid=nil}};
    false ->
        ?LOG_INFO("Compaction file still behind main file "
            "(update seq=~p. compact update seq=~p). Retrying.",
            [Db#db.update_seq, NewSeq]),
        close_db(NewDb),
        {reply, {retry, Db}, Db}
    end.


handle_cast(Msg, #db{name = Name} = Db) ->
    ?LOG_ERROR("Database `~s` updater received unexpected cast: ~p", [Name, Msg]),
    {stop, Msg, Db}.


handle_info({update_docs, Client, GroupedDocs, NonRepDocs, MergeConflicts,
        FullCommit, Clobber}, Db) ->
    GroupedDocs2 = [[{Client, D} || D <- DocGroup] || DocGroup <- GroupedDocs],
    if NonRepDocs == [] ->
        {GroupedDocs3, Clients, FullCommit2} = collect_updates(GroupedDocs2,
                [Client], MergeConflicts, FullCommit, Clobber);
    true ->
        GroupedDocs3 = GroupedDocs2,
        FullCommit2 = FullCommit,
        Clients = [Client]
    end,
    NonRepDocs2 = [{Client, NRDoc} || NRDoc <- NonRepDocs],
    try update_docs_int(Db, GroupedDocs3, NonRepDocs2, MergeConflicts,
                FullCommit2, Clobber) of
    {ok, Db2} ->
        ok = gen_server:call(Db#db.main_pid, {db_updated, Db2}),
        if Db2#db.update_seq /= Db#db.update_seq ->
            couch_db_update_notifier:notify({updated, Db2#db.name});
        true -> ok
        end,
        [catch(ClientPid ! {done, self()}) || ClientPid <- Clients],
        {noreply, Db2}
    catch
        throw: retry ->
            [catch(ClientPid ! {retry, self()}) || ClientPid <- Clients],
            {noreply, Db}
    end;
handle_info(delayed_commit, #db{waiting_delayed_commit=nil}=Db) ->
    %no outstanding delayed commits, ignore
    {noreply, Db};
handle_info(delayed_commit, Db) ->
    case commit_data(Db) of
        Db ->
            {noreply, Db};
        Db2 ->
            ok = gen_server:call(Db2#db.main_pid, {db_updated, Db2}),
            {noreply, Db2}
    end;
handle_info({'EXIT', _Pid, normal}, Db) ->
    {noreply, Db};
handle_info({'EXIT', _Pid, Reason}, Db) ->
    {stop, Reason, Db}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


collect_updates(SortedDocs, ClientsAcc, MergeConflicts, FullCommit, Clobber) ->
    {UnsortedDocs, Clients, FullCommit1} =
        collect_unsorted_updates([], ClientsAcc, MergeConflicts, FullCommit, Clobber),
    GroupedDocs = case UnsortedDocs of
    [] ->
        SortedDocs;
    _ ->
        sort_grouped_updates(SortedDocs ++ UnsortedDocs)
    end,
    {GroupedDocs, Clients, FullCommit1}.

sort_grouped_updates(BucketList) ->
    Sorted = lists:sort(
        fun([{_, #doc_update_info{id = A}} | _], [{_, #doc_update_info{id = B}} | _]) -> A < B end,
        BucketList),
    group_buckets(Sorted, []).

group_buckets([], Buckets) ->
    lists:reverse(Buckets);
group_buckets([Bucket | Rest], []) ->
    group_buckets(Rest, [Bucket]);
group_buckets([[{_, Doc1} | _] = B1 | Rest1], [[{_, Doc2} | _] = B2 | Rest2]) ->
    case Doc1#doc_update_info.id =:= Doc2#doc_update_info.id of
    true ->
        group_buckets(Rest1, [B1 ++ B2 | Rest2]);
    false ->
        group_buckets(Rest1, [B1, B2 | Rest2])
    end.

collect_unsorted_updates(DocsAcc, ClientsAcc, MergeConflicts, FullCommit, Clobber) ->
    receive
        % Only collect updates with the same MergeConflicts flag and without
        % local docs. It's easier to just avoid multiple _local doc
        % updaters than deal with their possible conflicts, and local docs
        % writes are relatively rare. Can be optmized later if really needed.
        {update_docs, Client, GroupedDocs, [], MergeConflicts, FullCommit2, Clobber} ->
            DocsAcc2 = DocsAcc ++ [[{Client, Doc} || Doc <- DocGroup]
                    || DocGroup <- GroupedDocs],
            collect_unsorted_updates(DocsAcc2, [Client | ClientsAcc],
                    MergeConflicts, (FullCommit or FullCommit2), Clobber)
    after 0 ->
        {DocsAcc, ClientsAcc, FullCommit}
    end.


btree_by_seq_split(#doc_info{id=Id, high_seq=KeySeq, revs=Revs}) ->
    {RevInfos, DeletedRevInfos} = lists:foldl(
        fun(#rev_info{deleted = false, seq = Seq} = Ri, {Acc, AccDel}) ->
                {[{Ri#rev_info.rev, Seq, Ri#rev_info.body_sp} | Acc], AccDel};
            (#rev_info{deleted = true, seq = Seq} = Ri, {Acc, AccDel}) ->
                {Acc, [{Ri#rev_info.rev, Seq, Ri#rev_info.body_sp} | AccDel]}
        end,
        {[], []}, Revs),
    {KeySeq, {Id, lists:reverse(RevInfos), lists:reverse(DeletedRevInfos)}}.

btree_by_seq_join(KeySeq, {Id, RevInfos, DeletedRevInfos}) ->
    #doc_info{
        id = Id,
        high_seq=KeySeq,
        revs =
            [#rev_info{rev=Rev,seq=Seq,deleted=false,body_sp = Bp} ||
                {Rev, Seq, Bp} <- RevInfos] ++
            [#rev_info{rev=Rev,seq=Seq,deleted=true,body_sp = Bp} ||
                {Rev, Seq, Bp} <- DeletedRevInfos]}.

btree_by_id_split(#full_doc_info{id=Id, update_seq=Seq,
        deleted=Deleted, rev_tree=Tree}) ->
    DiskTree =
    couch_key_tree:map(
        fun(_RevId, ?REV_MISSING) ->
            ?REV_MISSING;
        (_RevId, RevValue) ->
            IsDeleted = element(1, RevValue),
            BodyPointer = element(2, RevValue),
            UpdateSeq = element(3, RevValue),
            Size = case tuple_size(RevValue) of
            4 ->
                element(4, RevValue);
            3 ->
                % pre 1.2 format, will be upgraded on compaction
                nil
            end,
            {if IsDeleted -> 1; true -> 0 end, BodyPointer, UpdateSeq, Size}
        end, Tree),
    {Id, {Seq, if Deleted -> 1; true -> 0 end, DiskTree}}.

btree_by_id_join(Id, {HighSeq, Deleted, DiskTree}) ->
    {Tree, LeafsSize} =
    couch_key_tree:mapfold(
        fun(_RevId, {IsDeleted, BodyPointer, UpdateSeq}, leaf, _Acc) ->
            % pre 1.2 format, will be upgraded on compaction
            {{IsDeleted == 1, BodyPointer, UpdateSeq, nil}, nil};
        (_RevId, {IsDeleted, BodyPointer, UpdateSeq}, branch, Acc) ->
            {{IsDeleted == 1, BodyPointer, UpdateSeq, nil}, Acc};
        (_RevId, {IsDeleted, BodyPointer, UpdateSeq, Size}, leaf, Acc) ->
            Acc2 = sum_leaf_sizes(Acc, Size),
            {{IsDeleted == 1, BodyPointer, UpdateSeq, Size}, Acc2};
        (_RevId, {IsDeleted, BodyPointer, UpdateSeq, Size}, branch, Acc) ->
            {{IsDeleted == 1, BodyPointer, UpdateSeq, Size}, Acc};
        (_RevId, ?REV_MISSING, _Type, Acc) ->
            {?REV_MISSING, Acc}
        end, 0, DiskTree),
    #full_doc_info{
        id = Id,
        update_seq = HighSeq,
        deleted = (Deleted == 1),
        rev_tree = Tree,
        leafs_size = LeafsSize
    }.

btree_by_id_reduce(reduce, FullDocInfos) ->
    lists:foldl(
        fun(Info, {NotDeleted, Deleted, Size}) ->
            Size2 = sum_leaf_sizes(Size, Info#full_doc_info.leafs_size),
            case Info#full_doc_info.deleted of
            true ->
                {NotDeleted, Deleted + 1, Size2};
            false ->
                {NotDeleted + 1, Deleted, Size2}
            end
        end,
        {0, 0, 0}, FullDocInfos);
btree_by_id_reduce(rereduce, Reds) ->
    lists:foldl(
        fun({NotDeleted, Deleted}, {AccNotDeleted, AccDeleted, _AccSize}) ->
            % pre 1.2 format, will be upgraded on compaction
            {AccNotDeleted + NotDeleted, AccDeleted + Deleted, nil};
        ({NotDeleted, Deleted, Size}, {AccNotDeleted, AccDeleted, AccSize}) ->
            AccSize2 = sum_leaf_sizes(AccSize, Size),
            {AccNotDeleted + NotDeleted, AccDeleted + Deleted, AccSize2}
        end,
        {0, 0, 0}, Reds).

sum_leaf_sizes(nil, _) ->
    nil;
sum_leaf_sizes(_, nil) ->
    nil;
sum_leaf_sizes(Size1, Size2) ->
    Size1 + Size2.

btree_by_seq_reduce(reduce, DocInfos) ->
    % count the number of documents
    length(DocInfos);
btree_by_seq_reduce(rereduce, Reds) ->
    lists:sum(Reds).

simple_upgrade_record(Old, New) when tuple_size(Old) < tuple_size(New) ->
    OldSz = tuple_size(Old),
    NewValuesTail =
        lists:sublist(tuple_to_list(New), OldSz + 1, tuple_size(New) - OldSz),
    list_to_tuple(tuple_to_list(Old) ++ NewValuesTail);
simple_upgrade_record(Old, _New) ->
    Old.

-define(OLD_DISK_VERSION_ERROR,
    "Database files from versions smaller than 0.10.0 are no longer supported").

init_db(DbName, Filepath, Fd, Header0, Options) ->
    Header1 = simple_upgrade_record(Header0, #db_header{}),
    Header =
    case element(2, Header1) of
    1 -> throw({database_disk_version_error, ?OLD_DISK_VERSION_ERROR});
    2 -> throw({database_disk_version_error, ?OLD_DISK_VERSION_ERROR});
    3 -> throw({database_disk_version_error, ?OLD_DISK_VERSION_ERROR});
    4 -> Header1#db_header{security_ptr = nil}; % 0.10 and pre 0.11
    5 -> Header1; % pre 1.2
    ?LATEST_DISK_VERSION -> Header1;
    _ -> throw({database_disk_version_error, "Incorrect disk header version"})
    end,

    {ok, FsyncOptions} = couch_util:parse_term(
            couch_config:get("couchdb", "fsync_options",
                    "[before_header, after_header, on_file_open]")),

    case lists:member(on_file_open, FsyncOptions) of
    true -> ok = couch_file:sync(Fd);
    _ -> ok
    end,

    Compression = couch_compress:get_compression_method(),

    {ok, IdBtree} = couch_btree:open(Header#db_header.fulldocinfo_by_id_btree_state, Fd,
        [{split, fun(X) -> btree_by_id_split(X) end},
        {join, fun(X,Y) -> btree_by_id_join(X,Y) end},
        {reduce, fun(X,Y) -> btree_by_id_reduce(X,Y) end},
        {compression, Compression}]),
    {ok, SeqBtree} = couch_btree:open(Header#db_header.docinfo_by_seq_btree_state, Fd,
            [{split, fun(X) -> btree_by_seq_split(X) end},
            {join, fun(X,Y) -> btree_by_seq_join(X,Y) end},
            {reduce, fun(X,Y) -> btree_by_seq_reduce(X,Y) end},
            {compression, Compression}]),
    {ok, LocalDocsBtree} = couch_btree:open(Header#db_header.local_docs_btree_state, Fd,
        [{compression, Compression}]),
    case Header#db_header.security_ptr of
    nil ->
        Security = [],
        SecurityPtr = nil;
    SecurityPtr ->
        {ok, Security} = couch_file:pread_term(Fd, SecurityPtr)
    end,
    % convert start time tuple to microsecs and store as a binary string
    {MegaSecs, Secs, MicroSecs} = now(),
    StartTime = ?l2b(io_lib:format("~p",
            [(MegaSecs*1000000*1000000) + (Secs*1000000) + MicroSecs])),
    {ok, RefCntr} = couch_ref_counter:start([Fd]),
    #db{
        update_pid=self(),
        fd = Fd,
        fd_ref_counter = RefCntr,
        header=Header,
        fulldocinfo_by_id_btree = IdBtree,
        docinfo_by_seq_btree = SeqBtree,
        local_docs_btree = LocalDocsBtree,
        committed_update_seq = Header#db_header.update_seq,
        update_seq = Header#db_header.update_seq,
        name = DbName,
        filepath = Filepath,
        security = Security,
        security_ptr = SecurityPtr,
        instance_start_time = StartTime,
        revs_limit = Header#db_header.revs_limit,
        fsync_options = FsyncOptions,
        options = Options,
        compression = Compression
        }.


close_db(#db{fd_ref_counter = RefCntr}) ->
    couch_ref_counter:drop(RefCntr).


refresh_validate_doc_funs(Db) ->
    {ok, DesignDocs} = couch_db:get_design_docs(Db),
    ProcessDocFuns = lists:flatmap(
        fun(DesignDoc) ->
            case couch_doc:get_validate_doc_fun(DesignDoc) of
            nil -> [];
            Fun -> [Fun]
            end
        end, DesignDocs),
    Db#db{validate_doc_funs=ProcessDocFuns}.

% rev tree functions

send_result(Client, Id, OriginalRevs, NewResult) ->
    % used to send a result to the client
    catch(Client ! {result, self(), {{Id, OriginalRevs}, NewResult}}).


by_seq_index_entries([], AccNewBySeqEntries, AccRemoveBySeqs) ->
    {AccNewBySeqEntries, AccRemoveBySeqs};
by_seq_index_entries([{nil, NewFullInfo}|Rest], AccNewBySeqEntries, AccRemoveBySeqs) ->
    by_seq_index_entries(Rest, [couch_doc:to_doc_info(NewFullInfo) | AccNewBySeqEntries], AccRemoveBySeqs);
by_seq_index_entries([{#full_doc_info{update_seq=Seq}, #full_doc_info{update_seq=Seq}} | Rest], AccNewBySeqEntries, AccRemoveBySeqs) ->
by_seq_index_entries(Rest, AccNewBySeqEntries, AccRemoveBySeqs);
by_seq_index_entries([{#full_doc_info{update_seq=OldSeq}, NewFullInfo} | Rest], AccNewBySeqEntries, AccRemoveBySeqs) ->
    by_seq_index_entries(Rest,
            [couch_doc:to_doc_info(NewFullInfo) | AccNewBySeqEntries],
            [OldSeq | AccRemoveBySeqs]).


stem_full_doc_infos(#db{revs_limit=Limit}, DocInfos) ->
    [Info#full_doc_info{rev_tree=couch_key_tree:stem(Tree, Limit)} ||
            #full_doc_info{rev_tree=Tree}=Info <- DocInfos].

-spec to_path(#doc_update_info{}) -> path().
to_path(#doc_update_info{revs={Start, RevIds}}=Doc) ->
    [Branch] = to_branch(Doc, lists:reverse(RevIds)),
    {Start - length(RevIds) + 1, Branch}.

-spec to_branch(#doc_update_info{}, [RevId::binary()]) -> [branch()].
to_branch(Doc, [RevId]) ->
    [{RevId, Doc, []}];
to_branch(Doc, [RevId | Rest]) ->
    [{RevId, ?REV_MISSING, to_branch(Doc, Rest)}].


maybe_clobber(false, Tree, #doc_update_info{id = Id}, Client, Rev, _Limit) ->
    send_result(Client, Id, Rev, conflict),
    Tree;

maybe_clobber(true, Tree, #doc_update_info{id = Id} = Doc, Client, Rev, Limit) ->
    {_, {LeafPos, RevIds} = WinPath} = couch_doc:to_doc_info_path(
        #full_doc_info{rev_tree = Tree}),
    NewRevId = new_revid(Doc, WinPath),
    Doc2 = Doc#doc_update_info{revs = {LeafPos + 1, [NewRevId | RevIds]}},
    case couch_key_tree:merge(Tree, to_path(Doc2), Limit) of
    {Tree2, no_conflicts} ->
        send_result(Client, Id, Rev, {ok, {LeafPos + 1, NewRevId}}),
        Tree2;
    _ ->
        send_result(Client, Id, Rev, conflict),
        Tree
    end.


% Used only when the clobber option is used on updates.
new_revid(#doc_update_info{deleted = Del} = Doc, {OldPos, [OldRevId | _]}) ->
    #doc_update_info{
        body_md5 = BodyMd5,
        atts_md5 = AttsMd5
    } = Doc,
    couch_util:md5(?term_to_bin({Del, OldPos, OldRevId, BodyMd5, AttsMd5})).


modify_full_doc_info(Db, Id, MergeConflicts, Clobber, nil, Docs, AccSeq) ->
    modify_full_doc_info(Db, Id, MergeConflicts, Clobber, #full_doc_info{id=Id}, Docs, AccSeq);
modify_full_doc_info(Db, Id, MergeConflicts, Clobber, OldDocInfo,
        NewDocs, AccSeq) ->
    #db{fd=Fd,revs_limit=Limit}=Db,
    #full_doc_info{id=Id,rev_tree=OldTree,deleted=OldDeleted} = OldDocInfo,
    NewRevTree = lists:foldl(
        fun({Client, #doc_update_info{revs={Pos,[_Rev|PrevRevs]}}=NewDoc}, AccTree) ->
            if not MergeConflicts ->
                case couch_key_tree:merge(AccTree, to_path(NewDoc),
                    Limit) of
                {_NewTree, conflicts} when (not OldDeleted) ->
                    maybe_clobber(Clobber, AccTree, NewDoc, Client, {Pos - 1, PrevRevs}, Limit);
                {NewTree, conflicts} when PrevRevs /= [] ->
                    % Check to be sure if prev revision was specified, it's
                    % a leaf node in the tree
                    Leafs = couch_key_tree:get_all_leafs(AccTree),
                    IsPrevLeaf = lists:any(fun({_, {LeafPos, [LeafRevId|_]}}) ->
                            {LeafPos, LeafRevId} == {Pos-1, hd(PrevRevs)}
                        end, Leafs),
                    if IsPrevLeaf ->
                        NewTree;
                    true ->
                        maybe_clobber(Clobber, AccTree, NewDoc, Client, {Pos - 1, PrevRevs}, Limit)
                    end;
                {NewTree, no_conflicts} when  AccTree == NewTree ->
                    % the tree didn't change at all
                    % meaning we are saving a rev that's already
                    % been editted again.
                    if (Pos == 1) and OldDeleted ->
                        % this means we are recreating a brand new document
                        % into a state that already existed before.
                        % put the rev into a subsequent edit of the deletion
                        #doc_info{revs=[#rev_info{rev={OldPos,OldRev}}|_]} =
                                couch_doc:to_doc_info(OldDocInfo),
                        % note for expediencey we are just creating a
                        % a random rev id in this case instead of a deterministic
                        % one.
                        NewRevId = couch_util:md5(integer_to_list(couch_util:rand32())),
                        NewDoc2 = NewDoc#doc_update_info{revs={OldPos + 1, [NewRevId, OldRev]}},
                        {NewTree2, _} = couch_key_tree:merge(AccTree,
                                to_path(NewDoc2), Limit),
                        % we changed the rev id, this tells the caller we did
                        send_result(Client, Id, {Pos-1,PrevRevs},
                                {ok, {OldPos + 1, NewRevId}}),
                        NewTree2;
                    true ->
                        maybe_clobber(Clobber, AccTree, NewDoc, Client, {Pos - 1, PrevRevs}, Limit)
                    end;
                {NewTree, _} ->
                    NewTree
                end;
            true ->
                {NewTree, _} = couch_key_tree:merge(AccTree,
                            to_path(NewDoc), Limit),
                NewTree
            end
        end,
        OldTree, NewDocs),
    if NewRevTree == OldTree ->
        % nothing changed
        {OldDocInfo, AccSeq};
    true ->
        NewSeq = AccSeq+1,
        {FlushedRevTree, LeafsSize} = couch_key_tree:mapfold(
            fun(_Rev, Value, Type, Acc) ->
                case Value of
                #doc_update_info{deleted = IsDeleted, summary = Summary,
                                 fd = SummaryFd, size_atts = SizeAtts} ->
                    % this node value is actually an unwritten document summary,
                    % write to disk.
                    % make sure the Fd in the written bins is the same Fd we are
                    % and convert bins, removing the FD.
                    % All bins should have been written to disk already.
                    case {SummaryFd, Fd} of
                    {nil, _} ->
                        ok;
                    {SameFd, SameFd} ->
                        ok;
                    _ ->
                        % Fd where the attachments were written to, or the body
                        % (when doing optimistic updates), is not the same
                        % as our Fd. This can happen when a database is being
                        % switched out during a compaction.
                        ?LOG_DEBUG("File where the attachments or body were written"
                                " has changed. Possibly retrying.", []),
                        throw(retry)
                    end,
                    case Summary of
                    {SummaryPointer, SummarySize} ->
                        ok;
                    _ when is_list(Summary) orelse is_binary(Summary) ->
                        {ok, SummaryPointer, SummarySize} =
                            couch_file:append_raw_chunk(Fd, Summary)
                    end,
                    TotalSize = SummarySize + SizeAtts,
                    NewValue = {IsDeleted, SummaryPointer, NewSeq, TotalSize},
                    case Type of
                    leaf ->
                        {NewValue, Acc + TotalSize};
                    branch ->
                        {NewValue, Acc}
                    end;
                {_, _, _, LeafSize} when Type =:= leaf, LeafSize =/= nil ->
                    {Value, Acc + LeafSize};
                _ ->
                    {Value, Acc}
                end
            end, 0, NewRevTree),
        NewFullDocInfo = #full_doc_info{
             id = Id,
             update_seq = NewSeq,
             rev_tree = FlushedRevTree,
             leafs_size = LeafsSize
        },
        % get the deleted flag
        #doc_info{revs=[#rev_info{deleted=Deleted}|_]} =
                        couch_doc:to_doc_info(NewFullDocInfo),
        {NewFullDocInfo#full_doc_info{deleted=Deleted}, NewSeq}
    end.



update_docs_int(Db, DocsList, NonRepDocs, MergeConflicts, FullCommit, Clobber) ->
    #db{
        fulldocinfo_by_id_btree = DocInfoByIdBTree,
        docinfo_by_seq_btree = DocInfoBySeqBTree,
        update_seq = LastSeq,
        fd = Fd
        } = Db,
    % lookup up the old documents, if they exist.
    KeyModFuns = lists:map(fun([{_Client, #doc_update_info{id=Id}}|_] = Docs) ->
            {Id, fun(PrevValue, LastSeqAcc) ->
                modify_full_doc_info(Db, Id, MergeConflicts, Clobber, PrevValue, Docs, LastSeqAcc)
            end}
        end, DocsList),

    {ok, KeyResults, NewSeq, DocInfoByIdBTree2} =
            couch_btree:modify(DocInfoByIdBTree, KeyModFuns, LastSeq),

    % Write out the document summaries (the bodies are stored in the nodes of
    % the trees, the attachments are already written to disk)

    % and the indexes
    {NewBySeqEntries, RemoveSeqs} = by_seq_index_entries(KeyResults, [], []),
    InsertBySeq = [begin {K, V} = btree_by_seq_split(I), {insert, K, V} end || I <- NewBySeqEntries],
    RemoveBySeq = [{remove, Seq, nil} || Seq <- lists:sort(RemoveSeqs)],

    {ok, [], DocInfoBySeqBTree2} = couch_btree:query_modify_raw(DocInfoBySeqBTree, RemoveBySeq ++ InsertBySeq),

    {ok, Db2} = update_local_docs(Db, NonRepDocs, Clobber),

    Db3 = Db2#db{
        fulldocinfo_by_id_btree = DocInfoByIdBTree2,
        docinfo_by_seq_btree = DocInfoBySeqBTree2,
        update_seq = NewSeq
        },
    couch_file:flush(Fd),
    % Check if we just updated any design documents, and update the validation
    % funs if we did.
    case lists:any(
        fun(<<"_design/", _/binary>>) -> true; (_) -> false end,
            [Id || [{_Client, #doc_update_info{id=Id}}|_] <- DocsList]) of
    false ->
        Db4 = Db3;
    true ->
        Db4 = refresh_validate_doc_funs(Db3)
    end,

    {ok, commit_data(Db4, not FullCommit)}.


update_local_docs(Db, [], _Clobber) ->
    {ok, Db};
update_local_docs(#db{local_docs_btree=Btree}=Db, Docs, Clobber) ->
    Ids = [Id || {_Client, #doc{id=Id}} <- Docs],
    OldDocLookups = couch_btree:lookup(Btree, Ids),
    BtreeEntries = lists:zipwith(
        fun({Client, #doc{id=Id,deleted=Delete,revs={0,PrevRevs},body=Body}}, OldDocLookup) ->
            case PrevRevs of
            [RevStr|_] ->
                PrevRev0 = list_to_integer(?b2l(RevStr));
            [] ->
                PrevRev0 = 0
            end,
            OldRev =
            case OldDocLookup of
                {ok, {_, {OldRev0, _}}} -> OldRev0;
                not_found -> 0
            end,
            PrevRev = case Clobber of
            true ->
                OldRev;
            false ->
                PrevRev0
            end,
            case OldRev == PrevRev of
            true ->
                case Delete of
                    false ->
                        send_result(Client, Id, {0, PrevRevs}, {ok,
                                {0, ?l2b(integer_to_list(PrevRev + 1))}}),
                        {update, {Id, {PrevRev + 1, Body}}};
                    true  ->
                        send_result(Client, Id, {0, PrevRevs},
                                {ok, {0, <<"0">>}}),
                        {remove, Id}
                end;
            false ->
                send_result(Client, Id, {0, PrevRevs}, conflict),
                ignore
            end
        end, Docs, OldDocLookups),

    BtreeIdsRemove = [Id || {remove, Id} <- BtreeEntries],
    BtreeIdsUpdate = [{Key, Val} || {update, {Key, Val}} <- BtreeEntries],

    {ok, Btree2} =
        couch_btree:add_remove(Btree, BtreeIdsUpdate, BtreeIdsRemove),

    {ok, Db#db{local_docs_btree = Btree2}}.


commit_data(Db) ->
    commit_data(Db, false).

db_to_header(Db, Header) ->
    Header#db_header{
        update_seq = Db#db.update_seq,
        docinfo_by_seq_btree_state = couch_btree:get_state(Db#db.docinfo_by_seq_btree),
        fulldocinfo_by_id_btree_state = couch_btree:get_state(Db#db.fulldocinfo_by_id_btree),
        local_docs_btree_state = couch_btree:get_state(Db#db.local_docs_btree),
        security_ptr = Db#db.security_ptr,
        revs_limit = Db#db.revs_limit}.

commit_data(#db{waiting_delayed_commit=nil} = Db, true) ->
    Db#db{waiting_delayed_commit=erlang:send_after(1000,self(),delayed_commit)};
commit_data(Db, true) ->
    Db;
commit_data(Db, _) ->
    #db{
        fd = Fd,
        header = OldHeader,
        fsync_options = FsyncOptions,
        waiting_delayed_commit = Timer
    } = Db,
    if is_reference(Timer) -> erlang:cancel_timer(Timer); true -> ok end,
    case db_to_header(Db, OldHeader) of
    OldHeader ->
        Db#db{waiting_delayed_commit=nil};
    Header ->
        case lists:member(before_header, FsyncOptions) of
        true -> ok = couch_file:sync(Fd);
        _    -> ok
        end,

        ok = couch_file:write_header(Fd, Header),
        ok = couch_file:flush(Fd),

        case lists:member(after_header, FsyncOptions) of
        true -> ok = couch_file:sync(Fd);
        _    -> ok
        end,

        Db#db{waiting_delayed_commit=nil,
            header=Header,
            committed_update_seq=Db#db.update_seq}
    end.


copy_doc_attachments(#db{fd = SrcFd} = SrcDb, SrcSp, DestFd) ->
    {ok, {BodyData, BinInfos0}} = couch_db:read_doc(SrcDb, SrcSp),
    BinInfos = case BinInfos0 of
    _ when is_binary(BinInfos0) ->
        couch_compress:decompress(BinInfos0);
    _ when is_list(BinInfos0) ->
        % pre 1.2 file format
        BinInfos0
    end,
    % copy the bin values
    NewBinInfos = lists:map(
        fun({Name, Type, BinSp, AttLen, RevPos, Md5}) ->
            % 010 UPGRADE CODE
            {NewBinSp, AttLen, AttLen, Md5, _IdentityMd5} =
                couch_stream:copy_to_new_stream(SrcFd, BinSp, DestFd),
            {Name, Type, NewBinSp, AttLen, AttLen, RevPos, Md5, identity};
        ({Name, Type, BinSp, AttLen, DiskLen, RevPos, Md5, Enc1}) ->
            {NewBinSp, AttLen, _, Md5, _IdentityMd5} =
                couch_stream:copy_to_new_stream(SrcFd, BinSp, DestFd),
            Enc = case Enc1 of
            true ->
                % 0110 UPGRADE CODE
                gzip;
            false ->
                % 0110 UPGRADE CODE
                identity;
            _ ->
                Enc1
            end,
            {Name, Type, NewBinSp, AttLen, DiskLen, RevPos, Md5, Enc}
        end, BinInfos),
    {BodyData, NewBinInfos}.

copy_docs(Db, #db{fd = DestFd} = NewDb, InfoBySeq0, Retry) ->
    % COUCHDB-968, make sure we prune duplicates during compaction
    InfoBySeq = lists:usort(fun(#doc_info{id=A}, #doc_info{id=B}) -> A =< B end,
        InfoBySeq0),
    Ids = [Id || #doc_info{id=Id} <- InfoBySeq],
    ok = couch_file:flush(DestFd),
    LookupResults = couch_btree:lookup(Db#db.fulldocinfo_by_id_btree, Ids),

    NewFullDocInfos1 = lists:map(
        fun({ok, #full_doc_info{rev_tree=RevTree}=Info}) ->
            Info#full_doc_info{rev_tree=couch_key_tree:map(
                fun(_, _, branch) ->
                    ?REV_MISSING;
                (_Rev, LeafVal, leaf) ->
                    IsDel = element(1, LeafVal),
                    Sp = element(2, LeafVal),
                    Seq = element(3, LeafVal),
                    {_Body, AttsInfo} = Summary = copy_doc_attachments(
                        Db, Sp, DestFd),
                    {SummaryChunk, _} = make_doc_summary(NewDb, Summary),
                    {ok, Pos, SummarySize} = couch_file:append_raw_chunk(
                        DestFd, SummaryChunk),
                    TotalLeafSize = lists:foldl(
                        fun({_, _, _, AttLen, _, _, _, _}, S) -> S + AttLen end,
                        SummarySize, AttsInfo),
                    {IsDel, Pos, Seq, TotalLeafSize}
                end, RevTree)}
        end, LookupResults),

    NewFullDocInfos = stem_full_doc_infos(Db, NewFullDocInfos1),
    NewDocInfos = [couch_doc:to_doc_info(Info) || Info <- NewFullDocInfos],
    RemoveSeqs =
    case Retry of
    false ->
        [];
    true ->
        % We are retrying a compaction, meaning the documents we are copying may
        % already exist in our file and must be removed from the by_seq index.
        Existing = couch_btree:lookup(NewDb#db.fulldocinfo_by_id_btree, Ids),
        [Seq || {ok, #full_doc_info{update_seq=Seq}} <- Existing]
    end,

    {ok, DocInfoBTree} = couch_btree:add_remove(
            NewDb#db.docinfo_by_seq_btree, NewDocInfos, RemoveSeqs),
    {ok, FullDocInfoBTree} = couch_btree:add_remove(
            NewDb#db.fulldocinfo_by_id_btree, NewFullDocInfos, []),
    NewDb#db{ fulldocinfo_by_id_btree=FullDocInfoBTree,
              docinfo_by_seq_btree=DocInfoBTree}.



copy_compact(Db, NewDb0, Retry) ->
    FsyncOptions = [Op || Op <- NewDb0#db.fsync_options, Op == before_header],
    NewDb = NewDb0#db{fsync_options=FsyncOptions},
    ok = couch_file:flush(Db#db.fd),
    ok = couch_file:flush(NewDb#db.fd),
    TotalChanges = couch_db:count_changes_since(Db, NewDb#db.update_seq),
    BufferSize = list_to_integer(
        couch_config:get("database_compaction", "doc_buffer_size", "524288")),
    CheckpointAfter = couch_util:to_integer(
        couch_config:get("database_compaction", "checkpoint_after",
            BufferSize * 10)),

    EnumBySeqFun =
    fun(#doc_info{high_seq=Seq}=DocInfo, _Offset,
        {AccNewDb, AccUncopied, AccUncopiedSize, AccCopiedSize, TotalCopied}) ->

        AccUncopiedSize2 = AccUncopiedSize + ?term_size(DocInfo),
        if AccUncopiedSize2 >= BufferSize ->
            NewDb2 = copy_docs(
                Db, AccNewDb, lists:reverse([DocInfo | AccUncopied]), Retry),
            TotalCopied2 = TotalCopied + 1 + length(AccUncopied),
            couch_task_status:update("Copied ~p of ~p changes (~p%)",
                [TotalCopied2, TotalChanges, (TotalCopied2 * 100) div TotalChanges]),
            AccCopiedSize2 = AccCopiedSize + AccUncopiedSize2,
            if AccCopiedSize2 >= CheckpointAfter ->
                {ok, {commit_data(NewDb2#db{update_seq = Seq}), [],
                    0, 0, TotalCopied2}};
            true ->
                {ok, {NewDb2#db{update_seq = Seq}, [],
                    0, AccCopiedSize2, TotalCopied2}}
            end;
        true ->
            {ok, {AccNewDb, [DocInfo | AccUncopied], AccUncopiedSize2,
                AccCopiedSize, TotalCopied}}
        end
    end,

    couch_task_status:set_update_frequency(500),

    {ok, _, {NewDb2, Uncopied, _, _, ChangesDone}} =
        couch_btree:foldl(Db#db.docinfo_by_seq_btree, EnumBySeqFun,
            {NewDb, [], 0, 0, 0},
            [{start_key, NewDb#db.update_seq + 1}]),

    couch_task_status:update("Flushing"),

    NewDb3 = copy_docs(Db, NewDb2, lists:reverse(Uncopied), Retry),
    TotalChanges = ChangesDone + length(Uncopied),

    % copy misc header values
    if NewDb3#db.security /= Db#db.security ->
        {ok, Ptr, _} = couch_file:append_term(
            NewDb3#db.fd, Db#db.security,
            [{compression, NewDb3#db.compression}]),
        NewDb4 = NewDb3#db{security=Db#db.security, security_ptr=Ptr};
    true ->
        NewDb4 = NewDb3
    end,

    ok = couch_file:flush(NewDb4#db.fd),
    commit_data(NewDb4#db{update_seq=Db#db.update_seq}).

start_copy_compact(#db{name=Name,filepath=Filepath,header=#db_header{purge_seq=PurgeSeq}}=Db) ->
    CompactFile = Filepath ++ ".compact",
    ?LOG_DEBUG("Compaction process spawned for db \"~s\"", [Name]),
    case couch_file:open(CompactFile) of
    {ok, Fd} ->
        couch_task_status:add_task(<<"Database Compaction">>, <<Name/binary, " retry">>, <<"Starting">>),
        Retry = true,
        case couch_file:read_header(Fd) of
        {ok, Header} ->
            ok;
        no_valid_header ->
            ok = couch_file:write_header(Fd, Header=#db_header{})
        end;
    {error, enoent} ->
        couch_task_status:add_task(<<"Database Compaction">>, Name, <<"Starting">>),
        {ok, Fd} = couch_file:open(CompactFile, [create]),
        Retry = false,
        ok = couch_file:write_header(Fd, Header=#db_header{})
    end,
    NewDb = init_db(Name, CompactFile, Fd, Header, Db#db.options),
    NewDb2 = if PurgeSeq > 0 ->
        {ok, PurgedIdsRevs} = couch_db:get_last_purged(Db),
        {ok, Pointer, _} = couch_file:append_term(
            Fd, PurgedIdsRevs, [{compression, NewDb#db.compression}]),
        NewDb#db{header=Header#db_header{purge_seq=PurgeSeq, purged_docs=Pointer}};
    true ->
        NewDb
    end,
    unlink(Fd),

    NewDb3 = copy_compact(Db, NewDb2, Retry),
    close_db(NewDb3),
    case gen_server:call(
        Db#db.update_pid, {compact_done, CompactFile}, infinity) of
    ok ->
        ok;
    {retry, CurrentDb} ->
        start_copy_compact(CurrentDb)
    end.

make_doc_summary(#db{compression = Comp}, {Body0, Atts0}) ->
    Body = case couch_compress:is_compressed(Body0) of
    true ->
        Body0;
    false ->
        % pre 1.2 database file format
        couch_compress:compress(Body0, Comp)
    end,
    Atts = case couch_compress:is_compressed(Atts0) of
    true ->
        Atts0;
    false ->
        couch_compress:compress(Atts0, Comp)
    end,
    SummaryBin = ?term_to_bin({Body, Atts}),
    Summary = couch_file:assemble_file_chunk(
        SummaryBin, couch_util:md5(SummaryBin)),
    {Summary, Body}.
