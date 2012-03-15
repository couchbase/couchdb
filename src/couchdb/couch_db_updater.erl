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
    {ok, Db#db{main_pid = MainPid}}.


terminate(_Reason, Db) ->
    couch_util:shutdown_sync(Db#db.compactor_info),
    case (catch couch_file:only_snapshot_reads(Db#db.fd)) of
    ok -> ok;
    Error ->
        ?LOG_ERROR("Got error trying to silence couch_file: ~p~n~p", [Error, Db])
    end,
    ok.

handle_call(get_db, _From, Db) ->
    {reply, {ok, Db}, Db};
handle_call(full_commit, _From, #db{waiting_delayed_commit=nil}=Db) ->
    {reply, ok, Db}; % no data waiting, return ok immediately
handle_call(full_commit, _From,  Db) ->
    {reply, ok, commit_data(Db)}; % commit the data and return ok
handle_call(increment_update_seq, _From, Db) ->
    Db2 = commit_data(Db#db{update_seq=Db#db.update_seq+1}),
    ok = notify_db_updated(Db2),
    couch_db_update_notifier:notify({updated, Db#db.name}),
    {reply, {ok, Db2#db.update_seq}, Db2};

handle_call({set_security, NewSec}, _From, Db) ->
    {ok, Ptr, _} = couch_file:append_term(Db#db.fd, NewSec),
    Db2 = commit_data(Db#db{security=NewSec, security_ptr=Ptr,
            update_seq=Db#db.update_seq+1}),
    ok = notify_db_updated(Db2),
    {reply, ok, Db2};

handle_call({purge_docs, _IdRevs}, _From,
        #db{compactor_info=Pid}=Db) when Pid /= nil ->
    {reply, {error, purge_during_compaction}, Db};
handle_call({purge_docs, IdRevs}, _From, Db) ->
    #db{
        fd = Fd,
        docinfo_by_id_btree = DocInfoByIdBTree,
        docinfo_by_seq_btree = DocInfoBySeqBTree,
        update_seq = LastSeq,
        header = Header = #db_header{purge_seq=PurgeSeq}
        } = Db,
    DocLookups = couch_btree:lookup(DocInfoByIdBTree,
            [Id || {Id, _Rev} <- IdRevs]),

    NewDocInfos = lists:zipwith(
        fun({_Id, Rev}, {ok, #doc_info{rev=DiskRev}=DocInfo}) ->
            case Rev of
            DiskRev ->
                DocInfo;
            _ ->
                nil
            end;
        (_, not_found) ->
            nil
        end,
        IdRevs, DocLookups),

    SeqsToRemove = [Seq
            || #doc_info{local_seq=Seq} <- NewDocInfos],

    IdRevsPurged = [{Id, [Rev]}
            || #doc_info{id=Id, rev=Rev} <- NewDocInfos],

    IdsToRemove = [Id || #doc_info{id=Id} <- NewDocInfos],

    {ok, DocInfoBySeqBTree2} = couch_btree:add_remove(DocInfoBySeqBTree,
            [], SeqsToRemove),
    {ok, DocInfoByIdBTree2} = couch_btree:add_remove(DocInfoByIdBTree,
            [], IdsToRemove),
    {ok, Pointer, _} = couch_file:append_term(Fd, IdRevsPurged),

    Db2 = commit_data(
        Db#db{
            docinfo_by_id_btree = DocInfoByIdBTree2,
            docinfo_by_seq_btree = DocInfoBySeqBTree2,
            update_seq = LastSeq + 1,
            header=Header#db_header{purge_seq=PurgeSeq+1, purged_docs=Pointer}}),

    ok = notify_db_updated(Db2),
    couch_db_update_notifier:notify({updated, Db#db.name}),
    {reply, {ok, (Db2#db.header)#db_header.purge_seq, IdRevsPurged}, Db2};
handle_call(start_compact, _From, Db) ->
    case Db#db.compactor_info of
    nil ->
        ?LOG_INFO("Starting compaction for db \"~s\"", [Db#db.name]),
        Pid = spawn_link(fun() -> start_copy_compact(Db) end),
        Db2 = Db#db{compactor_info=Pid},
        ok = notify_db_updated(Db2),
        {reply, {ok, Pid}, Db2};
    _ ->
        % compact currently running, this is a no-op
        {reply, {ok, Db#db.compactor_info}, Db}
    end;
handle_call(cancel_compact, _From, #db{compactor_info = nil} = Db) ->
    {reply, ok, Db};
handle_call(cancel_compact, _From, #db{compactor_info = Pid} = Db) ->
    unlink(Pid),
    exit(Pid, kill),
    RootDir = couch_config:get("couchdb", "database_dir", "."),
    ok = couch_file:delete(RootDir, Db#db.filepath ++ ".compact"),
    {reply, ok, Db#db{compactor_info = nil}};


handle_call({compact_done, CompactFilepath}, _From, Db) ->
    #db{filepath = Filepath, fd = OldFd} = Db,
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
        NewFilePath = increment_filepath(Filepath),
        NewDb2 = commit_data(NewDb#db{
            local_docs_btree = NewLocalBtree,
            main_pid = Db#db.main_pid,
            filepath = NewFilePath,
            instance_start_time = Db#db.instance_start_time
        }),

        ?LOG_INFO("CouchDB swapping files ~s and ~s.",
                [NewFilePath, CompactFilepath]),
        % ensure the fd won't close, because after we delete and close,
        % it can't reopen
        ok = couch_file:set_close_after(OldFd, infinity),
        RootDir = couch_config:get("couchdb", "database_dir", "."),
        ok = notify_db_updated(NewDb2),
        ok = couch_file:only_snapshot_reads(OldFd), % prevent writes to the fd
        close_db(Db),
        ok = couch_file:rename(NewFd, NewFilePath),
        ok = couch_file:sync(NewFd),
        ok = couch_file:set_close_after(NewFd, ?FD_CLOSE_TIMEOUT_MS),
        couch_file:delete(RootDir, Filepath),
        couch_db_update_notifier:notify({compacted, NewDb2#db.name}),
        ?LOG_INFO("Compaction for db \"~s\" completed.", [Db#db.name]),
        {reply, ok, NewDb2#db{compactor_info=nil}};
    false ->
        ?LOG_INFO("Compaction file still behind main file "
            "(update seq=~p. compact update seq=~p). Retrying.",
            [Db#db.update_seq, NewSeq]),
        close_db(NewDb),
        {reply, {retry, Db}, Db}
    end;

handle_call({update_header_pos, FileVersion, NewPos}, _From, Db) ->
    ExistingFileVersion = file_version(Db#db.filepath),
    if FileVersion == ExistingFileVersion ->
        case couch_file:read_header(Db#db.fd, NewPos) of
        {ok, NewHeader} ->
            % disable any more writes, as we are being updated externally!
            ok = couch_file:only_snapshot_reads(Db#db.fd),
            % previous call sets close after timeout to infinity.
            ok = couch_file:set_close_after(Db#db.fd, ?FD_CLOSE_TIMEOUT_MS),
            if Db#db.update_seq > NewHeader#db_header.update_seq ->
                {reply, update_behind_couchdb, Db};
            true ->
                NewDb = populate_db_from_header(Db, NewHeader),
                ok = notify_db_updated(NewDb),
                couch_db_update_notifier:notify({updated, Db#db.name}),
                {reply, ok, NewDb}
            end;
        Error ->
            {reply, Error, Db}
        end;
    FileVersion < ExistingFileVersion ->
        {reply, retry_new_file_version, Db};
    true ->
        {reply, update_file_ahead_of_couchdb, Db}
    end.


handle_cast(Msg, #db{name = Name} = Db) ->
    ?LOG_ERROR("Database `~s` updater received unexpected cast: ~p", [Name, Msg]),
    {stop, Msg, Db}.


handle_info({update_docs, Client, Docs, NonRepDocs, FullCommit}, Db) ->
    try update_docs_int(Db, Docs, NonRepDocs, FullCommit) of
    {ok, Db2} ->
        ok = notify_db_updated(Db2),
        if Db2#db.update_seq /= Db#db.update_seq ->
            couch_db_update_notifier:notify({updated, Db2#db.name});
        true -> ok
        end,
        catch(Client ! {done, self()}),
        {noreply, Db2}
    catch
        throw: retry ->
            catch(Client ! {retry, self()}),
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
            ok = notify_db_updated(Db2),
            {noreply, Db2}
    end;
handle_info({'EXIT', _Pid, normal}, Db) ->
    {noreply, Db};
handle_info({'EXIT', _Pid, Reason}, Db) ->
    {stop, Reason, Db}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


file_version(FilePath) ->
    Tokens = string:tokens(FilePath, "."),
    list_to_integer(lists:last(Tokens)).

increment_filepath(FilePath) ->
    Tokens = string:tokens(FilePath, "."),
    NumStr = integer_to_list(list_to_integer(lists:last(Tokens)) + 1),
    string:join(lists:sublist(Tokens, length(Tokens) - 1) ++ [NumStr], ".").

btree_by_seq_split(#doc_info{id=Id, local_seq=Seq, rev=Rev,
        deleted=Deleted, body_ptr=Bp, content_meta=Meta, size=Size}) ->
    {Seq, {Id, Rev, Bp, if Deleted -> 1; true -> 0 end, Meta, Size}}.

btree_by_seq_join(Seq, {Id, Rev, Bp, Deleted, Meta, Size}) ->
    #doc_info{
        id = Id,
        local_seq = Seq,
        rev = Rev,
        deleted = (Deleted == 1),
        content_meta = Meta,
        body_ptr = Bp,
        size = Size}.

btree_by_id_split(#doc_info{id=Id, local_seq=Seq, rev=Rev,
        deleted=Deleted, body_ptr=Bp, content_meta=Meta, size=Size}) ->
    {Id, {Seq, Rev, Bp, if Deleted -> 1; true -> 0 end, Meta, Size}}.

btree_by_id_join(Id, {Seq, Rev, Bp, Deleted, Meta, Size}) ->
    #doc_info{
        id = Id,
        local_seq = Seq,
        rev = Rev,
        deleted = (Deleted == 1),
        body_ptr = Bp,
        content_meta = Meta,
        size = Size}.

btree_by_id_reduce(reduce, DocInfos) ->
    lists:foldl(
        fun(Info, {NotDeleted, Deleted, Size}) ->
            case Info#doc_info.deleted of
            true ->
                {NotDeleted, Deleted + 1, Size + Info#doc_info.size};
            false ->
                {NotDeleted + 1, Deleted, Size + Info#doc_info.size}
            end
        end,
        {0, 0, 0}, DocInfos);
btree_by_id_reduce(rereduce, Reds) ->
    lists:foldl(
        fun({NotDeleted, Deleted, Size}, {AccNotDeleted, AccDeleted, AccSize}) ->
            {AccNotDeleted + NotDeleted, AccDeleted + Deleted, AccSize + Size}
        end,
        {0, 0, 0}, Reds).

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

init_db(DbName, Filepath, Fd, Header, Options) ->

    {ok, FsyncOptions} = couch_util:parse_term(
            couch_config:get("couchdb", "fsync_options",
                    "[before_header, after_header, on_file_open]")),

    case lists:member(on_file_open, FsyncOptions) of
    true -> ok = couch_file:sync(Fd);
    _ -> ok
    end,
    % convert start time tuple to microsecs and store as a binary string
    {MegaSecs, Secs, MicroSecs} = now(),
    StartTime = ?l2b(io_lib:format("~p",
            [(MegaSecs*1000000*1000000) + (Secs*1000000) + MicroSecs])),
    {ok, RefCntr} = couch_ref_counter:start([Fd]),
    Db = #db{
        update_pid=self(),
        fd = Fd,
        fd_ref_counter = RefCntr,
        name = DbName,
        filepath = Filepath,
        instance_start_time = StartTime,
        fsync_options = FsyncOptions,
        options = Options
        },
    populate_db_from_header(Db, Header).


close_db(#db{fd_ref_counter = RefCntr}) ->
    couch_ref_counter:drop(RefCntr).


populate_db_from_header(Db, NewHeader) ->
    Header1 = simple_upgrade_record(NewHeader, #db_header{}),
    Header =
    case element(2, Header1) of
    ?LATEST_DISK_VERSION -> Header1;
    _ -> throw({database_disk_version_error, "Incorrect disk header version"})
    end,

    {ok, IdBtree} = couch_btree:open(Header#db_header.docinfo_by_id_btree_state,
            Db#db.fd,
            [{split, fun(X) -> btree_by_id_split(X) end},
            {join, fun(X,Y) -> btree_by_id_join(X,Y) end},
            {reduce, fun(X,Y) -> btree_by_id_reduce(X,Y) end}]),
    {ok, SeqBtree} = couch_btree:open(Header#db_header.docinfo_by_seq_btree_state,
            Db#db.fd,
            [{split, fun(X) -> btree_by_seq_split(X) end},
            {join, fun(X,Y) -> btree_by_seq_join(X,Y) end},
            {reduce, fun(X,Y) -> btree_by_seq_reduce(X,Y) end}]),
    {ok, LocalDocsBtree} = couch_btree:open(Header#db_header.local_docs_btree_state,
        Db#db.fd, []),
    case Header#db_header.security_ptr of
    nil ->
        Security = [],
        SecurityPtr = nil;
    SecurityPtr ->
        {ok, Security} = couch_file:pread_term(Db#db.fd, SecurityPtr)
    end,
    Db#db{
        header=Header,
        docinfo_by_id_btree = IdBtree,
        docinfo_by_seq_btree = SeqBtree,
        local_docs_btree = LocalDocsBtree,
        committed_update_seq = Header#db_header.update_seq,
        update_seq = Header#db_header.update_seq,
        security = Security,
        security_ptr = SecurityPtr
        }.

update_docs_int(Db, DocsList, NonRepDocs, FullCommit) ->
    #db{
        docinfo_by_id_btree = DocInfoByIdBTree,
        docinfo_by_seq_btree = DocInfoBySeqBTree,
        update_seq = LastSeq,
        fd = Fd
        } = Db,
    {NewDocInfos, NewSeq} =
        lists:mapfoldl(fun(DocUpdate, SeqAcc0)->
            SeqAcc = SeqAcc0 + 1,
            #doc_update_info{
                id=Id,
                rev=Rev,
                body_ptr=Bp,
                deleted=Deleted,
                size=Size,
                content_meta=Meta,
                fd=DocFd
                } = DocUpdate,
            if Fd /= DocFd ->
                throw(retry);
            true -> ok
            end,
            {#doc_info{
                id=Id,
                rev=Rev,
                body_ptr=Bp,
                content_meta=Meta,
                deleted=Deleted,
                local_seq=SeqAcc,
                size=Size
                }, SeqAcc}
        end, LastSeq, DocsList),
    RawKeys =
        lists:flatmap(fun(DocInfo) ->
            {K, V} = btree_by_id_split(DocInfo),
            [{fetch, K, nil}, {insert, K, V}]
        end, NewDocInfos),
    {ok, OldInfos, DocInfoByIdBTree2} = couch_btree:query_modify_raw(
            DocInfoByIdBTree, RawKeys),

    OldSeqs = [OldSeq || {ok, #doc_info{local_seq=OldSeq}} <- OldInfos],
    RemoveBySeq = [{remove, Seq, nil} || Seq <- lists:sort(OldSeqs)],
    InsertBySeq = lists:map(fun(DocInfo) ->
            {K, V} = btree_by_seq_split(DocInfo),
            {insert, K, V}
        end, NewDocInfos),
    {ok, [], DocInfoBySeqBTree2} = couch_btree:query_modify_raw(DocInfoBySeqBTree, RemoveBySeq ++ InsertBySeq),

    {ok, Db2} = update_local_docs(Db, NonRepDocs),

    Db3 = Db2#db{
        docinfo_by_id_btree = DocInfoByIdBTree2,
        docinfo_by_seq_btree = DocInfoBySeqBTree2,
        update_seq = NewSeq
        },
    couch_file:flush(Fd),

    {ok, commit_data(Db3, not FullCommit)}.


update_local_docs(Db, []) ->
    {ok, Db};
update_local_docs(#db{local_docs_btree=Btree}=Db, Docs) ->
    KVsAdd = [{Id, if is_tuple(Body) -> ?JSON_ENCODE(Body); true -> Body end} ||
            #doc{id=Id, deleted=false, body=Body} <- Docs],
    IdsRemove = [Id || #doc{id=Id, deleted=true} <- Docs],
    {ok, Btree2} =
        couch_btree:add_remove(Btree, KVsAdd, IdsRemove),

    {ok, Db#db{local_docs_btree = Btree2}}.


commit_data(Db) ->
    commit_data(Db, false).

db_to_header(Db, Header) ->
    Header#db_header{
        update_seq = Db#db.update_seq,
        docinfo_by_seq_btree_state = couch_btree:get_state(Db#db.docinfo_by_seq_btree),
        docinfo_by_id_btree_state = couch_btree:get_state(Db#db.docinfo_by_id_btree),
        local_docs_btree_state = couch_btree:get_state(Db#db.local_docs_btree),
        security_ptr = Db#db.security_ptr}.

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


copy_docs(#db{fd = SrcFd}, #db{fd = DestFd} = NewDb, Infos, Retry) ->
    NewInfos = lists:map(fun(#doc_info{body_ptr=Bp} = DocInfo) ->
            {ok, Body} = couch_file:pread_iolist(SrcFd, Bp),
            {ok, BpNew, _} = couch_file:append_binary_crc32(DestFd, Body),
            DocInfo#doc_info{body_ptr = BpNew}
        end, Infos),

    RemoveSeqs =
    case Retry of
    false ->
        [];
    true ->
        % We are retrying a compaction, meaning the documents we are copying may
        % already exist in our file and must be removed from the by_seq index.
        Ids = [Id || #doc_info{id=Id} <- NewInfos],
        Existing = couch_btree:lookup(NewDb#db.docinfo_by_id_btree, Ids),
        [Seq || {ok, #doc_info{local_seq=Seq}} <- Existing]
    end,

    {ok, DocInfoBTree} = couch_btree:add_remove(
            NewDb#db.docinfo_by_seq_btree, NewInfos, RemoveSeqs),
    {ok, FullDocInfoBTree} = couch_btree:add_remove(
            NewDb#db.docinfo_by_id_btree, NewInfos, []),
    update_compact_task(length(NewInfos)),
    NewDb#db{ docinfo_by_id_btree=FullDocInfoBTree,
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
    fun(#doc_info{local_seq=Seq}=DocInfo, _Offset,
        {AccNewDb, AccUncopied, AccUncopiedSize, AccCopiedSize}) ->

        AccUncopiedSize2 = AccUncopiedSize + ?term_size(DocInfo),
        if AccUncopiedSize2 >= BufferSize ->
            NewDb2 = copy_docs(
                Db, AccNewDb, lists:reverse([DocInfo | AccUncopied]), Retry),
            AccCopiedSize2 = AccCopiedSize + AccUncopiedSize2,
            if AccCopiedSize2 >= CheckpointAfter ->
                {ok, {commit_data(NewDb2#db{update_seq = Seq}), [], 0, 0}};
            true ->
                {ok, {NewDb2#db{update_seq = Seq}, [], 0, AccCopiedSize2}}
            end;
        true ->
            {ok, {AccNewDb, [DocInfo | AccUncopied], AccUncopiedSize2,
                AccCopiedSize}}
        end
    end,

    TaskProps0 = [
        {type, database_compaction},
        {database, Db#db.name},
        {progress, 0},
        {changes_done, 0},
        {total_changes, TotalChanges}
    ],
    case Retry and couch_task_status:is_task_added() of
    true ->
        couch_task_status:update([
            {retry, true},
            {progress, 0},
            {changes_done, 0},
            {total_changes, TotalChanges}
        ]);
    false ->
        couch_task_status:add_task(TaskProps0),
        couch_task_status:set_update_frequency(500)
    end,

    case Retry of
    false ->
        NewDb3 = initial_copy_compact(Db, NewDb);
    true ->
        {ok, _, {NewDb2, Uncopied, _, _}} =
            couch_btree:foldl(Db#db.docinfo_by_seq_btree, EnumBySeqFun,
                {NewDb, [], 0, 0},
                [{start_key, NewDb#db.update_seq + 1}]),
        NewDb3 = copy_docs(Db, NewDb2, lists:reverse(Uncopied), Retry)
    end,

    TotalChanges = couch_task_status:get(changes_done),

    % copy misc header values
    if NewDb3#db.security /= Db#db.security ->
        {ok, Ptr, _} = couch_file:append_term(NewDb3#db.fd, Db#db.security),
        NewDb4 = NewDb3#db{security=Db#db.security, security_ptr=Ptr};
    true ->
        NewDb4 = NewDb3
    end,
    ok = couch_file:flush(NewDb4#db.fd),
    commit_data(NewDb4#db{update_seq=Db#db.update_seq}).


initial_copy_compact(#db{docinfo_by_seq_btree=SrcBySeq,
        docinfo_by_id_btree=SrcById, fd=SrcFd},
        #db{docinfo_by_seq_btree=DestBySeq,
        docinfo_by_id_btree=DestById, fd=DestFd} = NewDb) ->
    CopyBodyFun = fun(#doc_info{body_ptr=Bp}=Info, ok) ->
        {ok, Body} = couch_file:pread_iolist(SrcFd, Bp),
        {ok, BpNew, _} = couch_file:append_binary_crc32(DestFd, Body),
        update_compact_task(1),
        {Info#doc_info{body_ptr = BpNew}, ok}
    end,
    % first copy the by_seq index and the values.
    {ok, NewBySeqRoot, ok} = couch_btree_copy:copy(
            SrcBySeq, DestFd, [{before_kv_write, {CopyBodyFun, ok}}]),
    % now dump the new by_seq to a temp file, sort and output to new file
    ok = couch_file:flush(DestFd),
    DbRootDir = couch_config:get("couchdb", "database_dir", "."),
    {A,B,C}=now(),
    TempName = lists:flatten(io_lib:format("~p.~p.~p",[A,B,C])),
    TempDir = couch_file:get_delete_dir(DbRootDir),
    TempFilepath = filename:join(TempDir, TempName),
    {ok, TempFd} = file:open(TempFilepath, [raw, delayed_write, append]),
    NewBySeqBtree = DestBySeq#btree{root=NewBySeqRoot},
    {ok, _, ok} = couch_btree:foldl(NewBySeqBtree, fun(DocInfo,_Offset,ok) ->
            Bin = term_to_binary(DocInfo),
            Size = size(Bin),
            ok = file:write(TempFd, [<<Size:32, Bin/binary>>]),
            {ok, ok}
        end,
        ok,
        []),
    ok = file:close(TempFd),
    BtreeOutputFun = couch_btree_copy:file_sort_output_fun(
            SrcById, DestFd, []),
    % no need to specify a sort function, the built in erlang sort will
    % sort the terms by the first differing slot, which is id
    {ok, NewByIdRoot} = file_sorter:sort([TempFilepath],
            BtreeOutputFun,[{tmpdir, TempDir}]),
    ok = file:delete(TempFilepath),
    ok = couch_file:flush(DestFd),
    NewDb#db{docinfo_by_seq_btree=NewBySeqBtree,
            docinfo_by_id_btree=DestById#btree{root=NewByIdRoot}}.

start_copy_compact(#db{name=Name,filepath=Filepath,header=#db_header{purge_seq=PurgeSeq}}=Db) ->
    CompactFile = Filepath ++ ".compact",
    ?LOG_DEBUG("Compaction process spawned for db \"~s\"", [Name]),
    case couch_file:open(CompactFile) of
    {ok, Fd} ->
        Retry = true,
        case couch_file:read_header(Fd) of
        {ok, Header} ->
            ok;
        no_valid_header ->
            ok = couch_file:write_header(Fd, Header=#db_header{})
        end;
    {error, enoent} ->
        {ok, Fd} = couch_file:open(CompactFile, [create]),
        Retry = false,
        ok = couch_file:write_header(Fd, Header=#db_header{})
    end,
    NewDb = init_db(Name, CompactFile, Fd, Header, Db#db.options),
    NewDb2 = if PurgeSeq > 0 ->
        {ok, PurgedIdsRevs} = couch_db:get_last_purged(Db),
        {ok, Pointer, _} = couch_file:append_term(Fd, PurgedIdsRevs),
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

update_compact_task(NumChanges) ->
    [Changes, Total] = couch_task_status:get([changes_done, total_changes]),
    Changes2 = Changes + NumChanges,
    Progress = case Total of
    0 ->
        0;
    _ ->
        (Changes2 * 100) div Total
    end,
    couch_task_status:update([{changes_done, Changes2}, {progress, Progress}]).


notify_db_updated(NewDb) ->
    Ref = make_ref(),
    NewDb#db.main_pid ! {db_updated, Ref, NewDb},
    receive
    {ok, Ref} ->
        ok;
    {'EXIT', Pid, Reason} when Pid =:= NewDb#db.main_pid ->
        exit(Reason)
    end.
