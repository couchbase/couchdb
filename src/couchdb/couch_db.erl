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

-module(couch_db).
-behaviour(gen_server).

-export([open/2,open_int/2,close/1,create/2,get_db_info/1,get_design_docs/1]).
-export([start_compact/1, cancel_compact/1,get_design_docs/2]).
-export([open_ref_counted/2,is_idle/1,monitor/1,count_changes_since/2]).
-export([update_doc/2,update_doc/3,update_header_pos/3]).
-export([update_docs/2,update_docs/3]).
-export([get_doc_info/2,open_doc/2,open_doc/3]).
-export([get_missing_revs/2,name/1,get_update_seq/1,get_committed_update_seq/1]).
-export([enum_docs/4,enum_docs_since/5]).
-export([enum_docs_since_reduce_to_count/1,enum_docs_reduce_to_count/1]).
-export([increment_update_seq/1,get_purge_seq/1,purge_docs/2,get_last_purged/1]).
-export([start_link/3,open_doc_int/3,ensure_full_commit/1]).
-export([set_security/2,get_security/1]).
-export([init/1,terminate/2,handle_call/3,handle_cast/2,code_change/3,handle_info/2]).
-export([changes_since/4,changes_since/5]).
-export([check_is_admin/1, check_is_member/1]).
-export([reopen/1,get_current_seq/1,fast_reads/2,get_trailing_file_num/1]).
-export([add_update_listener/3, remove_update_listener/2]).
-export([random_doc_info/1]).

-include("couch_db.hrl").

start_link(DbName, Filepath, Options) ->
    case open_db_file(Filepath, Options) of
    {ok, Fd, RealFilePath} ->
        StartResult = gen_server:start_link(couch_db, {DbName, RealFilePath, Fd, Options}, []),
        unlink(Fd),
        StartResult;
    Else ->
        Else
    end.

get_trailing_file_num(FileName) ->
    list_to_integer(lists:last(string:tokens(FileName, "."))).


open_db_file(Filepath, Options) ->
    case filelib:wildcard(Filepath ++ ".*") of
    [] ->
        case lists:member(create, Options) of
        true ->
            case couch_file:open(Filepath ++ ".1",
                    [{fd_close_after, ?FD_CLOSE_TIMEOUT_MS} | Options]) of
            {ok, Fd} ->
                {ok, Fd, Filepath ++ ".1"};
            Error ->
                Error
            end;
        false ->
            {not_found, no_db_file}
        end;
    MatchingFiles ->
        {CompactFiles, MatchingFiles2} =
            lists:partition(
                fun(FileName) ->
                    "compact" == lists:last(string:tokens(FileName, "."))
                end, MatchingFiles),
        case MatchingFiles2 of
        [] ->
            [file:delete(F) || F <- CompactFiles],
            case lists:member(create, Options) of
            true ->
                % we had some compaction files hanging around, now retry
                open_db_file(Filepath, Options);
            false ->
                {not_found, no_db_file}
            end;
        [ _ | _ ] ->
            % parse out the trailing #s and sort highest to lowest
            [NewestFile | RestOld] = lists:sort(fun(A,B) ->
                get_trailing_file_num(A) > get_trailing_file_num(B)
            end, MatchingFiles2),
            case couch_file:open(NewestFile,
                    [{fd_close_after, ?FD_CLOSE_TIMEOUT_MS} | Options]) of
            {ok, Fd} ->
                % delete the old files
                [file:delete(F) || F <- RestOld ++ CompactFiles],
                {ok, Fd, NewestFile};
            Error ->
                Error
            end
        end
    end.


create(DbName, Options) ->
    couch_server:create(DbName, Options).

% this is for opening a database for internal purposes like the replicator
% or the view indexer. it never throws a reader error.
open_int(DbName, Options) ->
    couch_server:open(DbName, Options).

% this should be called anytime an http request opens the database.
% it ensures that the http userCtx is a valid reader
open(DbName, Options) ->
    case couch_server:open(DbName, Options) of
        {ok, Db} ->
            try
                check_is_member(Db),
                {ok, Db}
            catch
                throw:Error ->
                    close(Db),
                    throw(Error)
            end;
        Else -> Else
    end.

reopen(#db{name = DbName, user_ctx = UserCtx} = Db) ->
    Result = open(DbName, [{user_ctx, UserCtx}]),
    ok = close(Db),
    Result.

get_current_seq(#db{main_pid = Pid}) ->
    gen_server:call(Pid, get_current_seq, infinity).

ensure_full_commit(#db{update_pid=UpdatePid,instance_start_time=StartTime}) ->
    ok = gen_server:call(UpdatePid, full_commit, infinity),
    {ok, StartTime}.

close(#db{fd_ref_counter=RefCntr}) ->
    catch couch_ref_counter:drop(RefCntr).

update_header_pos(#db{update_pid=Pid}, FileVersion, NewPos) ->
    gen_server:call(Pid, {update_header_pos, FileVersion, NewPos}, infinity).

open_ref_counted(MainPid, OpenedPid) ->
    gen_server:call(MainPid, {open_ref_count, OpenedPid}, infinity).

is_idle(#db{main_pid = MainPid}) ->
    is_idle(MainPid);
is_idle(MainPid) ->
    gen_server:call(MainPid, is_idle, infinity).

monitor(#db{main_pid=MainPid}) ->
    erlang:monitor(process, MainPid).

start_compact(#db{update_pid=Pid}) ->
    gen_server:call(Pid, start_compact, infinity).

cancel_compact(#db{update_pid=Pid}) ->
    gen_server:call(Pid, cancel_compact, infinity).

open_doc(Db, IdOrDocInfo) ->
    open_doc(Db, IdOrDocInfo, []).

open_doc(Db, Id, Options) ->
    case open_doc_int(Db, Id, Options) of
    {ok, #doc{deleted=true}=Doc} ->
        case lists:member(deleted, Options) of
        true ->
            apply_open_options({ok, Doc},Options);
        false ->
            {not_found, deleted}
        end;
    Else ->
        apply_open_options(Else,Options)
    end.

apply_open_options({ok, Doc},Options) ->
    apply_open_options2(Doc,Options);
apply_open_options(Else,_Options) ->
    Else.

apply_open_options2(Doc,[]) ->
    {ok, Doc};
apply_open_options2(Doc, [ejson_body | Rest]) ->
    apply_open_options2(couch_doc:with_ejson_body(Doc), Rest);
apply_open_options2(Doc, [json_bin_body | Rest]) ->
    apply_open_options2(couch_doc:with_json_body(Doc), Rest);
apply_open_options2(Doc,[_|Rest]) ->
    apply_open_options2(Doc,Rest).


% Each returned result is a list of tuples:
% {Id, MissingRev}
% if the is on disk, it's omitted from the results.
get_missing_revs(Db, IdRevList) ->
    Results = get_doc_infos(Db, [Id || {Id, _Rev} <- IdRevList]),
    {ok, find_missing(IdRevList, Results)}.


find_missing([], []) ->
    [];
find_missing([{Id,{RevPos, RevId}}|RestIdRevs],
        [{ok, #doc_info{rev={DiskRevPos, DiskRevId}}} | RestLookupInfo]) ->
    case {RevPos, RevId} of
    {DiskRevPos, DiskRevId} ->
        find_missing(RestIdRevs, RestLookupInfo);
    _ ->
        [{Id, {DiskRevPos, DiskRevId}} |
                find_missing(RestIdRevs, RestLookupInfo)]
    end;
find_missing([{Id, Rev}|RestIdRevs], [not_found | RestLookupInfo]) ->
    [{Id, Rev} | find_missing(RestIdRevs, RestLookupInfo)].


%   returns {ok, DocInfo} or not_found
get_doc_info(Db, Id) ->
    [Result] = get_doc_infos(Db, [Id]),
    Result.

get_doc_infos(Db, Ids) ->
    couch_btree:lookup(Db#db.docinfo_by_id_btree, Ids).


-spec random_doc_info(#db{}) -> {'ok', #doc_info{}} | 'empty'.
random_doc_info(#db{docinfo_by_id_btree = IdBtree}) ->
    {ok, <<DocCount:40, _DelDocCount:40, _Size:48>>} = couch_btree:full_reduce(IdBtree),
    case DocCount of
    0 ->
        empty;
    _ ->
        N = crypto:rand_uniform(1, DocCount + 1),
        Acc0 = {1, undefined},
        FoldFun = fun
        (value, #doc_info{deleted = false} = DI, _Reds, {I, undefined}) ->
            case I == N of
            true ->
                {stop, {I, DI}};
            false ->
                {ok, {I + 1, undefined}}
            end;
        (value, #doc_info{deleted = true}, _Reds, Acc) ->
            {ok, Acc};
        (branch, _Key, Red, {I, undefined}) ->
            <<BranchCount:40, _BranchDelCount:40, _SubSize:48>> = Red,
            I2 = I + BranchCount,
            case I2 >= N of
            true ->
                {ok, {I, undefined}};
            false ->
                {skip, {I2, undefined}}
            end
        end,
        {ok, _, FinalAcc} = couch_btree:fold(IdBtree, FoldFun, Acc0, []),
        {N, #doc_info{} = DocInfo} = FinalAcc,
        {ok, DocInfo}
    end.


increment_update_seq(#db{update_pid=UpdatePid}) ->
    gen_server:call(UpdatePid, increment_update_seq, infinity).

purge_docs(#db{update_pid=UpdatePid}, IdsRevs) ->
    gen_server:call(UpdatePid, {purge_docs, IdsRevs}, infinity).

get_committed_update_seq(#db{committed_update_seq=Seq}) ->
    Seq.

get_update_seq(#db{update_seq=Seq})->
    Seq.

get_purge_seq(#db{header=#db_header{purge_seq=PurgeSeq}})->
    PurgeSeq.

get_last_purged(#db{header=#db_header{purged_docs=nil}}) ->
    {ok, []};
get_last_purged(#db{fd=Fd, header=#db_header{purged_docs=PurgedPointer}}) ->
    couch_file:pread_term(Fd, PurgedPointer).

get_db_info(Db) ->
    #db{fd=Fd,
        header=#db_header{disk_version=DiskVersion},
        compactor_info=Compactor,
        update_seq=SeqNum,
        name=Name,
        instance_start_time=StartTime,
        committed_update_seq=CommittedUpdateSeq,
        docinfo_by_id_btree = IdBtree,
        docinfo_by_seq_btree = SeqBtree,
        local_docs_btree = LocalBtree
    } = Db,
    {ok, Size} = couch_file:bytes(Fd),
    {ok, <<DocCount:40, DocDelCount:40, DocAndAttsSize:48>>} =
            couch_btree:full_reduce(IdBtree),
    DataSize = couch_btree:size(SeqBtree) + couch_btree:size(IdBtree) +
        couch_btree:size(LocalBtree) + DocAndAttsSize,
    InfoList = [
        {db_name, Name},
        {doc_count, DocCount},
        {doc_del_count, DocDelCount},
        {update_seq, SeqNum},
        {purge_seq, couch_db:get_purge_seq(Db)},
        {compact_running, Compactor/=nil},
        {disk_size, Size},
        {data_size, DataSize},
        {instance_start_time, StartTime},
        {disk_format_version, DiskVersion},
        {committed_update_seq, CommittedUpdateSeq}
        ],
    {ok, InfoList}.


get_design_docs(Db) ->
    get_design_docs(Db, no_deletes).

get_design_docs(Db, DeletedAlso) ->
    {ok,_, Docs} = couch_btree:fold(Db#db.docinfo_by_id_btree,
        fun(#doc_info{deleted = true}, _Reds, AccDocs)
                    when DeletedAlso == no_deletes ->
            {ok, AccDocs};
        (#doc_info{id= <<"_design/",_/binary>>}=DocInfo, _Reds, AccDocs) ->
            {ok, Doc} = open_doc_int(Db, DocInfo, [ejson_body]),
            {ok, [Doc | AccDocs]};
        (_, _Reds, AccDocs) ->
            {stop, AccDocs}
        end,
        [], [{start_key, <<"_design/">>}, {end_key_gt, <<"_design0">>}]),
    {ok, Docs}.

check_is_admin(#db{user_ctx=#user_ctx{name=Name,roles=Roles}}=Db) ->
    {Admins} = get_admins(Db),
    AdminRoles = [<<"_admin">> | couch_util:get_value(<<"roles">>, Admins, [])],
    AdminNames = couch_util:get_value(<<"names">>, Admins,[]),
    case AdminRoles -- Roles of
    AdminRoles -> % same list, not an admin role
        case AdminNames -- [Name] of
        AdminNames -> % same names, not an admin
            throw({unauthorized, <<"You are not a db or server admin.">>});
        _ ->
            ok
        end;
    _ ->
        ok
    end.

check_is_member(#db{user_ctx=#user_ctx{name=Name,roles=Roles}=UserCtx}=Db) ->
    case (catch check_is_admin(Db)) of
    ok -> ok;
    _ ->
        {Members} = get_members(Db),
        ReaderRoles = couch_util:get_value(<<"roles">>, Members,[]),
        WithAdminRoles = [<<"_admin">> | ReaderRoles],
        ReaderNames = couch_util:get_value(<<"names">>, Members,[]),
        case ReaderRoles ++ ReaderNames of
        [] -> ok; % no readers == public access
        _Else ->
            case WithAdminRoles -- Roles of
            WithAdminRoles -> % same list, not an reader role
                case ReaderNames -- [Name] of
                ReaderNames -> % same names, not a reader
                    ?LOG_DEBUG("Not a reader: UserCtx ~p vs Names ~p Roles ~p",[UserCtx, ReaderNames, WithAdminRoles]),
                    throw({unauthorized, <<"You are not authorized to access this db.">>});
                _ ->
                    ok
                end;
            _ ->
                ok
            end
        end
    end.

get_admins(#db{security=SecProps}) ->
    couch_util:get_value(<<"admins">>, SecProps, {[]}).

get_members(#db{security=SecProps}) ->
    % we fallback to readers here for backwards compatibility
    couch_util:get_value(<<"members">>, SecProps,
        couch_util:get_value(<<"readers">>, SecProps, {[]})).

get_security(#db{security=SecProps}) ->
    {SecProps}.

set_security(#db{update_pid=Pid}=Db, {NewSecProps}) when is_list(NewSecProps) ->
    check_is_admin(Db),
    ok = validate_security_object(NewSecProps),
    ok = gen_server:call(Pid, {set_security, NewSecProps}, infinity),
    {ok, _} = ensure_full_commit(Db),
    ok;
set_security(_, _) ->
    throw(bad_request).

validate_security_object(SecProps) ->
    Admins = couch_util:get_value(<<"admins">>, SecProps, {[]}),
    % we fallback to readers here for backwards compatibility
    Members = couch_util:get_value(<<"members">>, SecProps,
        couch_util:get_value(<<"readers">>, SecProps, {[]})),
    ok = validate_names_and_roles(Admins),
    ok = validate_names_and_roles(Members),
    ok.

% validate user input
validate_names_and_roles({Props}) when is_list(Props) ->
    case couch_util:get_value(<<"names">>,Props,[]) of
    Ns when is_list(Ns) ->
            [throw("names must be a JSON list of strings") ||N <- Ns, not is_binary(N)],
            Ns;
    _ -> throw("names must be a JSON list of strings")
    end,
    case couch_util:get_value(<<"roles">>,Props,[]) of
    Rs when is_list(Rs) ->
        [throw("roles must be a JSON list of strings") ||R <- Rs, not is_binary(R)],
        Rs;
    _ -> throw("roles must be a JSON list of strings")
    end,
    ok.

name(#db{name=Name}) ->
    Name.

add_update_listener(#db{update_pid = UpPid}, Pid, Tag) ->
    gen_server:call(UpPid, {add_update_listener, Pid, Tag}, infinity).

remove_update_listener(#db{update_pid = UpPid}, Pid) ->
    gen_server:call(UpPid, {remove_update_listener, Pid}, infinity).

update_doc(Db, Docs) ->
    update_doc(Db, Docs, []).

update_doc(Db, Doc, Options) ->
    update_docs(Db, [Doc], Options).

update_docs(Db, Docs) ->
    update_docs(Db, Docs, []).

% This open a new raw file in current process for the duration of the Fun
% execution. For single reads, it's likely slower than regular reads due
% to the overhead of opening a new FD. But for lots of reads like,
% docs_since or enum_docs, it's often faster as it avoids the messaging
% overhead with couch_file.
fast_reads(Db, Fun) ->
    case file:open(Db#db.filepath, [binary, read, raw]) of
    {ok, FastReadFd} ->
        put({Db#db.fd, fast_fd_read}, FastReadFd),
        try
            Fun()
        after
            file:close(FastReadFd),
            erase({Db#db.fd, fast_fd_read})
        end;
    {error, enoent} ->
        ?LOG_INFO("Couldn't do fast read, compaction must have deleted" ++
            " previous storage file ~s, making reopening the raw file " ++ 
            "impossible. Reverting to slower reads", [Db#db.filepath]),
        Fun()
    end.




update_docs(Db, Docs, Options0) ->
    % go ahead and generate the new revision ids for the documents.
    % separate out the NonRep documents from the rest of the documents
    {Docs1, NonRepDocs1} = lists:foldl(
        fun(#doc{id=Id}=Doc, {DocsAcc, NonRepDocsAcc}) ->
            case Id of
            <<?LOCAL_DOC_PREFIX, _/binary>> ->
                {DocsAcc, [Doc | NonRepDocsAcc]};
            <<?DESIGN_DOC_PREFIX, _/binary>> ->
                validate_ddoc(Doc),
                {[Doc | DocsAcc], NonRepDocsAcc};
            Id ->
                {[Doc | DocsAcc], NonRepDocsAcc}
            end
        end, {[], []}, Docs),
    case lists:member(sort_docs, Options0) of
    true ->
        Docs2 = lists:keysort(#doc.id, Docs1),
        NonRepDocs = lists:keysort(#doc.id, NonRepDocs1);
    false ->
        Docs2 = Docs1,
        NonRepDocs = NonRepDocs1
    end,
    Options = set_commit_option(Options0),
    FullCommit = lists:member(full_commit, Options),
    Docs3 = write_doc_bodies_retry_closed(Db, Docs2),
    MRef = erlang:monitor(process, Db#db.update_pid),
    try
        Db#db.update_pid ! {update_docs, self(), Docs3, NonRepDocs,
                FullCommit},
        case get_result(Db#db.update_pid, MRef) of
        ok ->
            ok;
        retry ->
            % This can happen if the db file we wrote to was swapped out by
            % compaction. Retry by reopening the db and writing to the current file
            {ok, Db2} = open_ref_counted(Db#db.main_pid, self()),
            % We only retry once
            Docs4 = write_doc_bodies(Db2, Docs2),
            close(Db2),
            Db#db.update_pid ! {update_docs, self(), Docs4, NonRepDocs,
                    FullCommit},
            case get_result(Db#db.update_pid, MRef) of
            ok ->
                ok;
            retry -> throw({update_error, compaction_retry})
            end
        end
    after
        erlang:demonitor(MRef, [flush])
    end.

set_commit_option(Options) ->
    CommitSettings = {
        [true || O <- Options, O==full_commit orelse O==delay_commit],
        couch_config:get("couchdb", "delayed_commits", "false")
    },
    case CommitSettings of
    {[true], _} ->
        Options; % user requested explicit commit setting, do not change it
    {_, "true"} ->
        Options; % delayed commits are enabled, do nothing
    {_, "false"} ->
        [full_commit|Options];
    {_, Else} ->
        ?LOG_ERROR("[couchdb] delayed_commits setting must be true/false, not ~p",
            [Else]),
        [full_commit|Options]
    end.

get_result(UpdatePid, MRef) ->
    receive
    {done, UpdatePid} ->
        ok;
    {retry, UpdatePid} ->
        retry;
    {'DOWN', MRef, _, _, Reason} ->
        exit(Reason)
    end.


write_doc_bodies_retry_closed(Db, Docs) ->
    try
        write_doc_bodies(Db, Docs)
    catch
        throw:{update_error, compaction_retry} ->
            {ok, Db2} = open_ref_counted(Db#db.main_pid, self()),
            % We only retry once
            Docs2 = write_doc_bodies(Db2, Docs),
            close(Db2),
            Docs2
    end.


write_doc_bodies(Db, Docs) ->
    lists:map(
        fun(#doc{body = Body, content_meta = ContentMeta0} = Doc) ->
            {Prepped, ContentMeta} = prep_doc_body_binary(Body, ContentMeta0),
            case couch_file:append_binary_crc32(Db#db.fd, Prepped) of
            {ok, BodyPtr, Size} ->
                #doc_update_info{
                    id=Doc#doc.id,
                    rev=Doc#doc.rev,
                    deleted=Doc#doc.deleted,
                    body_ptr=BodyPtr,
                    content_meta=ContentMeta,
                    fd=Db#db.fd,
                    size=Size
                };
            {error, write_closed} ->
                throw({update_error, compaction_retry})
            end
        end,
        Docs).

prep_doc_body_binary(EJson, _ContentMeta) when is_tuple(EJson)->
    % convert ejson to json binary, clear out ContentMeta, set to
    % compressed json.
    {couch_compress:compress(?JSON_ENCODE(EJson)),
        ?CONTENT_META_JSON bor ?CONTENT_META_SNAPPY_COMPRESSED};
prep_doc_body_binary(Body, ContentMeta) ->
    % assume body is binary or iolist, and preserve ContentMeta
    {Body, ContentMeta}.

enum_docs_since_reduce_to_count(Reds) ->
    <<Count:40>> = couch_btree:final_reduce(
            fun couch_db_updater:btree_by_seq_reduce/2, Reds),
    Count.

enum_docs_reduce_to_count(Reds) ->
    <<Count:40, _DelCount:40, _Size:48>> = couch_btree:final_reduce(
            fun couch_db_updater:btree_by_id_reduce/2, Reds),
    Count.

changes_since(Db, StartSeq, Fun, Acc) ->
    changes_since(Db, StartSeq, Fun, [], Acc).

changes_since(Db, StartSeq, Fun, Options, Acc) ->
    Wrapper = fun(DocInfo, _Offset, Acc2) -> Fun(DocInfo, Acc2) end,
    {ok, _Reduce, AccOut} = couch_btree:fold(Db#db.docinfo_by_seq_btree,
            Wrapper, Acc, [{start_key, StartSeq + 1}] ++ Options),
    {ok, AccOut}.

count_changes_since(Db, SinceSeq) ->
    BTree = Db#db.docinfo_by_seq_btree,
    {ok, <<Changes:40>>} =
    couch_btree:fold_reduce(BTree,
        fun(_SeqStart, PartialReds, <<0:40>>) ->
            {ok, couch_btree:final_reduce(BTree, PartialReds)}
        end,
        <<0:40>>, [{start_key, SinceSeq + 1}]),
    Changes.

enum_docs_since(Db, SinceSeq, InFun, Acc, Options) ->
    {ok, LastReduction, AccOut} = couch_btree:fold(
        Db#db.docinfo_by_seq_btree, InFun, Acc,
        [{start_key, SinceSeq + 1} | Options]),
    {ok, enum_docs_since_reduce_to_count(LastReduction), AccOut}.

enum_docs(Db, InFun, InAcc, Options) ->
    {ok, LastReduce, OutAcc} = couch_btree:fold(
        Db#db.docinfo_by_id_btree, InFun, InAcc, Options),
    {ok, enum_docs_reduce_to_count(LastReduce), OutAcc}.

% server functions

init({DbName, Filepath, Fd, Options}) ->
    {ok, UpdaterPid} = gen_server:start_link(couch_db_updater, {self(), DbName, Filepath, Fd, Options}, []),
    {ok, #db{fd_ref_counter=RefCntr}=Db} = gen_server:call(UpdaterPid, get_db, infinity),
    couch_ref_counter:add(RefCntr),
    process_flag(trap_exit, true),
    {ok, Db}.

terminate(_Reason, Db) ->
    couch_util:shutdown_sync(Db#db.update_pid),
    ok.

handle_call({open_ref_count, OpenerPid}, _, #db{fd_ref_counter=RefCntr}=Db) ->
    ok = couch_ref_counter:add(RefCntr, OpenerPid),
    {reply, {ok, Db}, Db};
handle_call(is_idle, _From, #db{fd_ref_counter=RefCntr, compactor_info=Compact,
            waiting_delayed_commit=Delay}=Db) ->
    % Idle means no referrers. Unless in the middle of a compaction file switch,
    % there are always at least 2 referrers, couch_db_updater and us.
    {reply, (Delay == nil) andalso (Compact == nil) andalso (couch_ref_counter:count(RefCntr) == 2), Db};
handle_call(get_db, _From, Db) ->
    {reply, {ok, Db}, Db};
handle_call(get_current_seq, _From, #db{update_seq = Seq} = Db) ->
    {reply, {ok, Seq}, Db}.


handle_cast(Msg, Db) ->
    ?LOG_ERROR("Bad cast message received for db ~s: ~p", [Db#db.name, Msg]),
    exit({error, Msg}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_info({db_updated, Ref, NewDb}, Db) ->
    case NewDb#db.fd_ref_counter =:= Db#db.fd_ref_counter of
    true ->
        ok;
    false ->
        couch_ref_counter:add(NewDb#db.fd_ref_counter),
        couch_ref_counter:drop(Db#db.fd_ref_counter)
    end,
    NewDb#db.update_pid ! {ok, Ref},
    {noreply, NewDb};
handle_info({'EXIT', _Pid, normal}, Db) ->
    {noreply, Db};
handle_info({'EXIT', _Pid, Reason}, Server) ->
    {stop, Reason, Server};
handle_info(Msg, Db) ->
    ?LOG_ERROR("Bad message received for db ~s: ~p", [Db#db.name, Msg]),
    exit({error, Msg}).


%%% Internal function %%%

open_doc_int(Db, <<?LOCAL_DOC_PREFIX, _/binary>> = Id, Options) ->
    case couch_btree:lookup(Db#db.local_docs_btree, [Id]) of
    [{ok, {_, BodyData}}] ->
        Doc = #doc{id=Id, body=BodyData},
        apply_open_options({ok, Doc}, Options);
    [not_found] ->
        {not_found, missing}
    end;
open_doc_int(Db, #doc_info{id=Id,deleted=IsDeleted,rev=RevInfo, body_ptr=Bp,
        content_meta=ContentMeta}=DocInfo, Options) ->
    {ok, Body} = couch_file:pread_iolist(Db#db.fd, Bp),
    Doc = #doc{
        id = Id,
        rev = RevInfo,
        body = Body,
        deleted = IsDeleted,
        content_meta = ContentMeta
        },
    apply_open_options(
       {ok, Doc#doc{meta=doc_meta_info(DocInfo, Options)}}, Options);
open_doc_int(Db, Id, Options) ->
    case get_doc_info(Db, Id) of
    {ok, DocInfo} ->
        open_doc_int(Db, DocInfo, Options);
    not_found ->
        {not_found, missing}
    end.

doc_meta_info(#doc_info{local_seq=Seq}, Options) ->
    case lists:member(local_seq, Options) of
    false -> [];
    true -> [{local_seq, Seq}]
    end.


validate_ddoc(#doc{content_meta = ?CONTENT_META_JSON} = DDoc0) ->
    DDoc = couch_doc:with_ejson_body(DDoc0),
    try
        couch_set_view_mapreduce:validate_ddoc_views(DDoc),
        try
            couch_spatial_validation:validate_ddoc_spatial(DDoc)
        catch error:undef ->
            % Ignore, happens during make check or standalone CouchDB
            ok
        end
    catch throw:{error, Reason} ->
        throw({invalid_design_doc, Reason})
    end;
validate_ddoc(_DDoc) ->
    throw({invalid_design_doc, <<"Content is not json.">>}).
