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

-module(couch_file).
-behaviour(gen_server).

-include("couch_db.hrl").

-define(SIZE_BLOCK, 4096).
-define(RETRY_TIME_MS, 1000).
-define(MAX_RETRY_TIME_MS, 10000).

-record(file, {
    reader = nil,
    writer = nil,
    eof = 0
}).

% public API
-export([open/1, open/2, close/1, bytes/1, flush/1, sync/1, truncate/2]).
-export([pread_term/2, pread_iolist/2, pread_binary/2,rename/2]).
-export([append_binary/2, append_binary_crc32/2, set_close_after/2]).
-export([append_raw_chunk/2, assemble_file_chunk/1, assemble_file_chunk/2]).
-export([append_term/2]).
-export([write_header/2,write_header_bin/2, read_header/1,read_header/2]).
-export([read_header_bin/1,read_header_bin/2]).
-export([find_header_bin/2]).
-export([only_snapshot_reads/1, refresh_eof/1]).
-export([delete/2, delete/3, init_delete_dir/1,get_delete_dir/1]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

% for proc_lib
-export([spawn_reader/2, spawn_writer/2]).

% for hibernate
-export([reader_loop_wakeup/2]).

%%----------------------------------------------------------------------
%% Args:   Valid Options are [create] and [create,overwrite].
%%  and [{fd_close_after, Ms}].
%%  The fd_close_after will close the OS file descriptor if not used
%% for the specified # of Millisecs. When using the fd_close_after option
%% with any setting but 'infinity', the file should not be renamed or deleted
%% while in use, as the file will not be able to reopen the same file.
%%  Files are opened in read/write mode.
%% Returns: On success, {ok, Fd}
%%  or {error, Reason} if the file could not be opened.
%%----------------------------------------------------------------------

open(Filepath) ->
    open(Filepath, []).

open(Filepath, Options) ->
    proc_lib:start_link(?MODULE, init, [{Filepath, Options}]).

%%----------------------------------------------------------------------
%% Purpose: To append an Erlang term to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos, NumBytesWritten} where Pos is the file offset to
%%  the beginning the serialized  term. Use pread_term to read the term
%%  back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_term(Fd, Term) ->
    append_binary_crc32(Fd, couch_compress:compress(?term_to_bin(Term))).

%%----------------------------------------------------------------------
%% Purpose: To append an Erlang binary to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos, NumBytesWritten} where Pos is the file offset to the
%%  beginning the serialized term. Use pread_term to read the term back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_binary(Fd, Bin) ->
    gen_server:call(Fd, {append_bin, assemble_file_chunk(Bin)}, infinity).

append_binary_crc32(Fd, Bin) ->
    gen_server:call(Fd,
        {append_bin, assemble_file_chunk(Bin, erlang:crc32(Bin))}, infinity).

append_raw_chunk(Fd, Chunk) ->
    gen_server:call(Fd, {append_bin, Chunk}, infinity).

assemble_file_chunk(Bin) ->
    [<<0:1/integer, (iolist_size(Bin)):31/integer>>, Bin].

assemble_file_chunk(Bin, Crc32) ->
    [<<1:1/integer, (iolist_size(Bin)):31/integer, Crc32:32/integer>>, Bin].

%%----------------------------------------------------------------------
%% Purpose: Reads a term from a file that was written with append_term
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------


pread_term(Fd, Pos) ->
    case pread_binary(Fd, Pos) of
    {ok, Bin} ->
        {ok, binary_to_term(couch_compress:decompress(Bin))};
    Else ->
        Else
    end.


%%----------------------------------------------------------------------
%% Purpose: Reads a binrary from a file that was written with append_binary
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------

pread_binary(Fd, Pos) ->
    case pread_iolist(Fd, Pos) of
    {ok, L} ->
        {ok, iolist_to_binary(L)};
    Else ->
        Else
    end.


pread_iolist(Fd, Pos) ->
    case do_read(Fd, Pos) of
    {ok, IoList} ->
        {ok, IoList};
    {ok, IoList, <<Crc32:32/integer>>} ->
        case erlang:crc32(IoList) of
        Crc32 ->
            {ok, IoList};
        _ ->
            exit({file_corruption, <<"file corruption">>})
        end;
    Else ->
        Else
    end.


%%----------------------------------------------------------------------
%% Purpose: The length of a file, in bytes.
%% Returns: {ok, Bytes}
%%  or {error, Reason}.
%%----------------------------------------------------------------------

% length in bytes
bytes(Fd) ->
    gen_server:call(Fd, bytes, infinity).

%%----------------------------------------------------------------------
%% Purpose: Truncate a file to the number of bytes.
%% Returns: ok
%%  or {error, Reason}.
%%----------------------------------------------------------------------

truncate(Fd, Pos) ->
    gen_server:call(Fd, {truncate, Pos}, infinity).

%%----------------------------------------------------------------------
%% Purpose: Ensure all bytes written to the file are flushed to disk.
%% Returns: ok
%%  or {error, Reason}.
%%----------------------------------------------------------------------

sync(Fd) ->
    gen_server:call(Fd, sync, infinity).

%%----------------------------------------------------------------------
%% Purpose: Ensure that all the data the caller previously asked to write
%% to the file were flushed to disk (not necessarily fsync'ed).
%% Returns: ok
%%----------------------------------------------------------------------

flush(Fd) ->
    gen_server:call(Fd, flush, infinity).

%%----------------------------------------------------------------------
%% Purpose: Close the file.
%% Returns: ok
%%----------------------------------------------------------------------
close(Fd) ->
    couch_util:shutdown_sync(Fd).

%%----------------------------------------------------------------------
%% Purpose: Prevents writing to the file and keeps the read file open.
%% This allows the file to be used after deletion for snapshot reads
%% Returns: ok
%%----------------------------------------------------------------------
only_snapshot_reads(Fd) ->
    gen_server:call(Fd, snapshot_reads, infinity).


%%----------------------------------------------------------------------
%% Purpose: Sets the timeout where an unused FD will automatically close
%% itself after MS has passed. Set to 'infinity' to never close.
%% Returns: ok
%%----------------------------------------------------------------------
set_close_after(Fd, AfterMS) ->
    gen_server:call(Fd, {set_close_after, AfterMS}, infinity).


%%----------------------------------------------------------------------
%% Purpose: Renames the files safely and coordinates with couch_file_write_guard
%% NOTE: it is not safe to call this on a file with set_close_after called
%% with anything other than 'infinity'.
%% Returns: ok
%%----------------------------------------------------------------------
rename(Fd, NewFilepath) ->
    gen_server:call(Fd, {rename, NewFilepath}, infinity).


%%---------------------------------------------------------------------------
%% Purpose: Reopens the backing file and refreshes write fd to obtain new eof
%% Returns: ok
%%---------------------------------------------------------------------------
refresh_eof(Fd) ->
    gen_server:call(Fd, reopen_file, infinity).


delete(RootDir, Filepath) ->
    delete(RootDir, Filepath, true, false).

delete(RootDir, Filepath, Async) ->
    delete(RootDir, Filepath, Async, false).

delete(RootDir, Filepath, Async, Retry) ->
    DelFile = filename:join([RootDir,".delete", ?b2l(couch_uuids:random())]),
    ?LOG_INFO("Deleting couch file ~p with renaming it to ~p", [Filepath,
        DelFile]),
    case file2:rename(Filepath, DelFile) of
    ok ->
        if (Async) ->
            spawn(file2, delete, [DelFile]),
            ok;
        true ->
            file2:delete(DelFile)
        end;
    % The target directory might not exist, create it and retry
    {error, enoent} when Retry =:= false ->
        ok = file2:ensure_dir(DelFile),
        delete(RootDir, Filepath, Async, true);
    Error ->
        Error
    end.


get_delete_dir(RootDir) ->
    filename:join(RootDir,".delete").

init_delete_dir(RootDir) ->
    Dir = get_delete_dir(RootDir),
    % note: ensure_dir requires an actual filename companent, which is the
    % reason for "foo".
    file2:ensure_dir(filename:join(Dir,"foo")),
    file2:fold_files(Dir, ".*", true,
        fun(Filename, _) ->
            ok = file2:delete(Filename)
        end, ok).


read_header(Fd) ->
    case gen_server:call(Fd, find_header, infinity) of
    {ok, Bin, Pos} ->
        {ok, binary_to_term(Bin), Pos};
    Else ->
        Else
    end.


read_header_bin(Fd) ->
    gen_server:call(Fd, find_header, infinity).


read_header(Fd, Pos) ->
    case read_header_bin(Fd, Pos) of
    {ok, Bin} ->
        {ok, binary_to_term(Bin)};
    Else ->
        Else
    end.


read_header_bin(Fd, Pos) ->
    gen_server:call(Fd, {read_header, Pos}, infinity).


% Find a header backwards from this position. In case there
% is a header at exactly the given position return that one
% immmediately.
find_header_bin(Fd, eof) ->
    gen_server:call(Fd, find_header, infinity);
find_header_bin(Fd, Pos) ->
    gen_server:call(Fd, {find_header, Pos}, infinity).


write_header(Fd, Data) ->
    Bin = term_to_binary(Data),
    write_header_bin(Fd, Bin).


write_header_bin(Fd, Bin) ->
    Crc32 = erlang:crc32(Bin),
    % now we assemble the final header binary and write to disk
    FinalBin = <<Crc32:32, Bin/binary>>,
    {ok, _Pos} = gen_server:call(Fd, {write_header, FinalBin}, infinity).


% server functions

init({Filepath, Options}) ->
   try
       CloseTimeout = proplists:get_value(fd_close_after, Options, infinity),
       ok = maybe_create_file(Filepath, Options),
       process_flag(trap_exit, true),
       {ok, Reader} = proc_lib:start_link(?MODULE, spawn_reader,
            [Filepath, CloseTimeout]),
       {ok, Writer, Eof} = proc_lib:start_link(?MODULE, spawn_writer,
            [Filepath, CloseTimeout]),
       ok = couch_file_write_guard:add(Filepath, Writer),
       proc_lib:init_ack({ok, self()}),
       InitState = #file{
           reader = Reader,
           writer = Writer,
           eof = Eof
       },
       gen_server:enter_loop(?MODULE, [], InitState)
   catch
   error:{badmatch, {error, eacces}} ->
       proc_lib:init_ack({file_permission_error, Filepath});
   error:{badmatch, already_added_to_file_write_guard} ->
       proc_lib:init_ack({file_already_opened, Filepath});
   error:{badmatch, Error} ->
       proc_lib:init_ack(Error)
   end.

maybe_create_file(Filepath, Options) ->
    case lists:member(create, Options) of
    true ->
        file2:ensure_dir(Filepath),
        case file2:open(Filepath, [read, write, binary]) of
        {ok, Fd} ->
            {ok, Length} = file:position(Fd, eof),
            case Length > 0 of
            true ->
                % this means the file already exists and has data.
                % FYI: We don't differentiate between empty files and non-existant
                % files here.
                case lists:member(overwrite, Options) of
                true ->
                    {ok, 0} = file:position(Fd, 0),
                    ok = file:truncate(Fd),
                    ok = file:sync(Fd);
                false ->
                    ok = file:close(Fd),
                    file_exists
                end;
            false ->
                ok
            end;
        Error ->
            Error
        end;
    false ->
        ok
    end.

terminate(_Reason, #file{reader = Reader, writer = Writer}) ->
    couch_util:shutdown_sync(Reader),
    case Writer of
    nil -> ok;
    _ ->
        couch_util:shutdown_sync(Writer)
    end.


handle_call({pread_iolist, Pos}, From, #file{reader = Reader} = File) ->
    Reader ! {read, Pos, From},
    {noreply, File};

handle_call(bytes, _From, #file{eof = Eof} = File) ->
    {reply, {ok, Eof}, File};

handle_call(sync, From, #file{writer = W} = File) ->
    W ! {sync, From},
    {noreply, File};

handle_call({truncate, Pos}, _From, #file{writer = W} = File) ->
    W ! {truncate, Pos, self()},
    receive {W, truncated, Pos} -> ok end,
    {reply, ok, File#file{eof = Pos}};

handle_call({append_bin, _Bin}, _From, #file{writer = nil} = File) ->
    {reply, {error, write_closed}, File};
handle_call({append_bin, Bin}, From, #file{writer = W, eof = Pos} = File) ->
    Size = calculate_total_read_len(Pos rem ?SIZE_BLOCK, iolist_size(Bin)),
    gen_server:reply(From, {ok, Pos, Size}),
    W ! {chunk, Bin},
    {noreply, File#file{eof = Pos + Size}};

handle_call({write_header, Bin}, From, #file{writer = W, eof = Pos} = File) ->
    W ! {header, Bin},
    Pos2 = case Pos rem ?SIZE_BLOCK of
    0 ->
        Pos;
    BlockOffset ->
        Pos + (?SIZE_BLOCK - BlockOffset)
    end,
    gen_server:reply(From, {ok, Pos2}),
    File2 = File#file{
        eof = Pos2 + 5 + calculate_total_read_len(5, byte_size(Bin))
    },
    {noreply, File2};

handle_call(flush, _From, #file{writer =  nil} = File) ->
    % if the writer is shutdown, nothing to flush.
    {reply, ok, File};
handle_call(flush, From, #file{writer =  W} = File) ->
    W ! {flush, From},
    {noreply, File};

handle_call(find_header, From, #file{reader = Reader, eof = Eof} = File) ->
    Reader ! {find_header, Eof, From},
    {noreply, File};
handle_call({find_header, Pos}, From, #file{reader = Reader} = File) ->
    Reader ! {find_header, Pos, From},
    {noreply, File};

handle_call({read_header, Pos}, From, #file{reader = R} = File) ->
    R ! {read_header, Pos, From},
    % update the eof since file must have been updated externally
    R ! {get_eof, self()},
    receive
        {eof, Eof, R} -> ok;
        {'EXIT', R, Reason} ->
            Eof = ok, % appease compiler
            exit({read_loop_died, Reason})
    end,
    {noreply, File#file{eof=Eof}};

handle_call(snapshot_reads, _From, #file{reader = R, writer = W} = File) ->
    R ! {set_close_after, infinity, self()},
    couch_util:shutdown_sync(W), % no-op if nil
    receive
        {ok, R} -> ok;
        {'EXIT', R, Reason} ->
            exit({read_loop_died, Reason})
    end,
    {reply, ok, File#file{writer=nil}};

handle_call(reopen_file, _From, #file{writer = W} = File) ->
    W ! {reopen_file, self()},
    {ok, Eof} = receive
    {ok, W, Pos} ->
        {ok, Pos};
    {'EXIT', W, Reason2} ->
        exit({writer_loop_died, Reason2})
    end,
    {reply, ok, File#file{eof=Eof}};

handle_call({set_close_after, Ms}, _From, #file{reader = R, writer = W} = File) ->
    R ! {set_close_after, Ms, self()},
    case W of
    nil -> ok;
    _ ->
        W ! {set_close_after, Ms, self()},
        receive
            {ok, W} -> ok;
            {'EXIT', W, Reason2} ->
                exit({write_loop_died, Reason2})
        end
    end,
    receive
        {ok, R} -> ok;
        {'EXIT', R, Reason} ->
            exit({read_loop_died, Reason})
    end,
    {reply, ok, File};

handle_call({rename, Filepath}, _From, #file{reader = R, writer = W} = File) ->
    R ! {rename, Filepath, self()},
    W ! {rename, Filepath, self()},
    receive
        {ok, R} -> ok;
        {'EXIT', R, Reason} ->
            exit({read_loop_died, Reason})
    end,
    receive
        {ok, W} -> ok;
        {'EXIT', W, Reason2} ->
            exit({write_loop_died, Reason2})
    end,
    {reply, ok, File}.

handle_cast(unused, Fd) ->
    {stop,bad_message,Fd}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info(heart, File) ->
    {noreply, File};
handle_info({'EXIT', Pid, Reason}, #file{writer = Pid} = Fd) ->
    {stop, {write_loop_died, Reason}, Fd};
handle_info({'EXIT', Pid, Reason}, #file{reader = Pid} = Fd) ->
    {stop, {read_loop_died, Reason}, Fd};
handle_info({'EXIT', _, Reason}, Fd) ->
    {stop, Reason, Fd}.


do_read(Fd, Pos) ->
    case get({Fd, fast_fd_read}) of
    undefined ->
        gen_server:call(Fd, {pread_iolist, Pos}, infinity);
    ReaderFd ->
        read_iolist(ReaderFd, Pos)
    end.

do_find_header(_Fd, -1) ->
    no_valid_header;
do_find_header(Fd, Block) ->
    case (catch load_header(Fd, Block)) of
    {ok, Bin} ->
        {ok, Bin, Block * ?SIZE_BLOCK};
    _Error ->
        do_find_header(Fd, Block -1)
    end.

load_header(Fd, Block) ->
    {ok, <<1, HeaderLen:32/integer, RestBlock/binary>>} =
        file:pread(Fd, Block * ?SIZE_BLOCK, ?SIZE_BLOCK),
    TotalBytes = calculate_total_read_len(5, HeaderLen),
    case TotalBytes > byte_size(RestBlock) of
    false ->
        <<RawBin:TotalBytes/binary, _/binary>> = RestBlock;
    true ->
        {ok, Missing} = file:pread(
            Fd, (Block * ?SIZE_BLOCK) + 5 + byte_size(RestBlock),
            TotalBytes - byte_size(RestBlock)),
        RawBin = <<RestBlock/binary, Missing/binary>>
    end,
    <<Crc32:32, HeaderBin/binary>> =
        iolist_to_binary(remove_block_prefixes(RawBin, 5)),
    Crc32 = erlang:crc32(HeaderBin),
    {ok, HeaderBin}.

maybe_read_more_iolist(Buffer, DataSize, NextPos, Fd) ->
    case iolist_size(Buffer) of
    BufferSize when DataSize =< BufferSize ->
        {Buffer2, _} = couch_util:split_iolist(Buffer, DataSize),
        Buffer2;
    BufferSize ->
        {Missing, _} = read_raw_iolist_int(Fd, NextPos, DataSize-BufferSize),
        [Buffer, Missing]
    end.

read_raw_iolist_int(ReadFd, Pos, Len) ->
    BlockOffset = Pos rem ?SIZE_BLOCK,
    TotalBytes = calculate_total_read_len(BlockOffset, Len),
    case file:pread(ReadFd, Pos, TotalBytes) of
    {ok, <<RawBin:TotalBytes/binary>>} ->
        {remove_block_prefixes(RawBin, BlockOffset), Pos + TotalBytes};
    {ok, RawBin} ->
        UnexpectedBin = {
            unexpected_binary,
            {at, Pos},
            {wanted_bytes, TotalBytes},
            {got, byte_size(RawBin), RawBin}
        },
        throw({read_error, UnexpectedBin});
    Else ->
        throw({read_error, Else})
    end.

-spec extract_crc32(iolist()) -> {binary(), iolist()}.
extract_crc32(FullIoList) ->
    {CrcList, IoList} = couch_util:split_iolist(FullIoList, 4),
    {iolist_to_binary(CrcList), IoList}.

calculate_total_read_len(0, FinalLen) ->
    calculate_total_read_len(1, FinalLen) + 1;
calculate_total_read_len(BlockOffset, FinalLen) ->
    case ?SIZE_BLOCK - BlockOffset of
    BlockLeft when BlockLeft >= FinalLen ->
        FinalLen;
    BlockLeft ->
        FinalLen + ((FinalLen - BlockLeft) div (?SIZE_BLOCK -1)) +
            if ((FinalLen - BlockLeft) rem (?SIZE_BLOCK -1)) =:= 0 -> 0;
                true -> 1 end
    end.

remove_block_prefixes(<<>>, _BlockOffset) ->
    [];
remove_block_prefixes(<<_BlockPrefix, Rest/binary>>, 0) ->
    remove_block_prefixes(Rest, 1);
remove_block_prefixes(Bin, BlockOffset) ->
    BlockBytesAvailable = ?SIZE_BLOCK - BlockOffset,
    case size(Bin) of
    Size when Size > BlockBytesAvailable ->
        <<DataBlock:BlockBytesAvailable/binary,Rest/binary>> = Bin,
        [DataBlock | remove_block_prefixes(Rest, 0)];
    _Size ->
        [Bin]
    end.

make_blocks(0, IoList) ->
    case iolist_size(IoList) of
    0 ->
        [];
    _ ->
        [<<0>> | make_blocks(1, IoList)]
    end;
make_blocks(BlockOffset, IoList) ->
    case iolist_size(IoList) of
    0 ->
        [];
    _ ->
        case couch_util:split_iolist(IoList, (?SIZE_BLOCK - BlockOffset)) of
        {Begin, End} ->
            [Begin | make_blocks(0, End)];
        _SplitRemaining ->
            IoList
        end
    end.

try_open_fd(FilePath, Options, _Timewait, TotalTimeRemain)
        when TotalTimeRemain < 0 ->
    % Out of retry time.
    % Try one last time and whatever we get is the returned result.
    file2:open(FilePath, Options);
try_open_fd(FilePath, Options, Timewait, TotalTimeRemain) ->
    case file2:open(FilePath, Options) of
    {ok, Fd} ->
        {ok, Fd};
    {error, emfile} ->
        ?LOG_INFO("Too many file descriptors open, waiting"
                     ++ " ~pms to retry", [Timewait]),
        receive
        after Timewait ->
            try_open_fd(FilePath, Options, Timewait, TotalTimeRemain - Timewait)
        end;
    {error, eacces} ->
        ?LOG_INFO("eacces error opening file ~p waiting"
                         ++ " ~pms to retry", [FilePath, Timewait]),
         receive
         after Timewait ->
             try_open_fd(FilePath, Options, Timewait, TotalTimeRemain - Timewait)
         end;
    Error ->
        Error
    end.


spawn_writer(Filepath, CloseTimeout) ->
    case try_open_fd(Filepath, [binary, append, raw], ?RETRY_TIME_MS,
        ?MAX_RETRY_TIME_MS) of
    {ok, Fd} ->
        {ok, Eof} = file:position(Fd, eof),
        proc_lib:init_ack({ok, self(), Eof}),
        process_flag(trap_exit, true),
        writer_loop(Fd, Filepath, Eof, CloseTimeout);
    Error ->
        proc_lib:init_ack(Error)
    end.


spawn_reader(Filepath, CloseTimeout) ->
    case try_open_fd(Filepath, [binary, read, raw], ?RETRY_TIME_MS,
        ?MAX_RETRY_TIME_MS) of
    {ok, Fd} ->
        proc_lib:init_ack({ok, self()}),
        process_flag(trap_exit, true),
        reader_loop(Fd, Filepath, CloseTimeout);
    Error ->
        proc_lib:init_ack(Error)
    end.

writer_loop(Fd, FilePath, Eof, CloseTimeout) ->
    receive
    Msg ->
        handle_write_message(Msg, Fd, FilePath, Eof, CloseTimeout)
    after CloseTimeout ->
        % after nonuse timeout we close the Fd.
        file:close(Fd),
        receive
        {'EXIT', _From, Reason} ->
            ok = couch_file_write_guard:remove(self()),
            exit(Reason);
        Msg ->
            case try_open_fd(FilePath, [binary, append, raw], ?RETRY_TIME_MS,
                ?MAX_RETRY_TIME_MS) of
            {ok, Fd2} ->
                handle_write_message(Msg, Fd2, FilePath, Eof, CloseTimeout);
            Other ->
                erlang:exit({problem_reopening_file, Other, Msg, self(), FilePath, Eof, CloseTimeout})
            end
        end
    end.

handle_write_message(Msg, Fd, FilePath, Eof, CloseTimeout) ->
    case Msg of
    {reopen_file, From} ->
        file:close(Fd),
        case try_open_fd(FilePath, [binary, append, raw], ?RETRY_TIME_MS,
            ?MAX_RETRY_TIME_MS) of
        {ok, Fd2} ->
            {ok, Eof2} = file:position(Fd2, eof),
            From ! {ok, self(), Eof2},
            writer_loop(Fd2, FilePath, Eof2, CloseTimeout);
        Other ->
            erlang:exit({problem_reopening_file, Other, reopen_file,
                                         self(), FilePath, Eof, CloseTimeout})
        end;
    {chunk, Chunk} ->
        writer_collect_chunks(Fd, FilePath, Eof, CloseTimeout, [Chunk]);
    {header, Header} ->
        Eof2 = write_header_blocks(Fd, Eof, Header),
        writer_loop(Fd, FilePath, Eof2, CloseTimeout);
    {truncate, Pos, From} ->
        {ok, Pos} = file:position(Fd, Pos),
        ok = file:truncate(Fd),
        From ! {self(), truncated, Pos},
        writer_loop(Fd, FilePath, Pos, CloseTimeout);
    {flush, From} ->
        gen_server:reply(From, ok),
        writer_loop(Fd, FilePath, Eof, CloseTimeout);
    {sync, From} ->
        ok = file:sync(Fd),
        gen_server:reply(From, ok),
        writer_loop(Fd, FilePath, Eof, CloseTimeout);
    {set_close_after, NewCloseTimeout, From} ->
        From ! {ok, self()},
        writer_loop(Fd, FilePath, Eof, NewCloseTimeout);
    {rename, NewFilepath, From} ->
        ok = file2:rename(FilePath, NewFilepath),
        ok = couch_file_write_guard:remove(self()),
        ok = couch_file_write_guard:add(NewFilepath, self()),
        From ! {ok, self()},
        writer_loop(Fd, NewFilepath, Eof, CloseTimeout);
    {'EXIT', _From, Reason} ->
        ok = couch_file_write_guard:remove(self()),
        ok = file:close(Fd),
        exit(Reason)
    end.

writer_collect_chunks(Fd, FilePath, Eof, CloseTimeout, Acc) ->
    receive
    {chunk, Chunk} ->
        writer_collect_chunks(Fd, FilePath, Eof, CloseTimeout, [Chunk | Acc]);
    Msg ->
        Eof2 = write_blocks(Fd, Eof, Acc),
        handle_write_message(Msg, Fd, FilePath, Eof2, CloseTimeout)
    after 0 ->
        Eof2 = write_blocks(Fd, Eof, Acc),
        writer_loop(Fd, FilePath, Eof2, CloseTimeout)
    end.


write_blocks(Fd, Eof, Data) ->
    Blocks = make_blocks(Eof rem ?SIZE_BLOCK, lists:reverse(Data)),
    ok = file:write(Fd, Blocks),
    Eof + iolist_size(Blocks).

write_header_blocks(Fd, Eof, Header) ->
    case Eof rem ?SIZE_BLOCK of
    0 ->
        Padding = <<>>;
    BlockOffset ->
        Padding = <<0:(8 * (?SIZE_BLOCK - BlockOffset))>>
    end,
    FinalHeader = [
        Padding,
        <<1, (byte_size(Header)):32/integer>> | make_blocks(5, [Header])
    ],
    ok = file:write(Fd, FinalHeader),
    Eof + iolist_size(FinalHeader).


reader_loop(Fd, FilePath, CloseTimeout) ->
    receive
    Msg ->
        handle_reader_message(Msg, Fd, FilePath, CloseTimeout)
    after CloseTimeout ->
        % after nonuse timeout we close the Fd.
        file:close(Fd),
        erlang:hibernate(?MODULE, reader_loop_wakeup, [FilePath, CloseTimeout])
    end.

reader_loop_wakeup(FilePath, CloseTimeout) ->
    receive
        {'EXIT', _From, Reason} ->
            exit(Reason);
        Msg ->
            case try_open_fd(FilePath, [binary, read, raw], ?RETRY_TIME_MS,
                ?MAX_RETRY_TIME_MS) of
            {ok, Fd2} ->
                handle_reader_message(Msg, Fd2, FilePath, CloseTimeout);
            Other ->
                erlang:exit({problem_reopening_file, Other, Msg, self(), FilePath, CloseTimeout})
            end
    end.

handle_reader_message(Msg, Fd, FilePath, CloseTimeout) ->
    case Msg of
    {read, Pos, From} ->
        gen_server:reply(From, read_iolist(Fd, Pos)),
        reader_loop(Fd, FilePath, CloseTimeout);
    {find_header, Pos, From} ->
        gen_server:reply(From, do_find_header(Fd, Pos div ?SIZE_BLOCK)),
        reader_loop(Fd, FilePath, CloseTimeout);
    {read_header, Pos, From} ->
        Result = (catch load_header(Fd, Pos div ?SIZE_BLOCK)),
        gen_server:reply(From, Result),
        reader_loop(Fd, FilePath, CloseTimeout);
    {get_eof, From} ->
        {ok, Pos} = file:position(Fd, eof),
        From ! {eof, Pos, self()},
        reader_loop(Fd, FilePath, CloseTimeout);
    {set_close_after, NewCloseTimeout, From} ->
        From ! {ok, self()},
        reader_loop(Fd, FilePath, NewCloseTimeout);
    {rename, NewFilepath, From} ->
        From ! {ok, self()},
        reader_loop(Fd, NewFilepath, CloseTimeout);
    {'EXIT', _From, Reason} ->
        ok = file:close(Fd),
        exit(Reason)
    end.


read_iolist(Fd, Pos) ->
    try
        do_read_iolist(Fd, Pos)
    catch throw:{read_error, Error} ->
        Error
    end.

do_read_iolist(Fd, Pos) ->
    {RawData, NextPos} = try
        % up to 8Kbs of read ahead
        read_raw_iolist_int(Fd, Pos, 2 * ?SIZE_BLOCK - (Pos rem ?SIZE_BLOCK))
    catch
    _:_ ->
        read_raw_iolist_int(Fd, Pos, 4)
    end,
    {Begin, RestRawData} = couch_util:split_iolist(RawData, 4),
    <<Prefix:1/integer, Len:31/integer>> = iolist_to_binary(Begin),
    case Prefix of
    1 ->
        {Md5, Data} = extract_crc32(
            maybe_read_more_iolist(RestRawData, 4 + Len, NextPos, Fd)),
        {ok, Data, Md5};
    0 ->
        {ok, maybe_read_more_iolist(RestRawData, Len, NextPos, Fd)}
    end.
