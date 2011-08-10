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

-record(file, {
    reader = nil,
    writer = nil,
    eof = 0
}).

% public API
-export([open/1, open/2, close/1, bytes/1, flush/1, sync/1, truncate/2]).
-export([pread_term/2, pread_iolist/2, pread_binary/2]).
-export([append_binary/2, append_binary_md5/2]).
-export([append_raw_chunk/2, assemble_file_chunk/1, assemble_file_chunk/2]).
-export([append_term/2, append_term/3, append_term_md5/2, append_term_md5/3]).
-export([write_header/2, read_header/1]).
-export([delete/2, delete/3, init_delete_dir/1]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

%%----------------------------------------------------------------------
%% Args:   Valid Options are [create] and [create,overwrite].
%%  Files are opened in read/write mode.
%% Returns: On success, {ok, Fd}
%%  or {error, Reason} if the file could not be opened.
%%----------------------------------------------------------------------

open(Filepath) ->
    open(Filepath, []).

open(Filepath, Options) ->
    case gen_server:start_link(couch_file,
            {Filepath, Options, self(), Ref = make_ref()}, []) of
    {ok, Fd} ->
        {ok, Fd};
    ignore ->
        % get the error
        receive
        {Ref, Pid, Error} ->
            case process_info(self(), trap_exit) of
            {trap_exit, true} -> receive {'EXIT', Pid, _} -> ok end;
            {trap_exit, false} -> ok
            end,
            case Error of
            {error, eacces} -> {file_permission_error, Filepath};
            _ -> Error
            end
        end;
    Error ->
        Error
    end.


%%----------------------------------------------------------------------
%% Purpose: To append an Erlang term to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos, NumBytesWritten} where Pos is the file offset to
%%  the beginning the serialized  term. Use pread_term to read the term
%%  back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_term(Fd, Term) ->
    append_term(Fd, Term, []).

append_term(Fd, Term, Options) ->
    Comp = couch_util:get_value(compression, Options, ?DEFAULT_COMPRESSION),
    append_binary(Fd, couch_compress:compress(Term, Comp)).

append_term_md5(Fd, Term) ->
    append_term_md5(Fd, Term, []).

append_term_md5(Fd, Term, Options) ->
    Comp = couch_util:get_value(compression, Options, ?DEFAULT_COMPRESSION),
    append_binary_md5(Fd, couch_compress:compress(Term, Comp)).

%%----------------------------------------------------------------------
%% Purpose: To append an Erlang binary to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos, NumBytesWritten} where Pos is the file offset to the
%%  beginning the serialized term. Use pread_term to read the term back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_binary(Fd, Bin) ->
    gen_server:call(Fd, {append_bin, assemble_file_chunk(Bin)}, infinity).
    
append_binary_md5(Fd, Bin) ->
    gen_server:call(Fd,
        {append_bin, assemble_file_chunk(Bin, couch_util:md5(Bin))}, infinity).

append_raw_chunk(Fd, Chunk) ->
    gen_server:call(Fd, {append_bin, Chunk}, infinity).

assemble_file_chunk(Bin) ->
    [<<0:1/integer, (iolist_size(Bin)):31/integer>>, Bin].

assemble_file_chunk(Bin, Md5) ->
    [<<1:1/integer, (iolist_size(Bin)):31/integer>>, Md5, Bin].

%%----------------------------------------------------------------------
%% Purpose: Reads a term from a file that was written with append_term
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------


pread_term(Fd, Pos) ->
    {ok, Bin} = pread_binary(Fd, Pos),
    {ok, couch_compress:decompress(Bin)}.


%%----------------------------------------------------------------------
%% Purpose: Reads a binrary from a file that was written with append_binary
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------

pread_binary(Fd, Pos) ->
    {ok, L} = pread_iolist(Fd, Pos),
    {ok, iolist_to_binary(L)}.


pread_iolist(Fd, Pos) ->
    case gen_server:call(Fd, {pread_iolist, Pos}, infinity) of
    {ok, _IoList} = Ok ->
        Ok;
    {ok, IoList, Md5} ->
        case couch_util:md5(IoList) of
        Md5 ->
            {ok, IoList};
        _ ->
            exit({file_corruption, <<"file corruption">>})
        end;
    Error ->
        Error
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


delete(RootDir, Filepath) ->
    delete(RootDir, Filepath, true).


delete(RootDir, Filepath, Async) ->
    DelFile = filename:join([RootDir,".delete", ?b2l(couch_uuids:random())]),
    case file:rename(Filepath, DelFile) of
    ok ->
        if (Async) ->
            spawn(file, delete, [DelFile]),
            ok;
        true ->
            file:delete(DelFile)
        end;
    Error ->
        Error
    end.


init_delete_dir(RootDir) ->
    Dir = filename:join(RootDir,".delete"),
    % note: ensure_dir requires an actual filename companent, which is the
    % reason for "foo".
    filelib:ensure_dir(filename:join(Dir,"foo")),
    filelib:fold_files(Dir, ".*", true,
        fun(Filename, _) ->
            ok = file:delete(Filename)
        end, ok).


read_header(Fd) ->
    case gen_server:call(Fd, find_header, infinity) of
    {ok, Bin} ->
        {ok, binary_to_term(Bin)};
    no_valid_header ->
        flush(Fd),
        case gen_server:call(Fd, find_header, infinity) of
        {ok, Bin} ->
            {ok, binary_to_term(Bin)};
        Else ->
            Else
        end;
    Else ->
        Else
    end.

write_header(Fd, Data) ->
    Bin = term_to_binary(Data),
    Md5 = couch_util:md5(Bin),
    % now we assemble the final header binary and write to disk
    FinalBin = <<Md5/binary, Bin/binary>>,
    ok = gen_server:call(Fd, {write_header, FinalBin}, infinity).




init_status_error(ReturnPid, Ref, Error) ->
    ReturnPid ! {Ref, self(), Error},
    ignore.

% server functions

init({Filepath, Options, ReturnPid, Ref}) ->
   try
       maybe_create_file(Filepath, Options),
       process_flag(trap_exit, true),
       Reader = spawn_reader(Filepath),
       {Writer, Eof} = spawn_writer(Filepath),
       maybe_track_open_os_files(Options),
       {ok, #file{reader = Reader, writer = Writer, eof = Eof}}
   catch
   throw:{error, Err} ->
       init_status_error(ReturnPid, Ref, Err)
   end.

maybe_create_file(Filepath, Options) ->
    case lists:member(create, Options) of
    true ->
        filelib:ensure_dir(Filepath),
        case file:open(Filepath, [read, write, binary]) of
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
                    throw({error, file_exists})
                end;
            false ->
                ok
            end;
        Error ->
            throw({error, Error})
        end;
    false ->
        ok
    end.

maybe_track_open_os_files(FileOptions) ->
    case lists:member(sys_db, FileOptions) of
    true ->
        ok;
    false ->
        couch_stats_collector:track_process_count({couchdb, open_os_files})
    end.

terminate(_Reason, #file{reader = Reader, writer = Writer}) ->
    couch_util:shutdown_sync(Reader),
    couch_util:shutdown_sync(Writer).

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

handle_call({append_bin, Bin}, From, #file{writer = W, eof = Pos} = File) ->
    Size = calculate_total_read_len(Pos rem ?SIZE_BLOCK, iolist_size(Bin)),
    gen_server:reply(From, {ok, Pos, Size}),
    W ! {chunk, Bin},
    {noreply, File#file{eof = Pos + Size}};

handle_call({write_header, Bin}, From, #file{writer = W, eof = Pos} = File) ->
    gen_server:reply(From, ok),
    W ! {header, Bin},
    Pos2 = case Pos rem ?SIZE_BLOCK of
    0 ->
        Pos + 5;
    BlockOffset ->
        Pos + 5 + (?SIZE_BLOCK - BlockOffset)
    end,
    File2 = File#file{
        eof = Pos2 + calculate_total_read_len(5, byte_size(Bin))
    },
    {noreply, File2};

handle_call(flush, From, #file{writer =  W} = File) ->
    W ! {flush, From},
    {noreply, File};

handle_call(find_header, From, #file{reader = Reader, eof = Eof} = File) ->
    Reader ! {find_header, Eof, From},
    {noreply, File}.

handle_cast(close, Fd) ->
    {stop,normal,Fd}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info({'EXIT', _, normal}, Fd) ->
    {noreply, Fd};
handle_info({'EXIT', Pid, Reason}, #file{writer = Pid} = Fd) ->
    {stop, {write_loop_died, Reason}, Fd};
handle_info({'EXIT', Pid, Reason}, #file{reader = Pid} = Fd) ->
    {stop, {read_loop_died, Reason}, Fd};
handle_info({'EXIT', _, Reason}, Fd) ->
    {stop, Reason, Fd}.


find_header(_Fd, -1) ->
    no_valid_header;
find_header(Fd, Block) ->
    case (catch load_header(Fd, Block)) of
    {ok, Bin} ->
        {ok, Bin};
    _Error ->
        find_header(Fd, Block -1)
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
    <<Md5Sig:16/binary, HeaderBin/binary>> =
        iolist_to_binary(remove_block_prefixes(1, RawBin)),
    Md5Sig = couch_util:md5(HeaderBin),
    {ok, HeaderBin}.

maybe_read_more_iolist(Buffer, DataSize, _, _)
    when DataSize =< byte_size(Buffer) ->
    <<Data:DataSize/binary, _/binary>> = Buffer,
    [Data];
maybe_read_more_iolist(Buffer, DataSize, NextPos, Fd) ->
    {Missing, _} =
        read_raw_iolist_int(Fd, NextPos, DataSize - byte_size(Buffer)),
    [Buffer, Missing].

read_raw_iolist_int(ReadFd, {Pos, _Size}, Len) -> % 0110 UPGRADE CODE
    read_raw_iolist_int(ReadFd, Pos, Len);
read_raw_iolist_int(ReadFd, Pos, Len) ->
    BlockOffset = Pos rem ?SIZE_BLOCK,
    TotalBytes = calculate_total_read_len(BlockOffset, Len),
    {ok, <<RawBin:TotalBytes/binary>>} = file:pread(ReadFd, Pos, TotalBytes),
    {remove_block_prefixes(BlockOffset, RawBin), Pos + TotalBytes}.

-spec extract_md5(iolist()) -> {binary(), iolist()}.
extract_md5(FullIoList) ->
    {Md5List, IoList} = split_iolist(FullIoList, 16, []),
    {iolist_to_binary(Md5List), IoList}.

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

remove_block_prefixes(_BlockOffset, <<>>) ->
    [];
remove_block_prefixes(0, <<_BlockPrefix,Rest/binary>>) ->
    remove_block_prefixes(1, Rest);
remove_block_prefixes(BlockOffset, Bin) ->
    BlockBytesAvailable = ?SIZE_BLOCK - BlockOffset,
    case size(Bin) of
    Size when Size > BlockBytesAvailable ->
        <<DataBlock:BlockBytesAvailable/binary,Rest/binary>> = Bin,
        [DataBlock | remove_block_prefixes(0, Rest)];
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
        case split_iolist(IoList, (?SIZE_BLOCK - BlockOffset), []) of
        {Begin, End} ->
            [Begin | make_blocks(0, End)];
        _SplitRemaining ->
            IoList
        end
    end.

%% @doc Returns a tuple where the first element contains the leading SplitAt
%% bytes of the original iolist, and the 2nd element is the tail. If SplitAt
%% is larger than byte_size(IoList), return the difference.
-spec split_iolist(IoList::iolist(), SplitAt::non_neg_integer(), Acc::list()) ->
    {iolist(), iolist()} | non_neg_integer().
split_iolist(List, 0, BeginAcc) ->
    {lists:reverse(BeginAcc), List};
split_iolist([], SplitAt, _BeginAcc) ->
    SplitAt;
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc) when SplitAt > byte_size(Bin) ->
    split_iolist(Rest, SplitAt - byte_size(Bin), [Bin | BeginAcc]);
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc) ->
    <<Begin:SplitAt/binary,End/binary>> = Bin,
    split_iolist([End | Rest], 0, [Begin | BeginAcc]);
split_iolist([Sublist| Rest], SplitAt, BeginAcc) when is_list(Sublist) ->
    case split_iolist(Sublist, SplitAt, BeginAcc) of
    {Begin, End} ->
        {Begin, [End | Rest]};
    SplitRemaining ->
        split_iolist(Rest, SplitAt - (SplitAt - SplitRemaining), [Sublist | BeginAcc])
    end;
split_iolist([Byte | Rest], SplitAt, BeginAcc) when is_integer(Byte) ->
    split_iolist(Rest, SplitAt - 1, [Byte | BeginAcc]).


spawn_writer(Filepath) ->
    Parent = self(),
    Pid = spawn_link(fun() ->
        case file:open(Filepath, [binary, append, raw]) of
        {ok, Fd} ->
            {ok, Eof} = file:position(Fd, eof),
            Parent ! {self(), {ok, Eof}},
            writer_loop(Fd, Eof);
        Error ->
            Parent ! {self(), Error}
        end
    end),
    receive
    {Pid, {ok, Eof}} ->
         {Pid, Eof};
    {Pid, Error} ->
         throw({error, Error})
    end.


spawn_reader(Filepath) ->
    Parent = self(),
    Pid = spawn_link(fun() ->
        case file:open(Filepath, [binary, read, raw]) of
        {ok, Fd} ->
            Parent ! {self(), ok},
            reader_loop(Fd);
        Error ->
            Parent ! {self(), Error}
        end
    end),
    receive
    {Pid, ok} ->
         Pid;
    {Pid, Error} ->
         throw({error, Error})
    end.


writer_loop(Fd, Eof) ->
    receive
    {chunk, Chunk} ->
        writer_collect_chunks(Fd, Eof, [Chunk]);
    {header, Header} ->
        Eof2 = write_header_blocks(Fd, Eof, Header),
        writer_loop(Fd, Eof2);
    {truncate, Pos, From} ->
        {ok, Pos} = file:position(Fd, Pos),
        ok = file:truncate(Fd),
        From ! {self(), truncated, Pos},
        writer_loop(Fd, Pos);
    {flush, From} ->
        gen_server:reply(From, ok),
        writer_loop(Fd, Eof);
    {sync, From} ->
        ok = file:sync(Fd),
        gen_server:reply(From, ok),
        writer_loop(Fd, Eof);
    stop ->
        ok = file:close(Fd),
        exit(done)
    end.

writer_collect_chunks(Fd, Eof, Acc) ->
    receive
    {chunk, Chunk} ->
        writer_collect_chunks(Fd, Eof, [Chunk | Acc]);
    {header, Header} ->
        Eof2 = write_blocks(Fd, Eof, Acc),
        Eof3 = write_header_blocks(Fd, Eof2, Header),
        writer_loop(Fd, Eof3);
    {truncate, Pos, From} ->
        _ = write_blocks(Fd, Eof, Acc),
        {ok, Pos} = file:position(Fd, Pos),
        ok = file:truncate(Fd),
        From ! {self(), truncated, Pos},
        writer_loop(Fd, Pos);
    {flush, From} ->
        Eof2 = write_blocks(Fd, Eof, Acc),
        gen_server:reply(From, ok),
        writer_loop(Fd, Eof2);
    {sync, From} ->
        Eof2 = write_blocks(Fd, Eof, Acc),
        ok = file:sync(Fd),
        gen_server:reply(From, ok),
        writer_loop(Fd, Eof2)
    after 0 ->
        Eof2 = write_blocks(Fd, Eof, Acc),
        writer_loop(Fd, Eof2)
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


reader_loop(Fd) ->
    receive
    {read, Pos, From} ->
        read_iolist(Fd, Pos, From),
        reader_loop(Fd);
    {find_header, Eof, From} ->
        gen_server:reply(From, find_header(Fd, Eof div ?SIZE_BLOCK)),
        reader_loop(Fd);
    stop ->
        ok = file:close(Fd),
        exit(done)
    end.


-compile({inline, [read_iolist/3]}).

read_iolist(Fd, Pos, From) ->
    {RawData, NextPos} = try
        % up to 8Kbs of read ahead
        read_raw_iolist_int(Fd, Pos, 2 * ?SIZE_BLOCK - (Pos rem ?SIZE_BLOCK))
    catch
    _:_ ->
        read_raw_iolist_int(Fd, Pos, 4)
    end,
    <<Prefix:1/integer, Len:31/integer, RestRawData/binary>> =
        iolist_to_binary(RawData),
    case Prefix of
    1 ->
        {Md5, IoList} = extract_md5(
            maybe_read_more_iolist(RestRawData, 16 + Len, NextPos, Fd)),
        gen_server:reply(From, {ok, IoList, Md5});
    0 ->
        IoList = maybe_read_more_iolist(RestRawData, Len, NextPos, Fd),
        gen_server:reply(From, {ok, IoList})
    end.
