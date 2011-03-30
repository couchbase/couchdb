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

-module(couch_file_writer).
-behaviour(gen_server).

% public API
-export([start_link/1, close/1]).
-export([write_chunk/2, write_header/2]).
-export([flush/2, sync/1, truncate/2]).

-export([make_blocks/2, split_iolist/3]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-include("couch_db.hrl").

-define(SIZE_BLOCK, 4096).

-record(state, {
   fd,
   queue = queue:new(),
   pos = -1,              % most recently written position
   writer_pid,
   writer_req = nil,
   flush_waiting = ordsets:new(),
   close_req = nil,
   sync_req = nil
}).


start_link(FileName) ->
    gen_server:start_link(?MODULE, FileName, []).

write_chunk(Writer, Chunk) ->
    ok = gen_server:call(Writer, {queue_op, {chunk, Chunk}}, infinity).

write_header(Writer, Header) ->
    ok = gen_server:call(Writer, {queue_op, {header, Header}}, infinity).

truncate(Writer, Pos) ->
    ok = gen_server:call(Writer, {queue_op, {truncate, Pos}}, infinity).

flush(Writer, Pos) ->
    gen_server:call(Writer, {flush, {Pos, self()}}, infinity).

sync(Writer) ->
    ok = gen_server:call(Writer, {queue_op, sync}, infinity).

close(Writer) ->
    gen_server:call(Writer, close, infinity).


init(FileName) ->
    case file:open(FileName, [binary, append]) of
    {ok, Fd} ->
        {ok, Eof} = file:position(Fd, eof),
        process_flag(trap_exit, true),
        Parent = self(),
        WriterLoop = spawn_link(fun() -> writer_loop(Fd, Parent, Eof) end),
        {ok, #state{fd = Fd, writer_pid = WriterLoop}};
    Error ->
        {stop, Error}
    end.


handle_call({queue_op, sync}, From, #state{writer_req = nil} = State) ->
    {noreply, State#state{
        sync_req = From,
        queue = queue:in(sync, State#state.queue)
    }};

handle_call({queue_op, Op}, From, #state{writer_req = nil} = State) ->
    gen_server:reply(From, ok),
    {noreply, State#state{queue = queue:in(Op, State#state.queue)}};

handle_call({queue_op, Op}, From, #state{writer_req = WReq} = State) ->
    gen_server:reply(WReq, Op),
    case Op of
    sync ->
        {noreply, State#state{writer_req = nil, sync_req = From}};
    _ ->
        {reply, ok, State#state{writer_req = nil}}
    end;

handle_call({get_op, LastWrittenPos}, From, #state{queue = Q} = State) ->
    case queue:out(Q) of
    {empty, Q} ->
        FlushWaiting2 = flush_notify(State#state.flush_waiting, LastWrittenPos),
        case State#state.close_req of
        nil ->
            {noreply, State#state{
                flush_waiting = FlushWaiting2,
                writer_req = From,
                pos = LastWrittenPos}};
        CloseReq ->
            gen_server:reply(From, stop),
            ok = file:sync(State#state.fd),
            gen_server:reply(CloseReq, ok),
            {stop, normal, State}
        end;
    {{value, Op}, Q2} ->
        gen_server:reply(From, Op),
        FlushWaiting2 = flush_notify(State#state.flush_waiting, LastWrittenPos),
        {noreply, State#state{
            flush_waiting = FlushWaiting2,
            queue = Q2,
            pos = LastWrittenPos}}
    end;

handle_call({flush, {Pos, Pid}}, From, #state{pos = LastWrittenPos} = State) ->
    case LastWrittenPos >= Pos of
    true ->
        {reply, ok, State};
    false ->
        gen_server:reply(From, wait),
        Waiters2 = ordsets:add_element({Pos, Pid}, State#state.flush_waiting),
        {noreply, State#state{flush_waiting = Waiters2}}
    end;

handle_call(close, From, #state{writer_req = nil} = State) ->
    {noreply, State#state{close_req = From}};

handle_call(close, _From, #state{writer_req = Req, fd = Fd} = State) ->
    gen_server:reply(Req, stop),
    ok = file:sync(Fd),
    {stop, normal, ok, State};

handle_call(Msg, _From, State) ->
    {stop, {error, {unexpected_call, Msg}}, State}.


handle_cast(Msg, State) ->
    {stop, {error, {unexpected_cast, Msg}}, State}.


handle_info(synced, #state{sync_req = From} = State) ->
    gen_server:reply(From, ok),
    {noreply, State#state{sync_req = nil}};

handle_info({'EXIT', Pid, normal}, #state{writer_pid = Pid} = State) ->
    {noreply, State};

handle_info(Msg, State) ->
    {stop, {error, {unexpected_msg, Msg}}, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


writer_loop(Fd, Parent, Eof) ->
    case gen_server:call(Parent, {get_op, Eof}, infinity) of
    {chunk, Chunk} ->
        Blocks = make_blocks(Eof rem ?SIZE_BLOCK, [Chunk]),
        ok = file:write(Fd, Blocks),
        writer_loop(Fd, Parent, Eof + iolist_size(Blocks));
    {header, Header} ->
        case Eof rem ?SIZE_BLOCK of
        0 ->
            Padding = <<>>;
        BlockOffset ->
            Padding = <<0:(8 * (?SIZE_BLOCK - BlockOffset))>>
        end,
        FinalHeader = [
            Padding,
            <<1, (byte_size(Header)):32/integer>> | make_blocks(5, [Header])],
        ok = file:write(Fd, FinalHeader),
        writer_loop(Fd, Parent, Eof + iolist_size(FinalHeader));
    {truncate, Pos} ->
        {ok, Pos} = file:position(Fd, Pos),
        ok = file:truncate(Fd),
        writer_loop(Fd, Parent, Pos);
    sync ->
        ok = file:sync(Fd),
        Parent ! synced,
        writer_loop(Fd, Parent, Eof);
    stop ->
        ok
    end.


flush_notify([], _WrittenPos) ->
    [];
flush_notify([{Pos, _Pid} | _] = L, WrittenPos) when WrittenPos < Pos ->
    L;
flush_notify([{Pos, Pid} | Rest], WrittenPos) ->
    Pid ! {flushed, Pos},
    flush_notify(Rest, WrittenPos).


make_blocks(_BlockOffset, []) ->
    [];
make_blocks(0, IoList) ->
    [<<0>> | make_blocks(1, IoList)];
make_blocks(BlockOffset, IoList) ->
    case split_iolist(IoList, (?SIZE_BLOCK - BlockOffset), []) of
    {Begin, End} ->
        [Begin | make_blocks(0, End)];
    _SplitRemaining ->
        IoList
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
    <<Begin:SplitAt/binary, End/binary>> = Bin,
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
