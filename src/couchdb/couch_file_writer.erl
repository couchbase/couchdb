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
-export([write_chunk/2, flush/2, sync/1, truncate/2]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-include("couch_db.hrl").

-record(state, {
   fd,
   queue = queue:new(),
   pos = -1,              % most recently written position
   writer_pid,
   writer_req = nil,
   flush_waiting = ordsets:new(),
   close_req = nil,
   sync_pid = nil
}).


start_link(FileName) ->
    gen_server:start_link(?MODULE, FileName, []).

write_chunk(Writer, PosChunk) ->
    ok = gen_server:call(Writer, {queue_op, {write, PosChunk}}, infinity).

truncate(Writer, Pos) ->
    ok = gen_server:call(Writer, {queue_op, {truncate, Pos}}, infinity).

flush(Writer, Pos) ->
    ok = gen_server:call(Writer, {flush, {Pos, self()}}, infinity).

sync(Writer) ->
    ok = gen_server:call(Writer, sync, infinity).

close(Writer) ->
    gen_server:call(Writer, close, infinity).


init(FileName) ->
    case file:open(FileName, [binary, append]) of
    {ok, Fd} ->
        process_flag(trap_exit, true),
        Parent = self(),
        WriterLoop = spawn_link(fun() -> writer_loop(Fd, Parent, -1) end),
        {ok, #state{fd = Fd, writer_pid = WriterLoop}};
    Error ->
        {stop, Error}
    end.


handle_call({queue_op, Op}, From, #state{writer_req = nil} = State) ->
    gen_server:reply(From, ok),
    {noreply, State#state{queue = queue:in(Op, State#state.queue)}};

handle_call({queue_op, Op}, _From, #state{writer_req = WReq} = State) ->
    gen_server:reply(WReq, {ok, Op}),
    {reply, ok, State#state{writer_req = nil}};

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
        gen_server:reply(From, {ok, Op}),
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

handle_call(sync, _From, #state{sync_pid = nil, fd = Fd} = State) ->
    % TODO: maybe add a sync operation to the op queue
    {reply, ok, State#state{
        sync_pid = spawn_link(fun() -> ok = file:sync(Fd) end)}};

handle_call(sync, _From, State) ->
    {reply, ok, State};

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


handle_info({'EXIT', Pid, normal}, #state{sync_pid = Pid} = State) ->
    {noreply, State};

handle_info({'EXIT', Pid, normal}, #state{writer_pid = Pid} = State) ->
    {noreply, State};

handle_info(Msg, State) ->
    {stop, {error, {unexpected_msg, Msg}}, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


writer_loop(Fd, Parent, LastPos) ->
    case gen_server:call(Parent, {get_op, LastPos}, infinity) of
    {ok, {write, {Pos, Chunk}}} ->
        ok = file:write(Fd, Chunk),
        writer_loop(Fd, Parent, Pos);
    {ok, {truncate, Pos}} ->
        {ok, Pos} = file:position(Fd, Pos),
        ok = file:truncate(Fd),
        writer_loop(Fd, Parent, Pos);
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
