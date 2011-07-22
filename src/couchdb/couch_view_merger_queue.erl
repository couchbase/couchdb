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

-module(couch_view_merger_queue).

% TODO: try to simplify this module, taking into account efficient/practical
% use by couch_view_merger.erl

% public API
-export([start_link/2]).
% consumer API
-export([peek/1, peek_next/1, unpeek/1, flush/1]).
% producer API
-export([queue/2, done/1]).


% gen_server callbacks
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("couch_db.hrl").

-record(state, {
    rows,                     % a functional priority queue (skew)
    peeked = [],              % list of so far peeked items
    consumer = nil,           % peek request (only 1 consumer is supported)
    less_fun,
    num_producers
}).


start_link(NumProducers, LessFun) when is_integer(NumProducers), NumProducers > 0 ->
    gen_server:start_link(?MODULE, {NumProducers, LessFun}, []).

peek(Pid) ->
    try
        gen_server:call(Pid, peek, infinity)
    catch _:_ ->
        closed
    end.

peek_next(Pid) ->
    gen_server:call(Pid, peek_next, infinity).

unpeek(Pid) ->
    ok = gen_server:cast(Pid, unpeek).

queue(Pid, Row) ->
    ok = gen_server:call(Pid, {queue, Row}, infinity).

flush(Pid) ->
    ok = gen_server:cast(Pid, flush).

% Producers call this when they're done (they will not queue anymore).
done(Pid) ->
    ok = gen_server:cast(Pid, done).


init({NumProducers, LessFun}) ->
    State = #state{
        num_producers = NumProducers,
        rows = couch_skew:new(),
        less_fun = fun({_, A}, {_, B}) -> LessFun(A, B) end
    },
    {ok, State}.


handle_call(peek, From, #state{peeked = []} = State) ->
    #state{
        less_fun = LessFun,
        rows = Rows,
        num_producers = N
    } = State,
    case couch_skew:size(Rows) < N of
    true ->
        {noreply, State#state{consumer = From}};
    false ->
        {{_, MinRow} = X, Rows2} = couch_skew:out(LessFun, Rows),
        {reply, {ok, MinRow}, State#state{rows = Rows2, peeked = [X]}}
    end;

handle_call(peek_next, _From, #state{peeked = [_ | _] = Peeked} = State) ->
    #state{rows = Rows, less_fun = LessFun} = State,
    case couch_skew:size(Rows) of
    0 ->
        {reply, empty, State};
    _ ->
        {{_, MinRow} = X, Rows2} = couch_skew:out(LessFun, Rows),
        NewState = State#state{rows = Rows2, peeked = [X | Peeked]},
        {reply, {ok, MinRow}, NewState}
    end;

handle_call({queue, Row}, From, #state{num_producers = N} = State) when N > 0 ->
    #state{
        less_fun = LessFun,
        rows = Rows,
        consumer = Consumer,
        peeked = Peeked
    } = State,
    Rows2 = couch_skew:in({From, Row}, LessFun, Rows),
    case (Consumer =/= nil) andalso (couch_skew:size(Rows2) >= N) of
    true ->
        {{_, MinRow} = X, Rows3} = couch_skew:out(LessFun, Rows2),
        gen_server:reply(Consumer, {ok, MinRow}),
        Peeked2 = [X | Peeked],
        Consumer2 = nil;
    false ->
        Peeked2 = Peeked,
        Rows3 = Rows2,
        Consumer2 = Consumer
    end,
    NewState = State#state{
        rows = Rows3,
        consumer = Consumer2,
        peeked = Peeked2
    },
    {noreply, NewState}.


% Consumer "undoes" last peek. Doesn't make much sense in regular
% queing terminology.
handle_cast(unpeek, #state{peeked = [X | Rest]} = State) ->
    #state{less_fun = LessFun, rows = Rows} = State,
    Rows2 = couch_skew:in(X, LessFun, Rows),
    {noreply, State#state{rows = Rows2, peeked = Rest}};

% Consumer should call this after doing its processing of the previously
% peeked rows.
handle_cast(flush, #state{consumer = nil} = State) ->
    #state{
        peeked = Peeked,
        num_producers = N,
        rows = Rows
    } = State,
    lists:foreach(fun({Req, _}) -> gen_server:reply(Req, ok) end, Peeked),
    case (N =:= 0) andalso (couch_skew:size(Rows) =:= 0) of
    true ->
        {stop, normal, State#state{peeked = []}};
    false ->
        {noreply, State#state{peeked = []}}
    end;

handle_cast(done, #state{consumer = nil, num_producers = NumProds} = State) ->
    NumProds2 = NumProds - 1,
    case NumProds2 of
    0 ->
        case couch_skew:size(State#state.rows) > 0 of
        true ->
            {noreply, State#state{num_producers = NumProds2}};
        false ->
            {stop, normal, State#state{num_producers = NumProds2}}
        end;
    _ ->
        {noreply, State#state{num_producers = NumProds2}}
    end;

handle_cast(done, #state{peeked = []} = State) ->
    #state{
        less_fun = LessFun,
        rows = Rows,
        num_producers = NumProds,
        consumer = Consumer
    } = State,
    NumProds2 = NumProds - 1,
    case NumProds2 of
    0 ->
        gen_server:reply(Consumer, closed),
        {stop, normal, State};
    _ ->
        case couch_skew:size(Rows) < NumProds2 of
        true ->
            {noreply, State#state{num_producers = NumProds2}};
        false ->
            {{_, MinRow} = X, Rows2} = couch_skew:out(LessFun, Rows),
            gen_server:reply(Consumer, {ok, MinRow}),
            NewState = State#state{
                num_producers = NumProds2,
                consumer = nil,
                rows = Rows2,
                peeked = [X]
            },
            {noreply, NewState}
        end
    end.


handle_info(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
