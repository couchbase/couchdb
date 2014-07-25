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

-module(couch_upr_client).
-behaviour(gen_server).

% Public API
-export([start/5]).
-export([add_stream/6, get_sequence_numbers/2, get_num_items/2,
    get_failover_log/2]).
-export([get_stream_event/2, remove_stream/2, list_streams/1]).
-export([enum_docs_since/8, restart_worker/1]).
-export([get_sequence_numbers_async/1, parse_stats_seqnos/1]).

% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).
-export([format_status/2]).

-include("couch_db.hrl").
-include_lib("couch_upr/include/couch_upr.hrl").
-include_lib("couch_upr/include/couch_upr_typespecs.hrl").
-define(MAX_BUF_SIZE, 10485760).
-define(TIMEOUT, 60000).
-define(TIMEOUT_STATS, 2000).
-define(UPR_RETRY_TIMEOUT, 2000).

-type mutations_fold_fun() :: fun().
-type mutations_fold_acc() :: any().

-record(state, {
    socket = nil                    :: socket(),
    timeout = 5000                  :: timeout(),
    request_id = 0                  :: request_id(),
    pending_requests = dict:new()   :: dict(),
    stream_queues = dict:new()      :: dict(),
    active_streams = []             :: list(),
    worker_pid                      :: pid(),
    max_buffer_size = ?MAX_BUF_SIZE :: integer(),
    total_buffer_size = 0           :: non_neg_integer(),
    stream_info = dict:new()        :: dict(),
    args = []                       :: list()
}).

-record(stream_info, {
    part_id = 0                     :: partition_id(),
    part_uuid = 0                   :: uuid(),
    start_seq = 0                   :: update_seq(),
    end_seq = 0                     :: update_seq(),
    snapshot_seq = {0, 0}           :: {update_seq(), update_seq()},
    flags = 0                       :: non_neg_integer()
}).

% This gen server implements a UPR client with vbucket stream multiplexing
% The client spawns a worker process to handle all response messages and event
% messages received from the UPR server. For easiness, responses are classifed into
% two types of messages, stream_response and stream_event. For any UPR request, the
% corresponding response message is called stream_response. But a vbucket stream request
% is like a subscribe mechanism. After the stream initated response arrives, it will start
% sending vbucket events. This type of messages are called as stream_event.
%
% The user can request for a stream using add_stream() API and corresponding events can
% be received by using get_stream_event() API.


% Public API

-spec start(binary(), binary(), binary(), binary(), non_neg_integer()) -> {ok, pid()} | ignore |
                                   {error, {already_started, pid()} | term()}.
start(Name, Bucket, AdmUser, AdmPasswd, BufferSize) ->
    gen_server:start_link(?MODULE, [Name, Bucket, AdmUser, AdmPasswd, BufferSize], []).

-spec add_stream(pid(), partition_id(), uuid(), update_seq(),
    update_seq(), upr_data_type()) -> {request_id(), term()}.
add_stream(Pid, PartId, PartUuid, StartSeq, EndSeq, Flags) ->
    gen_server:call(
        Pid, {add_stream, PartId, PartUuid, StartSeq, EndSeq, Flags}, ?TIMEOUT).


-spec remove_stream(pid(), partition_id()) ->
                            'ok' | {'error', term()}.
remove_stream(Pid, PartId) ->
    gen_server:call(Pid, {remove_stream, PartId}, ?TIMEOUT).


-spec list_streams(pid()) -> list().
list_streams(Pid) ->
    gen_server:call(Pid, list_streams).


-spec get_stats_reply(pid(), reference()) -> term().
get_stats_reply(Pid, MRef) ->
    receive
    {get_stats, MRef, Reply} ->
        Reply;
    {'DOWN', MRef, process, Pid, Reason} ->
        exit({upr_client_died, Pid, Reason})
    after ?TIMEOUT_STATS ->
        ?LOG_ERROR("dcp client (~p): vbucket-seqno stats timed out after ~p seconds."
                   " Waiting...",
            [Pid, ?TIMEOUT_STATS / 1000]),
        get_stats_reply(Pid, MRef)
    end.


-spec get_stats(pid(), binary(), partition_id() | nil) -> term().
get_stats(Pid, Name, PartId) ->
    MRef = erlang:monitor(process, Pid),
    Pid ! {get_stats, Name, PartId, {MRef, self()}},
    Reply = get_stats_reply(Pid, MRef),
    erlang:demonitor(MRef, [flush]),
    Reply.


% The async get_sequence_numbers API requests asynchronous stats.
% The response will be sent to the caller process asynchronously in the
% following message format:
% {get_stats, nil, StatsResponse}.
% The receiver needs to use parse_stats_seqnos() method to parse the
% response to valid seqnos format.
-spec get_sequence_numbers_async(pid()) -> ok.
get_sequence_numbers_async(Pid) ->
    Pid ! {get_stats, <<"vbucket-seqno">>, nil, {nil, self()}},
    ok.

-spec parse_stats_seqnos([{binary(), binary()}]) -> [{partition_id(), update_seq()}].
parse_stats_seqnos(Stats) ->
    lists:foldr(fun({Key, SeqBin}, Acc) ->
        case binary:split(Key, <<":">>) of
        [<<"vb_", PartIdBin/binary>>, <<"high_seqno">>] ->
            PartId = list_to_integer(binary_to_list(PartIdBin)),
            Seq = list_to_integer(binary_to_list(SeqBin)),
            [{PartId, Seq} | Acc];
        _ ->
            Acc
        end
    end, [], Stats).


-spec get_sequence_numbers(pid(), [partition_id()]) ->
         {ok, [update_seq()] | {error, not_my_vbucket}} | {error, term()}.
get_sequence_numbers(Pid, PartIds) ->
    Reply = get_stats(Pid, <<"vbucket-seqno">>, nil),
    case Reply of
    {ok, Stats} ->
        Seqs = lists:map(fun(PartId) ->
            Key = list_to_binary([<<"vb_">>, integer_to_list(PartId), <<":high_seqno">>]),
            case lists:keyfind(Key, 1, Stats) of
            {Key, SeqBin} ->
                list_to_integer(binary_to_list(SeqBin));
            false ->
                {error, not_my_vbucket}
            end
        end, PartIds),
        {ok, Seqs};
    {error, _Error} = Error ->
        Error
    end.


-spec get_num_items(pid(), partition_id()) ->
                           {ok, non_neg_integer()} | {error, not_my_vbucket}.
get_num_items(Pid, PartId) ->
    Reply = get_stats(Pid, <<"vbucket-details">>, PartId),
    case Reply of
    {ok, Stats} ->
        BinPartId = list_to_binary(integer_to_list(PartId)),
        {_, NumItems} = lists:keyfind(
            <<"vb_", BinPartId/binary, ":num_items">>, 1, Stats),
        {ok, list_to_integer(binary_to_list(NumItems))};
    {error, {?UPR_STATUS_NOT_MY_VBUCKET, _}} ->
        {error, not_my_vbucket}
    end.


-spec get_failover_log(pid(), partition_id()) ->
                              {error, no_failover_log_found | upr_status()} |
                              {ok, partition_version()}.
get_failover_log(Pid, PartId) ->
    gen_server:call(Pid, {get_failover_log, PartId}).


-spec get_stream_event(pid(), request_id()) ->
                              {atom(), #upr_doc{}} | {'error', term()}.
get_stream_event(Pid, ReqId) ->
    MRef = erlang:monitor(process, Pid),
    Pid ! {get_stream_event, ReqId, self()},
    Reply = get_stream_event_get_reply(Pid, ReqId, MRef),
    erlang:demonitor(MRef, [flush]),
    Reply.

get_stream_event_get_reply(Pid, ReqId, MRef) ->
    receive
    {stream_event, ReqId, Reply} ->
        Reply;
    {'DOWN', MRef, process, Pid, Reason} ->
        exit({upr_client_died, Pid, Reason})
    after ?TIMEOUT ->
        Msg = {print_log, ReqId},
        Pid ! Msg,
        get_stream_event_get_reply(Pid, ReqId, MRef)
    end.




-spec enum_docs_since(pid(), partition_id(), partition_version(), update_seq(),
                      update_seq(), 0..255, mutations_fold_fun(),
                      mutations_fold_acc()) ->
                             {error, vbucket_stream_not_found |
                              wrong_start_sequence_number |
                              too_large_failover_log } |
                             {rollback, update_seq()} |
                             {ok, mutations_fold_acc(), partition_version()}.
enum_docs_since(_, _, [], _, _, _, _, _) ->
    % No matching partition version found. Recreate the index from scratch
    {rollback, 0};
enum_docs_since(Pid, PartId, PartVersions, StartSeq, EndSeq0, Flags,
        CallbackFn, InAcc) ->
    [PartVersion | PartVersionsRest] = PartVersions,
    {PartUuid, _} = PartVersion,
    EndSeq = case EndSeq0 < StartSeq of
    true ->
        ?LOG_INFO("dcp client (~p): Expecting a rollback for partition ~p. "
        "Found start_seqno > end_seqno (~p > ~p).",
        [Pid, PartId, StartSeq, EndSeq0]),
        StartSeq;
    false ->
        EndSeq0
    end,
    {RequestId, Resp} =  add_stream(
        Pid, PartId, PartUuid, StartSeq, EndSeq, Flags),
    case Resp of
    {failoverlog, FailoverLog} ->
        case length(FailoverLog) > ?UPR_MAX_FAILOVER_LOG_SIZE of
        true ->
            {error, too_large_failover_log};
        false ->
            InAcc2 = CallbackFn({part_versions, {PartId, FailoverLog}}, InAcc),
            InAcc3 = receive_events(Pid, RequestId, CallbackFn, InAcc2),
            {ok, InAcc3, FailoverLog}
        end;
    {error, vbucket_stream_not_found} ->
        enum_docs_since(Pid, PartId, PartVersionsRest, StartSeq, EndSeq,
            Flags, CallbackFn, InAcc);
    {error, vbucket_stream_tmp_fail} ->
        ?LOG_INFO("dcp client (~p): Temporary failure on stream request "
            "on partition ~p. Retrying...", [Pid, PartId]),
        timer:sleep(100),
        enum_docs_since(Pid, PartId, PartVersions, StartSeq, EndSeq,
            Flags, CallbackFn, InAcc);
    _ ->
        Resp
    end.


% gen_server callbacks

-spec init([binary() | non_neg_integer()]) -> {ok, #state{}} |
                    {stop, sasl_auth_failed | closed | inet:posix()}.
init([Name, Bucket, AdmUser, AdmPasswd, BufferSize]) ->
    UprTimeout = list_to_integer(
        couch_config:get("dcp", "connection_timeout")),
    UprPort = list_to_integer(couch_config:get("dcp", "port")),
    {ok, Socket} = gen_tcp:connect("localhost", UprPort,
        [binary, {packet, raw}, {active, false}, {nodelay, true}]),
    State = #state{
        socket = Socket,
        timeout = UprTimeout,
        request_id = 0
    },

    % Auth as admin and select bucket for the connection
    case sasl_auth(AdmUser, AdmPasswd, State) of
    {ok, State2} ->
        case select_bucket(Bucket, State2) of
        {ok, State3} ->
            % Store the meta information to reconnect
            Args = [Name, Bucket, AdmUser, AdmPasswd, BufferSize],
            State4 = State3#state{args = Args},
            case open_connection(Name, State4) of
            {ok, State5} ->
                Parent = self(),
                process_flag(trap_exit, true),
                WorkerPid = spawn_link(
                    fun() -> receive_worker(Socket, UprTimeout, Parent, []) end),
                case set_buffer_size(State5, BufferSize) of
                {ok, State6} ->
                    {ok, State6#state{worker_pid = WorkerPid}};
                {error, Reason} ->
                    exit(WorkerPid, shutdown),
                    {stop, Reason}
                end;
            {error, Reason} ->
                {stop, Reason}
            end;
        {error, Reason} ->
            {stop, Reason}
        end;
    {error, Reason} ->
        {stop, Reason}
    end.


% Add caller to the request queue and wait for gen_server to reply on response arrival
handle_call({add_stream, PartId, PartUuid, StartSeq, EndSeq, Flags},
        From, State) ->
    % TODO vmx 2014-04-04: And proper retry if the last request didn't return
    % the full expected result
    SnapshotStart = StartSeq,
    SnapshotEnd = StartSeq,
    case add_new_stream({PartId, PartUuid, StartSeq, EndSeq,
        init, 0, {SnapshotStart, SnapshotEnd}, Flags}, From, State) of
    {error, Reason} ->
        {reply, {error, Reason}, State};
    State2 ->
        {noreply, State2}
    end;

handle_call({remove_stream, PartId}, From, State) ->
    State2 = remove_stream_info(PartId, State),
    #state{
       request_id = RequestId,
       socket = Socket
    } = State2,
    StreamCloseRequest = couch_upr_consumer:encode_stream_close(
        PartId, RequestId),
    case gen_tcp:send(Socket, StreamCloseRequest) of
    ok ->
        State3 = next_request_id(State2),
        State4 = add_pending_request(State3, RequestId, {remove_stream, PartId}, From),
        {noreply, State4};
    {error, _Reason} = Error ->
        {reply, Error, State2}
    end;

handle_call(list_streams, _From, State) ->
    #state{
       active_streams = ActiveStreams
    } = State,
    Reply = lists:foldl(fun({PartId, _}, Acc) -> [PartId | Acc] end, [], ActiveStreams),
    {reply, Reply, State};

handle_call({get_failover_log, PartId}, From, State) ->
    #state{
       request_id = RequestId,
       socket = Socket
    } = State,
    FailoverLogRequest = couch_upr_consumer:encode_failover_log_request(
        PartId, RequestId),
    case gen_tcp:send(Socket, FailoverLogRequest) of
    ok ->
        State2 = next_request_id(State),
        State3 = add_pending_request(State2, RequestId, get_failover_log, From),
        {noreply, State3};
    Error ->
        {reply, Error, State}
    end;

% Only used by unit test
handle_call(get_buffer_size, _From, #state{total_buffer_size = Size} = State) ->
    {reply, Size, State};

% Only used by unit test
handle_call(reset_buffer_size, _From, #state{total_buffer_size = Size} = State) ->
    State2 = State#state{total_buffer_size = 0},
    {reply, Size, State2};

% Only used by unit test
handle_call(flush_old_streams_meta, _From, State) ->
    {reply, ok, State#state{stream_info = dict:new()}};

% Only used by unit test
handle_call(get_socket, _From, #state{socket = Socket} = State) ->
    {reply, Socket, State};

% Only used by unit test
handle_call({get_buffer_size, RequestId}, _From,
        #state{stream_queues = StreamQueues} = State) ->
    {ok, {_, EvQueue}} = dict:find(RequestId, StreamQueues),
    Size = get_queue_size(EvQueue, 0),
    {reply, Size, State};

% Only used by unit test
handle_call({get_event_size, Event}, _From, State) ->
    Size = get_event_size(Event),
    {reply, Size, State}.

% If a stream event for this requestId is present in the queue,
% dequeue it and reply back to the caller.
% Else, put the caller into the stream queue waiter list
handle_info({get_stream_event, RequestId, From}, State) ->
    case stream_event_present(State, RequestId) of
    true ->
        Event = peek_stream_event(State, RequestId),
        {Msg, State6} = case check_and_send_buffer_ack(State, RequestId, Event, mutation) of
        {ok, State3} ->
            {State4, Event} = dequeue_stream_event(State3, RequestId),
            State5 = case Event of
            {stream_end, _, _} ->
                remove_request_queue(State4, RequestId);
            _ ->
                State4
            end,
            {remove_body_len(Event), State5};
        {error, Reason} ->
            {{error, Reason}, State}
        end,
        From ! {stream_event, RequestId, Msg},
        {noreply, State6};
    false ->
        case add_stream_event_waiter(State, RequestId, From) of
        nil ->
            Reply = {error, event_request_already_exists},
            From ! {stream_event, RequestId, Reply},
            {noreply, State};
        State2 ->
            {noreply, State2}
        end;
    nil ->
        Reply = {error, vbucket_stream_not_found},
        From ! {stream_event, RequestId, Reply},
        {noreply, State}
    end;


handle_info({get_stats, Stat, PartId, From}, State) ->
    #state{
       request_id = RequestId,
       socket = Socket
    } = State,
    SeqStatRequest = couch_upr_consumer:encode_stat_request(
        Stat, PartId, RequestId),
    case gen_tcp:send(Socket, SeqStatRequest) of
    ok ->
        State2 = next_request_id(State),
        State3 = add_pending_request(State2, RequestId, get_stats, From),
        {noreply, State3};
    Error ->
        {reply, Error, State}
    end;


% Handle response message send by connection receiver worker
% Reply back to waiting callers
handle_info({stream_response, RequestId, Msg}, State) ->
    State3 = case find_pending_request(State, RequestId) of
    {_, nil} ->
        State;
    {ReqInfo, SendTo} ->
        State2 = case ReqInfo of
        {add_stream, PartId} ->
            gen_server:reply(SendTo, Msg),
            case Msg of
            {_, {failoverlog, _}} ->
                add_request_queue(State, PartId, RequestId);
            _ ->
                State
            end;
        {remove_stream, PartId} ->
            gen_server:reply(SendTo, Msg),
            StreamReqId = find_stream_req_id(State, PartId),
            case check_and_send_buffer_ack(State, StreamReqId, nil, remove_stream) of
            {ok, NewState} ->
                case Msg of
                ok ->
                    remove_request_queue(NewState, StreamReqId);
                {error, vbucket_stream_not_found} ->
                    remove_request_queue(NewState, StreamReqId);
                _ ->
                    NewState
                end;
            {error, Error} ->
                throw({control_ack_failed, Error}),
                State
            end;
        % Server sent the response for the internal control request
        {control_request, Size} ->
            State#state{max_buffer_size = Size};
        get_stats ->
            {MRef, From} = SendTo,
            From ! {get_stats, MRef, Msg},
            State;
        _ ->
            gen_server:reply(SendTo, Msg),
            State
        end,
        remove_pending_request(State2, RequestId);
    nil ->
        State
    end,
    {noreply, State3};

% Respond with the no op message reply to server
handle_info({stream_noop, RequestId}, State) ->
    #state {
        socket = Socket
    } = State,
    NoOpResponse = couch_upr_consumer:encode_noop_response(RequestId),
    % if noop reponse fails two times, server it self will close the connection
    gen_tcp:send(Socket, NoOpResponse),
    {noreply, State};

% Handle events send by connection receiver worker
% If there is a waiting caller for stream event, reply to them
% Else, queue the event into the stream queue
handle_info({stream_event, RequestId, Event}, State) ->
    {Optype, Data, _Length} = Event,
    State2 = case Optype of
    snapshot_marker ->
        store_snapshot_seq(RequestId, Data, State);
    snapshot_mutation ->
        store_snapshot_mutation(RequestId, Data, State);
    stream_end ->
        #state{
            stream_info = StreamData
        } = State,
        StreamData2 = dict:erase(RequestId, StreamData),
        State#state{stream_info = StreamData2};
    snapshot_deletion ->
        store_snapshot_mutation(RequestId, Data, State)
    end,
    case stream_event_waiters_present(State2, RequestId) of
    true ->
        {State3, Waiter} = remove_stream_event_waiter(State2, RequestId),
        {Msg, State6} = case check_and_send_buffer_ack(State3, RequestId, Event, mutation) of
        {ok, State4} ->
            State5 = case Event of
            {stream_end, _, _} ->
                remove_request_queue(State4, RequestId);
            _ ->
                State4
            end,
            {remove_body_len(Event), State5};
        {error, Reason} ->
            State4 = enqueue_stream_event(State3, RequestId, Event),
            {{error, Reason}, State4}
        end,
        Waiter ! {stream_event, RequestId, Msg},
        {noreply, State6};
    false ->
        State3 = enqueue_stream_event(State2, RequestId, Event),
        {noreply, State3};
    nil ->
        % We might have explicitly closed a stream using close_stream command
        % Before the server received close_stream message, it would have placed
        % some mutations in the network buffer queue. We still need to acknowledge
        % the mutations received.
        {ok, State3} = check_and_send_buffer_ack(State, RequestId, Event, mutation),
        {noreply, State3}
    end;

handle_info({'EXIT', Pid, {conn_error, Reason}}, #state{worker_pid = Pid} = State) ->
    [Name, Bucket, _AdmUser, _AdmPasswd, _BufferSize] = State#state.args,
    ?LOG_ERROR("dcp client (~s, ~s): dcp receive worker failed due to reason: ~p."
        " Restarting upr receive worker...",
        [Bucket, Name, Reason]),
    timer:sleep(?UPR_RETRY_TIMEOUT),
    restart_worker(State);

handle_info({'EXIT', Pid, Reason}, #state{worker_pid = Pid} = State) ->
    {stop, Reason, State};

handle_info({print_log, ReqId}, State) ->
    [Name, Bucket, _AdmUser, _AdmPasswd, _BufferSize] = State#state.args,
    case find_stream_info(ReqId, State) of
    nil ->
        ok;
    StreamInfo ->
        #stream_info{
           start_seq = Start,
           end_seq = End,
           part_id = PartId
        } = StreamInfo,
        ?LOG_ERROR("dcp client (~s, ~s): Obtaining mutation from server timed out "
            "after ~p seconds [RequestId ~p, PartId ~p, StartSeq ~p, EndSeq ~p]. Waiting...",
            [Bucket, Name, ?TIMEOUT / 1000, ReqId, PartId, Start, End])
    end,
    {noreply, State};

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

-spec handle_cast(any(), #state{}) ->
                         {stop, {unexpected_cast, any()}, #state{}}.
handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.


-spec terminate(any(), #state{}) -> ok.
terminate(_Reason, #state{worker_pid = Pid}) ->
    exit(Pid, shutdown),
    ok.


-spec code_change(any(), #state{}, any()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


format_status(_Opt, [_PDict, #state{stream_queues = StreamQueues} = State]) ->
    TransformFn = fun(_Key, {Waiter, Queue}) ->
                     {Waiter, {queue:len(Queue), queue:peek_r(Queue)}}
                  end,
    State#state{stream_queues = dict:map(TransformFn, StreamQueues)}.


% Internal functions

-spec sasl_auth(binary(), binary(), #state{}) -> {ok, #state{}} |
                            {error, sasl_auth_failed | closed | inet:posix()}.
sasl_auth(User, Passwd, State) ->
    #state{
        socket = Socket,
        timeout = UprTimeout,
        request_id = RequestId
    } = State,
    Authenticate = couch_upr_consumer:encode_sasl_auth(User, Passwd, RequestId),
    case gen_tcp:send(Socket, Authenticate) of
    ok ->
        case socket_recv(Socket, ?UPR_HEADER_LEN, UprTimeout) of
        {ok, Header} ->
            {sasl_auth, Status, RequestId, BodyLength} =
                couch_upr_consumer:parse_header(Header),
            % Receive the body so that it is not mangled with the next request,
            % we care about the status only though
            case socket_recv(Socket, BodyLength, UprTimeout) of
            {ok, _} ->
                case Status of
                ?UPR_STATUS_OK ->
                    {ok, State#state{request_id = RequestId + 1}};
                ?UPR_STATUS_SASL_AUTH_FAILED ->
                    {error, sasl_auth_failed}
                end;
            {error, _} = Error ->
                Error
            end;
        {error, _} = Error ->
            Error
        end;
    {error, _} = Error ->
        Error
    end.

-spec select_bucket(binary(), #state{}) -> {ok, #state{}} | {error, term()}.
select_bucket(Bucket, State) ->
    #state{
        socket = Socket,
        timeout = UprTimeout,
        request_id = RequestId
    } = State,
    SelectBucket = couch_upr_consumer:encode_select_bucket(Bucket, RequestId),
    case gen_tcp:send(Socket, SelectBucket) of
    ok ->
        case socket_recv(Socket, ?UPR_HEADER_LEN, UprTimeout) of
        {ok, Header} ->
            {select_bucket, Status, RequestId, BodyLength} =
                                    couch_upr_consumer:parse_header(Header),
            case Status of
            ?UPR_STATUS_OK ->
                {ok, State#state{request_id = RequestId + 1}};
            _ ->
                case parse_error_response(
                        Socket, UprTimeout, BodyLength, Status) of
                % When the authentication happened with bucket name and
                % password, then the correct bucket is already selected. In
                % this case a select bucket command returns "not supported".
                {error, not_supported} ->
                    {ok, State#state{request_id = RequestId + 1}};
                {error, _} = Error ->
                    Error
                end
            end;
        {error, _} = Error ->
            Error
        end;
    {error, _} = Error ->
        Error
    end.

-spec open_connection(binary(), #state{}) -> {ok, #state{}} | {error, term()}.
open_connection(Name, State) ->
    #state{
        socket = Socket,
        timeout = UprTimeout,
        request_id = RequestId
    } = State,
    OpenConnection = couch_upr_consumer:encode_open_connection(
        Name, RequestId),
    case gen_tcp:send(Socket, OpenConnection) of
    ok ->
        case socket_recv(Socket, ?UPR_HEADER_LEN, UprTimeout) of
        {ok, Header} ->
            {open_connection, RequestId} = couch_upr_consumer:parse_header(Header),
            {ok, next_request_id(State)};
        {error, _} = Error ->
            Error
        end;
    {error, _} = Error ->
        Error
    end.


-spec receive_snapshot_marker(socket(), timeout(),  size()) ->
                                     {ok, {update_seq(), update_seq(),
                                      non_neg_integer()}} |
                                     {error, closed}.
receive_snapshot_marker(Socket, Timeout, BodyLength) ->
    case socket_recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
         {snapshot_marker, StartSeq, EndSeq, Type} =
             couch_upr_consumer:parse_snapshot_marker(Body),
         {ok, {StartSeq, EndSeq, Type}};
    {error, _} = Error ->
        Error
    end.

-spec receive_snapshot_mutation(socket(), timeout(), partition_id(), size(),
                                size(), size(), uint64(), upr_data_type()) ->
                                       #upr_doc{} | {error, closed}.
receive_snapshot_mutation(Socket, Timeout, PartId, KeyLength, BodyLength,
        ExtraLength, Cas, DataType) ->
    case socket_recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
         {snapshot_mutation, Mutation} =
             couch_upr_consumer:parse_snapshot_mutation(KeyLength, Body,
                 BodyLength, ExtraLength),
         #mutation{
             seq = Seq,
             rev_seq = RevSeq,
             flags = Flags,
             expiration = Expiration,
             key = Key,
             value = Value
         } = Mutation,
         #upr_doc{
             id = Key,
             body = Value,
             data_type = DataType,
             partition = PartId,
             cas = Cas,
             rev_seq = RevSeq,
             seq = Seq,
             flags = Flags,
             expiration = Expiration,
             deleted = false
         };
    {error, _} = Error ->
        Error
    end.

-spec receive_snapshot_deletion(socket(), timeout(), partition_id(), size(),
                                size(), uint64(), upr_data_type()) ->
                                       #upr_doc{} |
                                       {error, closed | inet:posix()}.
receive_snapshot_deletion(Socket, Timeout, PartId, KeyLength, BodyLength,
        Cas, DataType) ->
    case socket_recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
         {snapshot_deletion, Deletion} =
             couch_upr_consumer:parse_snapshot_deletion(KeyLength, Body),
         {Seq, RevSeq, Key, _Metadata} = Deletion,
         #upr_doc{
             id = Key,
             body = <<>>,
             data_type = DataType,
             partition = PartId,
             cas = Cas,
             rev_seq = RevSeq,
             seq = Seq,
             flags = 0,
             expiration = 0,
             deleted = true
         };
    {error, Reason} ->
        {error, Reason}
    end.

-spec receive_stream_end(socket(), timeout(), size()) ->
                            <<_:32>> | {error, closed | inet:posix()}.
receive_stream_end(Socket, Timeout, BodyLength) ->
    case socket_recv(Socket, BodyLength, Timeout) of
    {ok, Flag} ->
        Flag;
    {error, Reason} ->
        {error, Reason}
    end.


% Returns the failover log as a list 2-tuple pairs with
% partition UUID and sequence number
-spec receive_failover_log(socket(), timeout(), char(), size()) ->
            {'ok', list(partition_version())} | {error, closed | inet:posix()}.
receive_failover_log(_Socket, _Timeout, _Status, 0) ->
    {error, no_failover_log_found};
receive_failover_log(Socket, Timeout, Status, BodyLength) ->
    case Status of
    ?UPR_STATUS_OK ->
        case socket_recv(Socket, BodyLength, Timeout) of
        {ok, Body} ->
            couch_upr_consumer:parse_failover_log(Body);
        {error, _} = Error->
            Error
        end;
    _ ->
        {error, Status}
    end.

-spec receive_rollback_seq(socket(), timeout(), size()) ->
                  {ok, update_seq()} | {error, closed | inet:posix()}.
receive_rollback_seq(Socket, Timeout, BodyLength) ->
    case socket_recv(Socket, BodyLength, Timeout) of
    {ok, <<RollbackSeq:?UPR_SIZES_BY_SEQ>>} ->
        {ok, RollbackSeq};
    {error, _} = Error->
        Error
    end.


-spec receive_stat(socket(), timeout(), upr_status(), size(), size()) ->
                          {ok, {binary(), binary()} |
                           {error, {upr_status(), binary()}}} |
                          {error, closed}.
receive_stat(Socket, Timeout, Status, BodyLength, KeyLength) ->
    case socket_recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
        couch_upr_consumer:parse_stat(
            Body, Status, KeyLength, BodyLength - KeyLength);
    {error, Reason} ->
        {error, Reason}
    end.


-spec receive_events(pid(), request_id(), mutations_fold_fun(),
                     mutations_fold_acc()) -> {ok, mutations_fold_acc()} |
                                              {error, term()}.
receive_events(Pid, RequestId, CallbackFn, InAcc) ->
    {Optype, Data} = get_stream_event(Pid, RequestId),
    case Optype of
    stream_end ->
        InAcc;
    snapshot_marker ->
        InAcc2 = CallbackFn({snapshot_marker, Data}, InAcc),
        receive_events(Pid, RequestId, CallbackFn, InAcc2);
    _ ->
        InAcc2 = CallbackFn(Data, InAcc),
        receive_events(Pid, RequestId, CallbackFn, InAcc2)
    end.


-spec socket_recv(socket(), size(), timeout()) ->
    {ok, binary()} | {error, closed | inet:posix()}.
socket_recv(_Socket, 0, _Timeout) ->
    {ok, <<>>};
socket_recv(Socket, Length, Timeout) ->
    gen_tcp:recv(Socket, Length, Timeout).


-spec add_pending_request(#state{}, request_id(), term(), nil | {pid(), term()}) -> #state{}.
add_pending_request(State, RequestId, ReqInfo, From) ->
    #state{
       pending_requests = PendingRequests
    } = State,
    PendingRequests2 = dict:store(RequestId, {ReqInfo, From}, PendingRequests),
    State#state{pending_requests = PendingRequests2}.

remove_pending_request(State, RequestId) ->
    #state{
       pending_requests = PendingRequests
    } = State,
    PendingRequests2 = dict:erase(RequestId, PendingRequests),
    State#state{pending_requests = PendingRequests2}.


-spec find_pending_request(#state{}, request_id()) -> nil | {term(), nil | {pid(), term()}}.
find_pending_request(State, RequestId) ->
    #state{
       pending_requests = PendingRequests
    } = State,
    case dict:find(RequestId, PendingRequests) of
    error ->
        nil;
    {ok, Pending} ->
        Pending
    end.

-spec next_request_id(#state{}) -> #state{}.
next_request_id(#state{request_id = RequestId} = State) ->
    RequestId2 = case RequestId of
    Id when Id + 1 < (1 bsl ?UPR_SIZES_OPAQUE) ->
        Id + 1;
    _ ->
        0
    end,
    State#state{request_id = RequestId2}.

-spec remove_request_queue(#state{}, request_id()) -> #state{}.
remove_request_queue(State, RequestId) ->
    #state{
       active_streams = ActiveStreams,
       stream_queues = StreamQueues
    } = State,
    ActiveStreams2 = lists:keydelete(RequestId, 2, ActiveStreams),

    % All active streams have finished reading
    % Let us ack for remaining unacked bytes
    case length(ActiveStreams2) of
    0 ->
        {ok, State2} = send_buffer_ack(State);
    _ ->
        State2 = State
    end,

    StreamQueues2 = dict:erase(RequestId, StreamQueues),
    State2#state{
       active_streams = ActiveStreams2,
       stream_queues = StreamQueues2
    }.


-spec add_request_queue(#state{}, partition_id(), request_id()) -> #state{}.
add_request_queue(State, PartId, RequestId) ->
    #state{
       active_streams = ActiveStreams,
       stream_queues = StreamQueues
    } = State,
   ActiveStreams2 =  [{PartId, RequestId} | ActiveStreams],
   StreamQueues2 = dict:store(RequestId, {nil, queue:new()}, StreamQueues),
   State#state{
       active_streams = ActiveStreams2,
       stream_queues = StreamQueues2
    }.


-spec enqueue_stream_event(#state{}, request_id(), tuple()) -> #state{}.
enqueue_stream_event(State, RequestId, Event) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    {ok, {Waiter, EvQueue}} = dict:find(RequestId, StreamQueues),
    State#state{
        stream_queues =
            dict:store(RequestId,
                       {Waiter, queue:in(Event, EvQueue)},
                       StreamQueues)
    }.

-spec dequeue_stream_event(#state{}, request_id()) ->
                               {#state{}, tuple()}.
dequeue_stream_event(State, RequestId) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    {ok, {Waiter, EvQueue}} = dict:find(RequestId, StreamQueues),
    {{value, Event}, Rest} = queue:out(EvQueue),
    State2 = State#state{
        stream_queues =
            dict:store(RequestId, {Waiter, Rest}, StreamQueues)
    },
    {State2, Event}.

-spec peek_stream_event(#state{}, request_id()) -> tuple().
peek_stream_event(State, RequestId) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    {ok, {_, EvQueue}} = dict:find(RequestId, StreamQueues),
    {value, Event} = queue:peek(EvQueue),
    Event.

-spec add_stream_event_waiter(#state{}, request_id(), term()) -> #state{} | nil.
add_stream_event_waiter(State, RequestId, NewWaiter) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    {ok, {Waiter, EvQueue}} = dict:find(RequestId, StreamQueues),
    case Waiter of
    nil ->
        StreamQueues2 = dict:store(RequestId, {NewWaiter, EvQueue}, StreamQueues),
        State#state{
           stream_queues = StreamQueues2
        };
    _ ->
        nil
    end.


-spec stream_event_present(#state{}, request_id()) -> nil | true | false.
stream_event_present(State, RequestId) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    case dict:find(RequestId, StreamQueues) of
    error ->
        nil;
    {ok, {_, EvQueue}} ->
        queue:is_empty(EvQueue) =:= false
    end.


-spec stream_event_waiters_present(#state{}, request_id()) -> nil | true | false.
stream_event_waiters_present(State, RequestId) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    case dict:find(RequestId, StreamQueues) of
    error ->
        nil;
    {ok, {Waiter, _}} ->
        Waiter =/= nil
    end.


-spec remove_stream_event_waiter(#state{}, request_id()) -> {#state{}, term()}.
remove_stream_event_waiter(State, RequestId) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    {ok, {Waiter, EvQueue}} = dict:find(RequestId, StreamQueues),
    State2 = State#state{
        stream_queues = dict:store(RequestId, {nil, EvQueue}, StreamQueues)
    },
    {State2, Waiter}.


-spec find_stream_req_id(#state{}, partition_id()) -> request_id() | nil.
find_stream_req_id(State, PartId) ->
    #state{
       active_streams = ActiveStreams
    } = State,
    case lists:keyfind(PartId, 1, ActiveStreams) of
    {PartId, StreamReqId} ->
        StreamReqId;
    false ->
        nil
    end.

-spec parse_error_response(socket(), timeout(), integer(), integer()) ->
                                     {'error', atom() | {'status', integer()}}.
parse_error_response(Socket, Timeout, BodyLength, Status) ->
    case socket_recv(Socket, BodyLength, Timeout) of
    {ok, _} ->
        case Status of
        ?UPR_STATUS_KEY_NOT_FOUND ->
            {error, vbucket_stream_not_found};
        ?UPR_STATUS_ERANGE ->
            {error, wrong_start_sequence_number};
        ?UPR_STATUS_KEY_EEXISTS ->
            {error, vbucket_stream_already_exists};
        ?UPR_STATUS_NOT_MY_VBUCKET ->
            {error, server_not_my_vbucket};
        ?UPR_STATUS_TMP_FAIL ->
            {error, vbucket_stream_tmp_fail};
        ?UPR_STATUS_NOT_SUPPORTED ->
            {error, not_supported};
        _ ->
            {error, {status, Status}}
        end;
    {error, _} = Error ->
        Error
    end.


% The worker process for handling upr connection downstream pipe
% Read and parse downstream messages and send to the gen_server process
-spec receive_worker(socket(), timeout(), pid(), list()) -> closed | inet:posix().
receive_worker(Socket, Timeout, Parent, MsgAcc0) ->
    case socket_recv(Socket, ?UPR_HEADER_LEN, infinity) of
    {ok, Header} ->
        {Action, MsgAcc} =
        case couch_upr_consumer:parse_header(Header) of
        {control_request, Status, RequestId} ->
            {done, {stream_response, RequestId, {RequestId, Status}}};
        {noop_request, RequestId} ->
            {done, {stream_noop, RequestId}};
        {buffer_ack, Status, _RequestId} ->
            Status = ?UPR_STATUS_OK,
            {true, []};
        {stream_request, Status, RequestId, BodyLength} ->
            Response = case Status of
            ?UPR_STATUS_OK ->
                case receive_failover_log(
                     Socket, Timeout, Status, BodyLength) of
                {ok, FailoverLog} ->
                    {failoverlog, FailoverLog};
                Error ->
                    Error
                end;
            ?UPR_STATUS_ROLLBACK ->
                case receive_rollback_seq(
                     Socket, Timeout, BodyLength) of
                {ok, RollbackSeq} ->
                    {rollback, RollbackSeq};
                Error ->
                    Error
                end;
            _ ->
                parse_error_response(Socket, Timeout, BodyLength, Status)
            end,
            {done, {stream_response, RequestId, {RequestId, Response}}};
        {failover_log, Status, RequestId, BodyLength} ->
            Response = receive_failover_log(Socket, Timeout, Status, BodyLength),
            {done, {stream_response, RequestId, Response}};
        {stream_close, Status, RequestId, BodyLength} ->
            Response = case Status of
            ?UPR_STATUS_OK ->
                ok;
            _ ->
                parse_error_response(Socket, Timeout, BodyLength, Status)
            end,
            {done, {stream_response, RequestId, Response}};
        {stats, Status, RequestId, BodyLength, KeyLength} ->
            case BodyLength of
            0 ->
                case Status of
                ?UPR_STATUS_OK ->
                    StatAcc = lists:reverse(MsgAcc0),
                    {done, {stream_response, RequestId, {ok, StatAcc}}};
                % Some errors might not contain a body
                _ ->
                    Error = {error, {Status, <<>>}},
                    {done, {stream_response, RequestId, Error}}
                end;
            _ ->
                case receive_stat(
                    Socket, Timeout, Status, BodyLength, KeyLength) of
                {ok, Stat} ->
                    {true, [Stat | MsgAcc0]};
                {error, _} = Error ->
                    {done, {stream_response, RequestId, Error}}
                end
            end;
        {snapshot_marker, _PartId, RequestId, BodyLength} ->
            {ok, SnapshotMarker} = receive_snapshot_marker(
                Socket, Timeout, BodyLength),
            {done, {stream_event, RequestId,
                {snapshot_marker, SnapshotMarker, BodyLength}}};
        {snapshot_mutation, PartId, RequestId, KeyLength, BodyLength,
                ExtraLength, Cas, DataType} ->
            Mutation = receive_snapshot_mutation(
                Socket, Timeout, PartId, KeyLength, BodyLength, ExtraLength,
                Cas, DataType),
            {done, {stream_event, RequestId, {snapshot_mutation, Mutation, BodyLength}}};
        % For the indexer and XDCR there's no difference between a deletion
        % end an expiration. In both cases the items should get removed.
        % Hence the same code can be used after the initial header
        % parsing (the body is the same).
        {OpCode, PartId, RequestId, KeyLength, BodyLength, Cas, DataType} when
                OpCode =:= snapshot_deletion orelse
                OpCode =:= snapshot_expiration ->
            Deletion = receive_snapshot_deletion(
                Socket, Timeout, PartId, KeyLength, BodyLength, Cas, DataType),
            {done, {stream_event, RequestId, {snapshot_deletion, Deletion, BodyLength}}};
        {stream_end, PartId, RequestId, BodyLength} ->
            Flag = receive_stream_end(Socket, Timeout, BodyLength),
            {done, {stream_event, RequestId, {stream_end, {RequestId, PartId, Flag}, BodyLength}}}
        end,
        case Action of
        done ->
            Parent ! MsgAcc,
            receive_worker(Socket, Timeout, Parent, []);
        true ->
            receive_worker(Socket, Timeout, Parent, MsgAcc)
        end;
    {error, Reason} ->
        exit({conn_error, Reason})
    end.

% Check if we need to send buffer ack to server and send it
% if required.
-spec check_and_send_buffer_ack(#state{}, request_id(), tuple() | nil, atom()) ->
                        {ok, #state{}} | {error, closed | inet:posix()}.
check_and_send_buffer_ack(State, RequestId, Event, Type) ->
    #state{
        socket = Socket,
        max_buffer_size = MaxBufSize,
        stream_queues = StreamQueues,
        total_buffer_size = Size
    } = State,
    Size2 = case Type of
    remove_stream ->
        case dict:find(RequestId, StreamQueues) of
        error ->
            Size;
        {ok, {_, EvQueue}} ->
            get_queue_size(EvQueue, Size)
        end;
    mutation ->
        Size + get_event_size(Event)
    end,
    MaxAckSize = MaxBufSize * ?UPR_BUFFER_ACK_THRESHOLD,
    {Status, Ret} = if
    Size2 > MaxAckSize ->
        BufferAckRequest = couch_upr_consumer:encode_buffer_request(0, Size2),
        case gen_tcp:send(Socket, BufferAckRequest) of
        ok ->
            {ok, 0};
        Error ->
            {error, Error}
        end;
    Size2 == 0 ->
        {false, stream_not_found};
    true ->
        {ok, Size2}
    end,
    case Status of
    ok ->
        State2 = State#state{
            total_buffer_size = Ret
        },
        {ok, State2};
    error ->
        {error, Ret};
    false ->
        {ok, State}
    end.

-spec send_buffer_ack(#state{}) ->
            {ok, #state{}} | {error, closed | inet:posix()}.
send_buffer_ack(State) ->
    #state{
       socket = Socket,
        total_buffer_size = Size
    } = State,
    BufferAckRequest = couch_upr_consumer:encode_buffer_request(0, Size),
    case gen_tcp:send(Socket, BufferAckRequest) of
    ok ->
        {ok, State#state{total_buffer_size = 0}};
    {error, _Reason} = Error ->
        Error
    end.


-spec set_buffer_size(#state{}, non_neg_integer()) -> {ok ,#state{}} |
                                            {error, closed | inet:posix()}.
set_buffer_size(State, Size) ->
    #state{
        socket = Socket,
        request_id = RequestId
    } = State,
    ControlRequest = couch_upr_consumer:encode_control_request(RequestId, connection, Size),
    case gen_tcp:send(Socket, ControlRequest) of
    ok ->
        State2 = next_request_id(State),
        State3 = add_pending_request(State2, RequestId, {control_request, Size}, nil),
        State4 = State3#state{max_buffer_size = Size},
        {ok, State4};
    {error, Error} ->
        {error, Error}
    end.

-spec get_queue_size(queue(), non_neg_integer()) -> non_neg_integer().
get_queue_size(EvQueue, Size) ->
    case queue:out(EvQueue) of
    {empty, _} ->
        Size;
    {{value, Item}, NewQueue} ->
        Size2 = Size + get_event_size(Item),
        get_queue_size(NewQueue, Size2)
    end.

-spec get_event_size({atom(), #upr_doc{}, non_neg_integer()}) -> non_neg_integer().
get_event_size({_Type, _Doc, BodyLength}) ->
    ?UPR_HEADER_LEN + BodyLength.

-spec remove_stream_info(partition_id(), #state{}) -> #state{}.
remove_stream_info(PartId, State) ->
    #state{
        active_streams = ActiveStreams,
        stream_info = StreamData
    } = State,
    case lists:keyfind(PartId, 1, ActiveStreams) of
    false ->
        State;
    {_, RequestId} ->
        StreamData2 = dict:erase(RequestId, StreamData),
        State#state{stream_info = StreamData2}
    end.

-spec insert_stream_info(partition_id(), request_id(), uuid(), non_neg_integer(),
                        non_neg_integer(), #state{}, non_neg_integer()) -> #state{}.
insert_stream_info(PartId, RequestId, PartUuid, StartSeq, EndSeq, State, Flags) ->
    #state{stream_info = StreamData} = State,
    Data = #stream_info{
        part_id = PartId,
        part_uuid = PartUuid,
        start_seq = StartSeq,
        end_seq = EndSeq,
        flags = Flags
    },
    StreamData2 = dict:store(RequestId, Data, StreamData),
    State#state{stream_info = StreamData2}.

-spec find_stream_info(request_id(), #state{}) -> #stream_info{} | nil.
find_stream_info(RequestId, State) ->
    #state{
       stream_info = StreamData
    } = State,
    case dict:find(RequestId, StreamData) of
    {ok, Info} ->
        Info;
    error ->
        nil
    end.

-spec add_new_stream({partition_id(), uuid(), update_seq(), update_seq(),
        init | retry, request_id(), {update_seq(), update_seq()}, 0..255},
        {pid(), string()}, #state{}) -> #state{} | {error, closed | inet:posix()}.
add_new_stream({PartId, PartUuid, StartSeq, EndSeq, Type, OldRequestId,
    {SnapSeqStart, SnapSeqEnd}, Flags}, From, State) ->
   #state{
       socket = Socket,
       request_id = RequestId
    } = State,
    Id = case Type of
    init ->
        RequestId;
    retry ->
        OldRequestId
    end,
    StreamRequest = couch_upr_consumer:encode_stream_request(
        PartId, Id, Flags, StartSeq, EndSeq, PartUuid, SnapSeqStart, SnapSeqEnd),
    case gen_tcp:send(Socket, StreamRequest) of
    ok ->
        case Type of
        init ->
            State2 = insert_stream_info(PartId, RequestId, PartUuid, StartSeq, EndSeq,
                State, Flags),
            State3 = next_request_id(State2),
            add_pending_request(State3, RequestId, {add_stream, PartId}, From);
        retry ->
            add_pending_request(State, OldRequestId, {add_stream, PartId}, nil)
        end;
    Error ->
        {error, Error}
    end.

-spec restart_worker(#state{}) -> {noreply, #state{}} | {stop, sasl_auth_failed}.
restart_worker(State) ->
    #state{
        args = Args
    } = State,
    case init(Args) of
    {stop, Reason} ->
        {stop, Reason, State};
    {ok, State2} ->
        #state{
            socket = Socket,
            worker_pid = WorkerPid
        } = State2,
        % Replace the socket
        State3 = State#state{
            socket = Socket,
            pending_requests = dict:new(),
            worker_pid = WorkerPid
        },
        #state{stream_info = StreamData} = State,
        State4 = dict:fold(fun(RequestId, Value, Acc) ->
            #stream_info{
                part_id = PartId,
                part_uuid= Puuid,
                start_seq = StartSeq,
                end_seq = EndSeq,
                snapshot_seq = SnapshotSeq,
                flags = Flags
            } = Value,
            add_new_stream({PartId, Puuid, StartSeq, EndSeq,
                retry, RequestId, SnapshotSeq, Flags}, 0, Acc)
        end, State3, StreamData),
        {noreply, State4}
    end.

-spec store_snapshot_seq(request_id(), {update_seq(), update_seq(),
                            non_neg_integer()}, #state{}) -> #state{}.
store_snapshot_seq(RequestId, Data, State) ->
    {StartSeq, EndSeq, _Type} = Data,
    #state{
        stream_info = StreamData
    } = State,
    case dict:find(RequestId, StreamData) of
    {ok, Val} ->
        Val2 = Val#stream_info{snapshot_seq = {StartSeq, EndSeq}},
        StreamData2 = dict:store(RequestId, Val2, StreamData),
        State#state{stream_info = StreamData2};
    error ->
        State
    end.

-spec store_snapshot_mutation(request_id(), #upr_doc{}, #state{}) -> #state{}.
store_snapshot_mutation(RequestId, Data, State) ->
  #upr_doc{
        seq = Seq
    } = Data,
    #state{
        stream_info = StreamData
    } = State,
    case dict:find(RequestId, StreamData) of
    error ->
        State;
    {ok, Val} ->
        Val2 = Val#stream_info{start_seq = Seq},
        StreamData2 = dict:store(RequestId, Val2, StreamData),
        State#state{stream_info = StreamData2}
    end.

-spec remove_body_len({atom(), tuple | #upr_doc{}, non_neg_integer()}) ->
                                                {atom(), tuple | #upr_doc{}}.
remove_body_len({Type, Data, _BodyLength}) ->
    {Type, Data}.
