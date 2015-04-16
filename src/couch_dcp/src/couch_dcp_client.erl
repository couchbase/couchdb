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

-module(couch_dcp_client).
-behaviour(gen_server).

% Public API
-export([start/5]).
-export([add_stream/6, get_seqs/2, get_num_items/2,
    get_failover_log/2]).
-export([get_stream_event/2, remove_stream/2, list_streams/1]).
-export([enum_docs_since/8, restart_worker/1]).
-export([get_seqs_async/1]).

% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).
-export([format_status/2]).

-include("couch_db.hrl").
-include_lib("couch_dcp/include/couch_dcp.hrl").
-include_lib("couch_dcp/include/couch_dcp_typespecs.hrl").
-define(MAX_BUF_SIZE, 10485760).
-define(TIMEOUT, 60000).
-define(TIMEOUT_STATS, 2000).
-define(DCP_RETRY_TIMEOUT, 2000).
-define(DCP_BUF_WINDOW, 65535).
-define(SOCKET(BufSocket), BufSocket#bufsocket.sockpid).

-type mutations_fold_fun() :: fun().
-type mutations_fold_acc() :: any().

-record(bufsocket, {
    sockpid = nil                   :: socket() | nil,
    sockbuf = <<>>                  :: binary()
}).

-record(state, {
    bufsocket = nil                 :: #bufsocket{} | nil,
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

% This gen server implements a DCP client with vbucket stream multiplexing
% The client spawns a worker process to handle all response messages and event
% messages received from the DCP server. For easiness, responses are classifed into
% two types of messages, stream_response and stream_event. For any DCP request, the
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
    update_seq(), dcp_data_type()) -> {error, term()} | {request_id(), term()}.
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
        exit({dcp_client_died, Pid, Reason})
    after ?TIMEOUT_STATS ->
        ?LOG_ERROR("dcp client (~p): stats timed out after ~p seconds."
                   " Waiting...",
            [Pid, ?TIMEOUT_STATS / 1000]),
        get_stats_reply(Pid, MRef)
    end.

-spec get_all_seqs_reply(pid(), reference()) -> term().
get_all_seqs_reply(Pid, MRef) ->
    receive
    {all_seqs, MRef, Reply} ->
        Reply;
    {'DOWN', MRef, process, Pid, Reason} ->
        exit({dcp_client_died, Pid, Reason})
    after ?TIMEOUT_STATS ->
        ?LOG_ERROR("dcp client (~p): dcp all seqs timed out after ~p seconds."
                   " Waiting...",
            [Pid, ?TIMEOUT_STATS / 1000]),
        get_all_seqs_reply(Pid, MRef)
    end.

-spec get_stats(pid(), binary(), partition_id() | nil) -> term().
get_stats(Pid, Name, PartId) ->
    MRef = erlang:monitor(process, Pid),
    Pid ! {get_stats, Name, PartId, {MRef, self()}},
    Reply = get_stats_reply(Pid, MRef),
    erlang:demonitor(MRef, [flush]),
    Reply.


% The async get_seqs API requests seq numbers asynchronously.
% The response will be sent to the caller process asynchronously in the
% following message format:
% {all_seqs, nil, StatsResponse}.
-spec get_seqs_async(pid()) -> ok.
get_seqs_async(Pid) ->
    Pid ! {get_all_seqs, {nil, self()}},
    ok.

-spec get_seqs(pid(), ordsets:ordset(partition_id()) | nil) ->
         {ok, partition_seqs()} | {error, term()}.
get_seqs(Pid, SortedPartIds) ->
    case get_all_seqs(Pid) of
    {ok, Seqs} ->
        case SortedPartIds of
        nil ->
            {ok, Seqs};
        _ ->
            Seqs2 = couch_set_view_util:filter_seqs(SortedPartIds, Seqs),
            {ok, Seqs2}
        end;
    {error, _} = Error ->
        Error
    end.

-spec get_all_seqs(pid()) -> term().
get_all_seqs(Pid) ->
    MRef = erlang:monitor(process, Pid),
    Pid ! {get_all_seqs, {MRef, self()}},
    Reply = get_all_seqs_reply(Pid, MRef),
    erlang:demonitor(MRef, [flush]),
    Reply.

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
    {error, {?DCP_STATUS_NOT_MY_VBUCKET, _}} ->
        {error, not_my_vbucket}
    end.


-spec get_failover_log(pid(), partition_id()) ->
                              {error, no_failover_log_found | dcp_status()} |
                              {ok, partition_version()}.
get_failover_log(Pid, PartId) ->
    gen_server:call(Pid, {get_failover_log, PartId}).


-spec get_stream_event(pid(), request_id()) ->
                              {atom(), #dcp_doc{}} | {'error', term()}.
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
        exit({dcp_client_died, Pid, Reason})
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

    case add_stream(Pid, PartId, PartUuid, StartSeq, EndSeq, Flags) of
    {error, _} = Error ->
        Error;
    {RequestId, Resp} ->
        case Resp of
        {failoverlog, FailoverLog} ->
            case length(FailoverLog) > ?DCP_MAX_FAILOVER_LOG_SIZE of
            true ->
                {error, too_large_failover_log};
            false ->
                InAcc2 = CallbackFn({part_versions, {PartId, FailoverLog}}, InAcc),
                case receive_events(Pid, RequestId, CallbackFn, InAcc2) of
                {ok, InAcc3} ->
                    {ok, InAcc3, FailoverLog};
                Error ->
                    Error
                end
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
        end
    end.


% gen_server callbacks

-spec init([binary() | non_neg_integer()]) -> {ok, #state{}} |
                    {stop, sasl_auth_failed | closed | inet:posix()}.
init([Name, Bucket, AdmUser, AdmPasswd, BufferSize]) ->
    DcpTimeout = list_to_integer(
        couch_config:get("dcp", "connection_timeout")),
    DcpPort = list_to_integer(couch_config:get("dcp", "port")),

    case gen_tcp:connect("localhost", DcpPort,
        [binary, {packet, raw}, {active, true}, {nodelay, true},
                 {buffer, ?DCP_BUF_WINDOW}], DcpTimeout) of
    {ok, SockPid} ->
        BufSocket = #bufsocket{sockpid = SockPid, sockbuf = <<>>},
        State = #state{
            bufsocket = BufSocket,
            timeout = DcpTimeout,
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
                    #state{bufsocket = BufSocket2} = State5,
                    WorkerPid = spawn_link(
                        fun() ->
                            receive_worker(BufSocket2, DcpTimeout, Parent, [])
                        end),
                    gen_tcp:controlling_process(?SOCKET(BufSocket), WorkerPid),
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
        end;
    {error, Reason} ->
        {stop, {error, {dcp_socket_connect_failed, Reason}}}
    end.


% Add caller to the request queue and wait for gen_server to reply on response arrival
handle_call({add_stream, PartId, PartUuid, StartSeq, EndSeq, Flags},
        From, State) ->
    SnapshotStart = StartSeq,
    SnapshotEnd = StartSeq,
    case add_new_stream({PartId, PartUuid, StartSeq, EndSeq,
        {SnapshotStart, SnapshotEnd}, Flags}, From, State) of
    {error, Reason} ->
        {reply, {error, Reason}, State};
    State2 ->
        {noreply, State2}
    end;

handle_call({remove_stream, PartId}, From, State) ->
    State2 = remove_stream_info(PartId, State),
    #state{
       request_id = RequestId,
       bufsocket = BufSocket
    } = State2,
    StreamCloseRequest = couch_dcp_consumer:encode_stream_close(
        PartId, RequestId),
    case bufsocket_send(BufSocket, StreamCloseRequest) of
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
       bufsocket = BufSocket
    } = State,
    FailoverLogRequest = couch_dcp_consumer:encode_failover_log_request(
        PartId, RequestId),
    case bufsocket_send(BufSocket, FailoverLogRequest) of
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
handle_call(get_socket, _From, #state{bufsocket = BufSocket} = State) ->
    {reply, BufSocket, State};

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
        {Optype, _, _} = Event,
        {Msg, State6} = case check_and_send_buffer_ack(State, RequestId, Event, mutation) of
        {ok, State3} ->
            {State4, Event} = dequeue_stream_event(State3, RequestId),
            State5 = case Optype =:= stream_end orelse Optype =:= error of
            true ->
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
       bufsocket = BufSocket
    } = State,
    SeqStatRequest = couch_dcp_consumer:encode_stat_request(
        Stat, PartId, RequestId),
    case bufsocket_send(BufSocket, SeqStatRequest) of
    ok ->
        State2 = next_request_id(State),
        State3 = add_pending_request(State2, RequestId, get_stats, From),
        {noreply, State3};
    Error ->
        {reply, Error, State}
    end;

handle_info({get_all_seqs, From}, State) ->
    #state{
       request_id = RequestId,
       bufsocket = BufSocket
    } = State,
    SeqStatRequest = couch_dcp_consumer:encode_all_seqs_request(RequestId),
    case bufsocket_send(BufSocket, SeqStatRequest) of
    ok ->
        State2 = next_request_id(State),
        State3 = add_pending_request(State2, RequestId, all_seqs, From),
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
        all_seqs ->
            {MRef, From} = SendTo,
            From ! {all_seqs, MRef, Msg},
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
        bufsocket = BufSocket
    } = State,
    NoOpResponse = couch_dcp_consumer:encode_noop_response(RequestId),
    % if noop reponse fails two times, server it self will close the connection
    bufsocket_send(BufSocket, NoOpResponse),
    {noreply, State};

% Handle events send by connection receiver worker
% If there is a waiting caller for stream event, reply to them
% Else, queue the event into the stream queue
handle_info({stream_event, RequestId, Event}, State) ->
    {Optype, Data, _Length} = Event,
    State2 = case Optype of
    Optype when Optype =:= stream_end orelse Optype =:= error ->
        #state{
            stream_info = StreamData
        } = State,
        StreamData2 = dict:erase(RequestId, StreamData),
        State#state{stream_info = StreamData2};
    snapshot_marker ->
        store_snapshot_seq(RequestId, Data, State);
    snapshot_mutation ->
        store_snapshot_mutation(RequestId, Data, State);
    snapshot_deletion ->
        store_snapshot_mutation(RequestId, Data, State)
    end,
    case stream_event_waiters_present(State2, RequestId) of
    true ->
        {State3, Waiter} = remove_stream_event_waiter(State2, RequestId),
        {Msg, State6} = case check_and_send_buffer_ack(State3, RequestId, Event, mutation) of
        {ok, State4} ->
            State5 = case Optype =:= stream_end orelse Optype =:= error of
            true ->
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
        " Restarting dcp receive worker...",
        [Bucket, Name, Reason]),
    timer:sleep(?DCP_RETRY_TIMEOUT),
    restart_worker(State);

handle_info({'EXIT', Pid, Reason}, #state{worker_pid = Pid} = State) ->
    {stop, Reason, State};

handle_info({print_log, ReqId}, State) ->
    [Name, Bucket, _AdmUser, _AdmPasswd, _BufferSize] = State#state.args,
    case find_stream_info(ReqId, State) of
    nil ->
        ?LOG_ERROR(
            "dcp client (~s, ~s): Obtaining message from server timed out "
            "after ~p seconds [RequestId ~p]. Waiting...",
            [Bucket, Name, ?TIMEOUT / 1000, ReqId]);
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
        bufsocket = BufSocket,
        timeout = DcpTimeout,
        request_id = RequestId
    } = State,
    Authenticate = couch_dcp_consumer:encode_sasl_auth(User, Passwd, RequestId),
    case bufsocket_send(BufSocket, Authenticate) of
    ok ->
        case bufsocket_recv(BufSocket, ?DCP_HEADER_LEN, DcpTimeout) of
        {ok, Header, BufSocket2} ->
            {sasl_auth, Status, RequestId, BodyLength} =
                couch_dcp_consumer:parse_header(Header),
            % Receive the body so that it is not mangled with the next request,
            % we care about the status only though
            case bufsocket_recv(BufSocket2, BodyLength, DcpTimeout) of
            {ok, _, BufSocket3} ->
                case Status of
                ?DCP_STATUS_OK ->
                    {ok, State#state{
                        request_id = RequestId + 1,
                        bufsocket = BufSocket3
                    }};
                ?DCP_STATUS_SASL_AUTH_FAILED ->
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
        bufsocket = BufSocket,
        timeout = DcpTimeout,
        request_id = RequestId
    } = State,
    SelectBucket = couch_dcp_consumer:encode_select_bucket(Bucket, RequestId),
    case bufsocket_send(BufSocket, SelectBucket) of
    ok ->
        case bufsocket_recv(BufSocket, ?DCP_HEADER_LEN, DcpTimeout) of
        {ok, Header, BufSocket2} ->
            {select_bucket, Status, RequestId, BodyLength} =
                                    couch_dcp_consumer:parse_header(Header),
            case Status of
            ?DCP_STATUS_OK ->
                {ok, State#state{
                    request_id = RequestId + 1,
                    bufsocket = BufSocket2
                }};
            _ ->
                case parse_error_response(
                        BufSocket2, DcpTimeout, BodyLength, Status) of
                % When the authentication happened with bucket name and
                % password, then the correct bucket is already selected. In
                % this case a select bucket command returns "not supported".
                {{error, not_supported}, BufSocket3} ->
                    {ok, State#state{
                        request_id = RequestId + 1,
                        bufsocket = BufSocket3
                    }};
                {{error, _}, _} = Error ->
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
        bufsocket = BufSocket,
        timeout = DcpTimeout,
        request_id = RequestId
    } = State,
    OpenConnection = couch_dcp_consumer:encode_open_connection(
        Name, RequestId),
    case bufsocket_send(BufSocket, OpenConnection) of
    ok ->
        case bufsocket_recv(BufSocket, ?DCP_HEADER_LEN, DcpTimeout) of
        {ok, Header, BufSocket2} ->
            {open_connection, RequestId} = couch_dcp_consumer:parse_header(Header),
            State2 = State#state{bufsocket = BufSocket2},
            {ok, next_request_id(State2)};
        {error, _} = Error ->
            Error
        end;
    {error, _} = Error ->
        Error
    end.


-spec receive_snapshot_marker(#bufsocket{}, timeout(),  size()) ->
                                     {ok, {update_seq(), update_seq(),
                                           non_neg_integer()}, #bufsocket{}} |
                                     {error, closed}.
receive_snapshot_marker(BufSocket, Timeout, BodyLength) ->
    case bufsocket_recv(BufSocket, BodyLength, Timeout) of
    {ok, Body, BufSocket2} ->
         {snapshot_marker, StartSeq, EndSeq, Type} =
             couch_dcp_consumer:parse_snapshot_marker(Body),
         {ok, {StartSeq, EndSeq, Type}, BufSocket2};
    {error, _} = Error ->
        Error
    end.

-spec receive_snapshot_mutation(#bufsocket{}, timeout(), partition_id(), size(),
                                size(), size(), uint64(), dcp_data_type()) ->
                                {#dcp_doc{}, #bufsocket{}} | {error, closed}.
receive_snapshot_mutation(BufSocket, Timeout, PartId, KeyLength, BodyLength,
        ExtraLength, Cas, DataType) ->
    case bufsocket_recv(BufSocket, BodyLength, Timeout) of
    {ok, Body, BufSocket2} ->
         {snapshot_mutation, Mutation} =
             couch_dcp_consumer:parse_snapshot_mutation(KeyLength, Body,
                 BodyLength, ExtraLength),
         #mutation{
             seq = Seq,
             rev_seq = RevSeq,
             flags = Flags,
             expiration = Expiration,
             key = Key,
             value = Value
         } = Mutation,
         {#dcp_doc{
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
         }, BufSocket2};
    {error, _} = Error ->
        Error
    end.

-spec receive_snapshot_deletion(#bufsocket{}, timeout(), partition_id(), size(),
                                size(), uint64(), dcp_data_type()) ->
                                        {#dcp_doc{}, #bufsocket{}} |
                                        {error, closed | inet:posix()}.
receive_snapshot_deletion(BufSocket, Timeout, PartId, KeyLength, BodyLength,
        Cas, DataType) ->
    case bufsocket_recv(BufSocket, BodyLength, Timeout) of
    {ok, Body, BufSocket2} ->
         {snapshot_deletion, Deletion} =
             couch_dcp_consumer:parse_snapshot_deletion(KeyLength, Body),
         {Seq, RevSeq, Key, _Metadata} = Deletion,
         {#dcp_doc{
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
         }, BufSocket2};
    {error, Reason} ->
        {error, Reason}
    end.

-spec receive_stream_end(#bufsocket{}, timeout(), size()) ->
            {<<_:32>>, #bufsocket{}} | {error, closed | inet:posix()}.
receive_stream_end(BufSocket, Timeout, BodyLength) ->
    case bufsocket_recv(BufSocket, BodyLength, Timeout) of
    {ok, Flag, BufSocket2} ->
        {Flag, BufSocket2};
    {error, Reason} ->
        {error, Reason}
    end.


% Returns the failover log as a list 2-tuple pairs with
% partition UUID and sequence number
-spec receive_failover_log(#bufsocket{}, timeout(), char(), size()) ->
        {{'ok', list(partition_version())}, #bufsocket{}} | {error, closed | inet:posix()}.
receive_failover_log(_BufSocket, _Timeout, _Status, 0) ->
    {error, no_failover_log_found};
receive_failover_log(BufSocket, Timeout, Status, BodyLength) ->
    case Status of
    ?DCP_STATUS_OK ->
        case bufsocket_recv(BufSocket, BodyLength, Timeout) of
        {ok, Body, BufSocket2} ->
            {couch_dcp_consumer:parse_failover_log(Body), BufSocket2};
        {error, _} = Error->
            Error
        end;
    _ ->
        {error, Status}
    end.

-spec receive_rollback_seq(#bufsocket{}, timeout(), size()) ->
        {ok, update_seq(), #bufsocket{}} | {error, closed | inet:posix()}.
receive_rollback_seq(BufSocket, Timeout, BodyLength) ->
    case bufsocket_recv(BufSocket, BodyLength, Timeout) of
    {ok, <<RollbackSeq:?DCP_SIZES_BY_SEQ>>, BufSocket2} ->
        {ok, RollbackSeq, BufSocket2};
    {error, _} = Error->
        Error
    end.


-spec receive_stat(#bufsocket{}, timeout(), dcp_status(), size(), size()) ->
        {ok, {binary(), binary()} |
        {ok, {binary(), binary()}, #bufsocket{}} |
        {error, {dcp_status(), binary()}}} |
        {error, closed}.
receive_stat(BufSocket, Timeout, Status, BodyLength, KeyLength) ->
    case bufsocket_recv(BufSocket, BodyLength, Timeout) of
    {ok, Body, BufSocket2} ->
        {couch_dcp_consumer:parse_stat(
            Body, Status, KeyLength, BodyLength - KeyLength), BufSocket2};
    {error, Reason} ->
        {error, Reason}
    end.

-spec receive_all_seqs(#bufsocket{}, timeout(), dcp_status(), size()) ->
        {ok, list()} |
        {error, {dcp_status(), binary()}} |
        {error, closed}.
receive_all_seqs(BufSocket, Timeout, Status, BodyLength) ->
    case bufsocket_recv(BufSocket, BodyLength, Timeout) of
    {ok, Body, BufSocket2} ->
        {couch_dcp_consumer:parse_all_seqs(Status, Body, []), BufSocket2};
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
        {ok, InAcc};
    snapshot_marker ->
        InAcc2 = CallbackFn({snapshot_marker, Data}, InAcc),
        receive_events(Pid, RequestId, CallbackFn, InAcc2);
    error ->
        {error, Data};
    _ ->
        InAcc2 = CallbackFn(Data, InAcc),
        receive_events(Pid, RequestId, CallbackFn, InAcc2)
    end.

-spec socket_send(socket(), iodata()) ->
        ok | {error, closed | inet:posix()}.
socket_send(Socket, Packet) ->
    gen_tcp:send(Socket, Packet).

-spec bufsocket_send(#bufsocket{}, iodata()) ->
        ok | {error, closed | inet:posix()}.
bufsocket_send(BufSocket, Packet) ->
    socket_send(?SOCKET(BufSocket), Packet).

-spec socket_recv(socket(), timeout()) ->
        {ok, binary()} | {error, closed | inet:posix()}.
socket_recv(SockPid, Timeout) ->
    receive
    {tcp, SockPid, Data} ->
        {ok, Data};
    {tcp_closed, SockPid} ->
        {error, closed};
    {tcp_error, SockPid, Reason} ->
        {error, Reason}
    after Timeout ->
        {ok, <<>>}
    end.

-spec bufsocket_recv(#bufsocket{}, size(), timeout()) ->
        {ok, binary(), #bufsocket{}} | {error, closed | inet:posix()}.
bufsocket_recv(BufSocket, 0, _Timeout) ->
    {ok, <<>>, BufSocket};
bufsocket_recv(BufSocket, Length, Timeout) ->
    #bufsocket{sockbuf = SockBuf} =  BufSocket,
    case erlang:byte_size(SockBuf) >= Length of
    true ->
        <<Head:Length/binary, Tail/binary>> = SockBuf,
        BufSocket2 = BufSocket#bufsocket{sockbuf = Tail},
        {ok, Head, BufSocket2};
    false ->
        case socket_recv(?SOCKET(BufSocket), Timeout) of
        {ok, Data} ->
            Buf = <<SockBuf/binary, Data/binary>>,
            BufSocket2 = BufSocket#bufsocket{sockbuf = Buf},
            bufsocket_recv(BufSocket2, Length, Timeout);
        {error, Reason} ->
            {error, Reason}
        end
    end.

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
    Id when Id + 1 < (1 bsl ?DCP_SIZES_OPAQUE) ->
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

-spec parse_error_response(#bufsocket{}, timeout(), integer(), integer()) ->
                    {'error', atom() | {'status', integer()}} |
                    {'error', atom() | {'status', integer()}, #bufsocket{}}.
parse_error_response(BufSocket, Timeout, BodyLength, Status) ->
    case bufsocket_recv(BufSocket, BodyLength, Timeout) of
    {ok, _, BufSocket2} ->
        {status_to_error(Status), BufSocket2};
    {error, _} = Error ->
        Error
    end.

-spec status_to_error(integer()) -> {'error', atom() | {'status', integer()}}.
status_to_error(?DCP_STATUS_KEY_NOT_FOUND) ->
    {error, vbucket_stream_not_found};
status_to_error(?DCP_STATUS_ERANGE) ->
    {error, wrong_start_sequence_number};
status_to_error(?DCP_STATUS_KEY_EEXISTS) ->
    {error, vbucket_stream_already_exists};
status_to_error(?DCP_STATUS_NOT_MY_VBUCKET) ->
    {error, server_not_my_vbucket};
status_to_error(?DCP_STATUS_TMP_FAIL) ->
    {error, vbucket_stream_tmp_fail};
status_to_error(?DCP_STATUS_NOT_SUPPORTED) ->
    {error, not_supported};
status_to_error(Status) ->
    {error, {status, Status}}.


% The worker process for handling dcp connection downstream pipe
% Read and parse downstream messages and send to the gen_server process
-spec receive_worker(#bufsocket{}, timeout(), pid(), list()) ->
                                                    closed | inet:posix().
receive_worker(BufSocket, Timeout, Parent, MsgAcc0) ->
    case bufsocket_recv(BufSocket, ?DCP_HEADER_LEN, infinity) of
    {ok, Header, BufSocket2} ->
        {Action, MsgAcc, BufSocket3} =
        case couch_dcp_consumer:parse_header(Header) of
        {control_request, Status, RequestId} ->
            {done, {stream_response, RequestId, {RequestId, Status}},
                BufSocket2};
        {noop_request, RequestId} ->
            {done, {stream_noop, RequestId}, BufSocket2};
        {buffer_ack, ?DCP_STATUS_OK, _RequestId} ->
            {true, [], BufSocket2};
        {stream_request, Status, RequestId, BodyLength} ->
            {Response, BufSocket5} = case Status of
            ?DCP_STATUS_OK ->
                case receive_failover_log(
                    BufSocket2, Timeout, Status, BodyLength) of
                {{ok, FailoverLog}, BufSocket4} ->
                    {{failoverlog, FailoverLog}, BufSocket4};
                Error ->
                    {Error, BufSocket2}
                end;
            ?DCP_STATUS_ROLLBACK ->
                case receive_rollback_seq(
                    BufSocket2, Timeout, BodyLength) of
                {ok, RollbackSeq, BufSocket4} ->
                    {{rollback, RollbackSeq}, BufSocket4};
                Error ->
                    {Error, BufSocket2}
                end;
            _ ->
                parse_error_response(BufSocket2, Timeout, BodyLength, Status)
            end,
            {done, {stream_response, RequestId, {RequestId, Response}},
                BufSocket5};
        {failover_log, Status, RequestId, BodyLength} ->
            {Response, BufSocket5} = receive_failover_log(
                BufSocket2, Timeout, Status, BodyLength),
            {done, {stream_response, RequestId, Response}, BufSocket5};
        {stream_close, Status, RequestId, BodyLength} ->
            {Response, BufSocket5} = case Status of
            ?DCP_STATUS_OK ->
                {ok, BufSocket2};
            _ ->
                parse_error_response(BufSocket2, Timeout, BodyLength, Status)
            end,
            {done, {stream_response, RequestId, Response}, BufSocket5};
        {stats, Status, RequestId, BodyLength, KeyLength} ->
            case BodyLength of
            0 ->
                case Status of
                ?DCP_STATUS_OK ->
                    StatAcc = lists:reverse(MsgAcc0),
                    {done, {stream_response, RequestId, {ok, StatAcc}},
                        BufSocket2};
                % Some errors might not contain a body
                _ ->
                    Error = {error, {Status, <<>>}},
                    {done, {stream_response, RequestId, Error}, BufSocket2}
                end;
            _ ->
                case receive_stat(
                    BufSocket2, Timeout, Status, BodyLength, KeyLength) of
                {{ok, Stat}, BufSocket5} ->
                    {true, [Stat | MsgAcc0], BufSocket5};
                {error, _} = Error ->
                    {done, {stream_response, RequestId, Error}, BufSocket2}
                end
            end;
        {snapshot_marker, _PartId, RequestId, BodyLength} ->
            {ok, SnapshotMarker, BufSocket5} = receive_snapshot_marker(
                BufSocket2, Timeout, BodyLength),
            {done, {stream_event, RequestId,
                {snapshot_marker, SnapshotMarker, BodyLength}}, BufSocket5};
        {snapshot_mutation, PartId, RequestId, KeyLength, BodyLength,
                ExtraLength, Cas, DataType} ->
            {Mutation, BufSocket5} = receive_snapshot_mutation(
                BufSocket2, Timeout, PartId, KeyLength, BodyLength, ExtraLength,
                Cas, DataType),
            {done, {stream_event, RequestId,
                    {snapshot_mutation, Mutation, BodyLength}}, BufSocket5};
        % For the indexer and XDCR there's no difference between a deletion
        % end an expiration. In both cases the items should get removed.
        % Hence the same code can be used after the initial header
        % parsing (the body is the same).
        {OpCode, PartId, RequestId, KeyLength, BodyLength, Cas, DataType} when
                OpCode =:= snapshot_deletion orelse
                OpCode =:= snapshot_expiration ->
            {Deletion, BufSocket5} = receive_snapshot_deletion(
                BufSocket2, Timeout, PartId, KeyLength, BodyLength,
                Cas, DataType),
            {done, {stream_event, RequestId,
                {snapshot_deletion, Deletion, BodyLength}}, BufSocket5};
        {stream_end, PartId, RequestId, BodyLength} ->
            {Flag, BufSocket5} = receive_stream_end(BufSocket2,
                Timeout, BodyLength),
            {done, {stream_event, RequestId, {stream_end,
                {RequestId, PartId, Flag}, BodyLength}}, BufSocket5};
        {all_seqs, Status, RequestId, BodyLength} ->
            {Resp, BufSocket5} = receive_all_seqs(
                BufSocket2, Timeout, Status, BodyLength),
            {done, {stream_response, RequestId, Resp}, BufSocket5}
        end,
        case Action of
        done ->
            Parent ! MsgAcc,
            receive_worker(BufSocket3, Timeout, Parent, []);
        true ->
            receive_worker(BufSocket3, Timeout, Parent, MsgAcc)
        end;
    {error, Reason} ->
        exit({conn_error, Reason})
    end.

% Check if we need to send buffer ack to server and send it
% if required.
-spec check_and_send_buffer_ack(#state{}, request_id(), tuple() | nil, atom()) ->
                        {ok, #state{}} | {error, closed | inet:posix()}.
check_and_send_buffer_ack(State, _RequestId, {error, _, _}, _Type) ->
    {ok, State};

check_and_send_buffer_ack(State, RequestId, Event, Type) ->
    #state{
        bufsocket = BufSocket,
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
    MaxAckSize = MaxBufSize * ?DCP_BUFFER_ACK_THRESHOLD,
    {Status, Ret} = if
    Size2 > MaxAckSize ->
        BufferAckRequest = couch_dcp_consumer:encode_buffer_request(0, Size2),
        case bufsocket_send(BufSocket, BufferAckRequest) of
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
        bufsocket = BufSocket,
        total_buffer_size = Size
    } = State,
    BufferAckRequest = couch_dcp_consumer:encode_buffer_request(0, Size),
    case bufsocket_send(BufSocket, BufferAckRequest) of
    ok ->
        {ok, State#state{total_buffer_size = 0}};
    {error, _Reason} = Error ->
        Error
    end.


-spec set_buffer_size(#state{}, non_neg_integer()) -> {ok ,#state{}} |
                                            {error, closed | inet:posix()}.
set_buffer_size(State, Size) ->
    #state{
        bufsocket = BufSocket,
        request_id = RequestId
    } = State,
    ControlRequest = couch_dcp_consumer:encode_control_request(RequestId, connection, Size),
    case bufsocket_send(BufSocket, ControlRequest) of
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

-spec get_event_size({atom(), #dcp_doc{}, non_neg_integer()}) -> non_neg_integer().
get_event_size({_Type, _Doc, BodyLength}) ->
    ?DCP_HEADER_LEN + BodyLength.

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
        {update_seq(), update_seq()}, 0..255},
        {pid(), string()}, #state{}) -> #state{} | {error, closed | inet:posix()}.
add_new_stream({PartId, PartUuid, StartSeq, EndSeq,
    {SnapSeqStart, SnapSeqEnd}, Flags}, From, State) ->
   #state{
       bufsocket = BufSocket,
       request_id = RequestId
    } = State,
    StreamRequest = couch_dcp_consumer:encode_stream_request(
        PartId, RequestId, Flags, StartSeq, EndSeq, PartUuid, SnapSeqStart, SnapSeqEnd),
    case bufsocket_send(BufSocket, StreamRequest) of
    ok ->
        State2 = insert_stream_info(PartId, RequestId, PartUuid, StartSeq, EndSeq,
            State, Flags),
        State3 = next_request_id(State2),
        add_pending_request(State3, RequestId, {add_stream, PartId}, From);
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
            bufsocket = BufSocket,
            worker_pid = WorkerPid
        } = State2,
        % Replace the socket
        State3 = State#state{
            bufsocket = BufSocket,
            pending_requests = dict:new(),
            worker_pid = WorkerPid
        },
        Error = {error, dcp_conn_closed},
        dict:map(fun(_RequestId, {ReqInfo, SendTo}) ->
            case ReqInfo of
            get_stats ->
                {MRef, From} = SendTo,
                From ! {get_stats, MRef, Error};
            {control_request, _} ->
                ok;
            _ ->
                gen_server:reply(SendTo, Error)
            end
        end, State#state.pending_requests),
        dict:map(fun(RequestId, _Value) ->
            Event = {error, dcp_conn_closed, 0},
            self() ! {stream_event, RequestId, Event}
        end, State#state.stream_info),
        {noreply, State3}
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

-spec store_snapshot_mutation(request_id(), #dcp_doc{}, #state{}) -> #state{}.
store_snapshot_mutation(RequestId, Data, State) ->
  #dcp_doc{
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

-spec remove_body_len({atom(), tuple | #dcp_doc{}, non_neg_integer()}) ->
                                                {atom(), tuple | #dcp_doc{}}.
remove_body_len({Type, Data, _BodyLength}) ->
    {Type, Data}.
