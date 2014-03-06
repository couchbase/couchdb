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
-export([start/2]).
-export([add_stream/5, get_sequence_number/2, get_failover_log/2]).
-export([get_stream_event/2, remove_stream/2, list_streams/1, set_buffer_size/2]).
-export([enum_docs_since/7]).

% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

-include("couch_db.hrl").
-include_lib("couch_upr/include/couch_upr.hrl").
-include_lib("couch_upr/include/couch_upr_typespecs.hrl").
-define(MAX_BUF_SIZE, 10485760).

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
    throttled = false               :: boolean()
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

-spec start(binary(), binary()) -> {ok, pid()} | ignore |
                                   {error, {already_started, pid()} | term()}.
start(Name, Bucket) ->
    gen_server:start_link(?MODULE, [Name, Bucket], []).


-spec add_stream(pid(), partition_id(), partition_version(), update_seq(), update_seq()) ->
                                                       {request_id(), term()}.
add_stream(Pid, PartId, PartVersion, StartSeq, EndSeq) ->
    gen_server:call(Pid, {add_stream, PartId, PartVersion, StartSeq, EndSeq}).


-spec remove_stream(pid(), partition_id()) ->
                            'ok' | {'error', term()}.
remove_stream(Pid, PartId) ->
    gen_server:call(Pid, {remove_stream, PartId}).


-spec list_streams(pid()) -> list().
list_streams(Pid) ->
    gen_server:call(Pid, list_streams).

-spec get_sequence_number(pid(), partition_id()) ->
                                 {ok, update_seq()} | {error, not_my_vbucket}.
get_sequence_number(Pid, PartId) ->
    case gen_server:call(Pid, {get_stats, <<"vbucket-seqno">>, PartId}) of
    {ok, [Stats, _]} ->
        {_, SeqBin} = Stats,
        {ok, list_to_integer(binary_to_list(SeqBin))};
    {error,  {?UPR_STATUS_NOT_MY_VBUCKET, _}} ->
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
    gen_server:call(Pid, {get_stream_event, ReqId}).


-spec set_buffer_size(pid(), integer()) -> ok.
set_buffer_size(Pid, Size) ->
    gen_server:call(Pid, {set_buffer_size, Size}).


-spec enum_docs_since(pid(), partition_id(), partition_version(), update_seq(),
                      update_seq(), mutations_fold_fun(),
                      mutations_fold_acc()) ->
                             {error, wrong_partition_version |
                              wrong_start_sequence_number |
                              too_large_failover_log } |
                             {rollback, update_seq()} |
                             {ok, mutations_fold_acc(), partition_version()}.
enum_docs_since(_, _, [], _, _, _, _) ->
    % No matching partition version found. Recreate the index from scratch
    {rollback, 0};
enum_docs_since(Pid, PartId, [PartVersion|PartVersions], StartSeq, EndSeq,
        CallbackFn, InAcc) ->
    {RequestId, Resp} =  add_stream(Pid, PartId, PartVersion, StartSeq, EndSeq),
    case Resp of
    {failoverlog, FailoverLog} ->
        case length(FailoverLog) > ?UPR_MAX_FAILOVER_LOG_SIZE of
        true ->
            {error, too_large_failover_log};
        false ->
            Acc2 = receive_events(Pid, RequestId, CallbackFn, InAcc),
            {ok, Acc2, FailoverLog}
        end;
    {error, wrong_partition_version} ->
        enum_docs_since(Pid, PartId, PartVersions, StartSeq, EndSeq, CallbackFn, InAcc);
    _ ->
        Resp
    end.


% gen_server callbacks

-spec init([binary()]) -> {ok, #state{}} | {stop, sasl_auth_failed}.
init([Name, Bucket]) ->
    UprTimeout = list_to_integer(
        couch_config:get("upr", "connection_timeout")),
    UprPort = list_to_integer(couch_config:get("upr", "port")),
    {ok, Socket} = gen_tcp:connect("localhost", UprPort,
        [binary, {packet, raw}, {active, false}]),
    State = #state{
        socket = Socket,
        timeout = UprTimeout,
        request_id = 0
    },
    % Authentication is used to specify from which bucket the data should
    % come from
    case sasl_auth(Bucket, State) of
    {ok, State2} ->
        case open_connection(Name, State2) of
        {ok, State3} ->
            Parent = self(),
            process_flag(trap_exit, true),
            WorkerPid = spawn_link(
                fun() -> receive_worker(Socket, UprTimeout, Parent, []) end),
            {ok, State3#state{worker_pid = WorkerPid}};
        {error, Reason} ->
            {stop, Reason}
        end;
    {error, Reason} ->
        {stop, Reason}
    end.


% Add caller to the request queue and wait for gen_server to reply on response arrival
handle_call({add_stream, PartId, PartVersion, StartSeq, EndSeq}, From, State) ->
    #state{
       socket = Socket,
       request_id = RequestId
    } = State,
    StreamRequest = couch_upr_consumer:encode_stream_request(
        PartId, RequestId, 0, StartSeq, EndSeq, PartVersion),
    case gen_tcp:send(Socket, StreamRequest) of
    ok ->
        State2 = next_request_id(State),
        State3 = add_pending_request(State2, RequestId, {add_stream, PartId}, From),
        {noreply, State3};
    Error ->
        {reply, Error, State}
    end;

handle_call({remove_stream, PartId}, From, State) ->
    #state{
       request_id = RequestId,
       socket = Socket
    } = State,
    StreamCloseRequest = couch_upr_consumer:encode_stream_close(
        PartId, RequestId),
    case gen_tcp:send(Socket, StreamCloseRequest) of
    ok ->
        State2 = next_request_id(State),
        State3 = add_pending_request(State2, RequestId, {remove_stream, PartId}, From),
        {noreply, State3};
    Error ->
        {reply, Error, State}
    end;

handle_call(list_streams, _From, State) ->
    #state{
       active_streams = ActiveStreams
    } = State,
    Reply = lists:foldl(fun({PartId, _}, Acc) -> [PartId | Acc] end, [], ActiveStreams),
    {reply, Reply, State};

handle_call({get_stats, Stat, PartId}, From, State) ->
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

% If a stream event for this requestId is present in the queue,
% dequeue it and reply back to the caller.
% Else, put the caller into the stream queue waiter list
handle_call({get_stream_event, RequestId}, From, State) ->
    #state{
       throttled = Throttled,
       max_buffer_size = MaxBufSize,
       worker_pid = Pid
    } = State,
    case stream_event_present(State, RequestId) of
    true ->
        {State2, Event, Size} = dequeue_stream_event(State, RequestId),
        State3 = case Throttled andalso (Size < MaxBufSize) of
        true ->
            Pid ! continue,
            State2#state{throttled = false};
        false ->
            State2
        end,
        State4 = case Event of
        {stream_end, _} ->
            remove_request_queue(State3, RequestId);
        _ ->
            State3
        end,
        {reply, Event, State4};
    false ->
        {noreply, add_stream_event_waiter(State, RequestId, From)};
    nil ->
        {reply, {error, vbucket_stream_not_found}, State}
    end;

handle_call({set_buffer_size, Size}, _From, State) ->
    {reply, ok, State#state{max_buffer_size = Size}};

% Only used by unit test
handle_call(throttled, _From, #state{throttled = Throttled} = State) ->
    {reply, Throttled, State}.


% Handle response message send by connection receiver worker
% Reply back to waiting callers
handle_info({stream_response, RequestId, Msg}, State) ->
    State3 = case find_pending_request(State, RequestId) of
    {ReqInfo, SendTo} ->
        gen_server:reply(SendTo, Msg),
        State2 = case ReqInfo of
        {add_stream, PartId} ->
            case Msg of
            {_, {failoverlog, _}} ->
                add_request_queue(State, PartId, RequestId);
            _ ->
                State
            end;
        {remove_stream, PartId} ->
            StreamReqId = find_stream_req_id(State, PartId),
            case Msg of
            ok ->
                remove_request_queue(State, StreamReqId);
            {error, vbucket_stream_not_found} ->
                remove_request_queue(State, StreamReqId);
            _ ->
                State
            end;
        _ ->
            State
        end,
        remove_pending_request(State2, RequestId);
    nil ->
        State
    end,
    {noreply, State3};

% Handle events send by connection receiver worker
% If there is a waiting caller for stream event, reply to them
% Else, queue the event into the stream queue
handle_info({stream_event, RequestId, Event}, State) ->
    case stream_event_waiters_present(State, RequestId) of
    true ->
        {State2, Waiter} = remove_stream_event_waiter(State, RequestId),
        gen_server:reply(Waiter, Event),
        State3 =
        case Event of
        {stream_end, _} ->
            remove_request_queue(State2, RequestId);
        _ ->
            State2
        end,
        {noreply, State3};
    false ->
        {State2, Size} = enqueue_stream_event(State, RequestId, Event),
        State3 = case should_throttle(State2, Size) of
        true ->
            State2#state.worker_pid ! throttle,
            State2#state{throttled = true};
        false ->
            State2
        end,
        {noreply, State3};
    nil ->
        {noreply, State}
    end;

handle_info({'EXIT', Pid, Reason}, #state{worker_pid = Pid} = State) ->
    {stop, Reason, State};

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


% Internal functions

-spec sasl_auth(binary(), #state{}) -> {ok, #state{}} |
                            {error, sasl_auth_failed | closed | inet:posix()}.
sasl_auth(Bucket, State) ->
    #state{
        socket = Socket,
        timeout = UprTimeout,
        request_id = RequestId
    } = State,
    Authenticate = couch_upr_consumer:encode_sasl_auth(Bucket, RequestId),
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


-spec receive_snapshot_mutation(socket(), timeout(), partition_id(), size(),
                                size(), size(), uint64()) ->
                                       #upr_doc{} | {error, closed}.
receive_snapshot_mutation(Socket, Timeout, PartId, KeyLength, BodyLength,
        ExtraLength, Cas) ->
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
             % XXX vmx 2014-02-26: TODO: datatype
             %data_type =
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
                                size(), uint64()) ->
                                       #upr_doc{} |
                                       {error, closed | inet:posix()}.
receive_snapshot_deletion(Socket, Timeout, PartId, KeyLength, BodyLength,
        Cas) ->
    case socket_recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
         {snapshot_deletion, Deletion} =
             couch_upr_consumer:parse_snapshot_deletion(KeyLength, Body),
         {Seq, RevSeq, Key, _Metadata} = Deletion,
         #upr_doc{
             id = Key,
             body = <<>>,
             % XXX vmx 2014-02-26: TODO: datatype
             %data_type =
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
                            {ok, <<_:32>>} | {error, closed | inet:posix()}.
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
                  {rollback, update_seq()} | {error, closed | inet:posix()}.
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
    {Optype, Doc} = get_stream_event(Pid, RequestId),
    case Optype of
    stream_end ->
        InAcc;
    snapshot_marker ->
        InAcc2 = CallbackFn(snapshot_marker, InAcc),
        receive_events(Pid, RequestId, CallbackFn, InAcc2);
    _ ->
        InAcc2 = CallbackFn(Doc, InAcc),
        receive_events(Pid, RequestId, CallbackFn, InAcc2)
    end.


-spec socket_recv(socket(), size(), timeout()) ->
    {ok, binary()} | {error, closed | inet:posix()}.
socket_recv(_Socket, 0, _Timeout) ->
    {ok, <<>>};
socket_recv(Socket, Length, Timeout) ->
    gen_tcp:recv(Socket, Length, Timeout).


-spec add_pending_request(#state{}, request_id(), term(), {pid(), term()}) -> #state{}.
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


-spec find_pending_request(#state{}, request_id()) -> nil | {term(), {pid(), term()}}.
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
    StreamQueues2 = dict:erase(RequestId, StreamQueues),
    State#state{
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
   StreamQueues2 = dict:store(RequestId, {[], 0, []}, StreamQueues),
   State#state{
       active_streams = ActiveStreams2,
       stream_queues = StreamQueues2
    }.


-spec enqueue_stream_event(#state{}, request_id(), tuple()) ->
                                            {#state{}, non_neg_integer()}.
enqueue_stream_event(State, RequestId, Event) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    {ok, {Waiters, Size, EvQueue}} = dict:find(RequestId, StreamQueues),
    Size2 = Size + erts_debug:size(Event),
    State2 = State#state{
        stream_queues =
            dict:store(RequestId,
                       {Waiters, Size2, EvQueue ++ [Event]},
                       StreamQueues)
    },
    {State2, Size2}.


-spec dequeue_stream_event(#state{}, request_id()) ->
                               {#state{}, tuple(), non_neg_integer()}.
dequeue_stream_event(State, RequestId) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    {ok, {Waiters, Size, [Event | Rest]}} = dict:find(RequestId, StreamQueues),
    Size2 = Size - erts_debug:size(Event),
    State2 = State#state{
        stream_queues =
            dict:store(RequestId, {Waiters, Size2, Rest}, StreamQueues)
    },
    {State2, Event, Size2}.


-spec add_stream_event_waiter(#state{}, request_id(), term()) -> #state{}.
add_stream_event_waiter(State, RequestId, Waiter) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    {ok, {Waiters, Size, EvQueue}} = dict:find(RequestId, StreamQueues),
    Waiters2 = [Waiter | Waiters],
    StreamQueues2 =
    dict:store(RequestId, {Waiters2, Size, EvQueue}, StreamQueues),
    State#state{
       stream_queues = StreamQueues2
    }.


-spec stream_event_present(#state{}, request_id()) -> nil | true | false.
stream_event_present(State, RequestId) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    case dict:find(RequestId, StreamQueues) of
    error ->
        nil;
    {ok, {_, _, Events}} ->
        case length(Events) of
        0 ->
            false;
        _ ->
            true
        end
    end.


-spec stream_event_waiters_present(#state{}, request_id()) -> nil | true | false.
stream_event_waiters_present(State, RequestId) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    case dict:find(RequestId, StreamQueues) of
    error ->
        nil;
    {ok, {Waiters, _, _}} ->
        case length(Waiters) of
        0 ->
            false;
        _ ->
            true
        end
    end.


-spec remove_stream_event_waiter(#state{}, request_id()) -> {#state{}, term()}.
remove_stream_event_waiter(State, RequestId) ->
    #state{
       stream_queues = StreamQueues
    } = State,
    {ok, {[Waiter | Rest], Size, EvQueue}} = dict:find(RequestId, StreamQueues),
    State2 = State#state{
        stream_queues = dict:store(RequestId, {Rest, Size, EvQueue}, StreamQueues)
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
            {error, wrong_partition_version};
        ?UPR_STATUS_ERANGE ->
            {error, wrong_start_sequence_number};
        ?UPR_STATUS_KEY_EEXISTS ->
            {error, vbucket_stream_already_exists};
        ?UPR_STATUS_NOT_MY_VBUCKET ->
            {error, vbucket_stream_not_found};
        _ ->
            {error, {status, Status}}
        end;
    {error, _} = Error ->
        Error
    end.


% The worker process for handling upr connection downstream pipe
% Read and parse downstream messages and send to the gen_server process
-spec receive_worker(socket(), timeout(), pid(), list()) -> any().
receive_worker(Socket, Timeout, Parent, MsgAcc0) ->
    receive
    throttle ->
        receive
        continue ->
            ok
        end;
    continue ->
        ok
    after 0 ->
        ok
    end,
    case socket_recv(Socket, ?UPR_HEADER_LEN, infinity) of
    {ok, Header} ->
        {Action, MsgAcc} =
        case couch_upr_consumer:parse_header(Header) of
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
                StatAcc = lists:reverse(MsgAcc0),
                {done, {stream_response, RequestId, {ok, StatAcc}}};
            _ ->
                case receive_stat(
                    Socket, Timeout, Status, BodyLength, KeyLength) of
                {ok, Stat} ->
                    {true, [Stat | MsgAcc0]};
                {error, _} = Error ->
                    {done, {stream_response, RequestId, Error}}
                end
            end;
        {snapshot_marker, PartId, RequestId} ->
            {done, {stream_event, RequestId, {snapshot_marker, PartId}}};
        {snapshot_mutation, PartId, RequestId, KeyLength, BodyLength,
                ExtraLength, Cas} ->
            Mutation = receive_snapshot_mutation(
                Socket, Timeout, PartId, KeyLength, BodyLength, ExtraLength,
                Cas),
            {done, {stream_event, RequestId, {snapshot_mutation, Mutation}}};
        % For the indexer and XDCR there's no difference between a deletion
        % end an expiration. In both cases the items should get removed.
        % Hence the same code can be used after the initial header
        % parsing (the body is the same).
        {OpCode, PartId, RequestId, KeyLength, BodyLength, Cas} when
                OpCode =:= snapshot_deletion orelse
                OpCode =:= snapshot_expiration ->
            Deletion = receive_snapshot_deletion(
                Socket, Timeout, PartId, KeyLength, BodyLength, Cas),
            {done, {stream_event, RequestId, {snapshot_deletion, Deletion}}};
        {stream_end, PartId, RequestId, BodyLength} ->
            Flag = receive_stream_end(Socket, Timeout, BodyLength),
            {done, {stream_event, RequestId, {stream_end, {RequestId, PartId, Flag}}}}
        end,
        case Action of
        done ->
            Parent ! MsgAcc,
            receive_worker(Socket, Timeout, Parent, []);
        true ->
            receive_worker(Socket, Timeout, Parent, MsgAcc)
        end;
    {error, Reason} ->
        Reason
    end.


-spec should_throttle(#state{}, integer()) -> boolean().
should_throttle(State, Size) ->
    #state{
       throttled = Throttled,
       max_buffer_size = MaxBufSize
    } = State,
    case Throttled of
    true ->
        false;
    false ->
        Size >= MaxBufSize
    end.
