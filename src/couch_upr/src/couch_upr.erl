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

-module(couch_upr).
-behaviour(gen_server).

% Public API
-export([start/2]).
-export([enum_docs_since/7, get_sequence_number/2, drain/1]).
% API for XDCR
-export([sasl_auth/4, open_connection/4]).

% Only used for testing
-export([get_failover_log/2]).

% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

-include("couch_db.hrl").
-include_lib("couch_upr/include/couch_upr.hrl").


-type mutations_fold_fun() :: fun().
-type mutations_fold_acc() :: any().

-record(state, {
    socket = nil           :: socket(),
    timeout = 5000         :: timeout(),
    request_id = 0         :: request_id()
}).


% Public API

-spec start(binary(), binary()) -> {ok, pid()} | ignore |
                                   {error, {already_started, pid()} | term()}.
start(Name, Bucket) ->
    gen_server:start_link(?MODULE, [Name, Bucket], []).


-spec enum_docs_since(pid(), partition_id(), partition_version(), update_seq(),
                      update_seq(), mutations_fold_fun(),
                      mutations_fold_acc()) ->
                             {error, wrong_partition_version |
                              wrong_start_sequence_number} |
                             {rollback, update_seq()} |
                             {ok, mutations_fold_acc(), partition_version()}.
enum_docs_since(_, _, [], _, _, _, _) ->
    % No matching partition version found. Recreate the index from scratch
    {rollback, 0};
enum_docs_since(Pid, PartId, [PartVersion|PartVersions], StartSeq, EndSeq,
        InFun, InAcc) ->
    RequestId = gen_server:call(Pid, get_request_id),
    {Socket, Timeout} = gen_server:call(Pid, get_socket_and_timeout),
    StreamRequest = couch_upr_consumer:encode_stream_request(
        PartId, RequestId, 0, StartSeq, EndSeq, PartVersion),
    ok = gen_tcp:send(Socket, StreamRequest),
    Result = receive_snapshots(Socket, Timeout, InFun, {nil, InAcc}, false),
    case Result of
    {ok, {FailoverLog, Mutations}} ->
        case length(FailoverLog) > ?UPR_MAX_FAILOVER_LOG_SIZE of
        true ->
            throw({error, <<"Failover log contains too many entries">>});
        false ->
            ok
        end,
        {ok, Mutations, FailoverLog};
    % The failover log doesn't match. Try a previous partition version. The
    % last partition in the list will work as it requests with a partition
    % version sequence number of 0, which means requesting from the beginning.
    {error, wrong_partition_version} ->
        enum_docs_since(
            Pid, PartId, PartVersions, StartSeq, EndSeq, InFun, InAcc);
    {error, _} = Error ->
        Error;
    {rollback, RollbackSeq} ->
        {rollback, RollbackSeq}
    end.


-spec get_sequence_number(pid(), partition_id()) ->
                                 {ok, update_seq()} | {error, not_my_vbucket}.
get_sequence_number(Pid, PartId) ->
    RequestId = gen_server:call(Pid, get_request_id),
    {Socket, Timeout} = gen_server:call(Pid, get_socket_and_timeout),
    SeqStatRequest = couch_upr_consumer:encode_seq_stat_request(
        PartId, RequestId),
    ok = gen_tcp:send(Socket, SeqStatRequest),
    case receive_stats(Socket, Timeout, []) of
    {ok, [{error, {?UPR_STATUS_NOT_MY_VBUCKET, _}}]} ->
        {error, not_my_vbucket};
    {ok, Stats} ->
        % The stats return the sequence number as well as the partition UUID,
        % but we care only about the sequence number
        [{_, SeqBin} | _] = Stats,
        {ok, list_to_integer(binary_to_list(SeqBin))}
    end.


% The failover log is a list of 2-tuples with the partition UUID and the
% sequence number when it was created
-spec get_failover_log(pid(), partition_id()) ->
                              {error, no_failover_log_found | upr_status()} |
                              {ok, partition_version()}.
get_failover_log(Pid, PartId) ->
    RequestId = gen_server:call(Pid, get_request_id),
    {Socket, Timeout} = gen_server:call(Pid, get_socket_and_timeout),
    FailoverLogRequest = couch_upr_consumer:encode_failover_log_request(
        PartId, RequestId),
    ok = gen_tcp:send(Socket, FailoverLogRequest),
    case gen_tcp:recv(Socket, ?UPR_HEADER_LEN, Timeout) of
    {ok, Header} ->
        case couch_upr_consumer:parse_header(Header) of
        {failover_log, ?UPR_STATUS_OK, RequestId, BodyLength} ->
            receive_failover_log(Socket, Timeout, BodyLength);
        {failover_log, Status, RequestId, 0} ->
            {error, Status}
        end
    end.


-spec drain(pid()) -> ok.
drain(Pid) ->
    {Socket, _} = gen_server:call(Pid, get_socket_and_timeout),
    case gen_tcp:recv(Socket, 0, 0) of
    {ok, _} ->
        ok;
    {error, timeout} ->
        ok
    end.


-spec sasl_auth(binary(), request_id(), socket(), timeout()) ->
                       ok | {stop, sasl_auth_failed} |
                       {error, closed | inet:posix()}.
sasl_auth(Bucket, RequestId, Socket, Timeout) ->
    Authenticate = couch_upr_consumer:encode_sasl_auth(Bucket, RequestId),
    ok = gen_tcp:send(Socket, Authenticate),
    case gen_tcp:recv(Socket, ?UPR_HEADER_LEN, Timeout) of
    {ok, Header} ->
        {sasl_auth, Status, RequestId, BodyLength} =
            couch_upr_consumer:parse_header(Header),
        % Receive the body so that it is not mangled with the next request,
        % we care about the status only though
        {ok, _} = gen_tcp:recv(Socket, BodyLength, Timeout),
        case Status of
        ?UPR_STATUS_OK ->
            ok;
        ?UPR_STATUS_SASL_AUTH_FAILED ->
            {stop, sasl_auth_failed}
        end;
    {error, _} = Error ->
        Error
    end.


-spec open_connection(binary(), request_id(), socket(), timeout()) ->
                             ok | {error, closed | inet:posix()}.
open_connection(Name, RequestId, Socket, Timeout) ->
    OpenConnection = couch_upr_consumer:encode_open_connection(
        Name, RequestId),
    ok = gen_tcp:send(Socket, OpenConnection),
    case gen_tcp:recv(Socket, ?UPR_HEADER_LEN, Timeout) of
    {ok, Header} ->
        {open_connection, RequestId} = couch_upr_consumer:parse_header(Header),
        ok;
    {error, _} = Error ->
        Error
    end.


% gen_server callbacks

-spec init([binary()]) -> {ok, #state{}} | {stop, sasl_auth_failed}.
init([Name, Bucket]) ->
    UprTimeout = list_to_integer(
        couch_config:get("upr", "connection_timeout")),
    UprPort = list_to_integer(couch_config:get("upr", "port")),
    {ok, Socket} = gen_tcp:connect("localhost", UprPort,
        [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    % Authentication is used to specify from which bucket the data should
    % come from
    case sasl_auth(Bucket, 0, Socket, UprTimeout) of
    ok ->
        ok = open_connection(Name, 1, Socket, UprTimeout),
        {ok, #state{
            socket = Socket,
            timeout = UprTimeout,
            request_id = 2
        }};
    {stop, sasl_auth_failed} = Stop ->
        Stop
    end.


-spec handle_call(atom(), {pid(), reference()}, #state{}) ->
                         {reply, any(), #state{}}.
handle_call(get_request_id, _From, State) ->
    RequestId = case State#state.request_id of
    RequestId0 when RequestId0 < 1 bsl (?UPR_SIZES_OPAQUE + 1) ->
        RequestId0;
    _ ->
        0
    end,
    {reply, RequestId, State#state{request_id=RequestId + 1}};

handle_call(get_socket_and_timeout, _From, State) ->
    {reply, {State#state.socket, State#state.timeout}, State}.


-spec handle_cast(any(), #state{}) ->
                         {stop, {unexpected_cast, any()}, #state{}}.
handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

-spec handle_info(any(), #state{}) ->
                         {stop, {unexpected_msg, any()}, #state{}}.
handle_info(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.


-spec terminate(any(), #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(any(), #state{}, any()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Internal functions

-spec receive_snapshots(socket(), timeout(), mutations_fold_fun(),
                        mutations_fold_acc(), boolean()) ->
                               {ok, mutations_fold_acc()} |
                               {error, wrong_partition_version |
                                wrong_start_sequence_number |
                                closed} |
                               {rollback, update_seq()}.
receive_snapshots(Socket, Timeout, MutationFun, Acc, SnapshotEnd) ->
    case gen_tcp:recv(Socket, ?UPR_HEADER_LEN, Timeout) of
    {ok, Header} ->
        case couch_upr_consumer:parse_header(Header) of
        {stream_request, Status, _RequestId, BodyLength} ->
            case gen_tcp:recv(Socket, BodyLength, Timeout) of
            {ok, Body} ->
            case Status of
                ?UPR_STATUS_OK ->
                    {ok, FailoverLog} =
                        couch_upr_consumer:parse_failover_log(Body),
                    {_, MutationAcc} = Acc,
                    Acc2 = {FailoverLog, MutationAcc},
                    receive_snapshots(
                        Socket, Timeout, MutationFun, Acc2, false);
                ?UPR_STATUS_ROLLBACK ->
                    <<RollbackSeq:?UPR_SIZES_BY_SEQ>> = Body,
                    {rollback, RollbackSeq};
                ?UPR_STATUS_KEY_NOT_FOUND ->
                    {error, wrong_partition_version};
                ?UPR_STATUS_ERANGE ->
                    {error, wrong_start_sequence_number}
                end;
            {error, closed} ->
                {error, closed}
            end;
        {snapshot_marker, _PartId, _RequestId} ->
            % A snapshot marker is a special item, don't take it into
            % account in the accumulator, hence don't pass on the the
            % accumulator.
            receive_snapshots(Socket, Timeout, MutationFun, Acc, true);
        {snapshot_mutation, PartId, _RequestId, KeyLength, BodyLength,
                ExtraLength, Cas} ->
            Mutation = receive_snapshot_mutation(
                Socket, Timeout, PartId, KeyLength, BodyLength, ExtraLength,
                Cas),
            Acc2 = case SnapshotEnd of
            true ->
                process_item(snapshot_marker, MutationFun, Acc);
            false ->
                Acc
            end,
            Acc3 = process_item(Mutation, MutationFun, Acc2),
            receive_snapshots(Socket, Timeout, MutationFun, Acc3, false);
        % For the indexer and XDCR there's no difference between a deletion
        % end an expiration. In both cases the items should get removed.
        % Hence the same code can be used after the initial header
        % parsing (the body is the same).
        {OpCode, PartId, _RequestId, KeyLength, BodyLength, Cas} when
                OpCode =:= snapshot_deletion orelse
                OpCode =:= snapshot_expiration ->
            Deletion = receive_snapshot_deletion(
                Socket, Timeout, PartId, KeyLength, BodyLength, Cas),
            Acc2 = case SnapshotEnd of
            true ->
                process_item(snapshot_marker, MutationFun, Acc);
            false ->
                Acc
            end,
            Acc3 = process_item(Deletion, MutationFun, Acc2),
            receive_snapshots(Socket, Timeout, MutationFun, Acc3, false);
        {stream_end, _PartId, _RequestId, BodyLength} ->
            _Flag = receive_stream_end(Socket, Timeout, BodyLength),
            {ok, Acc}
        end;
    {error, closed} ->
        {error, closed}
    end.


-spec process_item(#doc{} | snapshot_marker, mutations_fold_fun(),
                   mutations_fold_acc()) -> mutations_fold_acc().
process_item(Item, MutationFun, Acc) ->
    {FailoverLog, ItemsAcc} = Acc,
    ItemsAcc2 = MutationFun(Item, ItemsAcc),
    {FailoverLog, ItemsAcc2}.


-spec receive_snapshot_mutation(socket(), timeout(), partition_id(), size(),
                                size(), size(), uint64()) ->
                                       #doc{} | {error, closed}.
receive_snapshot_mutation(Socket, Timeout, PartId, KeyLength, BodyLength,
        ExtraLength, Cas) ->
    case gen_tcp:recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
         {snapshot_mutation, Mutation} =
             couch_upr_consumer:parse_snapshot_mutation(KeyLength, Body,
                 BodyLength, ExtraLength),
         % XXX vmx 2013-08-23: For now, queue in items in the way the current
         %     updater expects them. This can be changed later to a simpler
         %     format.
         #mutation{
             seq = Seq,
             rev_seq = RevSeq,
             flags = Flags,
             expiration = Expiration,
             key = Key,
             value = Value
         } = Mutation,
         #doc{
             id = Key,
             rev = {RevSeq, <<Cas:64, Expiration:32, Flags:32>>},
             body = Value,
             seq = Seq,
             partition = PartId
         };
    {error, closed} ->
        {error, closed}
    end.

-spec receive_snapshot_deletion(socket(), timeout(), partition_id(), size(),
                                size(), uint64()) ->
                                       #doc{} | {error, closed}.
receive_snapshot_deletion(Socket, Timeout, PartId, KeyLength, BodyLength,
        Cas) ->
    case gen_tcp:recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
         {snapshot_deletion, Deletion} =
             couch_upr_consumer:parse_snapshot_deletion(KeyLength, Body),
         % XXX vmx 2013-08-23: For now, queue in items in the way the current
         %     updater expects them. This can be changed later to a simpler
         %     format.
         {Seq, RevSeq, Key, _Metadata} = Deletion,
         #doc{
             id = Key,
             rev = {RevSeq, <<Cas:64, 0:32, 0:32>>},
             deleted = true,
             seq = Seq,
             partition = PartId
         };
    {error, closed} ->
        {error, closed}
    end.

-spec receive_stream_end(socket(), timeout(), size()) ->
                                {ok, <<_:32>>} | {error, closed}.
receive_stream_end(Socket, Timeout, BodyLength) ->
    case gen_tcp:recv(Socket, BodyLength, Timeout) of
    {ok, Flag} ->
        Flag;
    {error, closed} ->
        {error, closed}
    end.


% Returns the failover log as a list 2-tuple pairs with
% partition UUID and sequence number
-spec receive_failover_log(socket(), timeout(), size()) ->
                                  {ok, partition_version()} | {error, closed}.
receive_failover_log(_Socket, _Timeout, 0) ->
    {error, no_failover_log_found};
receive_failover_log(Socket, Timeout, BodyLength) ->
    case gen_tcp:recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
        couch_upr_consumer:parse_failover_log(Body);
    {error, closed} ->
        {error, closed}
    end.


-spec receive_stats(socket(), timeout(), [any()]) ->
                           {ok, [{binary(), binary()}] |
                            [{error, {upr_status(), binary()}}]} |
                           {error, closed}.
receive_stats(Socket, Timeout, Acc) ->
    case gen_tcp:recv(Socket, ?UPR_HEADER_LEN, Timeout) of
    {ok, Header} ->
        case couch_upr_consumer:parse_header(Header) of
        {stats, Status, _RequestId, BodyLength, KeyLength} when
                BodyLength > 0 ->
            {ok, Stat} = receive_stat(
                Socket, Timeout, Status, BodyLength, KeyLength),
            case Stat of
            {error, _} ->
                {ok, lists:reverse([Stat|Acc])};
            _ ->
                receive_stats(Socket, Timeout, [Stat|Acc])
            end;
        {stats, ?UPR_STATUS_OK, _RequestId, 0, 0} ->
            {ok, lists:reverse(Acc)}
        end;
    {error, closed} ->
        {error, closed}
    end.


-spec receive_stat(socket(), timeout(), upr_status(), size(), size()) ->
                          {ok, {binary(), binary()} |
                           {error, {upr_status(), binary()}}} |
                          {error, closed}.
receive_stat(Socket, Timeout, Status, BodyLength, KeyLength) ->
    case gen_tcp:recv(Socket, BodyLength, Timeout) of
    {ok, Body} ->
        couch_upr_consumer:parse_stat(
            Body, Status, KeyLength, BodyLength - KeyLength);
    {error, closed} ->
        {error, closed}
    end.
