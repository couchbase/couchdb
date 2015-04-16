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

-module(couch_dcp_fake_server).
-behaviour(gen_server).

% Public API
-export([start/1, reset/0]).

% Only uses by tests
-export([set_failover_log/2, set_persisted_items_fun/1,
    set_items_per_snapshot/1, set_dups_per_snapshot/1,
    pause_mutations/0, ceil_div/2, num_items_with_dups/3]).
-export([send_single_mutation/0, continue_mutations/0]).
-export([get_num_buffer_acks/0, is_control_req/0, close_connection/1]).

% Needed for internal process spawning
-export([accept/1, accept_loop/1]).

% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
    code_change/3]).

-include_lib("couch_dcp/include/couch_dcp.hrl").
-include_lib("couch_dcp/include/couch_dcp_typespecs.hrl").


-define(dbname(SetName, PartId),
    <<SetName/binary, $/, (list_to_binary(integer_to_list(PartId)))/binary>>).


% #doc_info{}, #doc{}, #db{} are copy & pasted from couch_db.hrl
-record(doc_info,
    {
    id = <<"">>,
    deleted = false,
    local_seq,
    rev = {0, <<>>},
    body_ptr,
    content_meta = 0, % should be 0-255 only.
    size = 0
    }).
-record(doc,
    {
    id = <<>>,
    rev = {0, <<>>},

    % the binary body
    body = <<"{}">>,
    content_meta = 0, % should be 0-255 only.

    deleted = false,

    % key/value tuple of meta information, provided when using special options:
    % couch_db:open_doc(Db, Id, Options).
    meta = []
    }).
-record(db,
    {main_pid = nil,
    update_pid = nil,
    compactor_info = nil,
    instance_start_time, % number of microsecs since jan 1 1970 as a binary string
    fd,
    fd_ref_counter,
    header,% = #db_header{},
    committed_update_seq,
    docinfo_by_id_btree,
    docinfo_by_seq_btree,
    local_docs_btree,
    update_seq,
    name,
    filepath,
    security = [],
    security_ptr = nil,
    user_ctx,% = #user_ctx{},
    waiting_delayed_commit = nil,
    fsync_options = [],
    options = []
    }).


-record(state, {
    streams = [],
    setname = nil,
    failover_logs = dict:new(),
    pause_mutations = false,
    % This dictionary contains the meta data for flow control
    % that will be used by unit tests.
    control_flow = dict:new(),
    % This is a function is used to simulate the stream request on persited
    % items only. It returns the sequence number up to which the items should
    % be considered as persisted.
    % By default it returns half of the current high sequence number and at
    % minimum 1. This ensures that a lot of the current test suites runs
    % across the code path of restarting the updater.
    persisted_items_fun = fun(Seq) -> max(Seq div 2, 1) end,
    % Determines how many items a snapshot should have, if it's set to 0,
    % all mutations will be returned in a single snapshot.
    items_per_snapshot = 0,
    % The number of duplicates a snapshot should contain. This option needs
    % to be combined with items_per_snapshot. A snapshot will contain the
    % given number of items from a previous snapshot (randomly chosen).
    dups_per_snapshot = 0
}).


% Public API

-spec start(binary()) -> {ok, pid()} | ignore |
                         {error, {already_started, pid()} | term()}.
start(SetName) ->
    % Start the fake DCP server where the original one is expected to be
    Port = list_to_integer(couch_config:get("dcp", "port", "0")),
    gen_server:start({local, ?MODULE}, ?MODULE, [Port, SetName], []).

-spec reset() -> ok.
reset() ->
    gen_server:call(?MODULE, reset).

% Only used by tests to populate the failover log
-spec set_failover_log(partition_id(), partition_version()) -> ok.
set_failover_log(PartId, FailoverLog) ->
    gen_server:call(?MODULE, {set_failover_log, PartId, FailoverLog}).

% For unit tests only
-spec set_persisted_items_fun(fun((update_seq()) -> update_seq())) -> ok.
set_persisted_items_fun(Fun) ->
    gen_server:call(?MODULE, {set_persisted_items_fun, Fun}).

% For unit tests only
-spec set_items_per_snapshot(non_neg_integer()) -> ok.
set_items_per_snapshot(Num) ->
    gen_server:call(?MODULE, {set_items_per_snapshot, Num}).

% For unit tests only
-spec set_dups_per_snapshot(non_neg_integer()) -> ok.
set_dups_per_snapshot(Num) ->
    gen_server:call(?MODULE, {set_dups_per_snapshot, Num}).

-spec pause_mutations() -> ok.
pause_mutations() ->
    gen_server:call(?MODULE, {pause_mutations, true}).

-spec continue_mutations() -> ok.
continue_mutations() ->
    ok = gen_server:call(?MODULE, {pause_mutations, false}),
    ?MODULE ! send_mutations,
    ok.

% Used by unit tests. Check if server got buffer ack.
-spec get_num_buffer_acks() -> integer().
get_num_buffer_acks() ->
    gen_server:call(?MODULE, get_num_buffer_acks).

% Used by unit tests. Check if server got control request.
-spec is_control_req() -> boolean().
is_control_req() ->
    gen_server:call(?MODULE, is_control_req).

% Used by unit test.
close_connection(PartId) ->
    ok = gen_server:call(?MODULE, {close_connection, PartId}).

% Used by unit test to control flow of mutation message from server to client
-spec send_single_mutation() -> ok.
send_single_mutation() ->
    ?MODULE ! send_mutations,
    ok.

% gen_server callbacks

-spec init([port() | binary()]) -> {ok, #state{}}.
init([Port, SetName]) ->
    {ok, Listen} = gen_tcp:listen(Port,
        [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    case Port of
    % In case the port was set to "0", the OS will decide which port to run
    % the fake DCP server on. Update the configuration so that we know which
    % port was chosen (that's only needed for the tests).
    0 ->
        {ok, RandomPort} = inet:port(Listen),
        couch_config:set("dcp", "port", integer_to_list(RandomPort), false);
    _ ->
        ok
    end,
    accept(Listen),
    {ok, #state{
        streams = [],
        setname = SetName
    }}.


-spec handle_call(tuple() | atom(), {pid(), reference()}, #state{}) ->
                         {reply, any(), #state{}}.
handle_call({add_stream, PartId, RequestId, StartSeq, EndSeq, Socket, FailoverLog}, _From, State) ->
    #state{
        streams = Streams,
        pause_mutations = Pause,
        items_per_snapshot = ItemsPerSnapshot,
        dups_per_snapshot = DupsPerSnapshot
    } = State,
    case lists:keyfind(PartId, 1, State#state.streams) of
    false ->
        StreamOk = couch_dcp_producer:encode_stream_request_ok(
            RequestId, FailoverLog),
        ok = gen_tcp:send(Socket, StreamOk),
        Mutations = case DupsPerSnapshot > 0 of
        true ->
            NumSnapshots = ceil_div(EndSeq - StartSeq, ItemsPerSnapshot),
            % The first snapshot mustn't contain duplicates
            EndSeq2 = EndSeq - (NumSnapshots - 1) * DupsPerSnapshot,
            Mutations2 = create_mutations(State#state.setname, PartId,
                StartSeq, EndSeq2),
            Mutations3 = case StartSeq of
            0 ->
                create_mutations_dups(Mutations2, ItemsPerSnapshot,
                    DupsPerSnapshot, 0);
            _ ->
                % create_mutations_dups/3 doesn't create duplicates in the
                % first snapshot. If we are not in an initial index build
                % (start seq > 0), then the first snapshot *must* contain
                % duplicates. To KISS we just take DupsPerSnapshot number of
                % items from the second (still unique) snapshot and prepend
                % them.
                Prepend = lists:sublist(Mutations2, ItemsPerSnapshot + 1,
                    DupsPerSnapshot),
                Dups = create_mutations_dups(Mutations2, ItemsPerSnapshot,
                    DupsPerSnapshot, DupsPerSnapshot),
                Prepend ++ Dups
            end,
            apply_sequence_numbers(Mutations3);
        false ->
            create_mutations(State#state.setname, PartId, StartSeq, EndSeq)
        end,
        Num = case ItemsPerSnapshot of
        0 ->
            length(Mutations);
        _ ->
            min(length(Mutations), ItemsPerSnapshot)
        end,
        Streams2 =
            [{PartId, {RequestId, Mutations, Socket, 0}} | Streams],
        case Pause of
        true ->
            ok;
        false ->
            % For unit tests it's OK to pretend that only snapshots
            % received from the start are on-disk snapshots.
            SnapshotType = case StartSeq of
            0 ->
                ?DCP_SNAPSHOT_TYPE_DISK;
            _ ->
                ?DCP_SNAPSHOT_TYPE_MEMORY
            end,
            Marker = couch_dcp_producer:encode_snapshot_marker(
                PartId, RequestId, StartSeq, StartSeq + Num, SnapshotType),
            ok = gen_tcp:send(Socket, Marker),
            self() ! send_mutations
        end,
        {reply, ok, State#state{streams = Streams2}};
    _ ->
        StreamExists = couch_dcp_producer:encode_stream_request_error(
                         RequestId, ?DCP_STATUS_KEY_EEXISTS),
        ok = gen_tcp:send(Socket, StreamExists),
        {reply, ok, State}
    end;

handle_call({remove_stream, PartId}, _From, #state{streams = Streams} = State) ->
    Streams2 = lists:keydelete(PartId, 1, Streams),
    Reply = case length(Streams2) =:= length(Streams) of
    true ->
        vbucket_stream_not_found;
    false ->
        ok
    end,
    {reply, Reply, State#state{streams = Streams2}};

handle_call({set_failover_log, PartId, FailoverLog}, _From, State) ->
    FailoverLogs = dict:store(PartId, FailoverLog, State#state.failover_logs),
    {reply, ok, State#state{
        failover_logs = FailoverLogs
    }};

handle_call({set_persisted_items_fun, Fun}, _From, State) ->
    {reply, ok, State#state{
        persisted_items_fun = Fun
    }};

handle_call({set_items_per_snapshot, Num}, _From, State) ->
    {reply, ok, State#state{
        items_per_snapshot = Num
    }};

handle_call({set_dups_per_snapshot, Num}, _From, State) ->
    {reply, ok, State#state{
        dups_per_snapshot = Num
    }};

handle_call({all_seqs, Socket, RequestId}, _From, State) ->
  #state{
        setname = SetName,
        items_per_snapshot = ItemsPerSnapshot,
        dups_per_snapshot = DupsPerSnapshot
    } = State,
    Partitions = list_partitions(SetName),
    Result = lists:foldl(fun(PartId, Acc) ->
        case get_sequence_number(SetName, PartId, ItemsPerSnapshot,
            DupsPerSnapshot) of
        {ok, Seq} ->
            [{PartId, Seq} | Acc]
        end
    end, [], Partitions),
    Data = couch_dcp_producer:encode_seqs(RequestId, lists:reverse(Result)),
    ok = gen_tcp:send(Socket, Data),
    {reply, ok, State};

handle_call({send_stat, Stat, Socket, RequestId, PartId}, _From, State) ->
    #state{
        setname = SetName
    } = State,
    case binary:split(Stat, <<" ">>) of
    [<<"vbucket-seqno">>] ->
        Partitions = list_partitions(SetName),
        send_vbucket_seqnos_stats(State, SetName, Socket, RequestId, Partitions);
    [<<"vbucket-seqno">>, BinPartId] ->
        PartIdInt = list_to_integer(binary_to_list(BinPartId)),
        send_vbucket_seqnos_stats(State, SetName, Socket, RequestId, [PartIdInt]);
    [<<"vbucket-details">>, _] ->
        case get_num_items(SetName, PartId) of
        {ok, NumItems} ->
            BinPartId = list_to_binary(integer_to_list(PartId)),
            NumItemsKey = <<"vb_", BinPartId/binary ,":num_items">>,
            NumItemsValue = list_to_binary(integer_to_list(NumItems)),
            % The real vbucket-details response contains a lot of more
            % stats, but we only care about the num_items
            NumItemsStat = couch_dcp_producer:encode_stat(
                RequestId, NumItemsKey, NumItemsValue),
            ok = gen_tcp:send(Socket, NumItemsStat),

            EndStat = couch_dcp_producer:encode_stat(RequestId, <<>>, <<>>),
            ok = gen_tcp:send(Socket, EndStat);
        {error, not_my_partition} ->
            StatError = couch_dcp_producer:encode_stat_error(
                RequestId, ?DCP_STATUS_NOT_MY_VBUCKET, <<>>),
            ok = gen_tcp:send(Socket, StatError)
        end
    end,
    {reply, ok, State};

handle_call({get_failover_log, PartId}, _From, State) ->
    FailoverLog = get_failover_log(PartId, State),
    {reply, FailoverLog, State};

handle_call(get_set_name, _From, State) ->
    {reply, State#state.setname, State};

handle_call(get_persisted_items_fun, _From, State) ->
    {reply, State#state.persisted_items_fun, State};

handle_call(get_items_per_snapshot, _From, State) ->
    {reply, State#state.items_per_snapshot, State};

handle_call(get_dups_per_snapshot, _From, State) ->
    {reply, State#state.dups_per_snapshot, State};

handle_call({pause_mutations, Flag}, _From, State) ->
    {reply, ok, State#state{pause_mutations = Flag}};

handle_call(reset, _From, State0) ->
    State = #state{
        setname = State0#state.setname
    },
    {reply, ok, State};

% Increment the count for the buffer ack request
handle_call({handle_buffer_ack, _Size}, _From, State) ->
    #state {
        control_flow = ControlFlow
    } = State,
    Val = case dict:find(num_buffer_acks, ControlFlow) of
    error ->
        1;
    {ok, Value} ->
        Value + 1
    end,
    ControlFlow2 = dict:store(num_buffer_acks, Val, ControlFlow),
    {reply, ok, State#state{
        control_flow = ControlFlow2
    }};

handle_call({handle_control_req, Size}, _From, State) ->
    #state {
        control_flow = ControlFlow
    } = State,
    ControlFlow2 = dict:store(control_req, Size, ControlFlow),
    {reply, ok, State#state{
        control_flow = ControlFlow2
    }};

% Return the current count of buffer ack requests
handle_call(get_num_buffer_acks, _From, State) ->
    #state {
        control_flow = ControlFlow
    } = State,
    Val = case dict:find(num_buffer_acks, ControlFlow) of
    error ->
        0;
    {ok, Value} ->
        Value
    end,
    {reply, Val, State};

% Return true if we got control request
handle_call(is_control_req, _From, State) ->
    #state {
        control_flow = ControlFlow
    } = State,
    Val = case dict:find(control_req, ControlFlow) of
    error ->
        false;
    _ ->
        true
    end,
    {reply, Val, State};

% Close all connections if PartId = nil
handle_call({close_connection, PartId}, _From, State) ->
     #state{
       streams = Streams
    } = State,
    Streams2 = lists:foldl(
        fun({PartId2, {_RequestId, _Mutation, Socket, _HiSeq}} = Entry, Acc) ->
            case PartId of
            nil ->
                ok = gen_tcp:close(Socket),
                Acc;
            PartId2 ->
                ok = gen_tcp:close(Socket),
                Acc;
            _ ->
                [Entry | Acc]
            end
        end,
    [], Streams),
    State2 = State#state{streams = Streams2},
    {reply, ok, State2}.

-spec handle_cast(any(), #state{}) ->
                         {stop, {unexpected_cast, any()}, #state{}}.
handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

-spec handle_info({'EXIT', {pid(), reference()}, normal} |
                  send_mutations,
                  #state{}) -> {noreply, #state{}}.
handle_info({'EXIT', _From, normal}, State)  ->
    {noreply, State};

handle_info(send_mutations, State) ->
    #state{
       streams = Streams,
       pause_mutations = Pause,
       items_per_snapshot = ItemsPerSnapshot
    } = State,
    Streams2 = lists:foldl(fun
        ({VBucketId, {RequestId, [Mutation | Rest], Socket, NumSent0}}, Acc) ->
            {Cas, Seq, RevSeq, Flags, Expiration, LockTime, Key, Value} = Mutation,
            NumSent = case ItemsPerSnapshot > 0 andalso
                    NumSent0 =:= ItemsPerSnapshot of
            true ->
                NumItems = min(length(Rest) + 1, ItemsPerSnapshot),
                Marker = couch_dcp_producer:encode_snapshot_marker(
                    VBucketId, RequestId, Seq - 1, Seq + NumItems - 1,
                    ?DCP_SNAPSHOT_TYPE_MEMORY),
                ok = gen_tcp:send(Socket, Marker),
                1;
            false ->
                NumSent0 + 1
            end,
            Encoded = case Value of
            deleted ->
                couch_dcp_producer:encode_snapshot_deletion(
                VBucketId, RequestId, Cas, Seq, RevSeq, Key);
            _ ->
                couch_dcp_producer:encode_snapshot_mutation(
                VBucketId, RequestId, Cas, Seq, RevSeq, Flags, Expiration,
                LockTime, Key, Value)
            end,
            ok = gen_tcp:send(Socket, Encoded),
            [{VBucketId, {RequestId, Rest, Socket, NumSent}} | Acc];
        ({VBucketId, {RequestId, [], Socket, _NumSent}}, Acc) ->
            StreamEnd = couch_dcp_producer:encode_stream_end(VBucketId, RequestId),
            ok = gen_tcp:send(Socket, StreamEnd),
            Acc
        end, [], Streams),
    case length(Streams2) of
    0 ->
        ok;
    _ ->
        case Pause of
        false ->
            self() ! send_mutations;
        true ->
            ok
        end
    end,
    {noreply, State#state{streams = Streams2}}.


-spec terminate(any(), #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(any(), #state{}, any()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Internal functions

-spec get_failover_log(partition_id()) -> partition_version().
get_failover_log(PartId) ->
    gen_server:call(?MODULE, {get_failover_log, PartId}).

-spec get_failover_log(partition_id(), #state{}) -> partition_version().
get_failover_log(PartId, State) ->
    case dict:find(PartId, State#state.failover_logs) of
    {ok, FailoverLog} ->
        FailoverLog;
    error ->
        % Use a different partition UUID for every partition
        [{1000 + PartId, 0}]
    end.


% Returns the current high sequence number of a partition
-spec get_sequence_number(binary(), partition_id()) ->
                                 {ok, update_seq()} |
                                 {error, not_my_partition}.
get_sequence_number(SetName, PartId) ->
    ItemsPerSnapshot = gen_server:call(?MODULE, get_items_per_snapshot),
    DupsPerSnapshot = gen_server:call(?MODULE, get_dups_per_snapshot),
    get_sequence_number(SetName, PartId, ItemsPerSnapshot,
        DupsPerSnapshot).

-spec get_sequence_number(binary(), partition_id(), non_neg_integer(),
                          non_neg_integer()) ->
                                 {ok, update_seq()} |
                                 {error, not_my_partition}.
get_sequence_number(SetName, PartId, ItemsPerSnapshot,
        DupsPerSnapshot) ->
    case open_db(SetName, PartId) of
    {ok, Db} ->
        Seq0 = Db#db.update_seq,
        couch_db:close(Db),
        Seq = case DupsPerSnapshot > 0 of
        true ->
            num_items_with_dups(Seq0, ItemsPerSnapshot, DupsPerSnapshot);
        false ->
             Seq0
        end,
        {ok, Seq};
    {error, cannot_open_db} ->
        {error, not_my_partition}
    end.


% Returns the current number of items of a partition
-spec get_num_items(binary(), partition_id()) -> {ok, non_neg_integer()} |
                                                 {error, not_my_partition}.
get_num_items(SetName, PartId) ->
    case open_db(SetName, PartId) of
    {ok, Db} ->
        {ok, DbInfo} = couch_db:get_db_info(Db),
        ok = couch_db:close(Db),
        NumItems = couch_util:get_value(doc_count, DbInfo),
        {ok, NumItems};
    {error, cannot_open_db} ->
        {error, not_my_partition}
    end.


-spec accept(socket()) -> pid().
accept(Listen) ->
    process_flag(trap_exit, true),
    spawn_link(?MODULE, accept_loop, [Listen]).

-spec accept_loop(socket()) -> ok.
accept_loop(Listen) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    % Let the server spawn a new process and replace this loop
    % with the read loop, to avoid blocking
    accept(Listen),
    read(Socket).


-spec read(socket()) -> ok.
read(Socket) ->
    case gen_tcp:recv(Socket, ?DCP_HEADER_LEN) of
    {ok, Header} ->
        case couch_dcp_producer:parse_header(Header) of
        {open_connection, BodyLength, RequestId} ->
            handle_open_connection_body(Socket, BodyLength, RequestId);
        {stream_request, BodyLength, RequestId, PartId} ->
            handle_stream_request_body(Socket, BodyLength, RequestId, PartId);
        {failover_log, RequestId, PartId} ->
            handle_failover_log(Socket, RequestId, PartId);
        {stats, BodyLength, RequestId, PartId} ->
            handle_stats_body(Socket, BodyLength, RequestId, PartId);
        {sasl_auth, BodyLength, RequestId} ->
            handle_sasl_auth_body(Socket, BodyLength, RequestId);
        {stream_close, RequestId, PartId} ->
            handle_stream_close_body(Socket, RequestId, PartId);
        {select_bucket, BodyLength, RequestId} ->
            handle_select_bucket_body(Socket, BodyLength, RequestId);
        {control_request, BodyLength, RequestId} ->
            handle_control_request(Socket, BodyLength, RequestId);
        {buffer_ack, BodyLength, RequestId} ->
            handle_buffer_ack_request(Socket, BodyLength, RequestId);
        {all_seqs, RequestId} ->
            handle_all_seqs(Socket, RequestId)
        end,
        read(Socket);
    {error, closed} ->
        ok
    end.

-spec handle_control_request(socket(), size(), request_id()) -> ok.
handle_control_request(Socket, BodyLength, RequestId) ->
    case gen_tcp:recv(Socket, BodyLength) of
    {ok, <<"connection_buffer_size", Size/binary>>} ->
        ok = gen_server:call(?MODULE, {handle_control_req, Size})
    end,
    ControlResponse = couch_dcp_producer:encode_control_flow_ok(RequestId),
    ok = gen_tcp:send(Socket, ControlResponse).

-spec handle_buffer_ack_request(socket(), size(), request_id()) -> ok .
handle_buffer_ack_request(Socket, BodyLength, RequestId) ->
    case gen_tcp:recv(Socket, BodyLength) of
    {ok, <<Size:?DCP_SIZES_BUFFER_SIZE>>} ->
        gen_server:call(?MODULE, {handle_buffer_ack, Size})
    end,
    BufferResponse = couch_dcp_producer:encode_buffer_ack_ok(RequestId),
    ok = gen_tcp:send(Socket, BufferResponse).


% XXX vmx: 2014-01-24: Proper logging/error handling is missing
-spec handle_open_connection_body(socket(), size(), request_id()) ->
                                         ok | {error, closed}.
handle_open_connection_body(Socket, BodyLength, RequestId) ->
    case gen_tcp:recv(Socket, BodyLength) of
    {ok, <<_SeqNo:?DCP_SIZES_SEQNO,
           ?DCP_FLAG_PRODUCER:?DCP_SIZES_FLAGS,
           _Name/binary>>} ->
        OpenConnection = couch_dcp_producer:encode_open_connection(RequestId),
        ok = gen_tcp:send(Socket, OpenConnection);
    {error, closed} ->
        {error, closed}
    end.

-spec handle_stream_request_body(socket(), size(), request_id(),
                                 partition_id()) -> ok | {error, closed}.
handle_stream_request_body(Socket, BodyLength, RequestId, PartId) ->
    case gen_tcp:recv(Socket, BodyLength) of
    % TODO vmx 2014-04-04: Make a rollback due to wrong SnapshotStart/End
    {ok, <<Flags:?DCP_SIZES_FLAGS,
           _Reserved:?DCP_SIZES_RESERVED,
           StartSeq:?DCP_SIZES_BY_SEQ,
           EndSeq:?DCP_SIZES_BY_SEQ,
           PartUuid:?DCP_SIZES_PARTITION_UUID,
           _SnapshotStart:?DCP_SIZES_BY_SEQ,
           _SnapshotEnd:?DCP_SIZES_BY_SEQ>>} ->
        FailoverLog = get_failover_log(PartId),
        case StartSeq > EndSeq of
        true ->
            send_error(Socket, RequestId, ?DCP_STATUS_ERANGE);
        false ->
            EndSeq2 = case Flags of
            ?DCP_FLAG_NOFLAG ->
                EndSeq;
            ?DCP_FLAG_USELATEST_ENDSEQNO ->
                EndSeq;
            Flags when (Flags band ?DCP_FLAG_DISKONLY) =/= 0 ->
                % Either of the following flags:
                % DCP_FLAG_DISKONLY
                % (DCP_FLAG_DISKONLY bor DCP_FLAG_USELATEST_ENDSEQNO)
                ItemsPerSnapshot = gen_server:call(
                    ?MODULE, get_items_per_snapshot),
                case ItemsPerSnapshot of
                0 ->
                    PersistedItemsFun = gen_server:call(
                        ?MODULE, get_persisted_items_fun),
                    PersistedItemsFun(EndSeq);
                % The items per snapshot have higher priority than the
                % persisted items function
                _ ->
                    ItemsPerSnapshot
                end
            end,
            Found = case lists:keyfind(PartUuid, 1, FailoverLog) of
            {PartUuid, PartVersionSeq} ->
                true;
            false ->
                PartVersionSeq = 0,
                false
            end,
            case Found orelse StartSeq =:= 0 of
            true ->
                send_ok_or_error(
                    Socket, RequestId, PartId, StartSeq, EndSeq2, PartUuid,
                    PartVersionSeq, FailoverLog);
            false ->
                send_error(Socket, RequestId, ?DCP_STATUS_KEY_NOT_FOUND)
            end
        end;
    {error, closed} ->
        {error, closed}
    end.

handle_stream_close_body(Socket, RequestId, PartId) ->
    Status = case gen_server:call(?MODULE, {remove_stream, PartId}) of
    ok ->
        ?DCP_STATUS_OK;
    vbucket_stream_not_found ->
        ?DCP_STATUS_KEY_NOT_FOUND
    end,
    Resp = couch_dcp_producer:encode_stream_close_response(
        RequestId, Status),
    ok = gen_tcp:send(Socket, Resp).

handle_select_bucket_body(Socket, BodyLength, RequestId) ->
    {ok, _} = gen_tcp:recv(Socket, BodyLength),
    Status = ?DCP_STATUS_OK,
    Resp = couch_dcp_producer:encode_select_bucket_response(
        RequestId, Status),
    ok = gen_tcp:send(Socket, Resp).

-spec send_ok_or_error(socket(), request_id(), partition_id(), update_seq(),
                       update_seq(), uuid(), update_seq(),
                       partition_version()) -> ok.
send_ok_or_error(Socket, RequestId, PartId, StartSeq, EndSeq, PartUuid,
        PartVersionSeq, FailoverLog) ->
    SetName = gen_server:call(?MODULE, get_set_name),
    {ok, HighSeq} = get_sequence_number(SetName, PartId),

    case StartSeq =:= 0 of
    true ->
        send_ok(Socket, RequestId, PartId, StartSeq, EndSeq, FailoverLog);
    false ->
        % The server might already have a different future than the client
        % has (the client and the server have a common history, but the server
        % is ahead with new failover log entries). We need to make sure the
        % requested `StartSeq` is lower than the sequence number of the
        % failover log entry that comes next (if there is any).
        DiffFailoverLog = lists:takewhile(fun({LogPartUuid, _}) ->
            LogPartUuid =/= PartUuid
        end, FailoverLog),

        case DiffFailoverLog of
        % Same history
        [] ->
            case StartSeq =< HighSeq of
            true ->
                send_ok(
                    Socket, RequestId, PartId, StartSeq, EndSeq, FailoverLog);
            false ->
                % The client tries to get items from the future, which
                % means that it got ahead of the server somehow.
                send_error(Socket, RequestId, ?DCP_STATUS_ERANGE)
            end;
        _ ->
            {_, NextHighSeqNum} = lists:last(DiffFailoverLog),
            case StartSeq < NextHighSeqNum of
            true ->
                send_ok(
                    Socket, RequestId, PartId, StartSeq, EndSeq, FailoverLog);
            false ->
                send_rollback(Socket, RequestId, PartVersionSeq)
            end
        end
    end.

-spec send_ok(socket(), request_id(), partition_id(), update_seq(),
              update_seq(), partition_version()) -> ok.
send_ok(Socket, RequestId, PartId, StartSeq, EndSeq, FailoverLog) ->
        ok = gen_server:call(?MODULE, {add_stream, PartId, RequestId,
                                       StartSeq, EndSeq, Socket, FailoverLog}).

-spec send_rollback(socket(), request_id(), update_seq()) -> ok.
send_rollback(Socket, RequestId, RollbackSeq) ->
    StreamRollback = couch_dcp_producer:encode_stream_request_rollback(
        RequestId, RollbackSeq),
    ok = gen_tcp:send(Socket, StreamRollback).

-spec send_error(socket(), request_id(), dcp_status()) -> ok.
send_error(Socket, RequestId, Status) ->
    StreamError = couch_dcp_producer:encode_stream_request_error(
        RequestId, Status),
    ok = gen_tcp:send(Socket, StreamError).


-spec handle_failover_log(socket(), request_id(), partition_id()) -> ok.
handle_failover_log(Socket, RequestId, PartId) ->
    FailoverLog = get_failover_log(PartId),
    FailoverLogResponse = couch_dcp_producer:encode_failover_log(
        RequestId, FailoverLog),
    ok = gen_tcp:send(Socket, FailoverLogResponse).


-spec handle_stats_body(socket(), size(), request_id(), partition_id()) ->
                               ok | not_yet_implemented |
                               {error, closed | not_my_partition}.
handle_stats_body(Socket, BodyLength, RequestId, PartId) ->
    case gen_tcp:recv(Socket, BodyLength) of
    {ok, Stat} ->
        gen_server:call(?MODULE, {send_stat, Stat, Socket, RequestId, PartId});
    {error, closed} ->
        {error, closed}
    end.

handle_all_seqs(Socket, RequestId) ->
    gen_server:call(?MODULE, {all_seqs, Socket, RequestId}).

% XXX vmx: 2014-01-24: Proper logging/error handling is missing
-spec handle_sasl_auth_body(socket(), size(), request_id()) ->
                                   ok | {error, closed}.
handle_sasl_auth_body(Socket, BodyLength, RequestId) ->
    case gen_tcp:recv(Socket, BodyLength) of
    % NOTE vmx 2014-01-10: Currently there's no real authentication
    % implemented in the fake server. Just always send back the authentication
    % was successful
    {ok, _} ->
        Authenticated = couch_dcp_producer:encode_sasl_auth(RequestId),
        ok = gen_tcp:send(Socket, Authenticated);
    {error, closed} ->
        {error, closed}
    end.


% This function creates mutations for one snapshot of one partition of a
% given size
-spec create_mutations(binary(), partition_id(), update_seq(), update_seq()) ->
                              [tuple()].
create_mutations(SetName, PartId, StartSeq, EndSeq) ->
    {ok, Db} = open_db(SetName, PartId),
    DocsFun = fun(DocInfo, Acc) ->
        #doc_info{
            id = DocId,
            deleted = Deleted,
            local_seq = Seq,
            rev = Rev
        } = DocInfo,
        Value = case Deleted of
        true ->
           deleted;
        false ->
            {ok, CouchDoc} = couch_db:open_doc_int(Db, DocInfo, []),
            iolist_to_binary(CouchDoc#doc.body)
        end,
        {RevSeq, Cas, Expiration, Flags} = extract_revision(Rev),
        {ok, [{Cas, Seq, RevSeq, Flags, Expiration, 0, DocId, Value}|Acc]}
    end,
    {ok, _NumDocs, Docs} = couch_db:fast_reads(Db, fun() ->
        couch_db:enum_docs_since(Db, StartSeq, DocsFun, [],
                                 [{end_key, EndSeq}])
    end),
    couch_db:close(Db),
    lists:reverse(Docs).


% Extract the CAS and flags out of thr revision
% The couchdb unit tests don't fill in a proper revision, but an empty binary
-spec extract_revision({non_neg_integer(), <<_:128>>}) ->
                              {non_neg_integer(), non_neg_integer(),
                               non_neg_integer(), non_neg_integer()}.
extract_revision({RevSeq, <<>>}) ->
    {RevSeq, 0, 0, 0};
% https://github.com/couchbase/ep-engine/blob/master/src/couch-kvstore/couch-kvstore.cc#L212-L216
extract_revision({RevSeq, RevMeta}) ->
    <<Cas:64, Expiration:32, Flags:32>> = RevMeta,
    {RevSeq, Cas, Expiration, Flags}.


-spec open_db(binary(), partition_id()) ->
                     {ok, #db{}} | {error, cannot_open_db}.
open_db(SetName, PartId) ->
    case couch_db:open_int(?dbname(SetName, PartId), []) of
    {ok, PartDb} ->
        {ok, PartDb};
    _Error ->
        {error, cannot_open_db}
    end.


-spec ceil_div(non_neg_integer(), pos_integer()) -> non_neg_integer().
ceil_div(Numerator, Denominator) ->
    (Numerator div Denominator) + min(Numerator rem Denominator, 1).


-spec create_mutations_dups([tuple()], pos_integer(), pos_integer(),
                            non_neg_integer()) -> [tuple()].
create_mutations_dups(_Mutations, ItemsPerSnapshot, DupsPerSnapshot,
        _InitCount) when DupsPerSnapshot >= (ItemsPerSnapshot div 2) ->
    % Else the algorithm for creating duplicates doesn't work properly
    throw({error, <<"The number of duplicates must be lower than half of "
        "the items per snapshot">>});
create_mutations_dups(Mutations, ItemsPerSnapshot, DupsPerSnapshot,
        InitCount) ->
    % Make sure every test run leads to the same result
    random:seed(5, 6, 7),
    {Mutations2, _} = lists:foldl(fun(Mutation, {Acc, I}) ->
        case I > 0 andalso (I rem ItemsPerSnapshot) =:= 0 of
        true ->
            Shuffled = [X || {_, X} <- lists:sort(
                [{random:uniform(), M} || M <- lists:usort(Acc)])],
            RandomMutations = lists:sublist(Shuffled, DupsPerSnapshot),
            {Acc ++ RandomMutations ++ [Mutation], I + 1 + DupsPerSnapshot};
        false ->
            {Acc ++ [Mutation], I + 1}
        end
    end, {[], InitCount}, Mutations),
    Mutations2.


% Apply sequentially increasing sequence numbers to the mutations
-spec apply_sequence_numbers([tuple()]) -> [tuple()].
apply_sequence_numbers(Mutations) ->
    StartSeq = element(2, hd(Mutations)),
    {Mutations2, _} = lists:mapfoldl(fun(M, I) ->
        {setelement(2, M, I), I + 1}
    end, StartSeq, Mutations),
    Mutations2.


% When a certain amount of items is added per snapshot, it can happen that
% the number of items overflow into a new snapshot which can keep cascading
-spec num_items_with_dups(pos_integer(), pos_integer(), pos_integer()) ->
                                 pos_integer().
num_items_with_dups(NumItems, ItemsPerSnapshot, DupsPerSnapshot) ->
    NumSnapshots = couch_dcp_fake_server:ceil_div(NumItems, ItemsPerSnapshot),
    % The first snapshot doesn't contain duplicates hence "- 1"
    num_items_with_dups(
        NumItems + (NumSnapshots - 1) * DupsPerSnapshot,
        ItemsPerSnapshot, DupsPerSnapshot, NumSnapshots).

-spec num_items_with_dups(pos_integer(), pos_integer(), pos_integer(),
                          pos_integer()) -> pos_integer().
num_items_with_dups(CurrentNum, ItemsPerSnapshot, DupsPerSnapshot,
        NumSnapshots) ->
    NewNumSnapshots = couch_dcp_fake_server:ceil_div(
        CurrentNum, ItemsPerSnapshot),
    case NewNumSnapshots =:= NumSnapshots of
    true ->
        CurrentNum;
    false ->
        NewNum = CurrentNum +
            (NewNumSnapshots - NumSnapshots) * DupsPerSnapshot,
        num_items_with_dups(
            NewNum, ItemsPerSnapshot, DupsPerSnapshot, NewNumSnapshots)
    end.

-spec list_partitions(binary()) -> [partition_id()].
list_partitions(SetName) ->
    FilePaths = couch_server:all_known_databases_with_prefix(SetName),
    Parts = lists:foldl(fun(P, Acc) ->
        File = lists:last(binary:split(P, <<"/">>)),
        case File of
        <<"master">> ->
            Acc;
        BinPartId ->
            PartId = list_to_integer(binary_to_list(BinPartId)),
            [PartId | Acc]
        end
    end, [], FilePaths),
    lists:sort(Parts).

-spec send_vbucket_seqnos_stats(#state{}, binary(),
        socket(), request_id(), [partition_id()]) -> ok.
send_vbucket_seqnos_stats(State, SetName, Socket, RequestId, Partitions) ->
    #state{
        setname = SetName,
        items_per_snapshot = ItemsPerSnapshot,
        dups_per_snapshot = DupsPerSnapshot
    } = State,
    Result = lists:map(fun(PartId) ->
        BinPartId = list_to_binary(integer_to_list(PartId)),
        case get_sequence_number(SetName, PartId, ItemsPerSnapshot,
            DupsPerSnapshot) of
        {ok, Seq} ->
            SeqKey = <<"vb_", BinPartId/binary ,":high_seqno">>,
            SeqValue = list_to_binary(integer_to_list(Seq)),
            SeqStat = couch_dcp_producer:encode_stat(
                RequestId, SeqKey, SeqValue),
            ok = gen_tcp:send(Socket, SeqStat),

            UuidKey = <<"vb_", BinPartId/binary ,":vb_uuid">>,
            FailoverLog = get_failover_log(PartId, State),
            {UuidValue, _} = hd(FailoverLog),
            UuidStat = couch_dcp_producer:encode_stat(
                RequestId, UuidKey, <<UuidValue:64/integer>>),
            ok = gen_tcp:send(Socket, UuidStat),
            true;
        {error, not_my_partition} ->
            % TODO sarath 2014-07-15: Fix get_stats API for single partition
            % The real response contains the vBucket map so that
            % clients can adapt. It's not easy to simulate, hence
            % we return an empty JSON object to keep things simple.
            %StatError = couch_dcp_producer:encode_stat_error(
            %    RequestId, ?DCP_STATUS_NOT_MY_VBUCKET,
            %    <<"{}">>),
            %ok = gen_tcp:send(Socket, StatError),
            true
        end
    end, Partitions),
    case lists:all(fun(E) -> E end, Result) of
    true ->
        EndStat = couch_dcp_producer:encode_stat(RequestId, <<>>, <<>>),
        ok = gen_tcp:send(Socket, EndStat);
    false ->
        ok
    end.
