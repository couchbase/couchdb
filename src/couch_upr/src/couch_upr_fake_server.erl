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

-module(couch_upr_fake_server).
-behaviour(gen_server).

% Public API
-export([start/1]).
% This will be implemented as ep-engine stat
-export([get_sequence_number/1]).

% Only uses by tests
-export([set_failover_log/2]).

% Needed for internal process spawning
-export([accept/1, accept_loop/1]).

% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
    code_change/3]).

-include_lib("couch_upr/include/couch_upr.hrl").


-define(dbname(SetName, PartId),
    <<SetName/binary, $/, (list_to_binary(integer_to_list(PartId)))/binary>>).


% #doc_info{}, #doc{}, #db{} are from couch_db.hrl. They are copy & pasted here
% as they will go away once the proper UPR is in place.
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
    streams = [], %:: [{partition_id(), {request_id(), sequence_number()}}]
    setname = nil,
    failover_logs = dict:new()
}).


% Public API

start(SetName) ->
    % Start the fake UPR server where the original one is expected to be
    Port = list_to_integer(couch_config:get("upr", "port", "0")),
    gen_server:start({local, ?MODULE}, ?MODULE, [Port, SetName], []).

% Returns the current high sequence number of a partition
get_sequence_number(PartId) ->
    gen_server:call(?MODULE, {get_sequence_number, PartId}).

get_failover_log(PartId) ->
    gen_server:call(?MODULE, {get_failover_log, PartId}).

% Only used by tests to populate the failover log
set_failover_log(PartId, FailoverLog) ->
    gen_server:call(?MODULE, {set_failover_log, PartId, FailoverLog}).


% gen_server callbacks

init([Port, SetName]) ->
    {ok, Listen} = gen_tcp:listen(Port,
        [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    case Port of
    % In case the port was set to "0", the OS will decide which port to run
    % the fake UPR server on. Update the configuration so that we know which
    % port was chosen (that's only needed for the tests).
    0 ->
        {ok, RandomPort} = inet:port(Listen),
        couch_config:set("upr", "port", integer_to_list(RandomPort), false);
    _ ->
        ok
    end,
    accept(Listen),
    {ok, #state{
        streams = [],
        setname = SetName
    }}.


% Returns 'ok' when the response contains mutations, returns 'done'
% if there were no further mutations
handle_call({send_snapshot, Socket, PartId, EndSeq}, _From, State) ->
    #state{
        streams = Streams,
        setname = SetName
    } = State,
    {RequestId, Seq} = proplists:get_value(PartId, Streams),
    Num = do_send_snapshot(Socket, SetName, PartId, RequestId, Seq, EndSeq),
    case Num of
    0 ->
        {reply, done, State};
    _ ->
        Streams2 = lists:keyreplace(
            PartId, 1, Streams, {PartId, {RequestId, Seq+Num}}),
        {reply, ok, State#state{streams = Streams2}}
    end;

handle_call({add_stream, PartId, RequestId, StartSeq}, _From, State) ->
    {reply, ok, State#state{
        streams = [{PartId, {RequestId, StartSeq}}|State#state.streams]
    }};

handle_call({set_failover_log, PartId, FailoverLog}, _From, State) ->
    FailoverLogs = dict:store(PartId, FailoverLog, State#state.failover_logs),
    {reply, ok, State#state{
        failover_logs = FailoverLogs
    }};

handle_call({get_sequence_number, PartId}, _From, State) ->
    Db = open_db(State#state.setname, PartId),
    Seq = Db#db.update_seq,
    couch_db:close(Db),
    {reply, Seq, State};

handle_call({get_failover_log, PartId}, _From, State) ->
    case dict:find(PartId, State#state.failover_logs) of
    {ok, FailoverLog} ->
        ok;
    error ->
        FailoverLog = [{<<"initial0">>, 0}]
    end,
    {reply, FailoverLog, State}.


handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.


handle_info({'EXIT', _From, normal}, State)  ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Internal functions

accept(Listen) ->
    process_flag(trap_exit, true),
    spawn_link(?MODULE, accept_loop, [Listen]).

accept_loop(Listen) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    % Let the server spawn a new process and replace this loop
    % with the read loop, to avoid blocking
    accept(Listen),
    read(Socket).


read(Socket) ->
    case gen_tcp:recv(Socket, ?UPR_HEADER_LEN) of
    {ok, Header} ->
        case parse_header(Header) of
        {open_connection, BodyLength, RequestId} ->
            handle_open_connection_body(Socket, BodyLength, RequestId);
        {stream_request, BodyLength, RequestId, PartId} ->
            handle_stream_request_body(Socket, BodyLength, RequestId, PartId);
        {failover_log, RequestId, PartId} ->
            handle_failover_log(Socket, RequestId, PartId)
        end,
        read(Socket);
    {error, closed} ->
        ok
    end.

parse_header(<<?UPR_MAGIC_REQUEST,
               Opcode,
               _KeyLength:?UPR_SIZES_KEY_LENGTH,
               _ExtraLength,
               _DataType,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               _Cas:?UPR_SIZES_CAS>>) ->
    case Opcode of
    ?UPR_OPCODE_OPEN_CONNECTION ->
        {open_connection, BodyLength, RequestId};
    ?UPR_OPCODE_STREAM_REQUEST ->
        {stream_request, BodyLength, RequestId, PartId};
    ?UPR_OPCODE_FAILOVER_LOG_REQUEST ->
        {failover_log, RequestId, PartId}
    end.


handle_open_connection_body(Socket, BodyLength, RequestId) ->
    case gen_tcp:recv(Socket, BodyLength) of
    {ok, <<_SeqNo:?UPR_SIZES_SEQNO,
           ?UPR_FLAG_CONSUMER:?UPR_SIZES_FLAGS,
           _Name/binary>>} ->
        OpenConnection = encode_open_connection(RequestId),
        ok = gen_tcp:send(Socket, OpenConnection);
    {error, closed} ->
        io:format("vmx: closed6~n", [])
    end.

handle_stream_request_body(Socket, BodyLength, RequestId, PartId) ->
    case gen_tcp:recv(Socket, BodyLength) of
    {ok, <<_Flags:?UPR_SIZES_FLAGS,
           _Reserved:?UPR_SIZES_RESERVED,
           StartSeq:?UPR_SIZES_BY_SEQ,
           EndSeq:?UPR_SIZES_BY_SEQ,
           _PartUuid:?UPR_SIZES_PARTITION_UUID,
           _HighSeq:?UPR_SIZES_BY_SEQ>>} ->
        FailoverLog = get_failover_log(PartId),
        PartSeq = get_sequence_number(PartId),
        case StartSeq =< PartSeq of
        true ->
            StreamOk = encode_stream_request_ok(RequestId, FailoverLog),
            ok = gen_tcp:send(Socket, StreamOk),
            ok = gen_server:call(
                ?MODULE, {add_stream, PartId, RequestId, StartSeq}),

            gen_server:call(?MODULE, {send_snapshot, Socket, PartId, EndSeq}),

            StreamEnd = encode_stream_end(PartId, RequestId),
            ok = gen_tcp:send(Socket, StreamEnd);
        % requested sequence number is higher than the db contains
        % => rollback
        false ->
            StreamRollback = encode_stream_request_rollback(
                RequestId, PartSeq),
            ok = gen_tcp:send(Socket, StreamRollback)
        end
    end.

handle_failover_log(Socket, RequestId, PartId) ->
    FailoverLog = get_failover_log(PartId),
    FailoverLogResponse = encode_failover_log(RequestId, FailoverLog),
    ok = gen_tcp:send(Socket, FailoverLogResponse).


% This function creates mutations for one snapshot of one partition of a
% given size
create_mutations(SetName, PartId, StartSeq, EndSeq) ->
    Db = open_db(SetName, PartId),
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
extract_revision({RevSeq, <<>>}) ->
    {RevSeq, 0, 0, 0};
% https://github.com/couchbase/ep-engine/blob/master/src/couch-kvstore/couch-kvstore.cc#L212-L216
extract_revision({RevSeq, RevMeta}) ->
    <<Cas:64, Expiration:32, Flags:32>> = RevMeta,
    {RevSeq, Cas, Expiration, Flags}.


do_send_snapshot(Socket, SetName, PartId, RequestId, StartSeq, EndSeq) ->
    Mutations = create_mutations(SetName, PartId, StartSeq, EndSeq),
    lists:foreach(fun
        ({Cas, Seq, RevSeq, _Flags, _Expiration, _LockTime, Key, deleted}) ->
            Encoded = encode_snapshot_deletion(PartId, RequestId, Cas, Seq,
                RevSeq, Key),
            ok = gen_tcp:send(Socket, Encoded);
        ({Cas, Seq, RevSeq, Flags, Expiration, LockTime, Key, Value}) ->
            Encoded = encode_snapshot_mutation(PartId, RequestId, Cas, Seq,
                RevSeq, Flags, Expiration, LockTime, Key, Value),
            ok = gen_tcp:send(Socket, Encoded)
    end, Mutations),
    Marker = encode_snapshot_marker(PartId, RequestId),
    ok = gen_tcp:send(Socket, Marker),
    length(Mutations).


%UPR_OPEN response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x50
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x0000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0x00000001
%CAS          (16-23): 0x0000000000000000
encode_open_connection(RequestId) ->
    <<?UPR_MAGIC_RESPONSE,
      ?UPR_OPCODE_OPEN_CONNECTION,
      0:?UPR_SIZES_KEY_LENGTH,
      0,
      0,
      0:?UPR_SIZES_STATUS,
      0:?UPR_SIZES_BODY,
      RequestId:?UPR_SIZES_OPAQUE,
      0:?UPR_SIZES_CAS>>.

%UPR_SNAPSHOT_MARKER command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x56
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0xdeadbeef
%CAS          (16-23): 0x0000000000000000
encode_snapshot_marker(PartId, RequestId) ->
    <<?UPR_MAGIC_REQUEST,
      ?UPR_OPCODE_SNAPSHOT_MARKER,
      0:?UPR_SIZES_KEY_LENGTH,
      0,
      0,
      PartId:?UPR_SIZES_PARTITION,
      0:?UPR_SIZES_BODY,
      RequestId:?UPR_SIZES_OPAQUE,
      0:?UPR_SIZES_CAS>>.

%UPR_MUTATION command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x57
%Key length   (2,3)  : 0x0005
%Extra length (4)    : 0x1c
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000027
%Opaque       (12-15): 0xdeadbeef
%CAS          (16-23): 0x0000000000000001
%  by seqno   (24-31): 0x0000000000000000
%  rev seqno  (32-39): 0x0000000000000000
%  flags      (40-43): 0x00000000
%  expiration (44-47): 0x00000000
%  lock time  (48-51): 0x00000000
%  key        (52-56): hello
%  value      (57-62): world
encode_snapshot_mutation(PartId, RequestId, Cas, Seq, RevSeq, Flags,
                         Expiration, LockTime, Key, Value) ->
    Body = <<Seq:?UPR_SIZES_BY_SEQ,
             RevSeq:?UPR_SIZES_REV_SEQ,
             Flags:?UPR_SIZES_FLAGS,
             Expiration:?UPR_SIZES_EXPIRATION,
             LockTime:?UPR_SIZES_LOCK,
             Key/binary,
             Value/binary>>,

    KeyLength = byte_size(Key),
    ValueLength = byte_size(Value),
    BodyLength = byte_size(Body),
    % XXX vmx 2013-08-19: Still only 80% sure that ExtraLength has the correct
    %    value
    ExtraLength = BodyLength - KeyLength - ValueLength,

    Header = <<?UPR_MAGIC_REQUEST,
               ?UPR_OPCODE_MUTATION,
               KeyLength:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               Cas:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%UPR_DELETION command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x58
%Key length   (2,3)  : 0x0005
%Extra length (4)    : 0x10
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000015
%Opaque       (12-15): 0xdeadbeef
%CAS          (16-23): 0x0000000000000001
%  by seqno   (24-31): 0x0000000000000000
%  rev seqno  (32-39): 0x0000000000000000
%  key        (40-44): hello
encode_snapshot_deletion(PartId, RequestId, Cas, Seq, RevSeq, Key) ->
    Body = <<Seq:?UPR_SIZES_BY_SEQ,
              RevSeq:?UPR_SIZES_REV_SEQ,
              Key/binary>>,

    KeyLength = byte_size(Key),
    BodyLength = byte_size(Body),
    % XXX vmx 2013-08-19: Still only 80% sure that ExtraLength has the correct
    %    value
    ExtraLength = BodyLength - KeyLength,

    Header = <<?UPR_MAGIC_REQUEST,
               ?UPR_OPCODE_DELETION,
               KeyLength:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               Cas:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%UPR_STREAM_REQ response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x53
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x0000
%Total body   (8-11) : 0x00000000
%Opaque       (12-15): 0x00001000
%CAS          (16-23): 0x0000000000000000
encode_stream_request_ok(RequestId, FailoverLog) ->
    {BodyLength, Value} = failover_log_to_bin(FailoverLog),
    ExtraLength = 0,
    Header= <<?UPR_MAGIC_RESPONSE,
              ?UPR_OPCODE_STREAM_REQUEST,
              0:?UPR_SIZES_KEY_LENGTH,
              ExtraLength,
              0,
              ?UPR_STATUS_OK:?UPR_SIZES_STATUS,
              BodyLength:?UPR_SIZES_BODY,
              RequestId:?UPR_SIZES_OPAQUE,
              0:?UPR_SIZES_CAS>>,
    <<Header/binary, Value/binary>>.

%UPR_STREAM_REQ response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x53
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x08
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x0023 (Rollback)
%Total body   (8-11) : 0x00000008
%Opaque       (12-15): 0x00001000
%CAS          (16-23): 0x0000000000000000
%  rollback # (24-31): 0x0000000000000000
encode_stream_request_rollback(RequestId, Seq) ->
    <<?UPR_MAGIC_RESPONSE,
      ?UPR_OPCODE_STREAM_REQUEST,
      0:?UPR_SIZES_KEY_LENGTH,
      (?UPR_SIZES_BY_SEQ div 8),
      0,
      ?UPR_STATUS_ROLLBACK:?UPR_SIZES_STATUS,
      (?UPR_SIZES_BY_SEQ div 8):?UPR_SIZES_BODY,
      RequestId:?UPR_SIZES_OPAQUE,
      0:?UPR_SIZES_CAS,
      Seq:?UPR_SIZES_BY_SEQ>>.

%UPR_STREAM_END command
%Field        (offset) (value)
%Magic        (0)    : 0x80
%Opcode       (1)    : 0x55
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x04
%Data type    (5)    : 0x00
%Vbucket      (6,7)  : 0x0000
%Total body   (8-11) : 0x00000004
%Opaque       (12-15): 0xdeadbeef
%CAS          (16-23): 0x0000000000000000
%  flag       (24-27): 0x00000000 (OK)
encode_stream_end(PartId, RequestId) ->
    % XXX vmx 2013-09-11: For now we return only success
    Body = <<?UPR_FLAG_OK:?UPR_SIZES_FLAGS>>,
    BodyLength = byte_size(Body),
    % XXX vmx 2013-08-19: Still only 80% sure that ExtraLength has the correct
    %    value
    ExtraLength = BodyLength,
    Header = <<?UPR_MAGIC_REQUEST,
               ?UPR_OPCODE_STREAM_END,
               0:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               PartId:?UPR_SIZES_PARTITION,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               0:?UPR_SIZES_CAS>>,
    <<Header/binary, Body/binary>>.

%UPR_GET_FAILOVER_LOG response
%Field        (offset) (value)
%Magic        (0)    : 0x81
%Opcode       (1)    : 0x54
%Key length   (2,3)  : 0x0000
%Extra length (4)    : 0x00
%Data type    (5)    : 0x00
%Status       (6,7)  : 0x0000
%Total body   (8-11) : 0x00000040
%Opaque       (12-15): 0xdeadbeef
%CAS          (16-23): 0x0000000000000000
%  vb UUID    (24-31): 0x00000000feeddeca
%  vb seqno   (32-39): 0x0000000000005432
%  vb UUID    (40-47): 0x0000000000decafe
%  vb seqno   (48-55): 0x0000000001343214
%  vb UUID    (56-63): 0x00000000feedface
%  vb seqno   (64-71): 0x0000000000000004
%  vb UUID    (72-79): 0x00000000deadbeef
%  vb seqno   (80-87): 0x0000000000006524
encode_failover_log(RequestId, FailoverLog) ->
    {BodyLength, Value} = failover_log_to_bin(FailoverLog),
    ExtraLength = 0,
    Header = <<?UPR_MAGIC_RESPONSE,
               ?UPR_OPCODE_FAILOVER_LOG_REQUEST,
               0:?UPR_SIZES_KEY_LENGTH,
               ExtraLength,
               0,
               ?UPR_STATUS_OK:?UPR_SIZES_STATUS,
               BodyLength:?UPR_SIZES_BODY,
               RequestId:?UPR_SIZES_OPAQUE,
               0:?UPR_SIZES_CAS>>,
    <<Header/binary, Value/binary>>.

failover_log_to_bin(FailoverLog) ->
    FailoverLogBin = [[Uuid, <<Seq:64>>] || {Uuid, Seq} <- FailoverLog],
    Value = list_to_binary(FailoverLogBin),
    {byte_size(Value), Value}.


open_db(SetName, PartId) ->
    case couch_db:open_int(?dbname(SetName, PartId), []) of
    {ok, PartDb} ->
        PartDb;
    Error ->
        ErrorMsg = io_lib:format("UPR error opening database `~s': ~w",
                                 [?dbname(SetName, PartId), Error]),
        throw({error, iolist_to_binary(ErrorMsg)})
    end.
