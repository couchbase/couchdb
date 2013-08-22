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
-export([start/2]).
% This will be implemented as ep-engine stat
-export([get_sequence_number/1]).

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
    socket = nil,
    setname = nil
}).


% Public API

start(Port, SetName) ->
    %spawn(?MODULE, server, [Port, SetName]).
    gen_server:start({local, ?MODULE}, ?MODULE, [Port, SetName], []).

% Returns the current high sequence number of a partition
get_sequence_number(PartId) ->
    gen_server:call(?MODULE, {get_sequence_number, PartId}).


% gen_server callbacks

init([Port, SetName]) ->
    {ok, Listen} = gen_tcp:listen(Port,
        [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
    accept(Listen),
    {ok, #state{
        streams = [],
        setname = SetName
    }}.


% Returns 'ok' when the response contains mutations, returns 'done'
% if there were no further mutations
handle_call({send_snapshot, PartId, EndSeq}, _From, State) ->
    #state{
        socket = Socket,
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

handle_call({set_socket, Socket}, _From, State) ->
    {reply, ok, State#state{
        socket = Socket
    }};

handle_call({get_sequence_number, PartId}, _From, State) ->
    Db = open_db(State#state.setname, PartId),
    Seq = Db#db.update_seq,
    couch_db:close(Db),
    {reply, Seq, State}.


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
    ok = gen_server:call(?MODULE, {set_socket, Socket}),
    % Let the server spawn a new process and replace this loop
    % with the read loop, to avoid blocking
    accept(Listen),
    read(Socket).


read(Socket) ->
    case gen_tcp:recv(Socket, 0) of
    {ok, Bin} ->
        case parse_request(Bin) of
        {stream_request, {PartId, RequestId, StartSeq, EndSeq, _PartUuid,
                          _HighSeq, _GroupId}} ->
            Reply = encode_stream_response_ok(PartId, RequestId),
            ok = gen_tcp:send(Socket, Reply),
            ok = gen_server:call(?MODULE, {add_stream, PartId, RequestId, StartSeq}),
            gen_server:call(?MODULE, {send_snapshot, PartId, EndSeq}),
            read(Socket);
        _ ->
            Reply = {error, "Can't parse request"},
            ok = gen_tcp:send(Socket, Reply)
        end;
    {error, closed} ->
        ok
    end.


parse_request(<<?UPR_WIRE_MAGIC_REQUEST,
                ?UPR_WIRE_OPCODE_REQUEST,
                _KeyLength:(?UPR_WIRE_SIZES_KEY_LENGTH*8),
                _ExtraLength,
                ?UPR_WIRE_REQUEST_TYPE_STREAM,
                PartId:(?UPR_WIRE_SIZES_PARTITION*8),
                _BodyLength:(?UPR_WIRE_SIZES_BODY*8),
                RequestId:(?UPR_WIRE_SIZES_REQUEST_ID*8),
                _Cas:(?UPR_WIRE_SIZES_CAS*8),
                StartSeq:(?UPR_WIRE_SIZES_BY_SEQ*8),
                EndSeq:(?UPR_WIRE_SIZES_BY_SEQ*8),
                PartUuid:(?UPR_WIRE_SIZES_PARTITION_UUID*8),
                HighSeq:(?UPR_WIRE_SIZES_BY_SEQ*8),
                GroupId/binary>>) ->
    {stream_request, {PartId, RequestId, StartSeq, EndSeq, PartUuid, HighSeq,
                      GroupId}}.


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
    Start = encode_snapshot_start(PartId, RequestId),
    ok = gen_tcp:send(Socket, Start),
    Mutations = create_mutations(SetName, PartId, StartSeq, EndSeq),
    lists:foreach(fun({Cas, Seq, RevSeq, Flags, Expiration, LockTime, Key, Value}) ->
        Encoded = encode_snapshot_mutation(PartId, RequestId, Cas, Seq,
            RevSeq, Flags, Expiration, LockTime, Key, Value),
        ok = gen_tcp:send(Socket, Encoded)
    end, Mutations),
    End = encode_snapshot_end(PartId, RequestId),
    ok = gen_tcp:send(Socket, End),
    length(Mutations).


%Field        (offset) (value)
%Magic          (0)     : 0x81                 (Response)
%Opcode         (1)     : 0x04                 (UPR Stream Message)
%Key Length     (2-3)   : 0x0000
%Extra Length   (4)     : 0x00
%Request Type   (5)     : 0x01                 (Snapshot Start)
%VBucket        (6-7)   : 0x000C               (12)
%Total Body     (8-11)  : 0x00000000
%Request ID     (12-15) : 0x0000002D           (45)
%Cas            (16-23) : 0x0000000000000000
encode_snapshot_start(PartId, RequestId) ->
    <<?UPR_WIRE_MAGIC_RESPONSE,
      ?UPR_WIRE_OPCODE_STREAM_MESSAGE,
      0:(?UPR_WIRE_SIZES_KEY_LENGTH*8),
      0,
      ?UPR_WIRE_REQUEST_TYPE_SNAPSHOT_START,
      PartId:(?UPR_WIRE_SIZES_PARTITION*8),
      0:(?UPR_WIRE_SIZES_BODY*8),
      RequestId:(?UPR_WIRE_SIZES_REQUEST_ID*8),
      0:(?UPR_WIRE_SIZES_CAS*8)>>.

%Field        (offset) (value)
%Magic          (0)     : 0x81                 (Response)
%Opcode         (1)     : 0x04                 (UPR Stream Message)
%Key Length     (2-3)   : 0x0000
%Extra Length   (4)     : 0x00
%Request Type   (5)     : 0x02                 (Snapshot End)
%VBucket        (6-7)   : 0x000C               (12)
%Total Body     (8-11)  : 0x00000000
%Request ID     (12-15) : 0x0000002D           (45)
%Cas            (16-23) : 0x0000000000000000
encode_snapshot_end(PartId, RequestId) ->
    <<?UPR_WIRE_MAGIC_RESPONSE,
      ?UPR_WIRE_OPCODE_STREAM_MESSAGE,
      0:(?UPR_WIRE_SIZES_KEY_LENGTH*8),
      0,
      ?UPR_WIRE_REQUEST_TYPE_SNAPSHOT_END,
      PartId:(?UPR_WIRE_SIZES_PARTITION*8),
      0:(?UPR_WIRE_SIZES_BODY*8),
      RequestId:(?UPR_WIRE_SIZES_REQUEST_ID*8),
      0:(?UPR_WIRE_SIZES_CAS*8)>>.

%Field        (offset) (value)
%Magic          (0)     : 0x81                 (Response)
%Opcode         (1)     : 0x04                 (UPR Stream Message)
%Key Length     (2-3)   : 0x0005               (5)
%Extra Length   (4)     : 0x24                 (36)
%Request Type   (5)     : 0x03                 (UPR Mutation)
%VBucket        (6-7)   : 0x000C               (12)
%Total Body     (8-11)  : 0x00000030           (48)
%Request ID     (12-15) : 0x0000002D           (45)
%Cas            (16-23) : 0x00000000000000A5   (165)
%By Seqno       (24-31) : 0x00000000000008CB   (2251)
%Rev Seqno      (32-39) : 0x0000000000000003   (3)
%Item Flags     (40-43) : 0x00000000           (0)
%Item Exp       (44-47) : 0x00000000           (0)
%Lock time      (48-51) : 0x00000000           (0)
%Key            (52-56) : "mykey"
%Value          (57-63) : "myvalue"
% It also encodes deletions
encode_snapshot_mutation(PartId, RequestId, Cas, Seq, RevSeq, Flags,
                         ExpireTime, LockTime, Key, Value) ->
    Body0 = <<Seq:(?UPR_WIRE_SIZES_BY_SEQ*8),
             RevSeq:(?UPR_WIRE_SIZES_REV_SEQ*8),
             Flags:(?UPR_WIRE_SIZES_FLAGS*8),
             ExpireTime:(?UPR_WIRE_SIZES_EXPIRY*8),
             LockTime:(?UPR_WIRE_SIZES_LOCK*8),
             Key/binary>>,
             %Key:KeyLength/binary>>

    case Value of
    deleted ->
        Body = Body0,
        ValueLength = 0,
        RequestType = ?UPR_WIRE_REQUEST_TYPE_DELETION;
    _ ->
        Body = <<Body0/binary, Value/binary>>,
        ValueLength = byte_size(Value),
        RequestType = ?UPR_WIRE_REQUEST_TYPE_MUTATION
    end,

    KeyLength = byte_size(Key),
    BodyLength = byte_size(Body),
    % XXX vmx 2013-08-19: Still only 80% sure that ExtraLength has the correct
    %    value
    ExtraLength = BodyLength - KeyLength - ValueLength,

    Header = <<?UPR_WIRE_MAGIC_RESPONSE,
               ?UPR_WIRE_OPCODE_STREAM_MESSAGE,
               KeyLength:(?UPR_WIRE_SIZES_KEY_LENGTH*8),
               ExtraLength,
               RequestType,
               PartId:(?UPR_WIRE_SIZES_PARTITION*8),
               BodyLength:(?UPR_WIRE_SIZES_BODY*8),
               RequestId:(?UPR_WIRE_SIZES_REQUEST_ID*8),
               Cas:(?UPR_WIRE_SIZES_CAS*8)>>,
    <<Header/binary, Body/binary>>.

%Field        (offset) (value)
%Magic          (0)     : 0x81                 (Response)
%Opcode         (1)     : 0x02                 (UPR Response)
%Key Length     (2-3)   : 0x0000
%Extra Length   (4)     : 0x00
%Request Type   (5)     : 0x01                 (OK)
%VBucket        (6-7)   : 0x000C               (12)
%Total Body     (8-11)  : 0x00000000
%Request ID     (12-15) : 0x0000002D           (45)
%Cas            (16-23) : 0x0000000000000000
encode_stream_response_ok(PartId, RequestId) ->
    <<?UPR_WIRE_MAGIC_RESPONSE,
      ?UPR_WIRE_OPCODE_RESPONSE,
      0:(?UPR_WIRE_SIZES_KEY_LENGTH*8),
      0,
      ?UPR_WIRE_REQUEST_TYPE_OK,
      PartId:(?UPR_WIRE_SIZES_PARTITION*8),
      0:(?UPR_WIRE_SIZES_BODY*8),
      RequestId:(?UPR_WIRE_SIZES_REQUEST_ID*8),
            0:(?UPR_WIRE_SIZES_CAS*8)>>.


open_db(SetName, PartId) ->
    case couch_db:open_int(?dbname(SetName, PartId), []) of
    {ok, PartDb} ->
        PartDb;
    Error ->
        ErrorMsg = io_lib:format("UPR error opening database `~s': ~w",
                                 [?dbname(SetName, PartId), Error]),
        throw({error, iolist_to_binary(ErrorMsg)})
    end.
