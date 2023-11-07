%% @author Couchbase <info@couchbase.com>
%% @copyright 2019 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc methods for interaction with memcached
%%

-module(memcached_calls).

-export([connect_memcached/1, send/2, recv/3, audit_put/3]).

-include("couch_db.hrl").
-define(MAGIC_REQUEST, 16#80).
-define(AUTH, 16#21).
-define(AUDIT_PUT, 16#27).
-define(RES_MAGIC, 16#81).
-define(HEADERLEN, 24).

encode_request(Opcode, Ext, Key, Body) ->
    ExtLength = byte_size(Ext),
    KeyLength = byte_size(Key),
    BodyLength = ExtLength + KeyLength + byte_size(Body),
    <<?MAGIC_REQUEST:8, Opcode:8, KeyLength:16, ExtLength:8, 0:8,
       0:16, BodyLength:32, 0:32, 0:64,
       Ext/binary, Key/binary, Body/binary>>.

send(Socket, Data) ->
    gen_tcp:send(Socket, Data).

recv(Socket, NumBytes, Timeout) ->
    case gen_tcp:recv(Socket, NumBytes, Timeout) of
        {ok, HeaderBin} ->
            {ExtLen, KeyLen, BodyLen} = decode_header(HeaderBin),
            recv_body(Socket, ExtLen, KeyLen, BodyLen, Timeout);
        Err -> Err
    end.

recv_body(Socket, ExtLen, KeyLen, BodyLen, Timeout) ->
    case BodyLen > 0 of
        true ->
            true = BodyLen >= (ExtLen + KeyLen),
            {ok, Ext} = recv_data(Socket, ExtLen, Timeout),
            {ok, Key} = recv_data(Socket, KeyLen, Timeout),
            {ok, Data} = recv_data(Socket,
                                   erlang:max(0, BodyLen - (ExtLen + KeyLen)),
                                   Timeout),
            {ok, Ext, Key, Data};
        false ->
            {ok, <<>>, <<>>, <<>>}
    end.

recv_data(_, 0, _) -> {ok, <<>>};
recv_data(Socket, NumBytes, Timeout) -> prim_inet:recv(Socket, NumBytes, Timeout).

decode_header(<<?RES_MAGIC:8, _Opcode:8, KeyLen:16, ExtLen:8,
                     _DataType:8, _Status:16, BodyLen:32,
                     _Opaque:32, _CAS:64>>) ->
     {ExtLen, KeyLen, BodyLen}.

connect_memcached(Tries) ->
    MemcachedPort = list_to_integer(couch_config:get("dcp", "port", 12000)),
    AddrFamily = misc:get_net_family(),
    Addr = misc:localhost(AddrFamily, []),
    try
        {ok, SocketPid} = gen_tcp:connect(Addr, MemcachedPort,
                        [AddrFamily, binary, {packet, raw}, {active, false}, {nodelay, true},
                         {buffer, 4098}]),
        case auth(SocketPid) of
        <<>> ->
                SocketPid;
        Reason ->
                ?LOG_ERROR("Error in connecting to memcached: Reason ~p",[Reason]),
                ok = gen_tcp:close(SocketPid),
                no_socket
        end
    catch
        E:R ->
            case Tries of
            1 ->
                ?LOG_INFO("Unable to connect to memcahced: ~p.", [{E, R}]),
                no_socket;
            _ ->
                timer:sleep(500),
                connect_memcached(Tries - 1)
            end
     end.

audit_put(no_socket, _Opcode, _Data) ->
    {error, no_socket};
audit_put(Socket, AuditOpcode, Data) ->
    RequestBody = encode_audit_put(AuditOpcode, Data),
    case send(Socket, RequestBody) of
    ok ->
        case recv(Socket, ?HEADERLEN, infinity) of
        {ok, _Ext, _Key, Body} -> {ok, Body};
        Error ->
            gen_tcp:close(Socket),
            Error
        end;
    Error ->
        gen_tcp:close(Socket),
        Error
    end.

auth(Socket) ->
    {User, Pass} = get_auth(3),
    RequestData = encode_auth_request(User, Pass),
    ok = send(Socket, RequestData),
    case recv(Socket, ?HEADERLEN, infinity) of
    {ok, _Ext, _Key, Body} -> Body;
    Error -> Error
    end.

encode_auth_request(User, Password) ->
    User2 = iolist_to_binary(User),
    Password2 = iolist_to_binary(Password),
    Key = <<"PLAIN">>,
    Body = <<0:8, User2/binary, 0:8, Password2/binary>>,
    encode_request(?AUTH, <<>>, Key, Body).

encode_audit_put(AuditOpcode, Data) ->
    Body = ejson:encode({Data}),
    Ext = <<AuditOpcode:32>>,
    encode_request(?AUDIT_PUT, Ext, <<>>, Body).

get_auth(0) ->
    {"", ""};
get_auth(N) ->
    case cb_auth_info:get() of
    {auth, User, Passwd} ->
        {User, Passwd};
    {error, server_not_ready} ->
        ?LOG_ERROR("Retrying to obtain admin auth info from ns_server", []),
        timer:sleep(1000),
        get_auth(N-1)
    end.
