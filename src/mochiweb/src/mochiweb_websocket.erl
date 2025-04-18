-module(mochiweb_websocket).

-author('lukasz.lalik@zadane.pl').

%% The MIT License (MIT)

%% Copyright (c) 2012 Zadane.pl sp. z o.o.

%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:

%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.

%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
%% THE SOFTWARE.

%% @doc Websockets module for Mochiweb. Based on Misultin websockets module.

-export([loop/5, request/6, upgrade_connection/2]).

-export([send/3, send/4]).

-ifdef(TEST).

-export([hixie_handshake/7, make_handshake/1,
         parse_hixie_frames/2, parse_hybi_frames/4]).

-endif.

loop(Socket, Body, State, WsVersion, ReplyChannel) ->
    loop(Socket, Body, State, WsVersion, ReplyChannel, fin).

loop(Socket, Body, State, WsVersion, ReplyChannel, FragState) ->
    ok =
	mochiweb_socket:exit_if_closed(mochiweb_socket:setopts(Socket,
							       [{packet, 0},
								{active,
								 once}])),
    proc_lib:hibernate(?MODULE, request,
		       [Socket, Body, State, WsVersion, ReplyChannel, FragState]).

request(Socket, Body, State, WsVersion, ReplyChannel, FragState) ->
    receive
      {tcp_closed, _} ->
	  mochiweb_socket:close(Socket), exit(normal);
      {ssl_closed, _} ->
	  mochiweb_socket:close(Socket), exit(normal);
      {tcp_error, _, _} ->
	  mochiweb_socket:close(Socket), exit(normal);
      {Proto, _, Packet}
	  when Proto =:= tcp orelse Proto =:= ssl ->
	    handle_packet(Socket, Body, State, WsVersion, ReplyChannel, FragState,
                      Packet);
      _ -> mochiweb_socket:close(Socket), exit(normal)
    end.

%% Handle a single packet. Parses any frames out of the packet, and calls the
%% body with any complete frames. Finally, re-enters the loop, or waits for
%% continuation of a part-frame.
handle_packet(Socket, Body, State, WsVersion, ReplyChannel, FragState,
              Packet) ->
    case parse_frames(WsVersion, Packet, FragState, Socket) of
        {close, _} -> mochiweb_socket:close(Socket), exit(normal);
        error -> mochiweb_socket:close(Socket), exit(normal);
        {error, _, _} = E -> mochiweb_socket:close(Socket), exit(E);
        {Payload, WsState} ->
            NewState = call_body(Body, Payload, State,
                         ReplyChannel),
            case WsState of
                {<<>>, fin} ->
                    loop(Socket, Body, NewState, WsVersion, ReplyChannel, fin);
                {PartFrame, NewFragState} ->
                    handle_continuation(Socket, Body, NewState, WsVersion,
                                        ReplyChannel, PartFrame, NewFragState)
            end
    end.

%% Handle the continuation of a part-frame. Assumes that more of the frame (or
%% control frames) will be received within 5s.
handle_continuation(Socket, Body, State, WsVersion, ReplyChannel, PartFrame,
                    FragState) ->
    ok = mochiweb_socket:exit_if_closed(
           mochiweb_socket:setopts(
             Socket, [{packet, 0}, {active, once}])),
    receive
        {tcp_closed, _} ->
            mochiweb_socket:close(Socket), exit(normal);
        {ssl_closed, _} ->
            mochiweb_socket:close(Socket), exit(normal);
        {tcp_error, _, _} ->
            mochiweb_socket:close(Socket), exit(normal);
        {Proto, _, Continuation}
          when Proto =:= tcp orelse Proto =:= ssl ->
            handle_packet(Socket, Body, State, WsVersion, ReplyChannel,
                          FragState, <<PartFrame/binary, Continuation/binary>>);
        _ -> mochiweb_socket:close(Socket), exit(normal)
    after 5000 ->
            mochiweb_socket:close(Socket), exit(timeout)
    end.

call_body({M, F, A}, Payload, State, ReplyChannel) ->
    erlang:apply(M, F, [Payload, State, ReplyChannel | A]);
call_body({M, F}, Payload, State, ReplyChannel) ->
    M:F(Payload, State, ReplyChannel);
call_body(Body, Payload, State, ReplyChannel) ->
    Body(Payload, State, ReplyChannel).

send(Socket, Payload, hybi) ->
    Opcode = 1,  %% text
    send(Socket, Opcode, Payload, hybi);
send(Socket, Payload, hixie) ->
    mochiweb_socket:send(Socket, [0, Payload, 255]).

send(Socket, Opcode, Payload, hybi) ->
    Fin = 1,  %% No further frames
    Rsv = 0,  %% No use of reserved bits
    PayloadSize = payload_length(size(Payload)),
    Prefix = <<Fin:1, Rsv:3, Opcode:4, PayloadSize/binary>>,
    mochiweb_socket:send(Socket, [Prefix, Payload]).

upgrade_connection({ReqM, _} = Req, Body) ->
    case make_handshake(Req) of
      {Version, Response} ->
	  Socket = ReqM:get(socket, Req),
	  ReplyChannel = fun ({Opcode, Payload}) ->
                            (?MODULE):send(Socket, Opcode, Payload, Version);
                         (Payload) ->
				            (?MODULE):send(Socket, Payload, Version)
			         end,
	  Reentry = fun (State) ->
			    (?MODULE):loop(Socket, Body, State, Version,
					   ReplyChannel)
		    end,
	  {Response, Reentry, ReplyChannel};
      _ ->
	  mochiweb_socket:close(ReqM:get(socket, Req)),
	  exit(normal)
    end.

make_handshake({ReqM, _} = Req) ->
    SecKey = ReqM:get_header_value("sec-websocket-key",
				   Req),
    Sec1Key = ReqM:get_header_value("Sec-WebSocket-Key1",
				    Req),
    Sec2Key = ReqM:get_header_value("Sec-WebSocket-Key2",
				    Req),
    Origin = ReqM:get_header_value(origin, Req),
    if SecKey =/= undefined -> hybi_handshake(SecKey);
       Sec1Key =/= undefined andalso Sec2Key =/= undefined ->
	   Host = ReqM:get_header_value("Host", Req),
	   Path = ReqM:get(path, Req),
	   Body = ReqM:recv(8, Req),
	   Scheme = scheme(Req),
	   hixie_handshake(Scheme, Host, Path, Sec1Key, Sec2Key,
			   Body, Origin);
       true -> error
    end.

hybi_handshake(SecKey) ->
    BinKey = list_to_binary(SecKey),
    Bin = <<BinKey/binary,
	    "258EAFA5-E914-47DA-95CA-C5AB0DC85B11">>,
    Challenge = base64:encode(crypto:hash(sha, Bin)),
    Response = {101,
		[{"Connection", "Upgrade"}, {"Upgrade", "websocket"},
		 {"Sec-Websocket-Accept", Challenge}],
		""},
    {hybi, Response}.

scheme(Req) ->
    case mochiweb_request:get(scheme, Req) of
      http -> "ws://";
      https -> "wss://"
    end.

hixie_handshake(Scheme, Host, Path, Key1, Key2, Body,
		Origin) ->
    Ikey1 = [D || D <- Key1, $0 =< D, D =< $9],
    Ikey2 = [D || D <- Key2, $0 =< D, D =< $9],
    Blank1 = length([D || D <- Key1, D =:= 32]),
    Blank2 = length([D || D <- Key2, D =:= 32]),
    Part1 = erlang:list_to_integer(Ikey1) div Blank1,
    Part2 = erlang:list_to_integer(Ikey2) div Blank2,
    Ckey = <<Part1:4/big-unsigned-integer-unit:8,
	     Part2:4/big-unsigned-integer-unit:8, Body/binary>>,
    Challenge = erlang:md5(Ckey),
    Location = lists:concat([Scheme, Host, Path]),
    Response = {101,
		[{"Upgrade", "WebSocket"}, {"Connection", "Upgrade"},
		 {"Sec-WebSocket-Origin", Origin},
		 {"Sec-WebSocket-Location", Location}],
		Challenge},
    {hixie, Response}.

parse_frames(hybi, Frames, FragState, Socket) ->
    try parse_hybi_frames(Socket, Frames, FragState, []) of
        {Parsed, WsState} -> {process_frames(Parsed, []), WsState}
    catch
        T:E -> {error, T, E}
    end;
parse_frames(hixie, Frames, _FragState, _Socket) ->
    try parse_hixie_frames(Frames, []) of
        Payload -> {Payload, {<<>>, fin}}
    catch
        _:_ -> error
    end.

-define(is_control_opcode(Opcode), Opcode =:= 16#8; Opcode =:= 16#9;
                                   Opcode =:= 16#A; Opcode =:= 16#B;
                                   Opcode =:= 16#C; Opcode =:= 16#D;
                                   Opcode =:= 16#E; Opcode =:= 16#F).

%%
%% Websockets internal functions for RFC6455 and hybi draft
%%
process_frames([], Acc) -> lists:reverse(Acc);
process_frames([{16#8, _Payload} | _Rest], _Acc) ->
    close;
process_frames([{Opcode, Payload} | Rest], Acc) ->
    ProcessedFrame =
        case Opcode of
            16#1 -> {text, Payload};
            16#2 -> {binary, Payload};
            16#9 -> ping;
            16#10 -> pong;
            _ -> {other, Payload}
        end,
    process_frames(Rest, [ProcessedFrame | Acc]).

parse_hybi_frames(_S, <<>>, {frag, _, _} = FragState, Acc) ->
    %% Last frame was not finished, so we expected another frame, but we return
    %% the Acc so that the websocket body can process any complete messages
    {Acc, {<<>>, FragState}};
parse_hybi_frames(_, <<>>, fin, Acc) -> {lists:reverse(Acc), {<<>>, fin}};
parse_hybi_frames(S,
                  <<Fin:1, _Rsv:3, Opcode:4, _Mask:1, PayloadLen:7,
                    MaskKey:4/binary, Payload:PayloadLen/binary-unit:8,
                    Rest/binary>>,
                  FragState, Acc)
    when PayloadLen < 126 ->
    {NewFragState, NewAcc} = parse_hybi_frame(Fin, Opcode, Payload, MaskKey,
                                            FragState, Acc),
    parse_hybi_frames(S, Rest, NewFragState, NewAcc);
parse_hybi_frames(S,
                  <<Fin:1, _Rsv:3, Opcode:4, _Mask:1, 126:7,
                    PayloadLen:16, MaskKey:4/binary,
                    Payload:PayloadLen/binary-unit:8, Rest/binary>>,
                  FragState, Acc) ->
    {NewFragState, NewAcc} = parse_hybi_frame(Fin, Opcode, Payload, MaskKey,
                                              FragState, Acc),
    parse_hybi_frames(S, Rest, NewFragState, NewAcc);
parse_hybi_frames(_Socket,
                  <<_Fin:1, _Rsv:3, _Opcode:4, _Mask:1, 126:7,
                    _PayloadLen:16, _MaskKey:4/binary, _/binary-unit:8>> =
                      PartFrame,
                  FragState, Acc) ->
    {Acc, {PartFrame, FragState}};
parse_hybi_frames(S,
                  <<Fin:1, _Rsv:3, Opcode:4, _Mask:1, 127:7, 0:1,
                    PayloadLen:63, MaskKey:4/binary,
                    Payload:PayloadLen/binary-unit:8, Rest/binary>>,
                  FragState, Acc) ->
    {NewFragState, NewAcc} = parse_hybi_frame(Fin, Opcode, Payload, MaskKey,
                                              FragState, Acc),
    parse_hybi_frames(S, Rest, NewFragState, NewAcc);
parse_hybi_frames(_Socket,
                  <<_Fin:1, _Rsv:3, _Opcode:4, _Mask:1, 127:7, 0:1,
                    _PayloadLen:63, _MaskKey:4/binary, _/binary-unit:8>> =
                      PartFrame,
                  FragState, Acc) ->
    {Acc, {PartFrame, FragState}};
parse_hybi_frames(_Socket, _Frame, _FragState, _Acc) ->
    throw(invalid_frame).

%% Unmask payload and merge with any frame fragment from the previous frame
parse_hybi_frame(0, Opcode, Payload, MaskKey, fin, Acc) when Opcode =/= 0 ->
    Frag = hybi_unmask(Payload, MaskKey, <<>>),
    {{frag, Opcode, Frag}, Acc};
parse_hybi_frame(0, 0, Payload, MaskKey, {frag, Opcode, Frag0}, Acc) ->
    Frag1 = hybi_unmask(Payload, MaskKey, <<>>),
    Frag2 = <<Frag0/binary, Frag1/binary>>,
    {{frag, Opcode, Frag2}, Acc};
parse_hybi_frame(1, Opcode, Payload, MaskKey, fin, Acc) when Opcode =/= 0 ->
    Payload2 = hybi_unmask(Payload, MaskKey, <<>>),
    {fin, [{Opcode, Payload2} | Acc]};
parse_hybi_frame(1, 0, Payload, MaskKey, {frag, Opcode, Frag0}, Acc) ->
    Frag1 = hybi_unmask(Payload, MaskKey, <<>>),
    Payload2 = <<Frag0/binary, Frag1/binary>>,
    {fin, [{Opcode, Payload2} | Acc]};
parse_hybi_frame(1, Opcode, Payload, MaskKey, {frag, _, _} = Frag, Acc)
    when ?is_control_opcode(Opcode) ->
    %% Control opcodes jump ahead of any unfinished fragmented message frames
    Payload2 = hybi_unmask(Payload, MaskKey, <<>>),
    {Frag, [{Opcode, Payload2} | Acc]};
parse_hybi_frame(_Fin, 0, _Payload, _MaskKey, fin, _Acc) ->
    throw(invalid_continuation_frame).

%% Unmasks RFC 6455 message
hybi_unmask(<<O:32, Rest/bits>>, MaskKey, Acc) ->
    <<MaskKey2:32>> = MaskKey,
    hybi_unmask(Rest, MaskKey,
		<<Acc/binary, (O bxor MaskKey2):32>>);
hybi_unmask(<<O:24>>, MaskKey, Acc) ->
    <<MaskKey2:24, _:8>> = MaskKey,
    <<Acc/binary, (O bxor MaskKey2):24>>;
hybi_unmask(<<O:16>>, MaskKey, Acc) ->
    <<MaskKey2:16, _:16>> = MaskKey,
    <<Acc/binary, (O bxor MaskKey2):16>>;
hybi_unmask(<<O:8>>, MaskKey, Acc) ->
    <<MaskKey2:8, _:24>> = MaskKey,
    <<Acc/binary, (O bxor MaskKey2):8>>;
hybi_unmask(<<>>, _MaskKey, Acc) -> Acc.

payload_length(N) ->
    case N of
      N when N =< 125 -> <<N>>;
      N when N =< 65535 -> <<126, N:16>>;
      N when N =< 9223372036854775807 -> <<127, N:64>>
    end.

%%
%% Websockets internal functions for hixie-76 websocket version
%%
parse_hixie_frames(<<>>, Frames) ->
    lists:reverse(Frames);
parse_hixie_frames(<<0, T/binary>>, Frames) ->
    {Frame, Rest} = parse_hixie(T, <<>>),
    parse_hixie_frames(Rest, [Frame | Frames]).

parse_hixie(<<255, Rest/binary>>, Buffer) ->
    {Buffer, Rest};
parse_hixie(<<H, T/binary>>, Buffer) ->
    parse_hixie(T, <<Buffer/binary, H>>).
