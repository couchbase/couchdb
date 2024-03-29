#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable

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

default_config() ->
    test_util:build_file("etc/couchdb/default_dev.ini").

main(_) ->
    test_util:init_code_path(),
    etap:plan(5),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.

test() ->
    couch_system_event:start_link(),
    couch_config:start_link([default_config()]),

    etap:is(
        couch_config:get("httpd", "port"),
        "5984",
        "{httpd, port} is 5984 by default."
    ),

    ok = couch_config:set("httpd", "port", "4895", false),

    etap:is(
        couch_config:get("httpd", "port"),
        "4895",
        "{httpd, port} changed to 4895"
    ),

    SentinelFunc = fun() ->
        % Ping/Pong to make sure we wait for this
        % process to die
        receive {ping, From} -> From ! pong end
    end,
    SentinelPid = spawn(SentinelFunc),

    couch_config:register(
        fun("httpd", "port", Value) ->
            etap:is(Value, "8080", "Registered function got notification.")
        end,
        SentinelPid
    ),

    ok = couch_config:set("httpd", "port", "8080", false),

    {Field, Addr, AddrAny} = case misc:is_ipv6() of
                true -> {"ip6_bind_address", "::1", "::0"};
                false -> {"ip4_bind_address", "127.0.0.1", "0.0.0.0"}
            end,


    % Implicitly checking that we *don't* call the function
    etap:is(
        couch_config:get("httpd", Field),
        Addr,
        "{httpd, " ++ Field ++ "} is not " ++ AddrAny
    ),
    ok = couch_config:set("httpd", Field, AddrAny, false),

    % Ping-Pong kill process
    SentinelPid ! {ping, self()},
    receive
        _Any -> ok
    after 1000 ->
        throw({timeout_error, registered_pid})
    end,

    ok = couch_config:set("httpd", "port", "80", false),
    etap:is(
        couch_config:get("httpd", "port"),
        "80",
        "Implicitly test that the function got de-registered"
    ),

    % test passing of Persist flag
    couch_config:register(
        fun("httpd", _, _, Persist) ->
            etap:is(Persist, false)
        end),
    ok = couch_config:set("httpd", "port", "80", false),

    ok.
