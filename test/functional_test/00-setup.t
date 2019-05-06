#!/usr/bin/env escript
%% @author Couchbase <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
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

main(_) ->
    %% test depends on port 9000 to port 9003 running
    %% check if all ports are running or not
    %% it will give econnrefused error when cluster_run is not running
    case cluster_ops:check_cluster_ports() of
    ok ->
        etap:plan(1),
        case (catch test()) of
            ok ->
                etap:end_tests();
            Other ->
                etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
                etap:bail(Other)
            end;
    Reason ->
        etap:plan({skip, Reason})
    end.

test() ->
    Ok = cluster_ops:init_setup(),
    etap:is_ok(Ok, "Cluster setup done").
