#! /usr/bin/env escript

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

main(_) ->
    test_util:init_code_path(),
    etap:plan(6),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail()
    end,
    ok.


loop() ->
    receive
    close -> ok
    end.


test() ->
    couch_file_write_guard:sup_start_link(),
    Pid1 = spawn(fun() -> ok end),
    Pid2 = spawn(fun() -> loop() end),

    %% Added sleep timer in order to allow Pid1 to finish
    %% else is_process_alive(Pid1) will return true
    timer:sleep(1000),
    etap:is(is_process_alive(Pid1), false, "Process is not alive"),
    etap:is(couch_file_write_guard:add("index.1", Pid1),
        ok,
        "Added a file to non-existent pid mapping into write guard"),

    etap:is(couch_file_write_guard:add("index.1", Pid2),
        ok,
        "Added file to existing pid mapping into write guard"),

    etap:is(couch_file_write_guard:remove(Pid2),
        ok,
        "Removing ets table entry"),

    etap:is(couch_file_write_guard:add("index.1", Pid1),
        ok,
        "Added file"),

    etap:is(couch_file_write_guard:remove(Pid2),
        removing_unadded_file,
        "Removing unadded entry ets table entry"),

    Pid2 ! close,
    ok.
