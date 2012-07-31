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

thread_counts() ->
    [
     erlang:system_info(schedulers),
     erlang:system_info(schedulers) * 2,
     erlang:system_info(schedulers) * 5,
     erlang:system_info(schedulers) * 11
    ].

main(_) ->
    test_util:init_code_path(),

    etap:plan(length(expectations()) + lists:sum(thread_counts())),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.


test() ->
    couch_set_view_test_util:start_server(<<"set_view_raw_collator">>),

    etap:diag("Running single thread tests"),
    single_thread(),

    lists:foreach(fun(C) ->
        etap:diag("Running tests with " ++ integer_to_list(C) ++ " threads"),
        multiple_threads(C)
    end, thread_counts()),

    couch_set_view_test_util:stop_server(),
    ok.


single_thread() ->
    lists:foreach(fun({Key1, Key2, Expected}) ->
        etap:is(couch_ejson_compare:less_json(Key1, Key2),
                Expected,
                assertion_string(Key1, Key2, Expected))
    end, expectations()).


multiple_threads(NumThreads) ->
    Expectations = lists:flatten(
        lists:duplicate(8, expectations() ++ lists:reverse(expectations()))),
    ThreadFun = fun() ->
         lists:foreach(fun({Key1, Key2, Expected}) ->
             AssertionString = assertion_string(Key1, Key2, Expected),
             case (catch couch_ejson_compare:less_json(Key1, Key2)) == Expected of
             true ->
                 ok;
             false ->
                 exit({failed, AssertionString})
             end
         end, Expectations),
         exit(ok)
     end,
    Pids = lists:map(fun(_N) -> spawn_monitor(ThreadFun) end,
                     lists:seq(1, NumThreads)),
    lists:foreach(fun({Pid, Ref}) ->
        receive
        {'DOWN', Ref, process, Pid, Reason} ->
            etap:is(Reason, ok, "Thread finished successfully")
        after 60000 ->
            etap:bail("Timeout waiting for thread to finish")
        end
    end, Pids),
    ok.


assertion_string(Key1, Key2, Expected) ->
    lists:flatten(io_lib:format("less_json(~p, ~p) == ~p", [Key1, Key2, Expected])).


expectations() ->
    [
     {<<"null">>, <<"null">>, 0},
     {<<"null">>, <<"true">>, -1},
     {<<"null">>, <<"false">>, -1},
     {<<"false">>, <<"false">>, 0},
     {<<"false">>, <<"true">>, -1},
     {<<"true">>, <<"false">>, 1},
     {<<"true">>, <<"true">>, 0},
     {<<"null">>, <<"111">>, -1},
     {<<"111">>, <<"null">>, 1},
     {<<"111">>, <<"true">>, 1},
     {<<"false">>, <<"111">>, -1},
     {<<"123">>, <<"1">>, 1},
     {<<"1">>, <<"123">>, -1},
     {<<"123">>, <<"123">>, 0},
     {<<"123">>, <<"-123">>, 1},
     {<<"-123">>, <<"-123">>, 0},
     {<<"-123">>, <<"123">>, -1},
     {<<"123">>, <<"123.0001">>, -1},
     {<<"123.0001">>, <<"123">>, 1},
     {<<"-123.1">>, <<"123">>, -1},
     {<<"-123.1">>, <<"123.2">>, -1},
     {<<"123">>, <<"123e1">>, -1},
     {<<"123">>, <<"123e0">>, 0},
     {<<"0">>, <<"3.1415">>, -1},
     {<<"123456">>, <<"[]">>, -1},
     {<<"[]">>, <<"123456">>, 1},
     {<<"[1,2,3,4]">>, <<"123456">>, 1},
     {<<"[null,true,false]">>, <<"123456">>, 1},
     {<<"123456">>, <<"[1,2,3,4]">>, -1},
     {<<"123456">>, <<"[null,true,false]">>, -1},
     {<<"[]">>, <<"[]">>, 0},
     {<<"[1,2,3]">>, <<"[1,2,3]">>, 0},
     {<<"[1,2]">>, <<"[1,2,3]">>, -1},
     {<<"[1,2]">>, <<"[1,2,0]">>, -1},
     {<<"[1,2,3]">>, <<"[1,2]">>, 1},
     {<<"[1,2,0]">>, <<"[1,2]">>, 1},
     {<<"[1,2,0]">>, <<"[1,2.1]">>, -1},
     {<<"[1,2.1]">>, <<"[1,2,0]">>, 1},
     {<<"[1,2.1,true]">>, <<"[1,2.1,null]">>, 1},
     {<<"[1,2.1,true]">>, <<"[1,2.1,null,0]">>, 1},
     {<<"[1,2.1,true,55]">>, <<"[1,2.1,null,0]">>, 1},
     {<<"[1,2.1,null,0]">>, <<"[1,2.1,true,55]">>, -1},
     {<<"[1,2.1,true,55]">>, <<"[1,2.1,true,55]">>, 0},
     {<<"\"\"">>, <<"null">>, 1},
     {<<"null">>, <<"\"\"">>, -1},
     {<<"\"\"">>, <<"false">>, 1},
     {<<"false">>, <<"\"\"">>, -1},
     {<<"125">>, <<"\"\"">>, -1},
     {<<"125.4e3">>, <<"\"\"">>, -1},
     {<<"\"\"">>, <<"\"\"">>, 0},
     {<<"\"abc\"">>, <<"\"abc\"">>, 0},
     {<<"[\"abc\"]">>, <<"[\"abc\"]">>, 0},
     {<<"[\"abc\",\"\"]">>, <<"[\"abc\",\"\"]">>, 0},
     {<<"[\"abc\",\"1a\"]">>, <<"[\"abc\",\"\"]">>, 1},
     {<<"[\"abc\",\"\"]">>, <<"[\"abc\",\"1a\"]">>, -1},
     {<<"\"abcdefghijklmniopqrstuvwxyz1234567890\"">>,
      <<"\"abcdefghijklmniopqrstuvwxyz1234567890\"">>, 0},
     {<<"\"abcdefghijklmniopqrstuvwxyz1234567891\"">>,
      <<"\"abcdefghijklmniopqrstuvwxyz1234567890\"">>, 1},
     {<<"\"abcdefghijklmniopqrstuvwxyz1234567890X\"">>,
      <<"\"abcdefghijklmniopqrstuvwxyz1234567890\"">>, 1},
     {<<"\"abcdefghijklmniopqrstuvwxyz1234567890\"">>,
      <<"\"abcdefghijklmniopqrstuvwxyz1234567891\"">>, -1},
     {<<"\"abcdefghijklmniopqrstuvwxyz1234567890\"">>,
      <<"\"abcdefghijklmniopqrstuvwxyz1234567890X\"">>, -1},
     {<<"\"Os cágados voadores que atacaram as vacas do senhor engenheiro Bairrão!\""/utf8>>,
      <<"\"Os cágados voadores que atacaram as vacas do senhor engenheiro Bairrão!\""/utf8>>,
      0},
     {<<"\"Os cagádos voadores que atacaram as vacas do senhor engenheiro Bairrão!\""/utf8>>,
      <<"\"Os cágados voadores que atacaram as vacas do senhor engenheiro Bairrão!\""/utf8>>,
      1},
     {<<"\"Os cágados voadores que atacaram as vacas do senhor engenheiro Bairrão!\""/utf8>>,
      <<"\"Os cagádos voadores que atacaram as vacas do senhor engenheiro Bairrão!\""/utf8>>,
      -1},
     {<<"\"\\\\u00e1\"">>, <<"\"\"">>, 1},
     {<<"\"\\\\U00e1\"">>, <<"\"\"">>, 1},
     {<<"\"h\\\\u00e1\"">>, <<"\"\\\\u00e1\"">>, 1},
     {<<"\"\\\\u00e1\"">>, <<"\"h\\\\u00e1\"">>, -1},
     {<<"\"h\\\\u00e1 \\\\u00e2\"">>, <<"\"h\\\\u00e1 \\\\u00e2\"">>, 0},
     {<<"\"h\\\\u00e1 \\\\u00e1\"">>, <<"\"h\\\\u00e1 \\\\u00e2\"">>, -1},
     {<<"\"h\\\\u00e1 \\\\u00e2\"">>, <<"\"h\\\\u00e1 \\\\u00e1\"">>, 1},
     {<<"{}">>, <<"[]">>, 1},
     {<<"{}">>, <<"{}">>, 0},
     {<<"[]">>, <<"{}">>, -1},
     {<<"{}">>, <<"321">>, 1},
     {<<"{}">>, <<"321.123">>, 1},
     {<<"{}">>, <<"true">>, 1},
     {<<"{}">>, <<"\"foo bar\"">>, 1},
     {<<"\"foo bar\"">>, <<"{}">>, -1},
     {<<"321">>, <<"{}">>, -1},
     {<<"321.123">>, <<"{}">>, -1},
     {<<"{}">>, <<"{\"prop1\":null}">>, -1},
     {<<"{}">>, <<"{\"prop1\":7777}">>, -1},
     {<<"{\"prop1\":123}">>, <<"{\"prop1\":7777}">>, -1},
     {<<"{\"prop1\":123}">>, <<"{\"prop1\":123}">>, 0},
     {<<"{\"prop1\":7777}">>, <<"{\"prop1\":123}">>, 1},
     {<<"{\"prop1\":123}">>, <<"{\"prop1\":123,\"prop2\":\"abc\"}">>, -1},
     {<<"{\"prop1\":123,\"prop2\":\"aba\"}">>, <<"{\"prop1\":123,\"prop2\":\"abc\"}">>, -1},
     {<<"{\"prop1\":123,\"prop2\":\"abc\"}">>, <<"{\"prop1\":123,\"prop2\":\"abc\"}">>, 0},
     {<<"{\"prop1\":123,\"prop2\":\"abc\"}">>, <<"{\"prop1\":123,\"prop2\":\"aba\"}">>, 1}
    ].
