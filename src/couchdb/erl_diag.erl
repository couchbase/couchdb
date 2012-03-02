% Copyright 2011,  Filipe David Manana  <fdmanana@apache.org>
% Web site:  http://github.com/fdmanana
%
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

-module(erl_diag).

-export([list/0, list/1]).
-export([print/0, print/1]).


print() ->
    print([]).

% Options:
% {sort_by, Field}
% {limit, N}
% desc | descending
% {filter_by, {Field, Value}}
%
print(Options) ->
    lists:foreach(
        fun({Pid, Info}) ->
            io:format("~nProcess ~p~n~n", [Pid]),
            lists:foreach(
                fun({Name, Value}) ->
                    io:format("~c~-40s: ~p~n", [$\t, Name, Value])
                end,
                Info)
        end,
        list(Options)).


list() ->
    list([]).

% Options:
% {sort_by, Field}
% {limit, N}
% desc | descending
% {filter_by, {Field, Value}}
%
list(Options) ->
    Procs = case proplists:get_value(sort_by, Options, nil) of
    nil ->
        [{P, process_info(P)} || P <- processes()];
    SortBy ->
        lists:sort(
            fun({_, A}, {_, B}) ->
                proplists:get_value(SortBy, A) < proplists:get_value(SortBy, B)
            end,
            [{P, process_info(P)} || P <- processes()])
    end,
    Result0 = lists:map(fun make_summary/1, Procs),
    Result1 = case proplists:get_value(filter_by, Options, nil) of
    nil ->
        Result0;
    {FieldName, FieldValue} ->
        lists:filter(
            fun({_, I}) -> proplists:get_value(FieldName, I) == FieldValue end,
            Result0)
    end,
    Result2 = case lists:member(descending, Options) orelse
        lists:member(desc, Options) of
    true ->
        lists:reverse(Result1);
    false ->
        Result1
    end,
    case proplists:get_value(limit, Options, nil) of
    nil ->
        Result2;
    Limit when is_integer(Limit) ->
        lists:sublist(Result2, Limit)
    end.


make_summary({Pid, Info}) ->
    InitCall = case proplists:get_value(initial_call, Info) of
    {proc_lib, init_p, _} ->
        proc_lib:translate_initial_call(Pid);
    Else ->
        Else
    end,
    TotalHeapSize = proplists:get_value(total_heap_size, Info) * erlang:system_info(wordsize),
    Summary = [
        {initial_call, InitCall},
        {status, proplists:get_value(status, Info)},
        {message_queue_len, proplists:get_value(message_queue_len, Info)},
        {total_heap_size, TotalHeapSize},
        {reductions, proplists:get_value(reductions, Info)}
    ],
    {Pid, Summary}.
