%% @copyright 2012 Couchbase, Inc.
%%
%% @author Filipe Manana  <filipe@couchbase.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%  http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(mapreduce).

-export([start_map_context/1]).
-export([map_doc/2]).

-export([start_reduce_context/1]).
-export([reduce/2, reduce/3]).
-export([rereduce/3]).

-on_load(init/0).


init() ->
    SoName = case code:priv_dir(?MODULE) of
    {error, bad_name} ->
        case filelib:is_dir(filename:join(["..", "priv"])) of
        true ->
            filename:join(["..", "priv", "mapreduce_nif"]);
        false ->
            filename:join(["priv", "mapreduce_nif"])
        end;
    Dir ->
        filename:join(Dir, "mapreduce_nif")
    end,
    (catch erlang:load_nif(SoName, 0)),
    case erlang:system_info(otp_release) of
    "R13B03" -> true;
    _ -> ok
    end.


start_map_context(_MapFunSources) ->
    erlang:nif_error(mapreduce_nif_not_loaded).


map_doc(_Context, _Doc) ->
    erlang:nif_error(mapreduce_nif_not_loaded).


start_reduce_context(_ReduceFunSources) ->
    erlang:nif_error(mapreduce_nif_not_loaded).


reduce(_Context, _KvList) ->
    erlang:nif_error(mapreduce_nif_not_loaded).


reduce(_Context, _ReduceFunNumber, _KvList) ->
    erlang:nif_error(mapreduce_nif_not_loaded).


rereduce(_Context, _ReduceFunNumber, _ReductionsList) ->
    erlang:nif_error(mapreduce_nif_not_loaded).
