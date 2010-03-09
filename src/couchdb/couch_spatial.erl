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

-module(couch_spatial).
-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3,foo/1]).


-include("couch_db.hrl").


-record(spatial,{
    count=0}).

start_link() ->
    ?LOG_DEBUG("Spatial daemon: starting link.", []),
    gen_server:start_link({local, couch_spatial}, couch_spatial, [], []).

foo(String) ->
    gen_server:call(couch_spatial, {do_foo, String}).

init([]) ->
    {ok, #spatial{}}.


terminate(Reason, _Srv) ->
    ok.


handle_call({do_foo,String}, _From, #spatial{count=Count}) ->
    {reply, ?l2b(lists:flatten(io_lib:format("~s ~w", [String, Count]))), #spatial{count=Count+1}}.

handle_cast(foo,State) ->
    {noreply, State}.

handle_info(Msg, Server) ->
    {noreply, Server}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


