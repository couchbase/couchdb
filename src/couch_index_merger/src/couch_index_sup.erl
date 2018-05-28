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
%%
%% @doc Supervisor for the query volume logger
%%

-module(couch_index_sup).
-behaviour(supervisor).

%% Supervisor callbacks
-export([start_link/0]).
-export([init/1]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    {ok, {{one_for_one, 3, 60},
       [{couch_query_logger,
         {couch_query_logger, start_link, []},
         permanent, 1000, worker, [couch_query_logger]}
       ]}}.
