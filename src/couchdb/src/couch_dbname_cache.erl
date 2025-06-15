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

-module(couch_dbname_cache).
-behaviour(gen_server).

-include("couch_db.hrl").

-record(dir_cache, {
    dbname_to_uuid_cache :: atom()
}).

% public API
-export([start_link/0, stop/0]).
-export([get_uuid_entry/1, delete/1, get_dbname_from_uuid/1]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-define(SETNAME_TO_UUID, dbname_to_uuid).

start_link() ->
    try
        gen_server:start_link({local, ?MODULE}, ?MODULE, [], [])
    catch
        Type:Reason ->
            catch(gen_server:stop(?MODULE)),
            {error, {Type, Reason}}
    end.

stop() ->
    catch(gen_server:stop(?MODULE)).

get_uuid_entry(Dbname) ->
    gen_server:call(?MODULE, {get, Dbname}, infinity).

delete(Dbname) ->
    gen_server:call(?MODULE, {delete, Dbname}, infinity).

get_dbname_from_uuid(UUID) ->
    gen_server:call(?MODULE, {get_dbname, UUID}, infinity).

get_bucket_uuid(DbName) ->
    BucketName = ?b2l(DbName),
    Snapshot = ns_bucket:get_snapshot(BucketName),
    ns_bucket:uuid(BucketName, Snapshot).

init([]) ->
    State = #dir_cache{
        dbname_to_uuid_cache = ?SETNAME_TO_UUID
    },
    ets:new(State#dir_cache.dbname_to_uuid_cache,
            [set, protected, named_table, {read_concurrency, true}]),
    {ok, State}.
 
terminate(_Reason, _State) ->
    ok.

handle_call({get, DbName}, _From, State) ->
    case ets:lookup(State#dir_cache.dbname_to_uuid_cache, DbName) of
    [] ->
        case get_bucket_uuid(DbName) of
        not_present ->
            {reply, DbName, State};
        UUID ->
            ets:insert(State#dir_cache.dbname_to_uuid_cache, {DbName, UUID}),
            {reply, UUID, State}
        end;
    [{_, UUID}] ->
        {reply, UUID, State}
    end;
handle_call({get_dbname, UUID}, _From, State) ->
    case ets:match_object(State#dir_cache.dbname_to_uuid_cache, {'_', UUID}) of
    [] ->
        {reply, UUID, State};
    [{DbName, _}] ->
        {reply, DbName, State}
    end;
handle_call({delete, DbName}, _From, State) ->
    ets:delete(State#dir_cache.dbname_to_uuid_cache, DbName),
    {reply, ok, State}.

handle_cast(_, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info(_, State) ->
    {noreply, State}.
