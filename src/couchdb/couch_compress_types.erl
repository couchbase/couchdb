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

% This stores information about the mime types that are compressable.

-module(couch_compress_types).
-include("couch_db.hrl").


-export([start_link/0,is_compressible/1]).

% gen_server callbacks
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, code_change/3, handle_info/2]).


start_link() ->
    gen_server:start_link(?MODULE, [], []).


is_compressible(MimeType) when is_binary(MimeType)->
    is_compressible(?b2l(MimeType));
is_compressible(MimeType) ->
    case ets:lookup(couch_compress_types, MimeType) /= [] of
    true ->
        true;
    false ->
        [MainType|_] = string:tokens(MimeType,"/"),
        ets:lookup(couch_compress_types, MainType ++ "/*") /= []
    end.
        

init([]) ->
    ets:new(couch_compress_types, [set, protected, named_table]),
    load(),
    ok = couch_config:register(
        fun("couchdb", "max_dbs_open", Max) ->
            gen_server:call(couch_server,
                    {set_max_dbs_open, list_to_integer(Max)})
        end),
    {ok, nil}.

terminate(_Reason, _) ->
    ok.

    
handle_call(foo, _From, _) ->
    exit(cant_get_here).


handle_cast(reload, _) ->
    ets:delete_all_objects(couch_compress_types),
    load(),
    {noreply, nil}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info(X, Q) ->
    {stop, X, Q}.


load() ->
    CompressTypes =
        string:tokens(
            couch_config:get("attachments", "compressible_types", ""), ", "),

    CompressTypesKVs = [{Type, 0} || Type <- CompressTypes],
    ets:insert(couch_compress_types, CompressTypesKVs).