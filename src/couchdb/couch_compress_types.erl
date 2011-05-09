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

-export([start_link/0, is_compressible/1]).

% gen_server callbacks
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, code_change/3, handle_info/2]).


-include("couch_db.hrl").
-define(MIME_TYPES, couch_compressible_mime_types).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


is_compressible(MimeType) when is_binary(MimeType) ->
    is_compressible(?b2l(MimeType));
is_compressible(FullMimeType) ->
    MimeType = case string:tokens(FullMimeType, ";") of
    [T | _] ->
        couch_util:trim(T);
    [] ->
        FullMimeType
    end,
    case ets:lookup(?MIME_TYPES, MimeType) /= [] of
    true ->
        true;
    false ->
        case string:tokens(MimeType, "/") of
        [MainType | _]  ->
            ets:lookup(?MIME_TYPES, MainType ++ "/*") /= [];
        [] ->
            false
        end
    end.


init(_) ->
    ?MIME_TYPES = ets:new(?MIME_TYPES, [named_table, set, protected]),
    load(couch_config:get("attachments", "compressible_types", "")),
    Parent = self(),
    ok = couch_config:register(
        fun("attachments", "compressible_types", Types) ->
            ok = gen_server:cast(Parent, {reload, Types})
        end),
    {ok, nil}.


handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.


handle_cast({reload, Types}, State) ->
    true = ets:delete_all_objects(?MIME_TYPES),
    load(Types),
    {noreply, State}.


handle_info(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.


terminate(_Reason, _State) ->
    true = ets:delete(?MIME_TYPES).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


load(Types) ->
    CompressTypes = re:split(Types, "\\s*,\\s*", [{return, list}]),
    true = ets:insert(?MIME_TYPES, [{Type, 0} || Type <- CompressTypes]).
