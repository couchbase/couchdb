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

-module(couch_view_parser).

-export([start_context/0, parse_chunk/2, next_state/1]).

-on_load(init/0).

-type key_docid()    :: {Key::binary(), DocId::binary()}.
-type map_value()    :: binary() |
                        {PartId::binary(), Node::binary(), Value::binary()}.
-type reduce_value() :: binary().
-type doc_member()   :: {'doc', Doc::binary()}.
-type view_row()     :: {Key::binary(), reduce_value()} |
                        {key_docid(), map_value()} |
                        {key_docid(), map_value(), doc_member()}.

init() ->
    SoName = case code:priv_dir(?MODULE) of
    {error, bad_name} ->
        case filelib:is_dir(filename:join(["..", "priv"])) of
        true ->
            filename:join(["..", "priv", "couch_view_parser_nif"]);
        false ->
            filename:join(["priv", "couch_view_parser_nif"])
        end;
    Dir ->
        filename:join(Dir, "couch_view_parser_nif")
    end,
    (catch erlang:load_nif(SoName, 0)),
    case erlang:system_info(otp_release) of
    "R13B03" -> true;
    _ -> ok
    end.


-spec start_context() -> {ok, Context::term()}.
start_context() ->
    erlang:nif_error(couch_view_parser_nif_not_loaded).


-spec parse_chunk(Context::term(), iolist()) -> 'ok' | {'error', term()}.
parse_chunk(_Ctx, _Chunk) ->
    erlang:nif_error(couch_view_parser_nif_not_loaded).


-spec next_state(Context::term()) ->
                        {'ok', 'need_more_data'} |
                        {'ok', 'debug_infos', [{From::binary(), Value::binary()}]} |
                        {'ok', 'row_count', string()} |
                        {'ok', 'rows', [view_row()]} |
                        {'ok', 'errors', [{From::binary(), Reason::binary()}]} |
                        {'ok', 'done'} |
                        {'error', term()}.
next_state(_Ctx) ->
    erlang:nif_error(couch_view_parser_nif_not_loaded).
