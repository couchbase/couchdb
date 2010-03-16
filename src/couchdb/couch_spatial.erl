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

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([update_tree/1, bbox_search/1]).

-define(FILENAME, "/tmp/couchdb_vtree.bin").


-include("couch_db.hrl").


-record(spatial, {
    seq=0,
    fd=nil,
    treepos=-1,
    btree=nil}).

-record(node, {
    % type = inner | leaf
    type=inner}).


start_link() ->
    ?LOG_DEBUG("Spatial daemon: starting link.", []),
    gen_server:start_link({local, couch_spatial}, couch_spatial, [], []).

update_tree(Db) ->
    %gen_server:call(couch_spatial, {do_get_docs, Seq}).
    gen_server:call(couch_spatial, {do_update_tree, Db}).

bbox_search(Bbox) ->
    gen_server:call(couch_spatial, {do_bbox_search, Bbox}).

init([]) ->
    case couch_file:open(?FILENAME) of
    {ok, Fd} ->
        {ok, State} = couch_file:read_header(Fd),
        {ok, Btree} = couch_btree:open(nil, Fd),
        {ok, State#spatial{fd=Fd, btree=Btree}};
    {error, enoent} ->
        case couch_file:open(?FILENAME, [create, overwrite]) of
        {ok, Fd} ->
            {ok, Btree} = couch_btree:open(nil, Fd),
            State = #spatial{fd=Fd, btree=Btree},
            couch_file:write_header(Fd, State),
            {ok, State};
        {error, Reason} ->
            io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
                      [Reason, ?FILENAME]),
            {error, Reason}
        end;
    {error, Reason} ->
        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
                  [Reason, ?FILENAME]),
        {error, Reason}
    end.

terminate(_Reason, _Srv) ->
    ok.

handle_call({do_update_tree, Db}, _From,
            #spatial{seq=Seq, fd=Fd}=State) ->
    {ok, _, StateNew} = couch_db:enum_docs_since(Db, Seq,
            fun(DocInfo, _, StateCur) ->
        {doc_info, DocId, DocSeq, _RevInfo} = DocInfo,
        {ok, Doc} = couch_db:open_doc(Db, DocInfo),
        {Body} = Doc#doc.body,
        Loc = proplists:get_value(<<"loc">>, Body, 0),
        if
%        %Loc /= undefined ->
        is_list(Loc) ->
            TreePos = insert_point(StateCur, DocId, list_to_tuple(Loc)),
            {ok, StateCur#spatial{seq=DocSeq, treepos=TreePos}};
        true ->
            {ok, StateCur#spatial{seq=DocSeq}}
        end
    end, State, []),
    couch_file:write_header(Fd, StateNew),
    {reply, [], StateNew};


handle_call({do_bbox_search, Bbox}, _From,
            #spatial{fd=Fd, treepos=TreePos}=State) ->
    Result = vtree:lookup(Fd, TreePos, Bbox),
%    ?LOG_DEBUG("bbox_search result: ~p", [Result]),
    Output = lists:foldl(fun({Loc, _Meta, DocId}, Acc) ->
         % NOTE vmx: it's only a point, but we saved MBRs
         {X, Y, _, _} = Loc,
         Acc ++ [{[{<<"id">>, DocId}, {<<"loc">>, [X, Y]}]}]
    end, [], Result),
    {reply, Output, State}.

insert_point(#spatial{fd=Fd, treepos=TreePos, btree=Btree}=State,
             DocId, {X, Y}) ->
    ?LOG_DEBUG("insert point: ~p", [DocId]),
    {ok, _Mbr, TreePosNew} = vtree:insert(Fd, TreePos,
            {{X, Y, X, Y}, #node{type=leaf}, DocId}),
    % store MBR in back-index
    couch_btree:add(Btree, [{DocId, {X, Y, X, Y}}]),

    TreePosNew.

handle_cast(foo,State) ->
    {noreply, State}.

handle_info(_Msg, Server) ->
    {noreply, Server}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
