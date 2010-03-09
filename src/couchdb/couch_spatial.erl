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

-export([foo/1, get_docs/2]).


-include("couch_db.hrl").


-record(spatial,{
    count=0,
    seq=0}).

start_link() ->
    ?LOG_DEBUG("Spatial daemon: starting link.", []),
    gen_server:start_link({local, couch_spatial}, couch_spatial, [], []).

foo(String) ->
    gen_server:call(couch_spatial, {do_foo, String}).

get_docs(Db, Seq) ->
    %gen_server:call(couch_spatial, {do_get_docs, Seq}).
    gen_server:call(couch_spatial, {do_get_docs, Db, Seq}).


init([]) ->
    {ok, #spatial{}}.

terminate(Reason, _Srv) ->
    ok.


handle_call({do_foo,String}, _From, #spatial{count=Count}) ->
    {reply, ?l2b(lists:flatten(io_lib:format("~s ~w", [String, Count]))), #spatial{count=Count+1}};

%handle_call({do_get_docs, Seq}, _From, #spatial{seq=OldSeq}) ->
handle_call({do_get_docs, Db, Seq}, _From, _State) ->
    %DocsFun = fun(A, B, C) ->
    %    5
    %end,
%    {ok, A, B} = couch_db:enum_docs_since(Db, Seq, fun(DocInfo, _, ChangesProcessed) ->
%        {ok, ChangesProcessed+1}
%    end,
    {ok, A, Acc} = couch_db:enum_docs_since(Db, Seq, fun(DocInfo, _, DocInfoAcc) ->
        %{ok, #doc_info{id=Id, revs=[#rev_info{deleted=false, rev=Rev}|_]}} = DocInfo,
        {doc_info, DocId, DocSeq, _RevInfo} = DocInfo,
        %?LOG_DEBUG("doc: id:~p, seq:~p~n", [DocId, DocSeq]),
        {ok, Doc} = couch_db:open_doc(Db, DocInfo),
        {Body} = Doc#doc.body,
        %?LOG_DEBUG("doc (~p) body: ~p~n", [DocId, Body]),
        Loc = proplists:get_value(<<"loc">>, Body, 0),
        if
        %Loc /= undefined ->
        is_list(Loc) ->
            insert_point("spatial", DocId, list_to_tuple(Loc)),
            {ok, DocInfoAcc ++ [{DocId, DocSeq}]};
        true ->
            {ok, DocInfoAcc}
        end,
        %?LOG_DEBUG("loc decoded: ~p~n", [?JSON_DECODE(?b2l(Loc))]),
        %insert_point("spatial", {}
        %{ok, DocInfoAcc ++ [DocInfo]}
        {ok, DocInfoAcc ++ [{DocId, DocSeq}]}
    end,
    [], []),
    {reply, ?l2b(io_lib:format("hello couch (seq: ~w, A: ~p, B: ~p)",
                               [Seq, A, Acc])), #spatial{seq=Seq}}.

insert_point(DbName, DocId, {X, Y}) ->
    ?LOG_DEBUG("Insert (~s) point (~w, ~w) into '~s'~n", [DocId, X, Y, DbName]).

handle_cast(foo,State) ->
    {noreply, State}.

handle_info(Msg, Server) ->
    {noreply, Server}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


