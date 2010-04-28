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

% The groupID is the name of the design document (ID minus the "_design/"

-module(couch_spatial).
-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([update_tree/3, bbox_search/4]).

-define(FILENAME, "/tmp/couchdb_vtree.bin").


-include("couch_db.hrl").
-include("couch_spatial.hrl").


%-record(spatial, {
%    root_dir=nil,
%    seq=0,
%    fds=[],
%    treepos=-1,
%    btree=nil}).

-record(node, {
    % type = inner | leaf
    type=inner}).


start_link() ->
    ?LOG_DEBUG("Spatial daemon: starting link.", []),
    gen_server:start_link({local, couch_spatial}, couch_spatial, [], []).

update_tree(Db, DDoc, SpatialName) ->
    {ok, Index, Group} = get_spatial_index(Db, DDoc#doc.id, SpatialName, nil),
%    do_update_tree(Db, DDoc, SpatialName, Group, Index),
    ?LOG_DEBUG("Update tree complete", []).
%    ok.
%    gen_server:call(couch_spatial, {do_update_tree, Db, DDoc, SpatialName, Group, Index}).

bbox_search(Db, DDoc, SpatialName, Bbox) ->
    {ok, Index, Group} = get_spatial_index(Db, DDoc#doc.id, SpatialName, nil),
    {ok, Result} = do_bbox_search(Bbox, Group, Index),
    %gen_server:call(couch_spatial, {do_bbox_search, Bbox}).
    %<<"foohoo">>.
    Result.

init([]) ->
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    ets:new(couch_spatial_groups_by_db, [bag, private, named_table]),
    ets:new(spatial_group_servers_by_sig, [set, protected, named_table]),
    ets:new(couch_spatial_groups_by_updater, [set, private, named_table]),
    process_flag(trap_exit, true),
    {ok, #spatial{root_dir=RootDir}}.

    %{ok, #spatial{root_dir=RootDir}}.

%%    case couch_file:open(?FILENAME) of
%%    {ok, Fd} ->
%%        {ok, State} = couch_file:read_header(Fd),
%%        {ok, Btree} = couch_btree:open(nil, Fd),
%%        {ok, State#spatial{fd=Fd, btree=Btree}};
%%    {error, enoent} ->
%        case couch_file:open(?FILENAME, [create, overwrite]) of
%        {ok, Fd} ->
%            {ok, Btree} = couch_btree:open(nil, Fd),
%            State = #spatial{fd=Fd, btree=Btree},
%            couch_file:write_header(Fd, State),
%            {ok, State};
%        {error, Reason} ->
%            io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
%                      [Reason, ?FILENAME]),
%            {error, Reason}
%%        end;
%%    {error, Reason} ->
%%        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
%%                  [Reason, ?FILENAME]),
%%        {error, Reason}
%    end.

add_to_ets(Pid, DbName, Sig) ->
    true = ets:insert(couch_spatial_groups_by_updater, {Pid, {DbName, Sig}}),
    true = ets:insert(spatial_group_servers_by_sig, {{DbName, Sig}, Pid}),
    true = ets:insert(couch_spatial_groups_by_db, {DbName, Sig}).


delete_from_ets(Pid, DbName, Sig) ->
    true = ets:delete(couch_spatial_groups_by_updater, Pid),
    true = ets:delete(spatial_group_servers_by_sig, {DbName, Sig}),
    true = ets:delete_object(couch_spatial_groups_by_db, {DbName, Sig}).


get_group_server(DbName, DDocName) ->
    % get signature for group
    case couch_spatial_group:open_db_group(DbName, DDocName) of
    % do we need to close this db?
    {ok, _Db, Group} ->
?LOG_DEBUG("get_group_server: ~p", [DDocName]),
        case gen_server:call(couch_spatial, {get_group_server, DbName, Group}) of
        {ok, Pid} ->
            Pid;
        Error ->
            throw(Error)
        end;
    Error ->
        throw(Error)
    end.

get_group(Db, GroupId, Stale) ->
    MinUpdateSeq = case Stale of
    ok -> 0;
    _Else -> couch_db:get_update_seq(Db)
    end,
?LOG_DEBUG("get_group: MinUpdateSeq: ~p (stale? ~p)", [MinUpdateSeq, Stale]),
    couch_spatial_group:request_group(
            get_group_server(couch_db:name(Db), GroupId),
            MinUpdateSeq).

delete_index_dir(RootDir, DbName) ->
    couch_view:nuke_dir(RootDir ++ "/." ++ ?b2l(DbName) ++ "_design").


% XXX NOTE vmx: I don't know when this case happens
do_reset_indexes(DbName, Root) ->
    % shutdown all the updaters and clear the files, the db got changed
    Names = ets:lookup(couch_spatial_groups_by_db, DbName),
    lists:foreach(
        fun({_DbName, Sig}) ->
            ?LOG_DEBUG("Killing update process for spatial group ~s. in database ~s.", [Sig, DbName]),
            [{_, Pid}] = ets:lookup(spatial_group_servers_by_sig, {DbName, Sig}),
            exit(Pid, kill),
            receive {'EXIT', Pid, _} ->
                delete_from_ets(Pid, DbName, Sig)
            end
        end, Names),
    delete_index_dir(Root, DbName),
    file:delete(Root ++ "/." ++ ?b2l(DbName) ++ "_temp").

% counterpart in couch_view is get_map_view/4
get_spatial_index(Db, GroupId, Name, Stale) ->
?LOG_DEBUG("get_spatial_index: ~p", [Name]),
    case get_group(Db, GroupId, Stale) of
    {ok, #spatial_group{indexes=Indexes}=Group} ->
        case get_spatial_index0(Name, Indexes) of
        {ok, Index} ->
            {ok, Index, Group};
        Else ->
            Else
        end;
    Error ->
        Error
    end.

get_spatial_index0(_Name, []) ->
    {not_found, missing_named_view};
get_spatial_index0(Name, [#spatial{index_names=IndexNames}=Index|Rest]) ->
?LOG_DEBUG("Name: ~p, IndexNames: ~p", [Name, IndexNames]),
    % NOTE vmx: I don't understand why need lists:member and recursion
    case lists:member(Name, IndexNames) of
        true -> {ok, Index};
        false -> get_spatial_index0(Name, Rest)
    end.





terminate(_Reason, _Srv) ->
    ok.
%terminate(Reason, State) ->
%    ?LOG_DEBUG("!!!! TERMINATION: ~p~n~p", [Reason, State]),
%    {reply, [], State}.

%get_index_name(#doc{body={Fields}}, SpatialName) ->
%    Language = proplists:get_value(<<"language">>, Fields, <<"javascript">>),
%    {RawIndexes} = proplists:get_value(<<"spatial">>, Fields, {[]}),
%    Index = proplists:get_value(SpatialName, RawIndexes, ""),
%    couch_util:to_hex(?b2l(erlang:md5(term_to_binary({Language, Index})))).
%%     filelib:wildcard(RootDir ++ "/." ++ ?b2l(couch_db:name(Db)) ++ "_design"++"/*")
%
%get_fd_name(DbName, DDocName, SpatialName) ->
%    DbName ++ "_" ++ DDocName ++ "_" ++ SpatialName.

%% Returns {ok, State}
%open_spatial_index(Filename, State#spatial{fds=Fds}) ->
%%    case couch_file:open(Filename) of
%%    {ok, Fd} ->
%%        {ok, State} = couch_file:read_header(Fd),
%%        {ok, Btree} = couch_btree:open(nil, Fd),
%%        {ok, State#spatial{fd=Fd, btree=Btree}};
%%    {error, enoent} ->
%        case couch_file:open(Filename, [create, overwrite]) of
%        {ok, Fd} ->
%            {ok, Btree} = couch_btree:open(nil, Fd),
%            Fds2 = Fds ++ [{get_fd_name(DbName, DDocName, SpatialName), Fd}]
%            State = #spatial{fds=Fds2, btree=Btree},
%            couch_file:write_header(Fd, State),
%            %{ok, State};
%            {Fd, Btree};
%        {error, Reason} ->
%            io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
%                      [Reason, Filename]),
%            {error, Reason}
%%        end;
%%    {error, Reason} ->
%%        io:format("ERROR (~s): Couldn't open file (~s) for tree storage~n",
%%                  [Reason, ?FILENAME]),
%%        {error, Reason}
%    end.

do_update_tree(Db, DDoc, SpatialName, #spatial_group{fd=Fd}=Group,
               #spatial{seq=Seq}=Index) ->
%    TreePos = Index#spatial.treepos,
%    {ok, _, StateNew2} = couch_db:enum_docs_since(Db, Seq,
%            fun(DocInfo, _, StateCur) ->
    ok.

do_bbox_search(Bbox, #spatial_group{fd=Fd}=Group,
               #spatial{treepos=TreePos}=Index) ->
    Result = vtree:lookup(Fd, TreePos, Bbox),
    ?LOG_DEBUG("bbox_search result: ~p", [Result]),
    Output = lists:foldl(fun({Loc, _Meta, DocId}, Acc) ->
         % NOTE vmx: it's only a point, but we saved MBRs
         {X, Y, _, _} = Loc,
         Acc ++ [{[{<<"id">>, DocId}, {<<"loc">>, [X, Y]}]}]
    end, [], Result),
    {ok, Output}.
    


%% % GroupId = DName (design document name without "_design/" prefix
%% handle_call({do_update_tree, Db, DDoc, SpatialName, Group, Index}, _From,
%%             #spatial{seq=Seq, root_dir=RootDir, btree=Btree}=State) ->
%% ?LOG_DEBUG("handle do_update_tree call", []),
%%     % Index is a #spatial record
%%     %{ok, Index, Group} = get_spatial_index(Db, DDoc#doc.id, SpatialName, ok),
%%     Fd = Group#spatial_group.fd,
%%     TreePos = Index#spatial.treepos,

%%     {ok, _, StateNew2} = couch_db:enum_docs_since(Db, Seq,
%%             fun(DocInfo, _, StateCur) ->
%%         % NOTE vmx: I only take the first element of RevInfo, that should be
%%         %enough
%%         {doc_info, DocId, DocSeq, [RevInfo|_]} = DocInfo,
%% ?LOG_DEBUG("(2) handle do_update_tree call", []),
%%         case RevInfo#rev_info.deleted of
%%         false ->
%%             {ok, Doc} = couch_db:open_doc(Db, DocInfo),
%%             JsonDoc = couch_query_servers:json_doc(Doc),
%% ?LOG_DEBUG("(3) handle do_update_tree call", []),
%%             % XXX vmx: ERRORHANDLING. If the function doesn't exist in the
%%             %     design document, then CouchDB should handle the problem.
%%             [<<"spatial">>, GeoJson] = couch_query_servers:ddoc_prompt(
%%                     DDoc, [<<"spatial">>, SpatialName], [JsonDoc]),
%%             case GeoJson of
%%             false ->
%%                 % Document doesn't contain spatial information
%%                 {ok, StateCur#spatial{seq=DocSeq}};
%%             _ ->
%% ?LOG_DEBUG("(4) handle do_update_tree call", []),
%%                 {GeoProplist} = GeoJson,
%%                 Coords = proplists:get_value(<<"coordinates">>, GeoProplist),
%%                 case proplists:get_value(<<"type">>, GeoProplist) of
%%                 <<"Point">> ->
%%                     {TreePos, BtreeNew} = insert_point(Fd, TreePos, Btree,
%%                                                        DocId,
%%                                                        list_to_tuple(Coords)),
%%                     % XXX vmx: not the whole state is needed. A new record
%%                     %     is needed for this loop. That record should contain
%%                     %     seq, treepos, btree.
%%                     {ok, StateCur#spatial{seq=DocSeq, treepos=TreePos,
%%                                           btree=BtreeNew}};
%%                 _ ->
%%                     ?LOG_INFO("Other types than Points are not supported yet", []),
%%                     {ok, StateCur#spatial{seq=DocSeq}}
%%                 end
%%             end;
%%         true ->
%%             ?LOG_DEBUG("document got deleted: ~p", [DocId]),
%% %            case couch_btree:lookup(StateCur#spatial.btree, [DocId]) of
%% %            [{ok, {DocId, Mbr}}] ->
%% %                ?LOG_DEBUG("~p's MBR is: ~p", [DocId, Mbr]),
%% %                %{TreePos, BtreeNew} = delete_point(State, DocId, Mbr),
%% %                {TreePos, BtreeNew} = delete_point(StateCur, DocId, Mbr),
%% %                {ok, StateCur#spatial{seq=DocSeq, treepos=TreePos,
%% %                                      btree=BtreeNew}};
%% %            [not_found] ->
%% %                % If it's not in the back-index it's not in the spatial index
%% %                {ok, StateCur#spatial{seq=DocSeq}}
%% %            end
%%             {ok, StateCur#spatial{seq=DocSeq}}
%%         end
%%     end, State, []),
%% %    couch_file:write_header(Fd, StateNew2),
%%     {reply, [], StateNew2};


%handle_call({do_update_tree_old, Db, DDoc, SpatialName}, _From,
%            #spatial{seq=Seq, root_dir=RootDir, btree=Btree, fds=Fds}=State) ->
%%            #spatial{seq=Seq, fd=Fd, btree=Btree}=State) ->
%    % NOTE vmx: for now I store the the file handlers with
%    %     databasename_ designdocumentname_spatialindexname as a key.
%    %      Probably the hash of the sptial index should be used.
%%RootDir ++ "/." ++ ?b2l(DbName) ++ "_design/".
%%    {Fd, BtreeNew} = case proplists:get_value(
%%                            get_fd_name(DbName, DDocName, SpatialName), Fds) of
%%    undefined ->
%%        Filename = couch_view_group:design_root(RootDir, DbName) ++
%%            get_index_name(DDoc, SpatialName) ++ ".spatial",
%%        %{ok, Fd2, BtreeNew2} = open_spatial_index(Filename, State),
%%        open_spatial_index(Filename, State),
%%    Fd2 ->
%%        {Fd2, Btree}
%%    end,
%%    StateNew = State#spatial{fds=Fds}
%
%    %Filename = couch_view_group:design_root(RootDir, DbName) ++ hex_sig(GroupSig) ++".view".
%%erlang:md5(term_to_binary({Views, Language, DesignOptions})
%%couch_util:to_hex(?b2l(GroupSig))
%%    Filename = couch_view_group:design_root(RootDir, DbName) ++ hex_sig(GroupSig) ++".view".
%    %?LOG_DEBUG("design doc: ~p", [DDoc]),
%    {ok, _, StateNew2} = couch_db:enum_docs_since(Db, Seq,
%            fun(DocInfo, _, StateCur) ->
%        % NOTE vmx: I only take the first element of RevInfo, that should be
%        %enough
%        {doc_info, DocId, DocSeq, [RevInfo|_]} = DocInfo,
%        case RevInfo#rev_info.deleted of
%        false ->
%            {ok, Doc} = couch_db:open_doc(Db, DocInfo),
%            JsonDoc = couch_query_servers:json_doc(Doc),
%            % XXX vmx: ERRORHANDLING. If the function doesn't exist in the
%            %     design document, then CouchDB should handle the problem.
%            [<<"spatial">>, GeoJson] = couch_query_servers:ddoc_prompt(
%                    DDoc, [<<"spatial">>, SpatialName], [JsonDoc]),
%            case GeoJson of
%            false ->
%                % Document doesn't contain spatial information
%                {ok, StateCur#spatial{seq=DocSeq}};
%            _ ->
%                {GeoProplist} = GeoJson,
%                Coords = proplists:get_value(<<"coordinates">>, GeoProplist),
%                case proplists:get_value(<<"type">>, GeoProplist) of
%                <<"Point">> ->
%                    {TreePos, BtreeNew} = insert_point(StateCur, DocId,
%                                                       list_to_tuple(Coords)),
%                    {ok, StateCur#spatial{seq=DocSeq, treepos=TreePos,
%                                          btree=BtreeNew}};
%                _ ->
%                    ?LOG_INFO("Other types than Points are not supported yet", []),
%                    {ok, StateCur#spatial{seq=DocSeq}}
%                end
%            end;
%        true ->
%            ?LOG_DEBUG("document got deleted: ~p", [DocId]),
%            case couch_btree:lookup(StateCur#spatial.btree, [DocId]) of
%            [{ok, {DocId, Mbr}}] ->
%                ?LOG_DEBUG("~p's MBR is: ~p", [DocId, Mbr]),
%                %{TreePos, BtreeNew} = delete_point(State, DocId, Mbr),
%                {TreePos, BtreeNew} = delete_point(StateCur, DocId, Mbr),
%                {ok, StateCur#spatial{seq=DocSeq, treepos=TreePos,
%                                      btree=BtreeNew}};
%            [not_found] ->
%                % If it's not in the back-index it's not in the spatial index
%                {ok, StateCur#spatial{seq=DocSeq}}
%            end
%        end
%    end, State, []),
%    couch_file:write_header(Fd, StateNew2),
%    {reply, [], StateNew2};



%handle_call({do_bbox_search, Bbox}, _From,
%            #spatial{fd=Fd, treepos=TreePos}=State) ->
%%            #spatial{root_dir=RootDir, treepos=TreePos}=State) ->
%%couch_view_group:design_root(RootDir, DbName) ++ hex_sig(GroupSig) ++".view".
%    Result = vtree:lookup(Fd, TreePos, Bbox),
%%    ?LOG_DEBUG("bbox_search result: ~p", [Result]),
%    Output = lists:foldl(fun({Loc, _Meta, DocId}, Acc) ->
%         % NOTE vmx: it's only a point, but we saved MBRs
%         {X, Y, _, _} = Loc,
%         Acc ++ [{[{<<"id">>, DocId}, {<<"loc">>, [X, Y]}]}]
%    end, [], Result),
%    {reply, Output, State#spatial{fd=Fd}};

handle_call({get_group_server, DbName,
    #spatial_group{name=GroupId,sig=Sig}=Group}, _From,
            #spatial{root_dir=Root}=Server) ->
?LOG_DEBUG("get_group_server handle:", []),
    case ets:lookup(spatial_group_servers_by_sig, {DbName, Sig}) of
    [] ->
        ?LOG_DEBUG("Spawning new group server for spatial group ~s in database ~s.",
            [GroupId, DbName]),
        case (catch couch_spatial_group:start_link({Root, DbName, Group})) of
        {ok, NewPid} ->
            add_to_ets(NewPid, DbName, Sig),
           {reply, {ok, NewPid}, Server};
        {error, invalid_view_seq} ->
            do_reset_indexes(DbName, Root),
            case (catch couch_spatial_group:start_link({Root, DbName, Group})) of
            {ok, NewPid} ->
                add_to_ets(NewPid, DbName, Sig),
                {reply, {ok, NewPid}, Server};
            Error ->
                {reply, Error, Server}
            end;
        Error ->
            {reply, Error, Server}
        end;
    [{_, ExistingPid}] ->
        {reply, {ok, ExistingPid}, Server}
    end.


insert_point(Fd, TreePos, Btree, DocId, {X, Y}) ->
    ?LOG_DEBUG("insert point: ~p", [DocId]),
    {ok, _Mbr, TreePosNew} = vtree:insert(Fd, TreePos,
            {{X, Y, X, Y}, #node{type=leaf}, DocId}),
    % store MBR in back-index
    {ok, BtreeNew} = couch_btree:add(Btree, [{DocId, {X, Y, X, Y}}]),

    {TreePosNew, BtreeNew}.

%insert_point_old(#spatial{fd=Fd, treepos=TreePos, btree=Btree}=State,
%             DocId, {X, Y}) ->
%    ?LOG_DEBUG("insert point: ~p", [DocId]),
%    {ok, _Mbr, TreePosNew} = vtree:insert(Fd, TreePos,
%            {{X, Y, X, Y}, #node{type=leaf}, DocId}),
%    % store MBR in back-index
%    {ok, BtreeNew} = couch_btree:add(Btree, [{DocId, {X, Y, X, Y}}]),
%
%    {TreePosNew, BtreeNew}.

%delete_point(#spatial{fd=Fd, treepos=TreePos, btree=Btree}=State,
%             DocId, Mbr) ->
%    ?LOG_DEBUG("delete point: ~p", [DocId]),
%    {ok, TreePosNew} = vtree:delete(Fd, DocId, Mbr, TreePos),
%    % delete from back-index
%    {ok, BtreeNew} = couch_btree:add_remove(Btree, [], [DocId]),
%
%    {TreePosNew, BtreeNew}.


handle_cast(foo,State) ->
    {noreply, State}.

%handle_info({'EXIT', FromPid, {{nocatch, Reason}, _Trace}}, State) ->
%    ?LOG_DEBUG("Uncaught throw() in linked pid: ~p", [{FromPid, Reason}]),
%    {stop, Reason, State};
%handle_info({'EXIT', FromPid, {{_, Reason}, _Trace}}, State) ->
%    ?LOG_DEBUG("Uncaught throw() in linked pid: ~p", [{FromPid, Reason}]),
%    {stop, Reason, State};

%handle_info({'EXIT', FromPid, {{bad_return_value, Reason}, _Trace}}, State) ->
%    ?LOG_DEBUG("Uncaught throw() in linked pid: ~p", [{FromPid, Reason}]),
%    {stop, Reason, State};
handle_info({'EXIT', FromPid, Reason}, State) ->
    ?LOG_DEBUG("Exit from linked pid: ~p", [{FromPid, Reason}]),
    {stop, Reason, State};

handle_info(_Msg, Server) ->
    {noreply, Server}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



