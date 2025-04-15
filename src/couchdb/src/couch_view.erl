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

-module(couch_view).
-behaviour(gen_server).

-export([start_link/0,fold/4,less_json/2,less_json_ids/2,expand_dups/2,
    detuple_kvs/2,init/1,terminate/2,handle_call/3,handle_cast/2,handle_info/2,
    code_change/3,get_reduce_view/4,get_temp_reduce_view/5,get_temp_map_view/4,
    get_map_view/4,get_row_count/1,reduce_to_count/1,fold_reduce/4,
    extract_map_view/1,get_group_server/2,get_group_info/2,cleanup_index_files/1]).

-include("couch_db.hrl").


-record(server,{
    root_dir = []}).

start_link() ->
    gen_server:start_link({local, couch_view}, couch_view, [], []).

get_temp_updater(DbName, Language, DesignOptions, MapSrc, RedSrc) ->
    {ok, Group} =
        couch_view_group:open_temp_group(DbName, Language, DesignOptions, MapSrc, RedSrc),
    case gen_server:call(couch_view, {get_group_server, DbName, Group}, infinity) of
    {ok, Pid} ->
        Pid;
    Error ->
        throw(Error)
    end.

get_group_server({DbName, GroupDbName}, GroupId) when is_binary(GroupId) ->
    DbGroup = case GroupId of
    <<?DESIGN_DOC_PREFIX, _/binary>> ->
        open_db_group(GroupDbName, GroupId);
    _ ->
        open_db_group(GroupDbName, <<?DESIGN_DOC_PREFIX, GroupId/binary>>)
    end,
    get_group_server(DbName, DbGroup);
get_group_server(DbName, GroupId) when is_binary(GroupId) ->
    Group = open_db_group(DbName, GroupId),
    get_group_server(DbName, Group);
get_group_server(DbName, Group) ->
    case gen_server:call(couch_view, {get_group_server, DbName, Group}, infinity) of
    {ok, Pid} ->
        Pid;
    Error ->
        throw(Error)
    end.

open_db_group(DbName, GroupId) ->
    case couch_view_group:open_db_group(DbName, GroupId) of
    {ok, Group} ->
        Group;
    Error ->
        throw(Error)
    end.

get_group(Db, {GroupDb, GroupId}, Stale) ->
    DbGroup = open_db_group(couch_db:name(GroupDb), GroupId),
    do_get_group(Db, DbGroup, Stale);
get_group(Db, GroupId, Stale) ->
    DbGroup = open_db_group(couch_db:name(Db), GroupId),
    do_get_group(Db, DbGroup, Stale).

do_get_group(Db, DbGroup, Stale) ->
    MinUpdateSeq = case Stale of
    ok -> 0;
    update_after -> 0;
    _Else -> couch_db:get_update_seq(Db)
    end,
    GroupPid = get_group_server(couch_db:name(Db), DbGroup),
    Result = couch_view_group:request_group(GroupPid, MinUpdateSeq),
    case Stale of
    update_after ->
        % best effort, process might die
        spawn(fun() ->
            LastSeq = couch_db:get_update_seq(Db),
            couch_view_group:request_group(GroupPid, LastSeq)
        end);
    _ ->
        ok
    end,
    Result.

get_temp_group(Db, Language, DesignOptions, MapSrc, RedSrc) ->
    couch_view_group:request_group(
        get_temp_updater(couch_db:name(Db), Language, DesignOptions, MapSrc, RedSrc),
        couch_db:get_update_seq(Db)).

get_group_info({DbName, GroupDbName}, GroupId) ->
    GroupPid = get_group_server({DbName, GroupDbName}, GroupId),
    couch_view_group:request_group_info(GroupPid);
get_group_info(#db{name = DbName}, GroupId) ->
    get_group_info(DbName, GroupId);
get_group_info(DbName, GroupId) ->
    couch_view_group:request_group_info(get_group_server(DbName, GroupId)).

cleanup_index_files(Db) ->
    % load all ddocs
    {ok, DesignDocs} = couch_db:get_design_docs(Db, no_deletes),
    Sigs = lists:map(fun couch_view_group:get_signature/1, DesignDocs),
    FileList = list_index_files(Db),

    DeleteFiles = case Sigs of
    [] ->
        FileList;
    _ ->
        % regex that matches all ddocs
        {ok, RegExp} = re:compile(["(", string:join(Sigs, "|"), ")"]),
        % filter out the ones in use
        [FilePath || FilePath <- FileList,
            re:run(FilePath, RegExp, [{capture, none}]) =:= nomatch]
    end,

    case DeleteFiles of
    [] ->
        ok;
    _ ->
        ?LOG_DEBUG("deleting unused view index files: ~p", [DeleteFiles]),
        RootDir = couch_config:get("couchdb", "view_index_dir"),
        [couch_file:delete(RootDir, File, false) || File <- DeleteFiles]
    end,
    ok.

list_index_files(Db) ->
    % call server to fetch the index files
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    filelib:wildcard(
        RootDir ++ "/." ++ ?b2l(couch_db:name(Db)) ++ "_design" ++ "/*.view").

get_row_count(#view{btree=Bt} = View) ->
    ok = couch_view_mapreduce:start_reduce_context(View),
    {ok, {Count, _Reds}} = couch_btree:full_reduce(Bt),
    ok = couch_view_mapreduce:end_reduce_context(View),
    {ok, Count}.

get_temp_reduce_view(Db, Language, DesignOptions, MapSrc, RedSrc) ->
    {ok, #group{views=[View]}=Group} =
        get_temp_group(Db, Language, DesignOptions, MapSrc, RedSrc),
    {ok, {temp_reduce, View}, Group}.


get_reduce_view(Db, GroupId, Name, Update) ->
    case get_group(Db, GroupId, Update) of
    {ok, #group{views=Views,def_lang=Lang}=Group} ->
        case get_reduce_view0(Name, Lang, Views) of
        {ok, View} ->
            {ok, View, Group};
        Else ->
            Else
        end;
    Error ->
        Error
    end.

get_reduce_view0(_Name, _Lang, []) ->
    {not_found, missing_named_view};
get_reduce_view0(Name, Lang, [#view{reduce_funs=RedFuns}=View|Rest]) ->
    case get_key_pos(Name, RedFuns, 0) of
        0 -> get_reduce_view0(Name, Lang, Rest);
        N -> {ok, {reduce, N, Lang, View}}
    end.

extract_map_view({reduce, _N, _Lang, View}) ->
    View.

detuple_kvs([], Acc) ->
    lists:reverse(Acc);
detuple_kvs([KV | Rest], Acc) ->
    {{Key,Id},Value} = KV,
    NKV = [[Key, Id], Value],
    detuple_kvs(Rest, [NKV | Acc]).

expand_dups([], Acc) ->
    lists:reverse(Acc);
expand_dups([{Key, {dups, Vals}} | Rest], Acc) ->
    Expanded = [{Key, Val} || Val <- Vals],
    expand_dups(Rest, Expanded ++ Acc);
expand_dups([KV | Rest], Acc) ->
    expand_dups(Rest, [KV | Acc]).

fold_reduce({temp_reduce, #view{btree=Bt}}, Fun, Acc, Options) ->
    WrapperFun = fun({GroupedKey, _}, PartialReds, Acc0) ->
            {_, [Red]} = couch_btree:final_reduce(Bt, PartialReds),
            Fun(GroupedKey, Red, Acc0)
        end,
    couch_btree:fold_reduce(Bt, WrapperFun, Acc, Options);

fold_reduce({reduce, NthRed, _Lang, View}, Fun, Acc, Options) ->
    #view{btree=Bt, reduce_funs=RedFuns} = View,
    PreResultPadding = lists:duplicate(NthRed - 1, []),
    PostResultPadding = lists:duplicate(length(RedFuns) - NthRed, []),
    couch_view_mapreduce:start_reduce_context(View),
    ReduceFun =
        fun(reduce, KVs) ->
            KVs2 = expand_dups(KVs, []),
            {ok, Reduced} = couch_view_mapreduce:reduce(View, NthRed, KVs2),
            {0, PreResultPadding ++ Reduced ++ PostResultPadding};
        (rereduce, Reds) ->
            UserReds = [[lists:nth(NthRed, UserRedsList)] || {_, UserRedsList} <- Reds],
            {ok, Reduced} = couch_view_mapreduce:rereduce(View, NthRed, UserReds),
            {0, PreResultPadding ++ Reduced ++ PostResultPadding}
        end,
    WrapperFun = fun({GroupedKey, _}, PartialReds, Acc0) ->
            {_, Reds} = couch_btree:final_reduce(ReduceFun, PartialReds),
            Fun(GroupedKey, lists:nth(NthRed, Reds), Acc0)
        end,
    try
        couch_btree:fold_reduce(Bt, WrapperFun, Acc, Options)
    after
        couch_view_mapreduce:end_reduce_context(View)
    end.

get_key_pos(_Key, [], _N) ->
    0;
get_key_pos(Key, [{Key1,_Value}|_], N) when Key == Key1 ->
    N + 1;
get_key_pos(Key, [_|Rest], N) ->
    get_key_pos(Key, Rest, N+1).


get_temp_map_view(Db, Language, DesignOptions, Src) ->
    {ok, #group{views=[View]}=Group} = get_temp_group(Db, Language, DesignOptions, Src, []),
    {ok, View, Group}.

get_map_view(Db, GroupId, Name, Stale) ->
    case get_group(Db, GroupId, Stale) of
    {ok, #group{views=Views}=Group} ->
        case get_map_view0(Name, Views) of
        {ok, View} ->
            {ok, View, Group};
        Else ->
            Else
        end;
    Error ->
        Error
    end.

get_map_view0(_Name, []) ->
    {not_found, missing_named_view};
get_map_view0(Name, [#view{map_names=MapNames}=View|Rest]) ->
    case lists:member(Name, MapNames) of
        true -> {ok, View};
        false -> get_map_view0(Name, Rest)
    end.

reduce_to_count(Reductions) ->
    {Count, _} =
    couch_btree:final_reduce(
        fun(reduce, KVs) ->
            Count = lists:sum(
                [case V of {dups, Vals} -> length(Vals); _ -> 1 end
                || {_,V} <- KVs]),
            {Count, []};
        (rereduce, Reds) ->
            {lists:sum([Count0 || {Count0, _} <- Reds]), []}
        end, Reductions),
    Count.



fold_fun(_Fun, [], _, Acc) ->
    {ok, Acc};
fold_fun(Fun, [KV|Rest], {KVReds, Reds}, Acc) ->
    case Fun(KV, {KVReds, Reds}, Acc) of
    {ok, Acc2} ->
        fold_fun(Fun, Rest, {[KV|KVReds], Reds}, Acc2);
    {stop, Acc2} ->
        {stop, Acc2}
    end.


fold(#view{btree=Btree}, Fun, Acc, Options) ->
    WrapperFun =
        fun(KV, Reds, Acc2) ->
            fold_fun(Fun, expand_dups([KV],[]), Reds, Acc2)
        end,
    {ok, _LastReduce, _AccResult} = couch_btree:fold(Btree, WrapperFun, Acc, Options).


init([]) ->
    % read configuration settings and register for configuration changes
    RootDir = couch_config:get("couchdb", "view_index_dir"),

    ets:new(couch_groups_by_db, [bag, protected, named_table]),
    ets:new(group_servers_by_sig, [set, protected, named_table]),
    ets:new(couch_groups_by_updater, [set, private, named_table]),

    couch_db_update_notifier:start_link(
        fun({deleted, DbName}) ->
            gen_server:cast(couch_view, {reset_indexes, DbName});
        ({created, DbName}) ->
            gen_server:cast(couch_view, {reset_indexes, DbName});
        ({ddoc_updated, {DbName, #doc{id = DDocId} = DDoc}}) ->
            EntriesWhereIsDataDb = ets:match_object(couch_groups_by_db, {{DbName, '_'}, {DDocId, '_'}}),
            EntriesWhereIsDDocDb = ets:match_object(couch_groups_by_db, {{'_', DbName}, {DDocId, '_'}}),
            DDocUpdateNotifyFun = fun({{DataDbName, DDocDbName}, {DDocId0, Sig}}) when DDocId0 == DDocId ->
                case ets:lookup(group_servers_by_sig, {DataDbName, DDocDbName, Sig}) of
                [{_, GroupPid}] ->
                    case DDoc#doc.deleted of
                    true ->
                        NewSig = <<>>;
                    false ->
                        DDoc2 = couch_doc:with_ejson_body(DDoc),
                        #group{sig = NewSig} = couch_view_group:design_doc_to_view_group(DDoc2)
                    end,
                    (catch gen_server:cast(GroupPid, {ddoc_updated, NewSig}));
                [] ->
                    ok
                end
            end,
            ok = lists:foreach(DDocUpdateNotifyFun, EntriesWhereIsDataDb),
            ok = lists:foreach(DDocUpdateNotifyFun, EntriesWhereIsDDocDb);
        (_Else) ->
            ok
        end),
    process_flag(trap_exit, true),
    ok = couch_file:init_delete_dir(RootDir),
    {ok, #server{root_dir=RootDir}}.


terminate(_Reason, _Srv) ->
    [couch_util:shutdown_sync(Pid) || {Pid, _} <-
            ets:tab2list(couch_groups_by_updater)],
    ok.


handle_call({get_group_server, DbName, Group}, From, #server{root_dir = Root} = Server) ->
    Key = {DbName, Group#group.ddoc_db_name, Group#group.sig},
    case ets:lookup(group_servers_by_sig, Key) of
    [] ->
        spawn_monitor(fun() -> new_group(Root, DbName, Group) end),
        ets:insert(group_servers_by_sig, {Key, [From]}),
        {noreply, Server};
    [{_, WaitList}] when is_list(WaitList) ->
        ets:insert(group_servers_by_sig, {Key, [From | WaitList]}),
        {noreply, Server};
    [{_, ExistingPid}] ->
        {reply, {ok, ExistingPid}, Server}
    end;

handle_call({reset_indexes, DbName}, _From, #server{root_dir=Root}=Server) ->
    do_reset_indexes(DbName, Root),
    {reply, ok, Server}.

handle_cast({reset_indexes, DbName}, #server{root_dir=Root}=Server) ->
    do_reset_indexes(DbName, Root),
    {noreply, Server}.

new_group(Root, DbName, #group{name=GroupId, sig=Sig, ddoc_db_name=DDocDbName} = Group) ->
    ?LOG_DEBUG("Spawning new group server for view group ~s in database ~s.",
        [GroupId, DbName]),
    case (catch couch_view_group:start_link({Root, DbName, Group})) of
    {ok, NewPid} ->
        unlink(NewPid),
        exit({DbName, DDocDbName, GroupId, Sig, {ok, NewPid}});
    {error, invalid_view_seq} ->
        ok = gen_server:call(couch_view, {reset_indexes, DbName}),
        new_group(Root, DbName, Group);
    Error ->
        exit({DbName, GroupId, Sig, Error})
    end.

do_reset_indexes(DbName, Root) ->
    % shutdown all the updaters and clear the files, the db got changed
    NamesWhereIsDataDb = ets:match_object(couch_groups_by_db, {{DbName, '_'}, '_'}),
    NamesWhereIsDDocDb = ets:match_object(couch_groups_by_db, {{'_', DbName}, '_'}),
    Fun = fun({{DataDbName, DDocDbName}, {DDocId, Sig}}) ->
        ?LOG_DEBUG("Killing update process for view group `~s`, signature `~s`, "
                   "data database `~s`, ddoc database `~s`",
                   [DDocId, couch_util:to_hex(Sig),
                    DataDbName, DDocDbName]),
        delete_index_dir(Root, DDocDbName),
        case ets:lookup(group_servers_by_sig, {DataDbName, DDocDbName, Sig}) of
        [{_, Pid}] when is_pid(Pid) ->
            couch_util:shutdown_sync(Pid),
            delete_from_ets(Pid, DataDbName, DDocDbName, DDocId, Sig);
        _ ->
            % Already shutdown in a previous iteration
            ok
        end
    end,
    ok = lists:foreach(Fun, NamesWhereIsDataDb),
    ok = lists:foreach(Fun, NamesWhereIsDDocDb),
    RootDelDir = couch_config:get("couchdb", "view_index_dir"),
    couch_file:delete(RootDelDir, Root ++ "/." ++ ?b2l(DbName) ++ "_temp").

handle_info({'EXIT', FromPid, Reason}, Server) ->
    case ets:lookup(couch_groups_by_updater, FromPid) of
    [] ->
        if Reason /= normal ->
            % non-updater linked process died, we propagate the error
            ?LOG_ERROR("Exit on non-updater process: ~p", [Reason]),
            exit(Reason);
        true -> ok
        end;
    [{_, {DbName, DDocDbName, Sig}}] ->
        [{{DbName, DDocDbName}, {DDocId, Sig}}] = ets:match_object(
            couch_groups_by_db, {{DbName, DDocDbName}, {'_', Sig}}),
        delete_from_ets(FromPid, DbName, DDocDbName, DDocId, Sig)
    end,
    {noreply, Server};

handle_info({'DOWN', _, _, _, {DbName, DDocDbName, DDocId, Sig, Reply}}, Server) ->
    [{_, WaitList}] = ets:lookup(group_servers_by_sig, {DbName, DDocDbName, Sig}),
    [gen_server:reply(From, Reply) || From <- WaitList],
    case Reply of {ok, NewPid} ->
        link(NewPid),
        add_to_ets(NewPid, DbName, DDocDbName, DDocId, Sig);
     _ -> ok end,
    {noreply, Server}.

add_to_ets(Pid, DbName, DDocDbName, DDocId, Sig) ->
    true = ets:insert(couch_groups_by_updater, {Pid, {DbName, DDocDbName, Sig}}),
    true = ets:insert(group_servers_by_sig, {{DbName, DDocDbName, Sig}, Pid}),
    true = ets:insert(couch_groups_by_db, {{DbName, DDocDbName}, {DDocId, Sig}}).

delete_from_ets(Pid, DbName, DDocDbName, DDocId, Sig) ->
    true = ets:delete(couch_groups_by_updater, Pid),
    true = ets:delete(group_servers_by_sig, {DbName, DDocDbName, Sig}),
    true = ets:delete_object(couch_groups_by_db, {{DbName, DDocDbName}, {DDocId, Sig}}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


delete_index_dir(RootDir, DbName) ->
    nuke_dir(RootDir, RootDir ++ "/." ++ ?b2l(DbName) ++ "_design").

nuke_dir(RootDelDir, Dir) ->
    case file:list_dir(Dir) of
    {error, enoent} -> ok; % doesn't exist
    {ok, Files} ->
        lists:foreach(
            fun(File)->
                Full = Dir ++ "/" ++ File,
                case couch_file:delete(RootDelDir, Full, false) of
                ok -> ok;
                % Directory doesn't exist
                {error, enoent} -> ok;
                {error, eperm} ->
                    ok = nuke_dir(RootDelDir, Full)
                end
            end,
            Files),
        case file:del_dir(Dir) of
        ok -> ok;
        % Directory doesn't exist (might have been deleted by some other
        % process already)
        {error, enoent} -> ok
        end
    end.


% keys come back in the language of btree - tuples.
less_json_ids({JsonA, IdA}, {JsonB, IdB}) ->
    case couch_ejson_compare:less(JsonA, JsonB) of
    0 ->
        IdA < IdB;
    Result ->
        Result < 0
    end.

less_json(A,B) ->
    couch_ejson_compare:less(A, B) < 0.
