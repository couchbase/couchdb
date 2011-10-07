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

-module(couch_set_view).
-behaviour(gen_server).

% public API
-export([start_link/0]).

-export([get_map_view/4, get_map_view/5, get_reduce_view/4, get_reduce_view/5]).
-export([get_group/3, get_group_pid/2, release_group/1, define_group/3]).
-export([get_group_info/2]).

-export([is_view_defined/2]).
-export([set_passive_partitions/3, set_active_partitions/3, set_cleanup_partitions/3]).

-export([fold/5, fold_reduce/5]).
-export([get_row_count/1, reduce_to_count/1, extract_map_view/1]).

-export([less_json/2, less_json_ids/2]).

% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

-include("couch_db.hrl").
-include("couch_set_view.hrl").


-record(server,{
    root_dir = []}).

% TODOs:
%
% 1) Adapt cleanup_index_files for set views
%
% 2) Make all this code more elegant and less verbose


% For a "set view" we have multiple databases which are indexed.
% The set has a name which is a prefix common to all source databases.
% Each database is designated as a "partition" and internally identified
% with an integer between 0 and N - 1 (N is total number of partitions).
% For example, if the set name is "myset", and the number of partitions
% is 4, then the "set view" indexer will index the following 4 databases:
%
%    "myset/0", "myset/1", "myset/2" and "myset/3"
%
% Not all paritions are necessarily indexed, so when the set view is created,
% the caller should specify not only the set name but also:
% 1) Total number of partitions
% 2) A list of active partition IDs
%
% Once a view is created, the caller can (via other APIs):
% 1) Change the list of active partitions (add or remove)
% 2) Add several "passive" partitions - these are partitions that are
%    indexed but whose results are not included in queries
% 3) Define a list of partitions to cleanup from the index. All
%    the view key/values that originated from any of these
%    partitions will eventually be removed from the index
%
get_group(SetName, DDocId, StaleType) ->
    GroupPid = get_group_pid(SetName, DDocId),
    case couch_set_view_group:request_group(GroupPid, StaleType) of
    {ok, Group} ->
        {ok, Group};
    view_undefined ->
        % caller must call ?MODULE:define_group/3
        throw(view_undefined);
    Error ->
        throw(Error)
    end.


get_group_pid(SetName, DDocId) ->
    get_group_server(SetName, open_set_group(SetName, DDocId)).


release_group(Group) ->
    couch_set_view_group:release_group(Group).


define_group(SetName, DDocId, #set_view_params{} = Params) ->
    GroupPid = get_group_pid(SetName, DDocId),
    ok = couch_set_view_group:define_view(GroupPid, Params).


is_view_defined(SetName, DDocId) ->
    GroupPid = get_group_pid(SetName, DDocId),
    couch_set_view_group:is_view_defined(GroupPid).


% Partitions list of partition IDs (e.g. [6, 9, 11])
set_passive_partitions(SetName, DDocId, Partitions) ->
    GroupPid = get_group_pid(SetName, DDocId),
    case couch_set_view_group:set_passive_partitions(GroupPid, Partitions) of
    ok ->
        ok;
    Error ->
        throw(Error)
    end.


% Partitions list of partition IDs (e.g. [6, 9, 11])
set_active_partitions(SetName, DDocId, Partitions) ->
    GroupPid = get_group_pid(SetName, DDocId),
    case couch_set_view_group:activate_partitions(GroupPid, Partitions) of
    ok ->
        ok;
    Error ->
        throw(Error)
    end.


% Partitions list of partition IDs (e.g. [6, 9, 11])
set_cleanup_partitions(SetName, DDocId, Partitions) ->
    GroupPid = get_group_pid(SetName, DDocId),
    case couch_set_view_group:cleanup_partitions(GroupPid, Partitions) of
    ok ->
        ok;
    Error ->
        throw(Error)
    end.


get_group_server(SetName, Group) ->
    case gen_server:call(?MODULE, {get_group_server, SetName, Group}, infinity) of
    {ok, Pid} ->
        Pid;
    Error ->
        throw(Error)
    end.


open_set_group(SetName, GroupId) ->
    case couch_set_view_group:open_set_group(SetName, GroupId) of
    {ok, Group} ->
        Group;
    Error ->
        throw(Error)
    end.


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


% TODO: fixme
get_group_info(#db{name = DbName}, GroupId) ->
    get_group_info(DbName, GroupId);
get_group_info(DbName, GroupId) ->
    couch_set_view_group:request_group_info(get_group_server(DbName, GroupId)).

cleanup_index_files(Db) ->
    % load all ddocs
    {ok, DesignDocs} = couch_db:get_design_docs(Db),

    % make unique list of group sigs
    Sigs = lists:map(fun(#doc{id = GroupId}) ->
        {ok, Info} = get_group_info(Db, GroupId),
        ?b2l(couch_util:get_value(signature, Info))
    end, [DD||DD <- DesignDocs, DD#doc.deleted == false]),

    FileList = list_index_files(Db),

    % regex that matches all ddocs
    RegExp = "("++ string:join(Sigs, "|") ++")",

    % filter out the ones in use
    DeleteFiles = [FilePath
           || FilePath <- FileList,
              re:run(FilePath, RegExp, [{capture, none}]) =:= nomatch],
    % delete unused files
    ?LOG_DEBUG("deleting unused view index files: ~p",[DeleteFiles]),
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    [couch_file:delete(RootDir,File,false)||File <- DeleteFiles],
    ok.

list_index_files(Db) ->
    % call server to fetch the index files
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    filelib:wildcard(RootDir ++ "/." ++ ?b2l(couch_db:name(Db)) ++ "_design"++"/*").


get_row_count(#set_view{btree=Bt}) ->
    {ok, {Count, _Reds, _AllPartitionsBitMaps}} = couch_btree:full_reduce(Bt),
    {ok, Count}.

extract_map_view({reduce, _N, _Lang, View}) ->
    View.

fold_reduce(Group, View, Fun, Acc, Options0) ->
    {reduce, NthRed, Lang, #set_view{btree = Bt, reduce_funs = RedFuns}} = View,
    Options = case (?set_pbitmask(Group) bor ?set_cbitmask(Group)) of
    0 ->
        Options0;
    _ ->
        ExcludeBitmask = ?set_pbitmask(Group) bor ?set_cbitmask(Group),
        FilterFun = fun(value, {_K, {dups, [{PartId, _} | _]}}) ->
            ((1 bsl PartId) band ?set_abitmask(Group)) =/= 0;
        (value, {_K, {PartId, _}}) ->
            ((1 bsl PartId) band ?set_abitmask(Group)) =/= 0;
        (branch, {_, _, PartsBitmap}) ->
            case PartsBitmap band ExcludeBitmask of
            0 ->
                all;
            _ ->
                case PartsBitmap bxor ExcludeBitmask of
                0 ->
                    none;
                _ ->
                    partial
                end
            end
        end,
        lists:keystore(filter_fun, 1, Options0, {filter_fun, FilterFun})
    end,
    PreResultPadding = lists:duplicate(NthRed - 1, []),
    PostResultPadding = lists:duplicate(length(RedFuns) - NthRed, []),
    {_Name, FunSrc} = lists:nth(NthRed,RedFuns),
    ReduceFun =
        fun(reduce, KVs) ->
            ExpandedKVs = couch_set_view_util:expand_dups(KVs, []),
            {ok, Reduced} = couch_query_servers:reduce(
                Lang,
                [FunSrc],
                couch_set_view_util:detuple_kvs(ExpandedKVs, [])),
            {0, PreResultPadding ++ Reduced ++ PostResultPadding, 0};
        (rereduce, Reds) ->
            UserReds = lists:map(
                fun({_, UserRedsList, _}) -> [lists:nth(NthRed, UserRedsList)] end,
                Reds),
            {ok, Reduced} = couch_query_servers:rereduce(Lang, [FunSrc], UserReds),
            {0, PreResultPadding ++ Reduced ++ PostResultPadding, 0}
        end,
    WrapperFun = fun({GroupedKey, _}, PartialReds, Acc0) ->
            {_, Reds, _} = couch_btree:final_reduce(ReduceFun, PartialReds),
            Fun(GroupedKey, lists:nth(NthRed, Reds), Acc0)
        end,
    couch_btree:fold_reduce(Bt, WrapperFun, Acc, Options).

get_key_pos(_Key, [], _N) ->
    0;
get_key_pos(Key, [{Key1,_Value}|_], N) when Key == Key1 ->
    N + 1;
get_key_pos(Key, [_|Rest], N) ->
    get_key_pos(Key, Rest, N+1).


get_map_view(SetName, DDocId, ViewName, StaleType) ->
    case get_map_view(SetName, DDocId, ViewName, StaleType, []) of
    {ok, View, Group, _} ->
        {ok, View, Group};
    Else ->
        Else
    end.

get_map_view(SetName, DDocId, ViewName, StaleType, FilterPartitions) ->
    {ok, Group0} = get_group(SetName, DDocId, StaleType),
    {Group, Unindexed} = modify_bitmasks(Group0, FilterPartitions),
    case get_map_view0(ViewName, Group#set_view_group.views) of
    {ok, View} ->
        {ok, View, Group, Unindexed};
    Else ->
        Else
    end.

get_map_view0(_Name, []) ->
    {not_found, missing_named_view};
get_map_view0(Name, [#set_view{map_names=MapNames}=View|Rest]) ->
    case lists:member(Name, MapNames) of
        true -> {ok, View};
        false -> get_map_view0(Name, Rest)
    end.

get_reduce_view(SetName, DDocId, ViewName, StaleType) ->
    case get_reduce_view(SetName, DDocId, ViewName, StaleType, []) of
    {ok, View, Group, _} ->
        {ok, View, Group};
    Else ->
        Else
    end.

get_reduce_view(SetName, DDocId, ViewName, StaleType, FilterPartitions) ->
    {ok, Group0} = get_group(SetName, DDocId, StaleType),
    {Group, Unindexed} = modify_bitmasks(Group0, FilterPartitions),
    #set_view_group{
        views = Views,
        def_lang = Lang
    } = Group,
    case get_reduce_view0(ViewName, Lang, Views) of
    {ok, View} ->
        {ok, View, Group, Unindexed};
    Else ->
        Else
    end.

get_reduce_view0(_Name, _Lang, []) ->
    {not_found, missing_named_view};
get_reduce_view0(Name, Lang, [#set_view{reduce_funs=RedFuns}=View|Rest]) ->
    case get_key_pos(Name, RedFuns, 0) of
        0 -> get_reduce_view0(Name, Lang, Rest);
        N -> {ok, {reduce, N, Lang, View}}
    end.


reduce_to_count(Reductions) ->
    {Count, _, _} =
    couch_btree:final_reduce(
        fun(reduce, KVs) ->
            Count = lists:sum(
                [case V of {dups, Vals} -> length(Vals); _ -> 1 end
                || {_,V} <- KVs]),
            {Count, [], 0};
        (rereduce, Reds) ->
            Count = lists:foldl(fun({C, _, _}, Acc) -> Acc + C end, 0, Reds),
            {Count, [], 0}
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

fold(Group, #set_view{btree=Btree}, Fun, Acc, Options) ->
    WrapperFun = case ?set_pbitmask(Group) bor ?set_cbitmask(Group) of
    0 ->
        fun(KV, Reds, Acc2) ->
            ExpandedKVs = couch_set_view_util:expand_dups([KV], []),
            fold_fun(Fun, ExpandedKVs, Reds, Acc2)
        end;
    _ ->
        fun(KV, Reds, Acc2) ->
            ExpandedKVs = couch_set_view_util:expand_dups([KV], ?set_abitmask(Group), []),
            fold_fun(Fun, ExpandedKVs, Reds, Acc2)
        end
    end,
    {ok, _LastReduce, _AccResult} = couch_btree:fold(Btree, WrapperFun, Acc, Options).


init([]) ->
    % read configuration settings and register for configuration changes
    RootDir = couch_config:get("couchdb", "view_index_dir"),
    Self = self(),
    ok = couch_config:register(
        fun("couchdb", "view_index_dir")->
            exit(Self, config_change)
        end),

    % {SetName, Signature}
    ets:new(couch_setview_name_to_sig, [bag, private, named_table]),
    % {{SetName, Signature}, Pid | WaitListPids}
    ets:new(couch_sig_to_setview_pid, [set, protected, named_table]),
    % {Pid, {SetName, Sig}}
    ets:new(couch_pid_to_setview_sig, [set, private, named_table]),

    couch_db_update_notifier:start_link(
        fun({deleted, DbName}) ->
            ok = gen_server:cast(?MODULE, {reset_indexes, DbName});
        ({created, _DbName}) ->
            % TODO: deal with this
            % ok = gen_server:cast(?MODULE, {reset_indexes, DbName});
            ok;
        (_Else) ->
            ok
        end),

    process_flag(trap_exit, true),
    ok = couch_file:init_delete_dir(RootDir),
    {ok, #server{root_dir=RootDir}}.


terminate(_Reason, _Srv) ->
    [couch_util:shutdown_sync(Pid) || {Pid, _} <-
            ets:tab2list(couch_pid_to_setview_sig)],
    ok.


handle_call({get_group_server, SetName, #set_view_group{sig=Sig}=Group}, From,
    #server{root_dir=Root}=Server) ->
    case ets:lookup(couch_sig_to_setview_pid, {SetName, Sig}) of
    [] ->
        spawn_monitor(fun() -> new_group(Root, SetName, Group) end),
        ets:insert(couch_sig_to_setview_pid, {{SetName, Sig}, [From]}),
        {noreply, Server};
    [{_, WaitList}] when is_list(WaitList) ->
        ets:insert(couch_sig_to_setview_pid, {{SetName, Sig}, [From | WaitList]}),
        {noreply, Server};
    [{_, ExistingPid}] ->
        {reply, {ok, ExistingPid}, Server}
    end.

handle_cast({reset_indexes, DbName}, #server{root_dir=Root}=Server) ->
    maybe_reset_indexes(DbName, Root),
    {noreply, Server}.

new_group(Root, SetName, #set_view_group{name=GroupId, sig=Sig} = Group) ->
    ?LOG_DEBUG("Spawning new group server for view group ~s, set ~s.",
        [GroupId, SetName]),
    case (catch couch_set_view_group:start_link({Root, SetName, Group})) of
    {ok, NewPid} ->
        unlink(NewPid),
        exit({SetName, Sig, {ok, NewPid}});
    Error ->
        exit({SetName, Sig, Error})
    end.

handle_info({'EXIT', FromPid, Reason}, Server) ->
    case ets:lookup(couch_pid_to_setview_sig, FromPid) of
    [] ->
        if Reason /= normal ->
            % non-updater linked process died, we propagate the error
            ?LOG_ERROR("Exit on non-updater process: ~p", [Reason]),
            exit(Reason);
        true -> ok
        end;
    [{_, {SetName, GroupId}}] ->
        delete_from_ets(FromPid, SetName, GroupId)
    end,
    {noreply, Server};

handle_info({'DOWN', _, _, _, {SetName, Sig, Reply}}, Server) ->
    [{_, WaitList}] = ets:lookup(couch_sig_to_setview_pid, {SetName, Sig}),
    [gen_server:reply(From, Reply) || From <- WaitList],
    case Reply of {ok, NewPid} ->
        link(NewPid),
        add_to_ets(NewPid, SetName, Sig);
     _ -> ok end,
    {noreply, Server}.

add_to_ets(Pid, SetName, Sig) ->
    true = ets:insert(couch_pid_to_setview_sig, {Pid, {SetName, Sig}}),
    true = ets:insert(couch_sig_to_setview_pid, {{SetName, Sig}, Pid}),
    true = ets:insert(couch_setview_name_to_sig, {SetName, Sig}).

delete_from_ets(Pid, SetName, Sig) ->
    true = ets:delete(couch_pid_to_setview_sig, Pid),
    true = ets:delete(couch_sig_to_setview_pid, {SetName, Sig}),
    true = ets:delete_object(couch_setview_name_to_sig, {SetName, Sig}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


delete_index_dir(RootDir, SetName) ->
    nuke_dir(RootDir, RootDir ++ "/set_view_" ++ ?b2l(SetName) ++ "_design/").

nuke_dir(RootDelDir, Dir) ->
    case file:list_dir(Dir) of
    {error, enoent} -> ok; % doesn't exist
    {ok, Files} ->
        lists:foreach(
            fun(File)->
                Full = Dir ++ "/" ++ File,
                case couch_file:delete(RootDelDir, Full, false) of
                ok -> ok;
                {error, eperm} ->
                    ok = nuke_dir(RootDelDir, Full)
                end
            end,
            Files),
        ok = file:del_dir(Dir)
    end.

maybe_reset_indexes(DbName, Root) ->
    case string:tokens(?b2l(DbName), "/") of
    [SetName0, "master"] ->
        SetName = ?l2b(SetName0),
        lists:foreach(
            fun({_SetName, Sig} = Key) ->
                [{_, Pid}] = ets:lookup(couch_sig_to_setview_pid, Key),
                couch_util:shutdown_sync(Pid),
                delete_from_ets(Pid, SetName, Sig)
            end,
            ets:lookup(couch_setview_name_to_sig, SetName)),
        delete_index_dir(Root, SetName);
    [SetName0, PartId0] ->
        SetName = ?l2b(SetName0),
        case (catch list_to_integer(PartId0)) of
        PartId when is_number(PartId) ->
            delete_index_dir(Root, SetName);
        _ ->
            ok
        end;
    _ ->
        nil
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


modify_bitmasks(Group, []) ->
    {Group, []};
modify_bitmasks(Group, Partitions) ->
    IndexedBitmask = ?set_abitmask(Group) bor ?set_pbitmask(Group),
    WantedBitmask = couch_set_view_util:build_bitmask(Partitions),
    UnindexedBitmask = WantedBitmask band (bnot IndexedBitmask),
    ABitmask2 = WantedBitmask band IndexedBitmask,
    PBitmask2 = (bnot ABitmask2) band IndexedBitmask,
    Header = (Group#set_view_group.index_header)#set_view_index_header{
        abitmask = ABitmask2 band (bnot ?set_cbitmask(Group)),
        pbitmask = PBitmask2
    },
    Unindexed = couch_set_view_util:decode_bitmask(UnindexedBitmask),
    {Group#set_view_group{index_header = Header}, Unindexed}.
