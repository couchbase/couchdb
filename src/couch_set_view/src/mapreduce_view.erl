% -*- Mode: Erlang; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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

-module(mapreduce_view).

% For the updater
-export([write_kvs/3, finish_build/3, get_state/1, set_state/2, view_name/2,
         convert_primary_index_kvs_to_binary/3, view_bitmap/1]).
-export([encode_key_docid/2, decode_key_docid/1]).
-export([convert_back_index_kvs_to_binary/2]).
-export([view_insert_doc_query_results/6]).
% For the group
-export([design_doc_to_set_view_group/2, view_group_data_size/2,
         reset_view/1, setup_views/5]).
% For the utils
-export([cleanup_view_group/1]).
% For the main module
-export([get_row_count/1, make_wrapper_fun/2, fold/4, index_extension/0,
        make_key_options/1, should_filter/1, query_args_view_name/1]).
-export([stats_ets/1, server_name/1, sig_to_pid_ets/1, name_to_sig_ets/1,
         pid_to_sig_ets/1]).
-export([view_info/1]).


-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").

% Same as in couch_btree.erl
-define(KEY_BITS,       12).
-define(VALUE_BITS,     28).
-define(MAX_KEY_SIZE,   ((1 bsl ?KEY_BITS) - 1)).

% View specific limits.
-define(VIEW_SINGLE_VALUE_BITS,     24).
-define(VIEW_ALL_VALUES_BITS,       ?VALUE_BITS).
-define(MAX_VIEW_SINGLE_VALUE_SIZE, ((1 bsl ?VIEW_SINGLE_VALUE_BITS) - 1)).
-define(MAX_VIEW_ALL_VALUES_SIZE,   ((1 bsl ?VIEW_ALL_VALUES_BITS) - 1)).

% Same as the compactor uses for the ID-btree
-define(SORTED_CHUNK_SIZE, 1024 * 1024).


-define(SET_VIEW_STATS_ETS_PROD, couch_set_view_stats_prod).
-define(SET_VIEW_NAME_TO_SIG_ETS_PROD, couch_set_view_name_to_sig_prod).
-define(SET_VIEW_SIG_TO_PID_ETS_PROD, couch_set_view_sig_to_pid_prod).
-define(SET_VIEW_PID_TO_SIG_ETS_PROD, couch_set_view_pid_to_sig_prod).
-define(SET_VIEW_STATS_ETS_DEV, couch_set_view_stats_dev).
-define(SET_VIEW_NAME_TO_SIG_ETS_DEV, couch_set_view_name_to_sig_dev).
-define(SET_VIEW_SIG_TO_PID_ETS_DEV, couch_set_view_sig_to_pid_dev).
-define(SET_VIEW_PID_TO_SIG_ETS_DEV, couch_set_view_pid_to_sig_dev).

-define(SET_VIEW_SERVER_NAME_PROD, couch_setview_server_name_prod).
-define(SET_VIEW_SERVER_NAME_DEV, couch_setview_server_name_dev).


write_kvs(Group, TmpFiles, ViewKVs) ->
    KVCount = lists:foldl(
        fun({#set_view{id_num = Id}, KvList}, AccCount) ->
            #set_view_tmp_file_info{fd = ViewFd} = dict:fetch(Id, TmpFiles),
            KvBins = convert_primary_index_kvs_to_binary(KvList, Group, []),
            ViewRecords = lists:foldr(
                fun({KeyBin, ValBin}, Acc) ->
                    KvBin = [<<(byte_size(KeyBin)):16>>, KeyBin, ValBin],
                    [[<<(iolist_size(KvBin)):32/native>>, KvBin] | Acc]
                end,
                [], KvBins),
            ok = file:write(ViewFd, ViewRecords),
            AccCount + length(KvBins)
        end,
        0, ViewKVs),
    {KVCount, TmpFiles}.


convert_primary_index_kvs_to_binary([], _Group, Acc) ->
    lists:reverse(Acc);
convert_primary_index_kvs_to_binary([{{Key, DocId}, {PartId, V0}} | Rest], Group, Acc)->
    KeyBin = encode_key_docid(Key, DocId),
    couch_set_view_util:check_primary_key_size(
        KeyBin, ?MAX_KEY_SIZE, Key, DocId, Group),
    V = case V0 of
    {dups, Values} ->
        ValueListBinary = lists:foldl(
            fun(V, Acc2) ->
                couch_set_view_util:check_primary_value_size(
                    V, ?MAX_VIEW_SINGLE_VALUE_SIZE, Key, DocId, Group),
                <<Acc2/binary, (byte_size(V)):?VIEW_SINGLE_VALUE_BITS, V/binary>>
            end,
            <<>>, Values),
        <<PartId:16, ValueListBinary/binary>>;
    _ ->
        couch_set_view_util:check_primary_value_size(
            V0, ?MAX_VIEW_SINGLE_VALUE_SIZE, Key, DocId, Group),
        <<PartId:16, (byte_size(V0)):?VIEW_SINGLE_VALUE_BITS, V0/binary>>
    end,
    couch_set_view_util:check_primary_value_size(
        V, ?MAX_VIEW_ALL_VALUES_SIZE, Key, DocId, Group),
    convert_primary_index_kvs_to_binary(Rest, Group, [{KeyBin, V} | Acc]).


-spec finish_build(#set_view_group{}, dict:dict(), string()) ->
                          {#set_view_group{}, pid()}.
finish_build(Group, TmpFiles, TmpDir) ->
    #set_view_group{
        sig = Sig,
        id_btree = IdBtree,
        views = Views
    } = Group,

    case os:find_executable("couch_view_index_builder") of
    false ->
        Cmd = nil,
        throw(<<"couch_view_index_builder command not found">>);
    Cmd ->
        ok
    end,
    Options = [exit_status, use_stdio, stderr_to_stdout, {line, 4096}, binary],
    Port = open_port({spawn_executable, Cmd}, Options),
    couch_set_view_util:send_group_info(Group, Port),
    true = port_command(Port, [TmpDir, $\n]),
    #set_view_tmp_file_info{name = IdFile} = dict:fetch(ids_index, TmpFiles),
    DestPath = couch_set_view_util:new_sort_file_path(TmpDir, updater),
    true = port_command(Port, [DestPath, $\n, IdFile, $\n]),
    lists:foreach(
        fun(#set_view{id_num = Id}) ->
            #set_view_tmp_file_info{
                name = ViewFile
            } = dict:fetch(Id, TmpFiles),
            true = port_command(Port, [ViewFile, $\n])
        end,
        Views),

    try
        couch_set_view_updater_helper:index_builder_wait_loop(Port, Group, [])
    after
        catch port_close(Port)
    end,

    {ok, NewFd} = couch_file:open(DestPath),
    unlink(NewFd),
    {ok, HeaderBin, NewHeaderPos} = couch_file:read_header_bin(NewFd),
    HeaderSig = couch_set_view_util:header_bin_sig(HeaderBin),
    case HeaderSig == Sig of
    true ->
        ok;
    false ->
        couch_file:close(NewFd),
        ok = file2:delete(DestPath),
        throw({error, <<"Corrupted initial build destination file.\n">>})
    end,
    NewHeader = couch_set_view_util:header_bin_to_term(HeaderBin),
    #set_view_index_header{
        id_btree_state = NewIdBtreeRoot,
        view_states = NewViewRoots
    } = NewHeader,

    NewIdBtree = couch_btree:set_state(IdBtree#btree{fd = NewFd}, NewIdBtreeRoot),
    NewViews = lists:zipwith(
        fun(#set_view{indexer = View} = V, NewRoot) ->
            #mapreduce_view{btree = Bt} = View,
            NewBt = couch_btree:set_state(Bt#btree{fd = NewFd}, NewRoot),
            NewView = View#mapreduce_view{btree = NewBt},
            V#set_view{indexer = NewView}
        end,
        Views, NewViewRoots),

    NewGroup = Group#set_view_group{
        id_btree = NewIdBtree,
        views = NewViews,
        index_header = NewHeader,
        header_pos = NewHeaderPos
    },
    NewGroup2 = couch_set_view_group:remove_duplicate_partitions(NewGroup),
    {NewGroup2, NewFd}.


% Return the state of a view (which will be stored in the header)
get_state(View) ->
    couch_btree:get_state(View#mapreduce_view.btree).

set_state(View, State) ->
    Btree = couch_btree:set_state(View#mapreduce_view.btree, State),
    View#mapreduce_view{btree = Btree}.


view_bitmap(View) ->
    {ok, <<_Size:40, Bm:?MAX_NUM_PARTITIONS, _/binary>>} =
        couch_btree:full_reduce(View#mapreduce_view.btree),
    Bm.


view_name(#set_view_group{views = SetViews}, ViewPos) ->
    View = (lists:nth(ViewPos, SetViews))#set_view.indexer,
    case View#mapreduce_view.map_names of
    [] ->
        [{Name, _} | _] = View#mapreduce_view.reduce_funs;
    [Name | _] ->
        ok
    end,
    Name.


-spec design_doc_to_set_view_group(binary(), #doc{}) -> #set_view_group{}.
design_doc_to_set_view_group(SetName, #doc{id = Id, body = {Fields}}) ->
    {DesignOptions} = couch_util:get_value(<<"options">>, Fields, {[]}),
    {RawViews} = couch_util:get_value(<<"views">>, Fields, {[]}),
    % add the views to a dictionary object, with the map source as the key
    DictBySrc =
    lists:foldl(
        fun({Name, {MRFuns}}, DictBySrcAcc) ->
            case couch_util:get_value(<<"map">>, MRFuns) of
            undefined -> DictBySrcAcc;
            MapSrc ->
                RedSrc = couch_util:get_value(<<"reduce">>, MRFuns, null),
                {ViewOptions} = couch_util:get_value(<<"options">>, MRFuns, {[]}),
                SetView =
                case dict:find({MapSrc, ViewOptions}, DictBySrcAcc) of
                {ok, SetView0} ->
                    SetView0;
                error ->
                   #set_view{
                        def = MapSrc,
                        indexer = #mapreduce_view{
                            options = ViewOptions
                        }
                    }
                end,
                View = SetView#set_view.indexer,
                View2 =
                if RedSrc == null ->
                    View#mapreduce_view{
                        map_names = [Name | View#mapreduce_view.map_names]};
                true ->
                    View#mapreduce_view{
                        reduce_funs = [{Name, RedSrc} |
                            View#mapreduce_view.reduce_funs]}
                end,
                dict:store({MapSrc, ViewOptions},
                    SetView#set_view{indexer = View2}, DictBySrcAcc)
            end
        end, dict:new(), RawViews),
    % number the views
    {SetViews, _N} = lists:mapfoldl(
        fun({_Src, SetView}, N) ->
            {SetView#set_view{id_num = N}, N + 1}
        end,
        0, lists:sort(dict:to_list(DictBySrc))),
    IndexXATTRonDeletedDocs = couch_util:get_value(<<"index_xattr_on_deleted_docs">>, Fields, false),
    SetViewGroup = #set_view_group{
        set_name = SetName,
        name = Id,
        views = SetViews,
        design_options = DesignOptions,
        mod = ?MODULE,
        extension = index_extension(),
        index_xattr_on_deleted_docs = IndexXATTRonDeletedDocs
    },
    set_view_sig(SetViewGroup).


% Couchbase 2.x has a different #set_view{} record. Though it is used
% for calculation the signature. Transform the new one into the old
% one so that the signature stays the same.
-spec set_view_for_sig(#set_view{}) -> tuple().
set_view_for_sig(SetView) ->
    #set_view{
        id_num = Id,
        def = Def,
        indexer = #mapreduce_view{
            map_names = MapNames,
            reduce_funs = ReduceFuns,
            options = Options
        }
    } = SetView,
    Btree = nil,
    Ref = undefined,
    {set_view, Id, MapNames, Def, Btree, ReduceFuns, Options, Ref}.


-spec set_view_sig(#set_view_group{}) -> #set_view_group{}.
set_view_sig(#set_view_group{views = Views} = Group) ->
    SetViews2 = [set_view_for_sig(SetView) || SetView <- Views],
    Sig = couch_util:md5(term_to_binary(SetViews2)),
    Group#set_view_group{sig = Sig}.


-spec index_extension() -> string().
index_extension() ->
    ".view".


-spec view_group_data_size(#btree{}, [#set_view{}]) -> non_neg_integer().
view_group_data_size(IdBtree, Views) ->
    lists:foldl(
        fun(SetView, Acc) ->
            Btree = (SetView#set_view.indexer)#mapreduce_view.btree,
            Acc + couch_btree:size(Btree)
        end,
        couch_btree:size(IdBtree),
        Views).


reset_view(View) ->
    View#mapreduce_view{btree = nil}.


setup_views(Fd, BtreeOptions, Group, ViewStates, Views) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = Type
    } = Group,
    lists:zipwith(fun(BTState, SetView) ->
        View = SetView#set_view.indexer,
        case View#mapreduce_view.reduce_funs of
        [{ViewName, _} | _] ->
            ok;
        [] ->
            [ViewName | _] = View#mapreduce_view.map_names
        end,
        ReduceFun =
            fun(reduce, KVs) ->
                AllPartitionsBitMap = couch_set_view_util:partitions_map(KVs, 0),
                KVs2 = couch_set_view_util:expand_dups(KVs, []),
                {ok, Reduced} =
                    try
                         couch_set_view_mapreduce:reduce(SetView, KVs2)
                    catch throw:{error, Reason} = Error ->
                        PrettyKVs = [
                            begin
                                {KeyDocId, <<_PartId:16, Value/binary>>} = RawKV,
                                {decode_key_docid(KeyDocId), Value}
                            end
                            || RawKV <- KVs2
                        ],
                        ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, error executing"
                                             " reduce function for view `~s'~n"
                                             "  reason:                ~s~n"
                                             "  input key-value pairs: ~s~n",
                                             [?LOG_USERDATA(SetName), Type, ?LOG_USERDATA(DDocId),
                                              ?LOG_USERDATA(ViewName), couch_util:to_binary(Reason),
                                              ?LOG_USERDATA(PrettyKVs)]),
                        throw(Error)
                    end,
                if length(Reduced) > 255 ->
                    throw({too_many_reductions, <<"Maximum reductions allowed is 255">>});
                true -> ok
                end,
                UserReductions = encode_reductions(Reduced),
                iolist_to_binary([<<(length(KVs2)):40, AllPartitionsBitMap:?MAX_NUM_PARTITIONS>> | UserReductions]);
            (rereduce, [<<Count0:40, AllPartitionsBitMap0:?MAX_NUM_PARTITIONS, Red0/binary>> | Reds]) ->
                {Count, AllPartitionsBitMap, UserReds} = lists:foldl(
                    fun(<<C:40, Apbm:?MAX_NUM_PARTITIONS, R/binary>>, {CountAcc, ApbmAcc, RedAcc}) ->
                        {C + CountAcc, Apbm bor ApbmAcc, [couch_set_view_util:parse_reductions(R) | RedAcc]}
                    end,
                    {Count0, AllPartitionsBitMap0, [couch_set_view_util:parse_reductions(Red0)]},
                    Reds),
                {ok, Reduced} =
                    try
                        couch_set_view_mapreduce:rereduce(SetView, UserReds)
                    catch throw:{error, Reason} = Error ->
                        ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, error executing"
                                             " rereduce function for view `~s'~n"
                                             "  reason:           ~s~n"
                                             "  input reductions: ~s~n",
                                             [?LOG_USERDATA(SetName), Type, ?LOG_USERDATA(DDocId),
                                              ?LOG_USERDATA(ViewName), couch_util:to_binary(Reason),
                                              ?LOG_USERDATA(UserReds)]),
                        throw(Error)
                    end,
                UserReductions = encode_reductions(Reduced),
                iolist_to_binary([<<Count:40, AllPartitionsBitMap:?MAX_NUM_PARTITIONS>> | UserReductions])
            end,
        Less = fun(A, B) ->
            {Key1, DocId1} = decode_key_docid(A),
            {Key2, DocId2} = decode_key_docid(B),
            case couch_ejson_compare:less_json(Key1, Key2) of
            0 ->
                DocId1 < DocId2;
            LessResult ->
                LessResult < 0
            end
        end,
        {ok, Btree} = couch_btree:open(
            BTState, Fd, [{less, Less}, {reduce, ReduceFun} | BtreeOptions]),
        SetView#set_view{
            indexer = View#mapreduce_view{btree = Btree}
        }
    end,
    ViewStates, Views).


% Native viewgroup cleanup
cleanup_view_group(Group) ->
    case os:find_executable("couch_view_group_cleanup") of
    false ->
        Cmd = nil,
        throw(<<"couch_view_group_cleanup command not found">>);
    Cmd ->
        ok
    end,
    Options = [exit_status, use_stdio, stderr_to_stdout, {line, 4096}, binary],
    Port = open_port({spawn_executable, Cmd}, Options),
    couch_set_view_util:send_group_info(Group, Port),
    PurgedCount = try cleanup_view_group_wait_loop(Port, Group, [], 0) of
    {ok, Count0} ->
        Count0
    catch
    Error ->
        exit(Error)
    after
        catch port_close(Port)
    end,
    {ok, Group, PurgedCount}.

cleanup_view_group_wait_loop(Port, Group, Acc, PurgedCount) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = Type
    } = Group,
    receive
    {Port, {exit_status, 0}} ->
        {ok, PurgedCount};
    {Port, {exit_status, 1}} ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, index cleaner stopped successfully.",
                   [?LOG_USERDATA(SetName), Type, ?LOG_USERDATA(DDocId)]),
        throw(stopped);
    {Port, {exit_status, Status}} ->
        throw({view_group_cleanup_exit, Status});
    {Port, {data, {noeol, Data}}} ->
        cleanup_view_group_wait_loop(Port, Group, [Data | Acc], PurgedCount);
    {Port, {data, {eol, <<"PurgedCount ", Data/binary>>}}} ->
        {Count,[]} = string:to_integer(erlang:binary_to_list(Data)),
        cleanup_view_group_wait_loop(Port, Group, Acc, Count);
    {Port, {data, {eol, Data}}} ->
        Msg = lists:reverse([Data | Acc]),
        ?LOG_ERROR("Set view `~s`, ~s group `~s`, received error from index cleanup: ~s",
                   [?LOG_USERDATA(SetName), Type, ?LOG_USERDATA(DDocId), Msg]),
        cleanup_view_group_wait_loop(Port, Group, [], PurgedCount);
    {Port, Error} ->
        throw({view_group_cleanup_error, Error});
    stop ->
        ?LOG_INFO("Set view `~s`, ~s group `~s`, sending stop message to index cleaner.",
                   [?LOG_USERDATA(SetName), Type, ?LOG_USERDATA(DDocId)]),
        port_command(Port, "exit"),
        cleanup_view_group_wait_loop(Port, Group, Acc, PurgedCount)
    end.

-spec get_row_count(#set_view{}) -> non_neg_integer().
get_row_count(SetView) ->
    Bt = (SetView#set_view.indexer)#mapreduce_view.btree,
    {ok, <<Count:40, _/binary>>} = couch_btree:full_reduce(Bt),
    Count.


make_wrapper_fun(Fun, Filter) ->
    case Filter of
    false ->
        fun(KV, Reds, Acc2) ->
            ExpandedKVs = couch_set_view_util:expand_dups([KV], []),
            fold_fun(Fun, ExpandedKVs, Reds, Acc2)
        end;
    {true, _, IncludeBitmask} ->
        fun(KV, Reds, Acc2) ->
            ExpandedKVs = couch_set_view_util:expand_dups([KV], IncludeBitmask, []),
            fold_fun(Fun, ExpandedKVs, Reds, Acc2)
        end
    end.


fold_fun(_Fun, [], _, Acc) ->
    {ok, Acc};
fold_fun(Fun, [KV | Rest], {KVReds, Reds}, Acc) ->
    {KeyDocId, <<PartId:16, Value/binary>>} = KV,
    {JsonKey, DocId} = decode_key_docid(KeyDocId),
    case Fun({{{json, JsonKey}, DocId}, {PartId, {json, Value}}}, {KVReds, Reds}, Acc) of
    {ok, Acc2} ->
        fold_fun(Fun, Rest, {[KV | KVReds], Reds}, Acc2);
    {stop, Acc2} ->
        {stop, Acc2}
    end.


fold(View, WrapperFun, Acc, Options) ->
    Bt = View#mapreduce_view.btree,
    couch_btree:fold(Bt, WrapperFun, Acc, Options).


-spec encode_reductions([binary()]) -> [binary()].
encode_reductions(Reduced) ->
    [
     begin
         RedSz = byte_size(R),
         case RedSz > ?MAX_USER_REDUCTION_SIZE of
         true ->
             ErrMsg = io_lib:format(
                        "Reduction too large (~p bytes)", [RedSz]),
             throw({error, iolist_to_binary(ErrMsg)});
         false ->
             <<RedSz:?USER_REDUCTION_SIZE_BITS, R/binary>>
         end
     end || R <- Reduced
    ].


-spec make_key_options(#view_query_args{}) -> [{atom(), term()}].
make_key_options(#view_query_args{direction = Dir} = QArgs) ->
    [{dir, Dir} | make_start_key_option(QArgs) ++ make_end_key_option(QArgs)].

make_start_key_option(#view_query_args{start_key = Key, start_docid = DocId}) ->
    if Key == undefined ->
        [];
    true ->
        [{start_key, encode_key_docid(?JSON_ENCODE(Key), DocId)}]
    end.

make_end_key_option(#view_query_args{end_key = undefined}) ->
    [];
make_end_key_option(#view_query_args{end_key = Key, end_docid = DocId, inclusive_end = true}) ->
    [{end_key, encode_key_docid(?JSON_ENCODE(Key), DocId)}];
make_end_key_option(#view_query_args{end_key = Key, end_docid = DocId,
        inclusive_end = false}) ->
    [{end_key_gt, encode_key_docid(?JSON_ENCODE(Key),
        reverse_key_default(DocId))}].

reverse_key_default(?MIN_STR) -> ?MAX_STR;
reverse_key_default(?MAX_STR) -> ?MIN_STR;
reverse_key_default(Key) -> Key.


-spec should_filter(#view_query_args{}) -> boolean().
should_filter(ViewQueryArgs) ->
    ViewQueryArgs#view_query_args.filter.


stats_ets(prod) ->
    ?SET_VIEW_STATS_ETS_PROD;
stats_ets(dev) ->
    ?SET_VIEW_STATS_ETS_DEV.

server_name(prod) ->
    ?SET_VIEW_SERVER_NAME_PROD;
server_name(dev) ->
    ?SET_VIEW_SERVER_NAME_DEV.

sig_to_pid_ets(prod) ->
    ?SET_VIEW_SIG_TO_PID_ETS_PROD;
sig_to_pid_ets(dev) ->
    ?SET_VIEW_SIG_TO_PID_ETS_DEV.

name_to_sig_ets(prod) ->
    ?SET_VIEW_NAME_TO_SIG_ETS_PROD;
name_to_sig_ets(dev) ->
    ?SET_VIEW_NAME_TO_SIG_ETS_DEV.

pid_to_sig_ets(prod) ->
    ?SET_VIEW_PID_TO_SIG_ETS_PROD;
pid_to_sig_ets(dev) ->
    ?SET_VIEW_PID_TO_SIG_ETS_DEV.

view_info(#mapreduce_view{reduce_funs = []}) ->
    [<<"0">>, $\n];
view_info(#mapreduce_view{reduce_funs = Funs}) ->
    Prefix = [integer_to_list(length(Funs)), $\n],
    Acc2 = lists:foldr(
        fun({Name, RedFun}, Acc) ->
            [Name, $\n, integer_to_list(byte_size(RedFun)), $\n, RedFun | Acc]
        end,
        [], Funs),
    [Prefix | Acc2].


-spec decode_key_docid(binary()) -> {binary(), binary()}.
decode_key_docid(<<KeyLen:16, JsonKey:KeyLen/binary, DocId/binary>>) ->
    {JsonKey, DocId}.


-spec encode_key_docid(binary(), binary()) -> binary().
encode_key_docid(JsonKey, DocId) ->
    <<(byte_size(JsonKey)):16, JsonKey/binary, DocId/binary>>.


convert_back_index_kvs_to_binary([], Acc)->
    lists:reverse(Acc);
convert_back_index_kvs_to_binary([{DocId, {PartId, ViewIdKeys}} | Rest], Acc) ->
    ViewIdKeysBinary = lists:foldl(
        fun({ViewId, Keys}, Acc2) ->
            KeyListBinary = lists:foldl(
                fun(Key, AccKeys) ->
                    <<AccKeys/binary, (byte_size(Key)):16, Key/binary>>
                end,
                <<>>, Keys),
            NumKeys = length(Keys),
            case NumKeys >= (1 bsl 16) of
            true ->
                ErrorMsg = io_lib:format(
                    "Too many (~p) keys emitted for "
                    "document `~s` (maximum allowed is ~p",
                    [NumKeys, ?LOG_USERDATA(DocId), (1 bsl 16) - 1]),
                throw({error, iolist_to_binary(ErrorMsg)});
            false ->
                ok
            end,
            <<Acc2/binary, ViewId:8, NumKeys:16, KeyListBinary/binary>>
        end,
        <<>>, ViewIdKeys),
    KvBin = {<<PartId:16, DocId/binary>>,
        <<PartId:16, ViewIdKeysBinary/binary>>},
    convert_back_index_kvs_to_binary(Rest, [KvBin | Acc]).


-spec view_insert_doc_query_results(
        binary(), partition_id(), [set_view_key_value()],
        [set_view_key_value()], [set_view_key_value()], [set_view_key()]) ->
            {[set_view_key_value()], [set_view_key()]}.
view_insert_doc_query_results(_DocId, _PartitionId, [], [], ViewKVsAcc,
        ViewIdKeysAcc) ->
    {lists:reverse(ViewKVsAcc), lists:reverse(ViewIdKeysAcc)};
view_insert_doc_query_results(DocId, PartitionId, [ResultKVs | RestResults],
        [{View, KVs} | RestViewKVs], ViewKVsAcc, ViewIdKeysAcc) ->
    % Take any identical keys and combine the values
    {NewKVs, NewViewIdKeysAcc} = lists:foldl(
        fun({Key, Val}, {[{{Key, PrevDocId} = Kd, PrevVal} | AccRest], AccVid})
                when PrevDocId =:= DocId ->
            AccKv2 = case PrevVal of
            {PartitionId, {dups, Dups}} ->
                [{Kd, {PartitionId, {dups, [Val | Dups]}}} | AccRest];
            {PartitionId, UserPrevVal} ->
                [{Kd, {PartitionId, {dups, [Val, UserPrevVal]}}} | AccRest]
            end,
            {AccKv2, AccVid};
        ({Key, Val}, {AccKv, AccVid}) ->
            {[{{Key, DocId}, {PartitionId, Val}} | AccKv], [Key | AccVid]}
        end,
        {KVs, []}, lists:sort(ResultKVs)),
    NewViewKVsAcc = [{View, NewKVs} | ViewKVsAcc],
    case NewViewIdKeysAcc of
    [] ->
        NewViewIdKeysAcc2 = ViewIdKeysAcc;
    _ ->
        NewViewIdKeysAcc2 = [
            {View#set_view.id_num, NewViewIdKeysAcc} | ViewIdKeysAcc]
    end,
    view_insert_doc_query_results(
        DocId, PartitionId, RestResults, RestViewKVs, NewViewKVsAcc,
        NewViewIdKeysAcc2).


-spec query_args_view_name(#view_query_args{}) -> binary().
query_args_view_name(#view_query_args{view_name = ViewName}) ->
    ViewName.
