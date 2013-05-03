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
-export([write_kvs/3, finish_build/3, get_state/1,
         start_reduce_context/1, end_reduce_context/1, view_name/2,
         update_tmp_files/3, view_bitmap/1]).
-export([update_index/5]).
% For the group
-export([design_doc_to_set_view_group/2, view_group_data_size/2,
         reset_view/1, make_views_fun/3]).
% For the utils
-export([clean_views/5]).
% For the compactor
-export([compact_view/6, get_row_count/1, apply_log/3]).


-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").


%% XXX vmx 2012-01-02: from couch_set_view_updater! Don't duplicate it, but
%%    move it to a common header file
-define(MAX_SORT_BUFFER_SIZE, 1048576).
-define(KEY_BITS,       12).
-define(MAX_KEY_SIZE,   ((1 bsl ?KEY_BITS) - 1)).
-record(writer_acc, {
    parent,
    owner,
    group,
    last_seqs = orddict:new(),
    compactor_running,
    write_queue,
    initial_build,
    view_empty_kvs,
    kvs = [],
    kvs_size = 0,
    kvs_length = 0,
    state = updating_active,
    final_batch = false,
    max_seqs,
    stats = #set_view_updater_stats{},
    tmp_dir = nil,
    initial_seqs,
    max_insert_batch_size,
    tmp_files = dict:new()
}).
-record(tmp_file_info, {
    name = nil,
    fd = nil,
    size = 0
}).
% XXX vmx 2013-05-03: from couch_set_view_compactor! Don't duplicate it, but
%    move it to a common header file
-define(SORTED_CHUNK_SIZE, 1024 * 1024).


write_kvs(Group, TmpFiles, ViewKVs) ->
    lists:foldl(
        fun({#set_view{id_num = Id}, KvList}, AccCount) ->
            #tmp_file_info{fd = ViewFd} = dict:fetch(Id, TmpFiles),
            KvBins = convert_primary_index_kvs_to_binary(KvList, Group, []),
            ViewRecords = lists:foldr(
                fun({KeyBin, ValBin}, Acc) ->
                    KvBin = [<<(byte_size(KeyBin)):16>>, KeyBin, ValBin],
                    [[<<(iolist_size(KvBin)):32>>, KvBin] | Acc]
                end,
                [], KvBins),
            ok = file:write(ViewFd, ViewRecords),
            AccCount + length(KvBins)
        end,
        0, ViewKVs).


convert_primary_index_kvs_to_binary([], _Group, Acc) ->
    lists:reverse(Acc);
convert_primary_index_kvs_to_binary([{{Key, DocId}, {PartId, V0}} | Rest], Group, Acc)->
    V = case V0 of
    {dups, Values} ->
        ValueListBinary = lists:foldl(
            fun(V, Acc2) ->
                <<Acc2/binary, (byte_size(V)):24, V/binary>>
            end,
            <<>>, Values),
        <<PartId:16, ValueListBinary/binary>>;
    _ ->
        <<PartId:16, (byte_size(V0)):24, V0/binary>>
    end,
    KeyBin = couch_set_view_util:encode_key_docid(Key, DocId),
    case byte_size(KeyBin) > ?MAX_KEY_SIZE of
    true ->
        #set_view_group{set_name = SetName, name = DDocId, type = Type} = Group,
        KeyPrefix = lists:sublist(unicode:characters_to_list(Key), 100),
        Error = iolist_to_binary(
            io_lib:format("key emitted for document `~s` is too long: ~s...", [DocId, KeyPrefix])),
        ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, ~s", [SetName, Type, DDocId, Error]),
        throw({error, Error});
    false ->
        convert_primary_index_kvs_to_binary(Rest, Group, [{KeyBin, V} | Acc])
    end.


% Build the tree out of the sorted files
finish_build(SetView, GroupFd, TmpFiles) ->
    #set_view{
        id_num = Id,
        btree = Bt
    } = SetView,

    #tmp_file_info{name = ViewFile} = dict:fetch(Id, TmpFiles),
    {ok, NewBtRoot} = couch_btree_copy:from_sorted_file(
        Bt, ViewFile, GroupFd,
        fun couch_set_view_updater:file_sorter_initial_build_format_fun/1),
    ok = file2:delete(ViewFile),
    SetView#set_view{
        btree = Bt#btree{root = NewBtRoot}
    }.


% Return the state of a view (which will be stored in the header)
get_state(Btree) ->
    couch_btree:get_state(Btree).


view_bitmap(Bt) ->
    {ok, <<_Size:40, Bm:?MAX_NUM_PARTITIONS, _/binary>>} =
        couch_btree:full_reduce(Bt),
    Bm.


start_reduce_context(Group) ->
    couch_set_view_mapreduce:start_reduce_context(Group).

end_reduce_context(Group) ->
    couch_set_view_mapreduce:end_reduce_context(Group).


view_name(#set_view_group{views = Views}, ViewPos) ->
    V = lists:nth(ViewPos, Views),
    case V#set_view.map_names of
    [] ->
        [{Name, _} | _] = V#set_view.reduce_funs;
    [Name | _] ->
        ok
    end,
    Name.


% Update the temporary files with the key-values from the indexer. Return
% the updated writer accumulator.
update_tmp_files(WriterAcc, ViewKeyValues, KeysToRemoveByView) ->
    #writer_acc{
       group = Group,
       tmp_files = TmpFiles
    } = WriterAcc,
    TmpFiles2 = lists:foldl(
        fun({#set_view{id_num = ViewId}, AddKeyValues}, AccTmpFiles) ->
            AddKeyValuesBinaries = convert_primary_index_kvs_to_binary(AddKeyValues, Group, []),
            KeysToRemove = couch_util:dict_find(ViewId, KeysToRemoveByView, []),
            BatchData = lists:map(
                fun(K) -> couch_set_view_updater_helper:encode_btree_op(remove, K) end,
                KeysToRemove),
            BatchData2 = lists:foldl(
                fun({K, V}, Acc) ->
                    Bin = couch_set_view_updater_helper:encode_btree_op(insert, K, V),
                    [Bin | Acc]
                end,
                BatchData, AddKeyValuesBinaries),
            ViewTmpFileInfo = dict:fetch(ViewId, TmpFiles),
            case ViewTmpFileInfo of
            #tmp_file_info{fd = nil} ->
                0 = ViewTmpFileInfo#tmp_file_info.size,
                ViewTmpFilePath = couch_set_view_updater:new_sort_file_name(WriterAcc),
                {ok, ViewTmpFileFd} = file2:open(ViewTmpFilePath, [raw, append, binary]),
                ViewTmpFileSize = 0;
            #tmp_file_info{fd = ViewTmpFileFd,
                           size = ViewTmpFileSize,
                           name = ViewTmpFilePath} ->
                ok
            end,
            ok = file:write(ViewTmpFileFd, BatchData2),
            ViewTmpFileInfo2 = ViewTmpFileInfo#tmp_file_info{
                fd = ViewTmpFileFd,
                name = ViewTmpFilePath,
                size = ViewTmpFileSize + iolist_size(BatchData2)
            },
            dict:store(ViewId, ViewTmpFileInfo2, AccTmpFiles)
        end,
    TmpFiles, ViewKeyValues),
    WriterAcc#writer_acc{
        tmp_files = TmpFiles2
    }.


-spec update_index(#btree{},
                   string(),
                   non_neg_integer(),
                   set_view_btree_purge_fun() | 'nil',
                   term()) ->
                          {'ok', term(), #btree{},
                           non_neg_integer(), non_neg_integer()}.
update_index(Bt, FilePath, BufferSize, PurgeFun, PurgeAcc) ->
    couch_set_view_updater_helper:update_btree(Bt, FilePath, BufferSize,
        PurgeFun, PurgeAcc).


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
                View =
                case dict:find({MapSrc, ViewOptions}, DictBySrcAcc) of
                    {ok, View0} -> View0;
                    error -> #set_view{def = MapSrc, options = ViewOptions}
                end,
                View2 =
                if RedSrc == null ->
                    View#set_view{map_names = [Name | View#set_view.map_names]};
                true ->
                    View#set_view{reduce_funs = [{Name, RedSrc} | View#set_view.reduce_funs]}
                end,
                dict:store({MapSrc, ViewOptions}, View2, DictBySrcAcc)
            end
        end, dict:new(), RawViews),
    % number the views
    {Views, _N} = lists:mapfoldl(
        fun({_Src, View}, N) ->
            {View#set_view{id_num = N}, N + 1}
        end,
        0, lists:sort(dict:to_list(DictBySrc))),
    SetViewGroup = #set_view_group{
        set_name = SetName,
        name = Id,
        views = Views,
        design_options = DesignOptions
    },
    couch_set_view_util:set_view_sig(SetViewGroup).


-spec view_group_data_size(#btree{}, [#set_view{}]) -> non_neg_integer().
view_group_data_size(IdBtree, Views) ->
    lists:foldl(
        fun(#set_view{btree = Btree}, Acc) ->
            Acc + couch_btree:size(Btree)
        end,
        couch_btree:size(IdBtree),
        Views).


reset_view(_Btree) ->
    nil.


% Needs to return that a function that takes 2 arguments, a btree state and
% the corresponding view
make_views_fun(Fd, BtreeOptions, Group) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = Type
    } = Group,
    fun(BTState, View) ->
        case View#set_view.reduce_funs of
        [{ViewName, _} | _] ->
            ok;
        [] ->
            [ViewName | _] = View#set_view.map_names
        end,
        ReduceFun =
            fun(reduce, KVs) ->
                AllPartitionsBitMap = couch_set_view_util:partitions_map(KVs, 0),
                KVs2 = couch_set_view_util:expand_dups(KVs, []),
                {ok, Reduced} =
                    try
                         couch_set_view_mapreduce:reduce(View, KVs2)
                    catch throw:{error, Reason} = Error ->
                        PrettyKVs = [
                            begin
                                {KeyDocId, <<_PartId:16, Value/binary>>} = RawKV,
                                {couch_set_view_util:split_key_docid(KeyDocId), Value}
                            end
                            || RawKV <- KVs2
                        ],
                        ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, error executing"
                                             " reduce function for view `~s'~n"
                                             "  reason:                ~s~n"
                                             "  input key-value pairs: ~p~n",
                                             [SetName, Type, DDocId, ViewName,
                                              couch_util:to_binary(Reason), PrettyKVs]),
                        throw(Error)
                    end,
                if length(Reduced) > 255 ->
                    throw({too_many_reductions, <<"Maximum reductions allowed is 255">>});
                true -> ok
                end,
                LenReductions = [<<(size(R)):16, R/binary>> || R <- Reduced],
                iolist_to_binary([<<(length(KVs2)):40, AllPartitionsBitMap:?MAX_NUM_PARTITIONS>> | LenReductions]);
            (rereduce, [<<Count0:40, AllPartitionsBitMap0:?MAX_NUM_PARTITIONS, Red0/binary>> | Reds]) ->
                {Count, AllPartitionsBitMap, UserReds} = lists:foldl(
                    fun(<<C:40, Apbm:?MAX_NUM_PARTITIONS, R/binary>>, {CountAcc, ApbmAcc, RedAcc}) ->
                        {C + CountAcc, Apbm bor ApbmAcc, [couch_set_view_util:parse_reductions(R) | RedAcc]}
                    end,
                    {Count0, AllPartitionsBitMap0, [couch_set_view_util:parse_reductions(Red0)]},
                    Reds),
                {ok, Reduced} =
                    try
                        couch_set_view_mapreduce:rereduce(View, UserReds)
                    catch throw:{error, Reason} = Error ->
                        ?LOG_MAPREDUCE_ERROR("Bucket `~s`, ~s group `~s`, error executing"
                                             " rereduce function for view `~s'~n"
                                             "  reason:           ~s~n"
                                             "  input reductions: ~p~n",
                                             [SetName, Type, DDocId, ViewName,
                                              couch_util:to_binary(Reason), UserReds]),
                        throw(Error)
                    end,
                LenReductions = [<<(size(R1)):16, R1/binary>> || R1 <- Reduced],
                iolist_to_binary([<<Count:40, AllPartitionsBitMap:?MAX_NUM_PARTITIONS>> | LenReductions])
            end,
        Less = fun(A, B) ->
            {Key1, DocId1} = couch_set_view_util:split_key_docid(A),
            {Key2, DocId2} = couch_set_view_util:split_key_docid(B),
            case couch_ejson_compare:less_json(Key1, Key2) of
            0 ->
                DocId1 < DocId2;
            LessResult ->
                LessResult < 0
            end
        end,
        {ok, Btree} = couch_btree:open(
            BTState, Fd, [{less, Less}, {reduce, ReduceFun} | BtreeOptions]),
        View#set_view{btree = Btree}
    end.


clean_views(_, _, [], Count, Acc) ->
    {Count, lists:reverse(Acc)};
clean_views(stop, _, Rest, Count, Acc) ->
    {Count, lists:reverse(Acc, Rest)};
clean_views(go, PurgeFun, [#set_view{btree = Btree} = View | Rest], Count, Acc) ->
    couch_set_view_mapreduce:start_reduce_context(View),
    {ok, NewBtree, {Go, PurgedCount}} =
        couch_btree:guided_purge(Btree, PurgeFun, {go, Count}),
    couch_set_view_mapreduce:end_reduce_context(View),
    NewAcc = [View#set_view{btree = NewBtree} | Acc],
    clean_views(Go, PurgeFun, Rest, PurgedCount, NewAcc).


compact_view(Fd, View, EmptyView, FilterFun, BeforeKVWriteFun, Acc0) ->
    #set_view{
        btree = ViewBtree
    } = EmptyView,

    couch_set_view_mapreduce:start_reduce_context(View),
    {ok, NewBtreeRoot, Acc2} = couch_btree_copy:copy(
        View#set_view.btree, Fd,
        [{before_kv_write, {BeforeKVWriteFun, Acc0}}, {filter, FilterFun}]),
    couch_set_view_mapreduce:end_reduce_context(View),

    ViewBtree2 = ViewBtree#btree{root = NewBtreeRoot},
    NewView = EmptyView#set_view{
        btree = ViewBtree2
    },
    {NewView, Acc2}.


-spec get_row_count(#set_view{}) -> non_neg_integer().
get_row_count(SetView) ->
    Bt = SetView#set_view.btree,
    ok = couch_set_view_mapreduce:start_reduce_context(SetView),
    {ok, <<Count:40, _/binary>>} = couch_btree:full_reduce(Bt),
    ok = couch_set_view_mapreduce:end_reduce_context(SetView),
    Count.


apply_log(SetViews, ViewLogFiles, TmpDir) ->
    lists:zipwith(fun(SetView, Files) ->
        Bt = SetView#set_view.btree,
        MergeFile = couch_set_view_compactor:merge_files(Files, Bt, TmpDir),
        {ok, NewBt, _, _} = couch_set_view_updater_helper:update_btree(
               Bt, MergeFile, ?SORTED_CHUNK_SIZE),
        ok = file2:delete(MergeFile),
        SetView#set_view{btree = NewBt}
    end, SetViews, ViewLogFiles).
