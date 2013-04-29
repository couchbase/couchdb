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
