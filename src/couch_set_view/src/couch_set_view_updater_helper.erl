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

-module(couch_set_view_updater_helper).

-export([update_btree/3, update_btree/5]).
-export([encode_btree_op/2, encode_btree_op/3]).
-export([file_sorter_batch_format_fun/1]).
-export([count_items_from_set/2]).


-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").



update_btree(Bt, FilePath, BufferSize) ->
    {ok, nil, Bt2, I, D} = update_btree(Bt, FilePath, BufferSize, nil, nil),
    {ok, Bt2, I, D}.


-spec update_btree(#btree{},
                   string(),
                   non_neg_integer(),
                   set_view_btree_purge_fun() | 'nil',
                   term()) ->
                          {'ok', term(), #btree{},
                           non_neg_integer(), non_neg_integer()}.
update_btree(Bt, FilePath, BufferSize, PurgeFun, PurgeAcc) ->
    {ok, Fd} = file2:open(FilePath, [read, raw, binary, {read_ahead, BufferSize}]),
    (catch file:advise(Fd, 0, 0, sequential)),
    try
        update_btree_loop(
            Fd, Bt, BufferSize, PurgeFun, PurgeAcc, [], 0, 0, 0, 0, 0)
    after
        ok = file:close(Fd)
    end.

update_btree_loop(Fd, Bt, BufferSize, PurgeFun, PurgeAcc,
                  Acc, AccSize, Inserted, Deleted, FlushStartOffset, FlushEndOffset) ->
    case file:read(Fd, 4) of
    {ok, <<Len:32/native>>} ->
        {ok, ActionBin} = file:read(Fd, Len),
        Action = file_sorter_batch_format_fun(ActionBin),
        Acc2 = [Action | Acc],
        AccSize2 = AccSize + Len,
        case element(1, Action) of
        remove ->
            Inserted2 = Inserted,
            Deleted2 = Deleted + 1;
        insert ->
            Inserted2 = Inserted + 1,
            Deleted2 = Deleted
        end,
        FlushEndOffset2 = FlushEndOffset + 4 + Len,
        case AccSize2 >= BufferSize of
        true ->
            _ = file:advise(Fd, FlushStartOffset, FlushEndOffset2, dont_need),
            Actions = lists:reverse(Acc2),
            {ok, [], PurgeAcc2, Bt2} = couch_btree:query_modify_raw(
                Bt, Actions, PurgeFun, PurgeAcc),
            ok = couch_file:flush(Bt#btree.fd),
            update_btree_loop(Fd, Bt2, BufferSize,
                              PurgeFun, PurgeAcc2, [], 0,
                              Inserted2, Deleted2, FlushEndOffset2, FlushEndOffset2);
        false ->
            update_btree_loop(Fd, Bt, BufferSize,
                              PurgeFun, PurgeAcc, Acc2, AccSize2,
                              Inserted2, Deleted2, FlushStartOffset, FlushEndOffset2)
        end;
    eof when Acc == [] ->
        {ok, PurgeAcc, Bt, Inserted, Deleted};
    eof ->
        _ = file:advise(Fd, FlushStartOffset, FlushEndOffset, dont_need),
        Actions = lists:reverse(Acc),
        {ok, [], PurgeAcc2, Bt2} = couch_btree:query_modify_raw(Bt, Actions, PurgeFun, PurgeAcc),
        ok = couch_file:flush(Bt#btree.fd),
        {ok, PurgeAcc2, Bt2, Inserted, Deleted}
    end.


-spec encode_btree_op('remove', binary()) -> binary().
encode_btree_op(remove = Op, Key) ->
    Data = <<(btree_op_to_code(Op)):8, (byte_size(Key)):16, Key/binary>>,
    <<(byte_size(Data)):32/native, Data/binary>>.


-spec encode_btree_op('insert', binary(), binary()) -> binary().
encode_btree_op(insert = Op, Key, Value) ->
    Data = <<(btree_op_to_code(Op)):8,
             (byte_size(Key)):16, Key/binary, Value/binary>>,
    <<(byte_size(Data)):32/native, Data/binary>>.


btree_op_to_code(remove) ->
    1;
btree_op_to_code(insert) ->
    2.


code_to_btree_op(1) ->
    remove;
code_to_btree_op(2) ->
    insert.


-spec file_sorter_batch_format_fun(binary()) -> view_btree_op().
file_sorter_batch_format_fun(<<Op:8, KeyLen:16, K:KeyLen/binary, Rest/binary>>) ->
    case code_to_btree_op(Op) of
    remove ->
        {remove, K, nil};
    insert ->
        {insert, K, Rest}
    end.

% Aggregate non-deleted documents count from given list of vbuckets belonging to a bucket
-spec count_items_from_set(#set_view_group{}, [integer()]) -> integer().
count_items_from_set(Group, Parts) ->
    Count = lists:foldr(
              fun(PartId, CountAcc) ->
                  case couch_set_view_util:has_part_seq(PartId, ?set_unindexable_seqs(Group))
                      andalso not lists:member(PartId, ?set_replicas_on_transfer(Group)) of
                   true ->
                       CountAcc;
                   false ->
                       SetName = Group#set_view_group.set_name,
                       Db = case couch_db:open_int(?dbname(SetName, PartId), []) of
                       {ok, PartDb} ->
                           PartDb;
                       Error ->
                           ErrorMsg = io_lib:format("Updater error opening database `~s': ~w",
                                         [?dbname(SetName, PartId), Error]),
                           throw({error, iolist_to_binary(ErrorMsg)})
                       end,
                       {ok, DbInfo} = couch_db:get_db_info(Db),
                       ok = couch_db:close(Db),
                       DocCount = couch_util:get_value(doc_count, DbInfo),
                       CountAcc + DocCount
                   end
                end, 0, Parts),
    Count.
