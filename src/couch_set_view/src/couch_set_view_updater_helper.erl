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
-export([update_btrees/5]).


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
                       {ok, DocCount} = couch_dcp_client:get_num_items(
                           Group#set_view_group.dcp_pid, PartId),
                       CountAcc + DocCount
                   end
                end, 0, Parts),
    Count.

% For a view group, perform btree updates for idbtree and view btrees
update_btrees(Group, TmpDir, LogFiles, MaxBatchSize, IsSorted) ->
    case os:find_executable("couch_view_index_updater") of
    false ->
        Cmd = nil,
        throw(<<"couch_view_index_updater command not found">>);
    Cmd ->
        ok
    end,
    Options = [exit_status, use_stdio, stderr_to_stdout, stream, binary],
    Port = open_port({spawn_executable, Cmd}, Options),

    true = port_command(Port, [TmpDir, $\n]),
    couch_set_view_util:send_group_info(Group, Port),

    % Send is_sorted flag
    case IsSorted of
    false ->
        true = port_command(Port, [$u, $\n]);
    true ->
        true = port_command(Port, [$s, $\n])
    end,

    % Send operations log file paths
    [IdFile | ViewFiles] = LogFiles,
    true = port_command(Port, [IdFile, $\n]),
    lists:foreach(
            fun(ViewFile) ->
            true = port_command(Port, [ViewFile, $\n])
            end,
        ViewFiles),
    true = port_command(Port, [integer_to_list(MaxBatchSize), $\n]),
    ok = couch_set_view_util:send_group_header(Group, Port),
    {NewGroup, Stats} =
    try update_btrees_wait_loop(Port, Group, <<>>, {0, 0, 0, 0, 0}) of
    {ok, Resp} ->
        Resp
    catch
    Error ->
        exit(Error)
    after
        catch port_close(Port)
    end,
    {ok, NewGroup, Stats}.

update_btrees_wait_loop(Port, Group, Acc0, Stats) ->
    #set_view_group{
        set_name = SetName,
        name = DDocId,
        type = Type
    } = Group,
    {Line, Acc} = couch_set_view_util:try_read_line(Acc0),
    case Line of
    nil ->
        receive
        {Port, {data, Data}} ->
            Acc2 = ?l2b([Acc, Data]),
            update_btrees_wait_loop(Port, Group, Acc2, Stats);
        {Port, {exit_status, 0}} ->
            {ok, {Group, Stats}};
        {Port, {exit_status, 1}} ->
            ?LOG_INFO("Set view `~s`, ~s group `~s`, index updater stopped successfully.",
                       [SetName, Type, DDocId]),
            exit(shutdown);
        {Port, {exit_status, Status}} ->
            throw({view_group_index_updater_exit, Status, Acc});
        {Port, Error} ->
            throw({view_group_index_updater_error, Error});
        stop ->
            ?LOG_INFO("Set view `~s`, ~s group `~s`, sending stop message to index updater.",
                       [SetName, Type, DDocId]),
            port_command(Port, "exit"),
            update_btrees_wait_loop(Port, Group, Acc, Stats)
        end;
    <<"Header Len : ", Data/binary>> ->
        % Read resulting group from stdout
        {ok, [HeaderLen], []} = io_lib:fread("~d", ?b2l(Data)),
        {NewGroup, Acc2} =
        case couch_set_view_util:receive_group_header(Port, HeaderLen, Acc) of
        {ok, HeaderBin, Rest} ->
            #set_view_group{
                id_btree = IdBtree,
                views = Views
            } = Group,
            Header  = couch_set_view_util:header_bin_to_term(HeaderBin),
            #set_view_index_header{
                id_btree_state = NewIdBtreeRoot,
                view_states = NewViewRoots
            } = Header,
            NewIdBtree = couch_btree:set_state(IdBtree, NewIdBtreeRoot),
            NewViews = case Views of
            % Spatial views use this function only for the ID b-tree. Their
            % views get removed before calling from the view group and get
            % added afterwards.
            [] ->
                [];
            _ ->
                lists:zipwith(
                    fun(#set_view{indexer = View} = V, NewRoot) ->
                        #mapreduce_view{btree = Bt} = View,
                        NewBt = couch_btree:set_state(Bt, NewRoot),
                        NewView = View#mapreduce_view{btree = NewBt},
                        V#set_view{indexer = NewView}
                    end,
                    Views, NewViewRoots)
            end,

            NewGroup0 = Group#set_view_group{
                id_btree = NewIdBtree,
                views = NewViews,
                index_header = Header
            },
            NewGroup2 = couch_set_view_group:remove_duplicate_partitions(NewGroup0),
            {NewGroup2, Rest};
        {error, Error, Rest} ->
            self() ! Error,
            {Group, Rest}
        end,
        update_btrees_wait_loop(Port, NewGroup, Acc2, Stats);
    <<"Results = ", Data/binary>> ->
        {ok, [IdInserted, IdDeleted, ViewInserted, ViewDeleted, Cleanups], []} =
            io_lib:fread("id_inserts : ~d, "
                         "id_deletes : ~d, "
                         "kv_inserts : ~d, "
                         "kv_deletes : ~d, "
                         "cleanups : ~d",
                         ?b2l(Data)),
        Stats2 = {IdInserted, IdDeleted, ViewInserted, ViewDeleted, Cleanups},
        update_btrees_wait_loop(Port, Group, Acc, Stats2);
    Msg ->
        ?LOG_ERROR("Set view `~s`, ~s group `~s`, received error from index updater: ~s",
                   [SetName, Type, DDocId, Msg]),
        Msg2 = case Msg of
        <<"Error updating index", _/binary>> ->
            Msg;
        _ ->
            <<>>
        end,
        update_btrees_wait_loop(Port, Group, Msg2, Stats)
    end.
