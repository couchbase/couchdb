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

-module(couch_db_consistency_check).

-export([check_db_file/1]).

-include("couch_db.hrl").

check_db_file(Filename) when is_list(Filename) or is_binary(Filename)->
    ?LOG_DEBUG("validation process for db file \"~s\"", [Filename]),
    Fd = case couch_file:open(Filename) of
    {ok, Fd0} ->
        Fd0;
    Error ->
        fatal_error("Couldn't open file ~s:~p", [Filename, Error])
    end,
    Header = try
        {ok, NewHeaderBin} = couch_file:read_header_bin(Fd),
        couch_db_updater:header_bin_to_db_header(NewHeaderBin)
    catch
    Type0:Error0 ->
        fatal_error("Problem reading header ~s:~p", [Filename, {Type0,Error0}])
    end,
    Db = try
        couch_db_updater:init_db(Filename, Filename, Fd, Header, [])
    catch
    Type1:Error1 ->
        fatal_error("Problem initializing DB ~s:~p", [Filename, {Type1,Error1}])
    end,
    check_db_file(Db),
    couch_file:close(Fd);
check_db_file(Db) ->
    Filename = Db#db.filepath,
    % first scan the by_sequence index
    Count = couch_db:count_changes_since(Db, 0),
    EtsById = ets:new(couch_db_consistency_check_name, [set,private]),
    {ok, {_,EncounteredCount}} = couch_db:changes_since(Db, 0,
        fun(DocInfo, {PrevSeq, Count0}) ->
            if PrevSeq > DocInfo#doc_info.local_seq ->
                fatal_error("Out of order seq found in by_seq idex ~s old ~p new ~p",
                    [Filename, PrevSeq, DocInfo#doc_info.local_seq]);
            true ->
                ok
            end,
            case ets:insert_new(EtsById, {DocInfo#doc_info.id, DocInfo}) of
            false ->
                fatal_error("Duplicate id found in by_seq idex ~s ~s",
                    [Filename, DocInfo#doc_info.id]);
            true ->
                ok
            end,
            if DocInfo#doc_info.body_ptr /= 0 ->
                {ok, Body} = try
                    {ok, _} = couch_file:pread_iolist(Db#db.fd,
                            DocInfo#doc_info.body_ptr)
                catch
                Type2:Error2 ->
                    fatal_error("Problem reading doc DB ~s id ~s: ~p",
                            [Filename, DocInfo#doc_info.id, {Type2,Error2}])
                end,
                if (DocInfo#doc_info.content_meta band
                        ?CONTENT_META_SNAPPY_COMPRESSED) > 0 ->
                    Body2 = try
                            couch_compress:decompress(Body)
                    catch
                    Type3:Error3 ->
                        fatal_error("Problem decompressing doc DB ~s id ~s: ~p",
                                [Filename, DocInfo#doc_info.id, {Type3,Error3}])
                    end;
                true ->
                    Body2 = Body
                end,
                if (DocInfo#doc_info.content_meta band
                        (bnot ?CONTENT_META_JSON)) == ?CONTENT_META_JSON ->
                    try
                        ?JSON_DECODE(Body2)
                    catch
                    Type4:Error4 ->
                        fatal_error("Problem validitate json DB ~s id ~s: ~p",
                                [Filename, DocInfo#doc_info.id, {Type4,Error4}])
                    end;
                true ->
                    ok
                end;
            true ->
                ok
            end,
            {ok, {DocInfo#doc_info.local_seq, Count0 + 1}}
        end, {0,0}),
    if EncounteredCount /= Count ->
        fatal_error("Count of by seq index is not the same as processed DB ~s ~p",
                [Filename, Count]);
    true ->
        ok
    end,
    {ok, CountById, _Id} = couch_db:enum_docs(Db, fun(DocInfo, PrevId) ->
            [{_, DocInfo2}] = ets:lookup(EtsById, DocInfo#doc_info.id),
            if DocInfo == DocInfo2 ->
                ok;
            true ->
                fatal_error("For file ~s found doc_info in by_id (~p) that"
                        " differs from the by_seq entry (~p)",
                        [Filename, DocInfo, DocInfo2])
            end,
            if PrevId < DocInfo#doc_info.id ->
                ok;
            true ->
                fatal_error("For file ~s found doc_info in by_id (~p) that"
                        " is in incorrect order",
                        [Filename, DocInfo])
            end,
            true = ets:delete(EtsById, DocInfo#doc_info.id),
            {ok, DocInfo#doc_info.id}
        end, 0, []), % first docid is num, as sorts lower than all strings
    case ets:tab2list(EtsById) of
    [] ->
        ok;
    LeftOverDocInfos ->
        fatal_error("Not all docs in by_seq index were found in the by id index ~s ~p",
            [Filename, LeftOverDocInfos])
    end,
    ets:delete(EtsById),
    if CountById /= Count ->
        fatal_error("Count of by id index is not the same as by seq DB ~s ~p ~p",
            [Filename, EncounteredCount, Count]);
    true ->
        ok
    end.

-spec fatal_error(list(), list()) -> no_return().
fatal_error(Fmt, Args) ->
    fatal_error(io_lib:format(Fmt, Args)).

-spec fatal_error(list()) -> no_return().
fatal_error(Msg) ->
    Msg2 = lists:flatten(Msg),
    ?LOG_ERROR("couch_db_consistency_check fatal error: ~s",[Msg2]),
    erlang:halt("couch_db_consistency_check fatal error: " ++ Msg2).
