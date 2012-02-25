#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable

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

-define(SIZE_BLOCK, 4096).

-record(user_ctx, {
    name = null,
    roles = [],
    handler
}).

-record(db, {
    main_pid = nil,
    update_pid = nil,
    compactor_pid = nil,
    instance_start_time, % number of microsecs since jan 1 1970 as a binary string
    fd,
    fd_ref_counter,
    header = nil,
    committed_update_seq,
    fulldocinfo_by_id_btree,
    docinfo_by_seq_btree,
    local_docs_btree,
    update_seq,
    name,
    filepath,
    security = [],
    security_ptr = nil,
    user_ctx = #user_ctx{},
    waiting_delayed_commit = nil,
    fsync_options = [],
    options = []
}).


-record(db_header,
    {disk_version,
     update_seq = 0,
     docinfo_by_id_btree_state = nil,
     docinfo_by_seq_btree_state = nil,
     local_docs_btree_state = nil,
     purge_seq = 0,
     purged_docs = nil,
     security_ptr = nil
    }).

-record(doc,
    {
    id = <<>>,
    rev = {0, <<>>},

    % the binary body
    body = <<"{}">>,
    content_meta = 0, % should be 0-255 only.

    deleted = false,

    % key/value tuple of meta information, provided when using special options:
    % couch_db:open_doc(Db, Id, Options).
    meta = []
    }).

main(_) ->
    test_util:init_code_path(),

    etap:plan(11),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.

test() ->
    couch_server_sup:start_link(test_util:config_files()),
    couch_file_write_guard:disable_for_testing(),
    couch_server:delete(<<"etap-test-db">>, []),
    {ok, Db} = couch_db:create(<<"etap-test-db">>, []),
    ok = couch_db:update_doc(Db, #doc{id = <<"1">>,body = <<"{foo:1}">>},
            [full_commit]),
    ok = couch_db:update_doc(Db, #doc{id = <<"2">>,body = <<"{foo:1}">>},
            [full_commit]),
    couch_db:close(Db),
    {ok, Db2} = couch_db:open(<<"etap-test-db">>, []),

    {ok, Doc1} = couch_db:open_doc(Db2, <<"1">>),
    etap:is(Doc1#doc.id, <<"1">>, "Doc 1 was created."),

    {ok, Doc2} = couch_db:open_doc(Db2, <<"2">>),
    etap:is(Doc2#doc.id, <<"2">>, "Doc 2 was created."),

    etap:is(couch_db:update_header_pos(Db2, 0, 0), retry_new_file_version,
            "Should retry, earlier file version"),
    etap:is(couch_db:update_header_pos(Db2, 1, 0), update_behind_couchdb,
            "Should be behind couchdb"),
    etap:is(couch_db:update_header_pos(Db2, 2, 0), update_file_ahead_of_couchdb,
            "Should be ahead couchdb"),


    DbRootDir = couch_config:get("couchdb", "database_dir", "."),
    Filename = filename:join(DbRootDir, "etap-test-db.couch.1"),
    {ok, Fd} = couch_file:open(Filename),
    {ok, FileLen} = couch_file:bytes(Fd),

    Header = Db2#db.header,
    Header2 = Header#db_header{update_seq = 0},

    etap:is(couch_file:write_header(Fd, Header2), ok,
            "Should write new header outside of couchdb"),

    % calculate where new header goes
    case FileLen rem ?SIZE_BLOCK of
    0 ->
        NewHeaderPos = FileLen + ?SIZE_BLOCK;
    BlockOffset ->
        NewHeaderPos = FileLen + (?SIZE_BLOCK - BlockOffset)
    end,
    etap:is(couch_db:update_header_pos(Db2, 1, NewHeaderPos), update_behind_couchdb,
            "Should be ahead couchdb"),

    Header3 = Header#db_header{update_seq = Header#db_header.update_seq + 1},
    etap:is(couch_file:write_header(Fd, Header3), ok,
            "Should write new header outside of couchdb"),
    NewHeaderPos2 = NewHeaderPos + ?SIZE_BLOCK,

    etap:is(couch_db:update_header_pos(Db2, 1, NewHeaderPos2), ok,
            "Should accept new header"),
    couch_db:close(Db2),

    {ok, Db3} = couch_db:open(<<"etap-test-db">>, []),

    etap:is(Db3#db.header, Header3,
            "Db header should be what we just wrote"),

    {ok, CompactPid} = couch_db:start_compact(Db3),
    monitor(process, CompactPid),
    receive
    {'DOWN', _MonitorRef, _Type, CompactPid, normal} ->
        Finished = true
    after 3000 ->
        Finished = false
    end,
    etap:is(Finished, true,
              "Db should still be compactable."),
    couch_db:close(Db3),
    couch_server:delete(<<"etap-test-db">>, []),
    ok.
