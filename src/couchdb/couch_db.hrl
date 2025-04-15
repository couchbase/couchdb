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

%% This file should ideally be in couchdb/include, but has a very large number
%% of dependents, including geocouch, and it was decided not worth having to
%% update all those dependents. We should probably move this eventually.

-ifndef(_COUCH_DB_COMMON__HRL_).
-define(_COUCH_DB_COMMON__HRL_,).

-define(LOCAL_DOC_PREFIX, "_local/").
-define(DESIGN_DOC_PREFIX0, "_design").
-define(DESIGN_DOC_PREFIX, "_design/").
-define(DEFAULT_COMPRESSION, snappy).
-define(FD_CLOSE_TIMEOUT_MS, 10). % Fds will close after msecs of non-use

-define(MIN_STR, <<"">>).
-define(MAX_STR, <<255>>). % illegal utf string

-define(QUERY_TIMING_STATS_ETS, view_query_timing_stats).

% the lowest possible database sequence number
-define(LOWEST_SEQ, 0).

-define(JSON_ENCODE(V), ejson:encode(V)).
-define(JSON_DECODE(V), ejson:decode(V)).

-define(b2l(V), binary_to_list(V)).
-define(l2b(V), list_to_binary(V)).
-define(term_to_bin(T), term_to_binary(T, [{minor_version, 1}])).
-define(term_size(T),
    try
        erlang:external_size(T)
    catch _:_ ->
        byte_size(?term_to_bin(T))
    end).

-define(DEFAULT_ATTACHMENT_CONTENT_TYPE, <<"application/octet-stream">>).

-define(LOG_DEBUG(Format, Args),
    case couch_log:debug_on() of
        true ->
            couch_log:debug(Format, Args);
        false -> ok
    end).

-define(LOG_INFO(Format, Args),
    case couch_log:info_on() of
        true ->
            couch_log:info(Format, Args);
        false -> ok
    end).

-define(LOG_ERROR(Format, Args), couch_log:error(Format, Args)).

-define(LOG_MAPREDUCE_ERROR(Format, Args),
    % On the full Couchbase stack the ale logger is available
    try
        {current_function, {_, FunName__, _}} = process_info(self(), current_function),
        ale:xerror(mapreduce_errors,
                   {?MODULE, FunName__, ?LINE}, undefined, Format, Args)
    catch error:undef ->
        ?LOG_ERROR(Format, Args)
    end).

-define(LOG_WITH_TAGS(Tag, Arg),
        couch_util:log_data(Tag, Arg)
        ).

-define(LOG_USERDATA(Arg),
        ?LOG_WITH_TAGS(ud, Arg)
       ).
-define(LOG_SYSTEMDATA(Arg),
        ?LOG_WITH_TAGS(sd, Arg)
       ).
-define(LOG_METADATA(Arg),
        ?LOG_WITH_TAGS(md, Arg)
       ).

-define(CONTENT_META_JSON, 0).
-define(CONTENT_META_INVALID_JSON, 1).
-define(CONTENT_META_NON_JSON_MODE, 3).

-define(CONTENT_META_SNAPPY_COMPRESSED, (1 bsl 7)).

-record(doc_info,
    {
    id = <<"">>,
    deleted = false,
    local_seq,
    rev = {0, <<>>},
    body_ptr,
    content_meta = 0, % should be 0-255 only.
    size = 0
    }).

-record(doc_update_info,
    {
    id = <<"">>,
    deleted = false,
    rev,
    body_ptr,
    content_meta = 0, % should be 0-255 only.
    size = 0,
    fd
    }).

-record(httpd,
    {mochi_req,
    peer,
    method,
    path_parts,
    db_frontend,
    db_url_handlers,
    user_ctx,
    req_body = undefined,
    design_url_handlers,
    auth,
    default_fun,
    url_handlers,
    extra_headers = []
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


-record(user_ctx,
    {
    name=null,
    roles=[],
    handler
    }).

% This should be updated anytime a header change happens that requires more
% than filling in new defaults.
%
% if the disk revision is incremented, then new upgrade logic will need to be
% added to couch_db_updater:init_db.

-define(LATEST_DISK_VERSION, 11).

-record(db_header,
    {disk_version = ?LATEST_DISK_VERSION,
     update_seq = 0,
     docinfo_by_id_btree_state = nil,
     docinfo_by_seq_btree_state = nil,
     local_docs_btree_state = nil,
     purge_seq = 0,
     purged_docs = nil,
     security_ptr = nil
    }).

-record(db,
    {main_pid = nil,
    update_pid = nil,
    compactor_info = nil,
    instance_start_time, % number of microsecs since jan 1 1970 as a binary string
    fd,
    fd_ref_counter,
    header = #db_header{},
    committed_update_seq,
    docinfo_by_id_btree,
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


-record(view_query_args, {
    start_key,
    end_key,
    start_docid = ?MIN_STR,
    end_docid = ?MAX_STR,

    direction = fwd,
    inclusive_end=true, % aka a closed-interval

    limit = 10000000000, % Huge number to simplify logic
    skip = 0,

    group_level = 0,

    view_type = nil,
    include_docs = false,
    conflicts = false,
    stale = false,
    multi_get = false,
    callback = nil,
    list = nil,

    % Used by view/index merger.
    run_reduce = true,
    keys = nil,
    view_name = nil,

    debug = false,
    % Whether to filter the passive/cleanup partitions out
    filter = true,
    % Whether to query the main or the replica index
    type = main
}).

-record(view_fold_helper_funs, {
    reduce_count,
    passed_end,
    start_response,
    send_row
}).

-record(reduce_fold_helper_funs, {
    start_response,
    send_row
}).

-record(extern_resp_args, {
    code = 200,
    stop = false,
    data = <<>>,
    ctype = "application/json",
    headers = []
}).

-record(group, {
    sig=nil,
    fd=nil,
    name,
    def_lang,
    design_options=[],
    views,
    lib,
    id_btree=nil,
    current_seq=0,
    purge_seq=0,
    waiting_delayed_commit=nil,
    ddoc_db_name=nil
    }).

-record(view,
    {id_num,
    update_seq=0,
    purge_seq=0,
    map_names=[],
    def,
    btree=nil,
    reduce_funs=[],
    options=[],
    ref
    }).

-record(index_header,
    {seq=0,
    purge_seq=0,
    id_btree_state=nil,
    view_states=nil
    }).

% small value used in revision trees to indicate the revision isn't stored
-define(REV_MISSING, []).

-record(changes_args, {
    feed = "normal",
    dir = fwd,
    since = 0,
    limit = 1000000000000000,
    style = main_only,
    heartbeat,
    timeout,
    filter = "",
    filter_fun,
    filter_args = [],
    include_docs = false,
    conflicts = false,
    db_open_options = []
}).

-record(btree, {
    fd,
    root,
    extract_kv = identity,  % fun({_Key, _Value} = KV) -> KV end,
    assemble_kv = identity, % fun({Key, Value}) -> {Key, Value} end,
    less = fun(A, B) -> A < B end,
    reduce = nil,
    kv_chunk_threshold = 16#4ff,
    kp_chunk_threshold = 2 * 16#4ff,
    binary_mode = false
}).

-endif.

