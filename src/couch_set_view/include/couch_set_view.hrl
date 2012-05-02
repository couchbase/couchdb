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

-define(SET_VIEW_STATS_ETS, couch_set_view_stats).

-define(set_view_group_stats_key(Group),
    {
        Group#set_view_group.set_name,
        Group#set_view_group.name,
        Group#set_view_group.sig,
        Group#set_view_group.type
    }
).

-define(dbname(SetName, PartId),
      <<SetName/binary, $/, (?l2b(integer_to_list(PartId)))/binary>>).

-define(master_dbname(SetName), <<SetName/binary, "/master">>).

-define(set_num_partitions(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.num_partitions).

-define(set_abitmask(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.abitmask).

-define(set_pbitmask(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.pbitmask).

-define(set_cbitmask(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.cbitmask).

-define(set_seqs(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.seqs).

-define(set_purge_seqs(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.purge_seqs).

-define(set_replicas_on_transfer(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.replicas_on_transfer).

-define(set_pending_transition(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.pending_transition).

-define(set_unindexable_seqs(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.unindexable_seqs).

-define(set_unindexable_purge_seqs(SetViewGroup),
        (SetViewGroup#set_view_group.index_header)#set_view_index_header.unindexable_purge_seqs).


-type partition_id()             :: non_neg_integer().
-type staleness()                :: 'update_after' | 'ok' | 'false'.
-type bitmask()                  :: non_neg_integer().
-type bitmap()                   :: non_neg_integer().
-type update_seq()               :: non_neg_integer().
-type btree_state()              :: 'nil' | tuple().
-type partition_seq()            :: {partition_id(), update_seq()}.
-type partition_seqs()           :: ordsets:ordset(partition_seq()).
-type view_state()               :: btree_state().
-type set_view_group_type()      :: 'main' | 'replica'.
-type set_view_ets_stats_key()   :: {binary(), binary(), binary(), set_view_group_type()}.
-type ejson_object()             :: {[{binary() | atom(), term()}]}.
-type set_view_updater_state()   :: 'updating_active' | 'updating_passive'.

-type set_view_key()             :: {Key::term(), DocId::binary()}.
-type set_view_value()           :: {partition_id(), Value::term()}.
-type set_view_key_value()       :: {set_view_key(), set_view_value()}.
-type set_view_reduction()       :: {Count::non_neg_integer(), bitmask()} |
                                    {Count::non_neg_integer(), UserReductions::[term()], bitmask()}.

-type set_view_btree_purge_fun() :: fun(('branch' | 'value',
                                         set_view_reduction() | set_view_key_value(),
                                         Acc::{'go', term()}) ->
                                    {'purge', FinalAcc::{'go' | 'stop', term()}} |
                                    {'keep', FinalAcc::{'go' | 'stop', term()}} |
                                    {'partial_purge', FinalAcc::{'go' | 'stop', term()}} |
                                    {'stop', FinalAcc::{'stop', term()}}).

-type set_view_fold_fun()        :: fun((set_view_key_value(), Offset::term(), Acc::term()) ->
                                    {'ok' | 'stop', FinalAcc::term()}).
-type set_view_fold_reduce_fun() :: fun((set_view_key(), Reduction::term(), Acc::term()) ->
                                    {'ok' | 'stop', FinalAcc::term()}).
-type set_view_key_group_fun()   :: fun((set_view_key(), set_view_key()) -> boolean()).


% Used to configure a new set view.
-record(set_view_params, {
    max_partitions = 0         :: non_neg_integer(),
    active_partitions = []     :: [partition_id()],
    passive_partitions = []    :: [partition_id()],
    use_replica_index = false  :: boolean()
}).

-record(set_view_group_req, {
    stale = updater_after   :: staleness(),
    update_stats = false    :: boolean(),
    wanted_partitions = []  :: [partition_id()],
    debug = false           :: boolean()
}).

-record(set_view_transition, {
    active = []  :: ordsets:ordset(partition_id()),
    passive = [] :: ordsets:ordset(partition_id()),
    cleanup = [] :: ordsets:ordset(partition_id())
}).

-define(LATEST_COUCH_SET_VIEW_HEADER_VERSION, 1).

-record(set_view_index_header, {
    version = ?LATEST_COUCH_SET_VIEW_HEADER_VERSION :: non_neg_integer(),
    % Maximum number of partitions this set view supports, nil means not yet defined.
    num_partitions = nil                            :: 'nil' | non_neg_integer(),
    % active partitions bitmap
    abitmask = 0                                    :: bitmask(),
    % passive partitions bitmap
    pbitmask = 0                                    :: bitmask(),
    % cleanup partitions bitmap
    cbitmask = 0                                    :: bitmask(),
    seqs = []                                       :: partition_seqs(),
    purge_seqs = []                                 :: partition_seqs(),
    id_btree_state = nil                            :: btree_state(),
    view_states = []                                :: [view_state()],
    has_replica = false                             :: boolean(),
    replicas_on_transfer = []                       :: ordsets:ordset(partition_id()),
    % Pending partition states transition.
    pending_transition = nil                        :: 'nil' | #set_view_transition{},
    % Type should be something like orddict(partition_seq()), but however the orddict
    % module doesn't export yet a type spec (unlike ordsets for e.g.).
    unindexable_seqs = []                           :: [partition_seq()],
    unindexable_purge_seqs = []                     :: [partition_seq()]
}).

% Keep all stats values as valid EJSON (except ets key).
-record(set_view_group_stats, {
    % as generated by ?set_view_group_stats_key(#set_view_group{})
    ets_key                 :: set_view_ets_stats_key(),
    % # accesses for view streaming
    accesses = 0            :: non_neg_integer(),
    full_updates = 0        :: non_neg_integer(),
    % # of updates that only finished updating the active partitions
    % (in the phase of updating passive partitions). Normally its value
    % is full_updates - 1.
    partial_updates = 0     :: non_neg_integer(),
    % # of times the updater was forced to stop (because partition states
    % were updated) while it was still indexing the active partitions.
    stopped_updates = 0     :: non_neg_integer(),
    compactions = 0         :: non_neg_integer(),
    % # of interrupted cleanups. Cleanups which were stopped (in order to do
    % higher priority tasks) and left the index in a not yet clean state (but
    % hopefully closer to a clean state).
    cleanup_stops = 0       :: non_neg_integer(),
    cleanups = 0            :: non_neg_integer(),
    updater_cleanups = 0    :: non_neg_integer(),
    update_errors = 0       :: non_neg_integer(),
    update_history = []     :: [ejson_object()],
    compaction_history = [] :: [ejson_object()],
    cleanup_history = []    :: [ejson_object()]
}).

-record(set_view_debug_info, {
    original_abitmask = 0             :: bitmask(),
    original_pbitmask = 0             :: bitmask(),
    stats = #set_view_group_stats{}   :: #set_view_group_stats{},
    replica_partitions = []           :: ordsets:ordset(partition_id())
}).

-record(set_view, {
    id_num = 0        :: non_neg_integer(),
    map_names = []    :: [binary()],
    def = <<>>        :: binary(),
    btree = nil       :: 'nil' | #btree{},
    reduce_funs = []  :: [{binary(), binary()}],
    options = []      :: [term()],
    ref               :: reference()
}).

-record(set_view_group, {
    sig = nil                           :: 'nil' | binary(),
    fd = nil                            :: 'nil' | pid(),
    set_name = <<>>                     :: binary(),
    name = <<>>                         :: binary(),
    design_options = []                 :: [any()],
    views = []                          :: [#set_view{}],
    id_btree = nil                      :: 'nil' | #btree{},
    ref_counter = nil                   :: 'nil' | pid(),
    index_header = nil                  :: 'nil' | #set_view_index_header{},
    db_set = nil                        :: 'nil' | pid(),
    type = main                         :: set_view_group_type(),
    replica_group = nil                 :: 'nil' | #set_view_group{},
    replica_pid = nil                   :: 'nil' | pid(),
    debug_info = nil                    :: #set_view_debug_info{} | 'nil',
    filepath = ""                       :: string()
}).

-record(set_view_updater_result, {
    group = #set_view_group{}  :: #set_view_group{},
    indexing_time = 0.0        :: float(),  % seconds
    blocked_time = 0.0         :: float(),  % seconds
    state = updating_active    :: set_view_updater_state(),
    cleanup_kv_count = 0       :: non_neg_integer(),
    cleanup_time = 0.0         :: float(),  % seconds
    inserted_ids = 0           :: non_neg_integer(),
    deleted_ids = 0            :: non_neg_integer(),
    inserted_kvs = 0           :: non_neg_integer(),
    deleted_kvs = 0            :: non_neg_integer()
}).

-record(set_view_compactor_result, {
    group = #set_view_group{}  :: #set_view_group{},
    compact_time = 0.0         :: float(), % seconds
    cleanup_kv_count = 0       :: non_neg_integer()
}).
