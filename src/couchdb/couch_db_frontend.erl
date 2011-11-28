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

-module(couch_db_frontend).

-include("couch_db.hrl").

-compile(export_all).

do_db_req(#httpd{user_ctx=UserCtx,path_parts=[DbName|_]}=Req, Fun) ->
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
        {ok, Db} ->
            try
                Fun(Req, Db)
            after
                catch couch_db:close(Db)
            end;
        Error ->
            throw(Error)
    end.

get_db_info(Db) ->
    couch_db:get_db_info(Db).

update_doc(Db, Doc, Options) ->
    couch_db:update_doc(Db, Doc, Options).

-spec ensure_full_commit(any(), integer()) -> {ok, binary()}.
ensure_full_commit(Db, RequiredSeq) ->
    UpdateSeq = couch_db:get_update_seq(Db),
    CommittedSeq = couch_db:get_committed_update_seq(Db),
    case RequiredSeq of
        undefined ->
            couch_db:ensure_full_commit(Db);
        _ ->
            if RequiredSeq > UpdateSeq ->
                    throw({bad_request,
                           "can't do a full commit ahead of current update_seq"});
               RequiredSeq > CommittedSeq ->
                    couch_db:ensure_full_commit(Db);
               true ->
                    {ok, Db#db.instance_start_time}
            end
    end.

check_is_admin(Db) ->
    couch_db:check_is_admin(Db).

handle_changes(ChangesArgs, Req, Db) ->
    couch_changes:handle_changes(ChangesArgs, Req, Db).

start_view_compact(DbName, GroupId) ->
    couch_view_compactor:start_compact(DbName, GroupId).

start_db_compact(Db) ->
    couch_db:start_compact(Db).

cleanup_view_index_files(Db) ->
    couch_view:cleanup_index_files(Db).

get_group_info(Db, DesignId) ->
    couch_view:get_group_info(Db, DesignId).

create_db(DbName, UserCtx) ->
    case couch_server:create(DbName, [{user_ctx, UserCtx}]) of
        {ok, Db} ->
            couch_db:close(Db),
            ok;
        Error ->
            Error
    end.

delete_db(DbName, UserCtx) ->
    couch_server:delete(DbName, [{user_ctx, UserCtx}]).

update_docs(Db, Docs, Options) ->
    couch_db:update_docs(Db, Docs, Options).

update_docs(Db, Docs, Options, replicated_changes) ->
    ok = couch_db:update_docs(Db, Docs, Options),
    {ok, []}.

purge_docs(Db, IdsRevs) ->
    couch_db:purge_docs(Db, IdsRevs).

get_missing_revs(Db, JsonDocIdRevs) ->
    couch_db:get_missing_revs(Db, JsonDocIdRevs).

set_security(Db, SecurityObj) ->
    couch_db:set_security(Db, SecurityObj).

get_security(Db) ->
    couch_db:get_security(Db).

set_revs_limit(Db, Limit) ->
    couch_db:set_revs_limit(Db, Limit).

get_revs_limit(Db) ->
    couch_db:get_revs_limit(Db).

open_doc(Db, DocId, Options) ->
    couch_db:open_doc(Db, DocId, Options).

couch_doc_open(Db, DocId, Options) ->
    case couch_db:open_doc(Db, DocId, Options) of
    {ok, Doc} ->
        Doc;
    Error ->
        throw(Error)
    end.

all_databases() ->
    couch_server:all_databases().

task_status_all() ->
    couch_task_status:all().

restart_core_server() ->
    couch_server_sup:restart_core_server().

config_all() ->
    couch_config:all().

config_get(Section) ->
    couch_config:get(Section).

config_get(Section, Key, Default) ->
    couch_config:get(Section, Key, Default).

config_set(Section, Key, Value, Persist) ->
    couch_config:set(Section, Key, Value, Persist).

config_delete(Section, Key, Persist) ->
    couch_config:delete(Section, Key, Persist).

increment_update_seq(Db) ->
    couch_db:increment_update_seq(Db).

stats_aggregator_all(Range) ->
    couch_stats_aggregator:all(Range).

stats_aggregator_get_json(Key, Range) ->
    couch_stats_aggregator:get_json(Key, Range).

stats_aggregator_collect_sample() ->
    couch_stats_aggregator:collect_sample().

welcome_message(WelcomeMessage) ->
    [
        {couchdb, WelcomeMessage},
        {version, list_to_binary(couch_server:get_version())}
        ] ++ case couch_config:get("vendor") of
        [] ->
            [];
        Properties ->
            [{vendor, {[{?l2b(K), ?l2b(V)} || {K, V} <- Properties]}}]
        end.
