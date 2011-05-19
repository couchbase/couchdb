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

-module(couch_internal_load_gen).

-include("couch_db.hrl").

-import(couch_httpd, [
    send_json/2,
    send_json/3,
    send_method_not_allowed/2
]).

-import(couch_util, [
    get_value/2,
    get_value/3
]).

-export([handle_req/1, generate_full_load/1]).

-record(load, {
          dbname,
          db,
          doc,
          starttime,
          total_arg,
          batch_arg,
          concurrency_arg,
          rounds_arg=10,
          delayed_commits = false,
          optimistic = false
         }).

parse_load_qs_args(Req) ->
    lists:foldl(fun({Key, Value}, Load) ->
        case {Key, Value} of
        {"db", Value} ->
            Load#load{dbname=?l2b(Value)};
        {"total", Value} ->
            Load#load{total_arg=list_to_integer(Value)};
        {"batch", Value} ->
            Load#load{batch_arg=list_to_integer(Value)};
        {"concurrency", Value} ->
            Load#load{concurrency_arg=list_to_integer(Value)};
        {"rounds", Value} ->
            Load#load{rounds_arg=list_to_integer(Value)};
        {"delayed_commits", Value} ->
            Load#load{delayed_commits = (Value =:= "true")};
        {"optimistic", Value} ->
            Load#load{optimistic = (Value =:= "true")}
        end
    end, #load{}, couch_httpd:qs(Req)).


handle_req(#httpd{method = 'POST'} = Req) ->
    process_flag(trap_exit, true),
    #load{dbname=DbName, delayed_commits=DelayedCommits} = Load0 = parse_load_qs_args(Req),
    PrevDelayedCommits = couch_config:get("couchdb", "delayed_commits", "true"),
    ok = couch_config:set("couchdb", "delayed_commits", atom_to_list(DelayedCommits)),
    DocBody = couch_httpd:json_body_obj(Req),
    couch_server:delete(DbName, []),
    {ok, Db} = couch_server:create(DbName, [{user_ctx, #user_ctx{roles=[<<"_admin">>]}}]),
    ok = couch_db:set_revs_limit(Db, 1),
    Load = Load0#load{doc=couch_doc:from_json_obj(DocBody),db=Db},
    Start = erlang:now(),
    generate_full_load(Load),
    End = erlang:now(),
    couch_db:close(Db),
    couch_server:delete(DbName, []),
    ok = couch_config:set("couchdb", "delayed_commits", PrevDelayedCommits),
    send_json(Req, 200,{[
        {<<"db">>, Load#load.dbname},
        {<<"total">>, Load#load.total_arg},
        {<<"batch">>, Load#load.batch_arg},
        {<<"concurrency">>, Load#load.concurrency_arg},
        {<<"rounds">>, Load#load.rounds_arg},
        {<<"delayed_commits">>, Load#load.delayed_commits},
        {<<"optimistic">>, Load#load.optimistic},
        {<<"total_time_ms">>, timer:now_diff(End, Start) div 1000}
        ]});

handle_req(Req) ->
    send_method_not_allowed(Req, "POST").


generate_full_load(#load{concurrency_arg=Concurrency,rounds_arg=Rounds}=Load) ->
    Self = self(),
    Pids = [spawn_link(fun() -> prep_and_generate_load(Load, N, Self) end) || N <- lists:seq(1, Concurrency)],
    generate_full_load(Load,Pids,Rounds,[]).

generate_full_load(_Load, Pids, 0, TimesAcc) ->
    [Pid ! stop || Pid <- Pids],
    [receive {'EXIT', Pid, _} -> ok end || Pid <- Pids],
    lists:reverse(TimesAcc);
generate_full_load(#load{total_arg=Total,rounds_arg=Rounds,db=Db}=Load,
        Pids, RoundsLeft, TimesAcc) ->
    Start = erlang:now(),
    [Pid ! do_round || Pid <- Pids],
    [receive {Pid, batch_complete} -> ok end || Pid <- Pids],
    couch_db:ensure_full_commit(Db),
    Time = timer:now_diff(erlang:now(), Start) div 1000,
    if Rounds == RoundsLeft ->
        io:format("Initialized and committed ~p docs in ~p ms (~p/s)~n", [Total, Time, (1000 * Total) div Time]);
    true ->
        io:format("Updated and committed ~p docs in ~p ms (~p/s)~n", [Total, Time, (1000 * Total) div Time])
    end,
    generate_full_load(Load, Pids, RoundsLeft-1, [Time|TimesAcc]).

prep_batches(Load, WorkerNum) ->
    #load{total_arg=TotalArg,
            batch_arg=BatchArg,
            concurrency_arg=ConcurrencyArg,
            doc=Doc} = Load,
    Total = TotalArg div ConcurrencyArg,
    TotalBatches = Total div BatchArg,
    DocBatchUnprepped = lists:duplicate(BatchArg, Doc),
    BatchesUnprepped0 = lists:duplicate(TotalBatches, DocBatchUnprepped),
    case Total rem BatchArg of
    0 ->
        BatchesUnprepped = BatchesUnprepped0;
    Rem ->
        BatchesUnprepped =
                [BatchesUnprepped0, lists:sublist(DocBatchUnprepped, Rem)]
    end,
    {DocBatches, _} =
    lists:mapfoldl(fun(DocBatch, DocNum) ->
            lists:mapfoldl(fun(Doc2, DocNum2) ->
                    B = ?l2b(integer_to_list(DocNum2)),
                    {Doc2#doc{id= <<"key-", B/binary>>}, DocNum2 + 1}
                end, DocNum, DocBatch)
        end, WorkerNum * Total, BatchesUnprepped),
    DocBatches.

prep_and_generate_load(Load, WorkerNum, Parent) ->
    Batches = prep_batches(Load, WorkerNum),
    generate_load(Load, Batches, Parent).

generate_load(#load{db=OldDb}=Load, Batches, Parent) ->
    receive
    do_round ->
        ok;
    stop ->
        exit(done)
    end,
    case Load#load.optimistic of
    true ->
        Db = OldDb;
    false ->
        {ok, Db} = couch_db:open(
            OldDb#db.name, [{user_ctx, #user_ctx{roles=[<<"_admin">>]}}])
    end,
    Batches2 =
    lists:map(fun(DocBatch) ->
            {ok, Results} = couch_db:update_docs(Db, DocBatch, [optimistic]),
            lists:zipwith(fun(Doc,{ok, {Pos, RevId}}) ->
                Doc#doc{revs={Pos,[RevId]}}
                end, DocBatch, Results)
        end, Batches),
    Parent ! {self(), batch_complete},
    case Load#load.optimistic of
    true ->
        ok;
    false ->
        couch_db:close(Db)
    end,
    generate_load(Load, Batches2, Parent).
