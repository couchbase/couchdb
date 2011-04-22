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

-export([handle_req/1, generate_full_load/1,do_it/0,do_it2/0]).

-record(load, {
          dbname,
          db,
          doc,
          starttime,
          total_arg,
          batch_arg,
          concurrency_arg,
          rounds_arg=10,
          delayed_commits = "false"
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
            Load#load{delayed_commits=Value}
        end
    end, #load{}, couch_httpd:qs(Req)).


handle_req(#httpd{method = 'POST'} = Req) ->
    process_flag(trap_exit, true),
    #load{dbname=DbName, delayed_commits=DelayedCommits} = Load0 = parse_load_qs_args(Req),
    PrevDelayedCommits = couch_config:get("couchdb", "delayed_commits", "true"),
    ok = couch_config:set("couchdb", "delayed_commits", DelayedCommits),
    DocBody = couch_httpd:json_body_obj(Req),
    io:format("Delayed commits set to ~s~n", [couch_config:get("couchdb", "delayed_commits")]),
    couch_server:delete(DbName, []),
    {ok, Db} = couch_server:create(DbName, [{user_ctx, #user_ctx{roles=[<<"_admin">>]}}]),
    ok = couch_db:set_revs_limit(Db, 1),
    Load = Load0#load{doc=couch_doc:from_json_obj(DocBody),db=Db},
    Start = erlang:now(),
    generate_full_load(Load),
    End = erlang:now(),
    couch_db:close(Db),
    ok = couch_config:set("couchdb", "delayed_commits", PrevDelayedCommits),
    send_json(Req, 200,{[
        {<<"db">>, Load#load.dbname},
        {<<"total">>, Load#load.total_arg},
        {<<"batch">>, Load#load.batch_arg},
        {<<"concurrency">>, Load#load.concurrency_arg},
        {<<"rounds">>, Load#load.rounds_arg},
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
generate_full_load(Load, Pids, RoundsLeft, TimesAcc) ->
    Start = erlang:now(),
    [Pid ! do_round || Pid <- Pids],
    [receive {Pid, batch_complete} -> ok end || Pid <- Pids],
    Time = timer:now_diff(erlang:now(), Start)/1000,
    io:format("Updated in ~p ms~n", [Time]),
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

generate_load(#load{db=Db}=Load, Batches, Parent) ->    
    receive
    do_round ->
        ok;
    stop ->
        exit(done)
    end,
    Batches2 =
    lists:map(fun(DocBatch) ->
            {ok, Results} = couch_db:update_docs(Db, DocBatch, [optimistic]),
            lists:zipwith(fun(Doc,{ok, {Pos, RevId}}) ->
                Doc#doc{revs={Pos,[RevId]}}
                end, DocBatch, Results)
        end, Batches),
    Parent ! {self(), batch_complete},
    generate_load(Load, Batches2, Parent).

do_it() ->
    {ok, Fd} = couch_file:open("test.foo",[create,overwrite]),
    write_to_file(Fd, <<1:800000>>, 2700),
    couch_file:sync(Fd),
    couch_file:close(Fd).

    

do_it2() ->
    file:delete("test.foo"),
    {ok, Fd} = file:open("test.foo",[binary, raw, append]),
    write_to_file2(Fd, <<1:800000>>, 2700),
    file:sync(Fd),
    file:close(Fd).

write_to_file(_Fd, _Doc, 0) ->
    ok;
write_to_file(Fd, Data, N) ->
    {ok, _} = couch_file:append_binary(Fd, Data),
    write_to_file(Fd, Data, N -1).

write_to_file2(_Fd, _Doc, 0) ->
    ok;
write_to_file2(Fd, Data, N) ->
    ok = file:write(Fd, Data),
    write_to_file2(Fd, Data, N -1).



