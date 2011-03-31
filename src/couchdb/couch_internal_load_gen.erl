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
    io:format("DocBody:~p~n", [DocBody]),
    io:format("Delayed commits set to ~s~n", [couch_config:get("couchdb", "delayed_commits")]),
    couch_server:delete(DbName, []),
    {ok, Db} = couch_server:create(DbName, []),
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
        {<<"total_time_ms">>, timer:now_diff(End, Start) div 1000}
        ]});


handle_req(Req) ->
    send_method_not_allowed(Req, "POST").


generate_full_load(Load) ->
    #load{concurrency_arg=Concurrency} = Load,
    Pids = [spawn_link(fun() -> generate_load(Load) end) || _ <- lists:seq(1, Concurrency)],
    [receive {'EXIT', Pid, Reason} -> Reason end || Pid <- Pids].


write_a_batch_of_docs(Db, DocBatch, MaxToWrite) ->    
    DocBatch2 = [Doc#doc{id=couch_uuids:new()} || Doc <- lists:sublist(DocBatch, MaxToWrite)],
    {ok, _ } = couch_db:update_docs(Db, DocBatch2, []).
    

generate_load(Load) ->
    #load{total_arg=TotalArg,
            batch_arg=BatchArg,
            concurrency_arg=ConcurrencyArg,
            doc=Doc,
            db=Db} = Load,
    Total = TotalArg div ConcurrencyArg,
    DocBatch =lists:duplicate(BatchArg, Doc),

    lists:foreach(fun(_I) ->
            write_a_batch_of_docs(Db, DocBatch, BatchArg)
        end, lists:seq(1, Total div BatchArg)),
    write_a_batch_of_docs(Db, DocBatch, Total rem BatchArg),
    exit(done).
    

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



