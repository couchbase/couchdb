%% @author Couchbase <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(bucket_ops).

-export([load_docs/3, generate_docs/2, delete_docs/3]).
-export([create_ddoc/3, delete_ddoc/2, query_view/5]).

-include("defs.hrl").

load_docs(_, [], Loaded) ->
    Loaded;
load_docs(Bucket, [{Id, DocValue}|Rest], Loaded) ->
    Data = lists:flatten(["value=",DocValue]),
    Url = lists:flatten([?DOC_OPS, Bucket, "/docs/", Id]),
    {ok, Code, _Header, _Body} = cluster_ops:request(Url, post,
                                    [{"content-type", "application/x-www-form-urlencoded"}],
                                    Data, 5),
    case Code of
    200 ->
        load_docs(Bucket, Rest, Loaded+1);
    _ ->
        load_docs(Bucket, Rest, Loaded)
    end.

delete_docs(_, [], Deleted)->
    Deleted;
delete_docs(Bucket, [Id|Rest], Deleted) ->
    Url = lists:flatten([?DOC_OPS, Bucket, "/docs/", Id]),
    {ok, Code, _Header, _Body} = cluster_ops:request(Url, delete, [], [], 5),
    case Code of
    200 ->
        delete_docs(Bucket, Rest, Deleted+1);
    _ ->
        delete_docs(Bucket, Rest, Deleted)
    end.

create_ddoc(Bucket, DDocName, Views) ->
    Url = lists:flatten([?VIEW_URL, Bucket, "/_design/", DDocName]),
    cluster_ops:request(Url, put, [{"content-type", "application/json"}],
                        to_json_string([{<<"\"views\"">>, Views}]), 5).

delete_ddoc(Bucket, DDocName) ->
    Url = lists:flatten([?VIEW_URL, Bucket, "/_design/", DDocName]),
    cluster_ops:request(Url, delete, [], [], 5).

query_view(Bucket, DDocName, ViewName, Args, ExpectedCode) ->
    Url = lists:flatten([?VIEW_URL, Bucket, "/_design/", DDocName, "/_view/", ViewName, "?", Args]),
    case cluster_ops:request(Url, get, [], [], 1) of
    {ok, Code, _, Response} ->
        case Code of
        ExpectedCode ->
            ok;
        _ ->
            io:format(standard_error, "~nView response body: ~p~n~n", [Response]),
            etap:bail("View response status is not " ++
                integer_to_list(ExpectedCode) ++ " (got " ++
                integer_to_list(Code) ++ ")")
        end,
        {ok, ejson:decode(Response)};
    {error, timeout} ->
        etap:diag("Query request timed out"),
        {error, timeout};
    Error ->
        Error
    end.

to_json_string([]) ->
    "";
to_json_string(Value) when is_binary(Value) ->
    binary_to_list(Value);
to_json_string({Key, Value}) ->
    "{"++binary_to_list(Key)++":"++to_json_string(Value)++"}";
to_json_string([{Key, Value}|Rest]) ->
    case Rest of
    [] ->
        "{"++binary_to_list(Key)++":"++to_json_string(Value)++"}";
    _ ->
        "{"++binary_to_list(Key)++":"++to_json_string(Value)++"}," ++ to_json_string(Rest)
    end;
to_json_string(Value) ->
    Value.

generate_docs(From, To) ->
    lists:map(
    fun(I) ->
        {iolist_to_binary(["doc", integer_to_list(I)]),
         to_json_string([{<<"value">>, integer_to_list(I)}])
        }
    end,
    lists:seq(From, To-1)).
