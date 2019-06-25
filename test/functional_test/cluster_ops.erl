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

-module(cluster_ops).

-export([init_setup/0, check_cluster_ports/0]).
-export([node_rename/1, trigger_rebalance/1, add_node/1]).
-export([create_bucket/1]).
-export([get_url_with_auth/1, request/5]).
-include("defs.hrl").

check_cluster_ports() ->
    lists:foldl(fun(Port, Result) ->
                case request("localhost:"++Port, get, [], [], 2) of
                econnrefused ->
                    cluster_not_running;
                {_, _, _, _} ->
                    Result
                end
    end, ok, ["9000", "9001", "9002", "9003"]).

init_node_paths()->
    Data = "data_path=%2Ftmp%2Fdata&index_path=%2Ftmp%2Fdata",
    {ok, Code, _Headers, Body} = request(?NODE_INIT_URL, post,
                                    [{"content-type", "application/x-www-form-urlencoded"}],
                                    Data, 5),
    {ok, Code, Body}.

add_node([]) ->
    ok;
add_node([Node| Rest]) ->
    Data = io_lib:format("hostname=~s&user=~s&password=~s&services=kv",[Node,?USERNAME,?PASSWORD]),
    {ok, Code, _Headers, Body} = request(?ADD_NODE_URL, post,
                                    [{"content-type", "application/x-www-form-urlencoded"}],
                                    Data, 5),
    case error_check(Code, 200, Body) of
    ok ->
        add_node(Rest);
    _ ->
        etap:diag(io_lib:format("Adding of ~p Node failed Reason ~p",[Node, Body])),
        {error, add_node_failure}
    end.

trigger_rebalance(NodesToRemove)->
    {KnownNodes, EjectedNodes} = otp_nodes(NodesToRemove),
    Data = io_lib:format("knownNodes=~s&ejectedNodes=~s",[KnownNodes, EjectedNodes]),
    {ok, Code, _Headers, Body} = request(?REBALANCE_URL, post,
                                    [{"content-type", "application/x-www-form-urlencoded"}],
                                    Data, 5),
    {ok, Code, Body}.

node_rename(Rename) ->
    Data = "hostname=" ++ Rename,
    {ok, Code, _Headers, Body} = request(?NODE_RENAME_URL, post,
                                    [{"content-type", "application/x-www-form-urlencoded"}],
                                    Data, 5),
    {ok, Code, Body}.

create_bucket() ->
    create_bucket(?DEFAULT_BUCKET).

create_bucket(BucketName) ->
    Data = io_lib:format("name=~s&bucketType=couchbase&evictionPolicy=valueOnly&ramQuotaMB=~s",
                        [BucketName, ?BUCKET_QUOTA]),
    {ok, Code, _Headers, Body} = request(?CREATE_BUCKET_URL, post,
                                    [{"content-type", "application/x-www-form-urlencoded"}],
                                    Data, 5),
   {ok, Code, Body}.

cluster_cred_setup() ->
    Data = io_lib:format("username=~s&password=~s&port=SAME",[?USERNAME, ?PASSWORD]),
    {ok, Code, _Headers, Body} = request(?NODE_CRUD_SETUP_URL, post,
                                    [{"content-type", "application/x-www-form-urlencoded"}],
                                    Data, 5),
    {ok, Code, Body}.

quota_setup() ->
    Data = "memoryQuota=4096&indexMemoryQuota=300",
    {ok, Code, _Headers, Body} = request(?QUOTA_SETUP_URL, post,
                                    [{"content-type", "application/x-www-form-urlencoded"}],
                                    Data, 5),
    {ok, Code, Body}.

init_service() ->
    init_service("kv").

init_service(Services) ->
    Data = "services="++ Services,
    {ok, Code, _Headers, Body} = request(?SERVICE_SETUP_URL, post,
                                    [{"content-type", "application/x-www-form-urlencoded"}],
                                    Data, 5),
    {ok, Code, Body}.

init_setup() ->
    %init cluster setup
    {_, Code, Body} = init_node_paths(),
    error_check(Code, 200, Body),
    {_, Code2, Body2} = node_rename("127.0.0.1"),
    error_check(Code2, 200, Body2),
    {_, Code3, Body3} = init_service(),
    error_check(Code3, 200, Body3),
    {_, Code4, Body4} = cluster_cred_setup(),
    error_check(Code4, 200, Body4),
    {_, Code5, Body5} = quota_setup(),
    error_check(Code5, 200, Body5),
    {_, Code6, Body6} = create_bucket(),
    error_check(Code6, 202, Body6),
    add_node(["http://127.0.0.1:9001", "http://127.0.0.1:9002", "http://127.0.0.1:9003"]),
    {_, Code8, Body8} = trigger_rebalance([]),
    error_check(Code8, 200, Body8),
    ok = wait_for_rebalance_finish().

error_check(Code, ExpectedCode, Body) ->
    case Code =:= ExpectedCode of
    true ->
        ok;
    false ->
        etap:bail(io_lib:format("Code received ~p  Reason ~p",[Code, Body]))
    end.

request(Url, Method, Headers, Body, Retry) ->
    Url2 = get_url_with_auth(Url),
    case test_util:request(Url2, Headers, Method, Body) of
    {ok, Code, ResponseHeader, ResponseBody} ->
        case Code of
        200 ->
            {ok, Code, ResponseHeader, ResponseBody};
        202 ->%request is accepted for processing but processing is not yet done
            {ok, Code, ResponseHeader, ResponseBody};
        _ ->
            case Retry of
            0 ->
                {ok, Code, ResponseHeader, ResponseBody};
            _ ->
                request(Url, Method, Headers, Body, Retry-1)
	    end
        end;
    {error, {econnrefused, _}} ->
        econnrefused;
    {error, {nxdomain, _}} ->
        econnrefused
    end.

otp_nodes(NodesToRemove) ->
    {ok, _Code, _Header, Body} = request(?CLUSTER_INFO, get, [], [], 5),
    {Response} = ejson:decode(Body),
    {_, NodeInfo} = lists:keyfind(<<"nodes">>, 1, Response),
    {KnownNodes, EjectedNodes, NodesToRemove} = lists:foldl(fun({R},
                                    {KnownNode, EjectedNode, NodesToRemove2}) ->
                        {_, OtpNode} = lists:keyfind(<<"otpNode">>, 1, R),
			{_, HostName} = lists:keyfind(<<"hostname">>, 1, R),
                        case lists:keymember(HostName, 2, NodesToRemove) of
			true ->
				{[binary_to_list(OtpNode) | KnownNode],
                                 [binary_to_list(OtpNode) | EjectedNode], NodesToRemove2};
			false ->
				{[binary_to_list(OtpNode) | KnownNode],
                                  EjectedNode, NodesToRemove2}
			end
                    end, {[],[],NodesToRemove}, NodeInfo),
    {lists:flatten(lists:join(",", KnownNodes)), lists:flatten(lists:join(",", EjectedNodes))}.

wait_for_rebalance_finish() ->
    timer:sleep(3*1000),
    case is_rebalance_finished() of
    true ->
        ok;
    false ->
        wait_for_rebalance_finish();
    Reason ->
        etap:diag("Rebalance failed with reason ~p",[Reason])
    end.

is_rebalance_finished() ->
    {ok, _Code, _Header, Body} = request(?REBALANCE_PROGRESS_URL, get, [], [], 5),
    {Response} = ejson:decode(Body),
    {_, Value} = lists:keyfind(<<"status">>, 1, Response),
    case binary_to_list(Value) of
    "running" -> false;
    _ ->
        case lists:keyfind(<<"errorMessage">>, 1, Response) of
        false -> true;
        {<<"errorMessage">>, Reason} ->
	    Reason
        end
    end.

get_url_with_auth(Url) ->
    lists:flatten(["http://", ?USERNAME, ":", ?PASSWORD, "@", Url]).
