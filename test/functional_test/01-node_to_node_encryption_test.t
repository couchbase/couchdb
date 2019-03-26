#!/usr/bin/env escript
%% @author Couchbase <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
%%
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

-include("defs.hrl").

-define(TEST_VIEW, <<"test_view">>).
-define(TEST_DESIGN_DOC, "Test_design_doc").
-define(NUM_DOCS, 4000).

main(_) ->
    etap:plan(2),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end.

test() ->
    %% Create Design Doc
    bucket_ops:create_ddoc(?DEFAULT_BUCKET, ?TEST_DESIGN_DOC,
        [{<<"\"" ,?TEST_VIEW/binary, "\"">>, {<<"\"map\"">>,
                                        <<"\"function (doc, meta) {emit(meta.id, null);}\"">>}}]),

    %% load some data into bucket
    DocList = bucket_ops:generate_docs(0, ?NUM_DOCS),
    Loaded = bucket_ops:load_docs(?DEFAULT_BUCKET, DocList, 0),

    %% Query the ddoc and check if it gives correct number of ddocs
    query_view(2, false, Loaded, "stale=false"),

    %% change the Encryption Level to all. _view_merge will hit ssl port now
    etap:diag("Changing cluster encryption level to 'all'"),
    cluster_ops:request(?SECURITY_URL, post, [{"content-type", "application/x-www-form-urlencoded"}],
			  "clusterEncryptionLevel=all", 5),

    %% change minimum protocol so that ssl port restarts
    %% same behaviour for non ssl port when we change configuartion
    etap:diag("Changing ssl minimum protocol"),
    cluster_ops:request(?DIAG_EVAL, post, [], "ns_config:set(ssl_minimum_protocol, 'tlsv1.2')", 5),

    %% fire a new query when encryption level is set to all
    query_view(5, true, Loaded, "stale=false"),

    etap:diag("Resetting cluster config"),
    reset_cluster().

reset_cluster() ->
    bucket_ops:delete_ddoc(?DEFAULT_BUCKET, ?TEST_DESIGN_DOC),
    cluster_ops:request(?SECURITY_URL, post, [{"content-type", "application/x-www-form-urlencoded"}],
                          "clusterEncryptionLevel=control", 5),
    cluster_ops:request(?DIAG_EVAL, post, [], "ns_config:set(ssl_minimum_protocol, 'tlsv1')", 5).

query_view(0, IsSSL, _DocsLoaded, _) ->
    etap:bail(io_lib:format("query request failed: Is encryption level all? ~p",[IsSSL]));
query_view(NumTries, IsSSL, DocsLoaded, Args) ->
    try
        %% if ssl port is not started it will result in error in response.
        {ok, {[{<<"total_rows">>, Num}, {<<"rows">>, _} | Rest]}} = bucket_ops:query_view(
                                                                        ?DEFAULT_BUCKET,
                                                                        ?TEST_DESIGN_DOC,
								        binary_to_list(?TEST_VIEW),
                                                                        Args, 200),

        case Rest of
        [] ->
            etap:is(Num, DocsLoaded, "Returned correct rows");
        [{<<"errors">>, _ErrorList}] ->
            case IsSSL of
            true ->
                etap:diag("_view_merge got error. Retry query..."),
                timer:sleep(500),
                query_view(NumTries-1, IsSSL, DocsLoaded, Args);
            false ->
                etap:bail("non ssl port not responding")
            end
       end
    catch
       _:_ ->
            timer:sleep(500),
            query_view(NumTries-1, IsSSL, DocsLoaded, Args)
    end.
