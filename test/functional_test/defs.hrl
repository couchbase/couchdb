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

-define(NODE_INIT_URL, "127.0.0.1:9000/nodes/self/controller/settings").
-define(NODE_RENAME_URL, "127.0.0.1:9000/node/controller/rename").
-define(CREATE_BUCKET_URL, "127.0.0.1:9000/pools/default/buckets").
-define(NODE_CRUD_SETUP_URL, "127.0.0.1:9000/settings/web").
-define(SERVICE_SETUP_URL, "127.0.0.1:9000/node/controller/setupServices").
-define(QUOTA_SETUP_URL, "127.0.0.1:9000/pools/default").
-define(ADD_NODE_URL, "127.0.0.1:9000/controller/addNode").
-define(REBALANCE_URL, "127.0.0.1:9000/controller/rebalance").
-define(CLUSTER_INFO, "127.0.0.1:9000/pools/default").
-define(REBALANCE_PROGRESS_URL, "127.0.0.1:9000/pools/default/rebalanceProgress").
-define(DOC_OPS, "127.0.0.1:9000/pools/default/buckets/").
-define(VIEW_URL, "127.0.0.1:9500/").
-define(SECURITY_URL, "127.0.0.1:9000/settings/security").
-define(DIAG_EVAL, "127.0.0.1:9001/diag/eval").

-define(DEFAULT_BUCKET, "Couch_Test").
-define(USERNAME, "Administrator").
-define(PASSWORD, "asdasd").
-define(BUCKET_QUOTA, "1024").
