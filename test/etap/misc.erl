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

-module(misc).

-export([is_ipv6/0]).

% This is a fake misc service (provided by ns_server in couchbase stack)
% This is used only in standalone couchdb unit tests

is_ipv6() ->
    case os:getenv("ipv6") of
        "true" -> true;
        _ -> false
    end.
