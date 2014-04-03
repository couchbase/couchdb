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

-module(cb_auth_info).

-export([get/0, trick_dialyzer/1]).

% This is a fake cb_auth_info service (provided by ns_server in couchbase stack)

get() ->
    % We can't just return `{auth, <<"admin_user">>, <<"admin_passwd">>}` as
    % the dialyzer would complain that it never returns
    % `{error, server_not_ready}`. Hence there's another function to trick the
    % dialyzer. As this module is *only* used to run couchdb without the full
    % stack, this is an acceptable trade-off.
    trick_dialyzer(auth).

trick_dialyzer(auth) ->
    {auth, <<"admin_user">>, <<"admin_passwd">>};
trick_dialyzer(error) ->
    {error, server_not_ready}.
