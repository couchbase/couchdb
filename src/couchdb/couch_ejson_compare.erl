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

-module(couch_ejson_compare).

-export([less/2, less_json/2]).

-on_load(init/0).

-type raw_json() :: binary() | iolist() | {'json', binary()} | {'json', iolist()}.


init() ->
    LibDir = case couch_config:get("couchdb", "util_driver_dir") of
    undefined ->
        filename:join(couch_util:priv_dir(), "lib");
    LibDir0 ->
        LibDir0
    end,
    NumScheds = erlang:system_info(schedulers),
    (catch erlang:load_nif(filename:join([LibDir, ?MODULE]), NumScheds)),
    case erlang:system_info(otp_release) of
    "R13B03" -> true;
    _ -> ok
    end.


-spec less(EJsonKey1::term(), EJsonKey2::term()) -> -1 .. 1.
less(A, B) ->
    case less_ejson_nif(A, B) of
    {error, _Reason} = Error ->
        throw(Error);
    Else ->
        Else
    end.


-spec less_json(raw_json(), raw_json()) -> -1 .. 1.
less_json(A, B) ->
    case less_json_nif(get_raw_json(A), get_raw_json(B)) of
    {error, _Reason} = Error ->
        throw(Error);
    Else ->
        Else
    end.


less_ejson_nif(_, _) ->
    erlang:nif_error(ejson_compare_nif_not_loaded).


less_json_nif(_, _) ->
    erlang:nif_error(ejson_compare_nif_not_loaded).


get_raw_json({json, RawJson}) ->
    RawJson;
get_raw_json(RawJson) ->
    RawJson.
