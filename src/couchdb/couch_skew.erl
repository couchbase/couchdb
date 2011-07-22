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

-module(couch_skew).

-export([new/0, size/1, in/3, out/2, min/1]).

-define(null, []).

new() ->
    ?null.

size(?null) ->
    0;
size({Sz, _, _, _}) ->
    Sz.

in(X, _LessFun, ?null) ->
    {1, X, ?null, ?null};
in(X, LessFun, A) ->
    merge(LessFun, {1, X, ?null, ?null}, A).

out(LessFun, {_Sz, X, A, B}) ->
    {X, merge(LessFun, A, B)}.

min({_, X, _, _}) ->
    X.

merge(_LessFun, A, ?null) ->
    A;
merge(_LessFun, ?null, B) ->
    B;
merge(LessFun, {_, Xa, _, _} = A, {_, Xb, _, _} = B) ->
    case LessFun(Xa, Xb) of
    true ->
        join(LessFun, A, B);
    false ->
        join(LessFun, B, A)
    end.

join(LessFun, {Sz1, X, A, B}, {Sz2, _, _, _} = C) ->
    {Sz1 + Sz2, X, B, merge(LessFun, A, C)}.
