% @copyright 2012 Couchbase, Inc.
%
% @author Filipe David Manana  <filipe@couchbase.com>
%
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

-module(file2).

% This module is a workaround against a Windows specific issue when
% doing some file operations concurrently. The issue is that some file
% operations from the 'file' module (such as file:read_file_info/1,2)
% do not open files with all the Windows share flags (FILE_SHARE_READ,
% FILE_SHARE_WRITE, FILE_SHARE_DELETE) - this makes it impossible for
% certain file operations to be done concurrently by different Erlang
% processes against the same file. In this case, all but one of the
% operations, will fail with the Windows error FILE_SHARE_VIOLATION,
% which the Erlang file driver maps to the POSIX error EACCES. This
% applies up to Erlang OTP R15B02 at least (a patch was submitted to
% add all the share flags to all file open operations done by the
% Erlang Windows file driver).
% The same type of issue happens when external processes are holding
% files open without all the share flags. Examples are Windows specific
% background indexing services and antivirus software.
%
% This module ensures we retry file operations (on Windows) for a
% limited period of time if they fail with error EACCES.

-export([open/2, rename/2, delete/1]).
-export([read_file/1, write_file/2, read_file_info/1]).
-export([ensure_dir/1, fold_files/5]).

-define(INIT_SLEEP, 10).
-define(MAX_SLEEP,  200).


open(Filepath, Options) ->
    do_file_op(file, open, [Filepath, Options]).


rename(Source, Dest) ->
    do_file_op(file, rename, [Source, Dest]).


delete(Filepath) ->
    do_file_op(file, delete, [Filepath]).


read_file(Filepath) ->
    do_file_op(file, read_file, [Filepath]).


write_file(Filepath, Bytes) ->
    do_file_op(file, write_file, [Filepath, Bytes]).


read_file_info(Filepath) ->
    do_file_op(file, read_file_info, [Filepath]).


ensure_dir(Path) ->
    do_file_op(filelib, ensure_dir, [Path]).


fold_files(Dir, RegExp, Recursive, Fun, Acc) ->
    do_file_op(filelib, fold_files, [Dir, RegExp, Recursive, Fun, Acc]).


do_file_op(Mod, Fun, Args) ->
    case erlang:apply(Mod, Fun, Args) of
    {error, eacces} = Error ->
        case os:type() of
        {win32, _} ->
            RetryPeriod = couch_config:get(
                "couchdb", "windows_file_op_retry_period", "5000"),
            do_file_op_loop(Mod, Fun, Args, ?INIT_SLEEP, list_to_integer(RetryPeriod));
        _ ->
            Error
        end;
    Else ->
        Else
    end.


do_file_op_loop(_Mod, _Fun, _Args, _Sleep, Left) when Left =< 0 ->
    {error, eacces};
do_file_op_loop(Mod, Fun, Args, Sleep, Left) ->
    ok = timer:sleep(Sleep),
    case erlang:apply(Mod, Fun, Args) of
    {error, eacces} ->
        Sleep2 = erlang:min(Sleep * 2, ?MAX_SLEEP),
        do_file_op_loop(Mod, Fun, Args, Sleep2, Left - Sleep);
    Else ->
        Else
    end.
