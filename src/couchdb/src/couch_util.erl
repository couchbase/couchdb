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

-module(couch_util).

-export([priv_dir/0, normpath/1]).
-export([should_flush/0, should_flush/1, to_existing_atom/1]).
-export([rand32/0, implode/2]).
-export([abs_pathname/1,abs_pathname/2, trim/1]).
-export([encodeBase64Url/1, decodeBase64Url/1]).
-export([validate_utf8/1, to_hex/1, parse_term/1, dict_find/3]).
-export([get_nested_json_value/2, json_user_ctx/1]).
-export([proplist_apply_field/2, json_apply_field/2]).
-export([to_binary/1, to_integer/1, to_list/1, url_encode/1]).
-export([verify/2,simple_call/2,shutdown_sync/1]).
-export([get_value/2, get_value/3]).
-export([md5/1, md5_init/0, md5_update/2, md5_final/1]).
-export([reorder_results/2]).
-export([url_strip_password/1]).
-export([encode_doc_id/1]).
-export([brace/1, debrace/1]).
-export([split_iolist/2]).
-export([log_data/2]).
-export([strong_rand_bytes/1]).
-export([parse_view_name/1, log_parse_post/1]).
-export([log_do_parse/1]).
-export([get_view_list/1, find_match/3]).
-export([rand_uniform/2]).
-export([to_json_key_value/2, timestamp_iso8601/1, sanitize/1]).
-export([iso_8601_fmt/3]).

-include("couch_db.hrl").

% arbitrarily chosen amount of memory to use before flushing to disk
-define(FLUSH_MAX_MEM, 10000000).

priv_dir() ->
    case code:priv_dir(couch) of
        {error, bad_name} ->
            % small hack, in dev mode "app" is couchdb. Fixing requires
            % renaming src/couch to src/couch. Not really worth the hassle.
            % -Damien
            code:priv_dir(couchdb);
        Dir -> Dir
    end.

%% Generates N: Lo =< N < Hi
rand_uniform(Lo, Hi) ->
    rand:uniform(Hi - Lo) + Lo - 1.

% Normalize a pathname by removing .. and . components.
normpath(Path) ->
    normparts(filename:split(Path), []).

normparts([], Acc) ->
    filename:join(lists:reverse(Acc));
normparts([".." | RestParts], [_Drop | RestAcc]) ->
    normparts(RestParts, RestAcc);
normparts(["." | RestParts], Acc) ->
    normparts(RestParts, Acc);
normparts([Part | RestParts], Acc) ->
    normparts(RestParts, [Part | Acc]).

% works like list_to_existing_atom, except can be list or binary and it
% gives you the original value instead of an error if no existing atom.
to_existing_atom(V) when is_list(V) ->
    try list_to_existing_atom(V) catch _:_ -> V end;
to_existing_atom(V) when is_binary(V) ->
    try list_to_existing_atom(?b2l(V)) catch _:_ -> V end;
to_existing_atom(V) when is_atom(V) ->
    V.

shutdown_sync(Pid) when not is_pid(Pid)->
    ok;
shutdown_sync(Pid) ->
    MRef = erlang:monitor(process, Pid),
    try
        catch unlink(Pid),
        catch exit(Pid, shutdown),
        receive
        {'DOWN', MRef, _, _, _} ->
            receive
            {'EXIT', Pid, _} ->
                ok
            after 0 ->
                ok
            end
        end
    after
        erlang:demonitor(MRef, [flush])
    end.


simple_call(Pid, Message) ->
    MRef = erlang:monitor(process, Pid),
    try
        Pid ! {self(), Message},
        receive
        {Pid, Result} ->
            Result;
        {'DOWN', MRef, _, _, Reason} ->
            exit(Reason)
        end
    after
        erlang:demonitor(MRef, [flush])
    end.

validate_utf8(Val) when is_binary(Val) ->
    case unicode:characters_to_binary(Val, utf8, utf8) of
    Bin when Val =:= Bin ->
        true;
    _ ->
        false
    end;
validate_utf8(Val) when is_list(Val) ->
    validate_utf8(?l2b(Val)).

to_hex([]) ->
    [];
to_hex(Bin) when is_binary(Bin) ->
    to_hex(binary_to_list(Bin));
to_hex([H|T]) ->
    [to_digit(H div 16), to_digit(H rem 16) | to_hex(T)].

to_digit(N) when N < 10 -> $0 + N;
to_digit(N)             -> $a + N-10.


parse_term(Bin) when is_binary(Bin) ->
    parse_term(binary_to_list(Bin));
parse_term(List) when is_list(List) ->
    {ok, Tokens, _} = erl_scan:string(List ++ "."),
    erl_parse:parse_term(Tokens);
parse_term(Data) ->
    Data.

get_value(Key, List) ->
    get_value(Key, List, undefined).

get_value(Key, List, Default) ->
    case lists:keysearch(Key, 1, List) of
    {value, {Key,Value}} ->
        Value;
    false ->
        Default
    end.

get_nested_json_value({Props}, [Key|Keys]) ->
    case get_value(Key, Props, nil) of
    nil -> throw({not_found, <<"missing json key: ", Key/binary>>});
    Value -> get_nested_json_value(Value, Keys)
    end;
get_nested_json_value(Value, []) ->
    Value;
get_nested_json_value(_NotJSONObj, _) ->
    throw({not_found, json_mismatch}).

proplist_apply_field(H, L) ->
    {R} = json_apply_field(H, {L}),
    R.

json_apply_field(H, {L}) ->
    json_apply_field(H, L, []).
json_apply_field({Key, NewValue}, [{Key, _OldVal} | Headers], Acc) ->
    json_apply_field({Key, NewValue}, Headers, Acc);
json_apply_field({Key, NewValue}, [{OtherKey, OtherVal} | Headers], Acc) ->
    json_apply_field({Key, NewValue}, Headers, [{OtherKey, OtherVal} | Acc]);
json_apply_field({Key, NewValue}, [], Acc) ->
    {[{Key, NewValue}|Acc]}.

json_user_ctx(#db{name=DbName, user_ctx=Ctx}) ->
    {[{<<"db">>, DbName},
            {<<"name">>,Ctx#user_ctx.name},
            {<<"roles">>,Ctx#user_ctx.roles}]}.


% returns a random integer
rand32() ->
    rand_uniform(0, 16#100000000).

% given a pathname "../foo/bar/" it gives back the fully qualified
% absolute pathname.
abs_pathname(" " ++ Filename) ->
    % strip leading whitspace
    abs_pathname(Filename);
abs_pathname([$/ |_]=Filename) ->
    Filename;
abs_pathname(Filename) ->
    {ok, Cwd} = file:get_cwd(),
    {Filename2, Args} = separate_cmd_args(Filename, ""),
    abs_pathname(Filename2, Cwd) ++ Args.

abs_pathname(Filename, Dir) ->
    Name = filename:absname(Filename, Dir ++ "/"),
    OutFilename = filename:join(fix_path_list(filename:split(Name), [])),
    % If the filename is a dir (last char slash, put back end slash
    case string:right(Filename,1) of
    "/" ->
        OutFilename ++ "/";
    "\\" ->
        OutFilename ++ "/";
    _Else->
        OutFilename
    end.

% if this as an executable with arguments, seperate out the arguments
% ""./foo\ bar.sh -baz=blah" -> {"./foo\ bar.sh", " -baz=blah"}
separate_cmd_args("", CmdAcc) ->
    {lists:reverse(CmdAcc), ""};
separate_cmd_args("\\ " ++ Rest, CmdAcc) -> % handle skipped value
    separate_cmd_args(Rest, " \\" ++ CmdAcc);
separate_cmd_args(" " ++ Rest, CmdAcc) ->
    {lists:reverse(CmdAcc), " " ++ Rest};
separate_cmd_args([Char|Rest], CmdAcc) ->
    separate_cmd_args(Rest, [Char | CmdAcc]).

% Is a character whitespace?
is_whitespace($\s) -> true;
is_whitespace($\t) -> true;
is_whitespace($\n) -> true;
is_whitespace($\r) -> true;
is_whitespace(_Else) -> false.


% removes leading and trailing whitespace from a string
trim(String) ->
    String2 = lists:dropwhile(fun is_whitespace/1, String),
    lists:reverse(lists:dropwhile(fun is_whitespace/1, lists:reverse(String2))).

% takes a heirarchical list of dirs and removes the dots ".", double dots
% ".." and the corresponding parent dirs.
fix_path_list([], Acc) ->
    lists:reverse(Acc);
fix_path_list([".."|Rest], [_PrevAcc|RestAcc]) ->
    fix_path_list(Rest, RestAcc);
fix_path_list(["."|Rest], Acc) ->
    fix_path_list(Rest, Acc);
fix_path_list([Dir | Rest], Acc) ->
    fix_path_list(Rest, [Dir | Acc]).


implode(List, Sep) ->
    implode(List, Sep, []).

implode([], _Sep, Acc) ->
    lists:flatten(lists:reverse(Acc));
implode([H], Sep, Acc) ->
    implode([], Sep, [H|Acc]);
implode([H|T], Sep, Acc) ->
    implode(T, Sep, [Sep,H|Acc]).


should_flush() ->
    should_flush(?FLUSH_MAX_MEM).

should_flush(MemThreshHold) ->
    {memory, ProcMem} = process_info(self(), memory),
    BinMem = lists:foldl(fun({_Id, Size, _NRefs}, Acc) -> Size+Acc end,
        0, element(2,process_info(self(), binary))),
    if ProcMem+BinMem > 2*MemThreshHold ->
        garbage_collect(),
        {memory, ProcMem2} = process_info(self(), memory),
        BinMem2 = lists:foldl(fun({_Id, Size, _NRefs}, Acc) -> Size+Acc end,
            0, element(2,process_info(self(), binary))),
        ProcMem2+BinMem2 > MemThreshHold;
    true -> false end.

encodeBase64Url(Url) ->
    Url1 = re:replace(base64:encode(Url), ["=+", $$], ""),
    Url2 = re:replace(Url1, "/", "_", [global]),
    re:replace(Url2, "\\+", "-", [global, {return, binary}]).

decodeBase64Url(Url64) ->
    Url1 = re:replace(Url64, "-", "+", [global]),
    Url2 = re:replace(Url1, "_", "/", [global]),
    Padding = lists:duplicate((4 - iolist_size(Url2) rem 4) rem 4, $=),
    base64:decode(iolist_to_binary([Url2, Padding])).

dict_find(Key, Dict, DefaultValue) ->
    case dict:find(Key, Dict) of
    {ok, Value} ->
        Value;
    error ->
        DefaultValue
    end.

to_binary(V) when is_binary(V) ->
    V;
to_binary(V) when is_list(V) ->
    try
        list_to_binary(V)
    catch
        _:_ ->
            list_to_binary(io_lib:format("~p", [V]))
    end;
to_binary(V) when is_atom(V) ->
    list_to_binary(atom_to_list(V));
to_binary(V) ->
    list_to_binary(io_lib:format("~p", [V])).

to_integer(V) when is_integer(V) ->
    V;
to_integer(V) when is_list(V) ->
    erlang:list_to_integer(V);
to_integer(V) when is_binary(V) ->
    erlang:list_to_integer(binary_to_list(V)).

to_list(V) when is_list(V) ->
    V;
to_list(V) when is_binary(V) ->
    binary_to_list(V);
to_list(V) when is_atom(V) ->
    atom_to_list(V);
to_list(V) ->
    lists:flatten(io_lib:format("~p", [V])).

url_encode(Bin) when is_binary(Bin) ->
    url_encode(binary_to_list(Bin));
url_encode([H|T]) ->
    if
    H >= $a, $z >= H ->
        [H|url_encode(T)];
    H >= $A, $Z >= H ->
        [H|url_encode(T)];
    H >= $0, $9 >= H ->
        [H|url_encode(T)];
    H == $_; H == $.; H == $-; H == $: ->
        [H|url_encode(T)];
    true ->
        case lists:flatten(io_lib:format("~.16.0B", [H])) of
        [X, Y] ->
            [$%, X, Y | url_encode(T)];
        [X] ->
            [$%, $0, X | url_encode(T)]
        end
    end;
url_encode([]) ->
    [].

verify([X|RestX], [Y|RestY], Result) ->
    verify(RestX, RestY, (X bxor Y) bor Result);
verify([], [], Result) ->
    Result == 0.

verify(<<X/binary>>, <<Y/binary>>) ->
    verify(?b2l(X), ?b2l(Y));
verify(X, Y) when is_list(X) and is_list(Y) ->
    case length(X) == length(Y) of
        true ->
            verify(X, Y, 0);
        false ->
            false
    end;
verify(_X, _Y) -> false.

-spec md5(Data::(iolist() | binary())) -> Digest::binary().
md5(Data) ->
    try crypto:hash(md5, Data) catch error:_ -> erlang:md5(Data) end.

-spec md5_init() -> Context::term().
md5_init() ->
    try crypto:hash_init(md5) catch error:_ -> erlang:md5_init() end.

-spec md5_update(Context::term(), Data::(iolist() | binary())) ->
    NewContext::term().
md5_update(Ctx, D) ->
    try crypto:hash_update(Ctx,D) catch error:_ -> erlang:md5_update(Ctx,D) end.

-spec md5_final(Context::term()) -> Digest::binary().
md5_final(Ctx) ->
    try crypto:hash_final(Ctx) catch error:_ -> erlang:md5_final(Ctx) end.

% linear search is faster for small lists, length() is 0.5 ms for 100k list
reorder_results(Keys, SortedResults) when length(Keys) < 100 ->
    [couch_util:get_value(Key, SortedResults) || Key <- Keys];
reorder_results(Keys, SortedResults) ->
    KeyDict = dict:from_list(SortedResults),
    [dict:fetch(Key, KeyDict) || Key <- Keys].

url_strip_password(Url) ->
    re:replace(Url,
        "http(s)?://([^:]+):[^@]+@(.*)$",
        "http\\1://\\2:*****@\\3",
        [{return, list}]).

encode_doc_id(#doc{id = Id}) ->
    encode_doc_id(Id);
encode_doc_id(Id) when is_list(Id) ->
    encode_doc_id(?l2b(Id));
encode_doc_id(<<"_design/", Rest/binary>>) ->
    "_design/" ++ url_encode(Rest);
encode_doc_id(<<"_local/", Rest/binary>>) ->
    "_local/" ++ url_encode(Rest);
encode_doc_id(Id) ->
    url_encode(Id).

debrace(IoList) ->
    JsonBinary = iolist_to_binary(IoList),
    InnerSize = size(JsonBinary) - 2,
    <<"{", Inner:InnerSize/binary, "}">> = JsonBinary,
    Inner.

brace(JsonBinary) ->
    iolist_to_binary(["{", JsonBinary, "}"]).


%% @doc Returns a tuple where the first element contains the leading SplitAt
%% bytes of the original iolist, and the 2nd element is the tail. If SplitAt
%% is larger than byte_size(IoList), return the difference.
-spec split_iolist(IoList::iolist(), SplitAt::non_neg_integer()) ->
    {iolist(), iolist()} | non_neg_integer().

split_iolist(List, SplitAt) ->
    split_iolist(List, SplitAt, []).

split_iolist(List, 0, BeginAcc) ->
    {lists:reverse(BeginAcc), List};
split_iolist([], SplitAt, _BeginAcc) ->
    SplitAt;
split_iolist([Bin | Rest], SplitAt, BeginAcc) when is_binary(Bin), SplitAt > byte_size(Bin) ->
    split_iolist(Rest, SplitAt - byte_size(Bin), [Bin | BeginAcc]);
split_iolist([Bin | Rest], SplitAt, BeginAcc) when is_binary(Bin) ->
    <<Begin:SplitAt/binary,End/binary>> = Bin,
    split_iolist([End | Rest], 0, [Begin | BeginAcc]);
split_iolist([Sublist| Rest], SplitAt, BeginAcc) when is_list(Sublist) ->
    case split_iolist(Sublist, SplitAt, BeginAcc) of
    {Begin, End} ->
        {Begin, [End | Rest]};
    SplitRemaining ->
        split_iolist(Rest, SplitAt - (SplitAt - SplitRemaining), [Sublist | BeginAcc])
    end;
split_iolist([Byte | Rest], SplitAt, BeginAcc) when is_integer(Byte) ->
    split_iolist(Rest, SplitAt - 1, [Byte | BeginAcc]).

log_data(Tag, Arg) when is_binary(Arg) ->
    io_lib:format("<~p>~s</~p>", [Tag, Arg, Tag]);
log_data(Tag, Arg) ->
    io_lib:format("<~p>~p</~p>", [Tag, Arg, Tag]).

strong_rand_bytes(N) ->
    strong_rand_bytes(N, 10).

strong_rand_bytes(N, Retries) ->
    try
        crypto:strong_rand_bytes(N)
    catch
        error:low_entropy when N > 1 ->
            SeedTerm = {erlang:monotonic_time(), erlang:unique_integer()},
            SeedBin = crypto:hash(sha256, term_to_binary(SeedTerm)),
            crypto:rand_seed(SeedBin),
            strong_rand_bytes(N, Retries - 1)
    end.

parse_view_name(Name) ->
    Tokens = string:tokens(couch_util:trim(?b2l(Name)), "/"),
    case [?l2b(couch_httpd:unquote(Token)) || Token <- Tokens] of
        [DDocName, ViewName] ->
            {<<"_design/", DDocName/binary>>, ViewName};
        [<<"_design">>, DDocName, ViewName] ->
            {<<"_design/", DDocName/binary>>, ViewName};
        _ ->
            throw({bad_request, "A `view` property must have the shape"
            " `ddoc_name/view_name`."})
    end.

log_parse_post(Req) ->
    try log_do_parse(Req) of Str -> ?l2b(Str)
    catch _:_ -> "" end.

log_do_parse(#httpd{method='POST'} = Req) ->
    {[{Bucket, {Props}}]} = get_nested_json_value(
        couch_httpd:json_body_obj(Req), [<<"views">>, <<"sets">>]),
    ViewName = get_value(<<"view">>, Props),
    {DDoc, View} = parse_view_name(ViewName),
    [<<"/">>, Bucket, <<"/">>, DDoc, <<"/_view/">>, View].

get_view_list(undefined) ->
    [];
get_view_list({Views}) ->
    case couch_util:get_value(<<"views">>, Views) of
    undefined -> [];
    {ViewDef} -> ViewDef
    end;
get_view_list(_) ->
    [].

find_match([], _, Default) ->
    Default;
find_match([Head|Tail], Prefix, Default) ->
    case string:prefix(Head, Prefix) of
    nomatch ->
        find_match(Tail, Prefix, Default);
    Matched ->
        Matched
    end.

to_json_key_value(Key, Value) when is_list(Value) ->
    {Key, list_to_binary(Value)};
to_json_key_value(Key, Value) ->
    {Key, Value}.

iso_8601_fmt_datetime({{Year,Month,Day},{Hour,Min,Sec}}, DateDelim, TimeDelim) ->
    lists:flatten(
      io_lib:format("~4.10.0B~s~2.10.0B~s~2.10.0BT~2.10.0B~s~2.10.0B~s~2.10.0B",
                    [Year, DateDelim, Month, DateDelim, Day, Hour, TimeDelim,
                     Min, TimeDelim, Sec])).

iso_8601_fmt({{Year,Month,Day},{Hour,Min,Sec}}, Millis, UTCOffset) ->
    TimeS = iso_8601_fmt_datetime({{Year,Month,Day}, {Hour,Min,Sec}}, "-", ":"),
    MilliSecond = case Millis of
                     undefined -> "";
                     _ -> io_lib:format(".~3.10.0B", [Millis])
                  end,
    OffsetS =
        case UTCOffset of
            undefined -> "";
            {0, 0} -> "Z";
            {H, M} ->
                Sign = case H < 0 of
                           true -> "-";
                           false -> "+"
                       end,
                io_lib:format("~s~2.10.0B:~2.10.0B", [Sign, abs(H), M])
        end,
    lists:flatten(TimeS ++ MilliSecond ++ OffsetS).

timestamp_iso8601(Time) ->
    UTCTime = calendar:now_to_universal_time(Time),
    Millis = erlang:element(3, Time) div 1000,
    iso_8601_fmt(UTCTime, Millis, {0,0}).

sanitize(Value) when is_list(Value) ->
    % Sanitize users
    Users = proplists:get_value(disabled_users, Value, []),
    case Users of
        [] -> Value;
        _ ->
            ProcessedUsers = lists:foldl(
            fun ({undefined, _}, Acc) ->
                    Acc;
                ({U, D}, Acc) ->
                    {_, SU} = ns_config_log:sanitize_value(U, [add_salt]),
                    [{binary_to_list(SU), D} | Acc];
                (_, Acc) ->
                    Acc
            end, [], Users),
            lists:keyreplace(disabled_users, 1, Value, {disabled_users, ProcessedUsers})
    end;
sanitize(Value) ->
        Value.
