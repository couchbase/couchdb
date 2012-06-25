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

-module(couch_doc).

-export([parse_rev/1,parse_revs/1,rev_to_str/1,revs_to_strs/1]).
-export([from_json_obj/1,to_json_obj/2,to_json_obj/1,to_json_bin/1,from_binary/3]).
-export([validate_docid/1,with_uncompressed_body/1]).
-export([with_ejson_body/1,with_json_body/1]).
-export([to_raw_json_binary_views/1]).

-include("couch_db.hrl").


% helpers used by to_json_obj
to_json_rev(0, _) ->
    [];
to_json_rev(Start, RevId) ->
    [{<<"rev">>, ?l2b([integer_to_list(Start),"-",revid_to_str(RevId)])}].

to_ejson_body(_ContentMeta, {Body}) ->
    {<<"json">>, {Body}};
to_ejson_body(?CONTENT_META_JSON, <<"{}">>) ->
    {<<"json">>, {[]}};
to_ejson_body(?CONTENT_META_JSON, Body) ->
    {R} = ?JSON_DECODE(Body),
    {<<"json">>, {R}};
to_ejson_body(ContentMeta, Body)
        when ContentMeta /= ?CONTENT_META_JSON ->
    {<<"base64">>, iolist_to_binary(base64:encode(iolist_to_binary(Body)))}.


revid_to_str(RevId) ->
    ?l2b(couch_util:to_hex(RevId)).

rev_to_str({Pos, RevId}) ->
    ?l2b([integer_to_list(Pos),"-",revid_to_str(RevId)]).
                    
                    
revs_to_strs([]) ->
    [];
revs_to_strs([{Pos, RevId}| Rest]) ->
    [rev_to_str({Pos, RevId}) | revs_to_strs(Rest)].

to_json_meta(Meta) ->
    lists:map(
        fun({local_seq, Seq}) ->
            {<<"local_seq">>, Seq}
        end, Meta).

revid_to_memcached_meta(<<_Cas:64, Expiration:32, Flags:32>> = _RevId) ->
    {Expiration, Flags};
revid_to_memcached_meta(_) ->
    nil.

content_meta_to_memcached_meta(?CONTENT_META_JSON) ->
    nil;
content_meta_to_memcached_meta(?CONTENT_META_INVALID_JSON) ->
    <<"invalid_json">>;
content_meta_to_memcached_meta(?CONTENT_META_INVALID_JSON_KEY) ->
    <<"invalid_key">>;
content_meta_to_memcached_meta(?CONTENT_META_NON_JSON_MODE) ->
    <<"non-JSON mode">>;
content_meta_to_memcached_meta(_Other) ->
    <<"raw">>.

to_memcached_meta(#doc{rev={_, RevId},content_meta=Meta}) ->
    case content_meta_to_memcached_meta(Meta) of
    nil ->
        [];
    AttReason ->
        [{<<"att_reason">>, AttReason}]
    end ++
    case revid_to_memcached_meta(RevId) of
    nil ->
        [];
    {Exp, Flags} ->
        [{<<"expiration">>, Exp}, {<<"flags">>, Flags}]
    end.

to_deleted_meta(true) ->
    [{<<"deleted">>, true}];
to_deleted_meta(_) ->
    [].

to_full_ejson_meta(#doc{id=Id,deleted=Del,rev={Start, RevId},
        meta=Meta, content_meta=ContentMeta}=Doc, IncludeType) ->
    {
        [json_id(Id)]
        ++ to_json_rev(Start, RevId)
        ++ to_json_meta(Meta)
        ++ to_memcached_meta(Doc)
        ++ to_deleted_meta(Del)
        ++ case {IncludeType, ContentMeta} of
        {true, ?CONTENT_META_JSON} ->
            [{<<"type">>, <<"json">>}];
        {true, _} ->
            [{<<"type">>, <<"base64">>}];
        _ ->
           []
        end
    }.

to_json_obj(Doc0)->
    to_json_obj(Doc0, []).

to_json_obj(Doc0, _Options)->
    JSONBin = to_json_bin(Doc0),
    ?JSON_DECODE(JSONBin).

to_json_bin(Doc0)->
    Doc = with_json_body(Doc0),
    DocMeta = ?JSON_ENCODE(to_full_ejson_meta(Doc, false)),
    {DocBody, _DocMeta} = to_raw_json_binary_views(Doc),
    case Doc#doc.content_meta of
    ?CONTENT_META_JSON ->
        <<"{\"meta\":", DocMeta/binary, ",\"json\":", DocBody/binary, "}">>;
    _ -> % encode as raw byte array (make conditional...)
        <<"{\"meta\":", DocMeta/binary, ",\"base64\":", DocBody/binary, "}">>
    end.



mk_json_doc_from_binary(<<?LOCAL_DOC_PREFIX, _/binary>> = Id, Value) ->
    case ejson:validate(Value, <<"_$">>) of
    {ok, JsonBinary} ->
        #doc{id=Id, body = JsonBinary};
    Error ->
        throw(Error)
    end;
mk_json_doc_from_binary(Id, Value) ->
    case ejson:validate(Value, <<"_$">>) of
    {error, invalid_json} ->
        #doc{id=Id, body = Value,
            content_meta = ?CONTENT_META_INVALID_JSON};
    {error, private_field_set} ->
        #doc{id=Id, body = Value,
            content_meta = ?CONTENT_META_INVALID_JSON_KEY};
    {error, garbage_after_value} ->
        #doc{id=Id, body = Value,
            content_meta = ?CONTENT_META_INVALID_JSON};
    {ok, JsonBinary} ->
        #doc{id=Id, body = couch_compress:compress(JsonBinary),
            content_meta = ?CONTENT_META_JSON bor ?CONTENT_META_SNAPPY_COMPRESSED}
    end.

from_binary(Id, Value, _WantJson=true) ->
    mk_json_doc_from_binary(Id, Value);
from_binary(Id, Value, false) ->
    #doc{id=Id, body = Value,
            content_meta = ?CONTENT_META_NON_JSON_MODE}.

from_json_obj({Props}) ->
    transfer_fields(Props, #doc{});

from_json_obj(_Other) ->
    throw({bad_request, "Document must be a JSON object"}).

parse_revid(RevId) when is_binary(RevId) ->
    parse_revid(?b2l(RevId));
parse_revid(RevId) ->
    Size = length(RevId),
    RevInt = erlang:list_to_integer(RevId, 16),
     <<RevInt:(Size*4)>>.


parse_rev(Rev) when is_binary(Rev) ->
    parse_rev(?b2l(Rev));
parse_rev(Rev) when is_list(Rev) ->
    SplitRev = lists:splitwith(fun($-) -> false; (_) -> true end, Rev),
    case SplitRev of
        {Pos, [$- | RevId]} -> {list_to_integer(Pos), parse_revid(RevId)};
        _Else -> throw({bad_request, <<"Invalid rev format">>})
    end;
parse_rev(_BadRev) ->
    throw({bad_request, <<"Invalid rev format">>}).

parse_revs([]) ->
    [];
parse_revs([Rev | Rest]) ->
    [parse_rev(Rev) | parse_revs(Rest)].


validate_docid(Id) when is_binary(Id) ->
    ok;

validate_docid(Id) ->
    ?LOG_DEBUG("Document id is not a string: ~p", [Id]),
    throw({bad_request, <<"Document id must be a string">>}).


transfer_meta([], Doc) ->
    Doc;

transfer_meta([{<<"id">>, Id} | Rest], Doc) when is_list(Id) ->
    BinId = list_to_binary(Id),
    validate_docid(BinId),
    transfer_meta(Rest, Doc#doc{id=BinId});

transfer_meta([{<<"id">>, Id} | Rest], Doc) ->
    validate_docid(Id),
    transfer_meta(Rest, Doc#doc{id=Id});

transfer_meta([{<<"rev">>, Rev} | Rest], #doc{rev={0, _}}=Doc) ->
    {Pos, RevId} = parse_rev(Rev),
    transfer_meta(Rest,
            Doc#doc{rev={Pos, RevId}});

transfer_meta([{<<"rev">>, _Rev} | Rest], Doc) ->
    % we already got the rev from the _revisions
    transfer_meta(Rest, Doc);

transfer_meta([{<<"deleted">>, B} | Rest], Doc) when is_boolean(B) ->
    transfer_meta(Rest, Doc#doc{deleted=B});

transfer_meta([{_Other, _} | Rest], Doc) ->
    % Ignore other meta
    transfer_meta(Rest, Doc).


transfer_fields([], #doc{body={Fields}}=Doc) when is_list(Fields) ->
    % convert fields back to json object
    Doc#doc{body=?JSON_ENCODE({Fields}), content_meta=?CONTENT_META_JSON};

transfer_fields([], #doc{}=Doc) ->
    Doc;

% if the body is nested we can transfer it without care for special fields.
transfer_fields([{<<"json">>, {JsonProps}} | Rest], Doc) ->
    transfer_fields(Rest, Doc#doc{body={JsonProps}, content_meta=?CONTENT_META_JSON});

% in case the body is a blob we transfer it as base64.
transfer_fields([{<<"base64">>, Bin} | Rest], Doc) ->
    transfer_fields(Rest, Doc#doc{body=base64:decode(Bin), content_meta=?CONTENT_META_NON_JSON_MODE}); % todo base64

transfer_fields([{<<"meta">>, {Meta}} | Rest], Doc) ->
    DocWithMeta = transfer_meta(Meta, Doc),
    transfer_fields(Rest, DocWithMeta);

% unknown top level field
transfer_fields([{Name, _} | _], _) ->
    throw({doc_validation,
        ?l2b(io_lib:format("User data must be in the `json` field, please nest `~s`", [Name]))}).


with_ejson_body(Doc) ->
    Uncompressed = with_uncompressed_body(Doc),
    #doc{body = Body, content_meta=Meta} = Uncompressed,
    {_Type, EJSONBody}= to_ejson_body(Meta, Body),
    Uncompressed#doc{body = EJSONBody}.

with_json_body(Doc) ->
    case with_uncompressed_body(Doc) of
    #doc{body = Body} = Doc2 when is_tuple(Body)->
        Doc2#doc{body = ?JSON_ENCODE(Body), content_meta=?CONTENT_META_JSON};
    #doc{body = Body} = Doc2 when is_binary(Body)->
        Doc2;
    #doc{} = Doc2 ->
        Doc2
    end.


with_uncompressed_body(#doc{body = {_}} = Doc) ->
    Doc;
with_uncompressed_body(#doc{body = Body, content_meta = Meta} = Doc)
        when (Meta band ?CONTENT_META_SNAPPY_COMPRESSED) > 0 ->
    NewMeta = Meta band (bnot ?CONTENT_META_SNAPPY_COMPRESSED),
    Doc#doc{body = couch_compress:decompress(Body), content_meta = NewMeta};
with_uncompressed_body(Doc) ->
    Doc.


json_id(Id) ->
    case couch_util:validate_utf8(Id) of
    false -> % encode as raw byte array
        {<<"id">>, binary_to_list(iolist_to_binary(Id))};
    _ ->
        {<<"id">>, Id}
    end.

to_raw_json_binary_views(Doc0) ->
    Doc = with_json_body(Doc0),
    MetaBin = ?JSON_ENCODE(to_full_ejson_meta(Doc, true)),
    ContentBin = case Doc#doc.content_meta of
    ?CONTENT_META_JSON ->
        iolist_to_binary(Doc#doc.body);
    _ -> % encode as raw byte array (make conditional...)
        iolist_to_binary([<<"\"">>, base64:encode(iolist_to_binary(Doc#doc.body)), <<"\"">>])
    end,
    {ContentBin, MetaBin}.
