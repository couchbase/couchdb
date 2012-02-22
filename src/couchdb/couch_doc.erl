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
-export([from_json_obj/1,to_json_obj/2,from_binary/3]).
-export([validate_docid/1,with_uncompressed_body/1]).
-export([with_ejson_body/1,with_json_body/1]).
-export([to_raw_json_binary/1]).

-include("couch_db.hrl").


% helpers used by to_json_obj
to_json_rev(0, _) ->
    [];
to_json_rev(Start, RevId) ->
    [{<<"_rev">>, ?l2b([integer_to_list(Start),"-",revid_to_str(RevId)])}].

to_ejson_body(true = _IsDeleted, ContentMeta, Body) ->
    to_ejson_body(false, ContentMeta, Body) ++ [{<<"_deleted">>, true}];
to_ejson_body(false, _ContentMeta, {Body}) ->
    Body;
to_ejson_body(false, ?CONTENT_META_JSON, <<"{}">>) ->
    [];
to_ejson_body(false, ?CONTENT_META_JSON, Body) ->
    {R} = ?JSON_DECODE(Body),
    R;
to_ejson_body(false, ContentMeta, _Body)
        when ContentMeta /= ?CONTENT_META_JSON ->
    [].


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
            {<<"_local_seq">>, Seq}
        end, Meta).

revid_to_memcached_meta(<<_Cas:64, Expiration:32, Flags:32>>) ->
    [{<<"$expiration">>, Expiration}, {<<"$flags">>, Flags}];
revid_to_memcached_meta(_) ->
    [].

content_meta_to_memcached_meta(?CONTENT_META_JSON) ->
    [];
content_meta_to_memcached_meta(?CONTENT_META_INVALID_JSON) ->
    [{<<"$att_reason">>, <<"invalid_json">>}];
content_meta_to_memcached_meta(?CONTENT_META_INVALID_JSON_KEY) ->
    [{<<"$att_reason">>, <<"invalid_key">>}];
content_meta_to_memcached_meta(?CONTENT_META_NON_JSON_MODE) ->
    [{<<"$att_reason">>, <<"non-JSON mode">>}].

to_memcached_meta(#doc{rev={_, RevId},content_meta=Meta}) ->
    revid_to_memcached_meta(RevId) ++ content_meta_to_memcached_meta(Meta).

to_json_obj(#doc{id=Id,deleted=Del,rev={Start, RevId},
        meta=Meta}=Doc0, _Options)->
    Doc = with_uncompressed_body(Doc0),
    #doc{body=Body,content_meta=ContentMeta} = Doc,
    {[{<<"_id">>, Id}]
        ++ to_json_rev(Start, RevId)
        ++ to_json_meta(Meta)
        ++ to_ejson_body(Del, ContentMeta, Body)
        ++ to_memcached_meta(Doc)
    }.


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

from_binary(Id, Value, WantJson) ->
    case WantJson of
    true ->
        mk_json_doc_from_binary(Id, Value);
    _ ->
        #doc{id=Id, body = Value,
                content_meta = ?CONTENT_META_NON_JSON_MODE}
    end.

from_json_obj({Props}) ->
    transfer_fields(Props, #doc{body=[]});

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
    case couch_util:validate_utf8(Id) of
        false -> throw({bad_request, <<"Document id must be valid UTF-8">>});
        true -> ok
    end,
    case Id of
    <<"_design/", _/binary>> -> ok;
    <<"_local/", _/binary>> -> ok;
    <<"_", _/binary>> ->
        throw({bad_request, <<"Only reserved document ids may start with underscore.">>});
    _Else -> ok
    end;
validate_docid(Id) ->
    ?LOG_DEBUG("Document id is not a string: ~p", [Id]),
    throw({bad_request, <<"Document id must be a string">>}).

transfer_fields([], #doc{body=Fields}=Doc) ->
    % convert fields back to json object
    Doc#doc{body=?JSON_ENCODE({lists:reverse(Fields)})};

transfer_fields([{<<"_id">>, Id} | Rest], Doc) ->
    validate_docid(Id),
    transfer_fields(Rest, Doc#doc{id=Id});

transfer_fields([{<<"_rev">>, Rev} | Rest], #doc{rev={0, _}}=Doc) ->
    {Pos, RevId} = parse_rev(Rev),
    transfer_fields(Rest,
            Doc#doc{rev={Pos, RevId}});

transfer_fields([{<<"_rev">>, _Rev} | Rest], Doc) ->
    % we already got the rev from the _revisions
    transfer_fields(Rest,Doc);

transfer_fields([{<<"_deleted">>, B} | Rest], Doc) when is_boolean(B) ->
    transfer_fields(Rest, Doc#doc{deleted=B});

% ignored fields
transfer_fields([{<<"_revs_info">>, _} | Rest], Doc) ->
    transfer_fields(Rest, Doc);
transfer_fields([{<<"_local_seq">>, _} | Rest], Doc) ->
    transfer_fields(Rest, Doc);
transfer_fields([{<<"_conflicts">>, _} | Rest], Doc) ->
    transfer_fields(Rest, Doc);
transfer_fields([{<<"_deleted_conflicts">>, _} | Rest], Doc) ->
    transfer_fields(Rest, Doc);

% special fields for replication documents
transfer_fields([{<<"_replication_state">>, _} = Field | Rest],
    #doc{body=Fields} = Doc) ->
    transfer_fields(Rest, Doc#doc{body=[Field|Fields]});
transfer_fields([{<<"_replication_state_time">>, _} = Field | Rest],
    #doc{body=Fields} = Doc) ->
    transfer_fields(Rest, Doc#doc{body=[Field|Fields]});
transfer_fields([{<<"_replication_id">>, _} = Field | Rest],
    #doc{body=Fields} = Doc) ->
    transfer_fields(Rest, Doc#doc{body=[Field|Fields]});

% unknown special field
transfer_fields([{<<"_",Name/binary>>, _} | _], _) ->
    throw({doc_validation,
            ?l2b(io_lib:format("Bad special document member: _~s", [Name]))});

transfer_fields([Field | Rest], #doc{body=Fields}=Doc) ->
    transfer_fields(Rest, Doc#doc{body=[Field|Fields]}).


with_ejson_body(Doc) ->
    Uncompressed = with_uncompressed_body(Doc),
    #doc{body = Body, content_meta=Meta} = Uncompressed,
    Uncompressed#doc{body = {to_ejson_body(false, Meta, Body)}}.

with_json_body(Doc) ->
    case with_uncompressed_body(Doc) of
    #doc{body = Body} = Doc2 when is_tuple(Body)->
        Doc2#doc{body = ?JSON_ENCODE(Body)};
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


to_raw_json_binary(Doc) ->
    #doc{
        id = Id,
        json = Json,
        rev = {Start, RevId},
        meta = ContentMeta
    } = with_json_body(Doc),
    iolist_to_binary([
        <<"{\"_id\":\"">>, Id, <<"\"">>,
        case Start of
        0 ->
            <<>>;
        _ ->
            [<<",\"_rev\":\"">>,
                integer_to_list(Start), <<"-">>, revid_to_str(RevId), <<"\"">>]
        end,
        case ContentMeta of
        [att_reason, AttReason] ->
             [<<",\"$att_reason\":\"">>, AttReason, <<"\"">>];
        _ ->
             <<>>
        end,
        case iolist_to_binary(Json) of
        <<"{}">> ->
            <<"}">>;
        <<${, JsonRest/binary>> ->
            <<",", JsonRest/binary>>
        end
    ]).
