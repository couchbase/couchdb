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
-export([validate_docid/1]).
-export([with_ejson_body/1,with_json_body/1]).

-include("couch_db.hrl").


% helpers used by to_json_obj
to_json_rev(0, _) ->
    [];
to_json_rev(Start, RevId) ->
    [{<<"_rev">>, ?l2b([integer_to_list(Start),"-",revid_to_str(RevId)])}].

to_ejson_body(true, {Body}) ->
    to_ejson_body(false, {Body}) ++ [{<<"_deleted">>, true}];
to_ejson_body(false, {Body}) ->
    Body;
to_ejson_body(false, <<"{}">>) ->
    [];
to_ejson_body(false, Body) when is_binary(Body) ->
    {R} = ?JSON_DECODE(Body),
    R.

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


to_json_obj(Doc, Options) ->
    doc_to_json_obj(with_ejson_body(Doc), Options).

doc_to_json_obj(#doc{id=Id,deleted=Del,json=Json,rev={Start, RevId},
        meta=Meta}, _Options)->
    {[{<<"_id">>, Id}]
        ++ to_json_rev(Start, RevId)
        ++ to_json_meta(Meta)
        ++ to_ejson_body(Del, Json)
    }.

mk_att_doc_from_binary(Id, Value, Reason) ->
    #doc{id=Id,
               meta = [att_reason, Reason],
               binary = Value}.


mk_json_doc_from_binary(Id, Value) ->
    case ejson:validate(Value, <<"_$">>) of
        {error, invalid_json} ->
            mk_att_doc_from_binary(Id, Value, <<"invalid_json">>);
        {error, private_field_set} ->
            mk_att_doc_from_binary(Id, Value, <<"invalid_key">>);
        {error, garbage_after_value} ->
            mk_att_doc_from_binary(Id, Value, <<"invalid_json">>);
        {ok, Json} ->
            #doc{id=Id, % now add in new meta
                       json=Json
                      }
    end.

from_binary(Id, Value, WantJson) ->
    case WantJson of
        true ->
            mk_json_doc_from_binary(Id, Value);
        _ ->
            mk_att_doc_from_binary(Id, Value, <<"non-JSON mode">>)
    end.

from_json_obj({Props}) ->
    transfer_fields(Props, #doc{json=[]});

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

transfer_fields([], #doc{json=Fields}=Doc) ->
    % convert fields back to json object
    Doc#doc{json={lists:reverse(Fields)}};

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
    #doc{json=Fields} = Doc) ->
    transfer_fields(Rest, Doc#doc{json=[Field|Fields]});
transfer_fields([{<<"_replication_state_time">>, _} = Field | Rest],
    #doc{json=Fields} = Doc) ->
    transfer_fields(Rest, Doc#doc{json=[Field|Fields]});
transfer_fields([{<<"_replication_id">>, _} = Field | Rest],
    #doc{json=Fields} = Doc) ->
    transfer_fields(Rest, Doc#doc{json=[Field|Fields]});

% unknown special field
transfer_fields([{<<"_",Name/binary>>, _} | _], _) ->
    throw({doc_validation,
            ?l2b(io_lib:format("Bad special document member: _~s", [Name]))});

transfer_fields([Field | Rest], #doc{json=Fields}=Doc) ->
    transfer_fields(Rest, Doc#doc{json=[Field|Fields]}).


with_ejson_body(Doc) ->
    Uncompressed = with_uncompressed_body(Doc),
    #doc{json = Body} = Uncompressed,
    Uncompressed#doc{json = {to_ejson_body(false, Body)}}.

with_json_body(Doc) ->
    case with_uncompressed_body(Doc) of
    #doc{json = Body} = Doc2 when is_binary(Body) ->
        Doc2;
    #doc{json = Body} = Doc2 when is_tuple(Body)->
        Doc2#doc{json = ?JSON_ENCODE(Body)}
    end.

with_uncompressed_body(#doc{json = Body} = Doc) when is_binary(Body) ->
    case couch_compress:is_compressed(Body) of
        true ->
            Doc#doc{json = couch_compress:decompress(Body)};
        false ->
            Doc
    end;
with_uncompressed_body(#doc{json = {_}} = Doc) ->
    Doc.
