-module(couch_btree_nif).
-include("couch_db.hrl").
-export([query_modify_raw/3, query_modify_raw_native/3, write_response/3, init/0]).
-on_load(init/0).

init() ->
    LibDir = case couch_config:get("couchdb", "util_driver_dir") of
    undefined ->
        filename:join(couch_util:priv_dir(), "lib");
    LibDir0 ->
        LibDir0
    end,
    erlang:load_nif(filename:join([LibDir, ?MODULE]), 0).

get_rq_info(#db{filepath=Filepath,
        local_docs_btree = LocalDocs,
        docinfo_by_id_btree = DocInfoById,
        docinfo_by_seq_btree = DocInfoBySeq}, #btree{root = Root} = WhichTree) ->
    case WhichTree of
        DocInfoById ->
            % raw ascii collation, by_id reduce
            {0, 2, Filepath, Root};
        LocalDocs ->
            % raw ascii collation, no reduce
            {0, 0, Filepath, Root};
        DocInfoBySeq ->
            % numeric term collation, by_seq reduce (count)
            {1, 1, Filepath, Root};
        _ -> unsupported_btree
    end.

format_newroot({P, Rbin, S}) ->
    {P, binary_to_term(iolist_to_binary([<<131>>, Rbin])), S}.

response_loop(Ref, #btree{assemble_kv=Assemble} = Bt, Root, QueryResults) ->
    receive
        {Ref, not_modified} ->
            {ok, QueryResults, Bt#btree{root = Root}};
        {Ref, {new_root, NewRoot}} ->
            {ok, QueryResults, Bt#btree{root = format_newroot(NewRoot)}};
        {Ref, {ok, Key, Value}} ->
            response_loop(Ref, Bt, Root, [{ok, Assemble(binary_to_term(Key), binary_to_term(Value))} 
                    | QueryResults]);
        {Ref, {not_found, Key}} ->
            response_loop(Ref, Bt, Root, [{not_found, {binary_to_term(Key), nil}} | QueryResults]);
        {Ref, {error, Err}} ->
            {error, Err}
    end.

action_type(fetch)  -> 0;
action_type(insert) -> 1;
action_type(remove) -> 2.

format_action(Action) ->
    case Action of
        {T,K} ->
            {action_type(T), term_to_binary(K)};
        {T,K,V} ->
            {action_type(T), term_to_binary(K), term_to_binary(V)}
    end.

query_modify_raw(Db, Bt, SortedActions) ->
    try query_modify_raw_native(Db, Bt, SortedActions)
    catch
    % Fall back onto the normal updater if we can't use the NIF
        Type:Error ->
            io:format("Didn't work!!!!!!!!:~p:~p~n", [Type, Error]),
            couch_btree:query_modify_raw(Bt, SortedActions)
    end.

query_modify_raw_native(Db, #btree{fd = Fd} = Bt, SortedActions) ->
    {CompareType, ReduceType, Filename, Root} = get_rq_info(Db, Bt),
    DoActions = [format_action(Act) || Act <- SortedActions],
    
    case do_native_modify(DoActions, Filename, CompareType,
            ReduceType, Root, Fd) of
    {ok, Ref} ->
        response_loop(Ref, Bt, Root, []);
    ok ->
        {ok, [], Bt}
    end.

do_native_modify(_, _, _, _, _, _) ->
    {error, nif_not_loaded}.

write_response(_, _, _) ->
    {error, nif_not_loaded}.
