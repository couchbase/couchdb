//
//  objc_to_term.h
//  iErl14
//
//  Created by Jens Alfke on 9/30/11.

#include <erl_nif.h>

/** Converts a JSON-compatible Objective-C object tree into an Erlang term.
    If the input is nil, returns the atom 'null'.
    If the object tree can't be converted, returns the atom 'undefined'. */
ERL_NIF_TERM objc_to_term(ErlNifEnv* env, id object);

/** Converts an NSString to an Erlang term (binary object).
    More efficient than objc_to_term if you already know the object is a string.
    Returns true on success, false on failure. */
bool nsstring_to_term(ErlNifEnv* env, NSString* string, ERL_NIF_TERM* term);
