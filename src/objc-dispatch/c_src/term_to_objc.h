//
//  term_to_objc.h
//  iErl14
//
//  Created by Jens Alfke on 9/30/11.

#include <erl_nif.h>
#import <Foundation/Foundation.h>

/** Converts a JSON-compatible Erlang term to Objective-C.
    Returns nil if the term can't be converted. */
id term_to_objc(ErlNifEnv* env, ERL_NIF_TERM term);

/** Converts an Erlang binary term to an NSString.
    Returns nil if the term is not a binary. */
NSString* term_to_nsstring(ErlNifEnv* env, ERL_NIF_TERM term);

/** Converts an Erlang list term to an NSArray.
    Returns nil if the term is not a list. */
NSArray *term_to_nsarray(ErlNifEnv *env, ERL_NIF_TERM term);

/** Converts an Erlang list term to an NSDictionary.
    Returns nil if the term is not a dictionary-formatted list. */
NSDictionary *term_to_nsdict(ErlNifEnv *env, ERL_NIF_TERM term);
