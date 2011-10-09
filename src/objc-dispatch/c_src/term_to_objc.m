//
//  term_to_objc.m
//  iErl14
//
//  Created by Jens Alfke on 9/30/11. Adapted from emonk's to_js.c
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#import "term_to_objc.h"
#include "atom.h"
#include "big.h"
#include "util.h"
#include <math.h>


static id to_objc_special(ErlNifEnv* env, ERL_NIF_TERM term)
{
    char atom[MAX_ATOM_LENGTH + 1]; // Space for NUL
    if(!enif_get_atom(env, term, atom, MAX_ATOM_LENGTH + 1, ERL_NIF_LATIN1))
        return nil;
    else if(strcmp(atom, "true") == 0)
        return (id)kCFBooleanTrue;
    else if(strcmp(atom, "false") == 0)
        return (id)kCFBooleanFalse;
    else if(strcmp(atom, "null") == 0)
        return [NSNull null];
    else
        return [NSString stringWithCString: atom encoding: NSISOLatin1StringEncoding];
}

static NSString* to_objc_string(ErlNifEnv* env, ERL_NIF_TERM term)
{
    ErlNifBinary bin;
    if(!enif_inspect_binary(env, term, &bin))
        return nil;
    return [[[NSString alloc] initWithBytes: bin.data length: bin.size
                                   encoding: NSUTF8StringEncoding] autorelease];
    // This is assuming Erlang strings are encoded in UTF-8.
}

NSString* term_to_nsstring(ErlNifEnv* env, ERL_NIF_TERM term)
{
    if (!enif_is_binary(env, term))
        return nil;
    return to_objc_string(env, term);
}

static NSArray* to_objc_array(ErlNifEnv* env, ERL_NIF_TERM head, ERL_NIF_TERM tail)
{
	unsigned int tailLength;
	if (!enif_get_list_length(env, tail, &tailLength))
		tailLength = 0;
	NSMutableArray* array = [NSMutableArray arrayWithCapacity:tailLength + 1];
    if(array == nil) return nil;

    do {
        id val = term_to_objc(env, head);
        if(val == nil) return nil;
        [array addObject: val];
    } while(enif_get_list_cell(env, tail, &head, &tail));

    return array;
}

NSArray *term_to_nsarray(ErlNifEnv *env, ERL_NIF_TERM term)
{
    ERL_NIF_TERM head, tail;
    if(enif_is_empty_list(env, term))
        return [NSArray array];
    else if(enif_get_list_cell(env, term, &head, &tail))
        return to_objc_array(env, head, tail);
    else
        return nil;
}


static NSString* to_objc_key(ErlNifEnv* env, ERL_NIF_TERM term)
{
    if(enif_is_atom(env, term)) {
        id object = to_objc_special(env,term);
        if ([object isKindOfClass: [NSString class]])
            return object;
        else
            return nil;
    } else
        return term_to_nsstring(env, term);
}

static NSDictionary* to_objc_dict(ErlNifEnv* env, ERL_NIF_TERM list)
{
    if(enif_is_empty_list(env, list))
        return [NSDictionary dictionary];

    ERL_NIF_TERM head;
    ERL_NIF_TERM tail;
    if(!enif_get_list_cell(env, list, &head, &tail))
        return nil;

	unsigned int count;
	if (!enif_get_list_length(env, list, &count))
		count = 0;
    NSMutableDictionary* dict = [NSMutableDictionary dictionaryWithCapacity:count];
    if(dict == nil)
        return nil;

    do {
        int arity;
        const ERL_NIF_TERM* pair;
        if(!enif_get_tuple(env, head, &arity, &pair) || arity != 2)
            return nil;

        NSString* kval = to_objc_key(env, pair[0]);
        if(kval == nil)
            return nil;
        id vval = term_to_objc(env, pair[1]);
        if(vval == nil)
            return nil;

        [dict setObject: vval forKey: kval];
    } while(enif_get_list_cell(env, tail, &head, &tail));
    return dict;
}

NSDictionary* term_to_nsdict(ErlNifEnv *env, ERL_NIF_TERM term) {
    const ERL_NIF_TERM* tuple;
    int arity;
    if (enif_get_tuple(env, term, &arity, &tuple) && arity == 1)
        return to_objc_dict(env, tuple[0]);
    else
        return nil;
}

id term_to_objc(ErlNifEnv* env, ERL_NIF_TERM term)
{
    int intval;
    unsigned int uintval;
    long longval;
    unsigned long ulongval;
    double doubleval;
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail;
    const ERL_NIF_TERM* tuple;
    int arity;

    if(enif_is_atom(env, term))
        return to_objc_special(env, term);
    else if(enif_is_binary(env, term))
        return to_objc_string(env, term);
    else if(enif_is_empty_list(env, term))
        return [NSArray array];
    else if(enif_get_int(env, term, &intval))
        return [NSNumber numberWithInt: intval];
    else if(enif_get_uint(env, term, &uintval))
        return [NSNumber numberWithUnsignedInt: uintval];
    else if(enif_get_long(env, term, &longval))
        return [NSNumber numberWithLong: longval];
    else if(enif_get_ulong(env, term, &ulongval))
        return [NSNumber numberWithUnsignedLong: ulongval];
    else if(enif_get_double(env, term, &doubleval))
        return [NSNumber numberWithDouble: doubleval];
    // enif doesn't seem to have any API to decode bignums, so use lower-level functions:
    else if(is_big(term) && big_to_double(term, &doubleval) == 0)
        return [NSNumber numberWithDouble: doubleval];
    else if(enif_get_list_cell(env, term, &head, &tail))
        return to_objc_array(env, head, tail);
    else if(enif_get_tuple(env, term, &arity, &tuple) && arity == 1)
        return to_objc_dict(env, tuple[0]);
    else {
        NSLog(@"term_to_objc: Could not convert term %x", term);
        return nil;
    }
}
