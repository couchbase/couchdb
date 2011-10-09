//
//  objc_to_term.m
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

#import "objc_to_term.h"
#include "util.h"
#include <math.h>


static bool objc_to_term_internal(ErlNifEnv* env, id object, ERL_NIF_TERM* term);


bool nsstring_to_term(ErlNifEnv* env, NSString* string, ERL_NIF_TERM* term)
{
    NSStringEncoding enc = NSUTF8StringEncoding;
    ErlNifBinary binary;
    if (!enif_alloc_binary([string lengthOfBytesUsingEncoding:enc], &binary))
        return false;
    if (![string getBytes:binary.data
                maxLength:binary.size
               usedLength:NULL
                 encoding:enc
                  options:0
                    range:NSMakeRange(0, string.length)
           remainingRange:NULL]) {
        enif_release_binary(&binary);
        return false;
    }
    *term = enif_make_binary(env, &binary);
    return true;
}

static bool number_to_term(ErlNifEnv* env, NSNumber* number, ERL_NIF_TERM* term)
{
    if (strcmp(number.objCType, "B")) {
        *term = util_mk_atom(env, (number.boolValue ? "true" : "false"));
    } else {
        double d = number.doubleValue;
        if (isfinite(d) && d >= INT32_MIN && d <= INT32_MAX && d == floor(d)) {
            *term = enif_make_int(env, (int32_t)d);
        } else {
            *term = enif_make_double(env, d);
        }
    }
    return true;
}

static bool array_to_term(ErlNifEnv* env, NSArray* obj, ERL_NIF_TERM* term)
{
    NSUInteger length = obj.count;
    if (length == 0) {
        *term = enif_make_list(env, 0);
        return true;
    }
    ERL_NIF_TERM* array = enif_alloc(length * sizeof(ERL_NIF_TERM));
    if(array == NULL) return false;

    int ret = false;
    ERL_NIF_TERM* dst = array;
    for (id item in obj) {
        if (!objc_to_term_internal(env, item, dst++))
            goto done;
    }

    *term = enif_make_list_from_array(env, array, length);
    ret = true;

done:
    enif_free(array);
    return ret;
}

int
dict_to_term(ErlNifEnv* env, NSDictionary* dict, ERL_NIF_TERM* term)
{
    NSUInteger length = dict.count;
    if (length == 0) {
        *term = enif_make_tuple1(env, enif_make_list(env, 0));
        return true;
    }
    ERL_NIF_TERM* array = enif_alloc(length * sizeof(ERL_NIF_TERM));
    if(array == NULL) return false;

    int ret = false;
    ERL_NIF_TERM* dst = array;
    for (NSString* key in dict) {
        if (![key isKindOfClass: [NSString class]]) {
            NSLog(@"dict_to_term: Could not convert dictionary key of class %@", [key class]);
            goto done;
        }
        ERL_NIF_TERM keyterm;
        if(!nsstring_to_term(env, key, &keyterm)) goto done;
        id val = [dict objectForKey: key];
        ERL_NIF_TERM valterm;
        if(!objc_to_term_internal(env, val, &valterm)) goto done;

        *(dst++) = enif_make_tuple2(env, keyterm, valterm);
    }

    ERL_NIF_TERM list = enif_make_list_from_array(env, array, length);
    *term = enif_make_tuple1(env, list);
    ret = true;

done:
    enif_free(array);
    return ret;
}

static bool objc_to_term_internal(ErlNifEnv* env, id object, ERL_NIF_TERM* term)
{
    if (object == nil || object == [NSNull null]) {
        *term = util_mk_atom(env, "null");
        return true;
    } else if ([object isKindOfClass: [NSString class]]) {
        return nsstring_to_term(env, object, term);
    } else if ([object isKindOfClass: [NSNumber class]]) {
        return number_to_term(env, object, term);
    } else if ([object isKindOfClass: [NSArray  class]]) {
        return array_to_term(env, object, term);
    } else if ([object isKindOfClass: [NSDictionary class]]) {
        return dict_to_term(env, object, term);
    } else {
        NSLog(@"objc-to-term: Could not convert object of class %@", [object class]);
        return false;
    }
}

ERL_NIF_TERM objc_to_term(ErlNifEnv* env, id object) {
    ERL_NIF_TERM ret;
    if(!objc_to_term_internal(env, object, &ret))
        ret = util_mk_atom(env, "undefined");
    return ret;
}
