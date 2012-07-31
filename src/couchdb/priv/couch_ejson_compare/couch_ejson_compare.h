/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#ifndef _COUCH_EJSON_COMPARE_H
#define _COUCH_EJSON_COMPARE_H

#include <assert.h>
#include <string.h>
#include "erl_nif_compat.h"
#include "unicode/ucol.h"
#include "unicode/ucasemap.h"


typedef struct {
    UCollator       **collators;
    int             collStackTop;
    int             numCollators;
    ErlNifMutex     *collMutex;
} couch_ejson_global_ctx_t;


typedef struct {
    couch_ejson_global_ctx_t *globalCtx;
    ErlNifEnv                *env;
    UCollator                *coll;
    int                      error;
    const char               *errorMsg;
} couch_ejson_ctx_t;


int less_json(const char *key1, const char *key2, couch_ejson_ctx_t *ctx);

void reserve_coll(couch_ejson_ctx_t *);
void release_coll(couch_ejson_ctx_t *);


#endif
