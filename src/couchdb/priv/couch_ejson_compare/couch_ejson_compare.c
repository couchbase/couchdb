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

#include <stdio.h>
#include "couch_ejson_compare.h"

#define MAX_DEPTH 20

#if (ERL_NIF_MAJOR_VERSION > 2) || \
    (ERL_NIF_MAJOR_VERSION == 2 && ERL_NIF_MINOR_VERSION >= 3)
/* OTP R15B or higher */
#define term_is_number(env, t) enif_is_number(env, t)
#else
#define term_is_number(env, t)  \
    (!enif_is_binary(env, t) && \
     !enif_is_list(env, t) &&   \
     !enif_is_tuple(env, t))
#endif

static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_FALSE;
static ERL_NIF_TERM ATOM_NULL;
static ERL_NIF_TERM ATOM_ERROR;


/* TODO: move all ejson helpers into dedicate file and rename this one
   to couch_ejson_compare_nif.c */

static ERL_NIF_TERM less_ejson_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static int on_load(ErlNifEnv*, void**, ERL_NIF_TERM);
static void on_unload(ErlNifEnv*, void*);
static __inline int less_ejson(int, couch_ejson_ctx_t*, ERL_NIF_TERM, ERL_NIF_TERM);
static __inline int atom_sort_order(ErlNifEnv*, ERL_NIF_TERM);
static __inline int compare_strings(couch_ejson_ctx_t*, ErlNifBinary, ErlNifBinary);
static __inline int compare_lists(int, couch_ejson_ctx_t*, ERL_NIF_TERM, ERL_NIF_TERM);
static __inline int compare_props(int, couch_ejson_ctx_t*, ERL_NIF_TERM, ERL_NIF_TERM);
static __inline ERL_NIF_TERM make_return_term(couch_ejson_ctx_t*, int);

static ERL_NIF_TERM less_json_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);



ERL_NIF_TERM
less_ejson_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    couch_ejson_ctx_t ctx;
    int result;

    ctx.env = env;
    ctx.error = 0;
    ctx.errorMsg = NULL;
    ctx.coll = NULL;
    ctx.globalCtx = (couch_ejson_global_ctx_t *) enif_priv_data(env);

    result = less_ejson(1, &ctx, argv[0], argv[1]);
    release_coll(&ctx);

    /*
     * There are 2 possible failure reasons:
     *
     * 1) We got an invalid EJSON operand;
     * 2) The EJSON structures are too deep - to avoid allocating too
     *    many C stack frames (because less_ejson is a recursive function),
     *    and running out of memory, we throw a badarg exception to Erlang
     *    and do the comparison in Erlang land. In practice, views keys are
     *    EJSON structures with very little nesting.
     */
    return make_return_term(&ctx, result);
}


ERL_NIF_TERM
less_json_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    couch_ejson_ctx_t ctx;
    int result;
    ErlNifBinary key1Bin, key2Bin;
    char *keys;

    if (!enif_inspect_iolist_as_binary(env, argv[0], &key1Bin)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_iolist_as_binary(env, argv[1], &key2Bin)) {
        return enif_make_badarg(env);
    }

    keys = (char *) enif_alloc(key1Bin.size + key2Bin.size + 2);
    if (keys == NULL) {
        return enif_make_tuple2(env, ATOM_ERROR, enif_make_atom(env, "Failed to allocate memory"));
    }

    memcpy(keys, key1Bin.data, key1Bin.size);
    keys[key1Bin.size] = '\0';
    memcpy(keys + key1Bin.size + 1, key2Bin.data, key2Bin.size);
    keys[key1Bin.size + 1 + key2Bin.size] = '\0';

    ctx.env = env;
    ctx.error = 0;
    ctx.errorMsg = NULL;
    ctx.coll = NULL;
    ctx.globalCtx = (couch_ejson_global_ctx_t *) enif_priv_data(env);

    result = less_json(keys, keys + key1Bin.size + 1, &ctx);
    release_coll(&ctx);
    enif_free(keys);

    return make_return_term(&ctx, result);
}


int
less_ejson(int depth, couch_ejson_ctx_t* ctx, ERL_NIF_TERM a, ERL_NIF_TERM b)
{
    int aIsAtom, bIsAtom;
    int aIsBin, bIsBin;
    int aIsNumber, bIsNumber;
    int aIsList, bIsList;
    int aArity, bArity;
    const ERL_NIF_TERM *aProps, *bProps;

    /*
     * Avoid too much recursion. Normally there isn't more than a few levels
     * of recursion, as in practice view keys do not go beyond 1 to 3 levels
     * of nesting. In case of too much recursion, signal it to the Erlang land
     * via an exception and do the EJSON comparison in Erlang land.
     */
    if (depth > MAX_DEPTH) {
        ctx->error = 1;
        return 0;
    }

    aIsAtom = enif_is_atom(ctx->env, a);
    bIsAtom = enif_is_atom(ctx->env, b);

    if (aIsAtom) {
        if (bIsAtom) {
            int aSortOrd, bSortOrd;

            if ((aSortOrd = atom_sort_order(ctx->env, a)) == -1) {
                ctx->error = 1;
                return 0;
            }

            if ((bSortOrd = atom_sort_order(ctx->env, b)) == -1) {
                ctx->error = 1;
                return 0;
            }

            return aSortOrd - bSortOrd;
        }

        return -1;
    }

    if (bIsAtom) {
        return 1;
    }

    aIsNumber = term_is_number(ctx->env, a);
    bIsNumber = term_is_number(ctx->env, b);

    if (aIsNumber) {
        if (bIsNumber) {
            return enif_compare_compat(ctx->env, a, b);
        }

        return -1;
    }

    if (bIsNumber) {
        return 1;
    }

    aIsBin = enif_is_binary(ctx->env, a);
    bIsBin = enif_is_binary(ctx->env, b);

    if (aIsBin) {
        if (bIsBin) {
            ErlNifBinary binA, binB;

            enif_inspect_binary(ctx->env, a, &binA);
            enif_inspect_binary(ctx->env, b, &binB);

            return compare_strings(ctx, binA, binB);
        }

        return -1;
    }

    if (bIsBin) {
        return 1;
    }

    aIsList = enif_is_list(ctx->env, a);
    bIsList = enif_is_list(ctx->env, b);

    if (aIsList) {
        if (bIsList) {
            return compare_lists(depth, ctx, a, b);
        }

        return -1;
    }

    if (bIsList) {
        return 1;
    }

    if (!enif_get_tuple(ctx->env, a, &aArity, &aProps)) {
        ctx->error = 1;
        return 0;
    }
    if ((aArity != 1) || !enif_is_list(ctx->env, aProps[0])) {
        ctx->error = 1;
        return 0;
    }

    if (!enif_get_tuple(ctx->env, b, &bArity, &bProps)) {
        ctx->error = 1;
        return 0;
    }
    if ((bArity != 1) || !enif_is_list(ctx->env, bProps[0])) {
        ctx->error = 1;
        return 0;
    }

    return compare_props(depth, ctx, aProps[0], bProps[0]);
}


int
atom_sort_order(ErlNifEnv* env, ERL_NIF_TERM a)
{
    if (enif_compare_compat(env, a, ATOM_NULL) == 0) {
        return 1;
    } else if (enif_compare_compat(env, a, ATOM_FALSE) == 0) {
        return 2;
    } else if (enif_compare_compat(env, a, ATOM_TRUE) == 0) {
        return 3;
    }

    return -1;
}


int
compare_lists(int depth, couch_ejson_ctx_t* ctx, ERL_NIF_TERM a, ERL_NIF_TERM b)
{
    ERL_NIF_TERM headA, tailA;
    ERL_NIF_TERM headB, tailB;
    int aIsEmpty, bIsEmpty;
    int result;

    while (1) {
        aIsEmpty = !enif_get_list_cell(ctx->env, a, &headA, &tailA);
        bIsEmpty = !enif_get_list_cell(ctx->env, b, &headB, &tailB);

        if (aIsEmpty) {
            if (bIsEmpty) {
                return 0;
            }
            return -1;
        }

        if (bIsEmpty) {
            return 1;
        }

        result = less_ejson(depth + 1, ctx, headA, headB);

        if (ctx->error || result != 0) {
            return result;
        }

        a = tailA;
        b = tailB;
    }

    return result;
}


int
compare_props(int depth, couch_ejson_ctx_t* ctx, ERL_NIF_TERM a, ERL_NIF_TERM b)
{
    ERL_NIF_TERM headA, tailA;
    ERL_NIF_TERM headB, tailB;
    int aArity, bArity;
    const ERL_NIF_TERM *aKV, *bKV;
    ErlNifBinary keyA, keyB;
    int aIsEmpty, bIsEmpty;
    int keyCompResult, valueCompResult;

    while (1) {
        aIsEmpty = !enif_get_list_cell(ctx->env, a, &headA, &tailA);
        bIsEmpty = !enif_get_list_cell(ctx->env, b, &headB, &tailB);

        if (aIsEmpty) {
            if (bIsEmpty) {
                return 0;
            }
            return -1;
        }

        if (bIsEmpty) {
            return 1;
        }

        if (!enif_get_tuple(ctx->env, headA, &aArity, &aKV)) {
            ctx->error = 1;
            return 0;
        }
        if ((aArity != 2) || !enif_inspect_binary(ctx->env, aKV[0], &keyA)) {
            ctx->error = 1;
            return 0;
        }

        if (!enif_get_tuple(ctx->env, headB, &bArity, &bKV)) {
            ctx->error = 1;
            return 0;
        }
        if ((bArity != 2) || !enif_inspect_binary(ctx->env, bKV[0], &keyB)) {
            ctx->error = 1;
            return 0;
        }

        keyCompResult = compare_strings(ctx, keyA, keyB);

        if (ctx->error || keyCompResult != 0) {
            return keyCompResult;
        }

        valueCompResult = less_ejson(depth + 1, ctx, aKV[1], bKV[1]);

        if (ctx->error || valueCompResult != 0) {
            return valueCompResult;
        }

        a = tailA;
        b = tailB;
    }

    return 0;
}


int
compare_strings(couch_ejson_ctx_t* ctx, ErlNifBinary a, ErlNifBinary b)
{
    UErrorCode status = U_ZERO_ERROR;
    UCharIterator iterA, iterB;
    int result;

    uiter_setUTF8(&iterA, (const char *) a.data, (uint32_t) a.size);
    uiter_setUTF8(&iterB, (const char *) b.data, (uint32_t) b.size);

    reserve_coll(ctx);
    result = ucol_strcollIter(ctx->coll, &iterA, &iterB, &status);

    if (U_FAILURE(status)) {
        ctx->error = 1;
        return 0;
    }

    /* ucol_strcollIter returns 0, -1 or 1
     * (see type UCollationResult in unicode/ucol.h) */

    return result;
}


ERL_NIF_TERM
make_return_term(couch_ejson_ctx_t *ctx, int result)
{
    if (ctx->error != 0) {
        if (ctx->errorMsg != NULL) {
            /* Memory allocation errors, etc */
            ERL_NIF_TERM reason = enif_make_string(ctx->env, ctx->errorMsg, ERL_NIF_LATIN1);

            return enif_make_tuple2(ctx->env, ATOM_ERROR, reason);
        } else {
            /* Invalid EJSON operand */
            return enif_make_badarg(ctx->env);
        }
    }

    result = (result < 0) ? -1 : ((result > 0) ? 1 : 0);

    return enif_make_int(ctx->env, result);
}


int
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    UErrorCode status = U_ZERO_ERROR;
    int i, j;
    couch_ejson_global_ctx_t *globalCtx;

    globalCtx = (couch_ejson_global_ctx_t *) enif_alloc(sizeof(couch_ejson_global_ctx_t));

    if (globalCtx == NULL) {
        return 1;
    }

    if (!enif_get_int(env, info, &globalCtx->numCollators)) {
        return 2;
    }

    if (globalCtx->numCollators < 1) {
        return 3;
    }

    globalCtx->collMutex = enif_mutex_create("coll_mutex");

    if (globalCtx->collMutex == NULL) {
        return 4;
    }

    globalCtx->collators = (UCollator **) enif_alloc(sizeof(UCollator *) * globalCtx->numCollators);

    if (globalCtx->collators == NULL) {
        enif_mutex_destroy(globalCtx->collMutex);
        return 5;
    }

    for (i = 0; i < globalCtx->numCollators; i++) {
        globalCtx->collators[i] = ucol_open("", &status);

        if (U_FAILURE(status)) {
            for (j = 0; j < i; j++) {
                ucol_close(globalCtx->collators[j]);
            }

            enif_free(globalCtx->collators);
            enif_mutex_destroy(globalCtx->collMutex);

            return 5;
        }
    }

    globalCtx->collStackTop = 0;
    *priv = globalCtx;

    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_FALSE = enif_make_atom(env, "false");
    ATOM_NULL = enif_make_atom(env, "null");
    ATOM_ERROR = enif_make_atom(env, "error");

    return 0;
}


void
on_unload(ErlNifEnv* env, void* priv_data)
{
    couch_ejson_global_ctx_t *globalCtx = (couch_ejson_global_ctx_t *) priv_data;
    int i;

    for (i = 0; i < globalCtx->numCollators; i++) {
        ucol_close(globalCtx->collators[i]);
    }

    enif_free(globalCtx->collators);
    enif_mutex_destroy(globalCtx->collMutex);
    enif_free(globalCtx);
}


static ErlNifFunc nif_functions[] = {
    {"less_ejson_nif", 2, less_ejson_nif},
    {"less_json_nif", 2, less_json_nif}
};


#ifdef __cplusplus
extern "C" {
#endif

ERL_NIF_INIT(couch_ejson_compare, nif_functions, &on_load, NULL, NULL, &on_unload);

#ifdef __cplusplus
}
#endif
