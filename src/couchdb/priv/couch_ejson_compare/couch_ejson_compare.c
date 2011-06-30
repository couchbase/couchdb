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
#include "erl_nif_compat.h"
#include "unicode/ucol.h"
#include "unicode/ucasemap.h"

#define COMP_ERROR -128

static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_FALSE;
static ERL_NIF_TERM ATOM_NULL;

static UCollator* coll = NULL;
static ErlNifMutex* collMutex = NULL;

static ERL_NIF_TERM less_json_nif(ErlNifEnv*, int, const ERL_NIF_TERM []);
static int on_load(ErlNifEnv*, void**, ERL_NIF_TERM);
static void on_unload(ErlNifEnv*, void*);
static __inline int less_json(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM);
static __inline int atom_sort_order(ErlNifEnv*, ERL_NIF_TERM);
static __inline int compare_strings(ErlNifBinary, ErlNifBinary);
static __inline int compare_lists(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM);
static __inline int compare_props(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM);
static __inline int term_is_number(ErlNifEnv*, ERL_NIF_TERM);



ERL_NIF_TERM
less_json_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int result = less_json(env, argv[0], argv[1]);

    return result == COMP_ERROR ? enif_make_badarg(env) : enif_make_int(env, result);
}


/**
 * TODO: eventually make this function non-recursive (use a stack).
 * Not an issue for now as view keys are normally not very deep structures.
 */
int
less_json(ErlNifEnv* env, ERL_NIF_TERM a, ERL_NIF_TERM b)
{
    int aIsAtom, bIsAtom;
    int aIsBin, bIsBin;
    int aIsNumber, bIsNumber;
    int aIsList, bIsList;
    int aArity, bArity;
    const ERL_NIF_TERM *aProps, *bProps;

    aIsAtom = enif_is_atom(env, a);
    bIsAtom = enif_is_atom(env, b);

    if (aIsAtom) {
        if (bIsAtom) {
            int aSortOrd, bSortOrd;

            if ((aSortOrd = atom_sort_order(env, a)) == -1) {
                return COMP_ERROR;
            }

            if ((bSortOrd = atom_sort_order(env, b)) == -1) {
                return COMP_ERROR;
            }

            return aSortOrd - bSortOrd;
        }

        return -1;
    }

    if (bIsAtom) {
        return 1;
    }

    aIsNumber = term_is_number(env, a);
    bIsNumber = term_is_number(env, b);

    if (aIsNumber) {
        if (bIsNumber) {
            int result = enif_compare_compat(env, a, b);

            if (result < 0) {
                return -1;
            } else if (result > 0) {
                return 1;
            }

            return 0;
        }

        return -1;
    }

    if (bIsNumber) {
        return 1;
    }

    aIsBin = enif_is_binary(env, a);
    bIsBin = enif_is_binary(env, b);

    if (aIsBin) {
        if (bIsBin) {
            ErlNifBinary binA, binB;

            enif_inspect_binary(env, a, &binA);
            enif_inspect_binary(env, b, &binB);

            return compare_strings(binA, binB);
        }

        return -1;
    }

    if (bIsBin) {
        return 1;
    }

    aIsList = enif_is_list(env, a);
    bIsList = enif_is_list(env, b);

    if (aIsList) {
        if (bIsList) {
            return compare_lists(env, a, b);
        }

        return -1;
    }

    if (bIsList) {
        return 1;
    }

    if (!enif_get_tuple(env, a, &aArity, &aProps)) {
        return COMP_ERROR;
    }
    if ((aArity != 1) || !enif_is_list(env, aProps[0])) {
        return COMP_ERROR;
    }

    if (!enif_get_tuple(env, b, &bArity, &bProps)) {
        return COMP_ERROR;
    }
    if ((bArity != 1) || !enif_is_list(env, bProps[0])) {
        return COMP_ERROR;
    }

    return compare_props(env, aProps[0], bProps[0]);
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
term_is_number(ErlNifEnv* env, ERL_NIF_TERM t)
{
    /* Determination by exclusion of parts. To be used only inside less_json! */
    return !enif_is_binary(env, t) && !enif_is_list(env, t) &&
        !enif_is_tuple(env, t);
}


int
compare_lists(ErlNifEnv* env, ERL_NIF_TERM a, ERL_NIF_TERM b)
{
    ERL_NIF_TERM headA, tailA;
    ERL_NIF_TERM headB, tailB;
    int aIsEmpty, bIsEmpty;
    int result;

    aIsEmpty = !enif_get_list_cell(env, a, &headA, &tailA);
    bIsEmpty = !enif_get_list_cell(env, b, &headB, &tailB);

    if (aIsEmpty) {
        if (bIsEmpty) {
            return 0;
        }
        return -1;
    }

    if (bIsEmpty) {
        return 1;
    }

    result = less_json(env, headA, headB);
    if (result == COMP_ERROR || result != 0) {
        return result;
    }

    return compare_lists(env, tailA, tailB);
}


int
compare_props(ErlNifEnv* env, ERL_NIF_TERM a, ERL_NIF_TERM b)
{
    ERL_NIF_TERM headA, tailA;
    ERL_NIF_TERM headB, tailB;
    int aArity, bArity;
    const ERL_NIF_TERM *aKV, *bKV;
    ErlNifBinary keyA, keyB;
    int aIsEmpty, bIsEmpty;
    int keyCompResult, valueCompResult;

    aIsEmpty = !enif_get_list_cell(env, a, &headA, &tailA);
    bIsEmpty = !enif_get_list_cell(env, b, &headB, &tailB);

    if (aIsEmpty) {
        if (bIsEmpty) {
            return 0;
        }
        return -1;
    }

    if (bIsEmpty) {
        return 1;
    }

    if (!enif_get_tuple(env, headA, &aArity, &aKV)) {
        return COMP_ERROR;
    }
    if ((aArity != 2) || !enif_inspect_binary(env, aKV[0], &keyA)) {
        return COMP_ERROR;
    }

    if (!enif_get_tuple(env, headB, &bArity, &bKV)) {
        return COMP_ERROR;
    }
    if ((bArity != 2) || !enif_inspect_binary(env, bKV[0], &keyB)) {
        return COMP_ERROR;
    }

    keyCompResult = compare_strings(keyA, keyB);

    if (keyCompResult == COMP_ERROR || keyCompResult != 0) {
        return keyCompResult;
    }

    valueCompResult = less_json(env, aKV[1], bKV[1]);

    if (valueCompResult == COMP_ERROR || valueCompResult != 0) {
        return valueCompResult;
    }

    return compare_props(env, tailA, tailB);
}


int
compare_strings(ErlNifBinary a, ErlNifBinary b)
{
    UErrorCode status = U_ZERO_ERROR;
    UCharIterator iterA, iterB;
    int result;

    uiter_setUTF8(&iterA, (const char *) a.data, (uint32_t) a.size);
    uiter_setUTF8(&iterB, (const char *) b.data, (uint32_t) b.size);

    enif_mutex_lock(collMutex);
    result = ucol_strcollIter(coll, &iterA, &iterB, &status);
    enif_mutex_unlock(collMutex);

    if (U_FAILURE(status)) {
        return COMP_ERROR;
    }

    if (result < 0) {
       return -1;
    } else if (result > 0) {
       return 1;
    }

    return 0;
}


int
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    UErrorCode status = U_ZERO_ERROR;

    coll = ucol_open("", &status);

    if (U_FAILURE(status)) {
        return 1;
    }

    collMutex = enif_mutex_create("coll_mutex");

    if (collMutex == NULL) {
        ucol_close(coll);
        return 2;
    }

    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_FALSE = enif_make_atom(env, "false");
    ATOM_NULL = enif_make_atom(env, "null");

    return 0;
}


void
on_unload(ErlNifEnv* env, void* priv_data)
{
    if (collMutex != NULL) {
        enif_mutex_destroy(collMutex);
    }
    if (coll != NULL) {
        ucol_close(coll);
    }
}


static ErlNifFunc nif_functions[] = {
    {"less_nif", 2, less_json_nif}
};


#ifdef __cplusplus
extern "C" {
#endif

ERL_NIF_INIT(couch_ejson_compare, nif_functions, &on_load, NULL, NULL, &on_unload);

#ifdef __cplusplus
}
#endif
