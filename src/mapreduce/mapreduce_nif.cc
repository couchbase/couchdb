/**
 * @copyright 2012 Couchbase, Inc.
 *
 * @author Filipe Manana  <filipe@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#include <iostream>
#include <cstring>
#include <sstream>

#include "erl_nif_compat.h"
#include "mapreduce.h"

static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_ERROR;

static ErlNifResourceType *MAP_REDUCE_CTX_RES;


// NIF API functions
static ERL_NIF_TERM startMapContext(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM doMapDoc(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM startReduceContext(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM doReduce(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM doRereduce(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

// NIF API callbacks
static int onLoad(ErlNifEnv* env, void** priv, ERL_NIF_TERM info);

// Utilities
static inline ERL_NIF_TERM makeError(ErlNifEnv *env, const std::string &msg);
static bool parseFunctions(ErlNifEnv *env, ERL_NIF_TERM functionsArg, std::list<std::string> &result);
static inline std::string binToFunctionString(const ErlNifBinary &bin);

// NIF resource functions
static void free_map_reduce_context(ErlNifEnv *env, void *res);



ERL_NIF_TERM startMapContext(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    std::list<std::string> mapFunctions;

    if (!parseFunctions(env, argv[0], mapFunctions)) {
        return enif_make_badarg(env);
    }

    map_reduce_ctx_t *ctx = static_cast<map_reduce_ctx_t *>(
        enif_alloc_resource(MAP_REDUCE_CTX_RES, sizeof(map_reduce_ctx_t)));

    try {
        initContext(ctx, mapFunctions);

        ERL_NIF_TERM res = enif_make_resource(env, ctx);
        enif_release_resource(ctx);

        return enif_make_tuple2(env, ATOM_OK, res);

    } catch(MapReduceError &e) {
        return makeError(env, e.getMsg());
    } catch(std::bad_alloc &) {
        return makeError(env, "memory allocation failure");
    }
}


ERL_NIF_TERM doMapDoc(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    map_reduce_ctx_t *ctx;

    if (!enif_get_resource(env, argv[0], MAP_REDUCE_CTX_RES, reinterpret_cast<void **>(&ctx))) {
        return enif_make_badarg(env);
    }

    ErlNifBinary docBin;

    if (!enif_inspect_iolist_as_binary(env, argv[1], &docBin)) {
        return enif_make_badarg(env);
    }

    json_bin_t doc((char *) docBin.data, (size_t) docBin.size);

    try {
        // Map results is a list of lists. An inner list is the list of key value
        // pairs emitted by a map function for the document.
        std::list< std::list< map_result_t > > mapResults = mapDoc(ctx, doc);
        ERL_NIF_TERM outerList = enif_make_list(env, 0);
        std::list< std::list< map_result_t > >::reverse_iterator i = mapResults.rbegin();
        bool allocError = false;

        for ( ; i != mapResults.rend(); ++i) {
            std::list< map_result_t > funResults = *i;
            ERL_NIF_TERM innerList = enif_make_list(env, 0);
            std::list< map_result_t >::reverse_iterator j = funResults.rbegin();

            for ( ; j != funResults.rend(); ++j) {
                json_bin_t key = j->first;
                json_bin_t value = j->second;

                if (!allocError) {
                    ErlNifBinary keyBin;
                    ErlNifBinary valueBin;
                    ERL_NIF_TERM kvPair;

                    if (!enif_alloc_binary_compat(env, key.length, &keyBin)) {
                        allocError = true;
                    } else {
                        if (!enif_alloc_binary_compat(env, value.length, &valueBin)) {
                            enif_release_binary(&keyBin);
                            allocError = true;
                        } else {
                            memcpy(keyBin.data, key.data, key.length);
                            memcpy(valueBin.data, value.data, value.length);

                            kvPair = enif_make_tuple2(
                                env,
                                enif_make_binary(env, &keyBin),
                                enif_make_binary(env, &valueBin));

                            innerList = enif_make_list_cell(env, kvPair, innerList);
                        }
                    }
                }

                delete [] key.data;
                delete [] value.data;
            }

            if (!allocError) {
                outerList = enif_make_list_cell(env, innerList, outerList);
            }
        }

        if (allocError) {
            return makeError(env, "memory allocation failure");
        }

        return enif_make_tuple2(env, ATOM_OK, outerList);

    } catch(MapReduceError &e) {
        return makeError(env, e.getMsg());
    } catch(std::bad_alloc &) {
        return makeError(env, "memory allocation failure");
    }
}


ERL_NIF_TERM startReduceContext(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    std::list<std::string> reduceFunctions;

    if (!parseFunctions(env, argv[0], reduceFunctions)) {
        return enif_make_badarg(env);
    }

    map_reduce_ctx_t *ctx = static_cast<map_reduce_ctx_t *>(
        enif_alloc_resource(MAP_REDUCE_CTX_RES, sizeof(map_reduce_ctx_t)));

    try {
        initContext(ctx, reduceFunctions);

        ERL_NIF_TERM res = enif_make_resource(env, ctx);
        enif_release_resource(ctx);

        return enif_make_tuple2(env, ATOM_OK, res);

    } catch(MapReduceError &e) {
        return makeError(env, e.getMsg());
    } catch(std::bad_alloc &) {
        return makeError(env, "memory allocation failure");
    }
}


ERL_NIF_TERM doReduce(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    map_reduce_ctx_t *ctx;

    if (!enif_get_resource(env, argv[0], MAP_REDUCE_CTX_RES, reinterpret_cast<void **>(&ctx))) {
        return enif_make_badarg(env);
    }

    int reduceFunNum = -1;
    std::list<json_bin_t> keys;
    std::list<json_bin_t> values;
    ERL_NIF_TERM tail;
    ERL_NIF_TERM head;

    if (!enif_get_int(env, argv[1], &reduceFunNum)) {
        if (!enif_is_list(env, argv[1])) {
            return enif_make_badarg(env);
        }
        tail = argv[1];
    } else {
        if (!enif_is_list(env, argv[2])) {
            return enif_make_badarg(env);
        }
        tail = argv[2];
    }

    while (enif_get_list_cell(env, tail, &head, &tail)) {
        const ERL_NIF_TERM* array;
        int arity;

        if (!enif_get_tuple(env, head, &arity, &array)) {
            return enif_make_badarg(env);
        }
        if (arity != 2) {
            return enif_make_badarg(env);
        }

        ErlNifBinary keyBin;
        ErlNifBinary valueBin;

        if (!enif_inspect_iolist_as_binary(env, array[0], &keyBin)) {
            return enif_make_badarg(env);
        }
        if (!enif_inspect_iolist_as_binary(env, array[1], &valueBin)) {
            return enif_make_badarg(env);
        }

        json_bin_t key((char *) keyBin.data, (size_t) keyBin.size);
        json_bin_t value((char *) valueBin.data, (size_t) valueBin.size);

        keys.push_back(key);
        values.push_back(value);
    }

    try {
        if (reduceFunNum == -1) {
            std::list<json_bin_t> results = runReduce(ctx, keys, values);

            ERL_NIF_TERM list = enif_make_list(env, 0);
            std::list<json_bin_t>::reverse_iterator it = results.rbegin();
            bool allocError = false;

            for ( ; it != results.rend(); ++it) {
                json_bin_t reduction = *it;

                if (!allocError) {
                    ErlNifBinary reductionBin;

                    if (!enif_alloc_binary_compat(env, reduction.length, &reductionBin)) {
                        allocError = true;
                    } else {
                        memcpy(reductionBin.data, reduction.data, reduction.length);
                        list = enif_make_list_cell(env, enif_make_binary(env, &reductionBin), list);
                    }
                }
                delete [] reduction.data;
            }

            if (allocError) {
                return makeError(env, "memory allocation failure");
            }

            return enif_make_tuple2(env, ATOM_OK, list);
        } else {
            json_bin_t result = runReduce(ctx, reduceFunNum, keys, values);

            ErlNifBinary reductionBin;

            if (!enif_alloc_binary_compat(env, result.length, &reductionBin)) {
                delete [] result.data;
                return makeError(env, "memory allocation failure");
            }
            memcpy(reductionBin.data, result.data, result.length);
            delete [] result.data;

            return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &reductionBin));
        }

    } catch(MapReduceError &e) {
        return makeError(env, e.getMsg());
    } catch(std::bad_alloc &) {
        return makeError(env, "memory allocation failure");
    }
}


ERL_NIF_TERM doRereduce(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    map_reduce_ctx_t *ctx;

    if (!enif_get_resource(env, argv[0], MAP_REDUCE_CTX_RES, reinterpret_cast<void **>(&ctx))) {
        return enif_make_badarg(env);
    }

    int reduceFunNum;

    if (!enif_get_int(env, argv[1], &reduceFunNum)) {
        return enif_make_badarg(env);
    }

    if (!enif_is_list(env, argv[2])) {
        return enif_make_badarg(env);
    }

    std::list<json_bin_t> reductions;
    ERL_NIF_TERM tail = argv[2];
    ERL_NIF_TERM head;

    while (enif_get_list_cell(env, tail, &head, &tail)) {
        ErlNifBinary reductionBin;

        if (!enif_inspect_iolist_as_binary(env, head, &reductionBin)) {
            return enif_make_badarg(env);
        }

        json_bin_t reduction((char *) reductionBin.data, (size_t) reductionBin.size);

        reductions.push_back(reduction);
    }

    try {
        json_bin_t result = runRereduce(ctx, reduceFunNum, reductions);

        ErlNifBinary finalReductionBin;

        if (!enif_alloc_binary_compat(env, result.length, &finalReductionBin)) {
            delete [] result.data;
            return makeError(env, "memory allocation failure");
        }
        memcpy(finalReductionBin.data, result.data, result.length);
        delete [] result.data;

        return enif_make_tuple2(env, ATOM_OK, enif_make_binary(env, &finalReductionBin));

    } catch(MapReduceError &e) {
        return makeError(env, e.getMsg());
    } catch(std::bad_alloc &) {
        return makeError(env, "memory allocation failure");
    }
}


int onLoad(ErlNifEnv *env, void **priv, ERL_NIF_TERM info)
{
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_ERROR = enif_make_atom(env, "error");

    MAP_REDUCE_CTX_RES = enif_open_resource_type(
        env,
        NULL,
        "map_reduce_context",
        free_map_reduce_context,
        ERL_NIF_RT_CREATE,
        NULL);

    if (MAP_REDUCE_CTX_RES == NULL) {
        return -1;
    }

    return 0;
}


bool parseFunctions(ErlNifEnv *env, ERL_NIF_TERM functionsArg, std::list<std::string> &result)
{
    if (!enif_is_list(env, functionsArg)) {
        return false;
    }

    ERL_NIF_TERM tail = functionsArg;
    ERL_NIF_TERM head;

    while (enif_get_list_cell(env, tail, &head, &tail)) {
        ErlNifBinary funBin;

        if (!enif_inspect_iolist_as_binary(env, head, &funBin)) {
            return false;
        }

        result.push_back(binToFunctionString(funBin));
    }

    return true;
}


std::string binToFunctionString(const ErlNifBinary &bin)
{
    std::stringstream ss;
    ss << "(";
    ss.write((char *) bin.data, bin.size);
    ss << ")";

    return ss.str();
}


ERL_NIF_TERM makeError(ErlNifEnv *env, const std::string &msg)
{
    ErlNifBinary reason;

    if (!enif_alloc_binary_compat(env, msg.length(), &reason)) {
        return ATOM_ERROR;
    } else {
        memcpy(reason.data, msg.data(), msg.length());
        return enif_make_tuple2(env, ATOM_ERROR, enif_make_binary(env, &reason));
    }
}


void free_map_reduce_context(ErlNifEnv *env, void *res) {
    map_reduce_ctx_t *ctx = static_cast<map_reduce_ctx_t *>(res);

    destroyContext(ctx);
}



static ErlNifFunc nif_functions[] = {
    {"start_map_context", 1, startMapContext},
    {"map_doc", 2, doMapDoc},
    {"start_reduce_context", 1, startReduceContext},
    {"reduce", 2, doReduce},
    {"reduce", 3, doReduce},
    {"rereduce", 3, doRereduce}
};



extern "C" {
    ERL_NIF_INIT(mapreduce, nif_functions, &onLoad, NULL, NULL, NULL);
}
