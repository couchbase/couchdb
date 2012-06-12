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
#include <string.h>
#include <assert.h>

#include "couch_view_parser.h"

static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_NEED_MORE_DATA;
static ERL_NIF_TERM ATOM_ROW_COUNT;
static ERL_NIF_TERM ATOM_ROWS;
static ERL_NIF_TERM ATOM_ERRORS;
static ERL_NIF_TERM ATOM_DEBUG_INFOS;
static ERL_NIF_TERM ATOM_DONE;

static ErlNifResourceType *CTX_RES;

// NIF API functions
static ERL_NIF_TERM startContext(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM parseChunk(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nextState(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]);

// NIF API callbacks
static int onLoad(ErlNifEnv *env, void **priv, ERL_NIF_TERM info);

// Utilities
static inline ERL_NIF_TERM return_rows(ErlNifEnv *env, ctx_t *ctx);
static inline ERL_NIF_TERM make_rows_list(ErlNifEnv *env, ctx_t *ctx);
static inline ERL_NIF_TERM return_error_entries(ErlNifEnv *env, ctx_t *ctx);
static inline ERL_NIF_TERM make_errors_list(ErlNifEnv *env, ctx_t *ctx);
static inline ERL_NIF_TERM return_debug_entries(ErlNifEnv *env, ctx_t *ctx);
static inline ERL_NIF_TERM make_debug_entries_list(ErlNifEnv *env, ctx_t *ctx);
static inline ERL_NIF_TERM makeError(ErlNifEnv *env, const std::string &msg);

// NIF resource functions
static void freeContext(ErlNifEnv *env, void *res);



static ERL_NIF_TERM startContext(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ctx_t *ctx = static_cast<ctx_t *>(enif_alloc_resource(CTX_RES, sizeof(ctx_t)));

    ERL_NIF_TERM res = enif_make_resource(env, ctx);
    enif_release_resource(ctx);

    initContext(ctx);

    return enif_make_tuple2(env, ATOM_OK, res);
}


static ERL_NIF_TERM parseChunk(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ctx_t *ctx;

    if (!enif_get_resource(env, argv[0], CTX_RES, reinterpret_cast<void **>(&ctx))) {
        return enif_make_badarg(env);
    }

    if (ctx->error != NULL) {
        return makeError(env, *ctx->error);
    }

    ErlNifBinary chunkBin;

    if (!enif_inspect_iolist_as_binary(env, argv[1], &chunkBin)) {
        return enif_make_badarg(env);
    }

    try {
        parseJsonChunk(ctx, chunkBin.data, chunkBin.size);
        return ATOM_OK;
    } catch(JsonParseException &e) {
        return makeError(env, e.getMsg());
    } catch(std::bad_alloc &) {
        return makeError(env, "memory allocation failure");
    }
}


static ERL_NIF_TERM nextState(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ctx_t *ctx;

    if (!enif_get_resource(env, argv[0], CTX_RES, reinterpret_cast<void **>(&ctx))) {
        return enif_make_badarg(env);
    }

    if (ctx->error != NULL) {
        return makeError(env, *ctx->error);
    }

    switch (ctx->caller_state) {
    case debug_infos:
        switch (ctx->parser_state) {
        case parser_starting:
            return enif_make_tuple2(env, ATOM_OK, ATOM_NEED_MORE_DATA);
        case parser_debug_info:
            if (ctx->debug_infos->empty()) {
                return enif_make_tuple2(env, ATOM_OK, ATOM_NEED_MORE_DATA);
            } else {
                return return_debug_entries(env, ctx);
            }
        case parser_row_count:
            {
                ctx->caller_state = row_count;
                if (ctx->debug_infos->empty()) {
                    return enif_make_tuple2(env, ATOM_OK, ATOM_NEED_MORE_DATA);
                } else {
                    return return_debug_entries(env, ctx);
                }
            }
        case parser_rows:
            if (ctx->debug_infos->empty()) {
                ctx->caller_state = rows;
                if (ctx->row_count != NULL) {
                    // map view
                    ERL_NIF_TERM row_count = enif_make_string(env, ctx->row_count, ERL_NIF_LATIN1);
                    return enif_make_tuple3(env, ATOM_OK, ATOM_ROW_COUNT, row_count);
                } else {
                    // reduce view
                    if (ctx->rows->empty()) {
                        return enif_make_tuple2(env, ATOM_OK, ATOM_NEED_MORE_DATA);
                    } else {
                        return return_rows(env, ctx);
                    }
                }
            } else {
                ctx->caller_state = row_count;
                return return_debug_entries(env, ctx);
            }
        case parser_errors:
            if (ctx->debug_infos->empty()) {
                if (ctx->row_count != NULL) {
                    // map view
                    ERL_NIF_TERM row_count = enif_make_string(env, ctx->row_count, ERL_NIF_LATIN1);
                    ctx->caller_state = rows;
                    return enif_make_tuple3(env, ATOM_OK, ATOM_ROW_COUNT, row_count);
                } else {
                    // reduce view
                    ctx->caller_state = error_entries;
                    if (ctx->rows->empty()) {
                        if (ctx->error_entries->empty()) {
                            return enif_make_tuple2(env, ATOM_OK, ATOM_NEED_MORE_DATA);
                        } else {
                            return return_error_entries(env, ctx);
                        }
                    } else {
                        return return_rows(env, ctx);
                    }
                }
            } else {
                ctx->caller_state = row_count;
                return return_debug_entries(env, ctx);
            }
        case parser_ending:
            if (ctx->debug_infos->empty()) {
                if (ctx->row_count != NULL) {
                    // map view
                    ERL_NIF_TERM row_count = enif_make_string(env, ctx->row_count, ERL_NIF_LATIN1);
                    ctx->caller_state = rows;
                    return enif_make_tuple3(env, ATOM_OK, ATOM_ROW_COUNT, row_count);
                } else {
                    // reduce view
                    if (ctx->rows->empty()) {
                        ctx->caller_state = ending;
                        if (ctx->error_entries->empty()) {
                            return enif_make_tuple2(env, ATOM_OK, ATOM_DONE);
                        } else {
                            return return_error_entries(env, ctx);
                        }
                    } else {
                        ctx->caller_state = error_entries;
                        return return_rows(env, ctx);
                    }
                }
            } else {
                ctx->caller_state = row_count;
                return return_debug_entries(env, ctx);
            }
        }
    case row_count:
        if (ctx->parser_state > parser_row_count) {
            if (ctx->row_count != NULL) {
                // map view
                ERL_NIF_TERM row_count = enif_make_string(env, ctx->row_count, ERL_NIF_LATIN1);
                ctx->caller_state = rows;
                return enif_make_tuple3(env, ATOM_OK, ATOM_ROW_COUNT, row_count);
            } else {
                // reduce view
                switch (ctx->parser_state) {
                case parser_rows:
                    {
                        ctx->caller_state = rows;
                        if (ctx->rows->empty()) {
                            return enif_make_tuple2(env, ATOM_OK, ATOM_NEED_MORE_DATA);
                        } else {
                            return return_rows(env, ctx);
                        }
                    }
                case parser_errors:
                    {
                        ctx->caller_state = error_entries;
                        if (ctx->rows->empty()) {
                            if (ctx->error_entries->empty()) {
                                return enif_make_tuple2(env, ATOM_OK, ATOM_NEED_MORE_DATA);
                            } else {
                                return return_error_entries(env, ctx);
                            }
                        } else {
                            return return_rows(env, ctx);
                        }
                    }
                case parser_ending:
                    {
                        ctx->caller_state = ending;
                        if (ctx->rows->empty()) {
                            if (ctx->error_entries->empty()) {
                                return enif_make_tuple2(env, ATOM_OK, ATOM_DONE);
                            } else {
                                return return_error_entries(env, ctx);
                            }
                        } else {
                            if (!ctx->error_entries->empty()) {
                                ctx->caller_state = error_entries;
                            }
                            return return_rows(env, ctx);
                        }
                    }
                default:
                    return makeError(env, "unexpected state");
                }
            }
        } else {
            return enif_make_tuple2(env, ATOM_OK, ATOM_NEED_MORE_DATA);
        }
    case rows:
        switch (ctx->parser_state) {
        case parser_rows:
            if (ctx->rows->empty()) {
                return enif_make_tuple2(env, ATOM_OK, ATOM_NEED_MORE_DATA);
            } else {
                return return_rows(env, ctx);
            }
        case parser_errors:
            {
                ctx->caller_state = error_entries;
                if (ctx->rows->empty()) {
                    if (ctx->error_entries->empty()) {
                        return enif_make_tuple2(env, ATOM_OK, ATOM_NEED_MORE_DATA);
                    } else {
                        return return_error_entries(env, ctx);
                    }
                } else {
                    return return_rows(env, ctx);
                }
            }
        case parser_ending:
            {
                ctx->caller_state = ending;
                if (ctx->rows->empty()) {
                    if (ctx->error_entries->empty()) {
                        return enif_make_tuple2(env, ATOM_OK, ATOM_DONE);
                    } else {
                        return return_error_entries(env, ctx);
                    }
                } else {
                    if (!ctx->error_entries->empty()) {
                        ctx->caller_state = error_entries;
                    }
                    return return_rows(env, ctx);
                }
            }
        default:
            return makeError(env, "unexpected state");
        }
    case error_entries:
        switch (ctx->parser_state) {
        case parser_errors:
            if (ctx->error_entries->empty()) {
                return enif_make_tuple2(env, ATOM_OK, ATOM_NEED_MORE_DATA);
            } else {
                return return_error_entries(env, ctx);
            }
        case parser_ending:
            {
                ctx->caller_state = ending;
                if (ctx->error_entries->empty()) {
                    return enif_make_tuple2(env, ATOM_OK, ATOM_DONE);
                } else {
                    return return_error_entries(env, ctx);
                }
            }
        default:
            return makeError(env, "unexpected state");
        }
    case ending:
        assert(ctx->parser_state == parser_ending);
        return enif_make_tuple2(env, ATOM_OK, ATOM_DONE);
    }

    return makeError(env, "unexpected state");
}


static inline ERL_NIF_TERM return_rows(ErlNifEnv *env, ctx_t *ctx)
{
    try {
        return enif_make_tuple3(env, ATOM_OK, ATOM_ROWS, make_rows_list(env, ctx));
    } catch(std::bad_alloc &e) {
        return makeError(env, "memory allocation failure");
    }
}


static inline ERL_NIF_TERM make_rows_list(ErlNifEnv *env, ctx_t *ctx)
{
    ERL_NIF_TERM result = enif_make_list(env, 0);
    bool isReduceView = (ctx->row_count == NULL);
    std::list<row_t *>::reverse_iterator it = ctx->rows->rbegin();

    for ( ; it != ctx->rows->rend(); ++it) {
        ErlNifBinary keyBin;
        ErlNifBinary idBin;
        ErlNifBinary valueBin;
        ErlNifBinary docBin;
        ErlNifBinary partBin;
        ErlNifBinary nodeBin;
        bool hasDoc = ((*it)->doc.length() > 0);
        bool hasPart = ((*it)->partition.length() > 0) || ((*it)->node.length() > 0);

        if (!enif_alloc_binary_compat(env, (*it)->key.length(), &keyBin)) {
            throw std::bad_alloc();
        }
        memcpy(keyBin.data, (*it)->key.data(), (*it)->key.length());

        if (!((*it)->error.empty())) {
            ERL_NIF_TERM key_docid = enif_make_tuple2(env, enif_make_binary(env, &keyBin), ATOM_ERROR);
            ErlNifBinary errorBin;
            ERL_NIF_TERM row;

            if (!enif_alloc_binary_compat(env, (*it)->error.length(), &errorBin)) {
                enif_release_binary(&keyBin);
                throw std::bad_alloc();
            }
            memcpy(errorBin.data, (*it)->error.data(), (*it)->error.length());
            row = enif_make_tuple2(env, key_docid, enif_make_binary(env, &errorBin));
            result = enif_make_list_cell(env, row, result);
            continue;
        }

        if (!isReduceView) {
            // id empty for reduce views
            if (!enif_alloc_binary_compat(env, (*it)->id.length(), &idBin)) {
                enif_release_binary(&keyBin);
                throw std::bad_alloc();
            }
            memcpy(idBin.data, (*it)->id.data(), (*it)->id.length());
        }

        if (!enif_alloc_binary_compat(env, (*it)->value.length(), &valueBin)) {
            enif_release_binary(&keyBin);
            enif_release_binary(&idBin);
            throw std::bad_alloc();
        }
        memcpy(valueBin.data, (*it)->value.data(), (*it)->value.length());

        if (hasDoc) {
            assert(isReduceView == false);
            if (!enif_alloc_binary_compat(env, (*it)->doc.length(), &docBin)) {
                enif_release_binary(&keyBin);
                enif_release_binary(&idBin);
                enif_release_binary(&valueBin);
                throw std::bad_alloc();
            }
            memcpy(docBin.data, (*it)->doc.data(), (*it)->doc.length());
        }

        if (hasPart) {
            if (!enif_alloc_binary_compat(env, (*it)->partition.length(), &partBin)) {
                enif_release_binary(&keyBin);
                enif_release_binary(&idBin);
                enif_release_binary(&valueBin);
                enif_release_binary(&docBin);
                throw std::bad_alloc();
            }
            if (!enif_alloc_binary_compat(env, (*it)->node.length(), &nodeBin)) {
                enif_release_binary(&keyBin);
                enif_release_binary(&idBin);
                enif_release_binary(&valueBin);
                enif_release_binary(&docBin);
                enif_release_binary(&partBin);
                throw std::bad_alloc();
            }
            memcpy(partBin.data, (*it)->partition.data(), (*it)->partition.length());
            memcpy(nodeBin.data, (*it)->node.data(), (*it)->node.length());
        }

        ERL_NIF_TERM keyTerm = enif_make_binary(env, &keyBin);
        ERL_NIF_TERM valueTerm = enif_make_binary(env, &valueBin);
        ERL_NIF_TERM row;

        if (isReduceView) {
            row = enif_make_tuple2(env, keyTerm, valueTerm);
        } else {
            // {Key, DocId}
            ERL_NIF_TERM key_docid = enif_make_tuple2(env, keyTerm, enif_make_binary(env, &idBin));
            ERL_NIF_TERM value;

            if (hasPart) {
                // {PartId, Node, Value}
                ERL_NIF_TERM partTerm = enif_make_binary(env, &partBin);
                ERL_NIF_TERM nodeTerm = enif_make_binary(env, &nodeBin);
                value = enif_make_tuple3(env, partTerm, nodeTerm, valueTerm);
            } else {
                value = valueTerm;
            }

            if (hasDoc) {
                // { {Key, DocId}, Value, Doc }
                ERL_NIF_TERM docTerm = enif_make_binary(env, &docBin);
                row = enif_make_tuple3(env, key_docid, value, docTerm);
            } else {
                // { {Key, DocId}, Value }
                row = enif_make_tuple2(env, key_docid, value);
            }
        }

        result = enif_make_list_cell(env, row, result);
        delete *it;
    }

    ctx->rows->clear();

    return result;
}


static inline ERL_NIF_TERM return_error_entries(ErlNifEnv *env, ctx_t *ctx)
{
    try {
        return enif_make_tuple3(env, ATOM_OK, ATOM_ERRORS, make_errors_list(env, ctx));
    } catch(std::bad_alloc &e) {
        return makeError(env, "memory allocation failure");
    }
}


static inline ERL_NIF_TERM make_errors_list(ErlNifEnv *env, ctx_t *ctx)
{
    ERL_NIF_TERM result = enif_make_list(env, 0);
    std::list<error_entry_t *>::reverse_iterator it = ctx->error_entries->rbegin();

    for ( ; it != ctx->error_entries->rend(); ++it) {
        ErlNifBinary fromBin;
        ErlNifBinary reasonBin;

        if (!enif_alloc_binary_compat(env, (*it)->from.length(), &fromBin)) {
            throw std::bad_alloc();
        }
        memcpy(fromBin.data, (*it)->from.data(), (*it)->from.length());

        if (!enif_alloc_binary_compat(env, (*it)->reason.length(), &reasonBin)) {
            enif_release_binary(&fromBin);
            throw std::bad_alloc();
        }
        memcpy(reasonBin.data, (*it)->reason.data(), (*it)->reason.length());

        ERL_NIF_TERM fromTerm = enif_make_binary(env, &fromBin);
        ERL_NIF_TERM reasonTerm = enif_make_binary(env, &reasonBin);

        result = enif_make_list_cell(env, enif_make_tuple2(env, fromTerm, reasonTerm), result);
        delete *it;
    }

    ctx->error_entries->clear();

    return result;
}


static inline ERL_NIF_TERM return_debug_entries(ErlNifEnv *env, ctx_t *ctx)
{
    try {
        return enif_make_tuple3(env, ATOM_OK, ATOM_DEBUG_INFOS, make_debug_entries_list(env, ctx));
    } catch(std::bad_alloc &e) {
        return makeError(env, "memory allocation failure");
    }
}


static inline ERL_NIF_TERM make_debug_entries_list(ErlNifEnv *env, ctx_t *ctx)
{
    ERL_NIF_TERM result = enif_make_list(env, 0);
    std::list<debug_info_t *>::reverse_iterator it = ctx->debug_infos->rbegin();

    for ( ; it != ctx->debug_infos->rend(); ++it) {
        ErlNifBinary fromBin;
        ErlNifBinary valueBin;

        if (!enif_alloc_binary_compat(env, (*it)->from.length(), &fromBin)) {
            throw std::bad_alloc();
        }
        memcpy(fromBin.data, (*it)->from.data(), (*it)->from.length());

        if (!enif_alloc_binary_compat(env, (*it)->value.length(), &valueBin)) {
            enif_release_binary(&fromBin);
            throw std::bad_alloc();
        }
        memcpy(valueBin.data, (*it)->value.data(), (*it)->value.length());

        ERL_NIF_TERM fromTerm = enif_make_binary(env, &fromBin);
        ERL_NIF_TERM valueTerm = enif_make_binary(env, &valueBin);

        result = enif_make_list_cell(env, enif_make_tuple2(env, fromTerm, valueTerm), result);
        delete *it;
    }

    ctx->debug_infos->clear();

    return result;
}


static int onLoad(ErlNifEnv *env, void **priv, ERL_NIF_TERM info)
{
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_NEED_MORE_DATA = enif_make_atom(env, "need_more_data");
    ATOM_ROW_COUNT = enif_make_atom(env, "row_count");
    ATOM_ROWS = enif_make_atom(env, "rows");
    ATOM_DONE = enif_make_atom(env, "done");
    ATOM_ERRORS = enif_make_atom(env, "errors");
    ATOM_DEBUG_INFOS = enif_make_atom(env, "debug_infos");

    CTX_RES = enif_open_resource_type(
        env,
        NULL,
        "couch_view_parser_context",
        freeContext,
        ERL_NIF_RT_CREATE,
        NULL);

    if (CTX_RES == NULL) {
        return -1;
    }

    return 0;
}


static void freeContext(ErlNifEnv *env, void *res)
{
    ctx_t *ctx = static_cast<ctx_t *>(res);

    destroyContext(ctx);
}


static ERL_NIF_TERM makeError(ErlNifEnv *env, const std::string &msg)
{
    ErlNifBinary reason;

    if (!enif_alloc_binary_compat(env, msg.length(), &reason)) {
        return enif_make_tuple2(env, ATOM_ERROR, enif_make_atom(env, "undefined"));
    } else {
        memcpy(reason.data, msg.data(), msg.length());
        return enif_make_tuple2(env, ATOM_ERROR, enif_make_binary(env, &reason));
    }
}


static ErlNifFunc nif_functions[] = {
    {"start_context", 0, startContext},
    {"parse_chunk", 2, parseChunk},
    {"next_state", 1, nextState}
};


extern "C" {
    ERL_NIF_INIT(couch_view_parser, nif_functions, &onLoad, NULL, NULL, NULL);
}
