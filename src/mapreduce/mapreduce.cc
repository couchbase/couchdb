/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <list>
#include <vector>
#include <string>
#include <string.h>
#include <sstream>
#include <v8.h>
#include <time.h>

#include "mapreduce.h"
#define MAX_LOG_STRING_SIZE 1024

#define MAX_EMIT_KEY_SIZE 4096

using namespace v8;

typedef struct {
    Persistent<Object>    jsonObject;
    Persistent<Function>  jsonParseFun;
    Persistent<Function>  stringifyFun;
    map_reduce_ctx_t      *ctx;
} isolate_data_t;


static const char *SUM_FUNCTION_STRING =
    "(function(values) {"
    "    var sum = 0;"
    "    for (var i = 0; i < values.length; ++i) {"
    "        sum += values[i];"
    "    }"
    "    return sum;"
    "})";

static const char *DATE_FUNCTION_STRING =
    // I wish it was on the prototype, but that will require bigger
    // C changes as adding to the date prototype should be done on
    // process launch. The code you see here may be faster, but it
    // is less JavaScripty.
    // "Date.prototype.toArray = (function() {"
    "(function(date) {"
    "    date = date.getUTCDate ? date : new Date(date);"
    "    return isFinite(date.valueOf()) ?"
    "      [date.getUTCFullYear(),"
    "      (date.getUTCMonth() + 1),"
    "       date.getUTCDate(),"
    "       date.getUTCHours(),"
    "       date.getUTCMinutes(),"
    "       date.getUTCSeconds()] : null;"
    "})";

static const char *BASE64_FUNCTION_STRING =
    "(function(b64) {"
    "    var i, j, l, tmp, scratch, arr = [];"
    "    var lookup = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';"
    "    if (typeof b64 !== 'string') {"
    "        throw 'Input is not a string';"
    "    }"
    "    if (b64.length % 4 > 0) {"
    "        throw 'Invalid base64 source.';"
    "    }"
    "    scratch = b64.indexOf('=');"
    "    scratch = scratch > 0 ? b64.length - scratch : 0;"
    "    l = scratch > 0 ? b64.length - 4 : b64.length;"
    "    for (i = 0, j = 0; i < l; i += 4, j += 3) {"
    "        tmp = (lookup.indexOf(b64[i]) << 18) | (lookup.indexOf(b64[i + 1]) << 12);"
    "        tmp |= (lookup.indexOf(b64[i + 2]) << 6) | lookup.indexOf(b64[i + 3]);"
    "        arr.push((tmp & 0xFF0000) >> 16);"
    "        arr.push((tmp & 0xFF00) >> 8);"
    "        arr.push(tmp & 0xFF);"
    "    }"
    "    if (scratch === 2) {"
    "        tmp = (lookup.indexOf(b64[i]) << 2) | (lookup.indexOf(b64[i + 1]) >> 4);"
    "        arr.push(tmp & 0xFF);"
    "    } else if (scratch === 1) {"
    "        tmp = (lookup.indexOf(b64[i]) << 10) | (lookup.indexOf(b64[i + 1]) << 4);"
    "        tmp |= (lookup.indexOf(b64[i + 2]) >> 2);"
    "        arr.push((tmp >> 8) & 0xFF);"
    "        arr.push(tmp & 0xFF);"
    "    }"
    "    return arr;"
    "})";

static void doInitContext(map_reduce_ctx_t *ctx,
                          const function_sources_list_t &funs,
                          const view_index_type_t viewType);
#ifdef V8_POST_3_19_API
static Local<Context> createJsContext(map_reduce_ctx_t *ctx);
static void emit(const v8::FunctionCallbackInfo<Value>& args);
static void log(const v8::FunctionCallbackInfo<Value>& args);
#else
static Persistent<Context> createJsContext(map_reduce_ctx_t *ctx);
static Handle<Value> emit(const Arguments& args);
static Handle<Value> log(const Arguments& args);
#endif

static void loadFunctions(map_reduce_ctx_t *ctx, const function_sources_list_t &funs);
static void freeJsonData(const json_results_list_t &data);
static void freeMapResult(const map_result_t &data);
static void freeMapResultList(const map_results_list_t &results);
static Handle<Function> compileFunction(const function_source_t &funSource);
static inline ErlNifBinary jsonStringify(const Handle<Value> &obj);
static inline Handle<Value> jsonParse(const ErlNifBinary &thing);
static inline Handle<Array> jsonListToJsArray(const json_results_list_t &list);
static inline isolate_data_t *getIsolateData();
static inline void taskStarted(map_reduce_ctx_t *ctx);
static inline void taskFinished(map_reduce_ctx_t *ctx);
static std::string exceptionString(const TryCatch &tryCatch);
static void freeLogResults(map_reduce_ctx_t *ctx);



void initContext(map_reduce_ctx_t *ctx, const function_sources_list_t &funs,
                 const view_index_type_t viewType)
{
    ctx = new (ctx) map_reduce_ctx_t();
    doInitContext(ctx, funs, viewType);

    try {
        Locker locker(ctx->isolate);
        Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
        HandleScope handleScope(ctx->isolate);
        Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#else
        HandleScope handleScope;
        Context::Scope contextScope(ctx->jsContext);

#endif
        loadFunctions(ctx, funs);
    } catch (...) {
        destroyContext(ctx);
        throw;
    }
}


void doInitContext(map_reduce_ctx_t *ctx, const function_sources_list_t &funs,
                   const view_index_type_t viewType)
{
    ctx->viewType = viewType;
    ctx->isolate = Isolate::New();
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
    HandleScope handleScope(ctx->isolate);

    ctx->jsContext.Reset(ctx->isolate, createJsContext(ctx));
    Local<Context> context = Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope contextScope(context);

    Handle<Object> jsonObject = Local<Object>::Cast(context->Global()->Get(String::New("JSON")));
#else
    HandleScope handleScope;

    ctx->jsContext = createJsContext(ctx);
    Context::Scope contextScope(ctx->jsContext);

    Handle<Object> jsonObject = Local<Object>::Cast(ctx->jsContext->Global()->Get(String::New("JSON")));
#endif

    Handle<Function> parseFun = Local<Function>::Cast(jsonObject->Get(String::New("parse")));
    Handle<Function> stringifyFun = Local<Function>::Cast(jsonObject->Get(String::New("stringify")));

    isolate_data_t *isoData = (isolate_data_t *) enif_alloc(sizeof(isolate_data_t));
    if (isoData == NULL) {
        throw std::bad_alloc();
    }

    isoData = new (isoData) isolate_data_t();
#ifdef V8_POST_3_19_API
    isoData->jsonObject.Reset(ctx->isolate, jsonObject);
    isoData->jsonParseFun.Reset(ctx->isolate, parseFun);
    isoData->stringifyFun.Reset(ctx->isolate, stringifyFun);
#else
    isoData->jsonObject = Persistent<Object>::New(jsonObject);
    isoData->jsonParseFun = Persistent<Function>::New(parseFun);
    isoData->stringifyFun = Persistent<Function>::New(stringifyFun);
#endif

    isoData->ctx = ctx;

    ctx->isolate->SetData(isoData);
    ctx->taskStartTime = 0;
}


map_results_list_t mapDoc(map_reduce_ctx_t *ctx,
                          const ErlNifBinary &doc,
                          const ErlNifBinary &meta)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
    HandleScope handleScope(ctx->isolate);
    Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#else
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
#endif
    Handle<Value> docObject = jsonParse(doc);
    Handle<Value> metaObject = jsonParse(meta);

    if (!metaObject->IsObject()) {
        throw MapReduceError("metadata is not a JSON object");
    }

    map_results_list_t results;
    Handle<Value> funArgs[] = { docObject, metaObject };

    taskStarted(ctx);

    for (unsigned int i = 0; i < ctx->functions->size(); ++i) {
        map_result_t mapResult;
#ifdef V8_POST_3_19_API
        Local<Function> fun = Local<Function>::New(ctx->isolate, *(*ctx->functions)[i]);
#else
        Handle<Function> fun = (*ctx->functions)[i];
#endif
        TryCatch trycatch;

        mapResult.type = MAP_KVS;
        mapResult.result.kvs = (kv_pair_list_t *) enif_alloc(sizeof(kv_pair_list_t));

        if (mapResult.result.kvs == NULL) {
            freeMapResultList(results);
            throw std::bad_alloc();
        }

        mapResult.result.kvs = new (mapResult.result.kvs) kv_pair_list_t();
        ctx->kvs = mapResult.result.kvs;
        ctx->emitKvSize = 0;
        Handle<Value> result = fun->Call(fun, 2, funArgs);

        if (result.IsEmpty()) {
            freeMapResult(mapResult);

            if (!trycatch.CanContinue()) {
                freeMapResultList(results);
                throw MapReduceError("timeout");
            }

            mapResult.type = MAP_ERROR;
            std::string exceptString = exceptionString(trycatch);
            size_t len = exceptString.length();

            mapResult.result.error = (ErlNifBinary *) enif_alloc(sizeof(ErlNifBinary));
            if (mapResult.result.error == NULL) {
                freeMapResultList(results);
                throw std::bad_alloc();
            }
            if (!enif_alloc_binary_compat(ctx->env, len, mapResult.result.error)) {
                freeMapResultList(results);
                throw std::bad_alloc();
            }
            // Caller responsible for invoking enif_make_binary() or enif_release_binary()
            memcpy(mapResult.result.error->data, exceptString.data(), len);
        }

        results.push_back(mapResult);
    }

    taskFinished(ctx);

    return results;
}


json_results_list_t runReduce(map_reduce_ctx_t *ctx,
                              const json_results_list_t &keys,
                              const json_results_list_t &values)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
    HandleScope handleScope(ctx->isolate);
    Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#else
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
#endif
    Handle<Array> keysArray = jsonListToJsArray(keys);
    Handle<Array> valuesArray = jsonListToJsArray(values);
    json_results_list_t results;

    Handle<Value> args[] = { keysArray, valuesArray, Boolean::New(false) };

    taskStarted(ctx);

    for (unsigned int i = 0; i < ctx->functions->size(); ++i) {
#ifdef V8_POST_3_19_API
        Local<Function> fun = Local<Function>::New(ctx->isolate, *(*ctx->functions)[i]);
#else
        Handle<Function> fun = (*ctx->functions)[i];
#endif
        TryCatch trycatch;
        Handle<Value> result = fun->Call(fun, 3, args);

        if (result.IsEmpty()) {
            freeJsonData(results);

            if (!trycatch.CanContinue()) {
                throw MapReduceError("timeout");
            }

            throw MapReduceError(exceptionString(trycatch));
        }

        try {
            ErlNifBinary jsonResult = jsonStringify(result);
            results.push_back(jsonResult);
        } catch(...) {
            freeJsonData(results);
            throw;
        }
    }

    taskFinished(ctx);
    freeLogResults(ctx);

    return results;
}


ErlNifBinary runReduce(map_reduce_ctx_t *ctx,
                       int reduceFunNum,
                       const json_results_list_t &keys,
                       const json_results_list_t &values)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
    HandleScope handleScope(ctx->isolate);
    Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#else
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
#endif

    reduceFunNum -= 1;
    if (reduceFunNum < 0 ||
        static_cast<unsigned int>(reduceFunNum) >= ctx->functions->size()) {
        throw MapReduceError("invalid reduce function number");
    }

#ifdef V8_POST_3_19_API
    Local<Function> fun = Local<Function>::New(ctx->isolate, *(*ctx->functions)[reduceFunNum]);
#else
    Handle<Function> fun = (*ctx->functions)[reduceFunNum];
#endif
    Handle<Array> keysArray = jsonListToJsArray(keys);
    Handle<Array> valuesArray = jsonListToJsArray(values);
    Handle<Value> args[] = { keysArray, valuesArray, Boolean::New(false) };

    taskStarted(ctx);

    TryCatch trycatch;
    Handle<Value> result = fun->Call(fun, 3, args);

    taskFinished(ctx);
    freeLogResults(ctx);

    if (result.IsEmpty()) {
        if (!trycatch.CanContinue()) {
            throw MapReduceError("timeout");
        }

        throw MapReduceError(exceptionString(trycatch));
    }

    return jsonStringify(result);
}


ErlNifBinary runRereduce(map_reduce_ctx_t *ctx,
                       int reduceFunNum,
                       const json_results_list_t &reductions)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
    HandleScope handleScope(ctx->isolate);
    Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#else
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
#endif

    reduceFunNum -= 1;
    if (reduceFunNum < 0 ||
        static_cast<unsigned int>(reduceFunNum) >= ctx->functions->size()) {
        throw MapReduceError("invalid reduce function number");
    }

#ifdef V8_POST_3_19_API
    Local<Function> fun = Local<Function>::New(ctx->isolate, *(*ctx->functions)[reduceFunNum]);
#else
    Handle<Function> fun = (*ctx->functions)[reduceFunNum];
#endif
    Handle<Array> valuesArray = jsonListToJsArray(reductions);
    Handle<Value> args[] = { Null(), valuesArray, Boolean::New(true) };

    taskStarted(ctx);

    TryCatch trycatch;
    Handle<Value> result = fun->Call(fun, 3, args);

    taskFinished(ctx);
    freeLogResults(ctx);

    if (result.IsEmpty()) {
        if (!trycatch.CanContinue()) {
            throw MapReduceError("timeout");
        }

        throw MapReduceError(exceptionString(trycatch));
    }

    return jsonStringify(result);
}


void destroyContext(map_reduce_ctx_t *ctx)
{
    {
        Locker locker(ctx->isolate);
        Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
        HandleScope handleScope(ctx->isolate);
        Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#else
        HandleScope handleScope;
        Context::Scope contextScope(ctx->jsContext);
#endif

        for (unsigned int i = 0; i < ctx->functions->size(); ++i) {
#ifdef V8_POST_3_19_API
            (*ctx->functions)[i]->Dispose();
            (*ctx->functions)[i]->~Persistent<v8::Function>();
            enif_free((*ctx->functions)[i]);
#else
            (*ctx->functions)[i].Dispose();
#endif
        }
        ctx->functions->~function_vector_t();
        enif_free(ctx->functions);

        isolate_data_t *isoData = getIsolateData();
        isoData->jsonObject.Dispose();
        isoData->jsonObject.Clear();
        isoData->jsonParseFun.Dispose();
        isoData->jsonParseFun.Clear();
        isoData->stringifyFun.Dispose();
        isoData->stringifyFun.Clear();
        isoData->~isolate_data_t();
        enif_free(isoData);

        ctx->jsContext.Dispose();
        ctx->jsContext.Clear();
    }

    ctx->isolate->Dispose();
    ctx->~map_reduce_ctx_t();
}


#ifdef V8_POST_3_19_API
static Local<Context> createJsContext(map_reduce_ctx_t *ctx)
{
    HandleScope handleScope(ctx->isolate);
#else
Persistent<Context> createJsContext(map_reduce_ctx_t *ctx)
{
    HandleScope handleScope;
#endif
    Handle<ObjectTemplate> global = ObjectTemplate::New();

    global->Set(String::New("emit"), FunctionTemplate::New(emit));

    global->Set(String::New("log"), FunctionTemplate::New(log));

#ifdef V8_POST_3_19_API
    Handle<Context> context = Context::New(ctx->isolate, NULL, global);
#else
    Persistent<Context> context = Context::New(NULL, global);
#endif
    Context::Scope contextScope(context);

    Handle<Function> sumFun = compileFunction(SUM_FUNCTION_STRING);
    context->Global()->Set(String::New("sum"), sumFun);

    Handle<Function> decodeBase64Fun = compileFunction(BASE64_FUNCTION_STRING);
    context->Global()->Set(String::New("decodeBase64"), decodeBase64Fun);

    Handle<Function> dateToArrayFun = compileFunction(DATE_FUNCTION_STRING);
    context->Global()->Set(String::New("dateToArray"), dateToArrayFun);

#ifdef V8_POST_3_19_API
    return handleScope.Close(context);
#else
    return context;
#endif
}

#define TRUNCATE_STR "Truncated: "

#ifdef V8_POST_3_19_API
static void log(const v8::FunctionCallbackInfo<Value>& args)
#else
Handle<Value> log(const Arguments& args)
#endif
{
    try {
        isolate_data_t *isoData = getIsolateData();
        map_reduce_ctx_t *ctx = isoData->ctx;
        /* Initialize only if log function is used */
        if (ctx->logResults == NULL) {
            ctx->logResults = (log_results_list_t *) enif_alloc(sizeof
                    (log_results_list_t));
            if (ctx->logResults == NULL) {
                throw std::bad_alloc();
            }
            ctx->logResults = new (ctx->logResults) log_results_list_t();
        }
        /* use only first argument */
        Handle<Value> logMsg = args[0];
        Handle<String> str;
        unsigned int len = 0;
        if (logMsg->IsString()) {
            str = Handle<String>::Cast(logMsg);
            len = str->Length();
            if (len > MAX_LOG_STRING_SIZE) {
                str = Handle<String>(String::Concat(String::New(TRUNCATE_STR), str)),
                len = MAX_LOG_STRING_SIZE + sizeof(TRUNCATE_STR) - 1;
            }
        } else {
            str = Handle<String>(String::New("Error while logging:Log value is" \
              " not a string"));
            len = str->Length();
        }
        ErlNifBinary resultBin;
        if (!enif_alloc_binary_compat(isoData->ctx->env, len, &resultBin)) {
            throw std::bad_alloc();
        }
        str->WriteUtf8(reinterpret_cast<char *>(resultBin.data),
                len, NULL, String::NO_NULL_TERMINATION);
        ctx->logResults->push_back(resultBin);
    } catch(Handle<Value> &ex) {
#ifdef V8_POST_3_19_API
        ThrowException(ex);
#else
        return ThrowException(ex);
#endif
    }

#ifndef V8_POST_3_19_API
    return Undefined();
#endif
}


#ifdef V8_POST_3_19_API
static void emit(const v8::FunctionCallbackInfo<Value>& args)
#else
Handle<Value> emit(const Arguments& args)
#endif
{
    isolate_data_t *isoData = getIsolateData();

    if (isoData->ctx->kvs == NULL) {
#ifdef V8_POST_3_19_API
        return;
#else
        return Undefined();
#endif
    }

    ErlNifBinary keyJson = jsonStringify(args[0]);

    // Spatial views may emit a geometry that is bigger, when serialized
    // to JSON, than the allowed size of a key. In later steps it will then
    // be reduced to a bouning box. Hence don't check the string size of the
    // key of spatial views here.
    if (isoData->ctx->viewType != VIEW_INDEX_TYPE_SPATIAL &&
            keyJson.size >= MAX_EMIT_KEY_SIZE) {
        std::stringstream msg;
        msg << "too long key emitted: " << keyJson.size << " bytes";

#ifdef V8_POST_3_19_API
        ThrowException(Handle<Value>(String::New(msg.str().c_str())));
#else
        return ThrowException(Handle<Value>(String::New(msg.str().c_str())));
#endif
    }

    try {
        ErlNifBinary valueJson = jsonStringify(args[1]);

        kv_pair_t result = kv_pair_t(keyJson, valueJson);
        isoData->ctx->kvs->push_back(result);
        isoData->ctx->emitKvSize += keyJson.size;
        isoData->ctx->emitKvSize += valueJson.size;

    } catch(Handle<Value> &ex) {
#ifdef V8_POST_3_19_API
        ThrowException(ex);
#else
        return ThrowException(ex);
#endif
    }

    if ((isoData->ctx->maxEmitKvSize > 0) &&
        (isoData->ctx->emitKvSize > isoData->ctx->maxEmitKvSize)) {
        std::stringstream msg;
        msg << "too much data emitted: " << isoData->ctx->emitKvSize << " bytes";

#ifdef V8_POST_3_19_API
        ThrowException(Handle<Value>(String::New(msg.str().c_str())));
#else
        return ThrowException(Handle<Value>(String::New(msg.str().c_str())));
#endif
    }

#ifndef V8_POST_3_19_API
    return Undefined();
#endif
}


ErlNifBinary jsonStringify(const Handle<Value> &obj)
{
    isolate_data_t *isoData = getIsolateData();
    Handle<Value> args[] = { obj };
    TryCatch trycatch;
#ifdef V8_POST_3_19_API
    Local<Function> stringifyFun = Local<Function>::New(Isolate::GetCurrent(), isoData->stringifyFun);
    Local<Object> jsonObject = Local<Object>::New(Isolate::GetCurrent(), isoData->jsonObject);
    Handle<Value> result = stringifyFun->Call(jsonObject, 1, args);
#else
    Handle<Value> result = isoData->stringifyFun->Call(isoData->jsonObject, 1, args);
#endif

    if (result.IsEmpty()) {
        if (trycatch.HasCaught()) {
            Local<Message> m = trycatch.Message();
            if (!m.IsEmpty()) {
                throw Local<String>(m->Get());
            }
        }
        throw Handle<Value>(String::New("JSON.stringify() error"));
    }

    unsigned len;
    ErlNifBinary resultBin;

    if (result->IsUndefined()) {
        len = static_cast<unsigned>(sizeof("null") - 1);
        if (!enif_alloc_binary_compat(isoData->ctx->env, len, &resultBin)) {
            throw std::bad_alloc();
        }
        memcpy(resultBin.data, "null", len);
    } else {
        Handle<String> str = Handle<String>::Cast(result);
        len = str->Utf8Length();
        if (!enif_alloc_binary_compat(isoData->ctx->env, len, &resultBin)) {
            throw std::bad_alloc();
        }
        str->WriteUtf8(reinterpret_cast<char *>(resultBin.data),
                       len, NULL, String::NO_NULL_TERMINATION);
    }

    // Caller responsible for invoking enif_make_binary() or enif_release_binary()
    return resultBin;
}


Handle<Value> jsonParse(const ErlNifBinary &thing)
{
    isolate_data_t *isoData = getIsolateData();
    Handle<Value> args[] = { String::New(reinterpret_cast<char *>(thing.data), thing.size) };
    TryCatch trycatch;
#ifdef V8_POST_3_19_API
    Local<Function> jsonParseFun = Local<Function>::New(Isolate::GetCurrent(), isoData->jsonParseFun);
    Local<Object> jsonObject = Local<Object>::New(Isolate::GetCurrent(), isoData->jsonObject);
    Handle<Value> result = jsonParseFun->Call(jsonObject, 1, args);
#else
    Handle<Value> result = isoData->jsonParseFun->Call(isoData->jsonObject, 1, args);
#endif

    if (result.IsEmpty()) {
        throw MapReduceError(exceptionString(trycatch));
    }

    return result;
}


void loadFunctions(map_reduce_ctx_t *ctx, const function_sources_list_t &funStrings)
{
#ifdef V8_POST_3_19_API
    HandleScope handleScope(ctx->isolate);
#else
    HandleScope handleScope;
#endif

    ctx->functions = (function_vector_t *) enif_alloc(sizeof(function_vector_t));

    if (ctx->functions == NULL) {
        throw std::bad_alloc();
    }

    ctx->functions = new (ctx->functions) function_vector_t();
    function_sources_list_t::const_iterator it = funStrings.begin();

    for ( ; it != funStrings.end(); ++it) {
        Handle<Function> fun = compileFunction(*it);
#ifdef V8_POST_3_19_API
        Persistent<Function> *perFn = (Persistent<Function> *) enif_alloc(sizeof(Persistent<Function>));
        if (perFn == NULL) {
            throw std::bad_alloc();
        }
        perFn = new (perFn) Persistent<Function>();
        perFn->Reset(ctx->isolate, fun);
        ctx->functions->push_back(perFn);
#else
        ctx->functions->push_back(Persistent<Function>::New(fun));
#endif
    }
}


Handle<Function> compileFunction(const function_source_t &funSource)
{
#ifdef V8_POST_3_19_API
    HandleScope handleScope(Isolate::GetCurrent());
#else
    HandleScope handleScope;
#endif
    TryCatch trycatch;
    Handle<String> source = String::New(funSource.data(), funSource.length());
    Handle<Script> script = Script::Compile(source);

    if (script.IsEmpty()) {
        throw MapReduceError(exceptionString(trycatch));
    }

    Handle<Value> result = script->Run();

    if (result.IsEmpty()) {
        throw MapReduceError(exceptionString(trycatch));
    }

    if (!result->IsFunction()) {
        throw MapReduceError(std::string("Invalid function: ") + funSource.c_str());
    }

    return handleScope.Close(Handle<Function>::Cast(result));
}


Handle<Array> jsonListToJsArray(const json_results_list_t &list)
{
    Handle<Array> array = Array::New(list.size());
    json_results_list_t::const_iterator it = list.begin();
    int i = 0;

    for ( ; it != list.end(); ++it, ++i) {
        Handle<Value> v = jsonParse(*it);
        array->Set(Number::New(i), v);
    }

    return array;
}


isolate_data_t *getIsolateData()
{
    Isolate *isolate = Isolate::GetCurrent();
    return reinterpret_cast<isolate_data_t*>(isolate->GetData());
}


void taskStarted(map_reduce_ctx_t *ctx)
{
    ctx->taskStartTime = gethrtime();
    ctx->kvs = NULL;
}


void taskFinished(map_reduce_ctx_t *ctx)
{
    ctx->taskStartTime = 0;
}


void terminateTask(map_reduce_ctx_t *ctx)
{
    V8::TerminateExecution(ctx->isolate);
    taskFinished(ctx);
}


std::string exceptionString(const TryCatch &tryCatch)
{
#ifdef V8_POST_3_19_API
    HandleScope handleScope(Isolate::GetCurrent());
#else
    HandleScope handleScope;
#endif
    String::Utf8Value exception(tryCatch.Exception());
    const char *exceptionString = (*exception);

    if (exceptionString) {
        return std::string(exceptionString);
    }

    return std::string("runtime error");
}


void freeMapResultList(const map_results_list_t &results)
{
    map_results_list_t::const_iterator it = results.begin();

    for ( ; it != results.end(); ++it) {
        freeMapResult(*it);
    }
}


void freeMapResult(const map_result_t &mapResult)
{
    switch (mapResult.type) {
    case MAP_KVS:
        {
            kv_pair_list_t::const_iterator it = mapResult.result.kvs->begin();
            for ( ; it != mapResult.result.kvs->end(); ++it) {
                ErlNifBinary key = it->first;
                ErlNifBinary value = it->second;
                enif_release_binary(&key);
                enif_release_binary(&value);
            }
            mapResult.result.kvs->~kv_pair_list_t();
            enif_free(mapResult.result.kvs);
        }
        break;
    case MAP_ERROR:
        enif_release_binary(mapResult.result.error);
        enif_free(mapResult.result.error);
        break;
    }
}


void freeJsonData(const json_results_list_t &data)
{
    json_results_list_t::const_iterator it = data.begin();
    for ( ; it != data.end(); ++it) {
        ErlNifBinary bin = *it;
        enif_release_binary(&bin);
    }
}

// Free the logresults with data. For avoiding one extra allocation
// of logResults, allocation is done in log function, so which can cause
// memory leak if it is called from other (reduce, rereduce) context,
// so free that memory here.
void freeLogResults(map_reduce_ctx_t *ctx)
{
    if (ctx->logResults) {
        log_results_list_t::reverse_iterator k = ctx->logResults->rbegin();
        for ( ; k != ctx->logResults->rend(); ++k) {
            enif_release_binary_compat(ctx->env, &(*k));
        }
        ctx->logResults->~log_results_list_t();
        enif_free(ctx->logResults);
        ctx->logResults = NULL;
    }
}
