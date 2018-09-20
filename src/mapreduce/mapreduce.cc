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
#include <string>
#include <string.h>
#include <sstream>
#include <time.h>
#include <vector>
#include "mapreduce.h"
// This is libv8_libplatform library which handles garbage collection for v8
#include <libplatform/libplatform.h>
// Esprima unused and builtin JavaScript contents in raw string format
#include "jsfunctions/jsfunctions_data.h"

#define MAX_LOG_STRING_SIZE 1024

#define MAX_EMIT_KEY_SIZE 4096

using namespace v8;

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



typedef struct {
    Persistent<Object>    jsonObject;
    Persistent<Function>  jsonParseFun;
    Persistent<Function>  stringifyFun;
    map_reduce_ctx_t      *ctx;
} isolate_data_t;

static bool optimize_doc_load = true;
static void doInitContext(map_reduce_ctx_t *ctx,
                          const function_sources_list_t &funs,
                          const view_index_type_t viewType);
static Local<Context> createJsContext(map_reduce_ctx_t *ctx);
static void emit(const v8::FunctionCallbackInfo<Value>& args);
static void log(const v8::FunctionCallbackInfo<Value>& args);

static void loadFunctions(map_reduce_ctx_t *ctx,
                          const function_sources_list_t &funs);
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

static Platform *v8platform;
void initV8()
{
    V8::InitializeICUDefaultLocation("");
    v8platform = platform::CreateDefaultPlatform();
    V8::InitializePlatform(v8platform);
    V8::Initialize();
}

void deinitV8()
{
    V8::Dispose();
    V8::ShutdownPlatform();
    delete v8platform;
}

void setOptimizeDocLoadFlag(const char *flag)
{
    if(!strcmp(flag, "true"))
        optimize_doc_load = true;
    else
        optimize_doc_load = false;
}

void initContext(map_reduce_ctx_t *ctx, const function_sources_list_t &funs,
                 const view_index_type_t viewType)
{
    ctx = new (ctx) map_reduce_ctx_t();

    try {
        doInitContext(ctx, funs, viewType);
        Locker locker(ctx->isolate);
        Isolate::Scope isolate_scope(ctx->isolate);
        HandleScope handle_scope(ctx->isolate);
        Local<Context> context =
            Local<Context>::New(ctx->isolate, ctx->jsContext);
        Context::Scope context_scope(context);

        loadFunctions(ctx, funs);
    } catch (...) {
        // Releasing resource will invoke NIF destructor that calls destroyCtx
        enif_release_resource(ctx);
        throw;
    }
}

static Local<String> createUtf8String(Isolate *isolate, const char *str)
{
    return String::NewFromUtf8(isolate, str,
        NewStringType::kNormal).ToLocalChecked();
}

static Local<String> createUtf8String(Isolate *isolate, const char *str,
                                      size_t len)
{
    return String::NewFromUtf8(isolate, str,
        NewStringType::kNormal, len).ToLocalChecked();
}


void doInitContext(map_reduce_ctx_t *ctx, const function_sources_list_t &funs,
                   const view_index_type_t viewType)
{
    ctx->viewType = viewType;
    Isolate::CreateParams createParams;
    createParams.array_buffer_allocator =
      ArrayBuffer::Allocator::NewDefaultAllocator();
    ctx->isolate = Isolate::New(createParams);
    ctx->logResults = NULL;
    Locker locker(ctx->isolate);
    Isolate::Scope isolate_scope(ctx->isolate);
    HandleScope handle_scope(ctx->isolate);

    ctx->jsContext.Reset(ctx->isolate, createJsContext(ctx));
    Local<Context> context = Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope context_scope(context);

    Local<String> jsonString = createUtf8String(ctx->isolate, "JSON");
    Handle<Object> jsonObject =
        Local<Object>::Cast(context->Global()->Get(jsonString));

    Local<String> parseString = createUtf8String(ctx->isolate, "parse");
    Handle<Function> parseFun =
        Local<Function>::Cast(jsonObject->Get(parseString));
    Local<String> stringifyString = createUtf8String(ctx->isolate, "stringify");
    Handle<Function> stringifyFun =
        Local<Function>::Cast(jsonObject->Get(stringifyString));

    isolate_data_t *isoData =
        (isolate_data_t *) enif_alloc(sizeof(isolate_data_t));
    if (isoData == NULL) {
        throw std::bad_alloc();
    }

    isoData = new (isoData) isolate_data_t();
    isoData->jsonObject.Reset(ctx->isolate, jsonObject);
    isoData->jsonParseFun.Reset(ctx->isolate, parseFun);
    isoData->stringifyFun.Reset(ctx->isolate, stringifyFun);

    isoData->ctx = ctx;

    ctx->isolate->SetData(0, (void *)isoData);
    ctx->taskStartTime = {};
}


map_results_list_t mapDoc(map_reduce_ctx_t *ctx,
                          const ErlNifBinary &doc,
                          const ErlNifBinary &meta)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolate_scope(ctx->isolate);
    HandleScope handle_scope(ctx->isolate);
    Local<Context> context = Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope context_scope(context);
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
        Local<Function> fun =
            Local<Function>::New(ctx->isolate, *(*ctx->functions)[i]);
        TryCatch try_catch(ctx->isolate);

        mapResult.type = MAP_KVS;
        mapResult.result.kvs =
            (kv_pair_list_t *) enif_alloc(sizeof(kv_pair_list_t));

        if (mapResult.result.kvs == NULL) {
            freeMapResultList(results);
            throw std::bad_alloc();
        }

        mapResult.result.kvs = new (mapResult.result.kvs) kv_pair_list_t();
        ctx->kvs = mapResult.result.kvs;
        ctx->emitKvSize = 0;
        Handle<Value> result = fun->Call(context->Global(), 2, funArgs);

        if (result.IsEmpty()) {
            freeMapResult(mapResult);

            if (!try_catch.CanContinue()) {
                freeMapResultList(results);
                throw MapReduceError("timeout");
            }

            mapResult.type = MAP_ERROR;
            std::string exceptString = exceptionString(try_catch);
            size_t len = exceptString.length();

            mapResult.result.error =
                (ErlNifBinary *) enif_alloc(sizeof(ErlNifBinary));
            if (mapResult.result.error == NULL) {
                freeMapResultList(results);
                throw std::bad_alloc();
            }
            if (!enif_alloc_binary_compat(ctx->env, len,
                    mapResult.result.error)) {
                freeMapResultList(results);
                throw std::bad_alloc();
            }
            // Caller responsible for invoking enif_make_binary()
            // or enif_release_binary()
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
    Isolate::Scope isolate_scope(ctx->isolate);
    HandleScope handle_scope(ctx->isolate);
    Local<Context> context = Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope context_scope(context);
    Handle<Array> keysArray = jsonListToJsArray(keys);
    Handle<Array> valuesArray = jsonListToJsArray(values);
    json_results_list_t results;

    Handle<Value> args[] = { keysArray, valuesArray,
                             Boolean::New(ctx->isolate, false) };

    taskStarted(ctx);

    for (unsigned int i = 0; i < ctx->functions->size(); ++i) {
        Local<Function> fun =
            Local<Function>::New(ctx->isolate, *(*ctx->functions)[i]);
        TryCatch try_catch(ctx->isolate);
        Handle<Value> result = fun->Call(context->Global(), 3, args);

        if (result.IsEmpty()) {
            freeJsonData(results);

            if (!try_catch.CanContinue()) {
                throw MapReduceError("timeout");
            }

            throw MapReduceError(exceptionString(try_catch));
        }

        try {
            ErlNifBinary jsonResult = jsonStringify(result);
            results.push_back(jsonResult);
        } catch(Handle<String> &ex) {
            freeJsonData(results);
            ctx->isolate->ThrowException(ex);
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
    Isolate::Scope isolate_scope(ctx->isolate);
    HandleScope handle_scope(ctx->isolate);
    Local<Context> context = Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope context_scope(context);

    reduceFunNum -= 1;
    if (reduceFunNum < 0 ||
        static_cast<unsigned int>(reduceFunNum) >= ctx->functions->size()) {
        throw MapReduceError("invalid reduce function number");
    }

    Local<Function> fun =
        Local<Function>::New(ctx->isolate, *(*ctx->functions)[reduceFunNum]);
    Handle<Array> keysArray = jsonListToJsArray(keys);
    Handle<Array> valuesArray = jsonListToJsArray(values);
    Handle<Value> args[] = { keysArray, valuesArray,
                             Boolean::New(ctx->isolate, false) };

    taskStarted(ctx);

    TryCatch try_catch(ctx->isolate);
    Handle<Value> result = fun->Call(context->Global(), 3, args);

    taskFinished(ctx);
    freeLogResults(ctx);

    if (result.IsEmpty()) {
        if (!try_catch.CanContinue()) {
            throw MapReduceError("timeout");
        }

        throw MapReduceError(exceptionString(try_catch));
    }

    ErlNifBinary jsonResult;
    try {
        jsonResult = jsonStringify(result);
    } catch(Handle<String> &ex) {
        ctx->isolate->ThrowException(ex);
    }

    return jsonResult;
}


ErlNifBinary runRereduce(map_reduce_ctx_t *ctx,
                       int reduceFunNum,
                       const json_results_list_t &reductions)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolate_scope(ctx->isolate);
    HandleScope handle_scope(ctx->isolate);
    Local<Context> context = Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope context_scope(context);

    reduceFunNum -= 1;
    if (reduceFunNum < 0 ||
        static_cast<unsigned int>(reduceFunNum) >= ctx->functions->size()) {
        throw MapReduceError("invalid reduce function number");
    }

    Local<Function> fun =
        Local<Function>::New(ctx->isolate, *(*ctx->functions)[reduceFunNum]);
    Handle<Array> valuesArray = jsonListToJsArray(reductions);
    Handle<Value> args[] =
        { Null(ctx->isolate), valuesArray, Boolean::New(ctx->isolate, true) };

    taskStarted(ctx);

    TryCatch try_catch(ctx->isolate);
    Handle<Value> result = fun->Call(context->Global(), 3, args);

    taskFinished(ctx);
    freeLogResults(ctx);

    if (result.IsEmpty()) {
        if (!try_catch.CanContinue()) {
            throw MapReduceError("timeout");
        }

        throw MapReduceError(exceptionString(try_catch));
    }

    ErlNifBinary jsonResult;
    try {
        jsonResult = jsonStringify(result);
    } catch(Handle<String> &ex) {
        ctx->isolate->ThrowException(ex);
    }

    return jsonResult;
}


void destroyContext(map_reduce_ctx_t *ctx)
{
    {
        Locker locker(ctx->isolate);
        Isolate::Scope isolate_scope(ctx->isolate);
        HandleScope handle_scope(ctx->isolate);
        Local<Context> context =
            Local<Context>::New(ctx->isolate, ctx->jsContext);
        Context::Scope context_scope(context);

        if (ctx->functions) {
            for (unsigned int i = 0; i < ctx->functions->size(); ++i) {
                (*ctx->functions)[i]->Reset();
                (*ctx->functions)[i]->~Persistent<v8::Function>();
                enif_free((*ctx->functions)[i]);
            }
            ctx->functions->~function_vector_t();
            enif_free(ctx->functions);
        }

        isolate_data_t *isoData = getIsolateData();
        if(isoData) {
            isoData->jsonObject.Reset();
            isoData->jsonParseFun.Reset();
            isoData->stringifyFun.Reset();
            isoData->~isolate_data_t();
            enif_free(isoData);
        }

        ctx->jsContext.Reset();
    }

    ctx->isolate->Dispose();
    ctx->~map_reduce_ctx_t();
}


static Local<Context> createJsContext(map_reduce_ctx_t *ctx)
{
    EscapableHandleScope handle_scope(ctx->isolate);
    Handle<ObjectTemplate> global = ObjectTemplate::New();

    global->Set(createUtf8String(ctx->isolate, "emit"),
            FunctionTemplate::New(ctx->isolate, emit));

    global->Set(createUtf8String(ctx->isolate, "log"),
            FunctionTemplate::New(ctx->isolate, log));

    Handle<Context> context = Context::New(ctx->isolate, NULL, global);
    Context::Scope context_scope(context);

    Handle<Function> sumFun = compileFunction(SUM_FUNCTION_STRING);
    context->Global()->Set(createUtf8String(ctx->isolate, "sum"), sumFun);

    Handle<Function> decodeBase64Fun =
        compileFunction(BASE64_FUNCTION_STRING);
    context->Global()->Set(createUtf8String(ctx->isolate, "decodeBase64"),
        decodeBase64Fun);

    Handle<Function> dateToArrayFun =
        compileFunction(DATE_FUNCTION_STRING);
    context->Global()->Set(createUtf8String(ctx->isolate, "dateToArray"),
                           dateToArrayFun);


    // Use EscapableHandleScope and return using .Escape
    // This will ensure that return values are not garbage collected
    // as soon as the function returns.
    return handle_scope.Escape(context);
}

#define TRUNCATE_STR "Truncated: "

static void log(const v8::FunctionCallbackInfo<Value>& args)
{
    isolate_data_t *isoData = getIsolateData();
    map_reduce_ctx_t *ctx = isoData->ctx;
    try {
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
                str = Handle<String>(String::Concat(
                          createUtf8String(ctx->isolate, TRUNCATE_STR), str)),
                len = MAX_LOG_STRING_SIZE + sizeof(TRUNCATE_STR) - 1;
            }
        } else {
            str = Handle<String>(createUtf8String(ctx->isolate,
                        "Error while logging:Log value is not a string"));
            len = str->Length();
        }
        ErlNifBinary resultBin;
        if (!enif_alloc_binary_compat(isoData->ctx->env, len, &resultBin)) {
            throw std::bad_alloc();
        }
        str->WriteUtf8(reinterpret_cast<char *>(resultBin.data),
                len, NULL, String::NO_NULL_TERMINATION);
        ctx->logResults->push_back(resultBin);
    } catch(Handle<String> &ex) {
        ctx->isolate->ThrowException(ex);
    }
}


static void emit(const v8::FunctionCallbackInfo<Value>& args)
{
    isolate_data_t *isoData = getIsolateData();

    if (isoData->ctx->kvs == NULL) {
        return;
    }

    ErlNifBinary keyJson;
    try {
        keyJson = jsonStringify(args[0]);
    } catch(Handle<String> &ex) {
        isoData->ctx->isolate->ThrowException(ex);
        return;
    }

    // Spatial views may emit a geometry that is bigger, when serialized
    // to JSON, than the allowed size of a key. In later steps it will then
    // be reduced to a bouning box. Hence don't check the string size of the
    // key of spatial views here.
    if (isoData->ctx->viewType != VIEW_INDEX_TYPE_SPATIAL &&
            keyJson.size >= MAX_EMIT_KEY_SIZE) {
        std::stringstream msg;
        msg << "too long key emitted: " << keyJson.size << " bytes";

        isoData->ctx->isolate->ThrowException(
                createUtf8String(isoData->ctx->isolate, msg.str().c_str())
                                             );
	return;
    }

    try {
        ErlNifBinary valueJson = jsonStringify(args[1]);

        kv_pair_t result = kv_pair_t(keyJson, valueJson);
        isoData->ctx->kvs->push_back(result);
        isoData->ctx->emitKvSize += keyJson.size;
        isoData->ctx->emitKvSize += valueJson.size;

    } catch(Handle<String> &ex) {
        isoData->ctx->isolate->ThrowException(ex);
    }

    if ((isoData->ctx->maxEmitKvSize > 0) &&
        (isoData->ctx->emitKvSize > isoData->ctx->maxEmitKvSize)) {
        std::stringstream msg;
        msg << "too much data emitted: " << isoData->ctx->emitKvSize << " bytes";

        isoData->ctx->isolate->ThrowException(
                createUtf8String(isoData->ctx->isolate, msg.str().c_str()));
    }
}


ErlNifBinary jsonStringify(const Handle<Value> &obj)
{
    isolate_data_t *isoData = getIsolateData();
    Handle<Value> args[] = { obj };
    TryCatch try_catch(isoData->ctx->isolate);
    Local<Function> stringifyFun =
        Local<Function>::New(isoData->ctx->isolate, isoData->stringifyFun);
    Local<Object> jsonObject =
        Local<Object>::New(isoData->ctx->isolate, isoData->jsonObject);
    Handle<Value> result = stringifyFun->Call(jsonObject, 1, args);

    if (result.IsEmpty()) {
        if (try_catch.HasCaught()) {
            Local<Message> m = try_catch.Message();
            if (!m.IsEmpty()) {
                throw Local<String>(m->Get());
            }
        }
        throw Handle<Value>(createUtf8String(isoData->ctx->isolate,
                    "JSON.stringify() error"));
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

    // Caller responsible for invoking enif_make_binary()
    // or enif_release_binary()
    return resultBin;
}


Handle<Value> jsonParse(const ErlNifBinary &thing)
{
    isolate_data_t *isoData = getIsolateData();
    Handle<Value> args[] = { createUtf8String(isoData->ctx->isolate,
                             reinterpret_cast<char *>(thing.data),
                             (size_t)thing.size) };
    TryCatch try_catch(isoData->ctx->isolate);
    Local<Function> jsonParseFun =
        Local<Function>::New(isoData->ctx->isolate, isoData->jsonParseFun);
    Local<Object> jsonObject =
        Local<Object>::New(isoData->ctx->isolate, isoData->jsonObject);
    Handle<Value> result = jsonParseFun->Call(jsonObject, 1, args);

    if (result.IsEmpty()) {
        throw MapReduceError(exceptionString(try_catch));
    }

    return result;
}


void loadFunctions(map_reduce_ctx_t *ctx,
                   const function_sources_list_t &funStrings)
{
    HandleScope handle_scope(ctx->isolate);
    Local<Context> context = Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope context_scope(context);

    bool isDocUsed;
    if(optimize_doc_load) {
        // If esprima compilation fails restore back to pulling in documents.
        try {
            compileFunction((char *)jsFunction_src);
            isDocUsed = false;
        } catch(...) {
            isDocUsed = true;
        }
    }
    else {
        isDocUsed = true;
    }

    ctx->functions = (function_vector_t *) enif_alloc(sizeof(function_vector_t));
    if (ctx->functions == NULL) {
        throw std::bad_alloc();
    }

    ctx->functions = new (ctx->functions) function_vector_t();
    function_sources_list_t::const_iterator it = funStrings.begin();

    for ( ; it != funStrings.end(); ++it) {
        Handle<Function> fun = compileFunction(*it);
        // Do this if is_doc_unused function compilation is successful
        if(optimize_doc_load && !isDocUsed) {
            Handle<Value> val = context->Global()->Get(
                createUtf8String(ctx->isolate, "is_doc_unused"));
            Handle<Function> unusedFun = Handle<Function>::Cast(val);
            Handle<Value> arg = createUtf8String(ctx->isolate, it->data());
            TryCatch try_catch(ctx->isolate);
            Handle<Value> js_result = unusedFun->Call(context->Global(), 1, &arg);
            if (try_catch.HasCaught()) {
                throw MapReduceError(exceptionString(try_catch));
            }
            if (js_result.IsEmpty()) {
                // Some error during static analysis of map function
                throw MapReduceError("Malformed map function");
            }
            bool isDocUnused = js_result->BooleanValue();
            if (isDocUnused == false) {
                isDocUsed = true;
            }
        }
        Persistent<Function> *perFn =
            (Persistent<Function> *) enif_alloc(sizeof(Persistent<Function>));
        if (perFn == NULL) {
            throw std::bad_alloc();
        }
        perFn = new (perFn) Persistent<Function>();
        perFn->Reset(ctx->isolate, fun);
        ctx->functions->push_back(perFn);
    }
    ctx->isDocUsed = isDocUsed;
}


Handle<Function> compileFunction(const function_source_t &funSource)
{
    Isolate *isolate = Isolate::GetCurrent();
    Local<Context> context(isolate->GetCurrentContext());
    EscapableHandleScope handle_scope(isolate);
    TryCatch try_catch(isolate);
    Handle<String> source =
        createUtf8String(isolate, funSource.data(), funSource.length());
    Local<Script> script;
    if (!Script::Compile(context, source).ToLocal(&script)) {
        throw MapReduceError(exceptionString(try_catch));
    }

    if (script.IsEmpty()) {
        throw MapReduceError(exceptionString(try_catch));
    }

    Handle<Value> result = script->Run();

    if (result.IsEmpty()) {
        throw MapReduceError(exceptionString(try_catch));
    }

    if (!result->IsFunction()) {
        throw MapReduceError(std::string("Invalid function: ") +
                funSource.c_str());
    }

    return handle_scope.Escape(Handle<Function>::Cast(result));
}


Handle<Array> jsonListToJsArray(const json_results_list_t &list)
{
    Isolate *isolate = Isolate::GetCurrent();
    Handle<Array> array = Array::New(isolate, list.size());
    json_results_list_t::const_iterator it = list.begin();
    int i = 0;

    for ( ; it != list.end(); ++it, ++i) {
        Handle<Value> v = jsonParse(*it);
        array->Set(Number::New(isolate, i), v);
    }

    return array;
}


isolate_data_t *getIsolateData()
{
    Isolate *isolate = Isolate::GetCurrent();
    return reinterpret_cast<isolate_data_t*>(isolate->GetData(0));
}


void taskStarted(map_reduce_ctx_t *ctx)
{
    ctx->exitMutex.lock();
    ctx->taskStartTime = std::chrono::high_resolution_clock::now();
    ctx->exitMutex.unlock();
    ctx->kvs = NULL;
}


void taskFinished(map_reduce_ctx_t *ctx)
{
    ctx->exitMutex.lock();
    ctx->taskStartTime = {};
    ctx->exitMutex.unlock();
}


void terminateTask(map_reduce_ctx_t *ctx)
{
    V8::TerminateExecution(ctx->isolate);
    ctx->taskStartTime = {};
}


std::string exceptionString(const TryCatch &tryCatch)
{
    HandleScope handle_scope(Isolate::GetCurrent());
    String::Utf8Value exception(tryCatch.Exception());
    const char *exceptionString = (*exception);

    if (exceptionString) {
        Handle<Message> message = tryCatch.Message();
        return std::string(exceptionString) + " (line " +
                std::to_string(message->GetLineNumber()) + ":" +
                std::to_string(message->GetStartColumn()) + ")";
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
