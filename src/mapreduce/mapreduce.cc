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
#include <v8.h>
#include <time.h>

#include "mapreduce.h"

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

static Persistent<Context> createJsContext(map_reduce_ctx_t *ctx);
static Handle<Value> emit(const Arguments& args);
static void loadFunctions(map_reduce_ctx_t *ctx, const function_sources_list_t &funs);
static void deleteJsonData(const json_results_list_t &data);
static void deleteJsonData(const map_results_list_t &data);
static Handle<Function> compileFunction(const function_source_t &funSource);
static inline ErlNifBinary jsonStringify(const Handle<Value> &obj);
static inline Handle<Value> jsonParse(const ErlNifBinary &thing);
static inline Handle<Array> jsonListToJsArray(const json_results_list_t &list);
static inline isolate_data_t *getIsolateData();
static inline void taskStarted(map_reduce_ctx_t *ctx);
static inline void taskFinished(map_reduce_ctx_t *ctx);
static std::string exceptionString(const TryCatch &tryCatch);



void initContext(map_reduce_ctx_t *ctx, const function_sources_list_t &funs)
{
    ctx->isolate = Isolate::New();
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
    Locker::StartPreemption(20);
    HandleScope handleScope;

    ctx->jsContext = createJsContext(ctx);
    Context::Scope contextScope(ctx->jsContext);

    Handle<Object> jsonObject = Local<Object>::Cast(ctx->jsContext->Global()->Get(String::New("JSON")));
    Handle<Function> parseFun = Local<Function>::Cast(jsonObject->Get(String::New("parse")));
    Handle<Function> stringifyFun = Local<Function>::Cast(jsonObject->Get(String::New("stringify")));

    isolate_data_t *isoData = (isolate_data_t *) enif_alloc(sizeof(isolate_data_t));
    if (isoData == NULL) {
        throw std::bad_alloc();
    }
    isoData->jsonObject = Persistent<Object>::New(jsonObject);
    isoData->jsonParseFun = Persistent<Function>::New(parseFun);
    isoData->stringifyFun = Persistent<Function>::New(stringifyFun);
    isoData->ctx = ctx;

    ctx->isolate->SetData(isoData);
    ctx->taskStartTime = -1;
    ctx->taskId = -1;

    loadFunctions(ctx, funs);
}


map_results_list_list_t mapDoc(map_reduce_ctx_t *ctx, const ErlNifBinary &doc, const ErlNifBinary &meta)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
    Handle<Value> docObject = jsonParse(doc);
    Handle<Value> metaObject = jsonParse(meta);

    if (!metaObject->IsObject()) {
        throw MapReduceError("metadata is not a JSON object");
    }

    map_results_list_list_t results;
    Handle<Value> funArgs[] = { docObject, metaObject };

    taskStarted(ctx);

    for (int i = 0; i < ctx->functions->size(); ++i) {
        map_results_list_t funResults;
        Handle<Function> fun = (*ctx->functions)[i];
        TryCatch trycatch;

        ctx->mapFunResults = &funResults;
        Handle<Value> result = fun->Call(fun, 2, funArgs);

        if (result.IsEmpty()) {
            map_results_list_list_t::iterator it = results.begin();
            for ( ; it != results.end(); ++it) {
                deleteJsonData(*it);
            }
            deleteJsonData(funResults);

            if (!trycatch.CanContinue()) {
                throw MapReduceError("timeout");
            }

            throw MapReduceError(exceptionString(trycatch));
        }

        results.push_back(funResults);
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
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
    Handle<Array> keysArray = jsonListToJsArray(keys);
    Handle<Array> valuesArray = jsonListToJsArray(values);
    json_results_list_t results;

    Handle<Value> args[] = { keysArray, valuesArray, Boolean::New(false) };

    taskStarted(ctx);

    for (int i = 0; i < ctx->functions->size(); ++i) {
        Handle<Function> fun = (*ctx->functions)[i];
        TryCatch trycatch;
        Handle<Value> result = fun->Call(fun, 3, args);

        if (result.IsEmpty()) {
            deleteJsonData(results);

            if (!trycatch.CanContinue()) {
                throw MapReduceError("timeout");
            }

            throw MapReduceError(exceptionString(trycatch));
        }

        try {
            ErlNifBinary jsonResult = jsonStringify(result);
            results.push_back(jsonResult);
        } catch(...) {
            deleteJsonData(results);
            throw;
        }
    }

    taskFinished(ctx);

    return results;
}


ErlNifBinary runReduce(map_reduce_ctx_t *ctx,
                       int reduceFunNum,
                       const json_results_list_t &keys,
                       const json_results_list_t &values)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);

    reduceFunNum -= 1;
    if (reduceFunNum < 0 || reduceFunNum >= ctx->functions->size()) {
        throw MapReduceError("invalid reduce function number");
    }

    Handle<Function> fun = (*ctx->functions)[reduceFunNum];
    Handle<Array> keysArray = jsonListToJsArray(keys);
    Handle<Array> valuesArray = jsonListToJsArray(values);
    Handle<Value> args[] = { keysArray, valuesArray, Boolean::New(false) };

    taskStarted(ctx);

    TryCatch trycatch;
    Handle<Value> result = fun->Call(fun, 3, args);

    taskFinished(ctx);

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
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);

    reduceFunNum -= 1;
    if (reduceFunNum < 0 || reduceFunNum >= ctx->functions->size()) {
        throw MapReduceError("invalid reduce function number");
    }

    Handle<Function> fun = (*ctx->functions)[reduceFunNum];
    Handle<Array> valuesArray = jsonListToJsArray(reductions);
    Handle<Value> args[] = { Null(), valuesArray, Boolean::New(true) };

    taskStarted(ctx);

    TryCatch trycatch;
    Handle<Value> result = fun->Call(fun, 3, args);

    taskFinished(ctx);

    if (result.IsEmpty()) {
        if (!trycatch.CanContinue()) {
            throw MapReduceError("timeout");
        }

        throw MapReduceError(exceptionString(trycatch));
    }

    return jsonStringify(result);
}


void deleteJsonData(const json_results_list_t &data)
{
    json_results_list_t::const_iterator it = data.begin();
    for ( ; it != data.end(); ++it) {
        ErlNifBinary bin = *it;
        enif_release_binary(&bin);
    }
}


void deleteJsonData(const map_results_list_t &data)
{
    map_results_list_t::const_iterator it = data.begin();
    for ( ; it != data.end(); ++it) {
        ErlNifBinary key = it->first;
        ErlNifBinary value = it->second;
        enif_release_binary(&key);
        enif_release_binary(&value);
    }
}


void destroyContext(map_reduce_ctx_t *ctx)
{
    {
        Locker locker(ctx->isolate);
        Isolate::Scope isolateScope(ctx->isolate);
        Locker::StopPreemption();
        HandleScope handleScope;
        Context::Scope contextScope(ctx->jsContext);

        for (int i = 0; i < ctx->functions->size(); ++i) {
            (*ctx->functions)[i].Dispose();
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
        enif_free(isoData);

        ctx->jsContext.Dispose();
        ctx->jsContext.Clear();
    }

    ctx->isolate->Dispose();
}


Persistent<Context> createJsContext(map_reduce_ctx_t *ctx)
{
    HandleScope handleScope;
    Handle<ObjectTemplate> global = ObjectTemplate::New();

    global->Set(String::New("emit"), FunctionTemplate::New(emit));

    Persistent<Context> context = Context::New(NULL, global);
    Context::Scope contextScope(context);

    Handle<Function> sumFun = compileFunction(SUM_FUNCTION_STRING);
    context->Global()->Set(String::New("sum"), sumFun);

    Handle<Function> decodeBase64Fun = compileFunction(BASE64_FUNCTION_STRING);
    context->Global()->Set(String::New("decodeBase64"), decodeBase64Fun);

    Handle<Function> dateToArrayFun = compileFunction(DATE_FUNCTION_STRING);
    context->Global()->Set(String::New("dateToArray"), dateToArrayFun);

    return context;
}


Handle<Value> emit(const Arguments& args)
{
    isolate_data_t *isoData = getIsolateData();

    try {
        ErlNifBinary keyJson = jsonStringify(args[0]);
        ErlNifBinary valueJson = jsonStringify(args[1]);

        map_result_t result = map_result_t(keyJson, valueJson);
        isoData->ctx->mapFunResults->push_back(result);

        return Undefined();
    } catch(Handle<Value> &ex) {
        return ThrowException(ex);
    }
}


ErlNifBinary jsonStringify(const Handle<Value> &obj)
{
    isolate_data_t *isoData = getIsolateData();
    Handle<Value> args[] = { obj };
    TryCatch trycatch;
    Handle<Value> result = isoData->stringifyFun->Call(isoData->jsonObject, 1, args);

    if (result.IsEmpty()) {
        throw trycatch.Exception();
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
    Handle<Value> result = isoData->jsonParseFun->Call(isoData->jsonObject, 1, args);

    if (result.IsEmpty()) {
        throw MapReduceError(exceptionString(trycatch));
    }

    return result;
}


void loadFunctions(map_reduce_ctx_t *ctx, const function_sources_list_t &funStrings)
{
    HandleScope handleScope;

    ctx->functions = (function_vector_t *) enif_alloc(sizeof(function_vector_t));

    if (ctx->functions == NULL) {
        throw std::bad_alloc();
    }

    ctx->functions = new (ctx->functions) function_vector_t();
    function_sources_list_t::const_iterator it = funStrings.begin();

    for ( ; it != funStrings.end(); ++it) {
        Handle<Function> fun = compileFunction(*it);

        ctx->functions->push_back(Persistent<Function>::New(fun));
    }
}


Handle<Function> compileFunction(const function_source_t &funSource)
{
    HandleScope handleScope;
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
    ctx->taskId = V8::GetCurrentThreadId();
    ctx->taskStartTime = static_cast<long>((clock() / CLOCKS_PER_SEC) * 1000);
}


void taskFinished(map_reduce_ctx_t *ctx)
{
    ctx->taskStartTime = -1;
    ctx->taskId = -1;
}


void terminateTask(map_reduce_ctx_t *ctx)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);

    if (ctx->taskId != -1) {
        V8::TerminateExecution(ctx->taskId);
        taskFinished(ctx);
    }
}


std::string exceptionString(const TryCatch &tryCatch)
{
    HandleScope handleScope;
    String::Utf8Value exception(tryCatch.Exception());
    const char *exceptionString = (*exception);

    if (exceptionString) {
        return std::string(exceptionString);
    }

    return std::string("runtime error");
}
