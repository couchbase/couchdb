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

// NOTE: keep this file clean (without knowledge) of any Erlang NIF APIs

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

static const char *BASE64_FUNCTION_STRING =
  "(function(b64) {"
  "	var i, j, l, tmp, scratch, arr = [], lookup = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';"
  "	if (b64.length % 4 > 0) {"
  "		throw 'Invalid base64 source.';"
  "	}"
  "	scratch = b64.indexOf('=');"
  "	scratch = scratch > 0 ? b64.length - scratch : 0;"
  "	l = scratch > 0 ? b64.length - 4 : b64.length;"
  "	for (i = 0, j = 0; i < l; i += 4, j += 3) {"
  "		tmp = (lookup.indexOf(b64[i]) << 18) | (lookup.indexOf(b64[i + 1]) << 12) | (lookup.indexOf(b64[i + 2]) << 6) | lookup.indexOf(b64[i + 3]);"
  "		arr.push((tmp & 0xFF0000) >> 16);"
  "		arr.push((tmp & 0xFF00) >> 8);"
  "		arr.push(tmp & 0xFF);"
  "	}"
  "	if (scratch === 2) {"
  "		tmp = (lookup.indexOf(b64[i]) << 2) | (lookup.indexOf(b64[i + 1]) >> 4);"
  "		arr.push(tmp & 0xFF);"
  "	} else if (scratch === 1) {"
  "		tmp = (lookup.indexOf(b64[i]) << 10) | (lookup.indexOf(b64[i + 1]) << 4) | (lookup.indexOf(b64[i + 2]) >> 2);"
  "		arr.push((tmp >> 8) & 0xFF);"
  "		arr.push(tmp & 0xFF);"
  "	}"
  "	return String.fromCharCode.apply(String, arr);"
  "})";

static Persistent<Context> createJsContext(map_reduce_ctx_t *ctx);
static Handle<Value> emit(const Arguments& args);
static void loadFunctions(map_reduce_ctx_t *ctx, const std::list<std::string> &funs);
static void deleteJsonData(const std::list<json_bin_t> &data);
static void deleteJsonData(const std::list< map_result_t > &data);
static Handle<Function> compileFunction(const std::string &funSource);
static inline json_bin_t jsonStringify(const Handle<Value> &obj);
static inline Handle<Value> jsonParse(const json_bin_t &thing);
static inline Handle<Array> jsonListToJsArray(const std::list<json_bin_t> &list);
static inline isolate_data_t *getIsolateData();
static inline void taskStarted(map_reduce_ctx_t *ctx);
static inline void taskFinished(map_reduce_ctx_t *ctx);



void initContext(map_reduce_ctx_t *ctx, const std::list<std::string> &funs)
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

    isolate_data_t *isoData = new isolate_data_t();
    isoData->jsonObject = Persistent<Object>::New(jsonObject);
    isoData->jsonParseFun = Persistent<Function>::New(parseFun);
    isoData->stringifyFun = Persistent<Function>::New(stringifyFun);
    isoData->ctx = ctx;

    ctx->isolate->SetData(isoData);
    ctx->taskStartTime = -1;
    ctx->taskId = -1;

    loadFunctions(ctx, funs);
}


std::list< std::list< map_result_t > > mapDoc(map_reduce_ctx_t *ctx, const json_bin_t &doc)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
    Handle<Value> docObject = jsonParse(doc);

    if (!docObject->IsObject()) {
        throw MapReduceError("document is not a JSON object");
    }

    std::list< std::list< map_result_t > > results;
    Handle<Value> funArgs[] = { docObject };

    taskStarted(ctx);

    for (int i = 0; i < ctx->functions->size(); ++i) {
        std::list< map_result_t > funResults;
        Handle<Function> fun = (*ctx->functions)[i];
        TryCatch trycatch;

        ctx->mapFunResults = &funResults;
        Handle<Value> result = fun->Call(fun, 1, funArgs);

        if (result.IsEmpty()) {
            std::list< std::list< map_result_t > >::iterator it = results.begin();
            for ( ; it != results.end(); ++it) {
                deleteJsonData(*it);
            }
            deleteJsonData(funResults);

            if (!trycatch.CanContinue()) {
                throw MapReduceError("timeout");
            }

            Handle<Value> exception = trycatch.Exception();
            String::AsciiValue exceptionStr(exception);

            throw MapReduceError(*exceptionStr);
        }

        results.push_back(funResults);
    }

    taskFinished(ctx);

    return results;
}


std::list<json_bin_t> runReduce(map_reduce_ctx_t *ctx, const std::list<json_bin_t> &keys, const std::list<json_bin_t> &values)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
    Handle<Array> keysArray = jsonListToJsArray(keys);
    Handle<Array> valuesArray = jsonListToJsArray(values);
    std::list<json_bin_t> results;

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

            Handle<Value> exception = trycatch.Exception();
            String::AsciiValue exceptionStr(exception);

            throw MapReduceError(*exceptionStr);
        }

        try {
            json_bin_t jsonResult = jsonStringify(result);
            results.push_back(jsonResult);
        } catch(...) {
            deleteJsonData(results);
            throw;
        }
    }

    taskFinished(ctx);

    return results;
}


json_bin_t runReduce(map_reduce_ctx_t *ctx, int reduceFunNum, const std::list<json_bin_t> &keys, const std::list<json_bin_t> &values)
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

        Handle<Value> exception = trycatch.Exception();
        String::AsciiValue exceptionStr(exception);

        throw MapReduceError(*exceptionStr);
    }

    return jsonStringify(result);
}


json_bin_t runRereduce(map_reduce_ctx_t *ctx, int reduceFunNum, const std::list<json_bin_t> &reductions)
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

        Handle<Value> exception = trycatch.Exception();
        String::AsciiValue exceptionStr(exception);

        throw MapReduceError(*exceptionStr);
    }

    return jsonStringify(result);
}


void deleteJsonData(const std::list<json_bin_t> &data)
{
    std::list<json_bin_t>::const_iterator it = data.begin();
    for ( ; it != data.end(); ++it) {
        delete [] it->data;
    }
}


void deleteJsonData(const std::list< map_result_t > &data)
{
    std::list< map_result_t >::const_iterator it = data.begin();
    for ( ; it != data.end(); ++it) {
        json_bin_t key = it->first;
        json_bin_t value = it->second;
        delete [] key.data;
        delete [] value.data;
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
        delete ctx->functions;

        isolate_data_t *isoData = getIsolateData();
        isoData->jsonObject.Dispose();
        isoData->jsonObject.Clear();
        isoData->jsonParseFun.Dispose();
        isoData->jsonParseFun.Clear();
        isoData->stringifyFun.Dispose();
        isoData->stringifyFun.Clear();
        delete isoData;

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

    return context;
}


Handle<Value> emit(const Arguments& args)
{
    isolate_data_t *isoData = getIsolateData();

    try {
        json_bin_t keyJson = jsonStringify(args[0]);
        json_bin_t valueJson = jsonStringify(args[1]);

        map_result_t result = std::pair<json_bin_t, json_bin_t>(keyJson, valueJson);
        isoData->ctx->mapFunResults->push_back(result);

        return Undefined();
    } catch(Handle<Value> &ex) {
        return ThrowException(ex);
    }
}


json_bin_t jsonStringify(const Handle<Value> &obj)
{
    isolate_data_t *isoData = getIsolateData();
    Handle<Value> args[] = { obj };
    TryCatch trycatch;
    Handle<Value> result = isoData->stringifyFun->Call(isoData->jsonObject, 1, args);

    if (result.IsEmpty()) {
        throw trycatch.Exception();
    }

    char *data;
    int len;

    if (result->IsUndefined()) {
        len = static_cast<int>(sizeof("null") - 1);
        data = new char[len];
        memcpy(data, "null", len);
    } else {
        Handle<String> str = Handle<String>::Cast(result);
        len = str->Utf8Length();
        data = new char[len];
        str->WriteUtf8(data, len, NULL, String::NO_NULL_TERMINATION);
    }

    // Caller responsible for deallocating data.
    return json_bin_t(data, len);
}


Handle<Value> jsonParse(const json_bin_t &thing)
{
    isolate_data_t *isoData = getIsolateData();
    Handle<Value> args[] = { String::New(thing.data, thing.length) };
    TryCatch trycatch;
    Handle<Value> result = isoData->jsonParseFun->Call(isoData->jsonObject, 1, args);

    if (result.IsEmpty()) {
        Handle<Value> exception = trycatch.Exception();
        String::AsciiValue exceptionStr(exception);

        throw MapReduceError(*exceptionStr);
    }

    return result;
}


void loadFunctions(map_reduce_ctx_t *ctx, const std::list<std::string> &funStrings)
{
    HandleScope handleScope;
    ctx->functions = new std::vector< v8::Persistent<v8::Function> >();
    std::list<std::string>::const_iterator it = funStrings.begin();

    for ( ; it != funStrings.end(); ++it) {
        Handle<Function> fun = compileFunction(*it);

        ctx->functions->push_back(Persistent<Function>::New(fun));
    }
}


Handle<Function> compileFunction(const std::string &funSource)
{
    HandleScope handleScope;
    TryCatch trycatch;
    Handle<String> source = String::New(funSource.data(), funSource.length());
    Handle<Script> script = Script::Compile(source);

    if (script.IsEmpty()) {
        Handle<Value> exception = trycatch.Exception();
        String::AsciiValue exceptionStr(exception);

        throw MapReduceError(*exceptionStr);
    }

    Handle<Value> result = script->Run();

    if (result.IsEmpty()) {
        Handle<Value> exception = trycatch.Exception();
        String::AsciiValue exceptionStr(exception);

        throw MapReduceError(*exceptionStr);
    }

    if (!result->IsFunction()) {
        throw MapReduceError(std::string("Invalid function: ") + funSource);
    }

    return handleScope.Close(Handle<Function>::Cast(result));
}


Handle<Array> jsonListToJsArray(const std::list<json_bin_t> &list)
{
    Handle<Array> array = Array::New(list.size());
    std::list<json_bin_t>::const_iterator it = list.begin();
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
