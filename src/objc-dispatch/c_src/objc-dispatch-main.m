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

#import "CouchbaseViewDispatcher.h"
#import "CouchbaseAppServer.h"
#include "erl_nif.h"
#include "util.h"
#import "objc_to_term.h"
#import "term_to_objc.h"


typedef struct {
    ErlNifResourceType *objcInstanceResourceType;
} objc_dispatch_state; // Module-wide state

static void release_instance(ErlNifEnv *env, void *obj);

static int load(ErlNifEnv *env, void **priv, ENTERM load_info)
{
    // Set up the Erlang resource type to be used by the wrap() and unwrap() functions below:
    ErlNifResourceType *instanceStateType = enif_open_resource_type(env, NULL, "objc_dispatch",
                                                                    release_instance,
                                                                    ERL_NIF_RT_CREATE, NULL);
    if (!instanceStateType)
        return -1;

    objc_dispatch_state *state = enif_alloc(sizeof(objc_dispatch_state));
    state->objcInstanceResourceType = instanceStateType;
    *priv = state;
    return 0;
}

static void unload(ErlNifEnv *env, void *priv)
{
    objc_dispatch_state *state = priv;
    enif_free(state);
}

static void release_instance(ErlNifEnv *env, void *obj)
{
    id resource = *(id *)obj;
    [resource release];
}


/** Wraps an Objective-C object in an Erlang resource and returns the resource. */
static ERL_NIF_TERM wrap(ErlNifEnv *env, id object) {
    objc_dispatch_state *state = enif_priv_data(env);
    CouchbaseAppServer **resource = enif_alloc_resource(state->objcInstanceResourceType,
                                                        sizeof(id));
    *resource = object;
    ERL_NIF_TERM response = enif_make_resource(env, resource);
    enif_release_resource(resource);
    return response;
}


/** Returns the Objective-C object wrapped in the given Erlang resource. */
static id unwrap(ErlNifEnv *env, ERL_NIF_TERM resource) {
    objc_dispatch_state *state = enif_priv_data(env);
    id* appResource;
    if (!enif_get_resource(env, resource,
                           state->objcInstanceResourceType,
                           (void *)&appResource))
        return nil;
    return *appResource;
}


#pragma mark Utilities

/** Runs the block inside a try-catch block, with an autorelease pool.
    If the block raises an exception or returns 0, returns {error,"<reason>"}.
    Otherwise returns whatever the block returned. */
static ERL_NIF_TERM safely(ErlNifEnv *env, ERL_NIF_TERM (^block)()) {
    ERL_NIF_TERM result;
    NSAutoreleasePool *pool = [[NSAutoreleasePool alloc] init];
    @try {
        result = block();
        if (!result)
            result = util_mk_error(env, "no term returned");
    } @catch (NSException *ex) {
        result = util_mk_error_str(env, [[ex description] UTF8String]);
    }
    [pool drain];
    return result;
}


static BOOL getBool(ErlNifEnv *env, ERL_NIF_TERM atomTerm, BOOL *outBool)
{
    char trueOrFalse[6];
    if (!enif_get_atom(env, atomTerm, trueOrFalse, sizeof(trueOrFalse), ERL_NIF_LATIN1))
        return NO;

    BOOL isTrue;
    if (strcmp(trueOrFalse, "true") == 0)
        isTrue = YES;
    else if (strcmp(trueOrFalse, "false") == 0)
        isTrue = NO;
    else
        return NO;

    if (outBool)
        *outBool = isTrue;
    return YES;
}


#pragma mark - View Server Functions

// create_nif(Name, MapKeys, RedKeys) ->
static ERL_NIF_TERM create(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc != 3) return enif_make_badarg(env);
    ERL_NIF_TERM queueNameTerm = argv[0], mapKeyList = argv[1], reduceKeyList = argv[2];
    if (!enif_is_binary(env, queueNameTerm) || !enif_is_list(env, mapKeyList)
            || !enif_is_list(env, reduceKeyList))
        return enif_make_badarg(env);

    return safely(env, ^ERL_NIF_TERM {
        CouchbaseViewDispatcher *dispatcher =
            [[CouchbaseViewDispatcher alloc] initWithQueueName:term_to_nsstring(env, queueNameTerm)
                                                       mapKeys:term_to_nsarray(env, mapKeyList)
                                                    reduceKeys:term_to_nsarray(env, reduceKeyList)];
        if (!dispatcher)
            return enif_make_badarg(env);
        return util_mk_ok(env, wrap(env, dispatcher));
    });
}


// query_view_version_nif(MapKey) ->
static ERL_NIF_TERM query_view_version(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc != 1) return enif_make_badarg(env);
    ERL_NIF_TERM mapKeyTerm = argv[0];
    if (!enif_is_binary(env, mapKeyTerm))
        return enif_make_badarg(env);

    return safely(env, ^ERL_NIF_TERM {
        NSString* mapKey = term_to_nsstring(env, mapKeyTerm);

        // Just use a no-op mapping assuming the fn def is a unique string
        // TODO: Is there anything better to do here?
        NSString *identifier = mapKey;

        ERL_NIF_TERM response;
        if (identifier.length == 0 || !nsstring_to_term(env, identifier, &response))
            return util_mk_error_str(env, "no view version identifier");

        return util_mk_ok(env, response);
    });
}


// map_nif(QueueRef, ReplyRef, DocJson) ->
static ERL_NIF_TERM map(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc != 3) return enif_make_badarg(env);
    ERL_NIF_TERM uniqueRef = argv[1], docTerm = argv[2];
    if (!enif_is_ref(env, uniqueRef) ||!enif_is_tuple(env, docTerm))
        return enif_make_badarg(env);

    CouchbaseViewDispatcher *dispatcher = unwrap(env, argv[0]);
    if (!dispatcher)
        return enif_make_badarg(env);

    ErlNifEnv *responseEnv = enif_alloc_env();
    if (!responseEnv)
        return util_mk_error_str(env, "Couldn't create NIF environment for response");
    ErlNifPid responsePid;
    enif_self(env, &responsePid);
    ERL_NIF_TERM requestRef = enif_make_copy(responseEnv, uniqueRef);

    return safely(env, ^ERL_NIF_TERM {
        NSDictionary *jsonDoc = term_to_nsdict(env, docTerm);
        if (!jsonDoc)
            return enif_make_badarg(env);

        dispatch_async(dispatcher.queue, ^{
            NSAutoreleasePool *pool = [[NSAutoreleasePool alloc] init];
            ErlNifPid pid = responsePid; // Copy to local stack for enif_send

            ERL_NIF_TERM resultList;
            NSString *errorReason = @"Null map result";
            BOOL success = NO;
            @try {
                NSArray* mapResults = [dispatcher mapDocument:jsonDoc];
                if (mapResults) {
                    resultList = objc_to_term(responseEnv, mapResults);
                    success = YES;
                }
            }
            @catch (NSException *ex) {
                errorReason = [ex description];
            }

            ERL_NIF_TERM responseTerm;
            if (success)
                responseTerm = util_mk_ok(responseEnv, resultList);
            else
                responseTerm = util_mk_error_str(responseEnv, [errorReason UTF8String]);

            if (!enif_send(NULL, &pid, responseEnv,
                           enif_make_tuple2(responseEnv, requestRef, responseTerm)))
                NSLog(@"Couldn't send map response to Erlang");

            enif_free_env(responseEnv);
            [pool drain];
        });
        return util_mk_atom(env, "ok");
    });
}


// reduce_nif(QueueRef, ReplyRef, RedIndexes, KeysJson, ValsJson, Rereduce) ->
static ERL_NIF_TERM reduce(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    if (argc != 6) return enif_make_badarg(env);
    ERL_NIF_TERM uniqueRef = argv[1], indexList = argv[2],
                 keysTerm = argv[3], valuesTerm = argv[4], rereduceTerm = argv[5];
    if (!enif_is_ref(env, uniqueRef) ||!enif_is_list(env, indexList)
            || !enif_is_list(env, keysTerm) || !enif_is_list(env, valuesTerm))
        return enif_make_badarg(env);

    CouchbaseViewDispatcher *dispatcher = unwrap(env, argv[0]);
    if (!dispatcher)
        return enif_make_badarg(env);

    BOOL isRereduce;
    if (!getBool(env, rereduceTerm, &isRereduce))
        return enif_make_badarg(env);

    ErlNifEnv *responseEnv = enif_alloc_env();
    if (!responseEnv)
        return util_mk_error_str(env, "Couldn't create NIF environment for response");
    ErlNifPid responsePid;
    enif_self(env, &responsePid);
    ERL_NIF_TERM requestRef = enif_make_copy(responseEnv, uniqueRef);

    return safely(env, ^ERL_NIF_TERM {
        NSArray *jsonKeys = term_to_objc(env, keysTerm);
        NSArray *jsonValues = term_to_objc(env, valuesTerm);
        if (![jsonKeys isKindOfClass: [NSArray class]]
                || ![jsonValues isKindOfClass: [NSArray class]]
                || jsonKeys.count != jsonValues.count)
			return enif_make_badarg(env);

        NSMutableIndexSet *reduceIndexes = [NSMutableIndexSet indexSet];
        ERL_NIF_TERM car, cdr = indexList;
        while (enif_get_list_cell(env, cdr, &car, &cdr)) {
            unsigned index;
            if (!enif_get_uint(env, car, &index))
                return enif_make_badarg(env);
            [reduceIndexes addIndex:index];
        }

        dispatch_async(dispatcher.queue, ^{
            NSAutoreleasePool *pool = [[NSAutoreleasePool alloc] init];
            ErlNifPid pid = responsePid; // Copy to local stack for enif_send

            ERL_NIF_TERM resultList;
            NSString *errorReason = @"Null reduce result";
            BOOL success = NO;
            @try {
                NSArray *reduceResults = [dispatcher reduceKeys:jsonKeys
                                                         values:jsonValues
                                                          again:isRereduce
                                         withFunctionsAtIndexes:reduceIndexes];
                if (reduceResults) {
                    resultList = objc_to_term(responseEnv, reduceResults);
                    success = YES;
                }
            }
            @catch (NSException *ex) {
                errorReason = [ex description];
            }

            ERL_NIF_TERM responseTerm;
            if (success)
                responseTerm = util_mk_ok(responseEnv, resultList);
            else
                responseTerm = util_mk_error_str(responseEnv, [errorReason UTF8String]);

            if (!enif_send(NULL, &pid, responseEnv,
                           enif_make_tuple2(responseEnv, requestRef, responseTerm)))
                NSLog(@"Couldn't send reduce response to Erlang");

            enif_free_env(responseEnv);
            [pool drain];
        });
        return util_mk_atom(env, "ok");
    });
}


#pragma mark - App Server

// app_create_nif(DDoc, DDocKey) ->
static ERL_NIF_TERM app_create(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    return safely(env, ^ERL_NIF_TERM {
        if (argc != 2) return enif_make_badarg(env);
        NSDictionary* designDoc = term_to_nsdict(env, argv[0]);
        if (!designDoc)
            return enif_make_badarg(env);
        CouchbaseAppServer *app = [[CouchbaseAppServer alloc] initWithDesignDoc: designDoc];
        if (!app)
            return enif_make_badarg(env);
        return util_mk_ok(env, wrap(env, app));
    });
}


// app_validate_update_nif(Ctx, DDocId, EditDoc, DiskDoc, Context, SecObj) ->
static ERL_NIF_TERM app_validate_update(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    // http://wiki.apache.org/couchdb/Document_Update_Validation
    return safely(env, ^ERL_NIF_TERM {
        if (argc != 6) return enif_make_badarg(env);
        CouchbaseAppServer *app = unwrap(env, argv[0]);
        if (!app) return enif_make_badarg(env);
        app.env = env;

        NSDictionary* error = nil;
        BOOL valid = [app validateUpdate: argv[2] ofDocument: argv[3]
                                 context: argv[4] security: argv[5]
                                   error: &error];
        if (valid)
            return enif_make_int(env, 1);
        if (!error)
            error = [NSDictionary dictionaryWithObject: @"invalid document" forKey: @"forbidden"];
        return objc_to_term(env, error);
    });
}


#pragma mark - NIF Registration

static ErlNifFunc nif_funcs[] = {
    // view server:
    {"create_nif", 3, create},
    {"query_view_version_nif", 1, query_view_version},
    {"map_nif", 3, map},
    {"reduce_nif", 6, reduce},
    // app server:
    {"app_create_nif", 2, app_create},
    {"app_validate_update_nif", 6, app_validate_update},
};

ERL_NIF_INIT(objc_dispatch, nif_funcs, load, NULL, NULL, unload);
