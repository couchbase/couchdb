#include <assert.h>
#include <string.h>

#include "queue.h"
#include "util.h"
#include "vm.h"

typedef enum
{
    job_unknown,
    job_close,
    job_eval,
    job_call,
    job_response
} job_type_e;

struct job_t
{
    job_type_e      type;

    ErlNifEnv*      env;
    ENTERM          ref;
    ErlNifPid       pid;
    
    ErlNifBinary    script;
    ENTERM          name;
    ENTERM          args;

    ENTERM          error;
};

typedef struct job_t* job_ptr;

struct vm_t
{
    ErlNifTid           tid;
    ErlNifThreadOpts*   opts;
    JSRuntime*          runtime;
    queue_ptr           jobs;
    job_ptr             curr_job;
    size_t              stack_size;
    int                 alive;
};

static JSClass global_class = {
    "global",
    JSCLASS_GLOBAL_FLAGS,
    JS_PropertyStub,
    JS_PropertyStub,
    JS_PropertyStub,
    JS_PropertyStub,
    JS_EnumerateStub,
    JS_ResolveStub,
    JS_ConvertStub,
    JS_FinalizeStub,
    JSCLASS_NO_OPTIONAL_MEMBERS
};

void* vm_run(void* arg);
ENTERM vm_eval(JSContext* cx, JSObject* gl, job_ptr job);
ENTERM vm_call(JSContext* cx, JSObject* gl, job_ptr job);
void vm_report_error(JSContext* cx, const char* mesg, JSErrorReport* report);
ENTERM vm_mk_ok(ErlNifEnv* env, ENTERM reason);
ENTERM vm_mk_error(ErlNifEnv* env, ENTERM reason);
ENTERM vm_mk_fatal(ErlNifEnv* env, ENTERM reason);
ENTERM vm_mk_message(ErlNifEnv* env, ENTERM data);

// Job constructor and destructor

job_ptr
job_create(job_type_e type)
{
    job_ptr ret = (job_ptr) enif_alloc(sizeof(struct job_t));
    if(ret == NULL) return NULL;

    ret->type = type;
    ret->env = enif_alloc_env();
    if(ret->env == NULL) goto error;

    ret->ref = 0;
    ret->script.data = NULL;
    ret->script.size = 0;
    ret->error = 0;

    return ret;

error:
    if(ret->env != NULL) enif_free_env(ret->env);
    enif_free(ret);
    return NULL;
}

void
job_destroy(void* obj)
{
    job_ptr job = (job_ptr) obj;
    if(job->script.data != NULL) enif_release_binary(&job->script);
    if(job->env != NULL) enif_free_env(job->env);
    enif_free(job);
}

// For the erlang global object.

static JSClass jserl_class = {
    "JSErlang",
    0,
    JS_PropertyStub,
    JS_PropertyStub,
    JS_PropertyStub,
    JS_PropertyStub,
    JS_EnumerateStub,
    JS_ResolveStub,
    JS_ConvertStub,
    JS_FinalizeStub,
    JSCLASS_NO_OPTIONAL_MEMBERS
};

static JSBool
jserl_send(JSContext* cx, JSObject* obj, uintN argc, jsval* argv, jsval* rval)
{
    vm_ptr vm = (vm_ptr) JS_GetContextPrivate(cx);
    ErlNifEnv* env;
    job_ptr job;
    ENTERM mesg;
    ENTERM ref;
    jsrefcount rc;
    
    if(argc < 0)
    {
        return JS_FALSE;
    }
    
    assert(vm != NULL && "Context has no vm.");
    
    env = enif_alloc_env();
    mesg = vm_mk_message(env, to_erl(env, cx, argv[0]));
    ref = enif_make_copy(env, vm->curr_job->ref);
    mesg = enif_make_tuple2(env, ref, mesg);

    // If pid is not alive, raise an error.
    // XXX: Can I make this uncatchable?
    if(!enif_send(NULL, &(vm->curr_job->pid), env, mesg))
    {
        JS_ReportError(cx, "Context closing.");
        return JS_FALSE;
    }

    rc = JS_SuspendRequest(cx);
    job = queue_receive(vm->jobs);
    JS_ResumeRequest(cx, rc);

    if(job->type == job_close)
    {
        // XXX: Can I make this uncatchable?
        job_destroy(job);
        JS_ReportError(cx, "Context closing.");
        return JS_FALSE;
    }
    
    assert(job->type == job_response && "Invalid message response.");
    
    *rval = to_js(job->env, cx, job->args);
    job_destroy(job);

    return JS_TRUE;
}

static JSBool
jserl_evalcx(JSContext* cx, JSObject* obj, uintN argc, jsval* argv, jsval* rval)
{
    vm_ptr vm;
    JSString* str;
    JSObject* sandbox;
    JSContext* subcx;
    const jschar* src;
    size_t srclen;
    JSBool ret = JS_FALSE;

    vm = JS_GetContextPrivate(cx);
    assert(vm != NULL && "Context is missing vm.");

    sandbox = NULL;
    if(!JS_ConvertArguments(cx, argc, argv, "S / o", &str, &sandbox))
    {
        return JS_FALSE;
    }

    subcx = JS_NewContext(vm->runtime, vm->stack_size);
    if(!subcx)
    {
        JS_ReportOutOfMemory(cx);
        return JS_FALSE;
    }
    
    JS_BeginRequest(subcx);

    src = JS_GetStringChars(str);
    srclen = JS_GetStringLength(str);

    if(!sandbox)
    {
        sandbox = JS_NewObject(subcx, NULL, NULL, NULL);
        if(!sandbox || !JS_InitStandardClasses(subcx, sandbox)) goto done;
    }
    
    if(srclen == 0)
    {
        *rval = OBJECT_TO_JSVAL(sandbox);
    }
    else
    {
        JS_EvaluateUCScript(subcx, sandbox, src, srclen, NULL, 0, rval);
    }
    
    ret = JS_TRUE;

done:
    JS_DestroyContext(subcx);
    return ret;
}

int
install_jserl(JSContext* cx, JSObject* gl)
{
    JSObject* obj;
    
    obj = JS_NewObject(cx, &jserl_class, NULL, NULL);
    if(obj == NULL)
    {
        return 0;
    }
    
    if(!JS_DefineFunction(cx, obj, "send", jserl_send, 1,
            JSPROP_ENUMERATE | JSPROP_READONLY | JSPROP_PERMANENT))
    {
        return 0;
    }
    
    if(!JS_DefineFunction(cx, obj, "evalcx", jserl_evalcx, 1,
            JSPROP_ENUMERATE | JSPROP_READONLY | JSPROP_PERMANENT))
    {
        return 0;
    }

    if(!JS_DefineProperty(cx, gl, "erlang", OBJECT_TO_JSVAL(obj), NULL, NULL,
            JSPROP_ENUMERATE | JSPROP_READONLY | JSPROP_PERMANENT))
    {
        return 0;
    }
    
    return 1;
}

//
//  VM Related API
//

vm_ptr
vm_init(ErlNifResourceType* res_type, JSRuntime* runtime, size_t stack_size)
{
    vm_ptr vm = (vm_ptr) enif_alloc_resource(res_type, sizeof(struct vm_t));
    if(vm == NULL) return NULL;

    vm->runtime = runtime;
    vm->curr_job = NULL;
    vm->stack_size = stack_size;

    vm->jobs = queue_create();
    if(vm->jobs == NULL) goto error;
    
    vm->opts = enif_thread_opts_create("vm_thread_opts");
    if(enif_thread_create("", &vm->tid, vm_run, vm, vm->opts) != 0) goto error;
    
    return vm;

error:
    enif_release_resource(vm);
    return NULL;
}

void
vm_destroy(ErlNifEnv* env, void* obj)
{
    vm_ptr vm = (vm_ptr) obj;
    job_ptr job;
    void* resp;
   
    job = job_create(job_close);
    assert(job != NULL && "Failed to create job.");
    queue_push(vm->jobs, job);
    
    job = job_create(job_close);
    assert(job != NULL && "Failed to create job.");
    queue_send(vm->jobs, job);

    enif_thread_join(vm->tid, &resp);

    while(queue_has_job(vm->jobs))
    {
        job = queue_pop(vm->jobs);
        job_destroy(job);
    }
    
    while(queue_has_msg(vm->jobs))
    {
        job = queue_receive(vm->jobs);
        job_destroy(job);
    }
    
    queue_destroy(vm->jobs);
    enif_thread_opts_destroy(vm->opts);
}

void*
vm_run(void* arg)
{
    vm_ptr vm = (vm_ptr) arg;
    JSContext* cx;
    JSObject* gl;
    job_ptr job;
    ENTERM resp;
    int flags;
    int alive = 1;

    cx = JS_NewContext(vm->runtime, vm->stack_size);
    if(cx == NULL) goto done;

    JS_BeginRequest(cx);

    flags = 0;
    flags |= JSOPTION_VAROBJFIX;
    flags |= JSOPTION_STRICT;
    flags |= JSVERSION_LATEST;
    flags |= JSOPTION_COMPILE_N_GO;
    flags |= JSOPTION_XML;
    JS_SetOptions(cx, JS_GetOptions(cx) | flags);
    
    gl = JS_NewObject(cx, &global_class, NULL, NULL);
    if(gl == NULL) goto done;
    if(!JS_InitStandardClasses(cx, gl)) goto done;
    if(!install_jserl(cx, gl)) goto done;
    
    JS_SetErrorReporter(cx, vm_report_error);
    JS_SetContextPrivate(cx, (void*) vm);

    JS_EndRequest(cx);

    while(alive)
    {
        job = queue_pop(vm->jobs);
        if(job->type == job_close)
        {
            job_destroy(job);
            goto done;
        }

        JS_BeginRequest(cx);
        assert(vm->curr_job == NULL && "vm already has a job set.");
        vm->curr_job = job;

        if(job->type == job_eval)
        {
            resp = vm_eval(cx, gl, job);
        }
        else if(job->type == job_call)
        {
            resp = vm_call(cx, gl, job);
        }
        else
        {
            resp = vm_mk_fatal(job->env, util_mk_atom(job->env, "bad_job"));
            alive = 0;
        }

        vm->curr_job = NULL;
        JS_EndRequest(cx);
        JS_MaybeGC(cx);

        // XXX: If pid is not alive, we just ignore it.
        enif_send(NULL, &(job->pid), job->env, resp);

        job_destroy(job);
    }

done:
    JS_BeginRequest(cx);
    if(cx != NULL) JS_DestroyContext(cx);
    return NULL;
}

int
vm_add_eval(vm_ptr vm, ENTERM ref, ENPID pid, ENBINARY bin)
{
    job_ptr job = job_create(job_eval);
    
    job->ref = enif_make_copy(job->env, ref);
    job->pid = pid;
    
    if(!enif_alloc_binary(bin.size, &(job->script))) goto error;
    memcpy(job->script.data, bin.data, bin.size);

    if(!queue_push(vm->jobs, job)) goto error;

    return 1;

error:
    if(job != NULL) job_destroy(job);
    return 0;
}

int
vm_add_call(vm_ptr vm, ENTERM ref, ENPID pid, ENTERM name, ENTERM args)
{
    job_ptr job = job_create(job_call);
    if(job == NULL) goto error;

    job->ref = enif_make_copy(job->env, ref);
    job->pid = pid;
    job->name = enif_make_copy(job->env, name);
    job->args = enif_make_copy(job->env, args);

    if(!queue_push(vm->jobs, job)) goto error;

    return 1;
error:
    if(job != NULL) job_destroy(job);
    return 0;
}

int
vm_send(vm_ptr vm, ENTERM data)
{
    job_ptr job = job_create(job_response);
    if(job == NULL) goto error;
    
    job->args = enif_make_copy(job->env, data);
    
    if(!queue_send(vm->jobs, job)) goto error;
    
    return 1;
error:
    if(job != NULL) job_destroy(job);
    return 0;
}

ENTERM
vm_eval(JSContext* cx, JSObject* gl, job_ptr job)
{
    ENTERM resp;
    const char* script;
    size_t length;
    jsval rval;
    int cnt;
    int i;

    script = (const char*) job->script.data;
    length = job->script.size;

    for(i = 0, cnt = 0; i < length; i++)
    {
        if(script[i] == '\n') cnt += 1;
    }

    if(!JS_EvaluateScript(cx, gl, script, length, "", cnt, &rval))
    {
        if(job->error != 0)
        {
            resp = vm_mk_error(job->env, job->error);
        }
        else
        {
            resp = vm_mk_error(job->env, util_mk_atom(job->env, "unknown"));
        }
    }
    else
    {
        resp = vm_mk_ok(job->env, to_erl(job->env, cx, rval));
    }

    return enif_make_tuple2(job->env, job->ref, resp);
}

ENTERM
vm_call(JSContext* cx, JSObject* gl, job_ptr job)
{
    ENTERM resp;
    ENTERM head;
    ENTERM tail;
    jsval func;
    jsval args[256];
    jsval rval;
    jsid idp;
    int argc;
    
    // Get the function object.
    
    func = to_js(job->env, cx, job->name);
    if(func == JSVAL_VOID)
    {
        resp = vm_mk_error(job->env, util_mk_atom(job->env, "invalid_name"));
        goto send;
    }

    if(!JS_ValueToId(cx, func, &idp))
    {
        resp = vm_mk_error(job->env, util_mk_atom(job->env, "internal_error"));
        goto send;
    }
    
    if(!JS_GetPropertyById(cx, gl, idp, &func))
    {
        resp = vm_mk_error(job->env, util_mk_atom(job->env, "bad_property"));
        goto send;
    }

    if(JS_TypeOfValue(cx, func) != JSTYPE_FUNCTION)
    {
        resp = vm_mk_error(job->env, util_mk_atom(job->env, "not_a_function"));
        goto send;
    }

    // Creating function arguments.
    
    if(enif_is_empty_list(job->env, job->args))
    {
        argc = 0;
    }
    else
    {
        if(!enif_get_list_cell(job->env, job->args, &head, &tail))
        {
            resp = vm_mk_error(job->env, util_mk_atom(job->env, "invalid_argv"));
            goto send;
        }

        argc = 0;
        do {
            args[argc++] = to_js(job->env, cx, head);
        } while(enif_get_list_cell(job->env, tail, &head, &tail) && argc < 256);
    }

    // Call function
    if(!JS_CallFunctionValue(cx, gl, func, argc, args, &rval))
    {
        if(job->error != 0)
        {
            resp = vm_mk_error(job->env, job->error);
        }
        else
        {
            resp = vm_mk_error(job->env, util_mk_atom(job->env, "unknown"));
        }
    }
    else
    {
        resp = vm_mk_ok(job->env, to_erl(job->env, cx, rval));
    }

send:
    return enif_make_tuple2(job->env, job->ref, resp);
}

void
vm_set_error(vm_ptr vm, ENBINARY mesg, ENBINARY src, unsigned int line)
{
    ENTERM tmesg = enif_make_binary(vm->curr_job->env, &mesg);
    ENTERM tsrc = enif_make_binary(vm->curr_job->env, &src);
    ENTERM tline = enif_make_int(vm->curr_job->env, line);

    vm->curr_job->error = enif_make_tuple3(
            vm->curr_job->env, tmesg, tsrc, tline
    );
}

void
vm_report_error(JSContext* cx, const char* mesg, JSErrorReport* report)
{
    vm_ptr vm;
    ErlNifBinary bmesg;
    ErlNifBinary bsrc;

    vm = (vm_ptr) JS_GetContextPrivate(cx);
    if(vm == NULL) return;

    if(!(report->flags & JSREPORT_EXCEPTION)) return;

    if(mesg == NULL) mesg = "";
    if(report->linebuf == NULL) report->linebuf = "";

    if(!enif_alloc_binary(strlen(mesg), &bmesg)) return;
    if(!enif_alloc_binary(strlen(report->linebuf), &bsrc)) return;

    memcpy(bmesg.data, mesg, strlen(mesg));
    memcpy(bsrc.data, report->linebuf, strlen(report->linebuf));

    vm_set_error(vm, bmesg, bsrc, report->lineno);
}

ENTERM
vm_mk_ok(ErlNifEnv* env, ENTERM reason)
{
    ENTERM ok = util_mk_atom(env, "ok");
    return enif_make_tuple2(env, ok, reason);
}

ENTERM
vm_mk_error(ErlNifEnv* env, ENTERM reason)
{
    ENTERM error = util_mk_atom(env, "error");
    return enif_make_tuple2(env, error, reason);
}

ENTERM
vm_mk_fatal(ErlNifEnv* env, ENTERM reason)
{
    ENTERM error = util_mk_atom(env, "fatal");
    return enif_make_tuple2(env, error, reason);
}

ENTERM
vm_mk_message(ErlNifEnv* env, ENTERM data)
{
    ENTERM message = util_mk_atom(env, "message");
    return enif_make_tuple2(env, message, data);
}
