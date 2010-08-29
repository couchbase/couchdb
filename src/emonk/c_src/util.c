
#include <string.h>

#include "util.h"

ENTERM
util_mk_atom(ErlNifEnv* env, const char* atom)
{
    ENTERM ret;
    
    if(enif_make_existing_atom(env, atom, &ret, ERL_NIF_LATIN1)) return ret;

    return enif_make_atom(env, atom);
}

ENTERM
util_mk_ok(ErlNifEnv* env, ENTERM value)
{
    ENTERM ok = util_mk_atom(env, "ok");
    return enif_make_tuple2(env, ok, value);
}

ENTERM
util_mk_error(ErlNifEnv* env, const char* reason)
{
    ENTERM error = util_mk_atom(env, "error");
    return enif_make_tuple2(env, error, util_mk_atom(env, reason));
}

void
util_debug_jsval(JSContext* cx, jsval val)
{
    JSString* str;
    char* bytes;
    
    str = JS_ValueToString(cx, val);
    if(!str)
    {
        fprintf(stderr, "DEBUG: Unable to convert value.\n");
        return;
    }
    
    bytes = JS_EncodeString(cx, str);
    if(!bytes)
    {
        fprintf(stderr, "DEBUG: Unable to encode string.\n");
        return;
    }
    
    fprintf(stderr, "%s\n", bytes);
}
