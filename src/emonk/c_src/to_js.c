
#include <string.h>

#include "util.h"

jsval
to_js_special(ErlNifEnv* env, JSContext* cx, ERL_NIF_TERM term)
{
    JSString* str = NULL;
    char atom[512]; // Pretty sure there's a 256 byte limit

    if(!enif_get_atom(env, term, atom, 512, ERL_NIF_LATIN1))
    {
        return JSVAL_VOID;
    }
    else if(strcmp(atom, "true") == 0)
    {
        return JSVAL_TRUE;
    }
    else if(strcmp(atom, "false") == 0)
    {
        return JSVAL_FALSE;
    }
    else if(strcmp(atom, "null") == 0)
    {
        return JSVAL_NULL;
    }
    else
    {
        str = JS_NewStringCopyZ(cx, atom);
        if(str == NULL) return JSVAL_VOID;
        return STRING_TO_JSVAL(str);
    }
}

jsval
to_js_number(JSContext* cx, double value)
{
    jsval ret;

    if(!JS_NewNumberValue(cx, value, &ret))
    {
        return JSVAL_VOID;
    }

    return ret;
}

jsval
to_js_string(ErlNifEnv* env, JSContext* cx, ERL_NIF_TERM term)
{
    ErlNifBinary bin;
    JSString* str;
    jschar* chars;
    size_t charslen;
    
    if(!enif_inspect_binary(env, term, &bin))
    {
        return JSVAL_VOID;
    }
    
    if(!JS_DecodeBytes(cx, (char*) bin.data, bin.size, NULL, &charslen))
    {
        return JSVAL_VOID;
    }
        
    chars = JS_malloc(cx, (charslen + 1) * sizeof(jschar));
    if(chars == NULL) return JSVAL_VOID;
    
    if(!JS_DecodeBytes(cx, (char*) bin.data, bin.size, chars, &charslen))
    {
        JS_free(cx, chars);
        return JSVAL_VOID;
    }
    chars[charslen] = '\0';
    
    str = JS_NewUCString(cx, chars, charslen);
    if(!str)
    {
        JS_free(cx, chars);
        return JSVAL_VOID;
    }

    return STRING_TO_JSVAL(str);
}

jsval
to_js_empty_array(JSContext* cx)
{
    JSObject* ret = JS_NewArrayObject(cx, 0, NULL);
    if(ret == NULL) return JSVAL_VOID;
    return OBJECT_TO_JSVAL(ret);
}

jsval
to_js_array(ErlNifEnv* env, JSContext* cx, ERL_NIF_TERM head, ERL_NIF_TERM tail)
{
    JSObject* ret;
    jsval val;
    jsint i;

    ret = JS_NewArrayObject(cx, 0, NULL);
    if(ret == NULL) return JSVAL_VOID;

    i = 0;
    do {
        val = to_js(env, cx, head);
        if(val == JSVAL_VOID) return JSVAL_VOID;
        if(!JS_SetElement(cx, ret, i, &val)) return JSVAL_VOID;
        i += 1;
    } while(enif_get_list_cell(env, tail, &head, &tail));
    
    return OBJECT_TO_JSVAL(ret);
}

jsval
to_js_key(ErlNifEnv* env, JSContext* cx, ERL_NIF_TERM term)
{
    if(enif_is_atom(env, term))
    {
        return to_js_special(env, cx, term);
    }
    else if(enif_is_binary(env, term))
    {
        return to_js_string(env, cx, term);
    }
    else
    {
        return JSVAL_VOID;
    }
}

jsval
to_js_object(ErlNifEnv* env, JSContext* cx, ERL_NIF_TERM list)
{
    JSObject* ret;
    jsval kval;
    jsval vval;
    jsid idp;
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail;
    const ERL_NIF_TERM* pair;
    int arity;

    ret = JS_NewObject(cx, NULL, NULL, NULL);
    if(ret == NULL) return JSVAL_VOID;
    
    if(enif_is_empty_list(env, list))
    {
        return OBJECT_TO_JSVAL(ret);
    }

    if(!enif_get_list_cell(env, list, &head, &tail))
    {
        return JSVAL_VOID;
    }

    do {
        if(!enif_get_tuple(env, head, &arity, &pair))
        {
            return JSVAL_VOID;
        }
        
        if(arity != 2)
        {
            return JSVAL_VOID;
        }
        
        kval = to_js_key(env, cx, pair[0]);
        if(kval == JSVAL_VOID) return JSVAL_VOID;
        if(!JS_ValueToId(cx, kval, &idp)) return JSVAL_VOID;
        vval = to_js(env, cx, pair[1]);
        if(vval == JSVAL_VOID) return JSVAL_VOID;
        
        if(!JS_SetPropertyById(cx, ret, idp, &vval))
        {
            return JSVAL_VOID;
        }
    } while(enif_get_list_cell(env, tail, &head, &tail));
    
    return OBJECT_TO_JSVAL(ret);
}

jsval
to_js(ErlNifEnv* env, JSContext* cx, ERL_NIF_TERM term)
{
    int intval;
    unsigned int uintval;
    long longval;
    unsigned long ulongval;
    double doubleval;
    ERL_NIF_TERM head;
    ERL_NIF_TERM tail;
    const ERL_NIF_TERM* tuple;
    int arity;
    
    if(enif_is_atom(env, term))
    {
        return to_js_special(env, cx, term);
    }
    
    if(enif_is_binary(env, term))
    {
        return to_js_string(env, cx, term);
    }
    
    if(enif_is_empty_list(env, term))
    {
        return to_js_empty_array(cx);
    }
    
    if(enif_get_int(env, term, &intval))
    {
        return to_js_number(cx, (double) intval);
    }
    
    if(enif_get_uint(env, term, &uintval))
    {
        return to_js_number(cx, (double) uintval);
    }
    
    if(enif_get_long(env, term, &longval))
    {
        return to_js_number(cx, (double) longval);
    }
    
    if(enif_get_ulong(env, term, &ulongval))
    {
        return to_js_number(cx, (double) ulongval);
    }
    
    if(enif_get_double(env, term, &doubleval))
    {
        return to_js_number(cx, doubleval);
    }
    
    if(enif_get_list_cell(env, term, &head, &tail))
    {
        return to_js_array(env, cx, head, tail);
    }
    
    if(enif_get_tuple(env, term, &arity, &tuple))
    {
        if(arity == 1)
        {
            return to_js_object(env, cx, tuple[0]);
        }
    }

    return JSVAL_VOID;
}
