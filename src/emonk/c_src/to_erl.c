
#include <string.h>

#include "util.h"

#define OK 1
#define ERROR 0

int to_erl_intern(ErlNifEnv* env, JSContext* cx, jsval val, ERL_NIF_TERM* term);

int
to_erl_atom(ErlNifEnv* env, const char* atom, ERL_NIF_TERM* term)
{
    *term = util_mk_atom(env, atom);
    return OK;
}

int
to_erl_string(ErlNifEnv* env, JSContext* cx, jsval val, ERL_NIF_TERM* term)
{
    JSString *str;
    ErlNifBinary bin;
    const char* data;
    size_t len;

    str = JS_ValueToString(cx, val);
    data = JS_GetStringBytesZ(cx, str);
    if(data == NULL) return ERROR;
    len = strlen(data);

    if(!enif_alloc_binary(len, &bin))
    {
        return ERROR;
    }

    memcpy(bin.data, data, len);
    *term = enif_make_binary(env, &bin);
    return OK;
}

int
to_erl_int(ErlNifEnv* env, JSContext* cx, jsval val, ERL_NIF_TERM* term)
{
    int32_t rval;
    if(!JS_ValueToInt32(cx, val, &rval)) return ERROR;
    *term = enif_make_int(env, rval);
    return OK;
}

int
to_erl_float(ErlNifEnv* env, JSContext* cx, jsval val, ERL_NIF_TERM* term)
{
    double rval;
    if(!JS_ValueToNumber(cx, val, &rval)) return ERROR;
    *term = enif_make_double(env, rval);
    return OK;
}

int
to_erl_array(ErlNifEnv* env, JSContext* cx, JSObject* obj, ERL_NIF_TERM* term)
{
    ERL_NIF_TERM* array = NULL;
    int ret = ERROR;
    unsigned int length;
    jsval v;
    int i;
    
    if(!JS_GetArrayLength(cx, obj, &length)) return ERROR;
    
    array = (ERL_NIF_TERM*) enif_alloc(length * sizeof(ERL_NIF_TERM));
    if(array == NULL) goto done;

    for(i = 0; i < length; i++)
    {
        if(!JS_GetElement(cx, obj, i, &v)) goto done;
        if(!to_erl_intern(env, cx, v, array+i)) goto done;
    }

    *term = enif_make_list_from_array(env, array, length);
    ret = OK;

done:
    if(array != NULL) enif_free(array);
    return ret;
}

int
to_erl_object(ErlNifEnv* env, JSContext* cx, JSObject* obj, ERL_NIF_TERM* term)
{
    ERL_NIF_TERM* array = NULL;
    ERL_NIF_TERM list;
    ERL_NIF_TERM keyterm;
    ERL_NIF_TERM valterm;
    JSObject* iter;
    jsid idp;
    jsval val;
    int length;
    int index;
    int ret = ERROR;

    iter = JS_NewPropertyIterator(cx, obj);
    if(iter == NULL) goto done;

    length = 0;
    while(JS_NextProperty(cx, iter, &idp))
    {
        if(idp == JSVAL_VOID) break;
        length += 1;
    }
    
    array = enif_alloc(length * sizeof(ERL_NIF_TERM));
    if(array == NULL) goto done;
    
    iter = JS_NewPropertyIterator(cx, obj);
    if(iter == NULL) goto done;

    index = 0;
    while(JS_NextProperty(cx, iter, &idp))
    {
        if(idp == JSVAL_VOID)
        {
            list = enif_make_list_from_array(env, array, length);
            *term = enif_make_tuple1(env, list);
            ret = OK;
            goto done;
        }

        if(!JS_IdToValue(cx, idp, &val)) goto done;
        if(!to_erl_string(env, cx, val, &keyterm)) goto done;
        if(!JS_GetPropertyById(cx, obj, idp, &val)) goto done;
        if(val == OBJECT_TO_JSVAL(obj)) val = JSVAL_NULL;
        if(!to_erl_intern(env, cx, val, &valterm)) goto done;
        
        array[index] = enif_make_tuple2(env, keyterm, valterm);
        index += 1;
    }

done:
    if(array != NULL) enif_free(array);
    return ret;
}

int
to_erl_convert(ErlNifEnv* env, JSContext* cx, JSObject* obj, ERL_NIF_TERM* term)
{
    JSObject* func;
    jsval tojson;
    jsval rval;
    
    if(!JS_GetProperty(cx, obj, "toJSON", &tojson))
    {
        return ERROR;
    }
    
    if(!JSVAL_IS_OBJECT(tojson)) return ERROR;
    func = JSVAL_TO_OBJECT(tojson);
    if(func == NULL) return ERROR;
    if(!JS_ObjectIsFunction(cx, func)) return ERROR;

    if(!JS_CallFunctionValue(cx, obj, tojson, 0, NULL, &rval))
    {
        return ERROR;
    }
    
    return to_erl_intern(env, cx, rval, term);
}

int
to_erl_intern(ErlNifEnv* env, JSContext* cx, jsval val, ERL_NIF_TERM* term)
{
    JSObject* obj = NULL;
    JSType type = JS_TypeOfValue(cx, val);
        
    if(val == JSVAL_NULL)
    {
        return to_erl_atom(env, "null", term);
    }
    else if(val == JSVAL_VOID)
    {
        return ERROR;
    }
    else if(type == JSTYPE_BOOLEAN)
    {
        if(val == JSVAL_TRUE)
            return to_erl_atom(env, "true", term);
        else
            return to_erl_atom(env, "false", term);
    }
    else if(type == JSTYPE_STRING)
    {
        return to_erl_string(env, cx, val, term);
    }
    else if(type == JSTYPE_XML)
    {
        return to_erl_string(env, cx, val, term);
    }
    else if(type == JSTYPE_NUMBER)
    {
        if(JSVAL_IS_INT(val))
            return to_erl_int(env, cx, val, term);
        else
            return to_erl_float(env, cx, val, term);
    }
    else if(type == JSTYPE_OBJECT)
    {
        obj = JSVAL_TO_OBJECT(val);

        if(OK == to_erl_convert(env, cx, obj, term))
        {
            return OK;
        }
        
        if(JS_IsArrayObject(cx, obj))
        {
            return to_erl_array(env, cx, obj, term);
        }

        return to_erl_object(env, cx, obj, term);
    }

    return ERROR;
}

ERL_NIF_TERM
to_erl(ErlNifEnv* env, JSContext* cx, jsval val)
{
    ERL_NIF_TERM ret = util_mk_atom(env, "undefined");
    
    if(!to_erl_intern(env, cx, val, &ret))
    {
        return util_mk_atom(env, "undefined");
    }

    return ret;
}
