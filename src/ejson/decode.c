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

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "erl_nif.h"
#include "erl_nif_compat.h"
#include "yajl/yajl_parse.h"
#include "yajl/yajl_parser.h"
#include "yajl/yajl_lex.h"

typedef struct {
    ERL_NIF_TERM head;
    ErlNifEnv* env;
} decode_ctx;

typedef struct {
    unsigned int depth;
    ErlNifBinary private;
    ErlNifBinary bin;
    size_t fill_offset;
    int error;
} validate_ctx;

#define ENV(ctx) ((decode_ctx*)ctx)->env

#define CONTINUE 1
#define CANCEL 0

#define NOERROR 0
#define ERR_MEMORY 1
#define ERR_PRIVATE_MEMBER 2

static ERL_NIF_TERM
make_error(yajl_handle handle, ErlNifEnv* env)
{
    char* yajlError = (char*) yajl_get_error(handle, 0, NULL, 0);
    ERL_NIF_TERM errMsg;

    if(yajlError != NULL)
    {
        errMsg = enif_make_string(env, yajlError, ERL_NIF_LATIN1);
        yajl_free_error(handle, (unsigned char*) yajlError);
    }
    else
    {
        errMsg = enif_make_string(env, "unknown parse error", ERL_NIF_LATIN1);
    }

    return enif_make_tuple(env, 2,
        enif_make_atom(env, "error"),
        enif_make_tuple(env, 2,
            enif_make_uint(env, handle->bytesConsumed),
            errMsg
        )
    );
}


static void
add_to_head(void* vctx, ERL_NIF_TERM newhead)
{
    decode_ctx* ctx = (decode_ctx*)vctx;
    ctx->head = enif_make_list_cell(ctx->env, newhead, ctx->head);
}

static int
decode_null(void* ctx)
{
    add_to_head(ctx, enif_make_atom(ENV(ctx), "null"));
    return CONTINUE;
}

static int
decode_boolean(void* ctx, int val)
{
    add_to_head(ctx, enif_make_atom(ENV(ctx), val ? "true" : "false"));
    return CONTINUE;
}

static int
decode_number(void * ctx, const char * numberVal, unsigned int numberLen)
{
    // scan in the input to see if it's a float or int

    int numberType = 0; // 0 means integer, 1 means float
    unsigned int i;
    ErlNifBinary bin;
    int missingDot = 1;
    unsigned int expPos;

    for(i=0; i<numberLen; i++) {
        switch (numberVal[i]) {
        case '.':
            missingDot = 0;
            numberType = 1; // it's  a float
            goto loopend;
        case 'E':
        case 'e':
            expPos = i;
            numberType = 1; // it's  a float
            goto loopend;
        }
    }
loopend:
    if ((numberType == 1) && missingDot)
    {
        if(!enif_alloc_binary_compat(ENV(ctx), numberLen + 2, &bin))
        {
            return CANCEL;
        }
        memcpy(bin.data, numberVal, expPos);
        bin.data[expPos] = '.';
        bin.data[expPos + 1] = '0';
        memcpy(bin.data + expPos + 2, numberVal + expPos, numberLen - expPos);
    }
    else
    {
        if(!enif_alloc_binary_compat(ENV(ctx), numberLen, &bin))
        {
            return CANCEL;
        }
        memcpy(bin.data, numberVal, numberLen);
    }
    add_to_head(ctx, enif_make_tuple(ENV(ctx), 2,
                        enif_make_int(ENV(ctx), numberType),
                        enif_make_binary(ENV(ctx), &bin)));
    return CONTINUE;
}



static int
decode_string(void* ctx, const unsigned char* data, unsigned int size)
{
    ErlNifBinary bin;
    if(!enif_alloc_binary_compat(ENV(ctx), size, &bin))
    {
        return CANCEL;
    }
    memcpy(bin.data, data, size);
    add_to_head(ctx, enif_make_binary(ENV(ctx), &bin));
    return CONTINUE;
}

static int
decode_start_array(void* ctx)
{
    add_to_head(ctx, enif_make_int(ENV(ctx), 0));
    return CONTINUE;
}


static int
decode_end_array(void* ctx)
{
    add_to_head(ctx, enif_make_int(ENV(ctx), 1));
    return CONTINUE;
}


static int
decode_start_map(void* ctx)
{
    add_to_head(ctx, enif_make_int(ENV(ctx), 2));
    return CONTINUE;
}


static int
decode_end_map(void* ctx)
{
    add_to_head(ctx, enif_make_int(ENV(ctx), 3));
    return CONTINUE;
}


static int
decode_map_key(void* ctx, const unsigned char* data, unsigned int size)
{
    ErlNifBinary bin;
    if(!enif_alloc_binary_compat(ENV(ctx), size, &bin))
    {
       return CANCEL;
    }
    memcpy(bin.data, data, size);
    add_to_head(ctx, enif_make_tuple(ENV(ctx), 2,
                        enif_make_int(ENV(ctx), 3),
                        enif_make_binary(ENV(ctx), &bin)));
    return CONTINUE;
}

static yajl_callbacks
decoder_callbacks = {
    decode_null,
    decode_boolean,
    NULL,
    NULL,
    decode_number,
    decode_string,
    decode_start_map,
    decode_map_key,
    decode_end_map,
    decode_start_array,
    decode_end_array
};


/****** validate/normalization functions ******/

static int
ensure_buffer(validate_ctx* vctx, unsigned int len) {
    validate_ctx* ctx = (validate_ctx*)vctx;
    if ((ctx->bin.size - ctx->fill_offset) < len)
    {
        if(!enif_realloc_binary_compat(ctx->env, &(ctx->bin),
                (ctx->bin.size * 2) + len))
        {
            return ERR_MEMORY;
        }
    }
    return NOERROR;
}

/* on success, always ensures one extra char available in out bin */
static int
fill_buffer(validate_ctx* ctx, const char* str, unsigned int len)
{
    if (ctx->error || (ctx->error = ensure_buffer(ctx, len + 1)))
        return CANCEL;
    memcpy(ctx->bin.data + ctx->fill_offset, str, len);
    ctx->fill_offset += len;
    return CONTINUE;
}

static int
validate_start_array(void* vctx)
{
    validate_ctx* ctx = (validate_ctx*)vctx;
    ctx->depth++;
    if (ctx->error || (ctx->error = ensure_buffer(ctx, 1)))
        return CANCEL;
    ctx->bin.data[ctx->fill_offset++] = '[';
    return CONTINUE;
}

static int
validate_end_array(void* vctx)
{
    validate_ctx* ctx = (validate_ctx*)vctx;
    ctx->depth--;
    if (ctx->bin.data[ctx->fill_offset - 1] == ',')
        ctx->fill_offset--;
    if (ctx->error || (ctx->error = ensure_buffer(ctx, 2)))
        return CANCEL;
    ctx->bin.data[ctx->fill_offset++] = ']';
    ctx->bin.data[ctx->fill_offset++] = ',';
    return CONTINUE;
}

static int
validate_start_map(void* vctx)
{
    validate_ctx* ctx = (validate_ctx*)vctx;
    ctx->depth++;
    if (ctx->error || (ctx->error = ensure_buffer(ctx, 1)))
        return CANCEL;
    ctx->bin.data[ctx->fill_offset++] = '{';
    return CONTINUE;
}

static int
validate_end_map(void* vctx)
{
    validate_ctx* ctx = (validate_ctx*)vctx;
    ctx->depth--;
    if (ctx->bin.data[ctx->fill_offset - 1] == ',')
        ctx->fill_offset--;
    if (ctx->error || (ctx->error = ensure_buffer(ctx, 2)))
        return CANCEL;
    ctx->bin.data[ctx->fill_offset++] = '}';
    ctx->bin.data[ctx->fill_offset++] = ',';
    return CONTINUE;
}

static int
validate_map_key(void* vctx, const unsigned char* data, unsigned int size)
{
    validate_ctx* ctx = (validate_ctx*)vctx;
    if(size != 0 && ctx->depth == 1)
    {
        int i = ctx->private.size;
        while(i--)
        {
            if(data[0] == ctx->private.data[i])
            {
                ctx->error = ERR_PRIVATE_MEMBER;
                return CANCEL;
            }
        }
    }
    if (ctx->error || (ctx->error = ensure_buffer(ctx, 1)))
        return CANCEL;
    ctx->bin.data[ctx->fill_offset++] = '"';
    yajl_string_encode2(fill_buffer, vctx, data, size);
    if (ctx->error || (ctx->error = ensure_buffer(ctx, 2)))
        return CANCEL;
    ctx->bin.data[ctx->fill_offset++] = '"';
    ctx->bin.data[ctx->fill_offset++] = ':';
    return CONTINUE;
}

static int
validate_number(void* vctx, const char* numstr, unsigned int size)
{
    validate_ctx* ctx = (validate_ctx*)vctx;
    if (CANCEL == fill_buffer(vctx, numstr, size))
        return CANCEL;
    ctx->bin.data[ctx->fill_offset++] = ',';
    return CONTINUE;
}

static int
validate_bool(void* vctx, int boolValue)
{
    int ret;
    validate_ctx* ctx = (validate_ctx*)vctx;
    if (boolValue)
    {
        ret = fill_buffer(vctx, "true", 4);
    }
    else
    {
        ret = fill_buffer(vctx, "false", 5);
    }
    if (CANCEL == ret)
        return CANCEL;
    ctx->bin.data[ctx->fill_offset++] = ',';
    return CONTINUE;
}

static int
validate_null(void* vctx)
{
    validate_ctx* ctx = (validate_ctx*)vctx;
    if (CANCEL == fill_buffer(vctx, "null", 4))
    {
        return CANCEL;
    }
    ctx->bin.data[ctx->fill_offset++] = ',';
    return CONTINUE;
}

static int
validate_string(void* vctx, const unsigned char* str, unsigned int size)
{
    validate_ctx* ctx = (validate_ctx*)vctx;
    if (ctx->error || (ctx->error = ensure_buffer(ctx, 1)))
        return;
    ctx->bin.data[ctx->fill_offset++] = '"';
    yajl_string_encode2(fill_buffer, vctx, str, size);
    if (ctx->error || (ctx->error = ensure_buffer(ctx, 2)))
        return;
    ctx->bin.data[ctx->fill_offset++] = '"';
    ctx->bin.data[ctx->fill_offset++] = ',';
    return CONTINUE;
}

static yajl_callbacks
validate_callbacks = {
    validate_null,
    validate_bool,
    NULL,
    NULL,
    validate_number,
    validate_string,
    validate_start_map,
    validate_map_key,
    validate_end_map,
    validate_start_array,
    validate_end_array
};

static int
check_rest(unsigned char* data, unsigned int size, unsigned int used)
{
    unsigned int i = 0;
    for(i = used; i < size; i++)
    {
        switch(data[i])
        {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                continue;
            default:
                return CANCEL;
        }
    }

    return CONTINUE;
}

ERL_NIF_TERM
validate_doc(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    yajl_parser_config conf = {0, 1};
    yajl_handle handle;

    yajl_status status;
    ErlNifBinary json;
    ERL_NIF_TERM ret;

    validate_ctx ctx;
    ctx.depth = 0;
    ctx.fill_offset = 0;
    ctx.error = 0;

    handle = yajl_alloc(&validate_callbacks, &conf, NULL, &ctx);

    if(!enif_inspect_iolist_as_binary(env, argv[0], &json))
    {
        ret = enif_make_badarg(env);
        goto done;
    }

    if(!enif_inspect_iolist_as_binary(env, argv[1], &ctx.private))
    {
        ret = enif_make_badarg(env);
        goto done;
    }
    /* normalized json will usually be no bigger than input */
    if (!enif_alloc_binary_compat(env, json.size, &ctx.bin))
    {
        ret = enif_make_tuple(env, 2,
                    enif_make_atom(env, "error"),
                    enif_make_atom(env, "insufficient_memory"));
        goto done;
    }

    status = yajl_parse(handle, json.data, json.size);

    if(status == yajl_status_insufficient_data &&
        handle->bytesConsumed == json.size)
    {
        status = yajl_parse_complete(handle);
    }

    if(status == yajl_status_ok)
    {
        if(handle->bytesConsumed != json.size &&
            check_rest(json.data, json.size, handle->bytesConsumed) == CANCEL)
        {
            enif_release_binary_compat(env, &ctx.bin);
            ret = enif_make_tuple(env, 2,
                enif_make_atom(env, "error"),
                enif_make_atom(env, "garbage_after_value")
            );
        }
        else
        {
            assert(ctx.error == NOERROR);
            if (ctx.fill_offset && ctx.bin.data[ctx.fill_offset - 1] == ',')
            {
                ctx.fill_offset--;
            }
            enif_realloc_binary_compat(env, &(ctx.bin), ctx.fill_offset);
            ret = enif_make_tuple(env, 2,
                    enif_make_atom(env, "ok"),
                    // make the binary term which transfers ownership
                    enif_make_binary(env, &ctx.bin)
                    );
        }
    }
    else
    {
        enif_release_binary_compat(env, &ctx.bin);
        if(status == yajl_status_client_canceled)
        {
            if (ctx.error == ERR_MEMORY)
            {
                ret = enif_make_tuple(env, 2,
                        enif_make_atom(env, "error"),
                        enif_make_atom(env, "insufficient_memory"));
            }
            else if (ctx.error == ERR_PRIVATE_MEMBER)
            {
                ret = enif_make_tuple(env, 2,
                        enif_make_atom(env, "error"),
                        enif_make_atom(env, "private_field_set")
                        );
            }
            else
            {
                //wtf!
                abort();
            }
        }
        else
        {
            ret = enif_make_tuple(env, 2,
                    enif_make_atom(env, "error"),
                    enif_make_atom(env, "invalid_json")
                    );
        }
    }

done:
    if(handle != NULL) yajl_free(handle);
    return ret;
}

ERL_NIF_TERM
reverse_tokens(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    decode_ctx ctx;
    yajl_parser_config conf = {0, 1}; // No comments, check utf8
    yajl_handle handle = yajl_alloc(&decoder_callbacks, &conf, NULL, &ctx);
    yajl_status status;
    unsigned int used;
    ErlNifBinary bin;
    ERL_NIF_TERM ret;

    ctx.env = env;
    ctx.head = enif_make_list_from_array(env, NULL, 0);

    if(!enif_inspect_iolist_as_binary(env, argv[0], &bin))
    {
        ret = enif_make_badarg(env);
        goto done;
    }

    status = yajl_parse(handle, bin.data, bin.size);
    used = handle->bytesConsumed;

    // Parsing something like "2.0" (without quotes) will
    // cause a spurious semi-error. We add the extra size
    // check so that "2008-20-10" doesn't pass.
    if(status == yajl_status_insufficient_data && used == bin.size)
    {
        status = yajl_parse_complete(handle);
    }

    if(status == yajl_status_ok && used != bin.size)
    {
        if(check_rest(bin.data, bin.size, used) == CANCEL)
        {
            ret = enif_make_tuple(env, 2,
                enif_make_atom(env, "error"),
                enif_make_atom(env, "garbage_after_value")
            );
            goto done;
        }
    }

    switch(status)
    {
        case yajl_status_ok:
            ret = enif_make_tuple(env, 2, enif_make_atom(env, "ok"), ctx.head);
            goto done;

        case yajl_status_error:
            ret = make_error(handle, env);
            goto done;

        case yajl_status_insufficient_data:
            ret = enif_make_tuple(env, 2,
                enif_make_atom(env, "error"),
                enif_make_atom(env, "insufficient_data")
            );
            goto done;

        case yajl_status_client_canceled:
        /* the only time we do this is when we can't allocate a binary. */
            ret = enif_make_tuple(env, 2,
                enif_make_atom(env, "error"),
                enif_make_atom(env, "insufficient_memory")
            );
            goto done;

        default:
            ret = enif_make_tuple(env, 2,
                enif_make_atom(env, "error"),
                enif_make_atom(env, "unknown")
            );
            goto done;
    }

done:
    if(handle != NULL) yajl_free(handle);
    return ret;
}
