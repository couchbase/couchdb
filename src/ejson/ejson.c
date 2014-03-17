#include "erl_nif.h"
#include "ejson.h"

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM info)
{
    return 0;
}

static int on_reload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM info)
{
    return 0;
}

static int on_upgrade(ErlNifEnv* env, void** priv_data, void** old_data, ERL_NIF_TERM info)
{
    return 0;
}

static ErlNifFunc nif_funcs[] =
{
    {"final_encode", 1, final_encode},
    {"reverse_tokens", 1, reverse_tokens},
    {"validate", 1, validate_doc}
};

#if defined (__SUNPRO_C) && (__SUNPRO_C >= 0x550)
__global
#elif defined __GNUC__
__attribute__ ((visibility("default")))
#endif
ERL_NIF_INIT(ejson, nif_funcs, &on_load, &on_reload, &on_upgrade, NULL)
