#include "config_static.h"

#include "couch_btree.h"

nif_writerq *nif_writerq_alloc(ssize_t size)
{
    nif_writerq *wrq = enif_alloc_resource(writerq_type, sizeof(nif_writerq) + size);
    wrq->size = size;
    return wrq;
}

void nif_write(couchfile_modify_request *rq, couchfile_pointer_info *dst, nif_writerq* wrq, ssize_t size)
{
    ErlNifEnv* msg_env = NULL;
    ERL_NIF_TERM msg_term;

    dst->writerq_resource = wrq;
    dst->pointer = 0;
    wrq->ptr = dst;

    msg_env = enif_alloc_env();
    msg_term = enif_make_tuple4(msg_env,
            get_atom(msg_env, "append_bin_btnif"),
            get_atom(msg_env, "snappy"), //COMPRESSION TYPE
            enif_make_resource(msg_env, wrq),
            enif_make_resource_binary(msg_env, wrq, &wrq->buf, size));
    enif_send(rq->caller_env, &rq->writer, msg_env, msg_term);
    enif_free_env(msg_env);
    enif_release_resource(wrq);
}

