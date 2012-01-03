#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <sys/param.h>
#include <time.h>

#include "couch_btree.h"
#include "erl_nif.h"
#include "ei.h"

ErlNifResourceType* writerq_type;
void queue_request(btreenif_state* state, couchfile_modify_request* rq);

ERL_NIF_TERM get_atom(ErlNifEnv* env, const char *name)
{
    ERL_NIF_TERM atom;
    if(!enif_make_existing_atom(env, name, &atom, ERL_NIF_LATIN1))
    {
        return enif_make_atom(env, name);
    }
    return atom;
}

void* ebin_from_ext(compare_info* c,char* buf, int pos) {
    int binsize;
    int type;
    eterm_buf* ebcmp = c->arg;
    ei_get_type(buf, &pos, &type, &binsize);
    ebcmp->buf = buf + pos + 5;
    ebcmp->size = binsize;
    return ebcmp;
}

void* term_from_ext(compare_info* c, char* buf, int pos) {
    int endpos = pos;
    eterm_buf* ebcmp = c->arg;
    ei_skip_term(buf, &endpos);
    ebcmp->buf = buf + pos;
    ebcmp->size = endpos - pos;
    return ebcmp;
}

int ebin_cmp(void* k1, void* k2) {
    eterm_buf *e1 = (eterm_buf*)k1;
    eterm_buf *e2 = (eterm_buf*)k2;
    int size;
    if(e2->size < e1->size)
    {
        size = e2->size;
    }
    else
    {
        size = e1->size;
    }

    int cmp = memcmp(e1->buf, e2->buf, size);
    if(cmp == 0)
    {
        if(size < e2->size)
        {
            return -1;
        }
        else if (size < e1->size)
        {
            return 1;
        }
    }
    return cmp;
}

int long_term_cmp(void *k1, void *k2) {
    eterm_buf *e1 = (eterm_buf*)k1;
    eterm_buf *e2 = (eterm_buf*)k2;
    int pos = 0;
    long e1val, e2val;
    ei_decode_long(e1->buf, &pos, &e1val);
    pos = 0;
    ei_decode_long(e2->buf, &pos, &e2val);
    if(e1val == e2val)
    {
        return 0;
    }
    return (e1val < e2val ? -1 : 1);
}


void by_seq_reduce (eterm_buf* dst, nodelist* leaflist, int count) {
    //will be freed by flush_mr
    dst->buf = malloc(12);
    if(!dst->buf)
        return;
    int pos = 0;
    ei_encode_long(dst->buf, &pos, count);
    dst->size = pos;
}

void by_seq_rereduce (eterm_buf* dst, nodelist* leaflist, int count) {
    long total = 0;
    long current = 0;
    int r_pos = 0;
    int pos = 0;

    //will be freed by flush_mr
    dst->buf = malloc(12);
    if(!dst->buf)
        return;
    nodelist* i = leaflist;
    while(i != NULL)
    {
        r_pos = 0;
        ei_decode_long(i->value.pointer->reduce_value.buf, &r_pos, &current);
        total += current;
        i = i->next;
    }
    ei_encode_long(dst->buf, &pos, total);
    dst->size = pos;
}

void by_id_rereduce(eterm_buf *dst, nodelist* leaflist, int count)
{
    //Source term {NotDeleted, Deleted, Size}
    //Result term {NotDeleted, Deleted, Size}
    dst->buf = malloc(30);
    if(!dst->buf)
        return;
    int dstpos = 0;
    int srcpos = 0;
    long notdeleted = 0, deleted = 0, size = 0;
    nodelist* i = leaflist;
    while(i != NULL)
    {
        srcpos = 0;
        long long src_deleted = 0;
        long long src_notdeleted = 0;
        long long src_size = 0;
        ei_decode_tuple_header(i->value.pointer->reduce_value.buf, &srcpos, NULL);
        ei_decode_longlong(i->value.pointer->reduce_value.buf, &srcpos, &src_notdeleted);
        ei_decode_longlong(i->value.pointer->reduce_value.buf, &srcpos, &src_deleted);
        ei_decode_longlong(i->value.pointer->reduce_value.buf, &srcpos, &src_size);
        size += src_size;
        deleted += src_deleted;
        notdeleted += src_notdeleted;
        i = i->next;
    }

    ei_encode_tuple_header(dst->buf, &dstpos, 3);
    ei_encode_longlong(dst->buf, &dstpos, notdeleted);
    ei_encode_longlong(dst->buf, &dstpos, deleted);
    ei_encode_longlong(dst->buf, &dstpos, size);
    dst->size = dstpos;
}

void by_id_reduce(eterm_buf *dst, nodelist* leaflist, int count)
{
    //Source term {Key, {Seq, Rev, Bp, Deleted, Size}}
    //Result term {NotDeleted, Deleted, Size}
    dst->buf = malloc(30);
    if(!dst->buf)
        return;
    int dstpos = 0;
    int srcpos = 0;
    long notdeleted = 0, deleted = 0, size = 0;
    nodelist* i = leaflist;
    while(i != NULL)
    {
        srcpos = 0;
        long src_deleted = 0;
        long long src_size = 0;
        ei_decode_tuple_header(i->value.leaf->term.buf, &srcpos, NULL);
        ei_skip_term(i->value.leaf->term.buf, &srcpos); //skip key
        ei_decode_tuple_header(i->value.leaf->term.buf, &srcpos, NULL);
        ei_skip_term(i->value.leaf->term.buf, &srcpos); //skip seq
        ei_skip_term(i->value.leaf->term.buf, &srcpos); //skip rev
        ei_skip_term(i->value.leaf->term.buf, &srcpos); //skip bp
        ei_decode_long(i->value.leaf->term.buf, &srcpos, &src_deleted);
        ei_decode_longlong(i->value.leaf->term.buf, &srcpos, &src_size);
        if(src_deleted == 1)
            deleted++;
        else
            notdeleted++;

        size += src_size;
        i = i->next;
    }

    ei_encode_tuple_header(dst->buf, &dstpos, 3);
    ei_encode_long(dst->buf, &dstpos, notdeleted);
    ei_encode_long(dst->buf, &dstpos, deleted);
    ei_encode_longlong(dst->buf, &dstpos, size);
    dst->size = dstpos;
}

int copy_and_inspect(ErlNifEnv *dst_env, ERL_NIF_TERM binterm, ErlNifBinary *bin)
{
    ERL_NIF_TERM newterm = enif_make_copy(dst_env, binterm);
    return enif_inspect_binary(dst_env, newterm, bin);
}

int get_pointer_info(ErlNifEnv* env, ERL_NIF_TERM term, couchfile_pointer_info *ptr)
{
    //This won't handle the reduce value.. we only use this on the root node
    //anyway.
    const ERL_NIF_TERM *ptrterms;
    int arity;
    enif_get_tuple(env, term, &arity, &ptrterms);
    if(arity != 3)
    {
        return -1;
    }
    ptr->writerq_resource = NULL;
    enif_get_uint64(env, ptrterms[0], (ErlNifUInt64*) &ptr->pointer);
    enif_get_uint64(env, ptrterms[2], (ErlNifUInt64*) &ptr->subtreesize);
    return 0;
}

ERL_NIF_TERM error_term(ErlNifEnv* env, int errcode)
{
    ERL_NIF_TERM error_atom;
    switch(errcode)
    {
        case ERROR_READ_FILE:
            error_atom = get_atom(env, "read_file");
        break;
        case ERROR_PARSE:
            error_atom = get_atom(env, "parse");
        break;
        case ERROR_WRITER_DEAD:
            error_atom = get_atom(env, "writer_dead");
        break;
        case ERROR_ALLOC_FAIL:
            error_atom = get_atom(env, "alloc_fail");
        break;
        default:
            error_atom = get_atom(env, "btreenif_error");
    }
    return enif_make_tuple2(env,
            get_atom(env, "error"),
            error_atom);
}

void fetch_send(couchfile_modify_request *rq, eterm_buf *k, eterm_buf *v)
{
    ERL_NIF_TERM key, value, msg;
    ErlNifBinary kbin, vbin;
    ErlNifEnv *ienv = enif_alloc_env();
    enif_alloc_binary(k->size + 1, &kbin);
    kbin.data[0] = 131;
    memcpy(kbin.data + 1, k->buf, k->size);
    key = enif_make_binary(ienv, &kbin);
    if(v != NULL)
    {
        enif_alloc_binary(v->size + 1, &vbin);
        vbin.data[0] = 131;
        memcpy(vbin.data + 1, v->buf, v->size);
        value = enif_make_binary(ienv, &vbin);
        msg = enif_make_tuple3(ienv,
                get_atom(ienv, "ok"),
                key, value);
    }
    else
    {
        msg = enif_make_tuple2(ienv,
                get_atom(ienv, "not_found"),
                key);
    }

    enif_send(rq->caller_env, &rq->caller, ienv, enif_make_tuple2(ienv,
                enif_make_copy(ienv, rq->ref), msg));
    enif_free_env(ienv);
}

void *query_modify_thread(void *arg)
{
    couchfile_modify_request *rq = arg;
    int errcode = 0;
    eterm_buf ebcmp;

    rq->cmp.arg = &ebcmp;

    couchfile_pointer_info *oldroot = NULL;
    if(rq->root.pointer != 0)
    {
        oldroot = &rq->root;
    }
    couchfile_pointer_info *newroot = modify_btree(rq, oldroot, &errcode);
    if(errcode < 0)
    {
        enif_send(rq->caller_env, &rq->caller, rq->nif_env,
                enif_make_tuple2(rq->nif_env,
                    rq->ref,
                    error_term(rq->nif_env, errcode)));
        goto cleanup;
    }

    if(newroot == oldroot)
    {
        //we got the same root back (no modify happened);
        enif_send(rq->caller_env, &rq->caller, rq->nif_env, enif_make_tuple2(rq->nif_env,
                    rq->ref,
                    get_atom(rq->nif_env, "not_modified")));
    }
    else
    {
        ERL_NIF_TERM reduce_term;
        char *reduceval = (char*) enif_make_new_binary(rq->nif_env,
                newroot->reduce_value.size,
                &reduce_term);
        memcpy(reduceval, newroot->reduce_value.buf, newroot->reduce_value.size);
        wait_pointer(rq, newroot);
        enif_send(rq->caller_env, &rq->caller, rq->nif_env, enif_make_tuple2(rq->nif_env,
                    rq->ref,
                    enif_make_tuple2(rq->nif_env,
                        get_atom(rq->nif_env, "new_root"),
                        enif_make_tuple3(rq->nif_env,
                            enif_make_long(rq->nif_env, newroot->pointer),
                            reduce_term,
                            enif_make_long(rq->nif_env, newroot->subtreesize)))));
        free(newroot);
    }
cleanup:
    close(rq->fd);
    enif_free_env(rq->nif_env);
    enif_free(rq->actions);
    enif_free(rq);
    return NULL;
}

// do_native_modify/6 (ActionList, filename, comparetype, reducetype, root, writer_pid)
// comparetype: 0 - raw ascii binary collation, 1 - integer term compare
// reducetype: 0 - no reduce, 1 - by_seq/docinfo reduce (count)
static ERL_NIF_TERM query_modify(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned int num_actions = 0, actnum = 0;
    ERL_NIF_TERM head = argv[0];
    ERL_NIF_TERM term;
    int fd, reducetype, comparetype;
    char filename[MAXPATHLEN];
    ERL_NIF_TERM nil_root_term = get_atom(env, "nil");
    btreenif_state* globalstate = enif_priv_data(env);
    enif_cond_broadcast(globalstate->writer_cond.cond);

    if(!enif_get_list_length(env, argv[0], &num_actions))
    {
        return enif_make_badarg(env);
    }

    if(!enif_get_string(env, argv[1], filename, MAXPATHLEN, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    if(num_actions == 0)
    {
        return get_atom(env, "ok");
    }

    enif_get_int(env, argv[3], &reducetype);
    enif_get_int(env, argv[2], &comparetype);

    couchfile_modify_request* rq = enif_alloc(sizeof(couchfile_modify_request));

    if(!enif_get_local_pid(env, argv[5], &rq->writer))
    {
        enif_free(rq);
        return enif_make_badarg(env);
    }

    fd = open(filename, O_RDWR | O_APPEND);
    if(fd == -1)
    {
        return enif_make_tuple2(env,
                get_atom(env, "error"),
                get_atom(env, "file_open"));
    }

    rq->nif_env = enif_alloc_env();
    enif_self(env, &rq->caller);
    rq->fd = fd;
    rq->num_actions = num_actions;
    rq->fetch_callback = &fetch_send;
    rq->root.writerq_resource = NULL;
    rq->globalstate = globalstate;


    char* actbuf = enif_alloc(
            (sizeof(eterm_buf) * 3 + sizeof(couchfile_modify_action)) * num_actions);

    rq->actions = (couchfile_modify_action*) actbuf;

    eterm_buf* keys = (eterm_buf*)
        (actbuf + sizeof(couchfile_modify_action) * num_actions);

    eterm_buf* cmp_keys = (eterm_buf*)
        (actbuf + sizeof(couchfile_modify_action) * num_actions
                + sizeof(eterm_buf) * num_actions);

    eterm_buf* values = (eterm_buf*)
        (actbuf + sizeof(couchfile_modify_action) * num_actions
                + sizeof(eterm_buf) * num_actions * 2);

    while(enif_get_list_cell(env, head, &term, &head))
    {
        const ERL_NIF_TERM *actterms;
        ErlNifBinary kvbin;
        int arity;

        enif_get_tuple(env, term, &arity, &actterms);
        enif_get_int(env, actterms[0], &rq->actions[actnum].type);

        copy_and_inspect(rq->nif_env, actterms[1], &kvbin);

        //skip version prefix from term_to_binary (131)
        keys[actnum].buf = (char*) kvbin.data + 1;
        keys[actnum].size = kvbin.size - 1;
        rq->actions[actnum].key = &keys[actnum];

        switch(comparetype)
        {
            case 0: //Compare ascii binaries
                //Skip 131, BINARY_EXT, binary size so that ebin_cmp is passed
                //only the contents of the binary
                cmp_keys[actnum].buf = (char*) kvbin.data + 6;
                cmp_keys[actnum].size = kvbin.size - 6;
                rq->actions[actnum].cmp_key = &cmp_keys[actnum];
            break;

            case 1: //Compare number terms
                cmp_keys[actnum].buf = keys[actnum].buf;
                cmp_keys[actnum].size = keys[actnum].size;
                rq->actions[actnum].cmp_key = &cmp_keys[actnum];
            break;
        }

        if(arity == 3) //Action has a value
        {
            copy_and_inspect(rq->nif_env, actterms[2], &kvbin);
            //skip version prefix from term_to_binary (131)
            values[actnum].buf = (char*) kvbin.data + 1;
            values[actnum].size = kvbin.size - 1;
            rq->actions[actnum].value = &values[actnum];
        }
        else
        {
            rq->actions[actnum].value = NULL;
        }
        actnum++;
    }

    switch(reducetype)
    {
        case 2:
            rq->reduce = by_id_reduce;
            rq->rereduce = by_id_rereduce;
        break;
        case 1:
            rq->reduce = by_seq_reduce;
            rq->rereduce = by_seq_rereduce;
        break;
        case 0:
        default:
            rq->reduce = NULL;
            rq->rereduce = NULL;
    }

    switch(comparetype)
    {
        case 0:
            rq->cmp.compare = ebin_cmp;
            rq->cmp.from_ext = ebin_from_ext;
        break;
        case 1:
            rq->cmp.compare = long_term_cmp;
            rq->cmp.from_ext = term_from_ext;
        break;
    }

    if(enif_is_identical(argv[4], nil_root_term))
    {
        rq->root.pointer = 0;
    }
    else
    {
        get_pointer_info(env, argv[4], &rq->root);
    }

    ERL_NIF_TERM ref = enif_make_ref(env);
    rq->ref = enif_make_copy(rq->nif_env, ref);
    rq->caller_env = NULL;

    queue_request(globalstate, rq);
    return enif_make_tuple2(env, get_atom(env, "ok"), ref);
}

// write_response/2 (rqresource, pos, size)
static ERL_NIF_TERM write_response(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    nif_writerq* wrq = NULL;
    uint64_t pointer = 0;
    uint64_t size = 0;
    btreenif_state* globalstate = enif_priv_data(env);

    enif_get_resource(env, argv[0], writerq_type, (void**) &wrq);
    enif_get_uint64(env, argv[1], (ErlNifUInt64*) &pointer);
    enif_get_uint64(env, argv[2], (ErlNifUInt64*) &size);
    enif_mutex_lock(globalstate->writer_cond.mtx);
    wrq->ptr->pointer = pointer;
    wrq->ptr->subtreesize += size;
    enif_keep_resource(wrq); //don't let the resource object get gc'd before the
                             //updater thread is done with it.
    enif_cond_broadcast(globalstate->writer_cond.cond);
    enif_mutex_unlock(globalstate->writer_cond.mtx);
    return get_atom(env, "ok");
}

void *worker_thread(void *arg)
{
    btreenif_state* state = arg;
    enif_mutex_lock(state->work_queue_cond.mtx);
    while(1)
    {
        while(state->queue != NULL)
        {
            couchfile_modify_request* rq = state->queue;
            state->queue = rq->next;
            enif_mutex_unlock(state->work_queue_cond.mtx);
            query_modify_thread(rq);
            enif_mutex_lock(state->work_queue_cond.mtx);
        }
        //if the work queue is empty, wait to be woken up.
        enif_cond_wait(state->work_queue_cond.cond, state->work_queue_cond.mtx);
    }
    enif_mutex_unlock(state->work_queue_cond.mtx);
    return NULL;
}

void queue_request(btreenif_state* state, couchfile_modify_request* rq)
{
    enif_mutex_lock(state->work_queue_cond.mtx);
    rq->next = NULL;
    if(state->queue == NULL)
    {
        state->queue = rq;
    }
    else
    {
        couchfile_modify_request *l = state->queue;
        while(l->next != NULL)
        {
            l = l->next;
        }
        l->next = rq;
    }
    enif_cond_signal(state->work_queue_cond.cond);
    enif_mutex_unlock(state->work_queue_cond.mtx);
}

static int btreenif_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    writerq_type = enif_open_resource_type(env, NULL, "writerq", NULL, ERL_NIF_RT_CREATE, NULL);
    btreenif_state *state = enif_alloc(sizeof(btreenif_state));
    *priv = state;

    state->writer_cond.cond = enif_cond_create("btreenif_cond");
    state->writer_cond.mtx = enif_mutex_create("btreenif_mutex");
    state->check_env = enif_alloc_env();
    state->atom_heart = get_atom(env, "heart"); //atom terms are env independent
    state->queue = NULL;

    state->work_queue_cond.cond = enif_cond_create("btreenif_workqueue_cond");
    state->work_queue_cond.mtx = enif_mutex_create("btreenif_workqueue_mtx");
    ErlNifTid workertid;
    enif_thread_create("native_btree_worker", &workertid, &worker_thread, state, NULL);

    return 0;
}

static ErlNifFunc nif_funcs [] =
{
    {"do_native_modify", 6, query_modify},
    {"write_response", 3, write_response}
};

ERL_NIF_INIT(couch_btree_nif, nif_funcs, &btreenif_load, NULL, NULL, NULL);

