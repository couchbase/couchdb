#include "config_static.h"

#include <stdlib.h>
#include <string.h>

#include "couch_btree.h"
#include "ei.h"

#define CHUNK_THRESHOLD 1279
//#define DBG(...) fprintf(stderr, __VA_ARGS__)
#define DBG(...) 

eterm_buf empty_root = {
    // {kv_node, []}
    "\x68\x02\x64\x00\x07kv_node\x6A",
    13
};

int flush_mr(couchfile_modify_result *res);

int find_first_gteq(char* buf, int pos, void* key, compare_info* lu, int at_least)
{
    int list_arity, inner_arity;
    int list_pos = 0, cmp_val;
    off_t pair_pos = 0;
    if(ei_decode_list_header(buf, &pos, &list_arity) < 0)
    {
        return ERROR_PARSE;
    }
    while(list_pos < list_arity)
    {
        //{<<"key", some other term}
        pair_pos = pos; //Save pos of kv/kp pair tuple

        if(ei_decode_tuple_header(buf, &pos, &inner_arity) < 0)
        {
            return ERROR_PARSE;
        }

        lu->last_cmp_key = (*lu->from_ext)(lu, buf,pos);
        cmp_val = (*lu->compare) (lu->last_cmp_key, key);
        lu->last_cmp_val = cmp_val;
        lu->list_pos = list_pos;
        if(cmp_val >= 0 && list_pos >= at_least)
        {
            break;
        }
        ei_skip_term(buf, &pos); //skip over the key
        ei_skip_term(buf, &pos); //skip over the value
        list_pos++;
    }
    return pair_pos;
}

int maybe_flush(couchfile_modify_result *mr)
{
    if(mr->modified && mr->node_len > CHUNK_THRESHOLD)
    {
        return flush_mr(mr);
    }
    return 0;
}


void free_nodelist(nodelist* nl)
{
    while(nl != NULL)
    {
        nodelist* next = nl->next;
        if(nl->value.mem != NULL)
        {
           free(nl->value.mem);
        }
        free(nl);
        nl = next;
    }
}

nodelist* make_nodelist()
{
    nodelist* r = malloc(sizeof(nodelist));
    if(!r)
        return NULL;
    r->next = NULL;
    r->value.mem = NULL;
    return r;
}

couchfile_modify_result *make_modres(couchfile_modify_request *rq) {
    couchfile_modify_result *res = malloc(sizeof(couchfile_modify_result));
    if(!res)
        return NULL;
    res->values = make_nodelist();
    if(!res->values)
    {
        free(res);
        return NULL;
    }
    res->values_end = res->values;
    res->pointers = make_nodelist();
    if(!res->pointers)
    {
        free(res);
        return NULL;
    }
    res->pointers_end = res->pointers;
    res->node_len = 0;
    res->count = 0;
    res->modified = 0;
    res->error_state = 0;
    res->rq = rq;
    return res;
}

void term_to_buf(eterm_buf *dst, char* buf, int *pos)
{
    int start = *pos;
    ei_skip_term(buf, pos);
    dst->buf = buf + start;
    dst->size = *pos - start;
}

void free_modres(couchfile_modify_result* mr)
{
    free_nodelist(mr->values);
    free_nodelist(mr->pointers);
    free(mr);
}

int mr_push_action(couchfile_modify_action *act, couchfile_modify_result *dst)
{
    nodelist* n = make_nodelist();
    //For ACTION_INSERT
    couchfile_leaf_value* lv = malloc(sizeof(couchfile_leaf_value) +
           act->key->size + act->value->size + 2);
    if(!lv)
        goto fail;
    //Allocate space for {K,V} term
    lv->term.buf = ((char*)lv) + sizeof(couchfile_leaf_value);
    lv->term.size = (act->key->size + act->value->size + 2);
    //tuple of arity 2
    lv->term.buf[0] = 104;
    lv->term.buf[1] = 2;
    //copy terms from the action
    memcpy(lv->term.buf + 2, act->key->buf, act->key->size);
    memcpy(lv->term.buf + 2 + act->key->size,
            act->value->buf, act->value->size);

    if(!n)
        goto fail;
    dst->values_end->next = n;
    dst->values_end = n;
    n->value.leaf = lv;

    dst->node_len += lv->term.size;
    dst->count++;
    return maybe_flush(dst);
fail:
    if(lv)
        free(lv);
    return ERROR_ALLOC_FAIL;
}

int mr_push_pointerinfo(couchfile_pointer_info* ptr, couchfile_modify_result* dst)
{
    nodelist* pel = make_nodelist();
    if(!pel)
        goto fail;
    pel->value.pointer = ptr;
    dst->values_end->next = pel;
    dst->values_end = pel;

    //max len of {key,{pointer, reduce_value, subtreesize}}
    dst->node_len += ptr->key.size + ptr->reduce_value.size + 24;
    dst->count++;
    return maybe_flush(dst);
fail:
    return ERROR_ALLOC_FAIL;
}

int mr_push_kv_range(char* buf, int pos, int bound, int end, couchfile_modify_result *dst)
{
    int current = 0;
    int term_begin_pos;
    int errcode = 0;
    ei_decode_list_header(buf, &pos, NULL);
    while(current < end && errcode == 0)
    {
        term_begin_pos = pos;
        ei_skip_term(buf, &pos);
        if(current >= bound)
        { 
            nodelist* n = make_nodelist();
            //Parse KV pair into a leaf_value
            couchfile_leaf_value *lv = malloc(sizeof(couchfile_leaf_value));
            if(!lv)
            {
                errcode = ERROR_ALLOC_FAIL;
                break;
            }

            lv->term.buf = buf+term_begin_pos;
            lv->term.size = pos-term_begin_pos;

            if(!n)
            {
                errcode = ERROR_ALLOC_FAIL;
                free(lv);
                break;
            }

            dst->values_end->next = n;
            dst->values_end = n;
            n->value.leaf = lv;

            dst->node_len += lv->term.size;
            dst->count++;
            errcode = maybe_flush(dst);
        }
        current++;
    }
    return errcode;
}

couchfile_pointer_info* read_pointer(char* buf, int pos)
{
//Parse KP pair into a couchfile_pointer_info {K, {ptr, reduce_value, subtreesize}}
    couchfile_pointer_info *p = malloc(sizeof(couchfile_pointer_info));
    if(!p)
        return NULL;
    p->writerq_resource = NULL;
    //DBG("%u,%u,%u,%u\n", buf[pos+0], buf[pos+1], buf[pos+2], buf[pos+3]);
    ei_decode_tuple_header(buf, &pos, NULL); //arity 2
    term_to_buf(&p->key, buf, &pos);
    ei_decode_tuple_header(buf, &pos, NULL); //arity 3
    ei_decode_ulonglong(buf, &pos, (unsigned long long*) &p->pointer);
    term_to_buf(&p->reduce_value, buf, &pos);
    ei_decode_ulonglong(buf, &pos, (unsigned long long*) &p->subtreesize);

    return p;
}

int mr_push_kp_range(char* buf, int pos, int bound, int end, couchfile_modify_result *dst)
{
    couchfile_pointer_info *read= NULL;
    int current = 0;
    DBG("Moving items %d - %d into result.\r\n", bound, end);

    ei_decode_list_header(buf, &pos, NULL);
    while(current < end)
    {
        if(current >= bound)
        {
            DBG("   .... %d\r\n", current);
            read = read_pointer(buf,pos);
            if(!read)
                return ERROR_ALLOC_FAIL;
            mr_push_pointerinfo(read, dst);
        }
        ei_skip_term(buf, &pos);
        current++;
    }
    return 0;
}

inline void append_buf(void* dst, int *dstpos, void* src, int len) {
    char* tmp = (char*)dst;
    memcpy(tmp + *dstpos, src, len);
    *dstpos += len;
}

int wait_pointer(couchfile_modify_request* rq, couchfile_pointer_info *ptr)
{
    int ret = 0;
    btreenif_state *state = rq->globalstate;

    if(ptr->writerq_resource == NULL)
        return 0;
    enif_mutex_lock(state->writer_cond.mtx);

    while(ptr->pointer == 0)
    {
        enif_cond_wait(state->writer_cond.cond, state->writer_cond.mtx);
        if(ptr->pointer == 0 &&
           !enif_send(rq->caller_env, &rq->writer, state->check_env, state->atom_heart))
        {
            //The writer process has died
            ret = ERROR_WRITER_DEAD;
            break;
        }
        enif_clear_env(state->check_env);
    }

    if(ptr->pointer != 0)
    {
        enif_release_resource(ptr->writerq_resource);
    }

    enif_mutex_unlock(state->writer_cond.mtx);

    ptr->writerq_resource = NULL;
    return ret;
}

//Write the current contents of the values list to disk as a node
//and add the resulting pointer to the pointers list.
int flush_mr(couchfile_modify_result *res)
{
    int nbufpos = 0;
    long long subtreesize = 0;
    eterm_buf reduce_value;
    //default reduce value []

    int reduced = 0;
    int errcode = 0;
    nif_writerq *wrq = NULL;
    char *nodebuf = NULL;
    nodelist* i = NULL;
    eterm_buf last_key;
    nodelist* pel = NULL;
    couchfile_pointer_info* ptr = NULL;

    reduce_value.buf = "\x6A"; //NIL_EXT
    reduce_value.size = 1;

    if(res->values_end == res->values || !res->modified)
    {
        //Empty
        return 0;
    }

    res->node_len += 19; //tuple header and node type tuple, list header and tail
    wrq = nif_writerq_alloc(res->node_len);
    nodebuf = wrq->buf;

    //External term header; tuple header arity 2;
    ei_encode_version(nodebuf, &nbufpos);
    ei_encode_tuple_header(nodebuf, &nbufpos, 2);
    switch(res->node_type)
    {
        case KV_NODE:
            ei_encode_atom_len(nodebuf, &nbufpos, "kv_node", 7);
            if(res->rq->reduce)
            {
                (*res->rq->reduce)(&reduce_value, res->values->next, res->count);
                reduced = 1;
            }
        break;
        case KP_NODE:
            ei_encode_atom_len(nodebuf, &nbufpos, "kp_node", 7);
            if(res->rq->rereduce)
            {
                (*res->rq->rereduce)(&reduce_value, res->values->next, res->count);
                reduced = 1;
            }
        break;
    }

    if(reduced && reduce_value.buf == NULL)
    {
        errcode = ERROR_ALLOC_FAIL;
        goto cleanup;
    }

    ei_encode_list_header(nodebuf, &nbufpos, res->count);

    i = res->values->next;

    while(i != NULL)
    {
        if(res->node_type == KV_NODE) //writing value in a kv_node
        {
            append_buf(nodebuf, &nbufpos, i->value.leaf->term.buf, i->value.leaf->term.size);
            if(i->next == NULL)
            {
                int pos = 0;
                term_to_buf(&last_key, i->value.leaf->term.buf+2, &pos);
            }
        }
        else if (res->node_type == KP_NODE) //writing value in a kp_node
        {
            if(wait_pointer(res->rq, i->value.pointer) < 0)
            {
                errcode = ERROR_WRITER_DEAD;
                goto cleanup;
            }
            subtreesize += i->value.pointer->subtreesize;
            ei_encode_tuple_header(nodebuf, &nbufpos, 2); //tuple arity 2
            append_buf(nodebuf, &nbufpos, i->value.pointer->key.buf, i->value.pointer->key.size);
            ei_encode_tuple_header(nodebuf, &nbufpos, 3); //tuple arity 3
            //pointer
            // v- between 2 and 10 bytes (ERL_SMALL_INTEGER_EXT to ERL_SMALL_BIG_EXT/8)
            ei_encode_ulonglong(nodebuf, &nbufpos, i->value.pointer->pointer);
            //reduce_value
            append_buf(nodebuf, &nbufpos, i->value.pointer->reduce_value.buf,
                    i->value.pointer->reduce_value.size);
            //subtreesize
            // v- between 2 and 10 bytes (ERL_SMALL_INTEGER_EXT to ERL_SMALL_BIG_EXT/8)
            ei_encode_ulonglong(nodebuf, &nbufpos, i->value.pointer->subtreesize);
            if(i->next == NULL)
            {
                last_key = i->value.pointer->key;
            }
        }
        i = i->next;
    }

    //NIL_EXT (list tail)
    ei_encode_empty_list(nodebuf, &nbufpos);

    ptr = malloc(sizeof(couchfile_pointer_info) +
            last_key.size + reduce_value.size);
    if(!ptr)
    {
        errcode = ERROR_ALLOC_FAIL;
        goto cleanup;
    }
    ptr->pointer = 0;

    nif_write(res->rq, ptr, wrq, nbufpos);

    ptr->key.buf = ((char*)ptr) + sizeof(couchfile_pointer_info);
    ptr->reduce_value.buf = ((char*)ptr) + sizeof(couchfile_pointer_info) + last_key.size;

    ptr->key.size = last_key.size;
    ptr->reduce_value.size = reduce_value.size;

    memcpy(ptr->key.buf, last_key.buf, last_key.size);
    memcpy(ptr->reduce_value.buf, reduce_value.buf, reduce_value.size);

    ptr->subtreesize = subtreesize;

    pel = make_nodelist();
    if(!pel)
    {
        errcode = ERROR_ALLOC_FAIL;
        free(ptr);
        goto cleanup;
    }

    pel->value.pointer = ptr;
    res->pointers_end->next = pel;
    res->pointers_end = pel;

    res->node_len = 0;
    res->count = 0;

    res->values_end = res->values;
    free_nodelist(res->values->next);
    res->values->next = NULL;
cleanup:

    if(errcode < 0)
    {
        enif_release_resource(wrq);
    }

    if(reduced)
    {
        free(reduce_value.buf);
    }
    return errcode;
}

//Move this node's pointers list to dst node's values list.
int mr_move_pointers(couchfile_modify_result *src, couchfile_modify_result *dst)
{
    int errcode = 0;
    nodelist *ptr = NULL;
    nodelist *next = NULL;
    if(src->pointers_end == src->pointers)
    {
        return 0;
    }

    ptr = src->pointers->next;
    next = ptr;
    while(ptr != NULL && errcode == 0)
    {
        //max on disk len of a pointer node
        dst->node_len += ptr->value.pointer->key.size + 
            ptr->value.pointer->reduce_value.size + 24;
        dst->count++;

        next = ptr->next;
        ptr->next = NULL;

        dst->values_end->next = ptr;
        dst->values_end = ptr;
        errcode = maybe_flush(dst);
        ptr = next;
    }

    src->pointers->next = next;
    src->pointers_end = src->pointers;
    return errcode;
}

int modify_node(couchfile_modify_request *rq, couchfile_pointer_info *nptr,
                int start, int end, couchfile_modify_result *dst)
{
    eterm_buf current_node;
    int curnode_pos = 0;
    int read_size = 0;
    int list_start_pos = 0;
    int node_len = 0;
    int node_bound = 0;
    int errcode = 0;
    int kpos = 0;
    couchfile_modify_result *local_result = NULL;

    char node_type[MAXATOMLEN + 1];
    node_type[0] = 0;

    DBG("Enter modify_node. %d - %d\r\n", start, end);

    if(start == end)
    {
        return 0;
    }

    if(nptr == NULL)
    {
        current_node = empty_root;
    }
    else
    {
        if((read_size = pread_bin(rq->fd, nptr->pointer, &current_node.buf)) < 0)
        {
            return ERROR_READ_FILE;
        }
        current_node.size = read_size;
        DBG("... read node from %d\r\n", nptr->pointer);
        curnode_pos++; //Skip over 131.
    }

    local_result = make_modres(rq);
    if(!local_result)
    {
        errcode = ERROR_ALLOC_FAIL;
        goto cleanup;
    }

    ei_decode_tuple_header(current_node.buf, &curnode_pos, NULL);
    if(ei_decode_atom(current_node.buf, &curnode_pos, node_type) < 0)
    {
        errcode = ERROR_PARSE;
        goto cleanup;
    }
    list_start_pos = curnode_pos;
    if(ei_decode_list_header(current_node.buf, &curnode_pos, &node_len) < 0)
    {
        errcode = ERROR_PARSE;
        goto cleanup;
    }

    if(strcmp("kv_node", node_type) == 0)
    {
        local_result->node_type = KV_NODE;
        while(start < end)
        {
            DBG("act on kvnode item\r\n");
            if(node_bound >= node_len)
            { //We're at the end of a leaf node.
                DBG("   ... exec action at end!\r\n");
                switch(rq->actions[start].type)
                {
                    case ACTION_INSERT:
                        local_result->modified = 1;
                        mr_push_action(&rq->actions[start], local_result);
                    break;

                    case ACTION_REMOVE:
                        local_result->modified = 1;
                    break;

                    case ACTION_FETCH:
                        if(rq->fetch_callback)
                        {
                            //not found
                            (*rq->fetch_callback)(rq, rq->actions[start].key, NULL);
                        }
                    break;
                }
                start++;
            }
            else
            {
                kpos = find_first_gteq(current_node.buf, list_start_pos,
                        rq->actions[start].cmp_key,
                        &rq->cmp, node_bound);

                if(kpos < 0)
                {
                    errcode = ERROR_PARSE;
                    goto cleanup;
                }

                //Add items from node_bound up to but not including the current
                mr_push_kv_range(current_node.buf, list_start_pos, node_bound,
                        rq->cmp.list_pos, local_result);

                if(rq->cmp.last_cmp_val > 0) // Node key > action key
                {
                    DBG("   Inserting action before\r\n");
                    switch(rq->actions[start].type)
                    {
                        case ACTION_INSERT:
                            local_result->modified = 1;
                            mr_push_action(&rq->actions[start], local_result);
                        break;

                        case ACTION_REMOVE:
                            local_result->modified = 1;
                        break;

                        case ACTION_FETCH:
                            if(rq->fetch_callback)
                            {
                                //not found
                                (*rq->fetch_callback)(rq, rq->actions[start].key, NULL);
                            }
                        break;
                    }

                    start++;
                    node_bound = rq->cmp.list_pos;
                }
                else if(rq->cmp.last_cmp_val < 0) // Node key < action key
                {
                    DBG("   -- Continue with this action\r\n");
                    node_bound = rq->cmp.list_pos + 1;
                    mr_push_kv_range(current_node.buf, list_start_pos, node_bound - 1,
                            node_bound, local_result);
                }
                else //Node key == action key
                {
                    DBG("   Replacing value with action\r\n");
                    switch(rq->actions[start].type)
                    {
                        case ACTION_INSERT:
                            local_result->modified = 1;
                            mr_push_action(&rq->actions[start], local_result);
                            node_bound = rq->cmp.list_pos + 1;
                        break;

                        case ACTION_REMOVE:
                            local_result->modified = 1;
                            node_bound = rq->cmp.list_pos + 1;
                        break;

                        case ACTION_FETCH:
                            if(rq->fetch_callback)
                            {
                                eterm_buf cb_tmp;
                                int cb_vpos = kpos;
                                ei_decode_tuple_header(current_node.buf, &cb_vpos, NULL);
                                ei_skip_term(current_node.buf, &cb_vpos);
                                cb_tmp.buf = current_node.buf + cb_vpos;
                                cb_tmp.size = cb_vpos;
                                ei_skip_term(current_node.buf, &cb_vpos);
                                cb_tmp.size = cb_vpos - cb_tmp.size;
                                (*rq->fetch_callback)(rq, rq->actions[start].key, &cb_tmp);
                            }
                            node_bound = rq->cmp.list_pos;
                        break;
                    }
                    start++;
                }
            }
        }
        //Push any items past the end of what we dealt with onto result.
        if(node_bound < node_len)
        {
            mr_push_kv_range(current_node.buf, list_start_pos, node_bound,
                    node_len, local_result);
        }
    }
    else if(strcmp("kp_node", node_type) == 0)
    {
        local_result->node_type = KP_NODE;
        while(start < end)
        {
            kpos = find_first_gteq(current_node.buf, list_start_pos,
                    rq->actions[start].cmp_key,
                    &rq->cmp, node_bound);

            if(kpos < 0)
            {
                errcode = ERROR_PARSE;
                goto cleanup;
            }

            if(rq->cmp.list_pos == (node_len - 1)) //got last item in kp_node
            {
                couchfile_pointer_info *desc = NULL;
                //Push all items except last onto mr
                errcode = mr_push_kp_range(current_node.buf, list_start_pos, node_bound,
                        rq->cmp.list_pos, local_result);
                if(errcode < 0)
                {
                    goto cleanup;
                }
                DBG("  ...descending into final item of kpnode\r\n");
                desc = read_pointer(current_node.buf, kpos);
                if(!desc)
                {
                    errcode = ERROR_ALLOC_FAIL;
                    goto cleanup;
                }

                errcode = modify_node(rq, desc, start, end, local_result);
                if(local_result->values_end->value.pointer != desc)
                {
                        free(desc);
                }

                if(errcode < 0)
                {
                    goto cleanup;
                }
                node_bound = node_len;
                break;
            }
            else
            {
                int range_end = start;
                couchfile_pointer_info *desc = NULL;

                //Get all actions with key <= the key of the current item in the
                //kp_node

                //Push items in node up to but not including current onto mr
                errcode = mr_push_kp_range(current_node.buf, list_start_pos, node_bound,
                        rq->cmp.list_pos, local_result);
                if(errcode < 0)
                {
                    goto cleanup;
                }

                while(range_end < end &&
                      ((*rq->cmp.compare)(rq->actions[range_end].cmp_key, rq->cmp.last_cmp_key) <= 0))
                {
                    range_end++;
                }

                DBG("  ...descending into item %d of kpnode\r\n", rq->cmp.list_pos);
                node_bound = rq->cmp.list_pos + 1;
                desc = read_pointer(current_node.buf, kpos);
                if(!desc)
                {
                    errcode = ERROR_ALLOC_FAIL;
                    goto cleanup;
                }

                errcode = modify_node(rq, desc, start, range_end, local_result);
                if(local_result->values_end->value.pointer != desc)
                {
                        free(desc);
                }
                if(errcode < 0)
                {
                    goto cleanup;
                }
                start = range_end;
            }
        }
        DBG(".. Finished kp node, up to %d\r\n", node_bound);
        if(node_bound < node_len)
        {
            //Processed all the actions but haven't exhausted this kpnode.
            //push the rest of it onto the mr.
            errcode = mr_push_kp_range(current_node.buf, list_start_pos, node_bound, node_len,
                    local_result);
            if(errcode < 0)
            {
                goto cleanup;
            }
        }
    }
    else
    {
        errcode = ERROR_PARSE;
        goto cleanup;
    }
    //If we've done modifications, write out the last leaf node.
    errcode = flush_mr(local_result);
    if(errcode == 0)
    {
        if(!local_result->modified && nptr != NULL)
        {
            //If we didn't do anything, give back the pointer to the original
            mr_push_pointerinfo(nptr, dst);
        }
        else
        {
            //Otherwise, give back the pointers to the nodes we've created.
            dst->modified = 1;
            errcode = mr_move_pointers(local_result, dst);
        }
    }
cleanup:
    free_modres(local_result);

    if(current_node.buf != empty_root.buf)
    {
        free(current_node.buf);
    }

    return errcode;
}

couchfile_pointer_info* finish_root(couchfile_modify_request* rq,
        couchfile_modify_result *root_result, int *errcode)
{
    couchfile_pointer_info *ret_ptr = NULL;
    couchfile_modify_result *collector = make_modres(rq);
    if(!collector)
    {
        return NULL;
    }
    collector->modified = 1;
    collector->node_type = KP_NODE;
    flush_mr(root_result);
    while(1)
    {
        if(root_result->pointers_end == root_result->pointers->next)
        {
            //The root result split into exactly one kp_node.
            //Return the pointer to it.
            ret_ptr = root_result->pointers_end->value.pointer;
            root_result->pointers_end->value.mem = NULL;
            break;
        }
        else
        {
            couchfile_modify_result *tmp = NULL;
            //The root result split into more than one kp_node.
            //Move the pointer list to the value list and write out the new node.
            *errcode = mr_move_pointers(root_result, collector);
            if(*errcode < 0)
            {
                goto cleanup;
            }

            *errcode = flush_mr(collector);
            if(*errcode < 0)
            {
                goto cleanup;
            }
            //Swap root_result and collector mr's.
            tmp = root_result;
            root_result = collector;
            collector = tmp;
        }
    }
cleanup:
    free_modres(root_result);
    free_modres(collector);
    return ret_ptr;
}

couchfile_pointer_info* modify_btree(couchfile_modify_request *rq,
        couchfile_pointer_info *root, int *errcode)
{
    couchfile_pointer_info* ret_ptr = root;
    couchfile_modify_result* root_result = make_modres(rq);
    if(!rq)
    {
        *errcode = ERROR_ALLOC_FAIL;
        return root;
    }
    root_result->node_type = KP_NODE;
    *errcode = modify_node(rq, root, 0, rq->num_actions, root_result);
    if(*errcode < 0)
    {
        free_modres(root_result);
        return NULL;
    }

    if(root_result->values_end->value.pointer == root)
    {
        //If we got the root pointer back, remove it from the list
        //so we don't try to free it.
        root_result->values_end->value.mem = NULL;
    }

    if(!root_result->modified)
    {
        free_modres(root_result);
    }
    else
    {
        if(root_result->count > 1 || root_result->pointers != root_result->pointers_end)
        {
            //The root was split
            //Write it to disk and return the pointer to it.
            ret_ptr = finish_root(rq, root_result, errcode);
            if(*errcode < 0)
            {
                ret_ptr = NULL;
            }
        }
        else
        {
            ret_ptr = root_result->values_end->value.pointer;
            root_result->values_end->value.mem = NULL;
            free_modres(root_result);
        }
    }
    return ret_ptr;
}

