#include <assert.h>
#include <stdio.h>

#include "queue.h"

struct qitem_t
{
    struct qitem_t*     next;
    void*               data;
};

typedef struct qitem_t* qitem_ptr;

struct qdata_t
{
    qitem_ptr           head;
    qitem_ptr           tail;
    int                 length;
};

typedef struct qdata_t* qdata_ptr;

struct queue_t
{
    ErlNifMutex*        lock;
    ErlNifCond*         cond;
    qdata_ptr           jobs;
    qdata_ptr           msgs;
};

int queue_has_item(queue_ptr queue, qdata_ptr data);
int queue_push_int(queue_ptr queue, qdata_ptr data, void* item);
void* queue_pop_int(queue_ptr queue, qdata_ptr data);

queue_ptr
queue_create(const char* name)
{
    queue_ptr ret;

    ret = (queue_ptr) enif_alloc(sizeof(struct queue_t));
    if(ret == NULL) goto error;

    ret->lock = NULL;
    ret->cond = NULL;
    ret->jobs = NULL;
    ret->msgs = NULL;

    ret->lock = enif_mutex_create("queue_lock");
    if(ret->lock == NULL) goto error;
    
    ret->cond = enif_cond_create("queue_cond");
    if(ret->cond == NULL) goto error;

    ret->jobs = (qdata_ptr) enif_alloc(sizeof(struct qdata_t));
    if(ret->jobs == NULL) goto error;
    ret->jobs->head = NULL;
    ret->jobs->tail = NULL;
    ret->jobs->length = 0;
    
    ret->msgs = (qdata_ptr) enif_alloc(sizeof(struct qdata_t));
    if(ret->msgs == NULL) goto error;
    ret->msgs->head = NULL;
    ret->msgs->tail = NULL;
    ret->msgs->length = 0;

    return ret;

error:
    if(ret->lock != NULL) enif_mutex_destroy(ret->lock);
    if(ret->cond != NULL) enif_cond_destroy(ret->cond);
    if(ret->jobs != NULL) enif_free(ret->jobs);
    if(ret->msgs != NULL) enif_free(ret->msgs);
    if(ret != NULL) enif_free(ret);
    return NULL;
}

void
queue_destroy(queue_ptr queue)
{
    ErlNifMutex* lock;
    ErlNifCond* cond;
    qdata_ptr jobs;
    qdata_ptr msgs;

    enif_mutex_lock(queue->lock);
    lock = queue->lock;
    cond = queue->cond;
    jobs = queue->jobs;
    msgs = queue->msgs;

    queue->lock = NULL;
    queue->cond = NULL;
    queue->jobs = NULL;
    queue->msgs = NULL;
    enif_mutex_unlock(lock);

    assert(jobs->length == 0 && "Destroying queue while jobs exist.");
    assert(msgs->length == 0 && "Destroying queue while messages exist.");
    enif_cond_destroy(cond);
    enif_mutex_destroy(lock);
    enif_free(jobs);
    enif_free(msgs);
    enif_free(queue);
}

int
queue_has_job(queue_ptr queue)
{
    return queue_has_item(queue, queue->jobs);
}

int
queue_push(queue_ptr queue, void* item)
{
    return queue_push_int(queue, queue->jobs, item);
}

void*
queue_pop(queue_ptr queue)
{
    return queue_pop_int(queue, queue->jobs);
}

int
queue_has_msg(queue_ptr queue)
{
    return queue_has_item(queue, queue->msgs);
}

int
queue_send(queue_ptr queue, void* item)
{
    return queue_push_int(queue, queue->msgs, item);
}

void*
queue_receive(queue_ptr queue)
{
    return queue_pop_int(queue, queue->msgs);
}

int
queue_has_item(queue_ptr queue, qdata_ptr data)
{
    int ret;

    enif_mutex_lock(queue->lock);
    ret = (data->length > 0);
    enif_mutex_unlock(queue->lock);
    
    return ret;
}

int
queue_push_int(queue_ptr queue, qdata_ptr data, void* item)
{
    qitem_ptr entry = (qitem_ptr) enif_alloc(sizeof(struct qitem_t));
    if(entry == NULL) return 0;

    entry->data = item;
    entry->next = NULL;

    enif_mutex_lock(queue->lock);

    assert(data->length >= 0 && "Invalid queue size at push");
    
    if(data->tail != NULL)
    {
        data->tail->next = entry;
    }

    data->tail = entry;

    if(data->head == NULL)
    {
        data->head = data->tail;
    }

    data->length += 1;

    enif_cond_signal(queue->cond);
    enif_mutex_unlock(queue->lock);

    return 1;
}

void*
queue_pop_int(queue_ptr queue, qdata_ptr data)
{
    qitem_ptr entry;
    void* item;

    enif_mutex_lock(queue->lock);
    
    // Wait for an item to become available.
    while(data->head == NULL)
    {
        enif_cond_wait(queue->cond, queue->lock);
    }
    
    assert(data->length >= 0 && "Invalid queue size at pop.");

    // Woke up because queue->head != NULL
    // Remove the entry and return the payload.

    entry = data->head;
    data->head = entry->next;
    entry->next = NULL;

    if(data->head == NULL)
    {
        assert(data->tail == entry && "Invalid queue state: Bad tail.");
        data->tail = NULL;
    }

    data->length -= 1;

    enif_mutex_unlock(queue->lock);

    item = entry->data;
    enif_free(entry);

    return item;
}