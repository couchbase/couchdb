/***************************************************
 * Code credit :- Filipe Manana
 * https://github.com/fdmanana/snappy-erlang-nif/
 **************************************************/
#ifndef __TASKQUEUE_H
#define __TASKQUEUE_H

#include <queue>
#include "erl_nif_compat.h"

template <class T>
class TaskQueue
{
    public:
        TaskQueue(const unsigned int sz);
        ~TaskQueue();
        void enqueue(T);
        T dequeue();

    private:
        ErlNifCond *notFull;
        ErlNifCond *notEmpty;
        ErlNifMutex *mutex;
        std::queue<T> q;
        const unsigned int size;
};

template <class T>
TaskQueue<T>::TaskQueue(const unsigned int sz) : size(sz)
{
    notFull = enif_cond_create(const_cast<char *>("not_full_cond"));
    if (notFull == NULL) {
        throw std::bad_alloc();
    }
    notEmpty = enif_cond_create(const_cast<char *>("not_empty_cond"));
    if (notEmpty == NULL) {
        enif_cond_destroy(notFull);
        throw std::bad_alloc();
    }
    mutex = enif_mutex_create(const_cast<char *>("queue_mutex"));
    if (mutex == NULL) {
        enif_cond_destroy(notFull);
        enif_cond_destroy(notEmpty);
        throw std::bad_alloc();
    }
}

template <class T>
TaskQueue<T>::~TaskQueue()
{
    enif_cond_destroy(notFull);
    enif_cond_destroy(notEmpty);
    enif_mutex_destroy(mutex);
}

template <class T>
void TaskQueue<T>::enqueue(T t)
{
    enif_mutex_lock(mutex);

    if (q.size() >= size) {
        enif_cond_wait(notFull, mutex);
    }
    q.push(t);
    if (q.size() == 1) {
        enif_cond_broadcast(notEmpty);
    }

    enif_mutex_unlock(mutex);
}

template <class T>
T TaskQueue<T>::dequeue()
{
    T t;

    enif_mutex_lock(mutex);

    if (q.empty()) {
        enif_cond_wait(notEmpty, mutex);
    }
    t = q.front();
    q.pop();
    if (q.size() == (size - 1)) {
        enif_cond_broadcast(notFull);
    }

    enif_mutex_unlock(mutex);

    return t;
}

#endif
