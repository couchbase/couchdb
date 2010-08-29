#ifndef EMONK_QUEUE_H
#define EMONK_QUEUE_H

#include "erl_nif.h"

typedef struct queue_t* queue_ptr;

queue_ptr queue_create();
void queue_destroy(queue_ptr queue);

int queue_has_job(queue_ptr queue);
int queue_push(queue_ptr queue, void* item);
void* queue_pop(queue_ptr queue);

int queue_has_msg(queue_ptr queue);
int queue_send(queue_ptr queue, void* item);
void* queue_receive(queue_ptr);

#endif // Included queue.h
