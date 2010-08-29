#ifndef EMONK_VM_H
#define EMONK_VM_H

#include <js/jsapi.h>
#include "erl_nif.h"

#include "alias.h"

typedef struct vm_t* vm_ptr;

vm_ptr vm_init(ErlNifResourceType* res_type, JSRuntime* runtime, size_t stack_size);
void vm_destroy(ErlNifEnv* env, void* obj);

int vm_add_eval(vm_ptr vm, ENTERM ref, ENPID pid, ENBINARY bin);
int vm_add_call(vm_ptr vm, ENTERM ref, ENPID pid, ENTERM name, ENTERM args);
int vm_send(vm_ptr vm, ENTERM data);

#endif // Included vm.h
