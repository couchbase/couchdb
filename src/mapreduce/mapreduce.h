/**
 * @copyright 2012 Couchbase, Inc.
 *
 * @author Filipe Manana  <filipe@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#ifndef _MAPREDUCE_H
#define _MAPREDUCE_H

#include <cstddef>
#include <iostream>
#include <string>
#include <list>
#include <vector>
#include <v8.h>

#include "erl_nif_compat.h"

class MapReduceError;
template<class T> class NifStlAllocator;

typedef std::list<ErlNifBinary, NifStlAllocator<ErlNifBinary> >  json_results_list_t;

typedef std::pair<ErlNifBinary, ErlNifBinary> map_result_t;
typedef std::list< map_result_t, NifStlAllocator< map_result_t > >  map_results_list_t;
typedef std::list< map_results_list_t,
                   NifStlAllocator< map_results_list_t > >  map_results_list_list_t;

typedef std::vector< v8::Persistent<v8::Function>,
                     NifStlAllocator< v8::Persistent<v8::Function> > >  function_vector_t;

typedef std::basic_string< char,
                           std::char_traits<char>,
                           NifStlAllocator<char> >  function_source_t;

typedef std::list< function_source_t,
                   NifStlAllocator< function_source_t > >  function_sources_list_t;


typedef struct {
    v8::Persistent<v8::Context>                  jsContext;
    v8::Isolate                                  *isolate;
    function_vector_t                            *functions;
    map_results_list_t                           *mapFunResults;
    unsigned int                                 key;
    ErlNifEnv                                    *env;
    volatile int                                 taskId;
    volatile long                                taskStartTime;
} map_reduce_ctx_t;


void initContext(map_reduce_ctx_t *ctx, const function_sources_list_t &funs);
void destroyContext(map_reduce_ctx_t *ctx);

map_results_list_list_t mapDoc(map_reduce_ctx_t *ctx,
                               const ErlNifBinary &doc,
                               const ErlNifBinary &meta);

json_results_list_t runReduce(map_reduce_ctx_t *ctx,
                              const json_results_list_t &keys,
                              const json_results_list_t &values);

ErlNifBinary runReduce(map_reduce_ctx_t *ctx, int reduceFunNum,
                       const json_results_list_t &keys,
                       const json_results_list_t &values);

ErlNifBinary runRereduce(map_reduce_ctx_t *ctx,
                         int reduceFunNum,
                         const json_results_list_t &reductions);

void terminateTask(map_reduce_ctx_t *ctx);


class MapReduceError {
public:
    MapReduceError(const char *msg) : _msg(msg) {
    }

    MapReduceError(const std::string &msg) : _msg(msg) {
    }

    const std::string& getMsg() const {
        return _msg;
    }

private:
    const std::string _msg;
};


// Some information about STL allocators:
// http://www.cplusplus.com/reference/std/memory/allocator/
//
template<class T> class NifStlAllocator {
public:
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    typedef T*        pointer;
    typedef const T*  const_pointer;
    typedef T&        reference;
    typedef const T&  const_reference;
    typedef T         value_type;

    NifStlAllocator() {}
    NifStlAllocator(const NifStlAllocator&) {}

    pointer allocate(size_type n, const void * = 0) {
        if (n > this->max_size()) {
            throw std::bad_alloc();
        }
        pointer t = static_cast<pointer>(enif_alloc(n * sizeof(value_type)));
        if (!t) {
            throw std::bad_alloc();
        }
        return t;
    }

    void deallocate(void *p, size_type) {
        if (p) {
            enif_free(p);
        }
    }

    pointer address(reference x) const {
        return &x;
    }

    const_pointer address(const_reference x) const {
        return &x;
    }

    void construct(pointer p, const_reference val) {
        new (p) value_type(val);
    }

    void destroy(pointer p) {
        p->~T();
    }

    size_type max_size() const {
        return size_t(-1) / sizeof(value_type);
    }

    template <class U>
    struct rebind {
        typedef NifStlAllocator<U> other;
    };

    template <class U>
    NifStlAllocator(const NifStlAllocator<U>&) {}

    template <class U>
    NifStlAllocator& operator=(const NifStlAllocator<U>&) {
        return *this;
    }
};

template<typename T>
inline bool operator==(const NifStlAllocator<T>&, const NifStlAllocator<T>&) {
    return true;
}

template<typename T>
inline bool operator!=(const NifStlAllocator<T>&, const NifStlAllocator<T>&) {
    return false;
}


#endif
