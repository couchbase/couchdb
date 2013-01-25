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
#include "nif_stl_allocator.h"

class MapReduceError;

typedef std::list<ErlNifBinary, NifStlAllocator<ErlNifBinary> >  json_results_list_t;
typedef std::pair<ErlNifBinary, ErlNifBinary>  kv_pair_t;
typedef std::list< kv_pair_t, NifStlAllocator< kv_pair_t > >  kv_pair_list_t;

typedef enum {
    MAP_KVS,
    MAP_ERROR
} map_result_type_t;

typedef struct {
    map_result_type_t type;
    union {
        kv_pair_list_t *kvs;
        ErlNifBinary *error;
    } result;
} map_result_t;

typedef std::list< map_result_t,
                   NifStlAllocator< map_result_t > >  map_results_list_t;

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
    kv_pair_list_t                               *kvs;
    unsigned int                                 key;
    ErlNifEnv                                    *env;
    volatile int                                 taskId;
    volatile long                                taskStartTime;
} map_reduce_ctx_t;


void initContext(map_reduce_ctx_t *ctx, const function_sources_list_t &funs);
void destroyContext(map_reduce_ctx_t *ctx);

map_results_list_t mapDoc(map_reduce_ctx_t *ctx,
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


#endif
