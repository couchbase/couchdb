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

#include <iostream>
#include <list>
#include <vector>
#include <v8.h>

class MapReduceError;

typedef struct json_bin_t {
    const char *data;
    size_t length;
    json_bin_t(char *bytes, size_t len) : data(bytes), length(len) { }
} json_bin_t;

typedef std::pair<json_bin_t, json_bin_t> map_result_t;

typedef struct {
    v8::Persistent<v8::Context>                  jsContext;
    v8::Isolate                                  *isolate;
    std::vector< v8::Persistent<v8::Function> >  *functions;
    std::list< map_result_t >                    *mapFunResults;
    std::string                                  *key;
    volatile int                                 taskId;
    volatile long                                taskStartTime;
} map_reduce_ctx_t;


void initContext(map_reduce_ctx_t *ctx, const std::list<std::string> &funs);
void destroyContext(map_reduce_ctx_t *ctx);
std::list< std::list< map_result_t > > mapDoc(map_reduce_ctx_t *ctx, const json_bin_t &doc, const json_bin_t &meta);
std::list<json_bin_t> runReduce(map_reduce_ctx_t *ctx, const std::list<json_bin_t> &keys, const std::list<json_bin_t> &values);
json_bin_t runReduce(map_reduce_ctx_t *ctx, int reduceFunNum, const std::list<json_bin_t> &keys, const std::list<json_bin_t> &values);
json_bin_t runRereduce(map_reduce_ctx_t *ctx, int reduceFunNum, const std::list<json_bin_t> &reductions);
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
