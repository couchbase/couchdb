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

#ifndef COUCH_VIEW_PARSER_H_
#define COUCH_VIEW_PARSER_H_

#include <string>
#include <list>

#include "yajl/yajl_parse.h"
#include "yajl/yajl_gen.h"

#include "erl_nif_compat.h"


typedef enum {
    debug_infos,
    row_count,
    rows,
    error_entries,
    ending
} caller_state_t;

typedef enum {
    parser_starting,
    parser_debug_info,
    parser_row_count,
    parser_rows,
    parser_errors,
    parser_ending
} parse_state_t;

typedef enum {
    // main state "parser_debug_info"
    parser_find_debug_info_key,   // looking for "debug_info" key
    parser_found_debug_info_key,  // found "debug_info" key, looking for its value
    parser_get_debug_infos,       // parser "inside" the "debug_info" object
    parser_get_debug_entry,       // parser collecting a value in the "debug_info" object
    // main state "parser_row_count"
    parser_find_row_count_key,    // looking for "total_rows" key
    parser_found_row_count_key,   // found "total_rows" key, looking for its value
    // main state "parser_rows"
    parser_find_rows_key,         // looking for "rows" key
    parser_found_rows_key,        // found "rows" key, looking for its value
    parser_get_row,               // parsing a row object (element of the array value of "rows")
    parser_get_row_id,            // parsing value of the "id" field of a row
    parser_get_row_key,           // parsing value of the "key" field of a row
    parser_get_row_value,         // parsing value of the "value" field of a row
    parser_get_row_doc,           // parsing value of the "doc" field of a row
    parser_get_row_partition,     // parsing value of the "partition" field of a row
    parser_get_row_node,          // parsing value of the "node" field of a row
    parser_get_row_error,         // parsing value of the "error" field of a row
    // main state "parser_errors"
    parser_find_errors_key,       // looking for "errors" key
    parser_found_errors_key,      // found "errors" key, looking for its value
    parser_get_error_entry,       // parsing an error entry from the "errors" array
    parser_get_error_from,        // parsing value of the "from" field of an error entry
    parser_get_error_reason       // parsing value of the "reason" field of an error entry
} parse_sub_state_t;


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
        pointer t = (pointer) enif_alloc(n * sizeof(value_type));
        if (t == NULL) {
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
        return size_t(-1);
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


typedef std::basic_string< char,
                           std::char_traits<char>,
                           NifStlAllocator<char> >  parser_string_t;

typedef struct {
    parser_string_t key;
    parser_string_t id;
    parser_string_t value;
    parser_string_t doc;
    parser_string_t partition;
    parser_string_t node;
    parser_string_t error;
} row_t;

typedef struct {
    parser_string_t from;
    parser_string_t reason;
} error_entry_t;

typedef struct {
    parser_string_t from;
    parser_string_t value;
} debug_info_t;

typedef std::list< row_t *, NifStlAllocator<row_t *> >                 row_list_t;
typedef std::list< error_entry_t *, NifStlAllocator<error_entry_t *> > error_entry_list_t;
typedef std::list< debug_info_t *, NifStlAllocator<debug_info_t *> >   debug_info_list_t;

typedef struct {
    yajl_handle                 handle;
    // current object depth level
    int                         level;
    caller_state_t              caller_state;
    parse_state_t               parser_state;
    parse_sub_state_t           parser_sub_state;
    // parse error message
    std::string                 *error;
    char                        *row_count;
    row_list_t                  *rows;
    row_t                       *tmp_row;
    error_entry_list_t          *error_entries;
    error_entry_t               *tmp_error_entry;
    debug_info_list_t           *debug_infos;
    debug_info_t                *tmp_debug_info;
    // nesting level of the object we're raw collecting
    // when > 0, we're inside an object or an array
    int                         value_nesting;
} ctx_t;


class JsonParseException {
public:
    JsonParseException(const std::string &msg) : _msg(msg) {
    }

    const std::string& getMsg() const {
        return _msg;
    }

private:
    const std::string _msg;
};


void initContext(ctx_t *context);
void destroyContext(ctx_t *context);
void parseJsonChunk(ctx_t *context, unsigned char *data, size_t len);


#endif
