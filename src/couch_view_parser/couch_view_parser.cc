/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <algorithm>
#include <iostream>
#include <platform/cbassert.h>
#include <string.h>

#include "couch_view_parser.h"

#define ROW_KEY_RESERVE_BYTES   40
#define ROW_VALUE_RESERVE_BYTES 128
#define ROW_DOC_RESERVE_BYTES   512


#define KEY_IS(key, len, desiredKey) \
    ((len == (sizeof(desiredKey) - 1)) && \
     (strncmp(reinterpret_cast<const char *>(key), desiredKey, (sizeof(desiredKey) - 1)) == 0))


/**
 *   Top level fields in a view response:
 *
 *   "debug_info", optional, must come before "total_rows" and its value is an object
 *       each key in the "debug_info" object can have any JSON value and this corresponds
 *       to debug information from one node
 *   "total_rows", field must come before any other field, and it's absent for reduce views
 *   "rows" must come before "debug_info" (if present) or before "total_rows" (if present)
 *       each row has the following fields:
 *           "id"        - must be a string, mandatory only for map views
 *           "key"       - can be any JSON value, mandatory
 *           "value"     - can be any JSON value, mandatory
 *           "partition" - optional, if present must be an integer
 *           "node"      - optional, if present must be a string
 *           "doc"       - optional, if present must be an object or null
 *           "error"     - optional, if present must be a string
 *   "errors" must come after "rows", its value is an array and it's an optional field
 *       each entry in "errors" is an object with the following fields:
 *           "from"      - must be a string, mandatory
 *           "reason"    - must be a string, mandatory
 **/


static const char *BAD_DEBUG_INFO_VALUE    = "'debug_info' value is not an object";
static const char *BAD_TOTAL_ROWS_VALUE    = "'total_rows' value is not a number";
static const char *BAD_ROWS_VALUE          = "'rows' value is not an array";
static const char *BAD_ROW_FIELD           = "unsupported field in row object: ";
static const char *BAD_ROW_ID_VALUE        = "row 'id' value is not a string";
static const char *BAD_ROW_PARTITION_VALUE = "row 'partition' value is not an integer";
static const char *BAD_ROW_NODE_VALUE      = "row 'node' value is not a string";
static const char *BAD_ROW_DOC_VALUE       = "row 'doc' value is not an object nor null";
static const char *BAD_ROW_ERROR_VALUE     = "row 'error' value is not a string";
static const char *BAD_ERRORS_VALUE        = "'errors' value is not an array";
static const char *BAD_ERROR_FROM_VALUE    = "error 'from' value is not a string";
static const char *BAD_ERROR_REASON_VALUE  = "error 'reason' value is not a string";
static const char *BAD_ERROR_FIELD         = "unsupported field in error object: ";


// YAJL parser callbacks
static int null_callback(void *ctx);
static int boolean_callback(void *ctx, int boolean);
static int number_callback(void *ctx, const char *number, size_t len);
static int string_callback(void *ctx, const unsigned char *str, size_t len);
static int start_object_callback(void *ctx);
static int end_object_callback(void *ctx);
static int object_key_callback(void *ctx, const unsigned char *key, size_t len);
static int start_array_callback(void *ctx);
static int end_array_callback(void *ctx);


// memory allocators for YAJL
static void * yajl_internal_malloc(void *ctx, size_t sz);
static void * yajl_internal_realloc(void *ctx, void *previous, size_t sz);
static void yajl_internal_free(void *ctx, void *ptr);


// Helpers to collect a raw JSON value into a string buffer
static inline void add_null(ctx_t *context, parser_string_t &buffer);
static inline void add_boolean(ctx_t *context, parser_string_t &buffer, int boolean);
static inline void add_number(ctx_t *context, parser_string_t &buffer, const char *number, size_t len);
static inline void add_string(ctx_t *context, parser_string_t &buffer, const unsigned char *str, size_t len);
static inline void add_object_start(ctx_t *context, parser_string_t &buffer);
static inline void add_object_end(ctx_t *context, parser_string_t &buffer);
static inline void add_object_key(ctx_t *context, parser_string_t &buffer, const unsigned char *key, size_t len);
static inline void add_array_start(ctx_t *context, parser_string_t &buffer);
static inline void add_array_end(ctx_t *context, parser_string_t &buffer);

// Helpers
static inline void maybe_debug_entry_end(ctx_t *context);
static inline void maybe_expand_buffer(parser_string_t &buffer, size_t appendLen);
static inline void alloc_error(ctx_t *context, const char *msg);


static yajl_callbacks callbacks = {
    null_callback,
    boolean_callback,
    NULL,
    NULL,
    number_callback,
    string_callback,
    start_object_callback,
    object_key_callback,
    end_object_callback,
    start_array_callback,
    end_array_callback
};

static yajl_alloc_funcs allocfuncs = {
    yajl_internal_malloc,
    yajl_internal_realloc,
    yajl_internal_free,
    nullptr
};


void initContext(ctx_t *context)
{
    context->handle = yajl_alloc(&callbacks, &allocfuncs, (void *) context);
    yajl_config(context->handle, yajl_dont_validate_strings, 1);
    yajl_config(context->handle, yajl_dont_unescape_strings, 1);

    context->level = 0;
    context->row_count = NULL;

    context->rows = (row_list_t *) enif_alloc(sizeof(row_list_t));
    if (context->rows == NULL) {
        throw std::bad_alloc();
    }
    context->rows = new (context->rows) row_list_t();

    context->error_entries = (error_entry_list_t *) enif_alloc(sizeof(error_entry_list_t));
    if (context->error_entries == NULL) {
        throw std::bad_alloc();
    }
    context->error_entries = new (context->error_entries) error_entry_list_t();

    context->debug_infos = (debug_info_list_t *) enif_alloc(sizeof(debug_info_list_t));
    if (context->debug_infos == NULL) {
        throw std::bad_alloc();
    }
    context->debug_infos = new (context->debug_infos) debug_info_list_t();

    context->error = NULL;
    context->caller_state = debug_infos;
    context->parser_state = parser_starting;
    context->parser_sub_state = parser_find_row_count_key;
    context->tmp_row = NULL;
    context->tmp_error_entry = NULL;
    context->tmp_debug_info = NULL;
}


void destroyContext(ctx_t *context)
{
    yajl_complete_parse(context->handle);
    yajl_free(context->handle);

    enif_free(context->row_count);
    if (context->error != NULL) {
        using namespace std;
        context->error->~string();
        enif_free(context->error);
    }
    if (context->tmp_row != NULL) {
        context->tmp_row->~row_t();
        enif_free(context->tmp_row);
    }
    if (context->tmp_error_entry != NULL) {
        context->tmp_error_entry->~error_entry_t();
        enif_free(context->tmp_error_entry);
    }
    if (context->tmp_debug_info != NULL) {
        context->tmp_debug_info->~debug_info_t();
        enif_free(context->tmp_debug_info);
    }

    for (row_list_t::iterator it = context->rows->begin();
         it != context->rows->end(); ++it) {
        (*it)->~row_t();
        enif_free(*it);
    }
    context->rows->~row_list_t();
    enif_free(context->rows);

    for (error_entry_list_t::iterator it = context->error_entries->begin();
         it != context->error_entries->end(); ++it) {
        (*it)->~error_entry_t();
        enif_free(*it);
    }
    context->error_entries->~error_entry_list_t();
    enif_free(context->error_entries);

    for (debug_info_list_t::iterator it = context->debug_infos->begin();
         it != context->debug_infos->end(); ++it) {
        (*it)->~debug_info_t();
        enif_free(*it);
    }
    context->debug_infos->~debug_info_list_t();
    enif_free(context->debug_infos);
}


void parseJsonChunk(ctx_t *context, unsigned char *data, size_t len)
{
    yajl_status status = yajl_parse(context->handle, data, len);

    if (status != yajl_status_ok) {
        yajl_complete_parse(context->handle);

        if (context->error == NULL) {
            unsigned char *buf = yajl_get_error(context->handle, 0, data, len);
            alloc_error(context, reinterpret_cast<char *>(buf));
            yajl_free_error(context->handle, buf);
        }

        throw JsonParseException(*context->error);
    }
}


static int null_callback(void *ctx)
{
    ctx_t *context = static_cast<ctx_t *>(ctx);

    switch (context->parser_sub_state) {
    case parser_found_debug_info_key:
        alloc_error(context, BAD_DEBUG_INFO_VALUE);
        return 0;
    case parser_get_debug_entry:
        add_null(context, context->tmp_debug_info->value);
        maybe_debug_entry_end(context);
        return 1;
    case parser_found_row_count_key:
        alloc_error(context, BAD_TOTAL_ROWS_VALUE);
        return 0;
    case parser_found_rows_key:
        alloc_error(context, BAD_ROWS_VALUE);
        return 0;
    case parser_get_row_id:
        alloc_error(context, BAD_ROW_ID_VALUE);
        return 0;
    case parser_get_row_key:
        add_null(context, context->tmp_row->key);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_value:
        add_null(context, context->tmp_row->value);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_doc:
        add_null(context, context->tmp_row->doc);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_partition:
        alloc_error(context, BAD_ROW_PARTITION_VALUE);
        return 0;
    case parser_get_row_node:
        alloc_error(context, BAD_ROW_NODE_VALUE);
        return 0;
    case parser_get_row_error:
        alloc_error(context, BAD_ROW_ERROR_VALUE);
        return 0;
    case parser_found_errors_key:
        alloc_error(context, BAD_ERRORS_VALUE);
        return 0;
    case parser_get_error_from:
        alloc_error(context, BAD_ERROR_FROM_VALUE);
        return 0;
    case parser_get_error_reason:
        alloc_error(context, BAD_ERROR_REASON_VALUE);
        return 0;
    default:
        break;
    }

    return 1;
}


static int boolean_callback(void *ctx, int boolean)
{
    ctx_t *context = static_cast<ctx_t *>(ctx);

    switch (context->parser_sub_state) {
    case parser_found_debug_info_key:
        alloc_error(context, BAD_DEBUG_INFO_VALUE);
        return 0;
    case parser_get_debug_entry:
        add_boolean(context, context->tmp_debug_info->value, boolean);
        maybe_debug_entry_end(context);
        return 1;
    case parser_found_row_count_key:
        alloc_error(context, BAD_TOTAL_ROWS_VALUE);
        return 0;
    case parser_found_rows_key:
        alloc_error(context, BAD_ROWS_VALUE);
        return 0;
    case parser_get_row_id:
        alloc_error(context, BAD_ROW_ID_VALUE);
        return 0;
    case parser_get_row_key:
        add_boolean(context, context->tmp_row->key, boolean);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_value:
        add_boolean(context, context->tmp_row->value, boolean);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_doc:
        if (context->value_nesting == 0) {
            alloc_error(context, BAD_ROW_DOC_VALUE);
            return 0;
        } else {
            add_boolean(context, context->tmp_row->doc, boolean);
        }
        return 1;
    case parser_get_row_partition:
        alloc_error(context, BAD_ROW_PARTITION_VALUE);
        return 0;
    case parser_get_row_node:
        alloc_error(context, BAD_ROW_NODE_VALUE);
        return 0;
    case parser_get_row_error:
        alloc_error(context, BAD_ROW_ERROR_VALUE);
        return 0;
    case parser_found_errors_key:
        alloc_error(context, BAD_ERRORS_VALUE);
        return 0;
    case parser_get_error_from:
        alloc_error(context, BAD_ERROR_FROM_VALUE);
        return 0;
    case parser_get_error_reason:
        alloc_error(context, BAD_ERROR_REASON_VALUE);
        return 0;
    default:
        break;
    }

    return 1;
}


static int number_callback(void *ctx, const char *number, size_t len)
{
    ctx_t *context = static_cast<ctx_t *>(ctx);

    switch (context->parser_sub_state) {
    case parser_found_debug_info_key:
        alloc_error(context, BAD_DEBUG_INFO_VALUE);
        return 0;
    case parser_get_debug_entry:
        add_number(context, context->tmp_debug_info->value, number, len);
        maybe_debug_entry_end(context);
        return 1;
    case parser_found_row_count_key:
        context->row_count = (char *) enif_alloc(len + 1);
        if (context->row_count == NULL) {
            throw std::bad_alloc();
        }
        memcpy(context->row_count, number, len);
        context->row_count[len] = '\0';
        context->parser_state = parser_rows;
        context->parser_sub_state = parser_find_rows_key;
        return 1;
    case parser_found_rows_key:
        alloc_error(context, BAD_ROWS_VALUE);
        return 0;
    case parser_get_row_id:
        alloc_error(context, BAD_ROW_ID_VALUE);
        return 0;
    case parser_get_row_key:
        add_number(context, context->tmp_row->key, number, len);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_value:
        add_number(context, context->tmp_row->value, number, len);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_doc:
        if (context->value_nesting == 0) {
            alloc_error(context, BAD_ROW_DOC_VALUE);
            return 0;
        } else {
            add_number(context, context->tmp_row->doc, number, len);
        }
        return 1;
    case parser_get_row_partition:
        add_number(context, context->tmp_row->partition, number, len);
        context->parser_sub_state = parser_get_row;
        return 1;
    case parser_get_row_node:
        alloc_error(context, BAD_ROW_NODE_VALUE);
        return 0;
    case parser_get_row_error:
        alloc_error(context, BAD_ROW_ERROR_VALUE);
        return 0;
    case parser_found_errors_key:
        alloc_error(context, BAD_ERRORS_VALUE);
        return 0;
    case parser_get_error_from:
        alloc_error(context, BAD_ERROR_FROM_VALUE);
        return 0;
    case parser_get_error_reason:
        alloc_error(context, BAD_ERROR_REASON_VALUE);
        return 0;
    default:
        break;
    }

    return 1;
}


static int string_callback(void *ctx, const unsigned char *str, size_t len)
{
    ctx_t *context = static_cast<ctx_t *>(ctx);

    switch (context->parser_sub_state) {
    case parser_found_debug_info_key:
        alloc_error(context, BAD_DEBUG_INFO_VALUE);
        return 0;
    case parser_get_debug_entry:
        add_string(context, context->tmp_debug_info->value, str, len);
        maybe_debug_entry_end(context);
        return 1;
    case parser_found_row_count_key:
        alloc_error(context, BAD_TOTAL_ROWS_VALUE);
        return 0;
    case parser_found_rows_key:
        alloc_error(context, BAD_ROWS_VALUE);
        return 0;
    case parser_get_row_id:
        add_string(context, context->tmp_row->id, str, len);
        context->parser_sub_state = parser_get_row;
        return 1;
    case parser_get_row_key:
        add_string(context, context->tmp_row->key, str, len);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_value:
        add_string(context, context->tmp_row->value, str, len);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_doc:
        if (context->value_nesting == 0) {
            alloc_error(context, BAD_ROW_DOC_VALUE);
            return 0;
        } else {
            add_string(context, context->tmp_row->doc, str, len);
            return 1;
        }
    case parser_get_row_partition:
        alloc_error(context, BAD_ROW_PARTITION_VALUE);
        return 0;
    case parser_get_row_node:
        add_string(context, context->tmp_row->node, str, len);
        context->parser_sub_state = parser_get_row;
        return 1;
    case parser_get_row_error:
        add_string(context, context->tmp_row->error, str, len);
        context->parser_sub_state = parser_get_row;
        return 1;
    case parser_found_errors_key:
        alloc_error(context, BAD_ERRORS_VALUE);
        return 0;
    case parser_get_error_from:
        add_string(context, context->tmp_error_entry->from, str, len);
        context->parser_sub_state = parser_get_error_entry;
        return 1;
    case parser_get_error_reason:
        add_string(context, context->tmp_error_entry->reason, str, len);
        context->parser_sub_state = parser_get_error_entry;
        return 1;
    default:
        break;
    }

    return 1;
}


static int start_object_callback(void *ctx)
{
    ctx_t *context = static_cast<ctx_t *>(ctx);

    switch (context->parser_sub_state) {
    case parser_found_debug_info_key:
        context->parser_sub_state = parser_get_debug_infos;
        break;
    case parser_get_debug_entry:
        add_object_start(context, context->tmp_debug_info->value);
        return 1;
    case parser_found_row_count_key:
        alloc_error(context, BAD_TOTAL_ROWS_VALUE);
        return 0;
    case parser_found_rows_key:
        alloc_error(context, BAD_ROWS_VALUE);
        return 0;
    case parser_get_row_id:
        alloc_error(context, BAD_ROW_ID_VALUE);
        return 0;
    case parser_get_row_key:
        add_object_start(context, context->tmp_row->key);
        return 1;
    case parser_get_row_value:
        add_object_start(context, context->tmp_row->value);
        return 1;
    case parser_get_row_doc:
        add_object_start(context, context->tmp_row->doc);
        return 1;
    case parser_get_row_partition:
        alloc_error(context, BAD_ROW_PARTITION_VALUE);
        return 0;
    case parser_get_row_node:
        alloc_error(context, BAD_ROW_NODE_VALUE);
        return 0;
    case parser_get_row_error:
        alloc_error(context, BAD_ROW_ERROR_VALUE);
        return 0;
    case parser_found_errors_key:
        alloc_error(context, BAD_ERRORS_VALUE);
        return 0;
    case parser_get_error_from:
        alloc_error(context, BAD_ERROR_FROM_VALUE);
        return 0;
    case parser_get_error_reason:
        alloc_error(context, BAD_ERROR_REASON_VALUE);
        return 0;
    default:
        break;
    }

    ++context->level;

    if (context->level == 1) {
        cb_assert(context->parser_state == parser_starting);
        context->parser_state = parser_debug_info;
        context->parser_sub_state = parser_find_debug_info_key;
    } else if (context->level == 2) {
        if (context->parser_state == parser_rows &&
            context->parser_sub_state == parser_get_row) {
            // starting to parse a row
            context->tmp_row = (row_t *) enif_alloc(sizeof(row_t));
            if (context->tmp_row == NULL) {
                throw std::bad_alloc();
            }
            context->tmp_row = new (context->tmp_row) row_t();
        } else if (context->parser_state == parser_errors &&
                   context->parser_sub_state == parser_get_error_entry) {
            // starting to parse an error entry
            context->tmp_error_entry = (error_entry_t *) enif_alloc(sizeof(error_entry_t));
            if (context->tmp_error_entry == NULL) {
                throw std::bad_alloc();
            }
            context->tmp_error_entry = new (context->tmp_error_entry) error_entry_t();
        }
    }

    return 1;
}


static int end_object_callback(void *ctx)
{
    ctx_t *context = static_cast<ctx_t *>(ctx);

    switch (context->parser_sub_state) {
    case parser_get_row_key:
        add_object_end(context, context->tmp_row->key);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_debug_entry:
        add_object_end(context, context->tmp_debug_info->value);
        maybe_debug_entry_end(context);
        return 1;
    case parser_get_row_value:
        add_object_end(context, context->tmp_row->value);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_doc:
        add_object_end(context, context->tmp_row->doc);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    default:
        break;
    }

    --context->level;

    if (context->level == 0) {
        context->parser_state = parser_ending;
    } else if (context->level == 1) {
        if (context->parser_state == parser_debug_info &&
            context->parser_sub_state == parser_get_debug_infos) {
            // finished parsing the debug_info object
            context->parser_state = parser_row_count;
            context->parser_sub_state = parser_find_row_count_key;
        } else if (context->parser_state == parser_rows &&
                   context->parser_sub_state == parser_get_row) {
            // finished parsing a row
            cb_assert(context->tmp_row != NULL);
            context->rows->push_back(context->tmp_row);
            context->tmp_row = NULL;
            context->parser_sub_state = parser_get_row;
        } else if (context->parser_state == parser_errors &&
                   context->parser_sub_state == parser_get_error_entry) {
            // finished parsing an error entry
            cb_assert(context->tmp_error_entry != NULL);
            context->error_entries->push_back(context->tmp_error_entry);
            context->tmp_error_entry = NULL;
            context->parser_sub_state = parser_get_error_entry;
        }
    }

    return 1;
}


static int object_key_callback(void *ctx, const unsigned char *key, size_t len)
{
    ctx_t *context = static_cast<ctx_t *>(ctx);

    switch (context->parser_sub_state) {
    case parser_get_debug_entry:
        add_object_key(context, context->tmp_debug_info->value, key, len);
        return 1;
    case parser_get_row_key:
        add_object_key(context, context->tmp_row->key, key, len);
        return 1;
    case parser_get_row_value:
        add_object_key(context, context->tmp_row->value, key, len);
        return 1;
    case parser_get_row_doc:
        add_object_key(context, context->tmp_row->doc, key, len);
        return 1;
    default:
        break;
    }

    if (context->level == 1) {
        if (context->parser_state == parser_debug_info &&
            context->parser_sub_state == parser_find_debug_info_key) {

            if (KEY_IS(key, len, "debug_info")) {
                context->parser_sub_state = parser_found_debug_info_key;
            } else if (KEY_IS(key, len, "total_rows")) {
                context->parser_state = parser_row_count;
                context->parser_sub_state = parser_found_row_count_key;
            } else if (KEY_IS(key, len, "rows")) {
                context->parser_state = parser_rows;
                context->parser_sub_state = parser_found_rows_key;
            } else if (KEY_IS(key, len, "errors")) {
                context->parser_state = parser_errors;
                context->parser_sub_state = parser_found_errors_key;
            }
        } else if (context->parser_state == parser_row_count &&
                   context->parser_sub_state == parser_find_row_count_key) {

            if (KEY_IS(key, len, "total_rows")) {
                context->parser_sub_state = parser_found_row_count_key;
            } else if (KEY_IS(key, len, "rows")) {
                // reduce view, no "total_rows" field
                context->parser_state = parser_rows;
                context->parser_sub_state = parser_found_rows_key;
            }
        } else if (context->parser_state == parser_rows &&
                   context->parser_sub_state == parser_find_rows_key) {

            if (KEY_IS(key, len, "rows")) {
                context->parser_sub_state = parser_found_rows_key;
            } else if (KEY_IS(key, len, "errors")) {
                context->parser_state = parser_errors;
                context->parser_sub_state = parser_found_errors_key;
            }
        } else if (context->parser_state == parser_errors &&
                   context->parser_sub_state == parser_find_errors_key) {

            if (KEY_IS(key, len, "errors")) {
                context->parser_sub_state = parser_found_errors_key;
            }
        }
    } else if (context->level == 2) {
        if (context->parser_state == parser_debug_info &&
            context->parser_sub_state == parser_get_debug_infos) {

            // starting to parse a debug info entry (relative to one node)
            cb_assert(context->tmp_debug_info == NULL);
            context->tmp_debug_info = (debug_info_t *) enif_alloc(sizeof(debug_info_t));
            if (context->tmp_debug_info == NULL) {
                throw std::bad_alloc();
            }
            context->tmp_debug_info = new (context->tmp_debug_info) debug_info_t();
            context->tmp_debug_info->from.reserve(len + 2);
            context->tmp_debug_info->from += "\"";
            context->tmp_debug_info->from.append(reinterpret_cast<const char *>(key), len);
            context->tmp_debug_info->from += "\"";
            context->value_nesting = 0;
            context->parser_sub_state = parser_get_debug_entry;
        }
        else if (context->parser_state == parser_rows &&
            context->parser_sub_state == parser_get_row) {

            context->value_nesting = 0;

            if (KEY_IS(key, len, "id")) {
                context->parser_sub_state = parser_get_row_id;
            } else if (KEY_IS(key, len, "key")) {
                context->tmp_row->key.reserve(ROW_KEY_RESERVE_BYTES);
                context->parser_sub_state = parser_get_row_key;
            } else if (KEY_IS(key, len, "value")) {
                context->tmp_row->value.reserve(ROW_VALUE_RESERVE_BYTES);
                context->parser_sub_state = parser_get_row_value;
            } else if (KEY_IS(key, len, "doc")) {
                context->tmp_row->doc.reserve(ROW_DOC_RESERVE_BYTES);
                context->parser_sub_state = parser_get_row_doc;
            } else if (KEY_IS(key, len, "partition")) {
                context->parser_sub_state = parser_get_row_partition;
            } else if (KEY_IS(key, len, "node")) {
                context->parser_sub_state = parser_get_row_node;
            } else if (KEY_IS(key, len, "error")) {
                context->parser_sub_state = parser_get_row_error;
            } else {
                alloc_error(context, BAD_ROW_FIELD);
                context->error->append(reinterpret_cast<const char *>(key), len);
                return 0;
            }
        } else if (context->parser_state == parser_errors &&
                   context->parser_sub_state == parser_get_error_entry) {

            context->value_nesting = 0;

            if (KEY_IS(key, len, "from")) {
                context->parser_sub_state = parser_get_error_from;
            } else if (KEY_IS(key, len, "reason")) {
                context->parser_sub_state = parser_get_error_reason;
            } else {
                alloc_error(context, BAD_ERROR_FIELD);
                context->error->append(reinterpret_cast<const char *>(key), len);
                return 0;
            }
        }
    }

    return 1;
}


static int start_array_callback(void *ctx)
{
    ctx_t *context = static_cast<ctx_t *>(ctx);

    switch (context->parser_sub_state) {
    case parser_found_debug_info_key:
        alloc_error(context, BAD_DEBUG_INFO_VALUE);
        return 0;
    case parser_get_debug_entry:
        add_array_start(context, context->tmp_debug_info->value);
        return 1;
    case parser_found_row_count_key:
        alloc_error(context, BAD_TOTAL_ROWS_VALUE);
        return 0;
    case parser_found_rows_key:
        context->parser_sub_state = parser_get_row;
        return 1;
    case parser_get_row_id:
        alloc_error(context, BAD_ROW_ID_VALUE);
        return 0;
    case parser_get_row_key:
        add_array_start(context, context->tmp_row->key);
        return 1;
    case parser_get_row_value:
        add_array_start(context, context->tmp_row->value);
        return 1;
    case parser_get_row_doc:
        if (context->value_nesting == 0) {
            alloc_error(context, BAD_ROW_DOC_VALUE);
            return 0;
        } else {
            add_array_start(context, context->tmp_row->doc);
            return 1;
        }
    case parser_get_row_partition:
        alloc_error(context, BAD_ROW_PARTITION_VALUE);
        return 0;
    case parser_get_row_node:
        alloc_error(context, BAD_ROW_NODE_VALUE);
        return 0;
    case parser_get_row_error:
        alloc_error(context, BAD_ROW_ERROR_VALUE);
        return 0;
    case parser_found_errors_key:
        context->parser_sub_state = parser_get_error_entry;
        return 1;
    case parser_get_error_from:
        alloc_error(context, BAD_ERROR_FROM_VALUE);
        return 0;
    case parser_get_error_reason:
        alloc_error(context, BAD_ERROR_REASON_VALUE);
        return 0;
    default:
        break;
    }

    return 1;
}


static int end_array_callback(void *ctx)
{
    ctx_t *context = static_cast<ctx_t *>(ctx);

    switch (context->parser_sub_state) {
    case parser_get_debug_entry:
        add_array_end(context, context->tmp_debug_info->value);
        maybe_debug_entry_end(context);
        return 1;
    case parser_get_row_key:
        add_array_end(context, context->tmp_row->key);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_value:
        add_array_end(context, context->tmp_row->value);
        if (context->value_nesting == 0) {
            context->parser_sub_state = parser_get_row;
        }
        return 1;
    case parser_get_row_doc:
        add_array_end(context, context->tmp_row->doc);
        cb_assert(context->value_nesting > 0);
        return 1;
    default:
        break;
    }

    if (context->level == 1) {
        if (context->parser_state == parser_rows &&
            context->parser_sub_state == parser_get_row) {
            // finished parsing the "rows" array
            context->parser_state = parser_errors;
            context->parser_sub_state = parser_find_errors_key;
        } else if (context->parser_state == parser_errors &&
                   context->parser_sub_state == parser_find_errors_key) {
            // finished parsing the "errors" array
            context->parser_state = parser_ending;
        }
    }

    return 1;
}


static inline void add_null(ctx_t *context, parser_string_t &buffer)
{
    maybe_expand_buffer(buffer, 5);
    if (context->value_nesting > 0) {
        buffer += "null,";
    } else {
        buffer += "null";
    }
}


static inline void add_boolean(ctx_t *context, parser_string_t &buffer, int boolean)
{
    maybe_expand_buffer(buffer, 6);
    if (context->value_nesting > 0) {
        buffer += (boolean ? "true," : "false,");
    } else {
        buffer += (boolean ? "true" : "false");
    }
}


static inline void add_number(ctx_t *context, parser_string_t &buffer, const char *number, size_t len)
{
    maybe_expand_buffer(buffer, len + 1);
    buffer.append(reinterpret_cast<const char *>(number), len);
    if (context->value_nesting > 0) {
        buffer += ',';
    }
}


static inline void add_string(ctx_t *context, parser_string_t &buffer, const unsigned char *str, size_t len)
{
    maybe_expand_buffer(buffer, len + 3);
    buffer += '"';
    buffer.append(reinterpret_cast<const char *>(str), len);
    if (context->value_nesting > 0) {
        buffer += "\",";
    } else {
        buffer += '"';
    }
}


static inline void add_object_start(ctx_t *context, parser_string_t &buffer)
{
    ++context->value_nesting;
    maybe_expand_buffer(buffer, 3);
    buffer += '{';
}


static inline void add_object_end(ctx_t *context, parser_string_t &buffer)
{
    size_t last = buffer.length() - 1;

    --context->value_nesting;

    if (buffer[last] == ',') {
        buffer[last] = '}';
    } else {
        buffer += '}';
    }

    if (context->value_nesting > 0) {
        buffer += ',';
    }
}


static inline void add_object_key(ctx_t *context, parser_string_t &buffer, const unsigned char *key, size_t len)
{
    maybe_expand_buffer(buffer, len + 3);
    buffer += '"';
    buffer.append(reinterpret_cast<const char *>(key), len);
    buffer += "\":";
}


static inline void add_array_start(ctx_t *context, parser_string_t &buffer)
{
    ++context->value_nesting;
    maybe_expand_buffer(buffer, 3);
    buffer += '[';
}


static inline void add_array_end(ctx_t *context, parser_string_t &buffer)
{
    size_t last = buffer.length() - 1;

    --context->value_nesting;

    if (buffer[last] == ',') {
        buffer[last] = ']';
    } else {
        buffer += ']';
    }

    if (context->value_nesting > 0) {
        buffer += ',';
    }
}


static inline void maybe_debug_entry_end(ctx_t *context)
{
    if (context->value_nesting == 0) {
        cb_assert(context->tmp_debug_info != NULL);
        context->debug_infos->push_back(context->tmp_debug_info);
        context->tmp_debug_info = NULL;
        context->parser_sub_state = parser_get_debug_infos;
    }
}


static inline void maybe_expand_buffer(parser_string_t &buffer, size_t appendLen)
{
    size_t len = buffer.length();
    size_t capacity = buffer.capacity();

    if ((len + appendLen) > capacity) {
        buffer.reserve(capacity + std::max(capacity, appendLen));
    }
}


static void * yajl_internal_malloc(void *ctx, size_t sz)
{
    return enif_alloc(sz);
}

static void * yajl_internal_realloc(void *ctx, void *previous, size_t sz)
{
    return enif_realloc(previous, sz);
}

static void yajl_internal_free(void *ctx, void *ptr)
{
    enif_free(ptr);
}


static inline void alloc_error(ctx_t *context, const char *msg)
{
    context->error = (std::string *) enif_alloc(sizeof(std::string));
    if (context->error == NULL) {
        throw std::bad_alloc();
    }
    context->error = new (context->error) std::string(msg);
}
