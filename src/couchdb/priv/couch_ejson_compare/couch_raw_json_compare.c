/**
 * @copyright 2012 Couchbase, Inc.
 *
 * @author Jens Alfke    <jens@couchbase.com>   (algorithm, original implementation)
 * @author Filipe Manana <filipe@couchbase.com> (Erlang NIF port)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <unicode/ucol.h>
#include <unicode/ucasemap.h>
#include "couch_ejson_compare.h"


#define CMP(a, b) ((a) > (b) ? 1 : ((a) < (b) ? -1 : 0))


/* Ordered by collation order. */
typedef enum {
    kEndArray,
    kEndObject,
    kComma,
    kColon,
    kNull,
    kFalse,
    kTrue,
    kNumber,
    kString,
    kArray,
    kObject,
    kIllegal
} ValueType;


static __inline ValueType valueTypeOf(char);
static int compareStringsUnicode(const char **, const char **, couch_ejson_ctx_t *);
static const char * createStringFromJSON(const char **, int *, int *, couch_ejson_ctx_t *);
static int compareUnicode(const char *, int, const char *, int, couch_ejson_ctx_t *);
static __inline char ConvertJSONEscape(const char **, couch_ejson_ctx_t *);
static __inline int digitToInt(int );


static const char *MALLOC_ERROR         = "Failed to allocate memory";
static const char *UNEXPECTED_CHARACTER = "Unexpected character in key";
static const char *ICU_ERROR            = "ICU collator error";


int less_json(const char *str1, const char *str2, couch_ejson_ctx_t *ctx)
{
    int depth = 0;

    do {
        /* Get the types of the next token in each string: */
        ValueType type1 = valueTypeOf(*str1);
        ValueType type2 = valueTypeOf(*str2);

        if (type1 == kIllegal || type2 == kIllegal) {
            ctx->error = 1;
            ctx->errorMsg = UNEXPECTED_CHARACTER;
            return -1;
        }

        /* If types don't match, stop and return their relative ordering: */
        if (type1 != type2) {
            return CMP(type1, type2);
        } else {
            /* If types match, compare the actual token values: */
            switch (type1) {
            case kNull:
            case kTrue:
                str1 += 4;
                str2 += 4;
                break;
            case kFalse:
                str1 += 5;
                str2 += 5;
                break;
            case kNumber: {
                char *next1, *next2;
                int diff = CMP( strtod(str1, &next1), strtod(str2, &next2) );
                if (diff != 0) {
                    return diff;
                }
                str1 = next1;
                str2 = next2;
                break;
            }
            case kString: {
                int diff = compareStringsUnicode(&str1, &str2, ctx);

                if (ctx->error != 0) {
                    return -1;
                }
                if (diff != 0) {
                    return diff;
                }
                break;
            }
            case kArray:
            case kObject:
                ++str1;
                ++str2;
                ++depth;
                break;
            case kEndArray:
            case kEndObject:
                ++str1;
                ++str2;
                --depth;
                break;
            case kComma:
            case kColon:
                ++str1;
                ++str2;
                break;
            case kIllegal:
                return 0;
            }
        }
    } while (depth > 0); /* Keep going as long as we're inside an array or object */
    return 0;
}


static __inline ValueType valueTypeOf(char c)
{
    switch (c) {
    case 'n':           return kNull;
    case 'f':           return kFalse;
    case 't':           return kTrue;
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
    case '-':           return kNumber;
    case '"':           return kString;
    case ']':           return kEndArray;
    case '}':           return kEndObject;
    case ',':           return kComma;
    case ':':           return kColon;
    case '[':           return kArray;
    case '{':           return kObject;
    default:
        return kIllegal;
    }
}


static int compareStringsUnicode(const char **in1, const char **in2,
                                 couch_ejson_ctx_t *ctx)
{
    int len1, len2;
    int free1 = 0, free2 = 0;
    const char *str1;
    const char *str2;
    int result = 0;

    str1 = createStringFromJSON(in1, &len1, &free1, ctx);
    if (ctx->error == 0) {
        str2 = createStringFromJSON(in2, &len2, &free2, ctx);
    }
    if (ctx->error == 0) {
        result = compareUnicode(str1, len1, str2, len2, ctx);
    }

    if (free1) {
        enif_free((void *) str1);
    }
    if (free2) {
        enif_free((void *) str2);
    }

    return result;
}


static const char * createStringFromJSON(const char **in, int *length,
                                         int *freeWhenDone,
                                         couch_ejson_ctx_t *ctx)
{
    /* Scan the JSON string to find its length and whether it contains escapes: */
    const char* start = ++*in;
    unsigned escapes = 0;
    const char* str;

    for (str = start; *str != '"'; ++str) {
        if (*str == '\\') {
            ++str;
            if (*str == 'u') {
                escapes += 5;  /* \uxxxx adds 5 bytes */
                str += 4;
            } else {
                escapes += 1;
            }
        }
    }
    *in = str + 1;
    *length = str - start;
    *freeWhenDone = 0;

    if (escapes > 0) {
        char* buf = NULL;
        char* dst = NULL;
        char c;

        *length -= escapes;
        buf = (char *) enif_alloc(*length);

        if (buf == NULL) {
            ctx->error = 1;
            ctx->errorMsg = MALLOC_ERROR;
            return NULL;
        }

        dst = buf;
        *freeWhenDone = 1;

        for (str = start; (c = *str) != '"'; ++str) {
            if (c == '\\') {
                c = ConvertJSONEscape(&str, ctx);
                if (ctx->error != 0) {
                    return buf;
                }
            }
            *dst++ = c;
        }
        assert(dst - buf == (int) *length);
        start = buf;
    }

    return start;
}


static int compareUnicode(const char *str1, int len1,
                          const char *str2, int len2,
                          couch_ejson_ctx_t *ctx)
{
    UCharIterator iterA, iterB;
    UErrorCode status = U_ZERO_ERROR;
    int result;

    reserve_coll(ctx);

    uiter_setUTF8(&iterA, str1, len1);
    uiter_setUTF8(&iterB, str2, len2);

    result = ucol_strcollIter(ctx->coll, &iterA, &iterB, &status);

    if (U_FAILURE(status)) {
        ctx->error = 1;
        ctx->errorMsg = ICU_ERROR;
        return -1;
    }

    return result;
}


static __inline char ConvertJSONEscape(const char **in, couch_ejson_ctx_t *ctx)
{
    char c = *++(*in);
    switch (c) {
    case 'u': {
        int uc;
        /* \u is a Unicode escape; 4 hex digits follow. */
        const char* digits = *in + 1;
        *in += 4;
        uc = (digitToInt(digits[0]) << 12) | (digitToInt(digits[1]) << 8) |
            (digitToInt(digits[2]) <<  4) | (digitToInt(digits[3]));
        if (uc > 127) {
            ctx->error = 1;
            ctx->errorMsg = UNEXPECTED_CHARACTER;
        }
        return (char) uc;
    }
    case 'b':   return '\b';
    case 'n':   return '\n';
    case 'r':   return '\r';
    case 't':   return '\t';
    default:    return c;
    }
}


static __inline int digitToInt(int c)
{
    if (isdigit(c)) {
        return c - '0';
    } else if (isxdigit(c)) {
        return 10 + tolower(c) - 'a';
    } else {
        return 0;
    }
}
