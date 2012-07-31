/**
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

#include "couch_ejson_compare.h"


__inline void reserve_coll(couch_ejson_ctx_t *ctx)
{
    if (ctx->coll == NULL) {
        enif_mutex_lock(ctx->globalCtx->collMutex);
        assert(ctx->globalCtx->collStackTop < ctx->globalCtx->numCollators);
        ctx->coll = ctx->globalCtx->collators[ctx->globalCtx->collStackTop];
        ctx->globalCtx->collStackTop += 1;
        enif_mutex_unlock(ctx->globalCtx->collMutex);
    }
}


__inline void release_coll(couch_ejson_ctx_t *ctx)
{
    if (ctx->coll != NULL) {
        enif_mutex_lock(ctx->globalCtx->collMutex);
        ctx->globalCtx->collStackTop -= 1;
        assert(ctx->globalCtx->collStackTop >= 0);
        enif_mutex_unlock(ctx->globalCtx->collMutex);
    }
}

