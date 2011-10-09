//
//  CouchbaseAppServer.h
//  iErl14
//
//  Created by Jens Alfke on 10/6/11.
//  Copyright (c) 2011 Couchbase, Inc. All rights reserved.
//

#import <Foundation/Foundation.h>
#import "CouchbaseCallbacks.h"
#include "erl_nif.h"

@interface CouchbaseAppServer : NSObject
{
    ErlNifEnv* _env;
    NSDictionary* _designDoc;
    CouchValidateUpdateBlock _validateUpdateBlock;
}

- (id) initWithDesignDoc: (NSDictionary*)designDoc;

@property ErlNifEnv* env;

- (BOOL) validateUpdate: (ERL_NIF_TERM)updatedDoc
             ofDocument: (ERL_NIF_TERM)currentRevision
                context: (ERL_NIF_TERM)context
               security: (ERL_NIF_TERM)security
                  error: (NSDictionary**)outError;

@end
