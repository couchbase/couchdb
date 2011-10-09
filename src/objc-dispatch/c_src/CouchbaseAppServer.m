//
//  CouchbaseAppServer.m
//  iErl14
//
//  Created by Jens Alfke on 10/6/11.
//  Copyright (c) 2011 Couchbase, Inc. All rights reserved.
//

#import "CouchbaseAppServer.h"
#import "term_to_objc.h"


@interface CouchbaseValidationContext : NSObject <CouchbaseValidationContext>
{
    @private
    ErlNifEnv* _env;
    ERL_NIF_TERM _currentRevisionTerm, _contextTerm, _securityTerm;
    NSDictionary* _currentRevision;
    NSDictionary* _contextDict;
    NSDictionary* _security;
    NSString* _errorType, *_errorMessage;
}
- (id) initWithEnv: (ErlNifEnv*)env
   currentRevision: (ERL_NIF_TERM)currentRevision
           context: (ERL_NIF_TERM)context
          security: (ERL_NIF_TERM)security;
@end


@implementation CouchbaseAppServer

- (id) initWithDesignDoc: (NSDictionary*)designDoc {
    self = [super init];
    if (self) {
        NSParameterAssert(designDoc);
        _designDoc = [designDoc copy];
    }
    return self;
}

- (void)dealloc {
    [_validateUpdateBlock release];
    [_designDoc release];
    [super dealloc];
}


@synthesize env = _env;


- (BOOL) validateUpdate: (ERL_NIF_TERM)documentTerm
             ofDocument: (ERL_NIF_TERM)currentRevisionTerm
                context: (ERL_NIF_TERM)contextTerm
               security: (ERL_NIF_TERM)securityTerm
                  error: (NSDictionary**)outError
{
    if (!_validateUpdateBlock) {
        NSString* key = [_designDoc objectForKey: @"validate_doc_update"];
        if ([key isKindOfClass: [NSString class]])
            _validateUpdateBlock = [[[CouchbaseCallbacks sharedInstance]
                                        validateUpdateBlockForKey: key] copy];

        if (!_validateUpdateBlock)
            [NSException raise: NSInternalInconsistencyException
                        format: @"Unregistered native validate_doc_update function '%@'", key];
    }

    NSDictionary* document = term_to_nsdict(_env, documentTerm);
    if (!document)
        return NO;

    CouchbaseValidationContext* context = [[CouchbaseValidationContext alloc]
                                                              initWithEnv: _env
                                                          currentRevision: currentRevisionTerm
                                                                  context: contextTerm
                                                                 security: securityTerm];
    [context autorelease];

    if (_validateUpdateBlock(document, context))
        return YES;

    *outError = [NSDictionary dictionaryWithObject: context.errorMessage forKey: context.errorType];
    NSLog(@"Doc failed validation with error {%@: %@}", context.errorType, context.errorMessage);
    return NO;
}

@end



@implementation CouchbaseValidationContext


- (id) initWithEnv: (ErlNifEnv*)env
   currentRevision: (ERL_NIF_TERM)currentRevision
           context: (ERL_NIF_TERM)context
          security: (ERL_NIF_TERM)security
{
    self = [super init];
    if (self) {
        NSParameterAssert(env);
        _env = env;
        _currentRevisionTerm = currentRevision;
        _contextTerm = context;
        _securityTerm = security;
        _errorType = [@"forbidden" retain];
        _errorMessage = [@"invalid document" retain];
    }
    return self;
}

- (void)dealloc {
    [_errorType release];
    [_errorMessage release];
    [_currentRevision release];
    [_contextDict release];
    [_security release];
    [super dealloc];
}

- (NSDictionary*) currentRevision {
    if (!_currentRevision)
        _currentRevision = [term_to_nsdict(_env, _currentRevisionTerm) retain];
    return _currentRevision;
}

- (NSDictionary*) contextDict {
    if (!_contextDict)
        _contextDict = [term_to_nsdict(_env, _contextTerm) retain];
    return _contextDict;
}

- (NSString*) databaseName {
    return [self.contextDict objectForKey: @"db"];
}

- (NSString*) userName {
    id user = [self.contextDict objectForKey: @"name"];
    if (![user isKindOfClass: [NSString class]])    // anonymous user will be a NSNull object
        return nil;
    return user;
}

- (BOOL) isAdmin {
    return [[self.contextDict objectForKey: @"roles"] containsObject: @"_admin"];
}

- (NSDictionary*) security {
    if (!_security)
        _security = [term_to_nsdict(_env, _securityTerm) retain];
    return _security;
}

@synthesize errorType = _errorType, errorMessage = _errorMessage;

@end
