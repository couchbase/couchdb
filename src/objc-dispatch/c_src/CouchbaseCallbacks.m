//
//  CouchbaseCallbacks.m
//  iErl14
//
//  Created by Jens Alfke on 10/3/11.
//  Copyright (c) 2011 Couchbase, Inc. All rights reserved.
//

#import "CouchbaseCallbacks.h"


typedef enum {
    kMapBlock,
    kReduceBlock,
    kValidateUpdateBlock,
    // add new ones here
    kNumBlockTypes
} BlockType;


@implementation CouchbaseCallbacks


+ (CouchbaseCallbacks*) sharedInstance {
    static dispatch_once_t onceToken;
    static CouchbaseCallbacks* sInstance;
    dispatch_once(&onceToken, ^{
        sInstance = [[self alloc] init];
    });
    return sInstance;
}


- (id)init {
    self = [super init];
    if (self) {
        NSMutableArray *mutableRegistries = [NSMutableArray arrayWithCapacity: kNumBlockTypes];
        for (int i=0; i<kNumBlockTypes; i++)
            [mutableRegistries addObject: [NSMutableDictionary dictionary]];
        _registries = [mutableRegistries copy];
    }
    return self;
}


- (void)dealloc {
    [_registries release];
    [super dealloc];
}


- (NSString*) generateKey {
    CFUUIDRef uuid = CFUUIDCreate(NULL);
    CFStringRef uuidStr = CFUUIDCreateString(NULL, uuid);
    CFRelease(uuid);
    return [NSMakeCollectable(uuidStr) autorelease];
}


- (void) registerBlock: (id)block ofType: (BlockType)type forKey: (NSString*)key {
    block = [block copy];
    NSMutableDictionary* registry = [_registries objectAtIndex: type];
    @synchronized(registry) {
        [registry setValue: block forKey: key];
    }
    [block release];
}

- (id) blockOfType: (BlockType)type forKey: (NSString*)key {
    NSMutableDictionary* registry = [_registries objectAtIndex: type];
    @synchronized(registry) {
        return [[[registry objectForKey: key] retain] autorelease];
    }
}


- (void) registerMapBlock: (CouchMapBlock)block forKey: (NSString*)key {
    [self registerBlock: block ofType: kMapBlock forKey: key];
}

- (CouchMapBlock) mapBlockForKey: (NSString*)key {
    return [self blockOfType: kMapBlock forKey: key];
}


- (void) registerReduceBlock: (CouchReduceBlock)block forKey: (NSString*)key {
    [self registerBlock: block ofType: kReduceBlock forKey: key];
}

- (CouchReduceBlock) reduceBlockForKey: (NSString*)key {
    return [self blockOfType: kReduceBlock forKey: key];
}


- (void) registerValidateUpdateBlock: (CouchValidateUpdateBlock)block forKey: (NSString*)key {
    [self registerBlock: block ofType: kValidateUpdateBlock forKey: key];
}

- (CouchValidateUpdateBlock) validateUpdateBlockForKey: (NSString*)key {
    return [self blockOfType: kValidateUpdateBlock forKey: key];
}


@end
