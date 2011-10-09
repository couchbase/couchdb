//
//  CouchbaseViewDispatcher.m
//  iErl14
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#import "CouchbaseViewDispatcher.h"
#import "CouchbaseCallbacks.h"
#import <objc/message.h>
#import "erl_nif.h"


@interface CouchbaseViewDispatcher ()
@property (nonatomic, copy) NSArray *mapBlocks;
@property (nonatomic, copy) NSArray *reduceBlocks;
@end


@implementation CouchbaseViewDispatcher


@synthesize queue = _queue, mapBlocks = _mapBlocks, reduceBlocks = _reduceBlocks;

- (void)dealloc
{
    dispatch_release(_queue);
    [_mapBlocks release];
    [_reduceBlocks release];
    [super dealloc];
}


- (id)initWithQueueName:(NSString *)name
                mapKeys:(NSArray *)mapKeys
             reduceKeys:(NSArray *)reduceKeys
{
    if (!(self = [super init]))
        return nil;

    if (!name || !mapKeys) {
        [self release];
        return nil;
    }

    _queue = dispatch_queue_create([name UTF8String], NULL);

    CouchbaseCallbacks* callbacks = [CouchbaseCallbacks sharedInstance];

    NSMutableArray *mapBlocksAcc = [NSMutableArray arrayWithCapacity:[mapKeys count]];
    for (NSString* mapKey in mapKeys) {
        CouchMapBlock map = [callbacks mapBlockForKey: mapKey];
        if (!map)
            [NSException raise:NSInvalidArgumentException
                        format:@"Unregistered native map function '%@'", mapKey];
        [mapBlocksAcc addObject: map];
    }

    NSMutableArray *reduceBlocksAcc = [NSMutableArray arrayWithCapacity:[reduceKeys count]];
    for (NSString* reduceKey in reduceKeys) {
        CouchReduceBlock reduce = [callbacks reduceBlockForKey: reduceKey];
        if (!reduce)
            [NSException raise:NSInvalidArgumentException
                        format:@"Unregistered native reduce function '%@'", reduceKey];
        [reduceBlocksAcc addObject: reduce];
    }

    self.mapBlocks = mapBlocksAcc;
    self.reduceBlocks = reduceBlocksAcc;
    return self;
}


- (NSArray *)mapDocument:(NSDictionary *)jsonDoc
{
    NSMutableArray* result = [NSMutableArray arrayWithCapacity:[_mapBlocks count]];
    for (CouchMapBlock map in _mapBlocks) {
        NSMutableArray *emittedPairs = [NSMutableArray array];
        CouchEmitBlock emit = ^(id key, id value) {
            if (!key) key = [NSNull null];
            if (!value) value = [NSNull null];
            [emittedPairs addObject: [NSArray arrayWithObjects: key, value, nil]];
        };

        map(jsonDoc, emit);

        [result addObject: emittedPairs];
    }
    //NSLog(@"CouchbaseViewDispatcher: result of map = %@", result);
    return result;
}


- (NSArray *)reduceKeys:(NSArray *)jsonKeys
                 values:(NSArray *)jsonValues
                  again:(BOOL)isRereduce
 withFunctionsAtIndexes:(NSIndexSet *)reduceFunctionIndexes
{
    NSMutableArray *result = [NSMutableArray arrayWithCapacity:[reduceFunctionIndexes count]];
    [_reduceBlocks enumerateObjectsAtIndexes:reduceFunctionIndexes
                                     options:0
                                  usingBlock:^(id reduce, NSUInteger idx, BOOL *stop) {
        id docResult = ((CouchReduceBlock)reduce)(jsonKeys, jsonValues, isRereduce);
        if (!docResult)
            [NSException raise:NSInvalidArgumentException format:@"Reduce function returned nil"];
        [result addObject:docResult];
    }];
    //NSLog(@"CouchbaseViewDispatcher: result of reduce = %@", result);
    return result;
}


@end
