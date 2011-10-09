//
//  CouchbaseViewDispatcher.h
//  iErl14

#import <Foundation/Foundation.h>

@interface CouchbaseViewDispatcher : NSObject
{
    @private
    dispatch_queue_t _queue;
    NSArray *_mapBlocks;
    NSArray *_reduceBlocks;
}

- (id)initWithQueueName:(NSString *)name
                mapKeys:(NSArray *)mapKeys
             reduceKeys:(NSArray *)reduceKeys;

@property (readonly, nonatomic, assign) dispatch_queue_t queue;

- (NSArray *)mapDocument:(NSDictionary *)jsonDoc;

- (NSArray *)reduceKeys:(NSArray *)jsonKeys
                 values:(NSArray *)jsonValues
                  again:(BOOL)isRereduce
 withFunctionsAtIndexes:(NSIndexSet *)reduceFunctionIndexes;

@end
