//  PARStore
//  Author: Charles Parnot
//  Licensed under the terms of the BSD License, as specified in the file 'LICENSE-BSD.txt' included with this distribution

#import <Foundation/Foundation.h>


#ifndef FDDISPATCHQUEUE_HEADER
#define FDDISPATCHQUEUE_HEADER

typedef void (^PARDispatchBlock)(void);

// Timer Behaviors
// PARTimerBehaviorCoalesce: subsequent calls can only reduce the time until firing, not extend
// PARTimerBehaviorDelay:    subsequent calls replace the existing time, potentially extending it
// PARTimerBehaviorThrottle: subsequent calls can only fire after the elapsed time, potentially immediately
typedef NS_ENUM(NSInteger, PARTimerBehavior)
{
    PARTimerBehaviorCoalesce,
    PARTimerBehaviorDelay,
    PARTimerBehaviorThrottle,
};


// Synchronous Dispatch Behaviors = what to do when dispatching synchronously a block and we are already within the queue
// PARDeadlockBehaviorExecute: do not add the block to the queue, execute inline (default)
// PARDeadlockBehaviorSkip:    do not add the block to the queue, drop it silently
// PARDeadlockBehaviorLog:     do not add the block to the queue, log to console
// PARDeadlockBehaviorAssert:  do not add the block to the queue, raise an exception
// PARDeadlockBehaviorBlock:   add the block to the queue, and be damned
typedef NS_ENUM(NSInteger, PARDeadlockBehavior)
{
    PARDeadlockBehaviorExecute,
    PARDeadlockBehaviorSkip,
    PARDeadlockBehaviorLog,
    PARDeadlockBehaviorAssert,
    PARDeadlockBehaviorBlock
};


// The APIs were audited. None of the method return values, method parameters or properties are nullable.
NS_ASSUME_NONNULL_BEGIN


@interface PARDispatchQueue : NSObject


/// @name Creating Queues
+ (PARDispatchQueue *)globalDispatchQueue;
+ (PARDispatchQueue *)mainDispatchQueue;
+ (PARDispatchQueue *)dispatchQueueWithLabel:(NSString *)label;
+ (PARDispatchQueue *)dispatchQueueWithLabel:(NSString *)label behavior:(PARDeadlockBehavior)behavior;

// queue created lazily, then shared and guaranteed to be always the same
// this is useful as an alternative to `globalDispatchQueue` to dispatch barrier blocks
+ (PARDispatchQueue *)sharedConcurrentQueue;


/// @name Properties
@property (readonly, copy) NSString *label;
@property (readonly) PARDeadlockBehavior deadlockBehavior;


/// @name Utilities
+ (NSString *)labelByPrependingBundleIdentifierToString:(NSString *)suffix;


/// @name Dispatching Blocks

- (void)dispatchSynchronously:(PARDispatchBlock)block;
- (void)dispatchAsynchronously:(PARDispatchBlock)block;
- (void)dispatchBarrierSynchronously:(PARDispatchBlock)block;
- (void)dispatchBarrierAsynchronously:(PARDispatchBlock)block;

// applicable only for serial queues, with one caveat for the main queue: all blocks in the stack should be dispatched using PARDispatchQueue `dispatchXXX:` calls
- (BOOL)isCurrentQueue;
- (BOOL)isInCurrentQueueStack;


/// @name Adding and Updating Timers
- (void)scheduleTimerWithName:(NSString *)name timeInterval:(NSTimeInterval)delay behavior:(PARTimerBehavior)behavior block:(PARDispatchBlock)block;
- (void)cancelTimerWithName:(NSString *)name;
- (void)cancelAllTimers;
- (NSUInteger)timerCount; // the returned value cannot be fully trusted, of course

@end


@interface PARBlockOperation : NSObject
+ (PARBlockOperation *)dispatchedOperationWithQueue:(PARDispatchQueue *)queue block:(PARDispatchBlock)block;
- (void)waitUntilFinished;
@end


NS_ASSUME_NONNULL_END

#endif
