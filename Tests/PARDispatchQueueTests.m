//  PARDispatchQueueTests.m
//  Created by Charles Parnot on 11/2/12.
//  Copyright (c) 2012 Charles Parnot. All rights reserved.

#import <XCTest/XCTest.h>
#import "PARDispatchQueue.h"

@interface PARDispatchQueueTests : XCTestCase

@end


@implementation PARDispatchQueueTests


#pragma mark - Current Queue

- (void)testIsCurrentQueue
{
    __block BOOL isCurrentQueue = NO;
    PARDispatchQueue *queue = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    [queue dispatchSynchronously:^{ isCurrentQueue = [queue isCurrentQueue]; }];
    XCTAssertTrue(isCurrentQueue, @"isCurrentQueue should be true when called from inside a block dispatched to that queue");
}

- (void)testIsNotCurrentQueue
{
    __block BOOL isCurrentQueue = NO;
    PARDispatchQueue *queue = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    [queue dispatchSynchronously:^{ isCurrentQueue = [queue isCurrentQueue]; }];
    XCTAssertFalse([queue isCurrentQueue], @"isCurrentQueue should be false when called from outside the queue");
}

- (void)testIsNotCurrentQueueEvenIfActive
{
    PARDispatchQueue *queue = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    [queue dispatchAsynchronously:^{ [NSThread sleepForTimeInterval:10.0]; }];
    XCTAssertFalse([queue isCurrentQueue], @"isCurrentQueue should be false when called from outside the queue, even if the queue is otherwise active");
}

- (void)testIsCurrentQueueWithinBlock
{
    __block BOOL isCurrentQueue = NO;
    PARDispatchQueue *queue = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    [queue dispatchSynchronously:^{
        PARDispatchBlock block = ^{ isCurrentQueue = [queue isCurrentQueue]; };
        block();
    }];
    XCTAssertTrue(isCurrentQueue, @"isCurrentQueue should be true when called from inside a block in a block dispatched to that queue");
}

- (void)testIsCurrentQueueWithinNativeQueue
{
    __block BOOL isCurrentQueue = NO;
    PARDispatchQueue *queue1 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    [queue1 dispatchSynchronously:^
     {
         dispatch_queue_t queue2 = dispatch_queue_create("test", DISPATCH_QUEUE_SERIAL);
         dispatch_sync(queue2, ^{ isCurrentQueue = [queue1 isCurrentQueue]; });
     }];
    XCTAssertTrue(!isCurrentQueue, @"isCurrentQueue should (unfortunately) be false when called from inside a block dispatched to that queue, because that call is itself called in another queue down the queue stack that is not using the PARDispatchQueue APIs");
}

- (void)testIsInCurrentQueueStack
{
    __block BOOL isCurrentQueue = NO;
    PARDispatchQueue *queue1 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    PARDispatchQueue *queue2 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    
    // both synchronous
    [queue1 dispatchSynchronously:^
     {
         [queue2 dispatchSynchronously:^{ isCurrentQueue = [queue1 isInCurrentQueueStack]; }];
     }];
    
    XCTAssertTrue(isCurrentQueue, @"isCurrentQueue should be true when called from inside a block dispatched to that queue, even if that call is itself called in another queue down the queue hierarchy");
    
}

- (void)testIsCurrentQueueWithinDispatchQueueAsync
{
    __block BOOL isCurrentQueue = NO;
    PARDispatchQueue *queue1 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    PARDispatchQueue *queue2 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    
    // both synchronous
    PARBlockOperation *operation = [PARBlockOperation dispatchedOperationWithQueue:queue1 block:^
       {
           [queue2 dispatchSynchronously:^{ isCurrentQueue = [queue2 isCurrentQueue]; }];
       }];
    [operation waitUntilFinished];
    XCTAssertTrue(isCurrentQueue, @"isCurrentQueue should be true when called from inside a block dispatched to that queue, even if that call is itself called in another queue down the queue hierarchy");
}

- (void)testIsNotCurrentQueueWithinDifferentQueue
{
    __block BOOL isCurrentQueue = NO;
    PARDispatchQueue *queue1 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    PARDispatchQueue *queue2 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    
    // both synchronous
    [queue1 dispatchSynchronously:^
     {
         isCurrentQueue = [queue2 isCurrentQueue];
     }];
    
    XCTAssertFalse(isCurrentQueue, @"isCurrentQueue should be false when called from within another queue");
    
}


#pragma mark - Timers

// timers can't be guaranteed to fire at the time set, but unless the computer is under heavy load, it will work well enough for the DELAY_TIME to be over while waiting for at least SLEEP_TIME
#define DELAY_TIME 0.010
#define SLEEP_TIME 0.012

- (void)testTimerSchedule
{
    __block BOOL done = NO;
    PARDispatchQueue *queue1 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    [queue1 scheduleTimerWithName:@"flag" timeInterval:DELAY_TIME behavior:PARTimerBehaviorDelay block:^{ done = YES; }];
    [NSThread sleepForTimeInterval:SLEEP_TIME];
    XCTAssertTrue(done, @"timer should have fired after and should have set the 'done' flag to YES");
}

- (void)testTimerCancel
{
    __block BOOL done = NO;
    PARDispatchQueue *queue1 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    [queue1 scheduleTimerWithName:@"flag" timeInterval:DELAY_TIME behavior:PARTimerBehaviorDelay block:^{ done = YES; }];
    [queue1 cancelTimerWithName:@"flag"];
    [NSThread sleepForTimeInterval:SLEEP_TIME];
    XCTAssertFalse(done, @"timer was canceled and should have not set the 'done' flag to YES");
}

// replacing the timer with a new delay should execute the second block, not the firt one
- (void)testTimerReplaceAndDelay
{
    __block NSUInteger flag = 0;
    PARDispatchQueue *queue1 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    [queue1 scheduleTimerWithName:@"flag" timeInterval:DELAY_TIME behavior:PARTimerBehaviorDelay block:^{ flag = 1; }];
    [queue1 scheduleTimerWithName:@"flag" timeInterval:DELAY_TIME behavior:PARTimerBehaviorDelay block:^{ flag = 2; }];
    [NSThread sleepForTimeInterval:SLEEP_TIME];
    NSUInteger expected = 2;
    XCTAssertEqual(flag, expected, @"timer should have set the flag value to %@, but it is %@", @(expected), @(flag));
}

// replacing the timer with a new delay should execute the second block, but only after the new delay
- (void)testTimerReplaceAndDelayLonger
{
    __block NSUInteger flag = 0;
    PARDispatchQueue *queue1 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    [queue1 scheduleTimerWithName:@"flag" timeInterval:DELAY_TIME behavior:PARTimerBehaviorDelay block:^{ flag = 1; }];
    [queue1 scheduleTimerWithName:@"flag" timeInterval:DELAY_TIME+SLEEP_TIME behavior:PARTimerBehaviorDelay block:^{ flag = 2; }];
    [NSThread sleepForTimeInterval:SLEEP_TIME];
    NSUInteger expected = 2;
    XCTAssertFalse(flag == expected, @"timer should have set the flag value to %@ yet, but it is %@", @(expected), @(flag));
    [NSThread sleepForTimeInterval:SLEEP_TIME];
    XCTAssertEqual(flag, expected, @"timer should have now set the flag value to %@, but it is %@", @(expected), @(flag));
}

// replacing the timer by coalescing a second timer should execute the second block, not the first one
- (void)testTimerReplaceAndCoalesce
{
    __block NSUInteger flag = 0;
    PARDispatchQueue *queue1 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    [queue1 scheduleTimerWithName:@"flag" timeInterval:DELAY_TIME behavior:PARTimerBehaviorDelay block:^{ flag = 1; }];
    [queue1 scheduleTimerWithName:@"flag" timeInterval:DELAY_TIME behavior:PARTimerBehaviorCoalesce block:^{ flag = 2; }];
    [NSThread sleepForTimeInterval:SLEEP_TIME];
    NSUInteger expected = 2;
    XCTAssertEqual(flag, expected, @"timer should have set the flag value to %@, but it is %@", @(expected), @(flag));
}

// coalescing the second timer should not reset the timer delay, but still execute the second block at the target time  set by the first timer
- (void)testTimerReplaceAndCoalesceLonger
{
    __block NSUInteger flag = 0;
    PARDispatchQueue *queue1 = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    [queue1 scheduleTimerWithName:@"flag" timeInterval:DELAY_TIME behavior:PARTimerBehaviorDelay block:^{ flag = 1; }];
    [queue1 scheduleTimerWithName:@"flag" timeInterval:SLEEP_TIME+SLEEP_TIME behavior:PARTimerBehaviorCoalesce block:^{ flag = 2; }];
    [NSThread sleepForTimeInterval:SLEEP_TIME];
    NSUInteger expected = 2;
    XCTAssertEqual(flag, expected, @"timer should have set the flag value to %@, but it is %@", @(expected), @(flag));
}

// coalescing the second timer should reset the timer delay because it is shorter, and execute the second block at the target time set by the second timer
- (void)testTimerReplaceAndCoalesceShorter
{
    PARDispatchQueue *queue = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];

    // schedule timer with 2 different blocks, where the second one should dictate the timing and the flag value
    __block NSDate *blockDate = nil;
    __block NSUInteger flag = 0;
    [queue scheduleTimerWithName:@"flag" timeInterval:DELAY_TIME behavior:PARTimerBehaviorCoalesce block:^
    {
        blockDate = [NSDate date];
        flag = 1;
     }];
    NSDate *startDate = [NSDate date];
    [queue scheduleTimerWithName:@"flag" timeInterval:DELAY_TIME / 2.0 behavior:PARTimerBehaviorCoalesce block:^
    {
        blockDate = [NSDate date];
        flag = 2;
    }];
    [NSThread sleepForTimeInterval:SLEEP_TIME];

    // check the flag value
    NSUInteger expectedFlag = 2;
    XCTAssertEqual(flag, expectedFlag, @"timer should have set the flag value to %@, but it is %@", @(expectedFlag), @(flag));
    
    // check the timing
    NSTimeInterval expectedDelay = DELAY_TIME / 2.0;
    NSTimeInterval actualDelay = [blockDate timeIntervalSinceDate:startDate];
    XCTAssertEqualWithAccuracy(expectedDelay, actualDelay, 0.001, @"expected delay of execution %@ but it was %@", @(expectedDelay), @(actualDelay));
}

// when **scheduling** a timer while the queue is busy, it should not affect the execution delay (assuming the queue is not busy when it's time to **execute** the scheduled block)
- (void)testBusyQueueNotAffectingDelay
{
    PARDispatchQueue *queue = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    
    // keep the queue busy
    [queue dispatchAsynchronously:^{ [NSThread sleepForTimeInterval:DELAY_TIME / 2.0]; }];
    
    // schedule the timer while the queue is busy
    NSTimeInterval expectedDelay = DELAY_TIME;
    __block NSTimeInterval actualDelay = 0;
    NSDate *startDate = [NSDate date];
    [queue scheduleTimerWithName:@"flag" timeInterval:expectedDelay behavior:PARTimerBehaviorDelay block:^
     {
         actualDelay = [[NSDate date] timeIntervalSinceDate:startDate];
     }];
    
    // by the time the timer fires, the queue should be ready to execute the timer block
    // we add more sleep time to account for the case where the test actually fails, and the timer is further delayed by the first block sent to the queue above
    [NSThread sleepForTimeInterval:2.0 * SLEEP_TIME];
    
    XCTAssertEqualWithAccuracy(expectedDelay, actualDelay, 0.001, @"expected delay of execution %@ but it was %@", @(expectedDelay), @(actualDelay));
}

- (void)testNow
{
    // dispatch queue just used to have accessed to the `_now` method
    PARDispatchQueue *queue = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    
    NSTimeInterval expectedElapsed = 1.0;
    NSTimeInterval t1 = [[NSDate date] timeIntervalSinceReferenceDate];
    NSTimeInterval u1 = [[queue valueForKey:@"_now"] doubleValue];
    [NSThread sleepForTimeInterval:expectedElapsed];
    NSTimeInterval t2 = [[NSDate date] timeIntervalSinceReferenceDate];
    NSTimeInterval u2 = [[queue valueForKey:@"_now"] doubleValue];
    
    XCTAssertEqualWithAccuracy(t2 - t1, expectedElapsed, 0.01, @"expected elpased time: %@", @(expectedElapsed));
    XCTAssertEqualWithAccuracy(u2 - u1, expectedElapsed, 0.01, @"expected elpased time: %@", @(expectedElapsed));
    XCTAssertEqualWithAccuracy(t2 - t1, u2 - u1, 0.01, @"expected elpased time: %@", @(expectedElapsed));
}

- (void)testTimerWithThrottleBehavior
{
    PARDispatchQueue *queue = [PARDispatchQueue dispatchQueueWithLabel:NSStringFromSelector(_cmd)];
    
    __block NSDate *start = [NSDate date];
    __block NSDate *date1 = nil;
    __block NSDate *date2 = nil;
    NSTimeInterval throttleDelay = 0.2;
    
    // first timer should be fired very quickly
    dispatch_semaphore_t sema = dispatch_semaphore_create(0);
    [queue scheduleTimerWithName:@"throttle" timeInterval:throttleDelay behavior:PARTimerBehaviorThrottle block:^{
       date1 = [NSDate date];
        NSLog(@"date1: %@", date1);
        dispatch_semaphore_signal(sema);
    }];
    long waitResult = dispatch_semaphore_wait(sema, dispatch_time(DISPATCH_TIME_NOW, 0.5 * NSEC_PER_SEC));
    XCTAssertTrue(waitResult == 0, @"Timeout while waiting for timer 1 to fire");

    // second timer should only fire after throttle time
    sema = dispatch_semaphore_create(0);
    [queue scheduleTimerWithName:@"throttle" timeInterval:throttleDelay behavior:PARTimerBehaviorThrottle block:^{
        date2 = [NSDate date];
        NSLog(@"date2: %@", date2);
        dispatch_semaphore_signal(sema);
    }];
    waitResult = dispatch_semaphore_wait(sema, dispatch_time(DISPATCH_TIME_NOW, throttleDelay * 2.0 * NSEC_PER_SEC));
    XCTAssertTrue(waitResult == 0, @"Timeout while waiting for timer 2 to fire");
    
    NSTimeInterval interval1 = [date1 timeIntervalSinceDate:start];
    NSTimeInterval interval2 = [date2 timeIntervalSinceDate:date1];
    
    XCTAssertNotNil(date1);
    XCTAssertNotNil(date2);
    XCTAssertEqualWithAccuracy(interval1, 0.0, 0.01);
    XCTAssertEqualWithAccuracy(interval2, throttleDelay, 0.01);
}


#pragma mark - Global Queue

// with a concurrent queue like the global dispatch queue, PARDispatchQueue should not keep track of the queue stack to try to avoid deadlocks, or it will be very confused
- (void)testGlobalQueueMultipleDispatchesIgnoreQueueStack
{
    [[PARDispatchQueue globalDispatchQueue] dispatchAsynchronously:^{[NSThread sleepForTimeInterval:0.02];}];
    [[PARDispatchQueue globalDispatchQueue] dispatchAsynchronously:^{[NSThread sleepForTimeInterval:0.02];}];
    [[PARDispatchQueue globalDispatchQueue] dispatchAsynchronously:^{[NSThread sleepForTimeInterval:0.02];}];
    [[PARDispatchQueue globalDispatchQueue] dispatchAsynchronously:^{[NSThread sleepForTimeInterval:0.02];}];
    [[PARDispatchQueue globalDispatchQueue] dispatchAsynchronously:^{[NSThread sleepForTimeInterval:0.02];}];
    [NSThread sleepForTimeInterval:0.1];
}



@end
