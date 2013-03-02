//  PARStore
//  Created by Charles Parnot on 10/10/12.
//  Licensed under the terms of the BSD License, as specified in the file 'LICENSE-BSD.txt' included with this distribution


#import "PARNotificationSemaphore.h"

@interface PARNotificationSemaphore()
@property (strong) dispatch_semaphore_t dispatchSemaphore;
@property (strong) NSOperationQueue *operationQueue;
@end

@implementation PARNotificationSemaphore

+ (PARNotificationSemaphore *)semaphoreForNotificationName:(NSString *)name object:(id)obj
{
    PARNotificationSemaphore *notificationSemaphore = [[PARNotificationSemaphore alloc] init];
    notificationSemaphore.dispatchSemaphore = dispatch_semaphore_create(0);
    notificationSemaphore.operationQueue = [[NSOperationQueue alloc] init];
    [[NSNotificationCenter defaultCenter] addObserverForName:name object:obj queue:notificationSemaphore.operationQueue usingBlock:^(NSNotification *note)
    {
        dispatch_semaphore_signal(notificationSemaphore.dispatchSemaphore);
    }];
    return notificationSemaphore;
}

- (BOOL)waitUntilNotificationWithTimeout:(NSTimeInterval)timeout;
{
    long waitResult = dispatch_semaphore_wait(self.dispatchSemaphore, dispatch_time(DISPATCH_TIME_NOW, timeout * NSEC_PER_SEC));
    return (waitResult == 0);
}

@end
