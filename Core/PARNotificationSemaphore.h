//  PARStore
//  Author: Charles Parnot
//  Licensed under the terms of the BSD License, as specified in the file 'LICENSE-BSD.txt' included with this distribution

#import <Foundation/Foundation.h>

@interface PARNotificationSemaphore : NSObject

+ (PARNotificationSemaphore *)semaphoreForNotificationName:(NSString *)name object:(id)obj;
- (BOOL)waitUntilNotificationWithTimeout:(NSTimeInterval)timeout;

@property (readonly) BOOL notificationWasPosted;

@end
