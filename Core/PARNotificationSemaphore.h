//  PARStore
//  Author: Charles Parnot
//  Licensed under the terms of the BSD License, as specified in the file 'LICENSE-BSD.txt' included with this distribution

#import <Foundation/Foundation.h>


// The APIs were audited. None of the method return values, method parameters or properties are nullable.
NS_ASSUME_NONNULL_BEGIN

@interface PARNotificationSemaphore : NSObject

+ (PARNotificationSemaphore *)semaphoreForNotificationName:(NSString *)name object:(id)obj;
- (BOOL)waitUntilNotificationWithTimeout:(NSTimeInterval)timeout;

@property (readonly) BOOL notificationWasPosted;

@end

NS_ASSUME_NONNULL_END
