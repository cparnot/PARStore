//  PARStore
//  Created by Charles Parnot on 10/10/12.
//  Licensed under the terms of the BSD License, as specified in the file 'LICENSE-BSD.txt' included with this distribution

#import <Foundation/Foundation.h>

@interface PARNotificationSemaphore : NSObject

+ (PARNotificationSemaphore *)semaphoreForNotificationName:(NSString *)name object:(id)obj;
- (BOOL)waitUntilNotificationWithTimeout:(NSTimeInterval)timeout;

@end
