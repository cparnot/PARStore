//  PARStore
//  Created by Charles Parnot on 3/2/13.
//  Copyright (c) 2013 Charles Parnot. All rights reserved.


#import "PARStoreExample.h"
#import <objc/runtime.h>

@interface PARStore (PARStorePrivate)
@property (retain) PARDispatchQueue *notificationQueue;
- (void)postNotificationWithName:(NSString *)notificationName;
- (NSArray *)_sortedLogRepresentationsFromDeviceIdentifier:(NSString *)deviceIdentifier;
@end

@implementation PARStoreExample

- (NSArray *)sortedLogRepresentationsFromDeviceIdentifier:(NSString *)deviceIdentifier
{
    return [super _sortedLogRepresentationsFromDeviceIdentifier:deviceIdentifier];
}


+ (NSArray *)relevantKeysForSync
{
    static dispatch_once_t onceToken;
    static NSArray *keys = nil;
    dispatch_once(&onceToken, ^
    {
        keys = @[@"first", @"last", @"title", @"summary"];
    });
    return keys;
}

- (NSArray *)relevantKeysForSync
{
    return [[self class] relevantKeysForSync];
}

// accessors are created dynamically based on the list of keys returned by the method `relevantKeysForSync`

@dynamic first, last, title, summary;

// inspired by http://stackoverflow.com/questions/3560364/writing-my-own-dynamic-properties-in-cocoa

+ (BOOL)resolveInstanceMethod:(SEL)aSEL
{
    // method name --> property
    NSString *property = NSStringFromSelector(aSEL);
    BOOL setter = ([property length] > 4 && [property hasPrefix:@"set"] && [property hasSuffix:@":"]);
    if (setter)
    {
        NSString *firstLetter = [[property substringWithRange:NSMakeRange(3, 1)] lowercaseString];
        property = [firstLetter stringByAppendingString:[property substringWithRange:NSMakeRange(4, [property length] - 5)]];
    }
    
    // valid metadata property?
    if (![[self relevantKeysForSync] containsObject:property])
        return [super resolveInstanceMethod:aSEL];
    
    // setter or getter
    if (setter)
        class_addMethod([self class], aSEL, (IMP) metadataSetter, "v@:@");
    else
        class_addMethod([self class], aSEL, (IMP) metadataGetter, "@@:");
    
    return YES;
}

id metadataGetter(id self, SEL _cmd)
{
    NSString *property = NSStringFromSelector(_cmd);
    return [self propertyListValueForKey:property];
}

void metadataSetter(id self, SEL _cmd, id newValue)
{
    // method name --> property
    NSString *property = NSStringFromSelector(_cmd);
    NSString *firstLetter = [[property substringWithRange:NSMakeRange(3, 1)] lowercaseString];
    property = [firstLetter stringByAppendingString:[property substringWithRange:NSMakeRange(4, [property length] - 5)]];
    
    // set metadata value
    [self setPropertyListValue:newValue forKey:property];
}

#pragma mark - Throttling notifications

// to test that notifications are all sent when `waitUntilFinished` returns, we introduce here an extra delay to make sure the test fails without the proper queuing
- (void)postNotificationWithName:(NSString *)notificationName
{
    [self.notificationQueue dispatchAsynchronously:^
     {
         if (self.shouldThrottleNotifications)
             [NSThread sleepForTimeInterval:0.3];
     }];
    [super postNotificationWithName:notificationName];
}


@end
