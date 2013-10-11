//  PARStore
//  Created by Charles Parnot on 3/2/13.
//  Copyright (c) 2013 Charles Parnot. All rights reserved.


#import "PARTestCase.h"

@implementation PARTestCase

#pragma mark - Utilities

- (NSURL *)urlWithUniqueTmpDirectory
{
    NSString *parentDir = [[NSBundle mainBundle] bundleIdentifier];
    NSString *uniqueDir = [[[NSDate date] description] stringByAppendingString:[[NSUUID UUID] UUIDString]];
    NSString *path = [[[@"~/Xcode-Tests" stringByAppendingPathComponent:parentDir] stringByAppendingPathComponent:uniqueDir] stringByStandardizingPath];
    NSURL *url = [NSURL fileURLWithPath:path];
    NSError *error = nil;
    BOOL success = [[NSFileManager defaultManager] createDirectoryAtURL:url withIntermediateDirectories:YES attributes:nil error:&error];
    XCTAssertTrue(success, @"Could not create temporary directory:\npath: %@\nerror: %@", path, error);
    return url;
}

@end
