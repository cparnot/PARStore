//  PARStoreBlobTests
//  Created by Sam Deane on 22/06/21.
//  All code (c) 2021 - present day, Elegant Chaos Limited.

#import "PARTestCase.h"
#import "PARStoreExample.h"
#import "PARNotificationSemaphore.h"

@interface PARStoreBlobTests : PARTestCase

@end


@implementation PARStoreBlobTests

- (void)setUp
{
    [super setUp];
    
    // Set-up code here.
}

- (void)tearDown
{
    // Tear-down code here.
    
    [super tearDown];
}

- (NSString *)deviceIdentifierForTest
{
    return @"948E9EEE-3398-4DD7-9183-C56866EF2350";
}

#pragma mark - 

- (void)testDeletion
{
    NSError *error = nil;
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStore *store = [PARStore storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob" error:&error]);

    NSURL *blobURL = [[url URLByAppendingPathComponent: @"Blobs"] URLByAppendingPathComponent: @"blob"];
    XCTAssertTrue([[NSFileManager defaultManager] fileExistsAtPath: blobURL.path]);

    XCTAssertTrue([store deleteBlobAtPath:@"blob" error:&error]);

    // blob file should have gone
    XCTAssertFalse([[NSFileManager defaultManager] fileExistsAtPath: blobURL.path]);

    [store tearDownNow];
}

- (void)testDeletionWithTombstone
{
    NSError *error = nil;
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStore *store = [PARStore storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob" error:&error]);

    NSURL *blobURL = [[url URLByAppendingPathComponent: @"Blobs"] URLByAppendingPathComponent: @"blob"];
    XCTAssertTrue([[NSFileManager defaultManager] fileExistsAtPath: blobURL.path]);

    XCTAssertTrue([store deleteBlobAtPath:@"blob" usingTombstone: YES error:&error]);

    // blob file should have gone
    XCTAssertFalse([[NSFileManager defaultManager] fileExistsAtPath: blobURL.path]);
    
    // tombstone file should have appeared in its place
    NSURL *tombstoneURL = [blobURL URLByAppendingPathExtension: @"deleted"];
    XCTAssertTrue([[NSFileManager defaultManager] fileExistsAtPath: tombstoneURL.path]);

    [store tearDownNow];
}

@end
