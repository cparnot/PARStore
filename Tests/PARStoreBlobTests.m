//  PARStoreBlobTests
//  Created by Sam Deane on 22/06/21.
//  All code (c) 2021 - present day, Elegant Chaos Limited.

#import "PARTestCase.h"
#import "PARStoreExample.h"
#import "PARNotificationSemaphore.h"

extern NSString *const TombstoneFileExtension; // normally private, but exposed for testing

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

    NSString *blobPath = [store absolutePathForBlobPath: @"blob"];
    XCTAssertTrue([[NSFileManager defaultManager] fileExistsAtPath: blobPath]);

    XCTAssertTrue([store deleteBlobAtPath:@"blob" error:&error]);

    // blob file should have gone
    XCTAssertFalse([[NSFileManager defaultManager] fileExistsAtPath: blobPath]);

    [store tearDownNow];
}

- (void)testDeletionWithTombstone
{
    NSError *error = nil;
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStore *store = [PARStore storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob" error:&error]);

    NSString *blobPath = [store absolutePathForBlobPath: @"blob"];
    XCTAssertTrue([[NSFileManager defaultManager] fileExistsAtPath: blobPath]);

    XCTAssertTrue([store deleteBlobAtPath:@"blob" usingTombstone: YES error:&error]);

    // blob file should have gone
    XCTAssertFalse([[NSFileManager defaultManager] fileExistsAtPath: blobPath]);
    
    // tombstone file should have appeared in its place
    NSString *tombstonePath = [blobPath stringByAppendingPathExtension: TombstoneFileExtension];
    XCTAssertTrue([[NSFileManager defaultManager] fileExistsAtPath: tombstonePath]);

    [store tearDownNow];
}

- (void)testTombstonePreventsData
{
    NSError *error = nil;
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStore *store = [PARStore storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob" error:&error]);

    
}

- (void)testTombstonePreventsEnumeration
{
    
}

@end
