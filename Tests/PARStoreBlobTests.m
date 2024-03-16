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
    // deleting a blob with the old API should work as before
    
    NSError *error = nil;
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStore *store = [PARStore storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob" error:&error]);

    NSString *blobPath = [store absolutePathForBlobPath: @"blob"];
    XCTAssertTrue([[NSFileManager defaultManager] fileExistsAtPath: blobPath]);

    XCTAssertTrue([store deleteBlobAtPath:@"blob" error:&error]);
    XCTAssertFalse([store blobExistsAtPath:@"blob"]);

    // blob file should have gone
    XCTAssertFalse([[NSFileManager defaultManager] fileExistsAtPath: blobPath]);

    // tombstone file should not have appeared in its place
    NSString *tombstonePath = [blobPath stringByAppendingPathExtension: TombstoneFileExtension];
    XCTAssertFalse([[NSFileManager defaultManager] fileExistsAtPath: tombstonePath]);
    XCTAssertFalse([store blobIsRegisteredDeletedAtPath:@"blob"]);

    [store tearDownNow];
}

- (void)testDeletionWithTombstone
{
    // deleting a blob with the new API should result in a tombstone file
    NSError *error = nil;
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStore *store = [PARStore storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob" error:&error]);

    NSString *blobPath = [store absolutePathForBlobPath: @"blob"];
    XCTAssertTrue([[NSFileManager defaultManager] fileExistsAtPath: blobPath]);

    XCTAssertTrue([store deleteBlobAtPath:@"blob" registeringDeletion: YES error:&error]);
    XCTAssertFalse([store blobExistsAtPath:@"blob"]);

    // blob file should have gone
    XCTAssertFalse([[NSFileManager defaultManager] fileExistsAtPath: blobPath]);
    
    // tombstone file should have appeared in its place
    NSString *tombstonePath = [blobPath stringByAppendingPathExtension: TombstoneFileExtension];
    XCTAssertTrue([[NSFileManager defaultManager] fileExistsAtPath: tombstonePath]);
    XCTAssertTrue([store blobIsRegisteredDeletedAtPath:@"blob"]);

    [store tearDownNow];
}


- (void)testBlobExists
{
    // exists should produce the right result before/after creation of a blob file
    
    NSError *error = nil;
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStore *store = [PARStore storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    
    XCTAssertFalse([store blobExistsAtPath:@"blob"]);

    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob" error:&error]);
    XCTAssertTrue([store blobExistsAtPath:@"blob"]);

    [store tearDownNow];
}


- (void)testTombstoneSuppressesFileExistence
{
    // if there's a tombstone file present, exists should return NO even if the actual blob
    // file is still present
    
    NSError *error = nil;
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStore *store = [PARStore storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob" error:&error]);
    XCTAssertTrue([store blobExistsAtPath:@"blob"]);

    // fake the presence of a tombstone, to simulate a situation where a partial
    // synchronisation has caused it to exist along with the blob
    NSString *tombstonePath = [[store absolutePathForBlobPath: @"blob"] stringByAppendingPathExtension: TombstoneFileExtension];
    [[@"foobar" dataUsingEncoding:NSUTF8StringEncoding] writeToFile:tombstonePath atomically:YES];
    
    XCTAssertFalse([store blobExistsAtPath:@"blob"]);
    
    [store tearDownNow];
}

- (void)testTombstoneSuppressesData
{
    // if there is a tombstone file present, no data should be returned even if the actual blob
    // file is still present
    
    NSError *error = nil;
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStore *store = [PARStore storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob" error:&error]);

    // fake the presence of a tombstone, to simulate a situation where a partial
    // synchronisation has caused it to exist along with the blob
    NSString *tombstonePath = [[store absolutePathForBlobPath: @"blob"] stringByAppendingPathExtension: TombstoneFileExtension];
    [[@"foobar" dataUsingEncoding:NSUTF8StringEncoding] writeToFile:tombstonePath atomically:YES];
    
    XCTAssertNil([store blobDataAtPath:@"blob" error:&error]);
    
    [store tearDownNow];
}

- (void)testTombstoneSuppressesEnumeration
{
    // tombstone files shouldn't be included in the enumeration
    // the presence of a tombstone file should also cause the corresponding data file to be skipped
    // from the enumeration (if it still exists)
    NSError *error = nil;
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStore *store = [PARStore storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob1" error:&error]);
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob2" error:&error]);

    // fake the presence of a tombstone, to simulate a situation where a partial
    // synchronisation has caused it to exist along with the blob
    NSString *tombstonePath = [[store absolutePathForBlobPath: @"blob1"] stringByAppendingPathExtension: TombstoneFileExtension];
    [[@"foobar" dataUsingEncoding:NSUTF8StringEncoding] writeToFile:tombstonePath atomically:YES];
    
    __block int count = 1;
    [store enumerateBlobs:^(NSString *blobPath) {
        ++count;
        XCTAssertEqual(blobPath, @"blob2");
    }];
    XCTAssertEqual(count, 1);
    
    [store tearDownNow];
}

- (void)testEnumeratingTombstones
{
    // we should be called back with both the original data file paths, and the paths to the tombstones
    NSError *error = nil;
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStore *store = [PARStore storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob1" error:&error]);
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob2" error:&error]);
    XCTAssertTrue([store writeBlobData:[@"test" dataUsingEncoding:NSUTF8StringEncoding] toPath:@"blob3" error:&error]);

    XCTAssertTrue([store deleteBlobAtPath:@"blob1" registeringDeletion: YES error:&error]);
    XCTAssertTrue([store deleteBlobAtPath:@"blob3" registeringDeletion: YES error:&error]);

    __block int count = 0;
    NSArray* expected = @[@"blob1", @"blob3"];
    NSArray* expectedTombstones = @[@"blob1.deleted", @"blob3.deleted"];
    [store enumerateDeletedBlobs:^(NSString *blobPath, NSString *markerPath) {
        ++count;
        XCTAssertTrue([expected containsObject: blobPath]);
        XCTAssertTrue([expectedTombstones containsObject: markerPath]);
    }];
    XCTAssertEqual(count, 2);
    
    [store tearDownNow];
}


@end
