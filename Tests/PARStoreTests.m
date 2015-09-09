//  PARStoreTests
//  Created by Charles Parnot on 3/2/13.
//  Copyright (c) 2013 Charles Parnot. All rights reserved.

#import "PARTestCase.h"
#import "PARStoreExample.h"
#import "PARNotificationSemaphore.h"

@interface PARStoreTests : PARTestCase

@end


@implementation PARStoreTests

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

#pragma mark - Creating and Loading Document

- (void)testCreateThenLoadDocument
{
    // first load = create and load store
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStoreExample *document1 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document1 loadNow];
    XCTAssertTrue([document1 loaded], @"Document not loaded");
    [document1 tearDownNow];
    XCTAssertFalse([document1 loaded], @"Document should not be loaded after tearing it down");
    document1 = nil;
    
    // second load = load document
    PARStoreExample *document2 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document2 loadNow];
    XCTAssertTrue([document2 loaded], @"Document not loaded");
    [document2 tearDownNow];
    XCTAssertFalse([document2 loaded], @"Document should not be loaded after tearing it down");
    document2 = nil;
}

- (void)testCreateThenDeleteDocument
{
    // first load = create and load document
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStoreExample *document1 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document1 loadNow];
    XCTAssertTrue([document1 loaded], @"Document not loaded");
    
    PARNotificationSemaphore *semaphore = [PARNotificationSemaphore semaphoreForNotificationName:PARStoreDidDeleteNotification object:document1];
    NSFileCoordinator *coordinator = [[NSFileCoordinator alloc] initWithFilePresenter:nil];
    [coordinator coordinateWritingItemAtURL:url options:NSFileCoordinatorWritingForDeleting error:NULL byAccessor:^(NSURL *newURL)
     {
         NSError *fmError = nil;
         BOOL fileDeletionSucceeeded = [[NSFileManager defaultManager] removeItemAtURL:newURL error:&fmError];
         XCTAssertTrue(fileDeletionSucceeeded, @"The file could not be deleted by NSFileManager: %@", [url path]);
     }];
    BOOL completedWithoutTimeout = [semaphore waitUntilNotificationWithTimeout:15.0];
    XCTAssertTrue(completedWithoutTimeout, @"Timeout while waiting for document deletion");
    
    XCTAssertTrue([document1 deleted], @"Document should be marked as deleted");
    [document1 tearDownNow];
    XCTAssertFalse([document1 deleted], @"Document should not marked as deleted anymore after tearing it down");
    document1 = nil;
}

- (void)testCloseDatabase
{
    // first load = create and load store
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStoreExample *document1 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document1 loadNow];
    XCTAssertTrue([document1 loaded], @"Document not loaded");
    [document1 closeDatabaseNow];
    XCTAssertTrue([document1 loaded], @"Document not loaded");
    [document1 loadNow];
    XCTAssertTrue([document1 loaded], @"Document not loaded");

    [document1 tearDownNow];
    XCTAssertFalse([document1 loaded], @"Document should not be loaded after tearing it down");
    document1 = nil;
}

- (void)testFilePackageIsNotDirectory
{
    // create and load document
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStoreExample *sound1 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [sound1 loadNow];
    XCTAssertTrue([sound1 loaded], @"Document not loaded");
    [sound1 tearDownNow];
    sound1 = nil;
    
    // mess up the file package
    NSError *error = nil;
    BOOL success = [[NSFileManager defaultManager] removeItemAtURL:url error:&error];
    XCTAssertTrue(success, @"Could not remove directory:\nurl: %@\nerror: %@", url, error);
    success = [@"blah" writeToURL:url atomically:YES encoding:NSUTF8StringEncoding error:&error];
    XCTAssertTrue(success, @"Could not write string to disk:\nurl: %@\nerror: %@", url, error);
    
    // second load = load document
    PARStoreExample *store2 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [store2 loadNow];
    XCTAssertFalse([store2 loaded], @"Corrupted document should not load");
    [store2 tearDownNow];
    store2 = nil;
}


#pragma mark - Testing Content Access

- (void)testPropertyListSetter
{
    NSString *title = @"Some title";
    NSString *first = @"Albert";
    
    // first load = create document and save data
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStoreExample *document1 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document1 loadNow];
    document1.title = title;
    [document1 setPropertyListValue:first forKey:@"first"];
    [document1 tearDownNow];
    document1 = nil;
    
    // second load = load document and compare data
    PARStoreExample *document2 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document2 loadNow];
    NSString *actualTitle = [document2 propertyListValueForKey:@"title"];
    NSString *actualFirst = document2.first;
    XCTAssertEqualObjects(actualTitle, title, @"unexpected 'title' value after closing and reopening a document: '%@' instead of '%@'", actualTitle, title);
    XCTAssertEqualObjects(actualFirst, first, @"unexpected 'first' value after closing and reopening a document: '%@' instead of '%@'", actualFirst, first);
    [document2 tearDownNow];
    document2 = nil;
}

- (void)testDictionarySetter
{
    NSString *title = @"Some title";
    NSString *first = @"Albert";
    NSDictionary *entries = @{@"title": title, @"first": first};
    
    // first load = create document and save data
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStoreExample *document1 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document1 loadNow];
    [document1 setEntriesFromDictionary:entries];
    [document1 tearDownNow];
    document1 = nil;
    
    // second load = load document and compare data
    PARStoreExample *document2 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document2 loadNow];
    NSDictionary *actualEntries = document2.allRelevantValues;
    NSString *actualTitle = actualEntries[@"title"];
    NSString *actualFirst = actualEntries[@"first"];
    XCTAssertEqualObjects(actualTitle, title, @"unexpected 'title' value after closing and reopening a document: '%@' instead of '%@'", actualTitle, title);
    XCTAssertEqualObjects(actualFirst, first, @"unexpected 'first' value after closing and reopening a document: '%@' instead of '%@'", actualFirst, first);
    [document2 tearDownNow];
    document2 = nil;
}

- (void)testPropertyAccessAfterClosingDatabase
{
    NSString *title   = @"Some title";
    NSString *summary = @"Awsome must read.";
    NSString *first   = @"Albert";
    NSString *last    = @"Einstaine";
    NSDictionary *entries1 = @{@"title": title, @"first": first};
    NSDictionary *entries2 = @{@"summary": summary, @"last": last};
    
    // first load = create document and save data
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStoreExample *document1 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document1 loadNow];
    [document1 setEntriesFromDictionary:entries1];
    
    // close database --> it should reopen automatically when we query again
    [document1 closeDatabaseNow];
    
    // get properties and compare data
    [document1 loadNow];
    NSDictionary *actualEntries1 = document1.allRelevantValues;
    NSString *actualTitle = actualEntries1[@"title"];
    NSString *actualFirst = actualEntries1[@"first"];
    XCTAssertEqualObjects(actualTitle, title, @"unexpected 'title' value after closing database of document: '%@' instead of '%@'", actualTitle, title);
    XCTAssertEqualObjects(actualFirst, first, @"unexpected 'first' value after closing database of document: '%@' instead of '%@'", actualFirst, first);

    // add more data
    [document1 setEntriesFromDictionary:entries2];
    [document1 tearDownNow];
    document1 = nil;

    // second load = load document and compare data
    PARStoreExample *document2 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document2 loadNow];
    NSDictionary *actualEntries2 = document2.allRelevantValues;
    NSString *actualSummary = actualEntries2[@"summary"];
    NSString *actualLast = actualEntries2[@"last"];
    XCTAssertEqualObjects(actualSummary, summary, @"unexpected 'summary' value after closing database, tearing down and reopening a document: '%@' instead of '%@'", actualSummary, summary);
    XCTAssertEqualObjects(actualLast, last, @"unexpected 'last' value after closing database, tearing down and reopening a document: '%@' instead of '%@'", actualLast, last);

    [document2 tearDownNow];
    document2 = nil;
}


#pragma mark - Testing Sync

- (void)testStoreSyncWithOneDevice
{
    // testing a bug that was still in commit 29c3af64047946244e23f52e85ebd8fe08c3fc8e, where the assertion for 'Inconsistent tracking of persistent stores' was wrongly raised
	NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"SyncTest.parstore"];
    PARStoreExample *store1 = [PARStoreExample storeWithURL:url deviceIdentifier:@"1"];
    [store1 loadNow];
	store1.title = @"New title";
    [store1 syncNow];
}

- (void)testStoreSync
{
	NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"SyncTest.parstore"];
	
    PARStoreExample *store1 = [PARStoreExample storeWithURL:url deviceIdentifier:@"1"];
    [store1 loadNow];
	XCTAssertTrue([store1 loaded], @"Store not loaded");
	XCTAssertNil(store1.title, @"A new store should not have a title");
	
	PARStoreExample *store2 = [PARStoreExample storeWithURL:url deviceIdentifier:@"2"];
    [store2 loadNow];
	XCTAssertTrue([store2 loaded], @"Store not loaded");
	XCTAssertNil(store2.title, @"A new store should not have a title");
	
    // change first store --> should trigger a change in the second store
    PARNotificationSemaphore *semaphore = [PARNotificationSemaphore semaphoreForNotificationName:PARStoreDidSyncNotification object:store2];
    NSString *title = @"The Title";
	store1.title = title;
    BOOL completedWithoutTimeout = [semaphore waitUntilNotificationWithTimeout:10.0];
	
    XCTAssertTrue(completedWithoutTimeout, @"Timeout while waiting for document change");
	XCTAssertEqualObjects(store1.title, title, @"Title is '%@' but should be '%@'", store1.title, title);
	XCTAssertEqualObjects(store2.title, title, @"Title is '%@' but should be '%@'", store2.title, title);
    
    [store1 tearDownNow];
    [store2 tearDownNow];
}

// same as `testStoreSync` but changing store 2
- (void)testDeviceAddition
{
	NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"SyncTest.parstore"];
	
    PARStoreExample *store1 = [PARStoreExample storeWithURL:url deviceIdentifier:@"1"];
    [store1 loadNow];
	XCTAssertTrue([store1 loaded], @"Store not loaded");
	XCTAssertNil(store1.title, @"A new store should not have a title");
    
    // add second device --> should trigger a sync in the first store, though no change yet
	PARStoreExample *store2 = [PARStoreExample storeWithURL:url deviceIdentifier:@"2"];
    [store2 loadNow];
	XCTAssertTrue([store2 loaded], @"Store not loaded");
	XCTAssertNil(store2.title, @"A new store should not have a title");
	
    // change second store --> should trigger a change in the first store
    PARNotificationSemaphore *semaphore = [PARNotificationSemaphore semaphoreForNotificationName:PARStoreDidSyncNotification object:store1];
    NSString *title = @"The Title";
	store2.title = title;
    BOOL completedWithoutTimeout = [semaphore waitUntilNotificationWithTimeout:10.0];
	
    XCTAssertTrue(completedWithoutTimeout, @"Timeout while waiting for document change");
	XCTAssertEqualObjects(store1.title, title, @" - title is '%@' but should be '%@'", store1.title, title);
	XCTAssertEqualObjects(store2.title, title, @" - title is '%@' but should be '%@'", store2.title, title);
    
    [store1 tearDownNow];
    [store2 tearDownNow];
}


#pragma mark - Testing Merge

- (void)testMerge
{
    NSURL *urlA = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"MergeTestA.parstore"];
    PARStoreExample *storeA1 = [PARStoreExample storeWithURL:urlA deviceIdentifier:@"1"];
    [storeA1 loadNow];
    PARStoreExample *storeA2 = [PARStoreExample storeWithURL:urlA deviceIdentifier:@"2"];
    [storeA2 loadNow];
    storeA1.title = @"titleA1";
    storeA2.title = @"titleA2";
    [storeA1 saveNow];
    [storeA2 saveNow];
    
    NSURL *urlB = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"MergeTestB.parstore"];
    PARStoreExample *storeB1 = [PARStoreExample storeWithURL:urlB deviceIdentifier:@"1"];
    [storeB1 loadNow];
    PARStoreExample *storeB2 = [PARStoreExample storeWithURL:urlB deviceIdentifier:@"2"];
    [storeB2 loadNow];
    storeB1.title = @"titleB1";
    storeB2.title = @"titleB2";
    [storeB1 saveNow];
    [storeB2 saveNow];
    
    // change second store --> should trigger a change in the first store
    PARNotificationSemaphore *semaphore = [PARNotificationSemaphore semaphoreForNotificationName:PARStoreDidLoadNotification object:storeA1];
    [storeA1 mergeStore:storeB1 unsafeDeviceIdentifiers:@[] completionHandler:^(NSError *error) {
        XCTAssertNil(error, @"error merging: %@", error);
        NSLog(@"done merging");
    }];
    BOOL completedWithoutTimeout = [semaphore waitUntilNotificationWithTimeout:100.0];
    XCTAssertTrue(completedWithoutTimeout, @"Timeout while waiting for PARStore merge");
    
    NSString *expectedTitle = @"titleB2";
    [storeA1 loadNow];
    XCTAssertEqualObjects(storeA1.title, expectedTitle, @" - title is '%@' but should be '%@'", storeA1.title, expectedTitle);
    
    [storeA1 tearDownNow];
    [storeA2 tearDownNow];
    [storeB1 tearDownNow];
    [storeB2 tearDownNow];
}


#pragma mark - Testing Timestamps

- (void)testTimestampNow
{
    NSNumber *timestamp1 = [PARStore timestampNow];
    NSNumber *timestamp2 = [PARStore timestampNow];
    NSNumber *timestamp3 = [PARStore timestampNow];
    XCTAssert([timestamp1 compare:timestamp2] != NSOrderedDescending, @"timestamp1 should be smaller than timestamp2 but %@ > %@", timestamp1, timestamp2);
    XCTAssert([timestamp1 compare:timestamp3] != NSOrderedDescending, @"timestamp1 should be smaller than timestamp3 but %@ > %@", timestamp1, timestamp3);
    XCTAssert([timestamp2 compare:timestamp3] != NSOrderedDescending, @"timestamp2 should be smaller than timestamp3 but %@ > %@", timestamp2, timestamp3);
}

- (void)testTimestampDistantPath
{
    NSNumber *timestamp1 = [PARStore timestampForDistantPast];
    NSNumber *timestamp2 = [PARStore timestampNow];
    XCTAssert([timestamp1 compare:timestamp2] == NSOrderedAscending, @"timestamp1 should be smaller than timestamp2 but %@ > %@", timestamp1, timestamp2);
}

- (void)testTimestampDistantFuture
{
    NSNumber *timestamp1 = [PARStore timestampNow];
    NSNumber *timestamp2 = [PARStore timestampForDistantFuture];
    XCTAssert([timestamp1 compare:timestamp2] == NSOrderedAscending, @"timestamp1 should be smaller than timestamp2 but %@ > %@", timestamp1, timestamp2);
}

- (void)testMostRecentTimestampForDeviceIdentifier
{
	NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"SyncTest.parstore"];
	
    PARStoreExample *store1 = [PARStoreExample storeWithURL:url deviceIdentifier:@"1"];
    [store1 loadNow];
	XCTAssertTrue([store1 loaded], @"Store not loaded");
	
	PARStoreExample *store2 = [PARStoreExample storeWithURL:url deviceIdentifier:@"2"];
    [store2 loadNow];
	XCTAssertTrue([store2 loaded], @"Store not loaded");
	
    // change first store
    NSString *title = @"The Title";
	store1.title = title;
    [store1 saveNow];
    [store2 syncNow];
    
    // check timestamps
    NSNumber *timestamp11 = [store1 mostRecentTimestampForDeviceIdentifier:@"1"];
    NSNumber *timestamp12 = [store1 mostRecentTimestampForDeviceIdentifier:@"2"];
    NSNumber *timestamp21 = [store2 mostRecentTimestampForDeviceIdentifier:@"1"];
    NSNumber *timestamp22 = [store2 mostRecentTimestampForDeviceIdentifier:@"2"];
    NSNumber *timestampForDistantPast = [PARStore timestampForDistantPast];
    XCTAssertNotNil(timestamp11, @"timestamp expected in store 1");
    XCTAssertNotNil(timestamp21, @"timestamp expected in store 1");
    XCTAssertEqualObjects(timestamp11, timestamp21, @"");
    
    XCTAssertEqualObjects(nil, timestamp12, @"no timestamp expected in store 2");
    XCTAssertEqualObjects(timestampForDistantPast, timestamp22, @"no timestamp expected in store 2");
    
    [store1 tearDownNow];
    [store2 tearDownNow];
}

- (void)testMostRecentTimestampsByDeviceIdentifier
{
	NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"SyncTest.parstore"];
	
    PARStoreExample *store1 = [PARStoreExample storeWithURL:url deviceIdentifier:@"1"];
    [store1 loadNow];
	XCTAssertTrue([store1 loaded], @"Store not loaded");
	
	PARStoreExample *store2 = [PARStoreExample storeWithURL:url deviceIdentifier:@"2"];
    [store2 loadNow];
	XCTAssertTrue([store2 loaded], @"Store not loaded");
	
    // change first store
    NSString *title = @"The Title";
	store1.title = title;
    [store1 saveNow];
    [store2 syncNow];
    
    // check timestamps
    NSNumber *timestamp11 = [store1 mostRecentTimestampsByDeviceIdentifier][@"1"];
    NSNumber *timestamp12 = [store1 mostRecentTimestampsByDeviceIdentifier][@"2"];
    NSNumber *timestamp21 = [store2 mostRecentTimestampsByDeviceIdentifier][@"1"];
    NSNumber *timestamp22 = [store2 mostRecentTimestampsByDeviceIdentifier][@"2"];
    NSNumber *timestampForDistantPast = [PARStore timestampForDistantPast];
    XCTAssertNotNil(timestamp11, @"timestamp expected in store 1");
    XCTAssertNotNil(timestamp21, @"timestamp expected in store 1");
    XCTAssertEqualObjects(timestamp11, timestamp21, @"");
    
    XCTAssertEqualObjects(nil, timestamp12, @"no timestamp expected in store 2");
    XCTAssertEqualObjects(timestampForDistantPast, timestamp22, @"no timestamp expected in store 2");
    
    [store1 tearDownNow];
    [store2 tearDownNow];
}

- (void)testTimestampOrder
{
	NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"SyncTest.parstore"];
	
    PARStoreExample *store = [PARStoreExample storeWithURL:url deviceIdentifier:@"1"];
    [store loadNow];
	XCTAssertTrue([store loaded], @"Store not loaded");
    
    // change #1
    NSNumber *timestamp1A = [PARStore timestampNow];
	store.first = @"Alice";
    NSNumber *timestamp1B = [PARStore timestampNow];
    NSNumber *timestamp1 = [store mostRecentTimestampForKey:@"first"];
    
    // change #2
    NSNumber *timestamp2A = [PARStore timestampNow];
	store.title = @"The Title";
    NSNumber *timestamp2B = [PARStore timestampNow];
    NSNumber *timestamp2 = [store mostRecentTimestampForKey:@"title"];

    // check timestamps not nil
    XCTAssertNotNil(timestamp1, @"timestamp expected for key 'first'");
    XCTAssertNotNil(timestamp2, @"timestamp expected for key 'title'");
    XCTAssertNotNil(timestamp1A, @"timestamp expected with [PARStore timestampNow]");
    XCTAssertNotNil(timestamp1B, @"timestamp expected with [PARStore timestampNow]");
    XCTAssertNotNil(timestamp2A, @"timestamp expected with [PARStore timestampNow]");
    XCTAssertNotNil(timestamp2B, @"timestamp expected with [PARStore timestampNow]");

    // check timestamp order
    NSArray *expectedOrders = @[
                                @[@"1A before 1", timestamp1A, timestamp1],
                                @[@"1B after  1", timestamp1, timestamp1B],
                                
                                @[@"2A before 2", timestamp2A, timestamp2],
                                @[@"2B after  2", timestamp2, timestamp2B],

                                @[@"1 before 2", timestamp1A, timestamp2],
                                ];
    for (NSArray *expectation in expectedOrders)
    {
        NSString *description = expectation[0];
        NSNumber *timestampX = expectation[1];
        NSNumber *timestampY = expectation[2];
        BOOL orderIsCorrect = [timestampX compare:timestampY] == NSOrderedAscending;
        XCTAssertTrue(orderIsCorrect, @"incorrect order: expected %@ but %@ is after %@", description, timestampX, timestampY);
    }
    
    [store tearDownNow];
}

- (void)testMostRecentTimestampForKey
{
	NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"SyncTest.parstore"];
	
    PARStoreExample *store = [PARStoreExample storeWithURL:url deviceIdentifier:@"1"];
    [store loadNow];
	XCTAssertTrue([store loaded], @"Store not loaded");

    // change first store
	store.first = @"Alice";
	store.title = @"The Title";
    [store saveNow];
    
    // check timestamps
    NSNumber *timestamp1 = [store mostRecentTimestampForDeviceIdentifier:@"1"];
    NSNumber *timestamp2 = [store mostRecentTimestampForKey:@"first"];
    NSNumber *timestamp3 = [store mostRecentTimestampForKey:@"title"];
    XCTAssertNotNil(timestamp1, @"timestamp expected in store 1");
    XCTAssertNotNil(timestamp2, @"timestamp expected for key 'first'");
    XCTAssertNotNil(timestamp3, @"timestamp expected for key 'title'");
    XCTAssertEqualObjects(timestamp1, timestamp3, @"timestamps should be the same for device and 'title' key but are %@ and %@", timestamp1, timestamp2);
    XCTAssertNotEqualObjects(timestamp1, timestamp2, @"timestamps should be different for device and 'first' key but are %@ and %@", timestamp1, timestamp2);
    
    [store tearDownNow];
}

- (void)testMostRecentTimestampsByKeys
{
	NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"SyncTest.parstore"];
	
    PARStoreExample *store = [PARStoreExample storeWithURL:url deviceIdentifier:@"1"];
    [store loadNow];
	XCTAssertTrue([store loaded], @"Store not loaded");
    
    // change first store
	store.first = @"Alice";
	store.title = @"The Title";
    [store saveNow];
    
    // check timestamps
    NSNumber *timestamp1  = [store mostRecentTimestampForDeviceIdentifier:@"1"];
    NSNumber *timestamp2a = [store mostRecentTimestampForKey:@"first"];
    NSNumber *timestamp3a = [store mostRecentTimestampForKey:@"title"];
    NSNumber *timestamp2b = [store mostRecentTimestampsByKey][@"first"];
    NSNumber *timestamp3b = [store mostRecentTimestampsByKey][@"title"];
    
    XCTAssertNotNil(timestamp1, @"timestamp expected in store 1");
    XCTAssertNotNil(timestamp2a, @"timestamp expected for key 'first'");
    XCTAssertNotNil(timestamp3a, @"timestamp expected for key 'title'");
    XCTAssertNotNil(timestamp2b, @"timestamp expected for key 'first'");
    XCTAssertNotNil(timestamp3b, @"timestamp expected for key 'title'");
    
    XCTAssertEqualObjects(timestamp1, timestamp3a, @"timestamps should be the same for device and 'title' key but are %@ and %@", timestamp1, timestamp2a);
    XCTAssertNotEqualObjects(timestamp1, timestamp2a, @"timestamps should be different for device and 'first' key but are %@ and %@", timestamp1, timestamp2a);

    XCTAssertEqualObjects(timestamp2a, timestamp2b, @"timestamps should be the same for 'first' key but are %@ and %@", timestamp2a, timestamp2b);
    XCTAssertEqualObjects(timestamp3a, timestamp3b, @"timestamps should be the same for 'title' key but are %@ and %@", timestamp3a, timestamp3b);

    [store tearDownNow];
}


#pragma mark - Testing History

- (void)testChangesHistory
{
    NSString *deviceIdentifier = [[NSUUID UUID] UUIDString];
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    NSString *first = @"Jane";
    NSString *last  = @"Doe";
    NSString *title = @"The Title";
    
    // feed data
    PARStoreExample *storeExample = [PARStoreExample storeWithURL:url deviceIdentifier:deviceIdentifier];
    [storeExample loadNow];
    NSNumber *beginTimestamp = [PARStore timestampNow];
    storeExample.first = first;
    storeExample.last  = last;
    storeExample.title = title;
    NSNumber *endTimestamp = [PARStore timestampNow];
    
    // actual changes
    NSArray *changes = [storeExample changesSinceTimestamp:nil];
    
    // change count
    XCTAssertTrue(changes.count == 3, @"expected 3 changes but got %@", @(changes.count));
    if (changes.count != 3)
    {
        return;
    }
    
    // timestamp order
    NSArray *actualTimestamps = [changes valueForKey:@"timestamp"];
    NSArray *orderedTimestamps = [actualTimestamps sortedArrayUsingSelector:@selector(compare:)];
    XCTAssertEqualObjects(actualTimestamps, orderedTimestamps, @"timestamps returned by API should already be ordered but are: %@\n instead of ordered timestamps: %@", actualTimestamps, orderedTimestamps);
    
    // timestamp range
    for (NSNumber *timestamp in actualTimestamps)
    {
        XCTAssertTrue([timestamp compare:beginTimestamp] != NSOrderedAscending, @"timestamp %@ should be after beginTimestamp %@", timestamp, beginTimestamp);
        XCTAssertTrue([timestamp compare:endTimestamp] != NSOrderedDescending, @"timestamp %@ should be before endTimestamp %@", timestamp, endTimestamp);
    }
    
    // expected changes
    PARChange *change0 = changes[0];
    PARChange *change1 = changes[1];
    PARChange *change2 = changes[2];
    NSArray *expectedChanges = @[
                                 [PARChange changeWithTimestamp:change0.timestamp parentTimestamp:change0.parentTimestamp key:@"first" propertyList:first],
                                 [PARChange changeWithTimestamp:change1.timestamp parentTimestamp:change1.parentTimestamp key:@"last" propertyList:last],
                                 [PARChange changeWithTimestamp:change2.timestamp parentTimestamp:change2.parentTimestamp key:@"title" propertyList:title],
                                 ];
    XCTAssertEqualObjects(changes, expectedChanges, @"unexpected changes: %@", changes);
}

- (void)testChangesHistoryWithSync
{
    NSString *device1 = [[NSUUID UUID] UUIDString];
    NSString *device2 = [[NSUUID UUID] UUIDString];
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    NSString *first = @"Jane";
    NSString *last  = @"Doe";
    NSString *title = @"The Title";
    
    // feed data
    PARStoreExample *store1 = [PARStoreExample storeWithURL:url deviceIdentifier:device1];
    PARStoreExample *store2 = [PARStoreExample storeWithURL:url deviceIdentifier:device2];
    [store1 loadNow];
    [store2 loadNow];
    NSNumber *beginTimestamp = [PARStore timestampNow];
    store1.first = first;
    store1.last  = last;
    store2.title = title;
    NSNumber *endTimestamp = [PARStore timestampNow];
    [store1 saveNow];
    [store2 saveNow];
    [store1 syncNow];
    [store2 syncNow];
    
    // actual changes
    NSArray *changes1 = [store1 changesSinceTimestamp:nil];
    NSArray *changes2 = [store2 changesSinceTimestamp:nil];
    XCTAssertEqualObjects(changes1, changes2, @"changes should be consistent in store1 and store2 because they come from the same URL");
    
    // change count
    XCTAssertTrue(changes1.count == 3, @"expected 3 changes but got %@", @(changes1.count));
    if (changes1.count != 3)
    {
        return;
    }
    
    // timestamp order
    NSArray *actualTimestamps = [changes1 valueForKey:@"timestamp"];
    NSArray *orderedTimestamps = [actualTimestamps sortedArrayUsingSelector:@selector(compare:)];
    XCTAssertEqualObjects(actualTimestamps, orderedTimestamps, @"timestamps returned by API should already be ordered but are: %@\n instead of ordered timestamps: %@", actualTimestamps, orderedTimestamps);
    
    // timestamp range
    for (NSNumber *timestamp in actualTimestamps)
    {
        XCTAssertTrue([timestamp compare:beginTimestamp] != NSOrderedAscending, @"timestamp %@ should be after beginTimestamp %@", timestamp, beginTimestamp);
        XCTAssertTrue([timestamp compare:endTimestamp] != NSOrderedDescending, @"timestamp %@ should be before endTimestamp %@", timestamp, endTimestamp);
    }
    
    // expected changes
    PARChange *change0 = changes1[0];
    PARChange *change1 = changes1[1];
    PARChange *change2 = changes1[2];
    NSArray *expectedChanges = @[
                                 [PARChange changeWithTimestamp:change0.timestamp parentTimestamp:change0.parentTimestamp key:@"first" propertyList:first],
                                 [PARChange changeWithTimestamp:change1.timestamp parentTimestamp:change1.parentTimestamp key:@"last" propertyList:last],
                                 [PARChange changeWithTimestamp:change2.timestamp parentTimestamp:change2.parentTimestamp key:@"title" propertyList:title],
                                 ];
    XCTAssertEqualObjects(changes1, expectedChanges, @"unexpected changes: %@", changes1);
}


#pragma mark - Testing Queues

// old bug now fixed
- (void) testStoreLoadNotificationDeadlock
{
	NSUUID *deviceUUID = [NSUUID UUID];
	NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
	
	// create a store
	PARStoreExample *store1 = [PARStoreExample storeWithURL:url deviceIdentifier:[deviceUUID UUIDString]];
	[store1 loadNow];
    NSString *title = @"The Title";
	store1.title = title;
	[store1 tearDownNow];
	
	// load store at same url again
	PARStoreExample *store2 = [PARStoreExample storeWithURL:url deviceIdentifier:[deviceUUID UUIDString]];
	
    // accessing a property on the dataQueue should not result in a dead-lock
	[[NSNotificationCenter defaultCenter] addObserverForName:PARStoreDidLoadNotification object:store2 queue:[NSOperationQueue mainQueue] usingBlock:^(NSNotification *note)
     {
         NSString *title2 = store2.title;
         title2 = nil;
     }];
	dispatch_semaphore_t sema = dispatch_semaphore_create(0);
	dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^
                   {
                       // fires did load notification on data queue, observer accesses layout, which also performs sync op on dataqueue
                       [store2 loadNow];
                       dispatch_semaphore_signal(sema);
                   });
    
	long waitResult = dispatch_semaphore_wait(sema, dispatch_time(DISPATCH_TIME_NOW, 10.0 * NSEC_PER_SEC));
	// NSLog(@"Wait result: %ld", waitResult);
	
	XCTAssertTrue(waitResult == 0, @"Timeout while waiting for document to load");
}


- (void)testWaitUntilFinished
{
	NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"SyncTest.parstore"];
	
    PARStoreExample *store1 = [PARStoreExample storeWithURL:url deviceIdentifier:@"1"];
    [store1 loadNow];
	XCTAssertTrue([store1 loaded], @"Store not loaded");
    
    // add second device --> should trigger a sync in the first store, though no change yet
	PARStoreExample *store2 = [PARStoreExample storeWithURL:url deviceIdentifier:@"2"];
    [store2 loadNow];
	XCTAssertTrue([store2 loaded], @"Store not loaded");
    
    // by throttling notifications, we increase the chance that the test will fail if `waitUntilFinished` is flawed
    store1.shouldThrottleNotifications = YES;
    store2.shouldThrottleNotifications = YES;
    
    // setup semaphores for the expected notifications after a change in store 1
    PARNotificationSemaphore *semaphore1 = [PARNotificationSemaphore semaphoreForNotificationName:PARStoreDidChangeNotification object:store1];
    PARNotificationSemaphore *semaphore2 = [PARNotificationSemaphore semaphoreForNotificationName:PARStoreDidSyncNotification object:store2];
    
    // change first store --> should trigger a notification change in the store 1 and a sync notification in store 2
	store1.title = @"New Title";
    
    // once `waitUntilFinished` returns, the change notification should have been sent
    [store1 waitUntilFinished];
    XCTAssertTrue(semaphore1.notificationWasPosted, @"change notification for store 1 not posted yet");
    
    // once `waitUntilFinished` returns, the sync notification should have been sent
    [store2 syncNow];
    [store2 waitUntilFinished];
    XCTAssertTrue(semaphore2.notificationWasPosted, @"sync notification for store 2 not posted yet");
    
    // timeout
    BOOL completedWithoutTimeout1 = [semaphore1 waitUntilNotificationWithTimeout:10.0];
    XCTAssertTrue(completedWithoutTimeout1, @"Timeout while waiting for store 1 change notification");
    BOOL completedWithoutTimeout2 = [semaphore2 waitUntilNotificationWithTimeout:10.0];
    XCTAssertTrue(completedWithoutTimeout2, @"Timeout while waiting for store 2 sync notification");
    
    // done
    [store1 tearDownNow];
    [store2 tearDownNow];
}


@end
