//  PARStoreTests
//  Created by Charles Parnot on 3/2/13.
//  Copyright (c) 2013 Charles Parnot. All rights reserved.

#import "PARStoreTests.h"
#import "PARStoreExample.h"
#import "PARNotificationSemaphore.h"

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

- (void)testCreateThenLoadDocument
{
    // first load = create and load store
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStoreExample *document1 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document1 loadNow];
    XCTAssertTrue([document1 loaded], @"Document not loaded");
    [document1 closeNow];
    XCTAssertFalse([document1 loaded], @"Document should not be loaded after closing it");
    document1 = nil;
    
    // second load = load document
    PARStoreExample *document2 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document2 loadNow];
    XCTAssertTrue([document2 loaded], @"Document not loaded");
    [document2 closeNow];
    XCTAssertFalse([document2 loaded], @"Document should not be loaded after closing it");
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
    [document1 closeNow];
    XCTAssertFalse([document1 deleted], @"Document should not marked as deleted anymore after closing it");
    document1 = nil;
}

- (void)testFilePackageIsNotDirectory
{
    // create and load document
    NSURL *url = [[self urlWithUniqueTmpDirectory] URLByAppendingPathComponent:@"doc.parstore"];
    PARStoreExample *sound1 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [sound1 loadNow];
    XCTAssertTrue([sound1 loaded], @"Document not loaded");
    [sound1 closeNow];
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
    [store2 closeNow];
    store2 = nil;
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
    
    [store1 closeNow];
    [store2 closeNow];
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
    
    [store1 closeNow];
    [store2 closeNow];
}

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
	[store1 closeNow];
	
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
    [document1 closeNow];
    document1 = nil;
    
    // second load = load document and compare data
    PARStoreExample *document2 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document2 loadNow];
    NSString *actualTitle = [document2 propertyListValueForKey:@"title"];
    NSString *actualFirst = document2.first;
    XCTAssertEqualObjects(actualTitle, title, @"unexpected 'title' value after closing and reopening a document: '%@' instead of '%@'", actualTitle, title);
    XCTAssertEqualObjects(actualFirst, first, @"unexpected 'first' value after closing and reopening a document: '%@' instead of '%@'", actualFirst, first);
    [document2 closeNow];
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
    [document1 closeNow];
    document1 = nil;
    
    // second load = load document and compare data
    PARStoreExample *document2 = [PARStoreExample storeWithURL:url deviceIdentifier:[self deviceIdentifierForTest]];
    [document2 loadNow];
    NSDictionary *actualEntries = document2.allRelevantValues;
    NSString *actualTitle = actualEntries[@"title"];
    NSString *actualFirst = actualEntries[@"first"];
    XCTAssertEqualObjects(actualTitle, title, @"unexpected 'title' value after closing and reopening a document: '%@' instead of '%@'", actualTitle, title);
    XCTAssertEqualObjects(actualFirst, first, @"unexpected 'first' value after closing and reopening a document: '%@' instead of '%@'", actualFirst, first);
    [document2 closeNow];
    document2 = nil;
}

- (void)testMostRecentTimestamp
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
    NSNumber *timestamp11 = [store1 mostRecentTimestampWithDeviceIdentifier:@"1"];
    NSNumber *timestamp12 = [store1 mostRecentTimestampWithDeviceIdentifier:@"2"];
    NSNumber *timestamp21 = [store2 mostRecentTimestampWithDeviceIdentifier:@"1"];
    NSNumber *timestamp22 = [store2 mostRecentTimestampWithDeviceIdentifier:@"2"];
    XCTAssertNotNil(timestamp11, @"timestamp expected in store 1");
    XCTAssertNotNil(timestamp21, @"timestamp expected in store 1");
    XCTAssertEqualObjects(timestamp11, timestamp21, @"");
    XCTAssertNil(timestamp12, @"no timestamp expected in store 2");
    XCTAssertNil(timestamp22, @"no timestamp expected in store 2");
    
    [store1 closeNow];
    [store2 closeNow];
}


@end
