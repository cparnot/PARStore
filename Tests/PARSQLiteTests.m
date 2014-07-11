//  PARStore
//  Created by Charles Parnot on 7/10/14.
//  Copyright (c) 2014 Charles Parnot. All rights reserved.

#import "PARTestCase.h"
#import <sqlite3.h>

@interface PARSQLiteTests : PARTestCase

@end

@implementation PARSQLiteTests

- (void)setUp
{
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
}

- (void)tearDown
{
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}


#pragma mark - Testing Journal Modes

- (void)testJournalModeWal
{
    NSString *directory = [[self urlWithUniqueTmpDirectory] path];
    NSString *databaseName = @"test";
    NSString *databasePath = [self databasePathWithName:databaseName directory:directory];

    // create stack
    NSManagedObjectContext *moc = [self managedObjectContext];
    NSPersistentStore *store1 = [self addPersistentStoreWithCoordinator:moc.persistentStoreCoordinator storePath:databasePath readOnly:NO journalMode:@"WAL"];
    
    // add content
    NSUInteger numberOfObjectsToAdd = 100;
    for (NSUInteger i = 0; i < numberOfObjectsToAdd; i++)
    {
        [self addTestManagedObjectToPersistentStore:store1 managedObjectContext:moc];
    }
    
    // assert files
    NSString *wal = [self walPathWithName:databaseName directory:directory];
    NSString *shm = [self shmPathWithName:databaseName directory:directory];
    NSString *journal = [self journalPathWithName:databaseName directory:directory];
    [self assertFileExists:databasePath];
    [self assertFileExists:wal];
    [self assertFileExists:shm];
    [self assertFileIsEmpty:wal];
    [self assertFileDoesNotExist:journal];

    // save
    NSError *saveError = nil;
    BOOL saveSuccess = [moc save:&saveError];
    XCTAssertTrue(saveSuccess, @"error saving: %@", saveError);

    // assert files
    [self assertFileExists:databasePath];
    [self assertFileExists:wal];
    [self assertFileExists:shm];
    [self assertFileIsNotEmpty:wal];
    [self assertFileDoesNotExist:journal];

    // closing the database connection
    moc = nil;
    
    // assert files
    // the wal file is empty after checkpointing: http://www.sqlite.org/wal.html#ckpt
    [self assertFileExists:databasePath];
    [self assertFileExists:wal];
    [self assertFileExists:shm];
    [self assertFileIsEmpty:wal];
    [self assertFileDoesNotExist:journal];
}

- (void)testJournalModeTruncate
{
    NSString *directory = [[self urlWithUniqueTmpDirectory] path];
    NSString *databaseName = @"test";
    NSString *databasePath = [self databasePathWithName:databaseName directory:directory];
    
    // create stack
    NSManagedObjectContext *moc = [self managedObjectContext];
    NSPersistentStore *store1 = [self addPersistentStoreWithCoordinator:moc.persistentStoreCoordinator storePath:databasePath readOnly:NO journalMode:@"TRUNCATE"];
    
    // add content
    NSUInteger numberOfObjectsToAdd = 100;
    for (NSUInteger i = 0; i < numberOfObjectsToAdd; i++)
    {
        [self addTestManagedObjectToPersistentStore:store1 managedObjectContext:moc];
    }
    
    // assert files
    NSString *wal = [self walPathWithName:databaseName directory:directory];
    NSString *shm = [self shmPathWithName:databaseName directory:directory];
    NSString *journal = [self journalPathWithName:databaseName directory:directory];
    [self assertFileExists:databasePath];
    [self assertFileDoesNotExist:journal];
    [self assertFileDoesNotExist:wal];
    [self assertFileDoesNotExist:shm];
    
    // save
    NSError *saveError = nil;
    BOOL saveSuccess = [moc save:&saveError];
    XCTAssertTrue(saveSuccess, @"error saving: %@", saveError);
    
    // assert files
    [self assertFileExists:databasePath];
    [self assertFileExists:journal];
    [self assertFileIsEmpty:journal];
    [self assertFileDoesNotExist:wal];
    [self assertFileDoesNotExist:shm];
    
    // closing the database connection
    moc = nil;
    
    // assert files
    [self assertFileExists:databasePath];
    [self assertFileExists:journal];
    [self assertFileIsEmpty:journal];
    [self assertFileDoesNotExist:wal];
    [self assertFileDoesNotExist:shm];
}

- (void)testJournalModeDelete
{
    NSString *directory = [[self urlWithUniqueTmpDirectory] path];
    NSString *databaseName = @"test";
    NSString *databasePath = [self databasePathWithName:databaseName directory:directory];
    
    // create stack
    NSManagedObjectContext *moc = [self managedObjectContext];
    NSPersistentStore *store1 = [self addPersistentStoreWithCoordinator:moc.persistentStoreCoordinator storePath:databasePath readOnly:NO journalMode:@"TRUNCATE"];
    
    // add content
    NSUInteger numberOfObjectsToAdd = 100;
    for (NSUInteger i = 0; i < numberOfObjectsToAdd; i++)
    {
        [self addTestManagedObjectToPersistentStore:store1 managedObjectContext:moc];
    }
    
    // assert files
    NSString *wal = [self walPathWithName:databaseName directory:directory];
    NSString *shm = [self shmPathWithName:databaseName directory:directory];
    NSString *journal = [self journalPathWithName:databaseName directory:directory];
    [self assertFileExists:databasePath];
    [self assertFileDoesNotExist:journal];
    [self assertFileDoesNotExist:wal];
    [self assertFileDoesNotExist:shm];
    
    // save
    NSError *saveError = nil;
    BOOL saveSuccess = [moc save:&saveError];
    XCTAssertTrue(saveSuccess, @"error saving: %@", saveError);
    
    // assert files
    [self assertFileExists:databasePath];
    [self assertFileExists:journal];
    [self assertFileIsEmpty:journal];
    [self assertFileDoesNotExist:wal];
    [self assertFileDoesNotExist:shm];
    
    // closing the database connection
    moc = nil;
    
    // assert files
    [self assertFileExists:databasePath];
    [self assertFileExists:journal];
    [self assertFileIsEmpty:journal];
    [self assertFileDoesNotExist:wal];
    [self assertFileDoesNotExist:shm];
}

- (void)testJournalModePersist
{
    NSString *directory = [[self urlWithUniqueTmpDirectory] path];
    NSString *databaseName = @"test";
    NSString *databasePath = [self databasePathWithName:databaseName directory:directory];
    
    // create stack
    NSManagedObjectContext *moc = [self managedObjectContext];
    NSPersistentStore *store1 = [self addPersistentStoreWithCoordinator:moc.persistentStoreCoordinator storePath:databasePath readOnly:NO journalMode:@"PERSIST"];
    
    // add content
    NSUInteger numberOfObjectsToAdd = 100;
    for (NSUInteger i = 0; i < numberOfObjectsToAdd; i++)
    {
        [self addTestManagedObjectToPersistentStore:store1 managedObjectContext:moc];
    }
    
    // assert files
    NSString *wal = [self walPathWithName:databaseName directory:directory];
    NSString *shm = [self shmPathWithName:databaseName directory:directory];
    NSString *journal = [self journalPathWithName:databaseName directory:directory];
    [self assertFileExists:databasePath];
    [self assertFileDoesNotExist:journal];
    [self assertFileDoesNotExist:wal];
    [self assertFileDoesNotExist:shm];
    
    // save
    NSError *saveError = nil;
    BOOL saveSuccess = [moc save:&saveError];
    XCTAssertTrue(saveSuccess, @"error saving: %@", saveError);
    
    // assert files
    [self assertFileExists:databasePath];
    [self assertFileExists:journal];
    [self assertFileIsNotEmpty:journal];
    [self assertFileDoesNotExist:wal];
    [self assertFileDoesNotExist:shm];
    
    // closing the database connection
    moc = nil;
    
    // assert files
    [self assertFileExists:databasePath];
    [self assertFileExists:journal];
    [self assertFileIsNotEmpty:journal];
    [self assertFileDoesNotExist:wal];
    [self assertFileDoesNotExist:shm];
}


#pragma mark - Testing Journal Mode Changes

- (void)testWalToTruncate
{
    NSString *directory = [[self urlWithUniqueTmpDirectory] path];
    NSString *databaseName = @"test";
    NSString *databasePath = [self databasePathWithName:databaseName directory:directory];
    
    // create stack in WAL mode
    NSManagedObjectContext *moc = [self managedObjectContext];
    NSPersistentStore *store1 = [self addPersistentStoreWithCoordinator:moc.persistentStoreCoordinator storePath:databasePath readOnly:NO journalMode:@"WAL"];
    
    // add content
    NSUInteger numberOfObjectsToAdd = 100;
    for (NSUInteger i = 0; i < numberOfObjectsToAdd; i++)
    {
        [self addTestManagedObjectToPersistentStore:store1 managedObjectContext:moc];
    }
    
    // save
    NSError *saveError = nil;
    BOOL saveSuccess = [moc save:&saveError];
    XCTAssertTrue(saveSuccess, @"error saving: %@", saveError);

    // snapshot of the content
    NSArray *snapshotWal = [self allManagedObjectRepresentationsForManagedObjectContext:moc];
    
    // close the database completely
    moc = nil;

    // assert files
    NSString *wal = [self walPathWithName:databaseName directory:directory];
    NSString *shm = [self shmPathWithName:databaseName directory:directory];
    NSString *journal = [self journalPathWithName:databaseName directory:directory];
    [self assertFileExists:databasePath];
    [self assertFileExists:wal];
    [self assertFileExists:shm];
    [self assertFileIsEmpty:wal];
    [self assertFileDoesNotExist:journal];
    
    
    // reopen stack in TRUNCATE mode
    moc = [self managedObjectContext];
    [self addPersistentStoreWithCoordinator:moc.persistentStoreCoordinator storePath:databasePath readOnly:NO journalMode:@"TRUNCATE"];

    // snapshot of the content
    NSArray *snapshotTruncate = [self allManagedObjectRepresentationsForManagedObjectContext:moc];
    
    // close the database completely
    moc = nil;

    // assert files
    // for some reason, the shm file is left behind
    [self assertFileExists:databasePath];
    [self assertFileExists:journal];
    [self assertFileIsEmpty:journal];
    [self assertFileDoesNotExist:wal];
    [self assertFileExists:shm];

    // compare snapshots
    XCTAssert(snapshotWal.count == snapshotTruncate.count, @"snapshots have different number of elements: %@ and %@", @(snapshotWal.count), @(snapshotTruncate.count));
    NSEnumerator *e1 = snapshotWal.objectEnumerator;
    NSEnumerator *e2 = snapshotTruncate.objectEnumerator;
    NSManagedObject *mo1 = nil;
    NSManagedObject *mo2 = nil;
    while ((mo1 = e1.nextObject) && (mo2 = e2.nextObject))
    {
        NSString *foo1 = [mo1 valueForKey:@"foo"];
        NSString *foo2 = [mo2 valueForKey:@"foo"];
        NSString *bar1 = [mo1 valueForKey:@"bar"];
        NSString *bar2 = [mo2 valueForKey:@"bar"];
        XCTAssertEqualObjects(foo1, foo2, @"foo property should be the same");
        XCTAssertEqualObjects(bar1, bar2, @"bar property should be the same");
    }

    
    // remove the shm file from disk
    NSError *removeError;
    BOOL removeSuccess = [[NSFileManager defaultManager] removeItemAtPath:shm error:&removeError];
    XCTAssertTrue(removeSuccess, @"error removing file at path '%@': %@", shm, removeError);

    // open stack in TRUNCATE mode, again
    moc = [self managedObjectContext];
    [self addPersistentStoreWithCoordinator:moc.persistentStoreCoordinator storePath:databasePath readOnly:NO journalMode:@"TRUNCATE"];
    
    // snapshot of the content
    snapshotTruncate = [self allManagedObjectRepresentationsForManagedObjectContext:moc];
    
    // close the database completely
    moc = nil;
    
    // assert files
    [self assertFileExists:databasePath];
    [self assertFileExists:journal];
    [self assertFileIsEmpty:journal];
    [self assertFileDoesNotExist:wal];
    [self assertFileDoesNotExist:shm];
    
    // compare snapshots
    XCTAssert(snapshotWal.count == snapshotTruncate.count, @"snapshots have different number of elements: %@ and %@", @(snapshotWal.count), @(snapshotTruncate.count));
    e1 = snapshotWal.objectEnumerator;
    e2 = snapshotTruncate.objectEnumerator;
    mo1 = nil;
    mo2 = nil;
    while ((mo1 = e1.nextObject) && (mo2 = e2.nextObject))
    {
        NSString *foo1 = [mo1 valueForKey:@"foo"];
        NSString *foo2 = [mo2 valueForKey:@"foo"];
        NSString *bar1 = [mo1 valueForKey:@"bar"];
        NSString *bar2 = [mo2 valueForKey:@"bar"];
        XCTAssertEqualObjects(foo1, foo2, @"foo property should be the same");
        XCTAssertEqualObjects(bar1, bar2, @"bar property should be the same");
    }
}


#pragma mark - Path Utilities

- (NSString *)databasePathWithName:(NSString *)name directory:(NSString *)directory
{
    return [directory stringByAppendingPathComponent:[NSString stringWithFormat:@"%@.db", name]];
}

- (NSString *)walPathWithName:(NSString *)name directory:(NSString *)directory
{
    return [[self databasePathWithName:name directory:directory] stringByAppendingString:@"-wal"];
}

- (NSString *)shmPathWithName:(NSString *)name directory:(NSString *)directory
{
    return [[self databasePathWithName:name directory:directory] stringByAppendingString:@"-shm"];
}

- (NSString *)journalPathWithName:(NSString *)name directory:(NSString *)directory
{
    return [[self databasePathWithName:name directory:directory] stringByAppendingString:@"-journal"];
}


#pragma mark - Assertions

- (void)assertFileExists:(NSString *)path
{
    BOOL isDir = NO;
    BOOL fileExists = [[NSFileManager defaultManager] fileExistsAtPath:path isDirectory:&isDir];
    XCTAssert(fileExists && !isDir, @"file should exist but %@ at path %@", isDir ? @"is a directory" : @"does not exist", path);
}

- (void)assertFileDoesNotExist:(NSString *)path
{
    BOOL isDir = NO;
    BOOL fileExists = [[NSFileManager defaultManager] fileExistsAtPath:path isDirectory:&isDir];
    XCTAssert(!fileExists && !isDir, @"file should not exist but %@ at path %@", isDir ? @"is a directory" : @"exists", path);
}

- (void)assertFileIsEmpty:(NSString *)path
{
    NSDictionary *attributes = [[NSFileManager defaultManager] attributesOfItemAtPath:path error:NULL];
    NSNumber *fileSize = attributes[NSFileSize];
    XCTAssert(attributes != nil && [attributes[NSFileSize] isEqualToNumber:@(0.0)] == YES, @"file size is expected to be zero but is %@ at path: %@", fileSize, path);
}

- (void)assertFileIsNotEmpty:(NSString *)path
{
    NSDictionary *attributes = [[NSFileManager defaultManager] attributesOfItemAtPath:path error:NULL];
    NSNumber *fileSize = attributes[NSFileSize];
    XCTAssert(attributes != nil && [attributes[NSFileSize] isEqualToNumber:@(0.0)] == NO, @"file size is expected to be non-zero but is %@ at path: %@", fileSize, path);
    
}


#pragma mark - Setting Up Test Core Data Stacks

- (NSManagedObjectContext *)managedObjectContext
{
    NSPersistentStoreCoordinator *psc = [[NSPersistentStoreCoordinator alloc] initWithManagedObjectModel:[self managedObjectModel]];
    XCTAssertNotNil(psc, @"error creating persistent store coordinator");
    
    NSManagedObjectContext *moc = [[NSManagedObjectContext alloc] init];
    moc.persistentStoreCoordinator = psc;
    
    XCTAssertNotNil(psc, @"error creating managed object context");
    return moc;
}

- (NSPersistentStore *)addPersistentStoreWithCoordinator:(NSPersistentStoreCoordinator *)psc storePath:(NSString *)storePath readOnly:(BOOL)readOnly journalMode:(NSString *)journalMode
{
    NSError *localError = nil;
    NSDictionary *pragmas = @{
                              @"journal_mode": journalMode ?: @"WAL"
                              };
    NSDictionary *storeOptions = @{
                                   NSMigratePersistentStoresAutomaticallyOption : @YES,
                                   NSInferMappingModelAutomaticallyOption:        @YES,
                                   NSReadOnlyPersistentStoreOption:               @(readOnly),
                                   NSSQLitePragmasOption:                         pragmas,
                                   };
    NSPersistentStore *store = [psc addPersistentStoreWithType:NSSQLiteStoreType configuration:nil URL:[NSURL fileURLWithPath:storePath] options:storeOptions error:&localError];

    XCTAssertNotNil(store, @"error creating the store at path '%@': %@", storePath, localError);
    return store;
}

- (NSManagedObjectModel *)managedObjectModel
{
    static dispatch_once_t pred = 0;
    static NSManagedObjectModel *mom = nil;
    dispatch_once(&pred,
                  ^{
                      NSAttributeDescription *fooAttribute = [[NSAttributeDescription alloc] init];
                      fooAttribute.name = @"foo";
                      fooAttribute.indexed = YES;
                      fooAttribute.attributeType = NSStringAttributeType;

                      NSAttributeDescription *barAttribute = [[NSAttributeDescription alloc] init];
                      barAttribute.name = @"bar";
                      barAttribute.attributeType = NSStringAttributeType;
                      
                      NSEntityDescription *entity = [[NSEntityDescription alloc] init];
                      entity.name = @"TestEntity";
                      entity.properties = @[barAttribute, fooAttribute];
                      
                      mom = [[NSManagedObjectModel alloc] init];
                      mom.entities = @[entity];
                  });

    XCTAssertNotNil(mom, @"managed object model was not properly created");
    return mom;
}

- (NSManagedObject *)addTestManagedObjectToPersistentStore:(NSPersistentStore *)store managedObjectContext:(NSManagedObjectContext *)moc
{
    XCTAssertTrue(store.persistentStoreCoordinator == moc.persistentStoreCoordinator, @"inconsistent store coordinators");
    
    NSManagedObjectModel *model = store.persistentStoreCoordinator.managedObjectModel;
    NSEntityDescription *entity = model.entities.firstObject;
    NSManagedObject *managedObject = [NSEntityDescription insertNewObjectForEntityForName:entity.name inManagedObjectContext:moc];
    [moc assignObject:managedObject toPersistentStore:store];
    [managedObject setValue:[[NSUUID UUID] UUIDString] forKey:@"foo"];
    [managedObject setValue:[[NSUUID UUID] UUIDString] forKey:@"bar"];
    
    XCTAssertNotNil(managedObject, @"managed object could not be created");
    return managedObject;
}

- (NSArray *)allManagedObjectRepresentationsForManagedObjectContext:(NSManagedObjectContext *)moc
{
    NSEntityDescription *entity = moc.persistentStoreCoordinator.managedObjectModel.entities.firstObject;

    NSError *localError = nil;
    NSFetchRequest *fetchRequest = [[NSFetchRequest alloc] init];
    fetchRequest.entity = entity;
    fetchRequest.predicate = nil;
    fetchRequest.resultType = NSDictionaryResultType;
    NSArray *results = [moc executeFetchRequest:fetchRequest error:&localError];
    results = [results sortedArrayUsingDescriptors:@[[NSSortDescriptor sortDescriptorWithKey:@"foo" ascending:YES]]];

    XCTAssertNotNil(results, @"error fetching ");
    return results;
}

@end
