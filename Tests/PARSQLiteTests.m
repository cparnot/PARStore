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
    NSDictionary *rep1 = nil;
    NSDictionary *rep2 = nil;
    while ((rep1 = e1.nextObject) && (rep2 = e2.nextObject))
    {
        NSString *foo1 = rep1[@"foo"];
        NSString *foo2 = rep2[@"foo"];
        NSString *bar1 = rep1[@"bar"];
        NSString *bar2 = rep2[@"bar"];
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
    rep1 = nil;
    rep2 = nil;
    while ((rep1 = e1.nextObject) && (rep2 = e2.nextObject))
    {
        NSString *foo1 = rep1[@"foo"];
        NSString *foo2 = rep2[@"foo"];
        NSString *bar1 = rep1[@"bar"];
        NSString *bar2 = rep2[@"bar"];
        XCTAssertEqualObjects(foo1, foo2, @"foo property should be the same");
        XCTAssertEqualObjects(bar1, bar2, @"bar property should be the same");
    }
}


#pragma mark - SQLite Tests

- (void)testSqliteHotJournalCapture
{
    // paths
    NSString *directory = [[self urlWithUniqueTmpDirectory] path];
    NSString *databaseName = @"test";
    NSString *databasePath = [self databasePathWithName:databaseName directory:directory];
    NSString *journal = [self journalPathWithName:databaseName directory:directory];
    NSString *journalCopy = [self journalCopyPathWithName:databaseName directory:directory];
    
    // create database using Core Data
    NSManagedObjectContext *moc = [self managedObjectContextForDatabaseAtPath:databasePath populate:YES];
    moc = nil;
    
    // assert files
    [self assertFileExists:databasePath];
    [self assertFileExists:journal];
    [self assertFileDoesNotExist:journalCopy];
    [self assertFileIsEmpty:journal];

    // capture sqlite database in "hot" state
    [self captureHotJournalWithDatabaseName:databaseName directory:directory addFooValue:@"ZZZ" barValue:@"ZZZ"];

    // assert files
    [self assertFileExists:databasePath];
    [self assertFileExists:journal];
    [self assertFileExists:journalCopy];
    [self assertFileIsEmpty:journal];
    [self assertFileIsNotEmpty:journalCopy];
}

- (void)testSqliteToCoreData
{
    // database path
    NSString *directory = [[self urlWithUniqueTmpDirectory] path];
    NSString *databaseName = @"test";
    NSString *databasePath = [self databasePathWithName:databaseName directory:directory];
    
    // create database using Core Data
    NSManagedObjectContext *moc = [self managedObjectContextForDatabaseAtPath:databasePath populate:YES];
    NSArray *snapshot1 = [self allManagedObjectRepresentationsForManagedObjectContext:moc];
    moc = nil;
    
    // capture sqlite database in "hot" state
    [self captureHotJournalWithDatabaseName:databaseName directory:directory addFooValue:@"ZZZ" barValue:@"ZZZ"];
    
    // there should be a new row in Core Data
    moc = [self managedObjectContextForDatabaseAtPath:databasePath populate:NO];
    NSArray *snapshot2 = [self allManagedObjectRepresentationsForManagedObjectContext:moc];
    moc = nil;
    
    // compare snapshots
    snapshot1 = [snapshot1 arrayByAddingObject:@{@"foo" : @"ZZZ", @"bar" : @"ZZZ"}];
    XCTAssert(snapshot1.count == snapshot2.count, @"snapshots have different number of elements: %@ and %@", @(snapshot1.count), @(snapshot2.count));
    NSEnumerator *e1 = snapshot1.objectEnumerator;
    NSEnumerator *e2 = snapshot2.objectEnumerator;
    NSDictionary *rep1 = nil;
    NSDictionary *rep2 = nil;
    while ((rep1 = e1.nextObject) && (rep2 = e2.nextObject))
    {
        NSString *foo1 = rep1[@"foo"];
        NSString *foo2 = rep2[@"foo"];
        NSString *bar1 = rep1[@"bar"];
        NSString *bar2 = rep2[@"bar"];
        XCTAssertEqualObjects(foo1, foo2, @"foo property should be the same");
        XCTAssertEqualObjects(bar1, bar2, @"bar property should be the same");
    }
}

- (void)testSqliteHotJournal
{
    // database path
    NSString *directory = [[self urlWithUniqueTmpDirectory] path];
    NSString *databaseName = @"test";
    NSString *databasePath = [self databasePathWithName:databaseName directory:directory];
    NSString *databaseCopy = [self databaseCopyPathWithName:databaseName directory:directory];
    NSString *journalCopy = [self journalCopyPathWithName:databaseName directory:directory];
    
    // create database using Core Data
    NSManagedObjectContext *moc = [self managedObjectContextForDatabaseAtPath:databasePath populate:YES];
    NSArray *snapshot1 = [self allManagedObjectRepresentationsForManagedObjectContext:moc];
    moc = nil;
    
    // capture sqlite database in "hot" state
    [self captureHotJournalWithDatabaseName:databaseName directory:directory addFooValue:@"ZZZ" barValue:@"ZZZ"];
    
    // open the "hot" database with Core Data
    moc = [self managedObjectContextForDatabaseAtPath:databaseCopy populate:NO];
    NSArray *snapshot2 = [self allManagedObjectRepresentationsForManagedObjectContext:moc];
    moc = nil;
    
    // Merely opening the database, without changing the content, like we just did, apparently does **not** reset the journal database. But the SQLite docs indicate that if the journal is "hot", it should be reset (see: http://www.sqlite.org/lockingv3.html#hot_journals). It is thus possible that the above does not generate a journal considered "hot"
    [self assertFileIsNotEmpty:journalCopy];

    // the "hot" database should have the same content as the initial database, since the new content was not committed
    XCTAssert(snapshot1.count == snapshot2.count, @"snapshots have different number of elements: %@ and %@", @(snapshot1.count), @(snapshot2.count));
    NSEnumerator *e1 = snapshot1.objectEnumerator;
    NSEnumerator *e2 = snapshot2.objectEnumerator;
    NSDictionary *rep1 = nil;
    NSDictionary *rep2 = nil;
    while ((rep1 = e1.nextObject) && (rep2 = e2.nextObject))
    {
        NSString *foo1 = rep1[@"foo"];
        NSString *foo2 = rep2[@"foo"];
        NSString *bar1 = rep1[@"bar"];
        NSString *bar2 = rep2[@"bar"];
        XCTAssertEqualObjects(foo1, foo2, @"foo property should be the same");
        XCTAssertEqualObjects(bar1, bar2, @"bar property should be the same");
    }

    // opening the "hot" database **and** modifying content will reset the journal file
    [self captureHotJournalWithDatabaseName:[self databaseCopyNameWithName:databaseName] directory:directory addFooValue:@"ZZZ" barValue:@"ZZZ"];
    [self assertFileExists:databaseCopy];
    [self assertFileExists:journalCopy];
    [self assertFileIsEmpty:journalCopy];
}

- (void)testSqliteSwapHotJournal
{
    // database path
    NSString *directory = [[self urlWithUniqueTmpDirectory] path];
    NSString *databaseName = @"test";
    NSString *databasePath = [self databasePathWithName:databaseName directory:directory];
    NSString *databaseCopy = [self databaseCopyPathWithName:databaseName directory:directory];
    NSString *journal = [self journalPathWithName:databaseName directory:directory];
    NSString *journalCopy = [self journalCopyPathWithName:databaseName directory:directory];
    
    // create database using Core Data
    NSManagedObjectContext *moc = [self managedObjectContextForDatabaseAtPath:databasePath populate:YES];
    NSArray *snapshot1 = [self allManagedObjectRepresentationsForManagedObjectContext:moc];
    moc = nil;
    
    // capture sqlite database in "hot" state
    [self captureHotJournalWithDatabaseName:databaseName directory:directory addFooValue:@"ZZZ" barValue:@"ZZZ"];
    
    // copy the hot journal to apply it to the non-hot database --> "heated" database
    [[NSFileManager defaultManager] removeItemAtPath:journal error:NULL];
    [[NSFileManager defaultManager] copyItemAtPath:journalCopy toPath:journal error:NULL];
    [self assertFileExists:journal];
    [self assertFileIsNotEmpty:journal];
    
    // open the "heated" database with Core Data
    moc = [self managedObjectContextForDatabaseAtPath:databasePath populate:NO];
    NSArray *snapshot2 = [self allManagedObjectRepresentationsForManagedObjectContext:moc];
    moc = nil;
    
    // Merely opening the database, without changing the content, like we just did, apparently does **not** reset the journal database. But the SQLite docs indicate that if the journal is "hot", it should be reset (see: http://www.sqlite.org/lockingv3.html#hot_journals). It is thus possible that the above does not generate a journal considered "hot"
    [self assertFileIsNotEmpty:journal];
    
    // adding the "hot" journal should not have change the content of the database, since it was not committed
    snapshot1 = [snapshot1 arrayByAddingObject:@{@"foo" : @"ZZZ", @"bar" : @"ZZZ"}];
    XCTAssert(snapshot1.count == snapshot2.count, @"snapshots have different number of elements: %@ and %@", @(snapshot1.count), @(snapshot2.count));
    NSEnumerator *e1 = snapshot1.objectEnumerator;
    NSEnumerator *e2 = snapshot2.objectEnumerator;
    NSDictionary *rep1 = nil;
    NSDictionary *rep2 = nil;
    while ((rep1 = e1.nextObject) && (rep2 = e2.nextObject))
    {
        NSString *foo1 = rep1[@"foo"];
        NSString *foo2 = rep2[@"foo"];
        NSString *bar1 = rep1[@"bar"];
        NSString *bar2 = rep2[@"bar"];
        XCTAssertEqualObjects(foo1, foo2, @"foo property should be the same");
        XCTAssertEqualObjects(bar1, bar2, @"bar property should be the same");
    }
    
    // opening the "heated" database **and** modifying content will reset the journal file
    [self captureHotJournalWithDatabaseName:[self databaseCopyNameWithName:databaseName] directory:directory addFooValue:@"ZZZ" barValue:@"ZZZ"];
    [self assertFileExists:databaseCopy];
    [self assertFileExists:journalCopy];
    [self assertFileIsEmpty:journalCopy];
}

- (NSManagedObjectContext *)managedObjectContextForDatabaseAtPath:(NSString *)databasePath populate:(BOOL)populate
{
    // create Core Data stack
    NSManagedObjectContext *moc = [self managedObjectContext];
    NSPersistentStore *store1 = [self addPersistentStoreWithCoordinator:moc.persistentStoreCoordinator storePath:databasePath readOnly:NO journalMode:@"TRUNCATE"];
    
    // add content
    NSUInteger numberOfObjectsToAdd = populate ? 100 : 0;
    for (NSUInteger i = 0; i < numberOfObjectsToAdd; i++)
    {
        [self addTestManagedObjectToPersistentStore:store1 managedObjectContext:moc];
    }
    
    // save and tear down Core Data
    NSError *saveError = nil;
    BOOL saveSuccess = [moc save:&saveError];
    XCTAssertTrue(saveSuccess, @"error saving: %@", saveError);
    
    return moc;
}

// - open database using sqlite
// - bring it to a state where a journal file is created (in the middle of a transaction)
// - capture the db and journal file by copying the file
// - tear things down
- (void)captureHotJournalWithDatabaseName:(NSString *)databaseName directory:(NSString *)directory addFooValue:(NSString *)fooValue barValue:(NSString *)barValue
{
    NSString *databasePath = [self databasePathWithName:databaseName directory:directory];
    NSString *databaseCopy = [self databaseCopyPathWithName:databaseName directory:directory];
    NSString *journal = [self journalPathWithName:databaseName directory:directory];
    NSString *journalCopy = [self journalCopyPathWithName:databaseName directory:directory];

    // open sqlite
    sqlite3 *sqlitedb = [self openSqliteDatabaseAtPath:databasePath];
    [self executeStatement:@"PRAGMA journal_mode = TRUNCATE" sqliteDatabase:sqlitedb];
    
    // set up a transaction
	[self executeStatement:@"BEGIN IMMEDIATE TRANSACTION" sqliteDatabase:sqlitedb];
    
    // add a row
    NSString *entityName = [self managedObjectModel].entitiesByName.allKeys.firstObject;
    NSString *tableName = [@"Z" stringByAppendingString:entityName];
    NSString *insertStatement = [NSString stringWithFormat:@"INSERT INTO %@ (Z_ENT, Z_OPT, ZFOO, ZBAR) VALUES (1, 1, '%@', '%@')", tableName, fooValue ?: @"value1", barValue ?: @"value2"];
	[self executeStatement:insertStatement sqliteDatabase:sqlitedb];
    
    // make a copy of the journal file, which is not empty at this point
    [self assertFileIsNotEmpty:journal];
    [[NSFileManager defaultManager] copyItemAtPath:journal toPath:journalCopy error:NULL];
    [[NSFileManager defaultManager] copyItemAtPath:databasePath toPath:databaseCopy error:NULL];
    [self assertFileExists:journalCopy];
    [self assertFileExists:databaseCopy];
    
    // end transaction
	[self executeStatement:@"COMMIT TRANSACTION" sqliteDatabase:sqlitedb];
    
    // close sqlite
    [self closeSqliteDatabase:sqlitedb];
}


#pragma mark - SQLite Utilities

- (sqlite3 *)openSqliteDatabaseAtPath:(NSString *)databasePath
{
    sqlite3 *sqlitedb;
    int err = sqlite3_open([databasePath fileSystemRepresentation], &sqlitedb);
    XCTAssertEqual(err, SQLITE_OK, @"error opening sqlite database: %@ - %@", @(err), @(sqlite3_errmsg(sqlitedb)));
    if (err != SQLITE_OK)
    {
        // NOTE: according to the docs for sqlite3_open, a database handle is returned even on
        // error (it will only be NULL if SQLite is unable to allocate memory), and the SQLite
        // close command should be used to release any resources associated with the db handle
        if (sqlitedb != NULL)
            [self closeSqliteDatabase:sqlitedb];
        return NULL;
    }

    XCTAssert(sqlitedb != NULL, @"sqlite handle should not be nil after opening the database");
    return sqlitedb;
}

- (BOOL)closeSqliteDatabase:(sqlite3 *)sqlitedb
{
    __block int err = sqlite3_close(sqlitedb);
    
    XCTAssertNotEqual(err, SQLITE_BUSY, @"Database busy when closing");
    XCTAssertEqual(err, SQLITE_OK, @"error closing sqlite database: %@ - %@", @(err), @(sqlite3_errmsg(sqlitedb)));

    return err != SQLITE_OK;
}

- (BOOL)executeStatement:(NSString *)statement sqliteDatabase:(sqlite3 *)sqlitedb
{
    int err = sqlite3_exec(sqlitedb, [statement UTF8String], NULL, NULL, NULL);
    XCTAssertEqual(err, SQLITE_OK, @"error executing statement: %@ - %@", @(err), @(sqlite3_errmsg(sqlitedb)));
    return err != SQLITE_OK;
}


#pragma mark - Path Utilities

- (NSString *)databaseCopyNameWithName:(NSString *)name
{
    return [name stringByAppendingString:@"-copy"];
}

- (NSString *)databasePathWithName:(NSString *)name directory:(NSString *)directory
{
    return [directory stringByAppendingPathComponent:[NSString stringWithFormat:@"%@.db", name]];
}

- (NSString *)databaseCopyPathWithName:(NSString *)name directory:(NSString *)directory
{
    return [self databasePathWithName:[self databaseCopyNameWithName:name] directory:directory];
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

- (NSString *)journalCopyPathWithName:(NSString *)name directory:(NSString *)directory
{
    return [self journalPathWithName:[self databaseCopyNameWithName:name] directory:directory];
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
    XCTAssert(attributes != nil && [fileSize isEqualToNumber:@(0.0)] == YES, @"file size is expected to be zero but is %@ at path: %@", fileSize, path);
}

- (void)assertFileIsNotEmpty:(NSString *)path
{
    NSDictionary *attributes = [[NSFileManager defaultManager] attributesOfItemAtPath:path error:NULL];
    NSNumber *fileSize = attributes[NSFileSize];
    XCTAssert(attributes != nil && [fileSize isEqualToNumber:@(0.0)] == NO, @"file size is expected to be non-zero but is %@ at path: %@", fileSize, path);
    
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
