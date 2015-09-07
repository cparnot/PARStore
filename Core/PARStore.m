//  PARStore
//  Authors: Charles Parnot and Joris Kluivers
//  Licensed under the terms of the BSD License, see license terms in 'LICENSE-BSD.txt'

#import "PARStore.h"
#import "NSError+Factory.h"
#import <CoreData/CoreData.h>

#define ErrorLog(fmt, ...) NSLog(fmt, ##__VA_ARGS__)

#ifdef DEBUG
#define DebugLog(fmt, ...) NSLog(fmt, ##__VA_ARGS__)
#else
#define DebugLog(fmt, ...) do {  } while(0)
#endif


NSString *PARStoreDidLoadNotification     = @"PARStoreDidLoadNotification";
NSString *PARStoreDidTearDownNotification = @"PARStoreDidTearDownNotification";
NSString *PARStoreDidDeleteNotification   = @"PARStoreDidDeleteNotification";
NSString *PARStoreDidChangeNotification   = @"PARStoreDidChangeNotification";
NSString *PARStoreDidSyncNotification     = @"PARStoreDidSyncNotification";


@interface PARStore ()
@property (readwrite, copy) NSURL *storeURL;
@property (readwrite, copy) NSString *deviceIdentifier;

// databaseQueue serializes access to all CoreData related stuff
@property (retain) PARDispatchQueue *databaseQueue;
@property (retain) NSManagedObjectContext *_managedObjectContext;
@property (retain) NSPersistentStore *readwriteDatabase;
@property (copy) NSArray *readonlyDatabases;
@property (retain) NSMutableDictionary *databaseTimestamps;
@property (copy) NSDictionary *keyTimestamps;

// memoryQueue serializes access to in-memory storage
// to avoid deadlocks, the memoryQueue should never schedule synchronous blocks in databaseQueue (but the opposite is fine)
@property (retain) PARDispatchQueue *memoryQueue;
@property (retain, nonatomic) NSMutableDictionary *_memory;
@property (readwrite, nonatomic) BOOL _loaded;
@property (readwrite, nonatomic) BOOL _deleted;
@property (readwrite, nonatomic) BOOL _inMemory;
@property (retain, nonatomic) NSMutableDictionary *_memoryFileData;
@property (retain) NSMutableDictionary *_memoryKeyTimestamps;

// queue for the notifications
@property (retain) PARDispatchQueue *notificationQueue;

// queue needed for NSFilePresenter protocol
@property (retain) NSOperationQueue *presenterQueue;

// responding to file system events (Mac only)
#if TARGET_OS_IPHONE | TARGET_IPHONE_SIMULATOR
#elif TARGET_OS_MAC
@property (strong) PARDispatchQueue *fileSystemEventQueue;
@property FSEventStreamRef eventStreamDevices;
@property FSEventStreamRef eventStreamLogs;
#endif

- (void)createFileSystemEventQueue;
- (void)startFileSystemEventStreams;
- (void)refreshFileSystemEventStreamLogs;
- (void)stopFileSystemEventStreams;

@end



@implementation PARStore

+ (id)storeWithURL:(NSURL *)url deviceIdentifier:(NSString *)identifier
{
    PARStore *store = [[self alloc] init];
    store.storeURL = url;
    store.deviceIdentifier = identifier;
    
    // queue labels appear in crash reports and other debugging info
    NSString *urlLabel = [[url lastPathComponent] stringByReplacingOccurrencesOfString:@"." withString:@"_"];
    NSString *databaseQueueLabel = [PARDispatchQueue labelByPrependingBundleIdentifierToString:[NSString stringWithFormat:@"database.%@", urlLabel]];
    NSString *memoryQueueLabel = [PARDispatchQueue labelByPrependingBundleIdentifierToString:[NSString stringWithFormat:@"memory.%@", urlLabel]];
    NSString *notificationQueueLabel = [PARDispatchQueue labelByPrependingBundleIdentifierToString:[NSString stringWithFormat:@"notifications.%@", urlLabel]];
    store.databaseQueue     = [PARDispatchQueue dispatchQueueWithLabel:databaseQueueLabel];
    store.memoryQueue       = [PARDispatchQueue dispatchQueueWithLabel:memoryQueueLabel];
    store.notificationQueue = [PARDispatchQueue dispatchQueueWithLabel:notificationQueueLabel];
    [store createFileSystemEventQueue];
    
    // misc initializations
    store.databaseTimestamps = [NSMutableDictionary dictionary];
    store.presenterQueue = [[NSOperationQueue alloc] init];
    [store.presenterQueue setMaxConcurrentOperationCount:1];
    store._memory = [NSMutableDictionary dictionary];
    store._memoryFileData = [NSMutableDictionary dictionary];
    store._memoryKeyTimestamps = [NSMutableDictionary dictionary];
    store._loaded = NO;
    store._deleted = NO;
	
    return store;
}

+ (id)inMemoryStore
{
    PARStore *store = [self storeWithURL:nil deviceIdentifier:nil];
    store._inMemory = YES;
    
    // no database layer, already loaded
    store.databaseQueue = nil;
    store._loaded = YES;
    
    return store;
}


#pragma mark - Loading / Closing Memory Layer

// loading = populating the memory cache with the values from disk
// loading is only done once, before adding or accessing values

- (BOOL)loaded
{
    __block BOOL loaded = NO;
    [self.memoryQueue dispatchSynchronously:^{ loaded = self._loaded; }];
    return loaded;
}

- (void)_load
{
    NSAssert([self.databaseQueue isInCurrentQueueStack], @"%@:%@ should only be called from within the database queue", [self class], NSStringFromSelector(_cmd));

    NSManagedObjectContext *moc = [self managedObjectContext];
    if (moc == nil)
    {
        return;
    }

    if ([self loaded])
    {
        return;
    }
    
    [self _sync];
    
    if ([self loaded])
    {
        DebugLog(@"%@ added as file presenter", self.deviceIdentifier);
        [NSFileCoordinator addFilePresenter:self];
        [self startFileSystemEventStreams];
    }
}

- (void)load
{
    [self.databaseQueue dispatchAsynchronously:^{ [self _load]; }];
}

- (void)loadNow
{
    if ([self.memoryQueue isInCurrentQueueStack])
    {
        ErrorLog(@"To avoid deadlocks, %@ should not be called within a transaction. Bailing out.", NSStringFromSelector(_cmd));
        return;
    }
    [self.databaseQueue dispatchSynchronously:^{ [self _load]; }];
}

- (void)_tearDown
{
    NSAssert([self.memoryQueue isInCurrentQueueStack], @"%@:%@ should only be called from within the memory queue", [self class], NSStringFromSelector(_cmd));

    // reset database
    // to avoid deadlocks, it is **critical** that the call into the database queue be asynchronous
    if (!self._deleted && !self._inMemory)
        [self.databaseQueue dispatchAsynchronously:^
         {
             [self _save:NULL];
             [self _closeDatabase];
             [NSFileCoordinator removeFilePresenter:self];
             [self stopFileSystemEventStreams];
             self.databaseTimestamps = nil;
         }];
    
    // reset in-memory info
    self._memory = [NSMutableDictionary dictionary];
    self._loaded = NO;
    self._deleted = NO;

    // to make sure the database is saved when the notification is received, the call is scheduled from within the database queue
    [self.databaseQueue dispatchAsynchronously:^
    {
        [self postNotificationWithName:PARStoreDidTearDownNotification userInfo:nil];
    }];
}

- (void)tearDown
{
    [self.memoryQueue dispatchAsynchronously:^{ [self _tearDown]; }];
}

- (void)tearDownNow
{
    if ([self.memoryQueue isInCurrentQueueStack])
    {
        ErrorLog(@"To avoid deadlocks, %@ should not be called within a transaction. Bailing out.", NSStringFromSelector(_cmd));
        return;
    }
    [self.memoryQueue       dispatchSynchronously:^{ [self _tearDown]; }];
    [self.databaseQueue     dispatchSynchronously:^{ }];
    [self.notificationQueue dispatchSynchronously:^{ }];
}

- (void)waitUntilFinished
{
    if ([self.memoryQueue isInCurrentQueueStack])
    {
        ErrorLog(@"To avoid deadlocks, %@ should not be called within a transaction. Bailing out.", NSStringFromSelector(_cmd));
        return;
    }
    [self.memoryQueue       dispatchSynchronously:^{ }];
    [self.databaseQueue     dispatchSynchronously:^{ [self _save:NULL]; [self _sync]; }];
    [self.notificationQueue dispatchSynchronously:^{ }];
}

// since `self` is retained by the blocks used for scheduling a 'save' or a 'sync', we do not expect to be in a situation where the store is in an unsaved or inconsistent state when dealloc-ed
// the database queue should not have any timer set anymore
- (void)dealloc
{
    // do not access '_loaded' via the queue or via the safe `loaded` accessor, to avoid further waiting for a queue: if closed properly, this will be safe, and otherwise, we are already in trouble
    NSUInteger timerCount = _databaseQueue.timerCount;
    if (timerCount > 0)
        ErrorLog(@"Unexpected timer count of %@ for the database queue of store at path: %@", @(timerCount), [self.storeURL path]);
}


#pragma mark - Paths

NSString *PARDatabaseFileName = @"logs.db";
NSString *PARDevicesDirectoryName = @"devices";

- (NSString *)deviceRootPath
{
    if (self._inMemory || ![self.storeURL isFileURL])
        return nil;
    return [[self.storeURL path] stringByAppendingPathComponent:PARDevicesDirectoryName];
}

- (NSString *)databasePathForDeviceIdentifier:(NSString *)deviceIdentifier
{
    return [[[self deviceRootPath] stringByAppendingPathComponent:deviceIdentifier] stringByAppendingPathComponent:PARDatabaseFileName];
}

- (NSString *)deviceIdentifierForDatabasePath:(NSString *)path
{
    return [[path stringByDeletingLastPathComponent] lastPathComponent];
}

- (NSString *)readwriteDirectoryPath
{
    if (self._inMemory || ![self.storeURL isFileURL])
        return nil;
    return [[self deviceRootPath] stringByAppendingPathComponent:self.deviceIdentifier];
}

- (BOOL)prepareFilePackageWithError:(NSError **)error
{
    if (self._inMemory)
        return YES;
	
    __block BOOL success = YES;
    __block NSError *localError = nil;

	// URL should be a file
	if (![self.storeURL isFileURL])
	{
        localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"%@ only supports files and cannot create file package with URL: %@", NSStringFromClass([self class]), self.storeURL] underlyingError:nil];
		success = NO;
	}
		
	// file should be a directory
	NSString *storePath = [self.storeURL path];
	BOOL isDir = NO;
	BOOL fileExists = [[NSFileManager defaultManager] fileExistsAtPath:storePath isDirectory:&isDir];
	if (success && fileExists && !isDir)
	{
        localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"The path for a store should be a directory at path: %@", storePath] underlyingError:nil];
		success = NO;
	}
		
	// file package should have a 'devices' subdirectory
	NSString *devicesPath = [self deviceRootPath];
	BOOL devicesPathIsDir = NO;
	BOOL devicesDirExists = [[NSFileManager defaultManager] fileExistsAtPath:devicesPath isDirectory:&devicesPathIsDir];
	if (success && devicesDirExists && !devicesPathIsDir)
	{
        localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"The file package for a store should have a 'devices' subdirectory at path: %@", storePath] underlyingError:nil];
		success = NO;
	}

	// 'devices' subdir may have a 'deviceID' subdir
	NSString *identifierPath = [self readwriteDirectoryPath];
	BOOL identifierPathIsDir = NO;
	BOOL identifierDirExists = [[NSFileManager defaultManager] fileExistsAtPath:identifierPath isDirectory:&identifierPathIsDir];
	if (success && identifierDirExists && !identifierPathIsDir)
    {
        localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"The device identifier subpath '%@' should be a directory in the file package for the store at path: %@", self.deviceIdentifier, storePath] underlyingError:nil];
		success = NO;
	}

	// create file package if necessary
	if (success && !fileExists)
	{
		NSFileCoordinator *coordinator = [[NSFileCoordinator alloc] initWithFilePresenter:self];
		[coordinator coordinateWritingItemAtURL:self.storeURL options:NSFileCoordinatorWritingForReplacing error:NULL byAccessor:^(NSURL *newURL) {
			NSError *fmError = nil;
			success = [[NSFileManager defaultManager] createDirectoryAtURL:self.storeURL withIntermediateDirectories:NO attributes:nil error:&fmError] && [[NSFileManager defaultManager] createDirectoryAtPath:devicesPath withIntermediateDirectories:NO attributes:nil error:&fmError];
			if (!success)
                localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"Could not create the root directory for the file package for the store at path: %@", storePath] underlyingError:fmError];
		}];
	}
		
	// create deviceID subfolder if necessary
	if (success && !identifierDirExists)
	{
		NSFileCoordinator *coordinator = [[NSFileCoordinator alloc] initWithFilePresenter:self];
		[coordinator coordinateWritingItemAtURL:[NSURL fileURLWithPath:identifierPath] options:NSFileCoordinatorWritingForReplacing error:NULL byAccessor:^(NSURL *newURL) {
			
			NSError *fmError = nil;
			success = [[NSFileManager defaultManager] createDirectoryAtPath:identifierPath withIntermediateDirectories:NO attributes:nil error:&fmError];
			if (!success)
                localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"Could not create a subdirectory for the device identifier '%@' in the file package for the store at path: %@", self.deviceIdentifier, storePath] underlyingError:fmError];
		}];
	}

    if (!success)
    {
        ErrorLog(@"Could not prepare file package for store at URL '%@' because of error: %@", self.storeURL, localError);
        if (error != NULL)
            *error = localError;
    }
    return success;
}

- (NSArray *)readonlyDirectoryPaths
{
    // store should be a file on disk
    if (self._inMemory || ![self.storeURL isFileURL])
        return @[];
    
    // file should be a directory
    NSString *storePath = [self.storeURL path];
    BOOL isDir = NO;
    BOOL fileExists = [[NSFileManager defaultManager] fileExistsAtPath:storePath isDirectory:&isDir];
    if (!fileExists || !isDir)
        return @[];
    
    // file package should have a 'devices' subdirectory
    NSString *devicesPath = [self deviceRootPath];
    BOOL devicesPathIsDir = NO;
    BOOL devicesDirExists = [[NSFileManager defaultManager] fileExistsAtPath:devicesPath isDirectory:&devicesPathIsDir];
    if (!devicesDirExists || !devicesPathIsDir)
        return @[];
    
    // subdirs of 'devices' have device-specific files
    NSArray *subpaths = [[NSFileManager defaultManager] contentsOfDirectoryAtPath:devicesPath error:NULL];
    if (!subpaths)
        return @[];
    NSMutableArray *paths = [NSMutableArray arrayWithCapacity:[subpaths count]];
    for (NSString *subpath in subpaths)
    {
        if ([subpath hasPrefix:@"."])
            continue;
        if ([subpath isEqualToString:self.deviceIdentifier])
            continue;
        NSString *fullPath = [devicesPath stringByAppendingPathComponent:subpath];
        BOOL subpathIsDir = NO;
        if ([[NSFileManager defaultManager] fileExistsAtPath:fullPath isDirectory:&subpathIsDir] && subpathIsDir)
            [paths addObject:fullPath];
    }
    return [NSArray arrayWithArray:paths];
}

- (NSString *)subpathInPackageForPath:(NSString *)path
{
	NSArray *components = [path pathComponents];
    NSArray *directoryComponents = [[self.storeURL path] pathComponents];
    if ([components count] < [directoryComponents count])
        return nil;
    return [[components subarrayWithRange:NSMakeRange([directoryComponents count] - 1, [components count] -[directoryComponents count] + 1)] componentsJoinedByString:@"/"];
}

- (BOOL)isReadwriteDirectorySubpath:(NSString *)path
{
    if (self._inMemory || ![self.storeURL isFileURL])
        return NO;
    
	// be careful to check relative path instead of full path comparison
	NSString *fileName = [self.storeURL lastPathComponent];
	NSString *relativeDevicesDir = [fileName stringByAppendingPathComponent:PARDevicesDirectoryName];
	NSString *relativeReadWriteDir = [relativeDevicesDir stringByAppendingPathComponent:self.deviceIdentifier];
	
	return [path rangeOfString:relativeReadWriteDir].location != NSNotFound;
}


#pragma mark - Loading / Closing Database Layer

+ (NSManagedObjectModel *)managedObjectModel
{
    static dispatch_once_t pred = 0;
    static NSManagedObjectModel *mom = nil;
    dispatch_once(&pred,
      ^{
          NSAttributeDescription *blobAttribute = [[NSAttributeDescription alloc] init];
          blobAttribute.name = @"blob";
          blobAttribute.attributeType = NSBinaryDataAttributeType;
          
          NSAttributeDescription *keyAttribute = [[NSAttributeDescription alloc] init];
          keyAttribute.name = @"key";
          keyAttribute.indexed = YES;
          keyAttribute.attributeType = NSStringAttributeType;
          
          NSAttributeDescription *timestampAttribute = [[NSAttributeDescription alloc] init];
          timestampAttribute.name = @"timestamp";
          timestampAttribute.indexed = YES;
          timestampAttribute.attributeType = NSInteger64AttributeType;
          
          NSAttributeDescription *parentTimestampAttribute = [[NSAttributeDescription alloc] init];
          parentTimestampAttribute.name = @"parentTimestamp";
          parentTimestampAttribute.indexed = YES;
          parentTimestampAttribute.attributeType = NSInteger64AttributeType;
          
          NSEntityDescription *entity = [[NSEntityDescription alloc] init];
          entity.name = @"Log";
          entity.properties = @[blobAttribute, keyAttribute, timestampAttribute, parentTimestampAttribute];
          
          mom = [[NSManagedObjectModel alloc] init];
          mom.entities = @[entity];
      });
    return mom;
}

- (NSPersistentStore *)addPersistentStoreWithCoordinator:(NSPersistentStoreCoordinator *)psc dirPath:(NSString *)path readOnly:(BOOL)readOnly error:(NSError **)error
{
    // for readonly stores, check wether a file is in fact present at that path (with iCloud or Dropbox, the directory could be there without the database yet)
    NSString *storePath = [path stringByAppendingPathComponent:PARDatabaseFileName];
    BOOL isDir = NO;
    if (readOnly && (![[NSFileManager defaultManager] fileExistsAtPath:storePath isDirectory:&isDir] || isDir))
    {
        if (isDir)
            ErrorLog(@"Cannot create persistent store for database at path '%@', because there is already a directory at this path", storePath);
        return nil;
    }
	
	// create the store
    NSError *localError = nil;
    NSDictionary *pragmas = @{
                              @"journal_mode": @"TRUNCATE"
                              };
    NSDictionary *storeOptions = @{
                                   NSMigratePersistentStoresAutomaticallyOption : @YES,
                                   NSInferMappingModelAutomaticallyOption:        @YES,
                                   NSReadOnlyPersistentStoreOption:               @(readOnly),
                                   NSSQLitePragmasOption:                         pragmas,
                                   };
    NSPersistentStore *store = [psc addPersistentStoreWithType:NSSQLiteStoreType configuration:nil URL:[NSURL fileURLWithPath:storePath] options:storeOptions error:&localError];
    if (!store)
    {
        ErrorLog(@"Error creating persistent store for database at path '%@':\n%@", storePath, localError);
        if (error != NULL)
            *error = localError;
        return nil;
    }
    return store;
}

// context for all the stores corresponding to the different devices
// only the 'main' store is read/write
- (NSManagedObjectContext *)managedObjectContext
{
    NSAssert([self.databaseQueue isInCurrentQueueStack], @"%@:%@ should only be called from within the database queue", [self class],NSStringFromSelector(_cmd));

    // lazy creation
    NSManagedObjectContext *managedObjectContext = self._managedObjectContext;
    if (managedObjectContext != nil)
    {
        return managedObjectContext;
    }

    // model
    NSManagedObjectModel *mom = [PARStore managedObjectModel];
    if (mom == nil)
    {
        return nil;
    }
    
    // prepare file package on disk
    if (![self prepareFilePackageWithError:NULL])
    {
        ErrorLog(@"Could not create managed object context for store at URL '%@'", self.storeURL);
        return nil;
    }
    
    // stores
    NSPersistentStoreCoordinator *psc = [[NSPersistentStoreCoordinator alloc] initWithManagedObjectModel:mom];
    NSError *error = nil;
    self.readwriteDatabase = [self addPersistentStoreWithCoordinator:psc dirPath:[self readwriteDirectoryPath] readOnly:NO error:&error];
    if (!self.readwriteDatabase)
        return nil;
    NSArray *otherDirs = [self readonlyDirectoryPaths];
    NSMutableArray *otherStores = [NSMutableArray array];
    for (NSString *dir in otherDirs)
    {
        NSPersistentStore *store = [self addPersistentStoreWithCoordinator:psc dirPath:dir readOnly:YES error:NULL];
        if (store)
            [otherStores addObject:store];
    }
    self.readonlyDatabases = [NSArray arrayWithArray:otherStores];

    // context
    NSManagedObjectContext *moc = [[NSManagedObjectContext alloc] init];
    [moc setPersistentStoreCoordinator:psc];
    self._managedObjectContext = moc;
    return moc;
}

- (void)refreshStoreList
{
    NSAssert([self.databaseQueue isInCurrentQueueStack], @"%@:%@ should only be called from within the database queue", [self class],NSStringFromSelector(_cmd));
    if (self._inMemory)
        return;
    
    NSPersistentStoreCoordinator *psc = [[self managedObjectContext] persistentStoreCoordinator];
    NSMutableArray *stores = [NSMutableArray arrayWithArray:self.readonlyDatabases];
    NSArray *currentDirs = [stores valueForKeyPath:@"URL.path"];
    NSArray *allDirs = [self readonlyDirectoryPaths];
    for (NSString *path in allDirs)
    {
		NSString *storePath = [path stringByAppendingPathComponent:PARDatabaseFileName];
        
        // store is already known
        // refresh the URL to force a cache flush and reload the synced contents if available
        if ([currentDirs containsObject:storePath])
        {
			// instead of ignoring this store, reload it to get the synced contents if available
			NSPersistentStore *existingStore = [psc persistentStoreForURL:[NSURL fileURLWithPath:storePath]];
            [psc setURL:existingStore.URL forPersistentStore:existingStore];
		}
		
        // new store
        else
        {
            NSPersistentStore *newStore = [self addPersistentStoreWithCoordinator:psc dirPath:path readOnly:YES error:NULL];
            if (newStore)
                [stores addObject:newStore];
        }
    }
    self.readonlyDatabases = [NSArray arrayWithArray:stores];
}

- (BOOL)_save:(NSError **)error
{
    NSAssert([self.databaseQueue isInCurrentQueueStack], @"%@:%@ should only be called from within the database queue", [self class],NSStringFromSelector(_cmd));
    [self.databaseQueue cancelTimerWithName:@"save_delay"];
    [self.databaseQueue cancelTimerWithName:@"save_coalesce"];
    
    if ([self deleted])
    {
        NSError *localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:@"Cannot save deleted store" underlyingError:nil];
        if (error != NULL)
        {
            *error = localError;
        }
        return NO;
    }

    // autoclose database
    [self closeDatabaseSoon];

    NSError *localError = nil;
    if (self._managedObjectContext)
    {
        NSFileCoordinator *coordinator = [[NSFileCoordinator alloc] initWithFilePresenter:self];
        NSURL *databaseURL = [NSURL fileURLWithPath:[[self readwriteDirectoryPath] stringByAppendingPathComponent:PARDatabaseFileName]];
        NSError *coordinatorError = nil;
        __block NSError *saveError = nil;
        [coordinator coordinateWritingItemAtURL:databaseURL options:NSFileCoordinatorWritingForReplacing error:&coordinatorError byAccessor:^(NSURL *newURL)
         {
             NSError *blockError = nil;
             if (![self._managedObjectContext save:&blockError])
                 saveError = blockError;
         }];
        localError =coordinatorError;
        if (!localError)
            localError = saveError;
    }
    else
    {
        localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:@"No managed object context" underlyingError:nil];
    }
    
    if (localError)
    {
        NSString *storePath = [self.storeURL path];
        ErrorLog(@"Could not save store:\npath: %@\nerror: %@\n", storePath, [localError localizedDescription]);
        if (error != NULL)
            *error = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"Could not save store for device identifier '%@' at path: %@", self.deviceIdentifier, [self.storeURL path]] underlyingError:localError];
        return NO;
    }
    
    #if TARGET_OS_IPHONE | TARGET_IPHONE_SIMULATOR
    
    #elif TARGET_OS_MAC
    // save was successful: "blink" the database which relinquishes the lock on the file just enough for a service like Dropbox to upload the new version on the server
    // not needed on iOS where the files are all under the control of the app and would need to be manually uploaded to online file services
    else
    {
        NSPersistentStore *store = self.readwriteDatabase;
        if (store !=  nil)
        {
            // "blinking" can be done by simply setting again the URL of the persistent store
            [[self._managedObjectContext persistentStoreCoordinator] setURL:store.URL forPersistentStore:store];
        }
    }
    #endif

    return YES;
}

- (void)saveNow
{
    if ([self.memoryQueue isInCurrentQueueStack])
    {
        ErrorLog(@"To avoid deadlocks, %@ should not be called within a transaction. Bailing out.", NSStringFromSelector(_cmd));
        return;
    }
    [self.databaseQueue dispatchSynchronously:^{ [self _save:NULL]; }];
}

- (void)saveSoon
{
    [self.databaseQueue scheduleTimerWithName:@"save_delay" timeInterval:1.0 behavior:PARTimerBehaviorDelay block:^{ [self _save:NULL]; }];
    [self.databaseQueue scheduleTimerWithName:@"save_coalesce" timeInterval:15.0 behavior:PARTimerBehaviorCoalesce block:^{ [self _save:NULL]; }];
}

- (void)_closeDatabase
{
    NSAssert([self.databaseQueue isInCurrentQueueStack], @"%@:%@ should only be called from within the database queue", [self class], NSStringFromSelector(_cmd));
    [self _save:NULL];
    [self.databaseQueue cancelTimerWithName:@"close_database"];
    self._managedObjectContext = nil;
}

- (void)closeDatabase
{
    [self.databaseQueue dispatchAsynchronously:^{ [self _closeDatabase]; }];
}

- (void)closeDatabaseSoon
{
    [self.databaseQueue scheduleTimerWithName:@"close_database" timeInterval:60.0 behavior:PARTimerBehaviorDelay block:^{ [self _closeDatabase]; }];
}

- (void)closeDatabaseNow
{
    if ([self.memoryQueue isInCurrentQueueStack])
    {
        ErrorLog(@"To avoid deadlocks, %@ should not be called within a transaction. Bailing out.", NSStringFromSelector(_cmd));
        return;
    }
    [self.databaseQueue dispatchSynchronously:^{ [self _closeDatabase]; }];
}


#pragma mark - NSData <--> Property List

- (NSData *)dataFromPropertyList:(id)plist error:(NSError **)error
{
    NSError *localError = nil;
    NSData *blob = [NSPropertyListSerialization dataWithPropertyList:plist format:NSPropertyListBinaryFormat_v1_0 options:0 error:&localError];
    if (!blob)
    {
        ErrorLog(@"Property list could not be serialized:\nproperty list: %@\nerror: %@", plist, localError);
        if (error)
            *error = localError;
    }
    return blob;
}

- (id)propertyListFromData:(NSData *)blob error:(NSError **)error
{
    if (!blob)
        return nil;
    
    NSError *localError = nil;
    id result = [NSPropertyListSerialization propertyListWithData:blob options:NSPropertyListImmutable format:NULL error:&localError];
    if (!result)
    {
        ErrorLog(@"Invalid blob '%@' cannot be deserialized because of error: %@", blob, localError);
        if (error)
            *error = localError;
    }
    return result;
}


#pragma mark - Content Manipulation

- (NSDictionary *)allRelevantValues
{
    __block NSDictionary *allValues = nil;
    [self.memoryQueue dispatchSynchronously:^{ allValues = [NSDictionary dictionaryWithDictionary:self._memory]; }];
    return allValues;
}

- (id)propertyListValueForKey:(NSString *)key
{
    __block id plist = nil;
    [self.memoryQueue dispatchSynchronously:^{ plist = self._memory[key]; }];
    return plist;
}

- (id)propertyListValueForKey:(NSString *)key class:(Class)class error:(NSError **)error
{
    id value = [self propertyListValueForKey:key];
    if (value && ![value isKindOfClass:class])
    {
        NSString *description = [NSString stringWithFormat:@"Value for key '%@' should be of class '%@', but is instead of class '%@', in store at path '%@': %@", key, NSStringFromClass(class), [value class], self.storeURL.path, value];
        ErrorLog(@"%@", description);
        if (error != NULL)
            *error = [NSError errorWithObject:self code:__LINE__ localizedDescription:description underlyingError:nil];
        return nil;
    }
    return value;
}



- (void)setPropertyListValue:(id)plist forKey:(NSString *)key
{
    [self.memoryQueue dispatchSynchronously:^
     {
         // get the timestamp **now**, so we have the current date, not the date at which the database block will run
         NSNumber *newTimestamp = [PARStore timestampNow];

         self._memory[key] = plist;
         if (self._inMemory)
         {
             [self postNotificationWithName:PARStoreDidChangeNotification userInfo:@{@"values": @{key: plist}, @"timestamps": @{key: newTimestamp}}];
             return;
         }
         
         NSError *error = nil;
         NSData *blob = [self dataFromPropertyList:plist error:&error];
         if (!blob)
             ErrorLog(@"Error creating data from plist:\nkey: %@:\nplist: %@\nerror: %@", key, plist, [error localizedDescription]);
         else
         {
             NSNumber *oldTimestamp = self._memoryKeyTimestamps[key];
             self._memoryKeyTimestamps[key] = newTimestamp;
             [self postNotificationWithName:PARStoreDidChangeNotification userInfo:@{@"values": @{key: plist}, @"timestamps": @{key: newTimestamp}}];
             
             [self.databaseQueue dispatchAsynchronously:
              ^{
                  NSManagedObjectContext *moc = [self managedObjectContext];
                  if (moc == nil)
                  {
                      return;
                  }
                  
                  NSManagedObject *newLog = [NSEntityDescription insertNewObjectForEntityForName:@"Log" inManagedObjectContext:moc];
                  [newLog setValue:newTimestamp forKey:@"timestamp"];
                  [newLog setValue:oldTimestamp forKey:@"parentTimestamp"];
                  [newLog setValue:key forKey:@"key"];
                  [newLog setValue:blob forKey:@"blob"];
                  self.databaseTimestamps[self.deviceIdentifier] = newTimestamp;
                  
                  // schedule database save
                  [self saveSoon];
              }];
         }
     }];
}

- (void)setEntriesFromDictionary:(NSDictionary *)dictionary
{
    [self.memoryQueue dispatchSynchronously:^
     {
         // get the timestamp **now**, so we have the current date, not the date at which the database block will run
         NSNumber *newTimestamp = [PARStore timestampNow];

         [self._memory addEntriesFromDictionary:dictionary];
         
         if (self._inMemory)
         {
             NSMutableDictionary *newTimestamps = [NSMutableDictionary dictionaryWithCapacity:dictionary.count];
             for (NSString *key in dictionary.keyEnumerator)
                 newTimestamps[key] = newTimestamp;
            [self postNotificationWithName:PARStoreDidChangeNotification userInfo:@{@"values": dictionary, @"timestamps": newTimestamps}];
             return;
         }
         
         // memory timestamps
         NSMutableDictionary *oldTimestamps = [NSMutableDictionary dictionaryWithCapacity:dictionary.count];
         NSMutableDictionary *newTimestamps = [NSMutableDictionary dictionaryWithCapacity:dictionary.count];
         for (NSString *key in dictionary.keyEnumerator)
         {
             NSNumber *oldTimestamp = self._memoryKeyTimestamps[key];
             if (oldTimestamp)
                 oldTimestamps[key] = self._memoryKeyTimestamps[key];
             self._memoryKeyTimestamps[key] = newTimestamp;
             newTimestamps[key] = newTimestamp;
         }

         [self postNotificationWithName:PARStoreDidChangeNotification userInfo:@{@"values": dictionary, @"timestamps": newTimestamps}];

         [self.databaseQueue dispatchAsynchronously: ^
          {
              NSManagedObjectContext *moc = [self managedObjectContext];
              if (moc == nil)
              {
                  return;
              }
              
              // each key/value --> new Log
              [dictionary enumerateKeysAndObjectsUsingBlock:^(id key, id plist, BOOL *stop)
              {
                  NSError *error = nil;
                  NSData *blob = [self dataFromPropertyList:plist error:&error];
                  if (!blob)
                  {
                      ErrorLog(@"Error creating data from plist:\nkey: %@:\nplist: %@\nerror: %@", key, plist, [error localizedDescription]);
                      return;
                  }

                  NSManagedObject *newLog = [NSEntityDescription insertNewObjectForEntityForName:@"Log" inManagedObjectContext:moc];
                  [newLog setValue:newTimestamp forKey:@"timestamp"];
                  [newLog setValue:oldTimestamps[key] forKey:@"parentTimestamp"];
                  [newLog setValue:key forKey:@"key"];
                  [newLog setValue:blob forKey:@"blob"];
              }];
              self.databaseTimestamps[self.deviceIdentifier] = newTimestamp;
              
              // schedule database save
              [self saveSoon];
          }];
     }];
}

- (void)runTransaction:(PARDispatchBlock)block
{
    [self.memoryQueue dispatchSynchronously:block];
}

- (BOOL)deleted
{
    __block BOOL deleted = NO;
    [self.memoryQueue dispatchSynchronously:^{ deleted = self._deleted; }];
    return deleted;
}

- (BOOL)inMemory
{
    return self._inMemory;
}

#pragma mark - Managing Blobs

- (NSURL *)blobDirectoryURL
{
    return [self.storeURL URLByAppendingPathComponent:@"blobs"];
}

- (BOOL)writeBlobData:(NSData *)data toPath:(NSString *)path error:(NSError **)error
{
    // nil path = error
    if (path == nil)
    {
        NSString *description = [NSString stringWithFormat:@"Blob cannot be saved because method '%@' was called with 'path' parameter nil, in store at path '%@'", NSStringFromSelector(_cmd), self.storeURL.path];
        ErrorLog(@"%@", description);
        if (error != NULL)
            *error = [NSError errorWithObject:self code:__LINE__ localizedDescription:description underlyingError:nil];
        return NO;
    }
    
    // nil data = empty data + warning
    if (data == nil)
    {
        NSString *description = [NSString stringWithFormat:@"Empty data will be used in method '%@' called with data parameter nil, in store at path '%@'", NSStringFromSelector(_cmd), self.storeURL.path];
        ErrorLog(@"%@", description);
        data = [NSData data];
    }

    // blobs for in-memory store are stored... in memory
    if (self._inMemory)
    {
        [self.memoryQueue dispatchAsynchronously:^
         {
             [self._memoryFileData setObject:data forKey:path];
         }];
        return YES;
    }
    
    // otherwise blobs are stored in a special blob directory
    __block NSError *localError = nil;
    NSURL *fileURL = [[self blobDirectoryURL] URLByAppendingPathComponent:path];
    NSError *coordinatorError = nil;
    [[[NSFileCoordinator alloc] initWithFilePresenter:self] coordinateWritingItemAtURL:fileURL options:NSFileCoordinatorWritingForReplacing error:&coordinatorError byAccessor:^(NSURL *newURL)
    {
        // create parent dirs (it will fail if one of the dir already exists but is a file)
        NSError *errorCreatingDir = nil;
        BOOL successCreatingDir = [[NSFileManager defaultManager] createDirectoryAtURL:[newURL URLByDeletingLastPathComponent] withIntermediateDirectories:YES attributes:nil error:&errorCreatingDir];
        if (!successCreatingDir)
        {
            localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"Could not create parent directories before writing blob at path '%@'", newURL.path] underlyingError:errorCreatingDir];
            return;
        }
        
        // write to disk (overwrite any file that was at that same path before)
        NSError *errorWritingData = nil;
        BOOL successWritingData = [data writeToURL:newURL options:0 error:&errorWritingData];
        if (!successWritingData)
            localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"Could not save data blob at path '%@'", newURL.path] underlyingError:errorWritingData];
    }];

    // error handling
    if (coordinatorError && !localError)
        localError = coordinatorError;
    if (localError)
    {
        ErrorLog(@"Error writing blob: %@", localError);
        if (error != NULL)
            *error = localError;
        return NO;
    }
    return YES;
}

- (BOOL)writeBlobFromPath:(NSString *)sourcePath toPath:(NSString *)targetSubpath error:(NSError **)error
{
    // nil local path = error
    if (targetSubpath == nil)
    {
        NSString *description = [NSString stringWithFormat:@"Blob cannot be saved because method '%@' was called with a nil value for the local path parameter, in store at path '%@'", NSStringFromSelector(_cmd), self.storeURL.path];
        ErrorLog(@"%@", description);
        if (error != NULL)
            *error = [NSError errorWithObject:self code:__LINE__ localizedDescription:description underlyingError:nil];
        return NO;
    }
    
    // nil absolute path = empty data + warning
    if (sourcePath == nil)
    {
        NSString *description = [NSString stringWithFormat:@"Blob cannot be saved because method '%@' was called with a nil value for the source path parameter, in store at path '%@'", NSStringFromSelector(_cmd), self.storeURL.path];
        ErrorLog(@"%@", description);
        if (error != NULL)
            *error = [NSError errorWithObject:self code:__LINE__ localizedDescription:description underlyingError:nil];
        return NO;
    }
    
    // blobs for in-memory store are stored... in memory
    if (self._inMemory)
    {
        NSError *errorReadingData = nil;
        NSData *sourceData = nil;
        if (sourcePath.length > 0)
            sourceData = [NSData dataWithContentsOfFile:sourcePath options:NSDataReadingMappedIfSafe error:&errorReadingData];
        else
            errorReadingData = [NSError errorWithObject:self code:__LINE__ localizedDescription:@"Cannot read data from empty path to store as blob in memory store" underlyingError:nil];
        if (!sourceData)
        {
            if (error != NULL)
                *error = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"Could not read data to store as blob in memory store, from source file at path '%@', ", sourcePath] underlyingError:errorReadingData];
            return NO;
        }
        return [self writeBlobData:sourceData toPath:targetSubpath error:error];
    }
    
    // otherwise blobs are stored in a special blob directory
    __block NSError *localError = nil;
    NSURL *targetURL = [[self blobDirectoryURL] URLByAppendingPathComponent:targetSubpath];
    NSError *coordinatorError = nil;
    [[[NSFileCoordinator alloc] initWithFilePresenter:self] coordinateWritingItemAtURL:targetURL options:NSFileCoordinatorWritingForReplacing error:&coordinatorError byAccessor:^(NSURL *newTargetURL)
     {
         // create parent dirs (it will fail if one of the dir already exists but is a file)
         NSError *errorCreatingDir = nil;
         BOOL successCreatingDir = [[NSFileManager defaultManager] createDirectoryAtURL:[newTargetURL URLByDeletingLastPathComponent] withIntermediateDirectories:YES attributes:nil error:&errorCreatingDir];
         if (!successCreatingDir)
         {
             localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"Could not create parent directories before writing blob at path '%@'", newTargetURL.path] underlyingError:errorCreatingDir];
             return;
         }
         
         // write to disk (overwrite any file that was at that same path before)
         NSError *errorWritingData = nil;
         BOOL successWritingData = [[NSFileManager defaultManager] copyItemAtURL:[NSURL fileURLWithPath:sourcePath] toURL:newTargetURL error:&errorWritingData];
         if (!successWritingData)
             localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"Could not copy file from source path '%@', into blob directory at path '%@'", sourcePath, newTargetURL.path] underlyingError:errorWritingData];
     }];
    
    // error handling
    if (coordinatorError && !localError)
        localError = coordinatorError;
    if (localError)
    {
        ErrorLog(@"Error writing blob: %@", localError);
        if (error != NULL)
            *error = localError;
    }
    return YES;
}

- (BOOL)deleteBlobAtPath:(NSString *)path error:(NSError **)error
{
    // nil path = error
    if (path == nil)
    {
        NSString *description = [NSString stringWithFormat:@"Blob cannot be deleted because method '%@' was called with 'path' parameter nil, in store at path '%@'", NSStringFromSelector(_cmd), self.storeURL.path];
        ErrorLog(@"%@", description);
        if (error != NULL)
            *error = [NSError errorWithObject:self code:__LINE__ localizedDescription:description underlyingError:nil];
        return NO;
    }
    
    // blobs for in-memory store are stored... in memory
    if (self._inMemory)
    {
        [self.memoryQueue dispatchAsynchronously:^
         {
             [self._memoryFileData removeObjectForKey:path];
         }];
        return YES;
    }
    
    // otherwise blobs are stored in a special blob directory
    __block NSError *localError = nil;
    NSURL *fileURL = [[self blobDirectoryURL] URLByAppendingPathComponent:path];
    NSError *coordinatorError = nil;
    [[[NSFileCoordinator alloc] initWithFilePresenter:self] coordinateWritingItemAtURL:fileURL options:NSFileCoordinatorWritingForReplacing error:&coordinatorError byAccessor:^(NSURL *newURL)
     {
         // write to disk (overwrite any file that was at that same path before)
         NSError *error = nil;
         BOOL success = [[NSFileManager defaultManager] removeItemAtURL:fileURL error:&error];
         if (!success)
             localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"Could not delete data blob at path '%@'", newURL.path] underlyingError:error];
     }];
    
    // error handling
    if (coordinatorError && !localError)
        localError = coordinatorError;
    if (localError)
    {
        ErrorLog(@"Error deleting blob: %@", localError);
        if (error != NULL)
            *error = localError;
        return NO;
    }
    return YES;
}

- (NSData *)blobDataAtPath:(NSString *)path error:(NSError **)error;
{
    // nil path = error
    if (path == nil)
    {
        NSString *description = [NSString stringWithFormat:@"Blob data cannot be retrieved because method '%@' was called with 'path' parameter nil, in store at path '%@'", NSStringFromSelector(_cmd), self.storeURL.path];
        ErrorLog(@"%@", description);
        if (error != NULL)
            *error = [NSError errorWithObject:self code:__LINE__ localizedDescription:description underlyingError:nil];
        return nil;
    }

    // blobs for in-memory store are stored... in memory
    if (self._inMemory)
    {
        __block NSData *foundData = nil;
        [self.memoryQueue dispatchSynchronously:^
        {
            foundData = [self._memoryFileData objectForKey:path];
        }];
        return foundData;
    }
    
    // otherwise blobs are stored in a special blob directory
    __block NSError *localError = nil;
    NSURL *fileURL = [[self blobDirectoryURL] URLByAppendingPathComponent:path];
    NSError *coordinatorError = nil;
    __block NSData *data = nil;
    [[[NSFileCoordinator alloc] initWithFilePresenter:self] coordinateReadingItemAtURL:fileURL options:NSFileCoordinatorReadingWithoutChanges error:&coordinatorError byAccessor:^(NSURL *newURL)
    {
        NSError *errorReadingData = nil;
        data = [NSData dataWithContentsOfURL:newURL options:NSDataReadingMappedIfSafe error:&errorReadingData];
        if (!data)
            localError = [NSError errorWithObject:self code:__LINE__ localizedDescription:[NSString stringWithFormat:@"Could not read data blob at path '%@'", newURL.path] underlyingError:errorReadingData];
    }];
    
    // error handling
    if (coordinatorError && !localError)
        localError = coordinatorError;
    if (localError)
    {
        ErrorLog(@"Error reading data blob: %@", localError);
        if (error != NULL)
            *error = localError;
        return nil;
    }

    return data;
}

- (NSString *)absolutePathForBlobPath:(NSString *)path
{
    return [[[self blobDirectoryURL] URLByAppendingPathComponent:path] path];
}


#pragma mark - Sync

- (NSArray *)relevantKeysForSync
{
    return [NSArray array];
}

- (void)applySyncChangeWithValues:(NSDictionary *)values timestamps:(NSDictionary *)timestamps
{
    NSAssert([self.memoryQueue isInCurrentQueueStack], @"%@:%@ should only be called from within the memory queue", [self class],NSStringFromSelector(_cmd));
    [values enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *s)     { self._memory[key] = obj;              }];
    [timestamps enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *s) { self._memoryKeyTimestamps[key] = obj; }];
}


// as long as we scan the results of a fetch in order, this guarantees that data is only queried from the database in batches of 1000
#define LOGS_BATCH_SIZE 1000

- (void)_sync
{
    NSAssert([self.databaseQueue isInCurrentQueueStack], @"%@:%@ should only be called from within the database queue", [self class],NSStringFromSelector(_cmd));
    
    // sync is not relevant for in-memory stores
    if (self._inMemory)
        return;

    // never resuscitate a deleted store
    if ([self deleted])
        return;
    
    // without a moc, the rest of the code will throw an exception
    NSManagedObjectContext *moc = [self managedObjectContext];
    if (moc == nil)
    {
        ErrorLog(@"Could not load managed object context and sync store at path '%@'", [self.storeURL path]);
        return;
    }
    
    // reset timers
    [self.databaseQueue cancelTimerWithName:@"sync_delay"];
    [self.databaseQueue cancelTimerWithName:@"sync_coalesce"];
    
    // autoclose database
    [self closeDatabaseSoon];

    // because of the way we use the `databaseQueue` and `memoryQueue`, the returned value is guaranteed to take into account any previous execution of `_sync`
    BOOL loaded = [self loaded];
    
    // this will be set to YES if at least one of latest values come from one of the foreign stores
    BOOL hasForeignChanges = NO;
    
    // timestampLimit = load only logs after that timestamp, so we only load the newest logs (will be nil if nothing was loaded yet)
    NSNumber *timestampLimit = nil;
    if (loaded)
    {
        // there are 2 ways to determine `timestampLimit`, which depends on wether a new database was added since the last sync
        [self refreshStoreList];
        NSUInteger countAllDatabasesBefore   = [self.databaseTimestamps count];
        NSUInteger countReadonlyDatabasesNow = [self.readonlyDatabases count];
        NSAssert(countReadonlyDatabasesNow + 1 >= countAllDatabasesBefore, @"Inconsistent tracking of persistent stores");
        BOOL newStoreAdded = (countAllDatabasesBefore < countReadonlyDatabasesNow + 1);
        
        // Case 1: new store was added --> use the oldest of the latest timestamps from each valid key
        // Case 2: no new store added  --> use the oldest of the latest timestamps from each store
        id <NSFastEnumeration> tableToQuery = newStoreAdded ? [self.keyTimestamps objectEnumerator] : [self.databaseTimestamps objectEnumerator];
        for (NSNumber *timestamp in tableToQuery)
        {
            if (timestampLimit == nil || [timestamp compare:timestampLimit] == NSOrderedAscending)
                timestampLimit = timestamp;
        }
    }
    
    // fetch Log rows in reverse timestamp order, starting at `timestampLimit`
    NSError *errorLogs = nil;
    NSFetchRequest *logsRequest = [NSFetchRequest fetchRequestWithEntityName:@"Log"];
    if (timestampLimit)
        [logsRequest setPredicate:[NSPredicate predicateWithFormat:@"timestamp > %@", timestampLimit]];
    [logsRequest setSortDescriptors:@[[NSSortDescriptor sortDescriptorWithKey:@"timestamp" ascending:NO]]];
    [logsRequest setFetchBatchSize:LOGS_BATCH_SIZE];
    [logsRequest setReturnsObjectsAsFaults:NO];
    NSArray *allLogs = [moc executeFetchRequest:logsRequest error:&errorLogs];
    if (!allLogs)
    {
        ErrorLog(@"Error fetching logs for store at path '%@' because of error: %@", [self.storeURL path], errorLogs);
        return;
    }
    
    // keep track of relevant timestamps that will be used to calculate the new logTimestamps and databaseTimestamps at the end
    NSMapTable *updatedDatabaseTimestamps = [NSMapTable weakToStrongObjectsMapTable];
    NSMutableDictionary *updatedKeyTimestamps = [NSMutableDictionary dictionary];
    
    // just go through each row (back in time) until all needed keys are found
    NSArray *relevantKeys = [self relevantKeysForSync];
    NSMutableSet *keysToFetch = [NSMutableSet setWithArray:relevantKeys];
    NSMutableDictionary *updatedValues = [NSMutableDictionary dictionary];
    for (NSManagedObject *log in allLogs)
    {
        // key
        NSString *key = [log valueForKey:@"key"];
        if (!key)
        {
            ErrorLog(@"Unexpected nil value for 'key' column:\nrow: %@\ndatabase: %@", log.objectID, log.objectID.persistentStore.URL.path);
            continue;
        }
        
        // timestamp
        NSNumber *logTimestamp = [log valueForKey:@"timestamp"];
        
        // keep track of the last timestamp for each persistent store
        NSPersistentStore *store = [[log objectID] persistentStore];
        if ([updatedDatabaseTimestamps objectForKey:store] == nil)
        {
            [updatedDatabaseTimestamps setObject:logTimestamp forKey:store];
        }
        
        // skip unused logs
        if (![keysToFetch containsObject:key])
            continue;
        
        // blob --> object
        NSError *blobError = nil;
        NSData *blob = [log valueForKey:@"blob"];
        if (!blob)
        {
            ErrorLog(@"Unexpected nil value for 'blob' column:\nrow: %@\ndatabase: %@", log.objectID, log.objectID.persistentStore.URL.path);
            continue;
        }
        id plistValue = [self propertyListFromData:blob error:&blobError];
        if (!plistValue)
        {
            ErrorLog(@"Error deserializing blob data:\nrow: %@\ndatabase: %@\nerror: %@", log.objectID, log.objectID.persistentStore.URL.path, blobError);
            continue;
        }
        
        // store object and keep track of used keys
        [updatedValues setObject:plistValue forKey:key];
        [keysToFetch removeObject:key];
        
        // keep track of the oldest of the values actually used
        [updatedKeyTimestamps setValue:logTimestamp forKey:key];
        
        if ([[log objectID] persistentStore] != self.readwriteDatabase)
            hasForeignChanges = YES;
        
        // stop when all expected data has been fetched
        if ([keysToFetch count] == 0)
            break;
    }
    
    // update the timestamps for the keys
    //  - unused keys should be gone from keyTimestamps
    //  - keys without a timestamp get a `distant past` value, because it means we need to go all the way back if a new store is later added
    NSMutableDictionary *newKeyTimestamps = [NSMutableDictionary dictionaryWithCapacity:[relevantKeys count]];
    NSMutableSet *allKeys = [NSMutableSet setWithArray:relevantKeys];
    for (NSString *key in allKeys)
    {
        NSNumber *timestamp = updatedKeyTimestamps[key];
        if (!timestamp)
            timestamp = self.keyTimestamps[key];
        if (!timestamp)
            timestamp = [PARStore timestampForDistantPast]; // distant past
        newKeyTimestamps[key] = timestamp;
    }
    self.keyTimestamps = [NSDictionary dictionaryWithDictionary:newKeyTimestamps];
    
    // update the timestamps for the databases
    NSMutableDictionary *newDatabaseTimestamps = [NSMutableDictionary dictionary];
    for (NSPersistentStore *store in [self.readonlyDatabases arrayByAddingObject:self.readwriteDatabase])
    {
        NSString *deviceIdentifier = [self deviceIdentifierForDatabasePath:store.URL.path];
        if (deviceIdentifier == nil)
        {
            continue;
        }
        NSNumber *timestamp = [updatedDatabaseTimestamps objectForKey:store] ?: self.databaseTimestamps[deviceIdentifier] ?: [PARStore timestampForDistantPast];
        newDatabaseTimestamps[deviceIdentifier] = timestamp;
    }
    self.databaseTimestamps = newDatabaseTimestamps;
    
    // store loaded the first time --> set all the data at once
    if (!loaded)
    {
        [self.memoryQueue dispatchAsynchronously:^
         {
             self._memory = [NSMutableDictionary dictionaryWithDictionary:updatedValues];
             self._memoryKeyTimestamps = [NSMutableDictionary dictionaryWithDictionary:updatedKeyTimestamps];
             self._loaded = YES;
             [self postNotificationWithName:PARStoreDidLoadNotification userInfo:nil];
         }];
    }
    
    // when store was already loaded, we need to create a dictionary change and merge with current values
    else if (hasForeignChanges)
    {
        [self.memoryQueue dispatchAsynchronously:^
         {
             NSMutableDictionary *changedValues = [NSMutableDictionary dictionaryWithCapacity:[updatedValues count]];
             NSMutableDictionary *changedTimestamps = [NSMutableDictionary dictionaryWithCapacity:[updatedKeyTimestamps count]];
             [updatedValues enumerateKeysAndObjectsUsingBlock:^(id key, id newValue, BOOL *stop)
              {
                  // the values could have changed while we were running the sync above; some of the keys could have been modified and have more recent timestamps, in which case we should not apply the new value obtained from the database
                  NSNumber *memoryLatestTimestamp = self._memoryKeyTimestamps[key];
                  NSNumber *databaseLatestTimestamp = newKeyTimestamps[key];
                  if (memoryLatestTimestamp ==nil || (databaseLatestTimestamp !=nil && [memoryLatestTimestamp compare:databaseLatestTimestamp] == NSOrderedAscending))
                  {
                      changedValues[key] = newValue;
                      changedTimestamps[key] = databaseLatestTimestamp;
                  }
              }];
             
             if (changedValues.count > 0)
             {
                 [self applySyncChangeWithValues:changedValues timestamps:changedTimestamps];
                 [self postNotificationWithName:PARStoreDidSyncNotification userInfo:@{@"values": changedValues, @"timestamps": changedTimestamps}];
             }
         }];
    }
}

- (id)syncedPropertyListValueForKey:(NSString *)key
{
    return [self syncedPropertyListValueForKey:key timestamp:nil];
}

- (id)syncedPropertyListValueForKey:(NSString *)key timestamp:(NSNumber *)timestamp
{
    if (self._inMemory)
        return nil;
    
    if ([self.memoryQueue isInCurrentQueueStack])
    {
        ErrorLog(@"To avoid deadlocks, %@ should not be called within a transaction. Bailing out.", NSStringFromSelector(_cmd));
        return nil;
    }
    
    __block id plist = nil;
    [self.databaseQueue dispatchSynchronously:^
     {
         NSManagedObjectContext *moc = [self managedObjectContext];
         if (moc == nil)
         {
             return;
         }

         NSError *fetchError = nil;
         NSFetchRequest *request = [NSFetchRequest fetchRequestWithEntityName:@"Log"];
         request.sortDescriptors = @[[NSSortDescriptor sortDescriptorWithKey:@"timestamp" ascending:NO]];
         if (timestamp == nil)
         {
             request.predicate = [NSPredicate predicateWithFormat:@"key == %@", key];
         }
         else
         {
             request.predicate = [NSPredicate predicateWithFormat:@"key == %@ AND timestamp <= %@", key, timestamp];
         }
         request.fetchLimit = 1;
         request.returnsObjectsAsFaults = NO;
         NSArray *results = [moc executeFetchRequest:request error:&fetchError];
         if (!results)
         {
             ErrorLog(@"Error fetching logs for store:\npath: %@\nerror: %@", [self.storeURL path], fetchError);
             return;
         }
         
         if ([results count] > 0)
         {
             NSManagedObject *latestLog = results.lastObject;
             NSError *plistError = nil;
             plist = [self propertyListFromData:[latestLog valueForKey:@"blob"] error:&plistError];
             if (plist == nil)
             {
                 ErrorLog(@"Error deserializing 'layout' data in Logs database:\nrow: %@\nfile: %@\nerror: %@", latestLog.objectID, latestLog.objectID.persistentStore.URL.path, plistError);
             }
         }
         
         [self closeDatabaseSoon];
     }];
    
    return plist;
}

- (void)sync
{
    [self syncSoon];
}

- (void)syncNow
{
    if ([self.memoryQueue isInCurrentQueueStack])
    {
        ErrorLog(@"To avoid deadlocks, %@ should not be called within a transaction. Bailing out.", NSStringFromSelector(_cmd));
        return;
    }
    [self.databaseQueue dispatchSynchronously:^{ [self _sync]; }];
}

- (void)syncSoon
{
    [self.databaseQueue scheduleTimerWithName:@"sync_delay" timeInterval:1.0 behavior:PARTimerBehaviorDelay block:^{ [self _sync]; }];
    [self.databaseQueue scheduleTimerWithName:@"sync_coalesce" timeInterval:15.0 behavior:PARTimerBehaviorCoalesce block:^{ [self _sync]; }];
}


#pragma mark - Notifications

// post notifications asynchronously, in a dedicated serial queue, to
// (1) avoid deadlocks when using a block for the notification callback and tries to access the data
// (2) enforce serialization of the notifications
// (3) allows us to properly close the store with all the notifications sent
- (void)postNotificationWithName:(NSString *)notificationName userInfo:(NSDictionary *)userInfo
{
    [self.notificationQueue dispatchAsynchronously:^
    {
        [[NSNotificationCenter defaultCenter] postNotificationName:notificationName object:self userInfo:userInfo];
    }];
    
}


#pragma mark - Getting Timestamps

#define MICROSECONDS_PER_SECOND (1000 * 1000)
+ (NSNumber *)timestampNow
{
    // timestamp is cast to a signed 64-bit integer (we can't use NSInteger on iOS for that)
    NSTimeInterval timestampInSeconds = [[NSDate date] timeIntervalSinceReferenceDate];
    return @((uint64_t)(timestampInSeconds * MICROSECONDS_PER_SECOND));
}

+ (NSNumber *)timestampForDistantPast
{
    static NSNumber *timestampForDistantPath = nil;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^
      {
          timestampForDistantPath = @(NSIntegerMin);
      });
    return timestampForDistantPath;
}

+ (NSNumber *)timestampForDistantFuture
{
    static NSNumber *timestampForDistantFuture = nil;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^
      {
          timestampForDistantFuture = @(NSIntegerMax);
      });
    return timestampForDistantFuture;
}

- (NSDictionary *)mostRecentTimestampsByDeviceIdentifier
{
    if ([self.memoryQueue isInCurrentQueueStack])
    {
        ErrorLog(@"To avoid deadlocks, %@ should not be called within a transaction. Bailing out.", NSStringFromSelector(_cmd));
        return nil;
    }

    NSMutableDictionary *timestamps = [NSMutableDictionary dictionary];
    [self.databaseQueue dispatchSynchronously:^
     {
         NSManagedObjectContext *moc = [self managedObjectContext];
         if (moc == nil)
         {
             return;
         }

         NSArray *allStores = [moc.persistentStoreCoordinator persistentStores];
         for (NSPersistentStore *store in allStores)
         {
             NSString *deviceIdentifier = [self deviceIdentifierForDatabasePath:store.URL.path];
             if (deviceIdentifier == nil)
             {
                 continue;
             }
             NSNumber *timestamp = self.databaseTimestamps[deviceIdentifier] ?: [PARStore timestampForDistantPast];
             timestamps[deviceIdentifier] = timestamp;
         }
         
         [self closeDatabaseSoon];
     }];
    return [NSDictionary dictionaryWithDictionary:timestamps];
}

- (NSNumber *)mostRecentTimestampForDeviceIdentifier:(NSString *)deviceIdentifier
{
    if (deviceIdentifier == nil)
        return nil;

    if ([self.memoryQueue isInCurrentQueueStack])
    {
        ErrorLog(@"To avoid deadlocks, %@ should not be called within a transaction. Bailing out.", NSStringFromSelector(_cmd));
        return nil;
    }

    __block NSNumber *timestamp = nil;
    [self.databaseQueue dispatchSynchronously:^
    {
        timestamp = self.databaseTimestamps[deviceIdentifier];
    }];
    
    return timestamp;
}

- (NSDictionary *)mostRecentTimestampsByKey
{
    __block NSDictionary *timestamps = [NSMutableDictionary dictionary];
    [self.memoryQueue dispatchSynchronously:^
     {
         timestamps = [NSDictionary dictionaryWithDictionary:self._memoryKeyTimestamps];
     }];
    return timestamps;
}

- (NSNumber *)mostRecentTimestampForKey:(NSString *)key
{
    if (key == nil)
        return nil;
    __block NSNumber *timestamp = nil;
    [self.memoryQueue dispatchSynchronously:^ { timestamp = self._memoryKeyTimestamps[key]; }];
    return timestamp;
}


#pragma mark - History

- (NSArray *)changesSinceTimestamp:(NSNumber *)timestampLimit
{
    if ([self.memoryQueue isInCurrentQueueStack])
    {
        ErrorLog(@"To avoid deadlocks, %@ should not be called within a transaction. Bailing out.", NSStringFromSelector(_cmd));
        return nil;
    }

    NSMutableArray *changes = [NSMutableArray array];
    [self.databaseQueue dispatchSynchronously:^
    {
        NSManagedObjectContext *moc = [self managedObjectContext];
        if (moc == nil)
        {
            return;
        }
        
        // From the documentation for `includesPendingChanges`: "A value of YES is not supported in conjunction with the result type NSDictionaryResultType, including calculation of aggregate results (such as max and min). For dictionaries, the array returned from the fetch reflects the current state in the persistent store, and does not take into account any pending changes, insertions, or deletions in the context."
        // this means we need to save pending changes first to make sure they show up in the query
        [self _save:NULL];
        
        // fetch Log rows in timestamp order, starting at `timestampLimit`
        NSError *errorLogs = nil;
        NSFetchRequest *logsRequest = [NSFetchRequest fetchRequestWithEntityName:@"Log"];
        if (timestampLimit != nil)
        {
            logsRequest.predicate = [NSPredicate predicateWithFormat:@"timestamp > %@", timestampLimit];
        }
        logsRequest.sortDescriptors = @[[NSSortDescriptor sortDescriptorWithKey:@"timestamp" ascending:YES]];
        logsRequest.resultType = NSDictionaryResultType;
        NSArray *logs = [moc executeFetchRequest:logsRequest error:&errorLogs];
        if (!logs)
        {
            ErrorLog(@"Error fetching logs for store at path '%@' because of error: %@", [self.storeURL path], errorLogs);
            return;
        }
        
        // logs --> changes
        for (NSDictionary *logDictionary in logs)
        {
            NSNumber *timestamp = logDictionary[@"timestamp"];
            NSNumber *parentTimestamp = logDictionary[@"parentTimestamp"];
            NSString *key = logDictionary[@"key"];
            NSData *blob = logDictionary[@"blob"];
            id propertyList = [self propertyListFromData:blob error:NULL];
            PARChange *change = [PARChange changeWithTimestamp:timestamp parentTimestamp:parentTimestamp key:key propertyList:propertyList];
            [changes addObject:change];
        }
        
        [self closeDatabaseSoon];
    }];
    
    return changes;
}


#pragma mark - File presenter protocol

- (NSURL *) presentedItemURL
{
	return self.storeURL;
}

- (NSOperationQueue *) presentedItemOperationQueue
{
	return self.presenterQueue;
}

- (void)presentedItemDidMoveToURL:(NSURL *)newURL
{
    // TODO: close the context, open new context at new location, (and read to update store contents?)
	self.storeURL = newURL;
}

- (void)syncDocumentForURLTrigger:(NSURL *)url selector:(SEL)selector
{
    // detect deletion of the file package
    if (![self deleted] && ![[NSFileManager defaultManager] fileExistsAtPath:[self.storeURL path]])
    {
        [self accommodatePresentedItemDeletionWithCompletionHandler:nil];
        return;
    }
    
    // ignore changes potentially triggered by our own actions
    if (url && [self isReadwriteDirectorySubpath:[url path]])
	{
		return;
	}
    
    [self syncSoon];
}

- (void)presentedSubitemDidAppearAtURL:(NSURL *)url
{
    [self syncDocumentForURLTrigger:url selector:_cmd];
}

- (void)presentedSubitemDidChangeAtURL:(NSURL *)url
{
    [self syncDocumentForURLTrigger:url selector:_cmd];
}

- (void)presentedItemDidChange
{
    [self syncDocumentForURLTrigger:nil selector:_cmd];
}

- (void)presentedItemDidGainVersion:(NSFileVersion *)version
{
	DebugLog(@"%s", __func__);
}

- (void)presentedItemDidLoseVersion:(NSFileVersion *)version
{
	DebugLog(@"%s", __func__);
}

// block access to the store while another presenter tries to read or write stuff: other processes or threads will do their work inside the `reader` or `writer` block, and then call the block we pass as argument when they are done
- (void)blockdatabaseQueueWhileRunningBlock:(void (^)(void (^callbackWhenDone)(void)))blockAccessingTheFile
{
    DebugLog(@"%@", NSStringFromSelector(_cmd));

    // when a file coordinator wants to read or write to our store, we can block access to our context using the databaseQueue and a semaphore
    [self.databaseQueue dispatchAsynchronously:^
     {
         NSManagedObjectContext *moc = [self managedObjectContext];
         if (moc == nil)
         {
             return;
         }

         // the semaphore will be used to block the datatase queue until access to the file is done
         dispatch_semaphore_t semaphore = dispatch_semaphore_create(0);
         
         // run the block accessing the file in a separate queue, to make sure any call to the database queue that may result from it is not reentrant into the database queue
         PARDispatchBlock callbackWhenDone = ^{ dispatch_semaphore_signal(semaphore); };
         dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^
          {
              blockAccessingTheFile(callbackWhenDone);
          });
         
         // we use a timeout to avoid being completely locked if iCloud/Apple messes up or if we end up with a dispatch sync into the database queue 
         // giving up is not a big risk, because it's still safe to change the db while other presenters are trying to read the file, and if those presenters take more than xx secs, we have bigger problems
         NSTimeInterval timeout = 5.0;
         if (0 != dispatch_semaphore_wait(semaphore, dispatch_time(DISPATCH_TIME_NOW, timeout * NSEC_PER_SEC)))
             ErrorLog(@"TIMEOUT: timeout on device '%@' while waiting for file coordinator to access store with URL: %@", self.deviceIdentifier, self.storeURL);
         semaphore = NULL;
         
         [self closeDatabaseSoon];
     }];
}

- (void)relinquishPresentedItemToReader:(void (^)(void (^reacquirer)(void)))reader
{
    DebugLog(@"%@", NSStringFromSelector(_cmd));
    //[self blockdatabaseQueueWhileRunningBlock:reader];
	reader(nil);
}

- (void)relinquishPresentedItemToWriter:(void (^)(void (^reacquirer)(void)))writer
{
    DebugLog(@"%@", NSStringFromSelector(_cmd));
    //[self blockdatabaseQueueWhileRunningBlock:writer];
    writer(^
           {
               [self syncSoon];
           });
}

- (void)savePresentedItemChangesWithCompletionHandler:(void (^)(NSError *errorOrNil))completionHandler
{
	[self.databaseQueue dispatchAsynchronously:^
     {
         NSError *saveError = nil;
         BOOL success = (self._managedObjectContext == nil) || [self._managedObjectContext save:&saveError];
         completionHandler((success) ? nil : saveError);
     }];
}

- (void)accommodatePresentedItemDeletionWithCompletionHandler:(void (^)(NSError *errorOrNil))completionHandler
{
    DebugLog(@"%@", NSStringFromSelector(_cmd));
    
    if ([self deleted])
        return;

	[self.memoryQueue dispatchSynchronously:^ { self._deleted = YES; }];
	[self.databaseQueue dispatchAsynchronously:^
    {
        [self _closeDatabase];
        [NSFileCoordinator removeFilePresenter:self];
    }];
	if (completionHandler)
	{
		completionHandler(nil);
	}
    
    [self postNotificationWithName:PARStoreDidDeleteNotification userInfo:nil];
}



#pragma mark - File System Events


#if TARGET_OS_IPHONE | TARGET_IPHONE_SIMULATOR

- (void)createFileSystemEventQueue      { }
- (void)startFileSystemEventStreams  { }
- (void)stopFileSystemEventStreams   { }
- (void)refreshFileSystemEventStreamLogs {}


#elif TARGET_OS_MAC

// FSEventStream callbacks
static void PARStoreDevicesDidChange(
                                  ConstFSEventStreamRef streamRef,
                                  void *callbackContext,
                                  size_t numEvents,
                                  void *eventPaths,
                                  const FSEventStreamEventFlags eventFlags[],
                                  const FSEventStreamEventId eventIds[]);
static void PARStoreLogsDidChange(
                                   ConstFSEventStreamRef streamRef,
                                   void *callbackContext,
                                   size_t numEvents,
                                   void *eventPaths,
                                   const FSEventStreamEventFlags eventFlags[],
                                   const FSEventStreamEventId eventIds[]);

- (void)createFileSystemEventQueue
{
    if (self.fileSystemEventQueue != nil)
    {
        ErrorLog(@"The file system event queue should be created only once in the init method");
        return;
    }
    
    NSString *urlLabel = [[self.storeURL lastPathComponent] stringByReplacingOccurrencesOfString:@"." withString:@"_"];
    NSString *fileSystemEventQueueLabel = [PARDispatchQueue labelByPrependingBundleIdentifierToString:[NSString stringWithFormat:@"fsevent.%@", urlLabel]];
    self.fileSystemEventQueue = [PARDispatchQueue dispatchQueueWithLabel:fileSystemEventQueueLabel];
}

- (void)startFileSystemEventStreams
{
    if (self.inMemory)
        return;
    
    [self.fileSystemEventQueue dispatchAsynchronously:^
    {
        if (_eventStreamDevices != NULL)
            return;
        
        // create event stream
        FSEventStreamContext callbackContext;
        callbackContext.version			= 0;
        callbackContext.info			= (__bridge void *)self;
        callbackContext.retain			= NULL;
        callbackContext.release			= NULL;
        callbackContext.copyDescription	= NULL;
        self.eventStreamDevices = FSEventStreamCreate(kCFAllocatorDefault,
                                               &PARStoreDevicesDidChange,
                                               &callbackContext,
                                               (__bridge CFArrayRef)@[[self deviceRootPath]],
                                               kFSEventStreamEventIdSinceNow,
                                               3.0,
                                               kFSEventStreamCreateFlagUseCFTypes | kFSEventStreamCreateFlagWatchRoot);
        // schedule and start the stream
        FSEventStreamSetDispatchQueue(_eventStreamDevices, [self.fileSystemEventQueue valueForKey:@"queue"]);
        
        // error
        if (FSEventStreamStart(_eventStreamDevices) == false)
        {
            ErrorLog(@"ERROR: could not start FSEventStream for path %@", [self deviceRootPath]);
            FSEventStreamRelease(_eventStreamDevices);
            self.eventStreamDevices = NULL;
            return;
        }
        
        // we can now also start the event streams for the log databases
        [self refreshFileSystemEventStreamLogs];
    }];
}

- (void)refreshFileSystemEventStreamLogs
{
    NSArray *directoriesToObserve = [self readonlyDirectoryPaths];

    [self.fileSystemEventQueue dispatchAsynchronously:^
     {
         // remove old stream
         if (_eventStreamLogs != NULL)
         {
             FSEventStreamStop(_eventStreamLogs);
             FSEventStreamInvalidate(_eventStreamLogs);
             FSEventStreamRelease(_eventStreamLogs);
             self.eventStreamLogs = NULL;
         }
         
         // no stream needed
         if (directoriesToObserve.count == 0)
         {
             return;
         }
         
         // create event stream
         FSEventStreamContext callbackContext;
         callbackContext.version			= 0;
         callbackContext.info			= (__bridge void *)self;
         callbackContext.retain			= NULL;
         callbackContext.release			= NULL;
         callbackContext.copyDescription	= NULL;
         self.eventStreamLogs = FSEventStreamCreate(kCFAllocatorDefault,
                                                       &PARStoreLogsDidChange,
                                                       &callbackContext,
                                                       (__bridge CFArrayRef)directoriesToObserve,
                                                       kFSEventStreamEventIdSinceNow,
                                                       3.0,
                                                       kFSEventStreamCreateFlagUseCFTypes | kFSEventStreamCreateFlagWatchRoot);
         // schedule and start the stream
         FSEventStreamSetDispatchQueue(_eventStreamLogs, [self.fileSystemEventQueue valueForKey:@"queue"]);
         if (FSEventStreamStart(_eventStreamLogs) == false)
         {
             ErrorLog(@"ERROR: could not start FSEventStream for paths: %@", directoriesToObserve);
             FSEventStreamRelease(_eventStreamLogs);
             self.eventStreamLogs = NULL;
         }
     }];

}

- (void)stopFileSystemEventStreams
{
    [self.fileSystemEventQueue dispatchSynchronously:^
     {
         if (_eventStreamDevices != NULL)
         {
             FSEventStreamStop(_eventStreamDevices);
             FSEventStreamInvalidate(_eventStreamDevices);
             FSEventStreamRelease(_eventStreamDevices);
             self.eventStreamDevices = NULL;
         }
         if (_eventStreamLogs != NULL)
         {
             FSEventStreamStop(_eventStreamLogs);
             FSEventStreamInvalidate(_eventStreamLogs);
             FSEventStreamRelease(_eventStreamLogs);
             self.eventStreamLogs = NULL;
         }
     }];
}

// one of the device directory changed --> one of the 'logs' db changed --> time to sync
- (void)respondToFileSystemEventWithPath:(NSString *)path
{
    DebugLog(@"%@ %@", NSStringFromSelector(_cmd), path);
    [self syncSoon];
}


// the 'devices' directory changed --> device was added or removed
static void PARStoreDevicesDidChange(
                                  ConstFSEventStreamRef streamRef,
                                  void *callbackContext,
                                  size_t numEvents,
                                  void *eventPaths,
                                  const FSEventStreamEventFlags eventFlags[],
                                  const FSEventStreamEventId eventIds[])
{
    __weak PARStore *store = (__bridge PARStore *)callbackContext;
    [store refreshFileSystemEventStreamLogs];
}

// one of the device directory changed --> one of the 'logs' db changed --> time to sync
static void PARStoreLogsDidChange(
                                         ConstFSEventStreamRef streamRef,
                                         void *callbackContext,
                                         size_t numEvents,
                                         void *eventPaths,
                                         const FSEventStreamEventFlags eventFlags[],
                                         const FSEventStreamEventId eventIds[])
{
    @autoreleasepool
    {
        NSArray *eventPathsArray = (__bridge NSArray *)eventPaths;
        
        for (NSUInteger i = 0; i < numEvents; ++i)
        {
            // the filesystem event
            // FSEventStreamEventFlags flags = eventFlags[i];
            // FSEventStreamEventId identifier = eventIds[i];
            NSString *eventPath	= [eventPathsArray[i] stringByStandardizingPath];
            
            // notify PARStore
            __weak PARStore *store = (__bridge PARStore *)callbackContext;
            [store respondToFileSystemEventWithPath:eventPath];
        }
    }
}

#endif

@end


#pragma mark - PARChange

@interface PARChange ()
@property (readwrite, copy) NSNumber *timestamp;
@property (readwrite, copy) NSNumber *parentTimestamp;
@property (readwrite, copy) NSString *key;
@property (readwrite, copy) id propertyList;
@end


@implementation PARChange

+ (PARChange *)changeWithTimestamp:(NSNumber *)timestamp parentTimestamp:(NSNumber *)parentTimestamp key:(NSString *)key propertyList:(id)propertyList
{
    PARChange *change = [[PARChange alloc] init];
    change.timestamp = timestamp;
    change.parentTimestamp = parentTimestamp;
    change.key = key;
    change.propertyList = propertyList;
    return change;
}

- (BOOL)isEqual:(id)object
{
    if ([object isKindOfClass:[PARChange class]] == NO)
    {
        return NO;
    }

    PARChange *change1 = self;
    PARChange *change2 = object;
    
    NSNumber *timestamp1 = change1.timestamp;
    NSNumber *timestamp2 = change2.timestamp;
    if (timestamp1 != nil && timestamp2 != nil && [timestamp1 isEqual:timestamp2] == NO)
    {
        return NO;
    }
    if ((timestamp1 == nil || timestamp2 == nil) && timestamp1 != timestamp2)
    {
        return NO;
    }

    NSNumber *parentTimestamp1 = change1.parentTimestamp;
    NSNumber *parentTimestamp2 = change2.parentTimestamp;
    if (parentTimestamp1 != nil && parentTimestamp2 != nil && [parentTimestamp1 isEqual:parentTimestamp2] == NO)
    {
        return NO;
    }
    if ((parentTimestamp1 == nil || parentTimestamp2 == nil) && parentTimestamp1 != parentTimestamp2)
    {
        return NO;
    }

    NSString *key1 = change1.key;
    NSString *key2 = change2.key;
    if (key1 != nil && key2 != nil && [key1 isEqual:key2] == NO)
    {
        return NO;
    }
    if ((key1 == nil || key2 == nil) && key1 != key2)
    {
        return NO;
    }

    id plist1 = change1.propertyList;
    id plist2 = change2.propertyList;
    if (plist1 != nil && plist2 != nil && [plist1 isEqual:plist2] == NO)
    {
        return NO;
    }
    if ((plist1 == nil || plist2 == nil) && plist1 != plist2)
    {
        return NO;
    }
    
    return YES;
}

- (NSString *)description
{
    return [NSString stringWithFormat:@"<%@:%p> = timestamp: %@, parentTimestamp: %@, key: %@, plist: %@", self.class, self, self.timestamp ?: @"", self.parentTimestamp ?: @"", self.key ?: @"–", self.propertyList ?: @"—"];
}

@end

