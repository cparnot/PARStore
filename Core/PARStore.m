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


NSString *PARStoreDidLoadNotification   = @"PARStoreDidLoadNotification";
NSString *PARStoreDidCloseNotification  = @"PARStoreDidCloseNotification";
NSString *PARStoreDidDeleteNotification = @"PARStoreDidDeleteNotification";
NSString *PARStoreDidChangeNotification = @"PARStoreDidChangeNotification";
NSString *PARStoreDidSyncNotification   = @"PARStoreDidSyncNotification";


@interface PARStore ()
@property (readwrite, copy) NSURL *storeURL;
@property (readwrite, copy) NSString *deviceIdentifier;

// databaseQueue serializes access to all CoreData related stuff
@property (retain) PARDispatchQueue *databaseQueue;
@property (retain) NSManagedObjectContext *_managedObjectContext;
@property (retain) NSPersistentStore *readwriteDatabase;
@property (copy) NSArray *readonlyDatabases;
@property (copy) NSMapTable *databaseTimestamps;
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
    
    // misc initializations
    store.databaseTimestamps = [NSMapTable weakToStrongObjectsMapTable];
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
    store._loaded = YES;
    store._inMemory = YES;
    store.databaseQueue = nil;
    return store;
}

- (void)_load
{
    NSAssert([self.databaseQueue isCurrentQueue], @"%@:%@ should only be called from within the database queue", [self class], NSStringFromSelector(_cmd));
    if ([self loaded])
        return;
    [self _sync];
    if ([self loaded])
    {
        DebugLog(@"%@ added as file presenter", self.deviceIdentifier);
        [NSFileCoordinator addFilePresenter:self];
    }
}

- (void)load
{
    [self.databaseQueue dispatchAsynchronously:^{ [self _load]; }];
}

- (void)loadNow
{
    [self.databaseQueue dispatchSynchronously:^{ [self _load]; }];
}

- (void)_close
{
    NSAssert([self.memoryQueue isCurrentQueue], @"%@:%@ should only be called from within the memory queue", [self class], NSStringFromSelector(_cmd));

    // reset database
    // to avoid deadlocks, it is **critical** that the call into the database queue be asynchronous
    if (!self._deleted && !self._inMemory)
        [self.databaseQueue dispatchAsynchronously:^
         {
             [self save:NULL];
             [self closeDatabase];
             [NSFileCoordinator removeFilePresenter:self];
         }];
    
    // reset in-memory info
    self.databaseTimestamps = [NSMapTable weakToStrongObjectsMapTable];
    self._memory = [NSMutableDictionary dictionary];
    self._loaded = NO;
    self._deleted = NO;

    // to make sure the database is saved when the notification is received, the call is scheduled from within the database queue
    [self.databaseQueue dispatchAsynchronously:^
    {
        [self postNotificationWithName:PARStoreDidCloseNotification];
    }];
}

- (void)close
{
    [self.memoryQueue dispatchAsynchronously:^{ [self _close]; }];
}

- (void)closeNow
{
    [self.memoryQueue       dispatchSynchronously:^{ [self _close]; }];
    [self.databaseQueue     dispatchSynchronously:^{ }];
    [self.notificationQueue dispatchSynchronously:^{ }];
}

- (void)waitUntilFinished
{
    [self.memoryQueue       dispatchSynchronously:^{ }];
    [self.databaseQueue     dispatchSynchronously:^{ [self save:NULL]; [self _sync]; }];
    [self.notificationQueue dispatchSynchronously:^{ }];
}

// since `self` is retained by the blocks used for scheduling a 'save' or a 'sync', we do not expect to be in a situation where the store is in an unsaved or inconsistent state when dealloc-ed
// the database queue should not have any timer set anymore
- (void)dealloc
{
    // do not access '_loaded' via the queue or via the safe [self loaded] accessor, to avoid further waiting for a queue: if closed properly, this will be safe, and otherwise, we are already in trouble
    NSUInteger timerCount = _databaseQueue.timerCount;
    if (timerCount > 0)
        ErrorLog(@"Unexpected timer count of %@ for the database queue of store at path: %@", @(timerCount), [self.storeURL path]);
}


#pragma mark - File Package

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
        localError = [NSError errorWithObject:self code:1 localizedDescription:[NSString stringWithFormat:@"%@ only supports files and cannot create file package with URL: %@", NSStringFromClass([self class]), self.storeURL] underlyingError:nil];
		success = NO;
	}
		
	// file should be a directory
	NSString *storePath = [self.storeURL path];
	BOOL isDir = NO;
	BOOL fileExists = [[NSFileManager defaultManager] fileExistsAtPath:storePath isDirectory:&isDir];
	if (success && fileExists && !isDir)
	{
        localError = [NSError errorWithObject:self code:2 localizedDescription:[NSString stringWithFormat:@"The path for a store should be a directory at path: %@", storePath] underlyingError:nil];
		success = NO;
	}
		
	// file package should have a 'devices' subdirectory
	NSString *devicesPath = [self deviceRootPath];
	BOOL devicesPathIsDir = NO;
	BOOL devicesDirExists = [[NSFileManager defaultManager] fileExistsAtPath:devicesPath isDirectory:&devicesPathIsDir];
	if (success && devicesDirExists && !devicesPathIsDir)
	{
        localError = [NSError errorWithObject:self code:3 localizedDescription:[NSString stringWithFormat:@"The file package for a store should have a 'devices' subdirectory at path: %@", storePath] underlyingError:nil];
		success = NO;
	}

	// 'devices' subdir may have a 'deviceID' subdir
	NSString *identifierPath = [self readwriteDirectoryPath];
	BOOL identifierPathIsDir = NO;
	BOOL identifierDirExists = [[NSFileManager defaultManager] fileExistsAtPath:identifierPath isDirectory:&identifierPathIsDir];
	if (success && identifierDirExists && !identifierPathIsDir)
    {
        localError = [NSError errorWithObject:self code:4 localizedDescription:[NSString stringWithFormat:@"The device identifier subpath '%@' should be a directory in the file package for the store at path: %@", self.deviceIdentifier, storePath] underlyingError:nil];
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
                localError = [NSError errorWithObject:self code:5 localizedDescription:[NSString stringWithFormat:@"Could not create the root directory for the file package for the store at path: %@", storePath] underlyingError:fmError];
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
                localError = [NSError errorWithObject:self code:6 localizedDescription:[NSString stringWithFormat:@"Could not create a subdirectory for the device identifier '%@' in the file package for the store at path: %@", self.deviceIdentifier, storePath] underlyingError:fmError];
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


#pragma mark - Core Data

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
    NSDictionary *storeOptions = @{
        NSMigratePersistentStoresAutomaticallyOption : @YES,
        NSInferMappingModelAutomaticallyOption:        @YES,
        NSReadOnlyPersistentStoreOption:               readOnly ? @YES : @NO
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
    NSAssert([self.databaseQueue isCurrentQueue], @"%@:%@ should only be called from within the database queue", [self class],NSStringFromSelector(_cmd));

    // lazy creation
    NSManagedObjectContext *managedObjectContext = self._managedObjectContext;
    if (managedObjectContext || self._inMemory)
        return managedObjectContext;

    // model
    NSManagedObjectModel *mom = [PARStore managedObjectModel];
    if (!mom)
        return nil;
    
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
    NSAssert([self.databaseQueue isCurrentQueue], @"%@:%@ should only be called from within the database queue", [self class],NSStringFromSelector(_cmd));
    if (self._inMemory)
        return;
    
    NSPersistentStoreCoordinator *psc = [[self managedObjectContext] persistentStoreCoordinator];
    NSMutableArray *stores = [NSMutableArray arrayWithArray:self.readonlyDatabases];
    NSArray *currentDirs = [stores valueForKeyPath:@"URL.path"];
    NSArray *allDirs = [self readonlyDirectoryPaths];
    for (NSString *path in allDirs)
    {
		NSString *storePath = [path stringByAppendingPathComponent:PARDatabaseFileName];
        if ([currentDirs containsObject:storePath])
        {
			// instead of ignoring this store, reload it to get the synced contents if available
			// TODO: very inefficient, should only reload when it has been changed of course.
			
			NSPersistentStore *existingStore = [psc persistentStoreForURL:[NSURL fileURLWithPath:storePath]];
			NSError *removeError = nil;
			
			if (![psc removePersistentStore:existingStore error:&removeError]) {
				ErrorLog(@"Error removing store: %@", removeError);
			}
		}
		
        NSPersistentStore *store = [self addPersistentStoreWithCoordinator:psc dirPath:path readOnly:YES error:NULL];
        if (store)
            [stores addObject:store];
    }
    self.readonlyDatabases = [NSArray arrayWithArray:stores];
}

- (BOOL)save:(NSError **)error
{
    NSAssert([self.databaseQueue isCurrentQueue], @"%@:%@ should only be called from within the database queue", [self class],NSStringFromSelector(_cmd));
    [self.databaseQueue cancelTimerWithName:@"save_delay"];
    [self.databaseQueue cancelTimerWithName:@"save_coalesce"];
    if (self._inMemory || [self deleted])
        return NO;
    
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
        localError = [NSError errorWithObject:self code:7 localizedDescription:@"No managed object context" underlyingError:nil];
    }
    
    if (localError)
    {
        NSString *storePath = [self.storeURL path];
        ErrorLog(@"Could not save store:\npath: %@\nerror: %@\n", storePath, [localError localizedDescription]);
        if (error != NULL)
            *error = [NSError errorWithObject:self code:8 localizedDescription:[NSString stringWithFormat:@"Could not save store for device identifier '%@' at path: %@", self.deviceIdentifier, [self.storeURL path]] underlyingError:localError];
        return NO;
    }

    return YES;
}

- (void)saveNow
{
    [self.databaseQueue dispatchSynchronously:^{ if ([self loaded]) [self save:NULL]; }];
}

- (void)saveSoon
{
    [self.databaseQueue scheduleTimerWithName:@"save_delay" timeInterval:1.0 behavior:PARTimerBehaviorDelay block:^{ [self save:NULL]; }];
    [self.databaseQueue scheduleTimerWithName:@"save_coalesce" timeInterval:15.0 behavior:PARTimerBehaviorCoalesce block:^{ [self save:NULL]; }];
}

- (void)closeDatabase
{
    NSAssert([self.databaseQueue isCurrentQueue], @"%@:%@ should only be called from within the database queue", [self class], NSStringFromSelector(_cmd));
    [self.databaseQueue cancelAllTimers];
    self._managedObjectContext = nil;
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
            *error = [NSError errorWithObject:self code:9 localizedDescription:description underlyingError:nil];
        return nil;
    }
    return value;
}


#define MICROSECONDS_PER_SECOND (1000 * 1000)

- (void)setPropertyListValue:(id)plist forKey:(NSString *)key
{
    [self.memoryQueue dispatchSynchronously:^
     {
         self._memory[key] = plist;
         [self postNotificationWithName:PARStoreDidChangeNotification];
         
         if (self._inMemory)
             return;
         
         NSError *error = nil;
         NSData *blob = [self dataFromPropertyList:plist error:&error];
         if (!blob)
             ErrorLog(@"Error creating data from plist:\nkey: %@:\nplist: %@\nerror: %@", key, plist, [error localizedDescription]);
         else
         {
             // set the timestamp **before** dispatching the block, so we have the current date, not the date at which the block will be run
             // timestamp is cast to a signed 64-bit integer (we can't use NSInteger on iOS for that)
             NSNumber *oldTimestamp = self._memoryKeyTimestamps[key];
             NSTimeInterval timestampInSeconds = [[NSDate date] timeIntervalSinceReferenceDate];
             NSNumber *newTimestamp = @((uint64_t)(timestampInSeconds * MICROSECONDS_PER_SECOND));
             self._memoryKeyTimestamps[key] = newTimestamp;
             [self.databaseQueue dispatchAsynchronously:
              ^{
                  NSManagedObjectContext *moc = [self managedObjectContext];
                  if (!moc)
                      return;
                  NSManagedObject *newLog = [NSEntityDescription insertNewObjectForEntityForName:@"Log" inManagedObjectContext:moc];
                  [newLog setValue:newTimestamp forKey:@"timestamp"];
                  [newLog setValue:oldTimestamp forKey:@"parentTimestamp"];
                  [newLog setValue:key forKey:@"key"];
                  [newLog setValue:blob forKey:@"blob"];
                  [self.databaseTimestamps setObject:newTimestamp forKey:self.readwriteDatabase];
                  
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
         [self._memory addEntriesFromDictionary:dictionary];
         [self postNotificationWithName:PARStoreDidChangeNotification];
         
         if (self._inMemory)
             return;
         
         // set the timestamp **before** dispatching to the database queue, so we have the current date, not the date at which the block runs
         // timestamp is cast to a signed 64-bit integer (we can't use NSInteger on iOS for that)
         NSTimeInterval timestampInSeconds = [[NSDate date] timeIntervalSinceReferenceDate];
         NSNumber *newTimestamp = @((uint64_t)(timestampInSeconds * MICROSECONDS_PER_SECOND));
         
         // memory timestamps
         NSMutableDictionary *oldTimestamps = [NSMutableDictionary dictionaryWithCapacity:dictionary.count];
         for (NSString *key in dictionary.keyEnumerator)
         {
             NSNumber *oldTimestamp = self._memoryKeyTimestamps[key];
             if (oldTimestamp)
                 [oldTimestamps setObject:self._memoryKeyTimestamps[key] forKey:key];
             self._memoryKeyTimestamps[key] = newTimestamp;
         }
         
         [self.databaseQueue dispatchAsynchronously: ^
          {
              NSManagedObjectContext *moc = [self managedObjectContext];
              if (!moc)
                  return;
              
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
              [self.databaseTimestamps setObject:newTimestamp forKey:self.readwriteDatabase];
              
              // schedule database save
              [self saveSoon];
          }];
     }];
}

- (void)runTransaction:(PARDispatchBlock)block
{
    [self.memoryQueue dispatchSynchronously:block];
}

- (BOOL)loaded
{
    __block BOOL loaded = NO;
    [self.memoryQueue dispatchSynchronously:^{ loaded = self._loaded; }];
    return loaded;
}

- (BOOL)deleted
{
    __block BOOL deleted = NO;
    [self.memoryQueue dispatchSynchronously:^{ deleted = self._deleted; }];
    return deleted;
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
            *error = [NSError errorWithObject:self code:10 localizedDescription:description underlyingError:nil];
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
            localError = [NSError errorWithObject:self code:11 localizedDescription:[NSString stringWithFormat:@"Could not create parent directories before writing blob at path '%@'", newURL.path] underlyingError:errorCreatingDir];
            return;
        }
        
        // write to disk (overwrite any file that was at that same path before)
        NSError *errorWritingData = nil;
        BOOL successWritingData = [data writeToURL:newURL options:0 error:&errorWritingData];
        if (!successWritingData)
            localError = [NSError errorWithObject:self code:12 localizedDescription:[NSString stringWithFormat:@"Could not save data blob at path '%@'", newURL.path] underlyingError:errorWritingData];
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
            *error = [NSError errorWithObject:self code:13 localizedDescription:description underlyingError:nil];
        return NO;
    }
    
    // nil absolute path = empty data + warning
    if (sourcePath == nil)
    {
        NSString *description = [NSString stringWithFormat:@"Blob cannot be saved because method '%@' was called with a nil value for the source path parameter, in store at path '%@'", NSStringFromSelector(_cmd), self.storeURL.path];
        ErrorLog(@"%@", description);
        if (error != NULL)
            *error = [NSError errorWithObject:self code:14 localizedDescription:description underlyingError:nil];
        return NO;
    }
    
    // blobs for in-memory store are stored... in memory
    if (self._inMemory)
    {
        NSError *errorReadingData = nil;
        NSData *sourceData = [NSData dataWithContentsOfFile:sourcePath options:NSDataReadingMappedIfSafe error:&errorReadingData];
        if (!sourceData)
        {
            if (error != NULL)
                *error = [NSError errorWithObject:self code:15 localizedDescription:[NSString stringWithFormat:@"Could not read data to store as blob in memory store, from source file at path '%@', ", sourcePath] underlyingError:errorReadingData];
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
             localError = [NSError errorWithObject:self code:16 localizedDescription:[NSString stringWithFormat:@"Could not create parent directories before writing blob at path '%@'", newTargetURL.path] underlyingError:errorCreatingDir];
             return;
         }
         
         // write to disk (overwrite any file that was at that same path before)
         NSError *errorWritingData = nil;
         BOOL successWritingData = [[NSFileManager defaultManager] copyItemAtURL:[NSURL fileURLWithPath:sourcePath] toURL:newTargetURL error:&errorWritingData];
         if (!successWritingData)
             localError = [NSError errorWithObject:self code:17 localizedDescription:[NSString stringWithFormat:@"Could not copy file from source path '%@', into blob directory at path '%@'", sourcePath, newTargetURL.path] underlyingError:errorWritingData];
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
            *error = [NSError errorWithObject:self code:10 localizedDescription:description underlyingError:nil];
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
             localError = [NSError errorWithObject:self code:12 localizedDescription:[NSString stringWithFormat:@"Could not delete data blob at path '%@'", newURL.path] underlyingError:error];
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
            *error = [NSError errorWithObject:self code:18 localizedDescription:description underlyingError:nil];
        return NO;
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
            localError = [NSError errorWithObject:self code:19 localizedDescription:[NSString stringWithFormat:@"Could not read data blob at path '%@'", newURL.path] underlyingError:errorReadingData];
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


#pragma mark - Sync

- (NSArray *)relevantKeysForSync
{
    return [NSArray array];
}

- (void)applySyncChangeWithValues:(NSDictionary *)values timestamps:(NSDictionary *)timestamps
{
    NSAssert([self.memoryQueue isCurrentQueue], @"%@:%@ should only be called from within the memory queue", [self class],NSStringFromSelector(_cmd));
    [values enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *s)     { self._memory[key] = obj;              }];
    [timestamps enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *s) { self._memoryKeyTimestamps[key] = obj; }];
}


// as long as we scan the results of a fetch in order, this guarantees that data is only queried from the database in batches of 1000
#define LOGS_BATCH_SIZE 1000

- (void)_sync
{
    NSAssert([self.databaseQueue isCurrentQueue], @"%@:%@ should only be called from within the database queue", [self class],NSStringFromSelector(_cmd));
    
    // sync is not relevant for in-memory stores
    if (self._inMemory)
        return;

    // never resuscitate a deleted store
    if ([self deleted])
        return;
    
    // without a moc, the rest of the code will throw an exception
    if (![self managedObjectContext])
    {
        ErrorLog(@"Could not load managed object context and sync store at path '%@'", [self.storeURL path]);
        return;
    }
    
    // reset timers
    [self.databaseQueue cancelTimerWithName:@"sync_delay"];
    [self.databaseQueue cancelTimerWithName:@"sync_coalesce"];

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
    NSArray *allLogs = [[self managedObjectContext] executeFetchRequest:logsRequest error:&errorLogs];
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
        if (![updatedDatabaseTimestamps objectForKey:store])
            [updatedDatabaseTimestamps setObject:logTimestamp forKey:store];
        
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
            timestamp = [PARStore timestampForDistantPath]; // distant past
        newKeyTimestamps[key] = timestamp;
    }
    self.keyTimestamps = [NSDictionary dictionaryWithDictionary:newKeyTimestamps];
    
    // update the timestamps for the databases
    NSMapTable *newDatabaseTimestamps = [NSMapTable weakToStrongObjectsMapTable];
    for (NSPersistentStore *store in [self.readonlyDatabases arrayByAddingObject:self.readwriteDatabase])
    {
        NSNumber *timestamp = [updatedDatabaseTimestamps objectForKey:store];
        if (!timestamp)
            timestamp = [self.databaseTimestamps objectForKey:store];
        if (!timestamp)
            timestamp = [PARStore timestampForDistantPath]; // distant past
        [newDatabaseTimestamps setObject:timestamp forKey:store];
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
             [self postNotificationWithName:PARStoreDidLoadNotification];
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
             
             [self applySyncChangeWithValues:changedValues timestamps:changedTimestamps];
             [self postNotificationWithName:PARStoreDidSyncNotification];
         }];
    }
}

- (id)syncedPropertyListValueForKey:(NSString *)key
{
    if (self._inMemory)
        return nil;
    
    __block id plist = nil;
    [self.databaseQueue dispatchSynchronously:^
    {
        NSError *fetchError = nil;
        NSFetchRequest *request = [NSFetchRequest fetchRequestWithEntityName:@"Log"];
        [request setSortDescriptors:@[[NSSortDescriptor sortDescriptorWithKey:@"timestamp" ascending:NO]]];
        [request setPredicate:[NSPredicate predicateWithFormat:@"key == %@", key]];
        [request setFetchLimit:1];
        [request setReturnsObjectsAsFaults:NO];
        NSArray *results = [[self managedObjectContext] executeFetchRequest:request error:&fetchError];
        if (!results)
        {
            ErrorLog(@"Error fetching logs for store:\npath: %@\nerror: %@", [self.storeURL path], fetchError);
            return;
        }

        if ([results count] > 0)
        {
            NSManagedObject *latestLog = [results lastObject];
            NSError *plistError = nil;
            plist = [self propertyListFromData:[latestLog valueForKey:@"blob"] error:&plistError];
            if (!plist)
                ErrorLog(@"Error deserializing 'layout' data in Logs database:\nrow: %@\nfile: %@\nerror: %@", latestLog.objectID, latestLog.objectID.persistentStore.URL.path, plistError);
        }
    }];
    
    return plist;
}

- (void)sync
{
    [self.databaseQueue dispatchAsynchronously:^{ if ([self loaded]) [self _sync]; }];
}

- (void)syncNow
{
    [self.databaseQueue dispatchSynchronously:^{ if ([self loaded]) [self _sync]; }];
}


#pragma mark - Notifications

// post notifications asynchronously, in a dedicated serial queue, to
// (1) avoid deadlocks when using a block for the notification callback and tries to access the data
// (2) enforce serialization of the notifications
// (3) allows us to properly close the store with all the notifications sent
- (void)postNotificationWithName:(NSString *)notificationName
{
    [self.notificationQueue dispatchAsynchronously:^
    {
        #ifdef TEST
        if (self.shouldThrottleNotifications)
            [NSThread sleepForTimeInterval:1.0];
        #endif
        [[NSNotificationCenter defaultCenter] postNotificationName:notificationName object:self];
    }];
    
}


#pragma mark - Getting Timestamps

+ (NSNumber *)timestampForDistantPath
{
    static NSNumber *timestampForDistantPath = nil;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^
    {
        timestampForDistantPath = @(NSIntegerMin);
    });
    return timestampForDistantPath;
}

- (NSDictionary *)mostRecentTimestampsByDeviceIdentifiers
{
    NSMutableDictionary *timestamps = [NSMutableDictionary dictionary];
    [self.databaseQueue dispatchSynchronously:^
     {
         NSArray *allStores = [self._managedObjectContext.persistentStoreCoordinator persistentStores];
         for (NSPersistentStore *store in allStores)
         {
             NSString *deviceIdentifier = [self deviceIdentifierForDatabasePath:store.URL.path];
             if (!deviceIdentifier)
                 continue;
             NSNumber *timestamp = [self.databaseTimestamps objectForKey:store];
             if (!timestamp)
                 timestamp = [PARStore timestampForDistantPath];
             timestamps[deviceIdentifier] = timestamp;
         }
     }];
    return [NSDictionary dictionaryWithDictionary:timestamps];
}

- (NSNumber *)mostRecentTimestampWithDeviceIdentifier:(NSString *)deviceIdentifier
{
    if (deviceIdentifier == nil)
        return nil;
    
    __block NSNumber *timestamp = nil;
    [self.databaseQueue dispatchSynchronously:^
    {
        // store
        NSPersistentStore *store = [self._managedObjectContext.persistentStoreCoordinator persistentStoreForURL:[NSURL fileURLWithPath:[self databasePathForDeviceIdentifier:deviceIdentifier]]];
        if (!store)
            return;
        
        // timestamp
        timestamp = [self.databaseTimestamps objectForKey:store];
    }];
    
    return timestamp;
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
        
    [self.databaseQueue scheduleTimerWithName:@"sync_delay" timeInterval:1.0 behavior:PARTimerBehaviorDelay block:^{ [self _sync]; }];
    [self.databaseQueue scheduleTimerWithName:@"sync_coalesce" timeInterval:15.0 behavior:PARTimerBehaviorCoalesce block:^{ [self _sync]; }];
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
	writer(^{
        [self sync];
    });
}

- (void)savePresentedItemChangesWithCompletionHandler:(void (^)(NSError *errorOrNil))completionHandler
{
	[self.databaseQueue dispatchAsynchronously:^
     {
         NSError *saveError = nil;
         BOOL success = [[self managedObjectContext] save:&saveError];
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
        [self closeDatabase];
        [NSFileCoordinator removeFilePresenter:self];
    }];
	if (completionHandler)
	{
		completionHandler(nil);
	}
    
    [self postNotificationWithName:PARStoreDidDeleteNotification];
}

@end
