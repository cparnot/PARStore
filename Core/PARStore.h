//  PARStore
//  Authors: Charles Parnot and Joris Kluivers
//  Licensed under the terms of the BSD License, see license terms in 'LICENSE-BSD.txt'

#import "PARDispatchQueue.h"


/// Key-value store for local storage of app data, with the following characteristics:
///  - persistent storage in a file package
///  - transparent syncing between multiple devices sharing the file via Dropbox, iCloud, or other file-based syncing system
///  - includes full history

/// Note on memory management: before releasing a store object, it is recommended to call `saveNow` or `waitUntilFinished`, but not necessary. Any change will schedule a save operation that will still be performed and the object will still be retained until that happens. It is not necessary to explicitly `close` a store.

// TODO: add documentation to methods, also include whether the method will hit the db or not

NS_ASSUME_NONNULL_BEGIN

@class PARChange;

/// @name Notifications
/// Notifications are posted asynchronously. You cannot expect the store to be in the state that it was after the last operation that triggered the notification. The 'Change' and 'Sync' notifications includes a user info dictionary with two entries @"values" and @"timestamps"; each entry contain a dictionary where the keys correspond to the keys changed by the sync, and the values corresponding property list values and timestamps, respectively. In the case of 'Sync' notifications, these are the same dictionaries as the one passed to the method `applySyncChangeWithValues:timestamps:`.

extern NSString *PARStoreDidLoadNotification;
extern NSString *PARStoreDidTearDownNotification;
extern NSString *PARStoreDidDeleteNotification;
extern NSString *PARStoreDidChangeNotification;
extern NSString *PARStoreDidSyncNotification;

@interface PARStore : NSObject <NSFilePresenter>

/// @name Creating and Loading
+ (instancetype)storeWithURL:(nullable NSURL *)url deviceIdentifier:(NSString *)identifier;
- (instancetype)initWithURL:(nullable NSURL *)url deviceIdentifier:(NSString *)identifier;
+ (instancetype)inMemoryStore;
- (void)load;
- (void)closeDatabase;
- (void)tearDown;

/// @name Getting Store Information
@property (readonly, copy, nullable) NSURL *storeURL;
@property (readonly, copy) NSString *deviceIdentifier;
@property (readonly, copy) NSArray *foreignDeviceIdentifiers;
@property (readonly) BOOL loaded;
@property (readonly) BOOL deleted;
@property (readonly) BOOL inMemory;
@property (readonly) BOOL inMemoryCacheEnabled;

/// @name Memory Cache
- (void)disableInMemoryCache;

/// @name Adding and Accessing Values
- (nullable id)propertyListValueForKey:(NSString *)key;
- (void)setPropertyListValue:(nullable id)plist forKey:(NSString *)key;
- (NSArray *)allKeys;
- (NSDictionary *)allEntries;
- (void)setEntriesFromDictionary:(NSDictionary *)dictionary NS_SWIFT_NAME(setEntries(from:));
- (void)setEntriesFromDictionary:(NSDictionary *)dictionary timestampApplied:(NSNumber * __autoreleasing _Nonnull * _Nullable)returnTimestamp NS_SWIFT_NAME(setEntries(from:timestampApplied:));

- (void)runTransaction:(PARDispatchBlock)block;

/// @name Adding and Accessing Blobs
- (BOOL)writeBlobData:(NSData *)data toPath:(NSString *)path error:(NSError **)error;
- (BOOL)writeBlobFromPath:(NSString *)sourcePath toPath:(NSString *)path error:(NSError **)error;
- (nullable NSData *)blobDataAtPath:(NSString *)path error:(NSError **)error;
- (BOOL)deleteBlobAtPath:(NSString *)path error:(NSError **)error;
- (nullable NSString *)absolutePathForBlobPath:(NSString *)path;
- (void)enumerateBlobs:(void(^)(NSString *path))block;

/// @name Syncing
- (void)sync;

// These methods should not be called from within a transaction, or they will fail.
- (NSArray *)fetchAllKeys;
- (nullable id)fetchPropertyListValueForKey:(NSString *)key;
- (nullable id)fetchPropertyListValueForKey:(NSString *)key timestamp:(nullable NSNumber *)timestamp;
// for subclassing
- (void)applySyncChangeWithValues:(NSDictionary *)values timestamps:(NSDictionary *)timestamps NS_REQUIRES_SUPER;

/// @name Merging
- (void)mergeStore:(PARStore *)store unsafeDeviceIdentifiers:(NSArray *)activeDeviceIdentifiers completionHandler:(nullable void(^)(NSError*))completionHandler;

/// @name Getting Timestamps
+ (NSNumber *)timestampNow;
+ (NSNumber *)timestampForDistantPast;
+ (NSNumber *)timestampForDistantFuture;

- (NSDictionary *)mostRecentTimestampsByKey;
- (nullable NSNumber *)mostRecentTimestampForKey:(NSString *)key;
// These methods should not be called from within a transaction, or they will fail.
- (NSDictionary *)mostRecentTimestampsByDeviceIdentifier;
- (nullable NSNumber *)mostRecentTimestampForDeviceIdentifier:(nullable NSString *)deviceIdentifier;

/// @name Synchronous Method Calls
// Synchronous calls can potentially result in longer wait, and should be avoided in the main thread. These should not be called from within a transaction, or they will fail.
// In addition, syncing and saving should normally be triggered automatically and asynchronously.
- (void)loadNow;
- (void)syncNow;
- (void)saveNow;
- (void)closeDatabaseNow;
- (void)tearDownNow;
- (void)waitUntilFinished;

/// @name History
// This method returns an array of PARChange instances. It should not be called from within a transaction, or it will fail.
- (NSArray<PARChange *> *)fetchChangesSinceTimestamp:(nullable NSNumber *)timestamp;

/// This method returns an array of PARChange instances for the device identifier passed in.
/// It should not be called from within a transaction, or it will fail.
/// Pass in nil for the device identifier to get results for all devices.
- (NSArray<PARChange *> *)fetchChangesSinceTimestamp:(nullable NSNumber *)timestamp forDeviceIdentifier:(nullable NSString *)deviceIdentifier;

/// This method returns an array of PARChange instances for the device identifier passed in, between (and including) the timestamps passed.
/// It should not be called from within a transaction, or it will fail.
/// Pass in nil for the device identifier to get results for all devices.
/// Pass nil for either timestamp to have an open range.
- (NSArray<PARChange *> *)fetchChangesFromTimestamp:(nullable NSNumber *)firstTimestamp toTimestamp:(nullable NSNumber *)lastTimestamp forDeviceIdentifier:(nullable NSString *)deviceIdentifier;

/// Fetches the most recent predecessor of each change passed in, for the device passed in. If the device passed in is `nil`,
/// it gives back the predecessor from any device.
/// The returned dictionary contains the predecessors by `key` atribute.
/// If a key is missing from the dictionary, no predecessor was found for that key.
- (NSDictionary<NSString *, PARChange *> *)fetchMostRecentPredecessorsOfChanges:(NSArray *)changes forDeviceIdentifier:(nullable NSString *)deviceIdentifier;

/// Fetches the most recent successor of each change passed in, for the device passed in. If the device passed in is `nil`,
/// it gives back the successor from any device.
/// The returned dictionary contains the successors by `key` atribute.
/// If a key is missing from the dictionary, no successor was found for that key (ie the change itself is most recent).
- (NSDictionary<NSString *, PARChange *> *)fetchMostRecentSuccessorsOfChanges:(NSArray *)changes forDeviceIdentifier:(nullable NSString *)deviceIdentifier;

/// Returns an array representing the most recent set of changes matching a given key prefix. A single device can be passed in, or nil,
/// to search across all devices.
- (NSArray<PARChange *> *)fetchMostRecentChangesMatchingKeyPrefix:(NSString *)prefix forDeviceIdentifier:(nullable NSString *)fetchDeviceIdentifier;

// TODO: error handling

@end


// Accesss for backends that need to import data from other devices via cloud services.
@interface PARStore (RemoteUpdates)

/// Inserts the changes passed for the device given.
/// If `appendOnly` is false, it will insert the new changes regardless of the existing stored changes,
/// though it will avoid inserting duplicates.
/// If `appendOnly` is true, it will only insert a change that occurs on or after the most recent change in the store.
/// Returns `NO` on failure, and the `error` is set on failure.
- (BOOL)insertChanges:(NSArray *)changes forDeviceIdentifier:(NSString *)deviceIdentifier appendOnly:(BOOL)appendOnly error:(NSError * __autoreleasing *)error;

@end


@interface PARChange : NSObject
+ (PARChange *)changeWithTimestamp:(NSNumber *)timestamp parentTimestamp:(nullable NSNumber *)parentTimestamp key:(NSString *)key propertyList:(nullable id)propertyList;
+ (PARChange *)changeWithPropertyDictionary:(NSDictionary *)propertyDictionary;
@property (readonly, copy) NSNumber *timestamp;
@property (readonly, copy, nullable) NSNumber *parentTimestamp;
@property (readonly, copy) NSString *key;
@property (readonly, copy, nullable) id propertyList;
@property (readonly, copy) NSDictionary *propertyDictionary;
- (BOOL)isEqual:(nullable id)object;
@end


NS_ASSUME_NONNULL_END

/** Subclassing notes:

- `-applySyncChangeWithValues:timestamps` --> Default implementation updates the internal representation of the store to include the change. Subclasses can override as follows:
 
        1. inspect the change to detect conflicts
        2. call `[super applySyncChangeWithValues:values timestamps:timestamps]`
        3. further change the store if necessary to resolve conflicts

    Note: the state of the store is guaranteed to be consistent during this call, by serializing access to the store. This also means the implementation may block the main thread if user input happens while the method is running. It is thus recommended to keep this implementation as fast as possible.
*/
