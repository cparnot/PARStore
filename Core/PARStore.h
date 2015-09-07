//  PARStore
//  Authors: Charles Parnot and Joris Kluivers
//  Licensed under the terms of the BSD License, see license terms in 'LICENSE-BSD.txt'

#import "PARDispatchQueue.h"

/// Key-value store for local storage of app data, with the following characteristics:
///  - persistent storage in a file package
///  - transparent syncing between multiple devices sharing the file via Dropbox, iCloud, or other file-based syncing system
///  - includes full history

/// Note on memory management: before releasing a store object, it is recommended to call `saveNow` or `waitUntilFinished`, but not necessary. Any change will schedule a save operation that will still be performed and the object will still be retained until that happens. It is not necessary to explicitely `close` a store.


/// @name Notifications
/// Notifications are posted asynchronously. You cannot expect the store to be in the state that it was after the last operation that triggered the notification. The 'Change' and 'Sync' notifications includes a user info dictionary with two entries @"values" and @"timestamps"; each entry contain a dictionary where the keys correspond to the keys changed by the sync, and the values corresponding property list values and timestamps, respectively. In the case of 'Sync' notifications, these are the same dictionaries as the one passed to the method `applySyncChangeWithValues:timestamps:`.
extern NSString *PARStoreDidLoadNotification;
extern NSString *PARStoreDidCloseNotification;
extern NSString *PARStoreDidDeleteNotification;
extern NSString *PARStoreDidChangeNotification;
extern NSString *PARStoreDidSyncNotification;


@interface PARStore : NSObject <NSFilePresenter>

/// @name Creating and Loading
+ (id)storeWithURL:(NSURL *)url deviceIdentifier:(NSString *)identifier;
+ (id)inMemoryStore;
- (void)load;
- (void)close;
- (void)closeDatabase;

/// @name Getting Store Information
@property (readonly, copy) NSURL *storeURL;
@property (readonly, copy) NSString *deviceIdentifier;
@property (readonly) BOOL loaded;
@property (readonly) BOOL deleted;
@property (readonly) BOOL inMemory;

/// @name Adding and Accessing Values
- (id)propertyListValueForKey:(NSString *)key;
- (void)setPropertyListValue:(id)plist forKey:(NSString *)key;
- (NSDictionary *)allRelevantValues;
- (void)setEntriesFromDictionary:(NSDictionary *)dictionary;
- (void)runTransaction:(PARDispatchBlock)block;

/// @name Adding and Accessing Blobs
- (BOOL)writeBlobData:(NSData *)data toPath:(NSString *)path error:(NSError **)error;
- (BOOL)writeBlobFromPath:(NSString *)sourcePath toPath:(NSString *)path error:(NSError **)error;
- (NSData *)blobDataAtPath:(NSString *)path error:(NSError **)error;
- (BOOL)deleteBlobAtPath:(NSString *)path error:(NSError **)error;
- (NSString *)absolutePathForBlobPath:(NSString *)path;

/// @name Syncing
- (void)sync;
// These methods should not be called from within a transaction, or they will fail.
- (id)syncedPropertyListValueForKey:(NSString *)key;
- (id)syncedPropertyListValueForKey:(NSString *)key timestamp:(NSNumber *)timestamp;
// for subclassing
- (NSArray *)relevantKeysForSync;
- (void)applySyncChangeWithValues:(NSDictionary *)values timestamps:(NSDictionary *)timestamps;

/// @name Getting Timestamps
+ (NSNumber *)timestampNow;
+ (NSNumber *)timestampForDistantPast;
+ (NSNumber *)timestampForDistantFuture;
- (NSDictionary *)mostRecentTimestampsByKey;
- (NSNumber *)mostRecentTimestampForKey:(NSString *)key;
// These methods should not be called from within a transaction, or they will fail.
- (NSDictionary *)mostRecentTimestampsByDeviceIdentifier;
- (NSNumber *)mostRecentTimestampForDeviceIdentifier:(NSString *)deviceIdentifier;

/// @name Synchronous Method Calls
// Synchronous calls can potentially result in longer wait, and should be avoided in the main thread. These should not be called from within a transaction, or they will fail.
// In addition, syncing and saving should normally be triggered automatically and asynchronously.
- (void)loadNow;
- (void)closeNow;
- (void)syncNow;
- (void)saveNow;
- (void)waitUntilFinished;

/// @name History
// This method returns an array of PARChange instances. It should not be called from within a transaction, or it will fail.
- (NSArray *)changesSinceTimestamp:(NSNumber *)timestamp;

// TODO: error handling

@end


@interface PARChange : NSObject
+ (PARChange *)changeWithTimestamp:(NSNumber *)timestamp parentTimestamp:(NSNumber *)parentTimestamp key:(NSString *)key propertyList:(id)propertyList;
@property (readonly, copy) NSNumber *timestamp;
@property (readonly, copy) NSNumber *parentTimestamp;
@property (readonly, copy) NSString *key;
@property (readonly, copy) id propertyList;
- (BOOL)isEqual:(id)object;
@end


/** Subclassing notes:

- `-applySyncChangeWithValues:timestamps` --> Default implementation updates the internal representation of the store to include the change. Subclasses can override as follows:
 
        1. inspect the change to detect conflicts
        2. call `[super applySyncChangeWithValues:values timestamps:timestamps]`
        3. further change the store if necessary to resolve conflicts

    Note: the state of the store is guaranteed to be consistent during this call, by serializing access to the store. This also means the implementation may block the main thread if user input happens while the method is running. It is thus recommended to keep this implementation as fast as possible.
*/
