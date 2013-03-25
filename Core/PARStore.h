//  PARStore
//  Authors: Charles Parnot and Joris Kluivers
//  Licensed under the terms of the BSD License, see license terms in 'LICENSE-BSD.txt'

#import "PARDispatchQueue.h"

/// Key-value store for local storage of app data, with the following characteristics:
///  - persistent storage in a file package
///  - transparent syncing between multiple devices sharing the file via Dropbox, iCloud, or other file-based syncing system
///  - includes full history

/// @name Notifications
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

/// @name Getting Store Information
@property (readonly, copy) NSURL *storeURL;
@property (readonly, copy) NSString *deviceIdentifier;
@property (readonly) BOOL loaded;
@property (readonly) BOOL deleted;

/// @name Accessing Store Content
- (id)propertyListValueForKey:(NSString *)key;
- (void)setPropertyListValue:(id)plist forKey:(NSString *)key;
- (NSDictionary *)allRelevantValues;
- (void)setEntriesFromDictionary:(NSDictionary *)dictionary;
- (void)runTransaction:(PARDispatchBlock)block;

/// @name Managing Data Blobs
- (void) writeBlobData:(NSData *)data toPath:(NSString *)path;
- (NSData *) blobDataAtPath:(NSString *)path;

/// @name Syncing
- (void)sync;
- (id)syncedPropertyListValueForKey:(NSString *)key;
// for subclassing
- (NSArray *)relevantKeysForSync;
- (void)applySyncChange:(NSDictionary *)change;

/// @name Synchronous Method Calls
- (void)loadNow;
- (void)closeNow;
- (void)syncNow;

@end


/** Subclassing notes:

- `-applySyncChange:` --> Default implementation updates the internal representation of the store to include the change. Subclasses can override as follows:
 
        1. inspect the change to detect conflicts
        2. call `[super applySyncChange:change]`
        3. further change the store if necessary to resolve conflicts

    Note: the state of the store is guaranteed to be consistent during this call, by serializing access to the store. This also means the implementation may block the main thread if user input happens while the method is running. It is thus recommended to keep this implementation as fast as possible.
*/
