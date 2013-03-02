//  PARStore
//  Created by Charles Parnot on 3/2/13.
//  Copyright (c) 2013 Charles Parnot. All rights reserved.


#import "PARStore.h"

@interface PARStoreExample : PARStore

@property (copy) NSString *first;
@property (copy) NSString *last;
@property (copy) NSString *title;
@property (copy) NSString *summary;


- (NSArray *)relevantKeysForSync;

@end
