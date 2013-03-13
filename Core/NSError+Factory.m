//  PARStore
//  Created by Charles Parnot on 3/13/13.
//  Licensed under the terms of the BSD License, as specified in the file 'LICENSE-BSD.txt' included with this distribution


#import "NSError+Factory.h"

@implementation NSError (Factory)

+ (NSError *)errorWithObject:(id)object code:(NSInteger)code localizedDescription:(NSString *)description underlyingError:(NSError *)underlyingError
{
	NSMutableDictionary *userInfo = [NSMutableDictionary dictionary];
	if (description)
        userInfo[NSLocalizedDescriptionKey] = description;
    if (underlyingError)
        userInfo[NSUnderlyingErrorKey] = underlyingError;
    return [NSError errorWithDomain:NSStringFromClass([object class]) code:code userInfo:userInfo];
}

@end
