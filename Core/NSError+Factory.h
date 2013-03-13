//  PARStore
//  Created by Charles Parnot on 3/13/13.
//  Licensed under the terms of the BSD License, as specified in the file 'LICENSE-BSD.txt' included with this distribution


@interface NSError (Factory)
+ (NSError *)errorWithObject:(id)object code:(NSInteger)code localizedDescription:(NSString *)description underlyingError:(NSError *)underlyingError;
@end
