//  PARStore
//  Created by Charles Parnot on 3/13/13.
//  Licensed under the terms of the BSD License, as specified in the file 'LICENSE-BSD.txt' included with this distribution

@import Foundation;

NS_ASSUME_NONNULL_BEGIN

@interface NSError (Factory)
+ (NSError *)errorWithObject:(id)object code:(NSInteger)code localizedDescription:(nullable NSString *)description underlyingError:(nullable NSError *)underlyingError;
@end

NS_ASSUME_NONNULL_END