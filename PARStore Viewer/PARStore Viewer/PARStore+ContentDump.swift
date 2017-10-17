//
//  PARStore+ContentDump.swift
//  PARStore Viewer
//
//  Created by Charles Parnot on 10/13/17.
//  Copyright © 2017 Charles Parnot. All rights reserved.
//

import Foundation

extension PARStore {
    
    var contentDump: String {
        typealias StoreEntry = (key: String, value: Any)
        var entries = [StoreEntry]()
        allEntries().forEach {
            entries.append(($0 as! String, $1))
        }
        entries.sort { $0.key < $1.key }
        let descriptions = entries.map({"\($0.key) : \($0.value)"})
        return descriptions.joined(separator: "\n")
    }
    
}
