//
//  PARStoreDocument.swift
//  PARStore Viewer
//
//  Created by Charles Parnot on 10/13/17.
//  Copyright © 2017 Charles Parnot. All rights reserved.
//

import Cocoa

class PARStoreDocument: NSDocument {
    
    var historyViewController: HistoryViewController?
    
    var store: PARStore?
    
    override func makeWindowControllers() {
        let windowController: DocumentWindowController = NSStoryboard(name: "Main", bundle: nil).instantiateController(withIdentifier: "Document Window Controller") as! DocumentWindowController
        addWindowController(windowController)
        historyViewController = windowController.historyViewController;
        historyViewController?.store = store
        windowController.refresh()
    }
    
    override func data(ofType typeName: String) throws -> Data {
        fatalError("Method not implemented: \(#selector(data(ofType:)))")
    }
    
    override func read(from url: URL, ofType typeName: String) throws {
        var deviceIdentifier = NSUUID().uuidString
        let deviceSubpaths = try FileManager.default.contentsOfDirectory(atPath: url.appendingPathComponent("devices").path)
        for subpath in deviceSubpaths {
            guard !subpath.hasPrefix(".") else { continue }
            deviceIdentifier = subpath
        }
        store = PARStore(url: url, deviceIdentifier: deviceIdentifier)
        historyViewController?.store = store
    }
    
    override var isEntireFileLoaded: Bool {
        return false
    }
    
}
