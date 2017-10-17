//
//  HistoryViewController.swift
//  PARStore Viewer
//
//  Created by Charles Parnot on 10/13/17.
//  Copyright © 2017 Charles Parnot. All rights reserved.
//

import Cocoa

class HistoryViewController: NSViewController {
    
    // MARK: Properties
    
    @IBOutlet weak var tableView: NSTableView!
    
    typealias ChangeRep = (device: String, change: PARChange)
    fileprivate var changeReps: [ChangeRep] = []
    
    var store: PARStore? {
        didSet {
            refresh()
        }
    }
    
    var changeForSelectedTimestamp: PARChange? {
        let selectedRow = tableView.selectedRow
        guard selectedRow >= 0 else {
            return nil
        }
        return changeReps[selectedRow].change
    }
    
    var storeForSelectedTimestamp: PARStore {
        let selectedRow = tableView.selectedRow
        guard selectedRow >= 0 else {
            store?.loadNow()
            let snapshotStore = PARStore.inMemory()
            snapshotStore.setEntries(from: (store ?? PARStore()).allEntries())
            store?.tearDownNow()
            return snapshotStore
        }
        let snapshotStore = PARStore.inMemory()
        for change in changeReps[0...selectedRow] {
            snapshotStore.setPropertyListValue(change.change.propertyList, forKey: change.change.key)
        }
        return snapshotStore
    }
    
    
    // MARK: Life Cycle
    
    override func viewDidLoad() {
        super.viewDidLoad()
        tableView?.enclosingScrollView?.borderType = .noBorder
    }
    
    func refresh() {
        guard let store = self.store else { return }
        changeReps = HistoryViewController.changeReps(from: store)
        tableView.reloadData()
    }
    
}

extension HistoryViewController: NSTableViewDataSource, NSTableViewDelegate {
    
    func numberOfRows(in tableView: NSTableView) -> Int {
        return changeReps.count
    }
    
    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        
        guard let columnIdentifier = tableColumn?.identifier else { return nil }
        
        let change = changeReps[row]
        let stringValue: String
        
        // timestamp column
        if columnIdentifier == "Timestamp" {
            let microseconds = Double(change.change.timestamp.int64Value)
            let seconds: Double = microseconds / (1000.0 * 1000.0)
            let date = Date(timeIntervalSinceReferenceDate: seconds)
            stringValue = date.description
        }
            
            // key column
        else if columnIdentifier == "Key" {
            // layout also gets the number of paragraphs
            if change.change.key == "layout", let layout = change.change.propertyList as? [String] {
                stringValue = "layout (\(layout.count))"
            }
            else {
                stringValue = change.change.key
            }
        }
            
            // device column
        else if columnIdentifier == "Device" {
            stringValue = change.device
        }
            
            // error
        else {
            fatalError("invalid column identifier: \(columnIdentifier)")
        }
        
        // final cell view
        let cellView = tableView.make(withIdentifier: columnIdentifier, owner: nil) as? NSTableCellView
        cellView?.textField?.stringValue = stringValue
        cellView?.imageView?.image = nil
        return cellView
    }
    
    func tableViewSelectionDidChange(_ notification: Notification) {
        nextResponder?.try(toPerform: #selector(HistoryViewControllerActions.historyViewControllerSelectionDidChange(_:)), with: self)
    }
    
}

fileprivate extension HistoryViewController {
    
    fileprivate class func changeReps(from parStore: PARStore) -> [ChangeRep] {
        
        // device identifiers --> device names
        var namesFromIdentifiers: [String : String] = [:]
        if let infoDirectory = parStore.storeURL?.appendingPathComponent("blobs").appendingPathComponent("info") {
            do {
                let files = try FileManager.default.contentsOfDirectory(atPath: infoDirectory.path)
                for fileName in files {
                    let fileURL = infoDirectory.appendingPathComponent(fileName)
                    let identifier = fileURL.deletingPathExtension().lastPathComponent
                    if let content = NSDictionary(contentsOf: fileURL) as? [String: Any], let name = content["deviceName"] as? String {
                        namesFromIdentifiers[identifier] = name
                    }
                }
            } catch {
                NSLog("could not access info file for store \(String(describing: parStore.storeURL)) because of error: \(error)")
            }
        }
        
        // changes --> change reps
        let identifiers = (parStore.foreignDeviceIdentifiers + [parStore.deviceIdentifier]) as! [String]
        var changeReps: [ChangeRep] = []
        for deviceIdentifier in identifiers {
            let deviceName = namesFromIdentifiers[deviceIdentifier] ?? deviceIdentifier
            let moreStoreChanges = parStore.fetchChanges(sinceTimestamp: PARStore.timestampForDistantPast(), forDeviceIdentifier: deviceIdentifier)
            changeReps += moreStoreChanges.map { return (device: deviceName, change: $0) }
        }
        changeReps.sort { $0.change.timestamp.int64Value < $1.change.timestamp.int64Value }
        return changeReps
    }
    
}

/// Action sent up the responder chain by HistoryViewController when its selection changes.
@objc protocol HistoryViewControllerActions {
    
    @objc optional func historyViewControllerSelectionDidChange(_ sender: Any)
    
}
