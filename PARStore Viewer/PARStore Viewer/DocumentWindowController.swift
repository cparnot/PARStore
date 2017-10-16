//
//  DocumentWindowController.swift
//  PARStore Viewer
//
//  Created by Charles Parnot on 10/13/17.
//  Copyright © 2017 Charles Parnot. All rights reserved.
//

import Cocoa

class DocumentWindowController: NSWindowController {
    
    var rootSplitViewController: NSSplitViewController {
        return contentViewController?.childViewControllers[0] as! NSSplitViewController
    }
    
    var textSplitViewController: NSSplitViewController {
        return rootSplitViewController.splitViewItems[1].viewController as! NSSplitViewController
    }
    
    var historyViewController: HistoryViewController {
        return rootSplitViewController.splitViewItems[0].viewController as! HistoryViewController
    }
    
    var textViewController1: TextViewController {
        return textSplitViewController.splitViewItems[0].viewController as! TextViewController
    }
    
    var textViewController2: TextViewController {
        return textSplitViewController.splitViewItems[1].viewController as! TextViewController
    }
    
    @IBAction func historyViewControllerSelectionDidChange(_ sender: Any) {
        refresh()
    }
    
    @IBAction func reload(_ sender: Any) {
        historyViewController.refresh()
        refresh()
    }
    
    func refresh() {
        // change and snapshot
        let change   = historyViewController.changeForSelectedTimestamp?.description ?? ""
        let snapshot = historyViewController.storeForSelectedTimestamp
        let state    = snapshot.contentDump
        // update views
        textViewController1.string = change
        textViewController2.string = state
    }
    
}
