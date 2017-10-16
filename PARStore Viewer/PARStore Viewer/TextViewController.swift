//
//  TextViewController.swift
//  PARStore Viewer
//
//  Created by Charles Parnot on 10/13/17.
//  Copyright © 2017 Charles Parnot. All rights reserved.
//

import Cocoa

class TextViewController: NSViewController {
    
    @IBOutlet weak var textView: NSTextView?
    
    var string: String {
        get {
            return textView?.string ?? ""
        }
        set {
            textView?.string = newValue
            let font = NSFont(name: "courier", size: 11.0)!
            textView?.setFont(font, range: NSRange(location: 0, length: newValue.characters.count))
        }
    }
    
    override func viewDidLoad() {
        super.viewDidLoad()
        textView?.enclosingScrollView?.borderType = .noBorder
    }
    
}
