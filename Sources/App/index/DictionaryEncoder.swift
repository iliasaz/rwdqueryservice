//
//  DictionaryEncoder.swift
//  rwdquery
//
//  Created by Ilia Sazonov on 9/2/25.
//

import Foundation

// Simple dictionary-encoder (unchanged)
public final class DictionaryEncoder {
    private(set) var attrToID: [String: Int] = [:]
    private(set) var valueToID: [Int: [String: Int]] = [:]
    private var nextAttrID = 0

    // === Person dictionary (external GUID <-> internal PersonID) ===
    private var personGuidToID: [String: PersonID] = [:]
    private(set) var personIndexToGuid: [String] = []
    private var nextPersonID: PersonID = 0
    
    public var personDictSize: Int { personGuidToID.keys.count }
    
    public func printValues() {
        for v in valueToID.values {
            print("\(v.keys)")
        }
    }

    public init() {}

    public func attrID(_ name: String) -> Int {
        if let id = attrToID[name] { return id }
        let id = nextAttrID; nextAttrID += 1
        attrToID[name] = id
        valueToID[id] = [:]
        return id
    }
    public func valueID(attrID: Int, value: String) -> Int {
        var vt = valueToID[attrID]!
        if let id = vt[value] { return id }
        let newID = vt.count
        vt[value] = newID
        valueToID[attrID] = vt
        return newID
    }
    public func makeAttrVal(attr: String, value: String?) -> AttrVal? {
        guard let value else { return nil }
        let aid = attrID(attr)
        let vid = valueID(attrID: aid, value: value)
        return AttrVal(attr: aid, val: vid)
    }

    // MARK: - Person dictionary API
    /// Returns the internal PersonID for a given external GUID.
    /// If the GUID hasn't been seen before, a new PersonID is allocated.
    @discardableResult
    public func personID(forExternalGUID guid: String) -> PersonID {
        if let pid = personGuidToID[guid] { return pid }
        let pid = nextPersonID
        personGuidToID[guid] = pid
        personIndexToGuid.append(guid)
        nextPersonID &+= 1
        return pid
    }

    /// Looks up the internal PersonID for a given external GUID without creating one.
    public func lookupPersonID(forExternalGUID guid: String) -> PersonID? {
        personGuidToID[guid]
    }

    /// Reverse lookup: returns the external GUID for a given internal PersonID, if present.
    public func externalGUID(forPersonID pid: PersonID) -> String? {
        let idx = Int(pid)
        guard idx >= 0 && idx < personIndexToGuid.count else { return nil }
        return personIndexToGuid[idx]
    }

    // MARK: - Snapshot Export/Import
    /// Exports a positionally-indexed snapshot of the current dictionaries.
    /// - Returns: `attrNames[aid]` gives the attribute name for attribute id `aid`.
    ///            `valueTables[aid][vid]` gives the value string for (attr id `aid`, value id `vid`).
    /// Note: this does not include persons; use exportFullSnapshot() for a bundle including persons.
    public func exportSnapshot() -> (attrNames: [String], valueTables: [[String]]) {
        // Attributes by ID
        var attrNames = Array(repeating: "", count: nextAttrID)
        for (name, id) in attrToID { attrNames[id] = name }

        // Values by (attrID, valueID)
        var valueTables: [[String]] = Array(repeating: [], count: nextAttrID)
        for aid in 0..<nextAttrID {
            let vt = valueToID[aid] ?? [:]
            var values = Array(repeating: "", count: vt.count)
            for (valStr, vid) in vt { values[vid] = valStr }
            valueTables[aid] = values
        }
        return (attrNames, valueTables)
    }

    /// Imports a snapshot, replacing all existing mappings.
    /// IDs are positional: `aid == index in attrNames`, `vid == index in valueTables[aid]`.
    public func importSnapshot(attrNames: [String], valueTables: [[String]]) {
        precondition(valueTables.count == attrNames.count, "valueTables must have one array per attribute")
        attrToID.removeAll(keepingCapacity: false)
        valueToID.removeAll(keepingCapacity: false)

        // Rebuild attribute ids
        for (aid, name) in attrNames.enumerated() {
            attrToID[name] = aid
            valueToID[aid] = [:]
        }
        nextAttrID = attrNames.count

        // Rebuild value ids per attribute
        for (aid, values) in valueTables.enumerated() {
            var vt: [String:Int] = [:]
            vt.reserveCapacity(values.count)
            for (vid, vname) in values.enumerated() {
                vt[vname] = vid
            }
            valueToID[aid] = vt
        }
    }

    // MARK: - Full snapshot (attributes, values, persons)
    /// Exports attributes, values, and persons in a single bundle for IndexStore persistence.
    /// - Returns:
    ///   - attrNames: index == attr id → attribute name
    ///   - valueTables: valueTables[aid][vid] == value string
    ///   - personGuids: index == PersonID → external GUID
    public func exportFullSnapshot() -> (attrNames: [String], valueTables: [[String]], personGuids: [String]) {
        let base = exportSnapshot()
        let persons = exportPersonsSnapshot()
        return (base.attrNames, base.valueTables, persons)
    }

    /// Imports attributes, values, and persons from a single bundle.
    public func importFullSnapshot(attrNames: [String], valueTables: [[String]], personGuids: [String]) {
        importSnapshot(attrNames: attrNames, valueTables: valueTables)
        importPersonsSnapshot(personGuids)
    }

    // MARK: - Person dictionary snapshot (export/import)
    /// Exports an array where index == PersonID and value == external GUID.
    public func exportPersonsSnapshot() -> [String] {
        personIndexToGuid
    }

    /// Imports a persons snapshot, replacing the current person dictionary.
    /// The array index is treated as PersonID; the value is the external GUID.
    public func importPersonsSnapshot(_ personGuids: [String]) {
        personGuidToID.removeAll(keepingCapacity: false)
        personIndexToGuid = personGuids
        personGuidToID.reserveCapacity(personGuids.count)
        for (i, g) in personGuids.enumerated() { personGuidToID[g] = PersonID(i) }
        nextPersonID = PersonID(personGuids.count)
    }
}
