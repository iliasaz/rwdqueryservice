//
//  QueryEngine.swift.swift
//  rwdqueryservice
//
//  Created by Ilia Sazonov on 9/4/25.
//

import Foundation
import Logging
import AppAPI

typealias Attribute = AppAPI.Operations.ListAttributeValues.Input.Path.AttrPayload

class QueryEngine: @unchecked Sendable {
    let index = PeopleIndex()
    let dict = DictionaryEncoder()
    let store = IndexStore()
    let logger: Logger
    
    private(set) var patientCount = 0
    private(set) var factCount = 0
    
    //    var attrValueStats = [Attribute: [String: Int]]() // atttribute -> value -> count
    
    init(logger: Logger = Logger(label: "queryengine")) {
        self.logger = logger
    }
    
    func loadIndex(from fileURL: URL) throws {
        try store.load(into: self.index, dict: self.dict, from: IndexStore.Paths(fileURL: fileURL))
        populateStats()
    }
    
    func populateStats() {
        patientCount = index.universeSize
        index.enumerateValuePostings { a, p in factCount += p.count }
        index.enumerateYearPostings { _, p in factCount += p.count }
    }
    
    // Helper function to convert attribute filters to [AttrVal]
    func convertAttrFilters(_ filters: [Components.Schemas.AttrVal]) -> [AttrVal] {
        filters.compactMap { filter in
            if let attrId = dict.attrToID[filter.attr.rawValue],
               let valId = dict.valueToID[attrId]?[filter.value] {
                return AttrVal(attr: attrId, val: valId)
            }
            return nil
        }
    }
    
    // Helper function to expand month range from start to end yyyymm (inclusive)
    func expandMonthRange(from start: Int, to end: Int) -> [Int] {
        var months: [Int] = []
        var current = start
        while current <= end {
            months.append(current)
            let year = current / 100
            let month = current % 100
            if month == 12 {
                current = (year + 1) * 100 + 1
            } else {
                current += 1
            }
        }
        return months
    }
    
    // Helper function to create [AttrValYear] from attrId, val, and months
    func makeAttrValYears(attrId: Int, val: Int, months: [Int]) -> [AttrValYear] {
        months.map { month in
            AttrValYear(attr: attrId, val: val, year: month)
        }
    }
    
    // Helper function to convert event filters to [AttrVal] and [AttrValYear]
    func convertEventFilters(_ filters: [Components.Schemas.EventFilter]) -> (vals: [AttrVal], years: [AttrValYear]) {
        var vals: [AttrVal] = []
        var years: [AttrValYear] = []
        
        for filter in filters {
            let attrKey = filter.attr.rawValue
            guard let attrId = dict.attrToID[attrKey] else { continue }
            
            if filter.value.contains("*") {
                let expandedVals = dict.makeAttrVals(attr: attrKey, pattern: filter.value)
                if let start = filter.startYyyymm, let end = filter.endYyyymm {
                    let expandedMonths = expandMonthRange(from: start, to: end)
                    for val in expandedVals {
                        years.append(contentsOf: makeAttrValYears(attrId: attrId, val: val.val, months: expandedMonths))
                    }
                } else {
                    vals.append(contentsOf: expandedVals)
                }
            } else if let start = filter.startYyyymm, let end = filter.endYyyymm,
                      let valId = dict.valueToID[attrId]?[filter.value] {
                let expanded = expandMonthRange(from: start, to: end)
                years.append(contentsOf: makeAttrValYears(attrId: attrId, val: valId, months: expanded))
            } else if let valId = dict.valueToID[attrId]?[filter.value] {
                vals.append(AttrVal(attr: attrId, val: valId))
            }
        }
        return (vals, years)
    }
    
    func queryFromPayload(queryRequest: Components.Schemas.QueryRequest, countOnly: Bool) -> Components.Schemas.QueryResults {
        // Extract attribute filters
        let attrAllOf = convertAttrFilters(queryRequest.attributes?.allOf ?? [])
        let attrAnyOf = convertAttrFilters(queryRequest.attributes?.anyOf ?? [])
        let attrExclude = convertAttrFilters(queryRequest.attributes?.exclude ?? [])
        
        // Extract event filters
        let (eventAllOfVals, eventAllOfYears) = convertEventFilters(queryRequest.events?.allOf ?? [])
        let (eventAnyOfVals, eventAnyOfYears) = convertEventFilters(queryRequest.events?.anyOf ?? [])
        let (eventExcludeVals, eventExcludeYears) = convertEventFilters(queryRequest.events?.exclude ?? [])
        
        // Query the index
        let result = index.query(
            attrAllOf: attrAllOf,
            attrAnyOf: attrAnyOf,
            attrExclude: attrExclude,
            eventAllOf: eventAllOfVals,
            eventAnyOf: eventAnyOfVals,
            eventExclude: eventExcludeVals,
            eventAllOfYears: eventAllOfYears,
            eventAnyOfYears: eventAnyOfYears,
            eventExcludeYears: eventExcludeYears
        )
        
        let count = result.count
        let patients = countOnly ? nil : Array(result).map { dict.personIndexToGuid[Int($0)] }
        
        return Components.Schemas.QueryResults(count: count, patients: patients)
    }
}

extension PeopleIndex {
    /// Full-featured query supporting attributes and events
    public func query(
        attrAllOf: [AttrVal] = [],
        attrAnyOf: [AttrVal] = [],
        attrExclude: [AttrVal] = [],
        eventAllOf: [AttrVal] = [],
        eventAnyOf: [AttrVal] = [],
        eventExclude: [AttrVal] = [],
        eventAllOfYears: [AttrValYear] = [],
        eventAnyOfYears: [AttrValYear] = [],
        eventExcludeYears: [AttrValYear] = []
    ) -> [PersonID] {
        var acc: Posting? = nil
        
        // allOf (attributes + events)
        let ands = attrAllOf + eventAllOf
        for p in ands {
            guard let s = postingsValue[p], !s.isEmpty else { return [] }
            acc = (acc == nil) ? s : acc!.intersect(s)
            if acc!.isEmpty { return [] }
        }
        
        // allOfYears
        for p in eventAllOfYears {
            guard let s = postingsYear[p], !s.isEmpty else { return [] }
            acc = (acc == nil) ? s : acc!.intersect(s)
            if acc!.isEmpty { return [] }
        }
        
        // anyOf (attributes + events)
        let ors = attrAnyOf + eventAnyOf
        for p in ors {
            guard let s = postingsValue[p], !s.isEmpty else { continue }
            acc = (acc == nil) ? s : acc!.union(s)
        }
        
        // anyOfYears
        for p in eventAnyOfYears {
            guard let s = postingsYear[p], !s.isEmpty else { continue }
            acc = (acc == nil) ? s : acc!.union(s)
        }
        
        // exclude (attributes + events)
        let nots = attrExclude + eventExclude
        var neg: Posting? = nil
        for p in nots {
            guard let s = postingsValue[p], !s.isEmpty else { continue }
            neg = (neg == nil) ? s : neg!.union(s)
        }
        
        // excludeYears
        for p in eventExcludeYears {
            guard let s = postingsYear[p], !s.isEmpty else { continue }
            neg = (neg == nil) ? s : neg!.union(s)
        }
        
        if let neg { acc = acc?.subtract(neg) }
        return acc?.toArray() ?? []
    }
}
