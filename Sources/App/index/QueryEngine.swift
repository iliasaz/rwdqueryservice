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
        struct DecodedEventFilter: Codable {
            let attr: String
            let value: String?
            let start_yyyymm: Int?
            let end_yyyymm: Int?
            let pattern: String?
        }
        var vals: [AttrVal] = []
        var years: [AttrValYear] = []
        let jsonEncoder = JSONEncoder()
        let jsonDecoder = JSONDecoder()

        for filter in filters {
            // Re-encode the opaque value container and decode into a strongly-typed struct
            guard let data = try? jsonEncoder.encode(filter),
                  let df = try? jsonDecoder.decode(DecodedEventFilter.self, from: data) else {
                continue
            }
            guard let attrId = dict.attrToID[df.attr] else { continue }

            if let pattern = df.pattern {
                let expandedVals = dict.makeAttrVals(attr: df.attr, pattern: pattern)
                if let start = df.start_yyyymm, let end = df.end_yyyymm {
                    let expandedMonths = expandMonthRange(from: start, to: end)
                    for val in expandedVals {
                        years.append(contentsOf: makeAttrValYears(attrId: attrId, val: val.val, months: expandedMonths))
                    }
                } else {
                    vals.append(contentsOf: expandedVals)
                }
            } else if let start = df.start_yyyymm, let end = df.end_yyyymm, let v = df.value,
                      let valId = dict.valueToID[attrId]?[v] {
                let expanded = expandMonthRange(from: start, to: end)
                years.append(contentsOf: makeAttrValYears(attrId: attrId, val: valId, months: expanded))
            } else if let v = df.value, let valId = dict.valueToID[attrId]?[v] {
                vals.append(AttrVal(attr: attrId, val: valId))
            }
        }
        return (vals, years)
    }
    func queryFromPayload(payload: AppAPI.Operations.QueryPatients.Input.Body.JsonPayload, countOnly: Bool) -> AppAPI.Operations.QueryPatients.Output.Ok.Body.JsonPayload {
        // Extract attribute filters
        let attrAllOf = convertAttrFilters(payload.attributes?.allOf ?? [])
        let attrAnyOf = convertAttrFilters(payload.attributes?.anyOf ?? [])
        let attrExclude = convertAttrFilters(payload.attributes?.exclude ?? [])
        
        // Extract event filters
        let (eventAllOfVals, eventAllOfYears) = convertEventFilters(payload.events?.allOf ?? [])
        let (eventAnyOfVals, eventAnyOfYears) = convertEventFilters(payload.events?.anyOf ?? [])
        let (eventExcludeVals, eventExcludeYears) = convertEventFilters(payload.events?.exclude ?? [])
        
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
        
        return AppAPI.Operations.QueryPatients.Output.Ok.Body.JsonPayload(count: count, patients: patients)
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

        // anyOf (attributes + events)
        let ors = attrAnyOf + eventAnyOf
        for p in ors {
            guard let s = postingsValue[p], !s.isEmpty else { continue }
            acc = (acc == nil) ? s : acc!.union(s)
        }

        // allOf (attributes + events)
        let ands = attrAllOf + eventAllOf
        for p in ands {
            guard let s = postingsValue[p], !s.isEmpty else { return [] }
            acc = (acc == nil) ? s : acc!.intersect(s)
            if acc!.isEmpty { return [] }
        }

        // anyOfYears
        for p in eventAnyOfYears {
            guard let s = postingsYear[p], !s.isEmpty else { continue }
            acc = (acc == nil) ? s : acc!.union(s)
        }

        // allOfYears
        for p in eventAllOfYears {
            guard let s = postingsYear[p], !s.isEmpty else { return [] }
            acc = (acc == nil) ? s : acc!.intersect(s)
            if acc!.isEmpty { return [] }
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
