//
//  QueryEngine.swift.swift
//  rwdqueryservice
//
//  Created by Ilia Sazonov on 9/4/25.
//

import Foundation
import Logging
import AppAPI

class QueryEngine: @unchecked Sendable {
    let index = PeopleIndex()
    let dict = DictionaryEncoder()
    let store = IndexStore()
    let logger: Logger
    
    private(set) var patientCount = 0
    private(set) var factCount = 0

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
    
    struct EventFilterGroup {
        var values: [AttrVal] = []
        var years: [AttrValYear] = []
        var isEmpty: Bool { values.isEmpty && years.isEmpty }
    }
    
    func convertEventFilters(_ filters: [Components.Schemas.EventFilter]) -> [EventFilterGroup] {
        var groups: [EventFilterGroup] = []
        groups.reserveCapacity(filters.count)
        
        for filter in filters {
            let attrKey = filter.attr.rawValue
            guard let attrId = dict.attrToID[attrKey] else { continue }
            var group = EventFilterGroup()
            
            // Determine code expansion
            if filter.value.contains("*") {
                let expandedVals = dict.makeAttrVals(attr: attrKey, pattern: filter.value) // returns [AttrVal]
                if let start = filter.startYYYYMM, let end = filter.endYYYYMM {
                    // OR all months across all expanded codes (as years terms)
                    let months = expandMonthRange(from: start, to: end)
                    for av in expandedVals {
                        group.years.append(contentsOf: makeAttrValYears(attrId: av.attr, val: av.val, months: months))
                    }
                } else {
                    // OR all expanded codes at timeless level
                    group.values.append(contentsOf: expandedVals)
                }
            } else {
                // Exact code
                if let valId = dict.valueToID[attrId]?[filter.value] {
                    if let start = filter.startYYYYMM, let end = filter.endYYYYMM {
                        let months = expandMonthRange(from: start, to: end)
                        group.years.append(contentsOf: makeAttrValYears(attrId: attrId, val: valId, months: months))
                    } else {
                        group.values.append(AttrVal(attr: attrId, val: valId))
                    }
                }
            }
            
            if !group.isEmpty { groups.append(group) }
        }
        
        return groups
    }
    
    
    private func unionPostings(_ postings: [Posting]) -> Posting? {
        guard var acc = postings.first else { return nil }
        for i in 1..<postings.count {
            acc = acc.union(postings[i])
        }
        return acc
    }
    
    private func intersectPostings(_ postings: [Posting]) -> Posting? {
        guard !postings.isEmpty else { return nil }
        let sorted = postings.sorted { $0.count < $1.count }
        var acc = sorted[0]
        for i in 1..<sorted.count {
            acc = acc.intersect(sorted[i])
            if acc.isEmpty { return acc }
        }
        return acc
    }
    
    private func postingFor(values: [AttrVal]) -> Posting? {
        var acc: Posting? = nil
        for v in values {
            guard let s = index.postingsValue[v], !s.isEmpty else { continue }
            let p = Posting(array: s.toArray())
            acc = (acc == nil) ? p : acc!.union(p)
        }
        return acc
    }
    
    private func postingFor(years: [AttrValYear]) -> Posting? {
        var acc: Posting? = nil
        for y in years {
            guard let s = index.postingsYear[y], !s.isEmpty else { continue }
            let p = Posting(array: s.toArray())
            acc = (acc == nil) ? p : acc!.union(p)
        }
        return acc
    }
    
    private func postingFor(group: EventFilterGroup) -> Posting? {
        let pv = postingFor(values: group.values)
        let py = postingFor(years: group.years)
        switch (pv, py) {
            case (nil, nil): return nil
            case (let p?, nil): return p
            case (nil, let p?): return p
            case (let p1?, let p2?): return p1.union(p2)
        }
    }
    
    func queryFromPayload(queryRequest: Components.Schemas.QueryRequest, countOnly: Bool, profile: Bool = false) -> Components.Schemas.QueryResults {
        // Attributes → timeless
        let attrAllOf = convertAttrFilters(queryRequest.attributes?.allOf ?? [])
        let attrAnyOf = convertAttrFilters(queryRequest.attributes?.anyOf ?? [])
        let attrExclude = convertAttrFilters(queryRequest.attributes?.exclude ?? [])
        
        // Events → grouped by filter (OR inside each group)
        let eventAllOfGroups = convertEventFilters(queryRequest.events?.allOf ?? [])
        let eventAnyOfGroups = convertEventFilters(queryRequest.events?.anyOf ?? [])
        let eventExcludeGroups = convertEventFilters(queryRequest.events?.exclude ?? [])
        
        // Build postings for attributes and events
        var acc: Posting? = nil
        
        if !attrAllOf.isEmpty {
            var andPostings: [Posting] = []
            andPostings.reserveCapacity(attrAllOf.count)
            for v in attrAllOf {
                if let s = index.postingsValue[v], !s.isEmpty {
                    andPostings.append(Posting(array: s.toArray()))
                } else {
                    return Components.Schemas.QueryResults(count: 0, patients: countOnly ? nil : [], cohortProfile: nil)
                }
            }
            if let andPosting = intersectPostings(andPostings) {
                acc = (acc == nil) ? andPosting : acc!.intersect(andPosting)
                if acc!.isEmpty {
                    return Components.Schemas.QueryResults(count: 0, patients: countOnly ? nil : [], cohortProfile: nil)
                }
            }
        }
        
        if !eventAllOfGroups.isEmpty {
            var groupPostings: [Posting] = []
            groupPostings.reserveCapacity(eventAllOfGroups.count)
            for g in eventAllOfGroups {
                if let pg = postingFor(group: g) {
                    groupPostings.append(pg)
                } else {
                    return Components.Schemas.QueryResults(count: 0, patients: countOnly ? nil : [], cohortProfile: nil)
                }
            }
            if let allEventPosting = intersectPostings(groupPostings) {
                acc = (acc == nil) ? allEventPosting : acc!.intersect(allEventPosting)
                if acc!.isEmpty {
                    return Components.Schemas.QueryResults(count: 0, patients: countOnly ? nil : [], cohortProfile: nil)
                }
            }
        }
        
        if let anyAttrPosting = postingFor(values: attrAnyOf) {
            acc = (acc == nil) ? anyAttrPosting : acc!.intersect(anyAttrPosting)
            if acc!.isEmpty {
                return Components.Schemas.QueryResults(count: 0, patients: countOnly ? nil : [], cohortProfile: nil)
            }
        }
        
        if !eventAnyOfGroups.isEmpty {
            var groupPostings: [Posting] = []
            for g in eventAnyOfGroups {
                if let pg = postingFor(group: g) {
                    groupPostings.append(pg)
                }
            }
            if let anyEventPosting = unionPostings(groupPostings) {
                acc = (acc == nil) ? anyEventPosting : acc!.intersect(anyEventPosting)
                if acc!.isEmpty {
                    return Components.Schemas.QueryResults(count: 0, patients: countOnly ? nil : [], cohortProfile: nil)
                }
            }
        }
        
        // exclude (attributes + events): OR all exclusions then subtract once
        var neg: Posting? = nil
        
        if let attrNeg = postingFor(values: attrExclude) {
            neg = attrNeg
        }
        if !eventExcludeGroups.isEmpty {
            var negGroups: [Posting] = []
            for g in eventExcludeGroups {
                if let pg = postingFor(group: g) {
                    negGroups.append(pg)
                }
            }
            if let evtNeg = unionPostings(negGroups) {
                neg = (neg == nil) ? evtNeg : neg!.union(evtNeg)
            }
        }
        
        if let neg, let acc0 = acc {
            acc = acc0.subtract(neg)
            if acc!.isEmpty {
                return Components.Schemas.QueryResults(count: 0, patients: countOnly ? nil : [], cohortProfile: nil)
            }
        }
        
        // If no positive criteria produced an accumulator, return empty set
        guard let cohort = acc else {
            return Components.Schemas.QueryResults(count: 0, patients: countOnly ? nil : [], cohortProfile: nil)
        }

        // Optional profiling
        var apiProfile: Components.Schemas.CohortProfile? = nil
        if profile {
            let includedEvents = extractIncludedEventValues(from: eventAllOfGroups, and: eventAnyOfGroups)
            let internalProfile = buildCohortProfile(cohort: cohort, includedEvents: includedEvents)
            apiProfile = makeAPICohortProfile(internalProfile)
        }

        let ids = cohort.toArray()
        let count = ids.count
        let patients = countOnly ? nil : ids.map { dict.personIndexToGuid[Int($0)] }
        return Components.Schemas.QueryResults(count: count, patients: patients, cohortProfile: apiProfile)
    }

    // Search event code values for type-ahead suggestions
    // - eventType: "conditionCode" | "medicationCode" | "procedureCode"
    // - matchMode: "prefix" (default) or "contains"
    // Returns paged values and total matching count.
    func searchEventValues(eventType: String, keyword: String, matchMode: String = "prefix", limit: Int = 20, offset: Int = 0) -> (values: [String], total: Int) {
        let key = keyword.lowercased()
        guard !key.isEmpty,
              let attrId = dict.attrToID[eventType],
              let vmap = dict.valueToID[attrId] else {
            return ([], 0)
        }

        // Compute matches with required ordering
        let allValues = Array(vmap.keys)
        let prefixMatches = allValues.filter { $0.lowercased().hasPrefix(key) }.sorted()

        let matches: [String]
        if matchMode == "contains" {
            let substrMatches = allValues.filter { val in
                let lower = val.lowercased()
                return lower.contains(key) && !lower.hasPrefix(key)
            }.sorted()
            matches = prefixMatches + substrMatches
        } else {
            matches = prefixMatches
        }

        // Paging
        let total = matches.count
        let safeLimit = max(1, min(limit, 100))
        let safeOffset = max(0, offset)
        let start = min(safeOffset, total)
        let end = min(start + safeLimit, total)
        let page = (start < end) ? Array(matches[start..<end]) : []
        return (page, total)
    }

    struct CohortProfile {
        struct KeyCount {
            let key: String
            let count: Int
        }
        struct AttributeGroup {
            let attribute: String
            var values: [KeyCount]
        }
        struct EventGroup {
            let eventType: String
            var codes: [KeyCount]
        }
        var attributes: [AttributeGroup]
        var events: [EventGroup]
    }

    private func attrName(forAttrID id: Int) -> String? {
        for (name, aid) in dict.attrToID where aid == id { return name }
        return nil
    }

    private func valueName(forAttrID id: Int, valueID: Int) -> String? {
        guard let vmap = dict.valueToID[id] else { return nil }
        for (vname, vid) in vmap where vid == valueID { return vname }
        return nil
    }

    private func extractIncludedEventValues(from groups1: [EventFilterGroup], and groups2: [EventFilterGroup]) -> [AttrVal] {
        var set = Set<AttrVal>()
        for g in groups1 {
            for av in g.values { set.insert(av) }
            for y in g.years { set.insert(AttrVal(attr: y.attr, val: y.val)) }
        }
        for g in groups2 {
            for av in g.values { set.insert(av) }
            for y in g.years { set.insert(AttrVal(attr: y.attr, val: y.val)) }
        }
        return Array(set)
    }

    // Profiles a cohort by calculating group-by counts by attributes and events
    // The key idea is to get a posting (bitmap) for a given attribute or event and intersect it with the cohort posting.
    // This gives us the posting of patients who have the given attribute or event.
    // Since the intersect operation is done at the bitwise level, it is very efficient.
    func buildCohortProfile(cohort: Posting, includedEvents: [AttrVal]) -> CohortProfile {
        // Demographics from API enum, not hardcoded
        let demographicAttrs = Components.Schemas.AttrVal.AttrPayload.allCases.map { $0.rawValue }

        // Attribute groups
        var attributeGroups: [CohortProfile.AttributeGroup] = []
        attributeGroups.reserveCapacity(demographicAttrs.count)

        for attrName in demographicAttrs {
            guard let aid = dict.attrToID[attrName],
                  let vmap = dict.valueToID[aid] else { continue }

            var counts: [CohortProfile.KeyCount] = []
            counts.reserveCapacity(vmap.count)

            for (valueStr, vid) in vmap {
                let av = AttrVal(attr: aid, val: vid)
                // get posting for this attribute value and intersect with cohort posting to get the count of a given attrubute in the cohort
                if let p = index.postingsValue[av], !p.isEmpty {
                    let c = cohort.intersect(p).count
                    if c > 0 {
                        counts.append(.init(key: valueStr, count: c))
                    }
                }
            }

            if !counts.isEmpty {
                counts.sort { $0.count > $1.count }
                attributeGroups.append(.init(attribute: attrName, values: counts))
            }
        }

        // Event groups: only codes included in the query (drop years)
        var eventGroups: [String: [CohortProfile.KeyCount]] = [:]
        let uniqueEvents = Array(Set(includedEvents))

        for av in uniqueEvents {
            // get posting for a given event and intersect with cohort posting to get the count of the given event in the cohort
            guard let p = index.postingsValue[av], !p.isEmpty else { continue }
            let count = cohort.intersect(p).count
            if count == 0 { continue }

            let eventType = attrName(forAttrID: av.attr) ?? "\(av.attr)"
            let code = valueName(forAttrID: av.attr, valueID: av.val) ?? "\(av.val)"
            eventGroups[eventType, default: []].append(.init(key: code, count: count))
        }

        var eventGroupArray: [CohortProfile.EventGroup] = []
        eventGroupArray.reserveCapacity(eventGroups.count)
        for (etype, codes) in eventGroups {
            var sorted = codes
            sorted.sort { $0.count > $1.count }
            eventGroupArray.append(.init(eventType: etype, codes: sorted))
        }
        eventGroupArray.sort { $0.eventType < $1.eventType }

        return CohortProfile(attributes: attributeGroups, events: eventGroupArray)
    }

    // Convert cohort profile from internal representation to API result
    private func makeAPICohortProfile(_ p: CohortProfile) -> Components.Schemas.CohortProfile {
        let attrGroups = p.attributes.map { ag in
            Components.Schemas.AttributeGroup(
                attribute: ag.attribute,
                values: ag.values.map { Components.Schemas.KeyCount(key: $0.key, count: $0.count) }
            )
        }
        let evtGroups = p.events.map { eg in
            Components.Schemas.EventGroup(
                eventType: eg.eventType,
                codes: eg.codes.map { Components.Schemas.KeyCount(key: $0.key, count: $0.count) }
            )
        }
        return Components.Schemas.CohortProfile(attributes: attrGroups, events: evtGroups)
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
