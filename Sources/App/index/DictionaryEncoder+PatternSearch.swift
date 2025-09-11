//
//  DictionaryEncoder+PatternSearch.swift
//  rwdquery
//
//  Created by Ilia Sazonov on 9/2/25.
//

import Foundation
import Algorithms

extension DictionaryEncoder {
    /// Expand a value pattern with optional '*' suffix into AttrVal keys.
    /// Optimized for prefix lookups (pattern ending in '*').
    func makeAttrVals(attr: String, pattern: String) -> [AttrVal] {
        guard let aid = attrToID[attr],
              let vmap = valueToID[aid] else { return [] }

        // Case 1: pattern = "*" → everything
        // we don't want this because it's too expensive to iterate over all values
        if pattern == "*" {
//            return vmap.map { (valStr, vid) in AttrVal(attr: aid, val: vid) }
            return []
        }

        // Case 2: prefix match "foo*"
        if pattern.hasSuffix("*") {
            let prefix = String(pattern.dropLast())

            // Build sorted list of values for this attribute
            let sortedValues = vmap.keys.sorted()

            // Binary search lower bound
            let lower = sortedValues.partitioningIndex { $0 >= prefix }

            // Construct an artificial "upper bound" key just after the prefix
            let nextPrefix = prefix + "\u{FFFF}"
            let upper = sortedValues.partitioningIndex { $0 >= nextPrefix }

            var results: [AttrVal] = []
            results.reserveCapacity(upper - lower)
            for i in lower..<upper {
                if let vid = vmap[sortedValues[i]] {
                    results.append(AttrVal(attr: aid, val: vid))
                }
            }
            return results
        }

        // Case 3: exact match (no wildcard)
        if let vid = vmap[pattern] {
            return [AttrVal(attr: aid, val: vid)]
        }

        return []
    }

    /// Same, but with year constraint
    func makeAttrValYears(attr: String, pattern: String, year: Int) -> [AttrValYear] {
        return makeAttrVals(attr: attr, pattern: pattern).map { av in
            AttrValYear(attr: av.attr, val: av.val, year: year)
        }
    }
    
    /// Expand an inclusive range of yyyymm values into all months.
    @inline(__always) func expandMonthRange(from start: Int, to end: Int) -> [Int] {
        precondition(start <= end, "start must be <= end")
        
        var result: [Int] = []
        
        // Parse year and month
        var year  = start / 100
        var month = start % 100
        
        let endYear  = end / 100
        let endMonth = end % 100
        
        while year < endYear || (year == endYear && month <= endMonth) {
            result.append(year * 100 + month)
            
            // Increment month
            month += 1
            if month > 12 {
                month = 1
                year += 1
            }
        }
        
        return result
    }

    /// Expand a value pattern into all AttrValYear for a given month range.
    /// Example: pattern "H91.1*" and range 202104–202106 →
    /// [ (H91.10, 202104), (H91.10, 202105), (H91.10, 202106),
    ///   (H91.11, 202104), ... ]
    func makeAttrValYears(
        attr: String,
        pattern: String,
        from start: Int,
        to end: Int
    ) -> [AttrValYear] {
        let values = makeAttrVals(attr: attr, pattern: pattern)
        let months = expandMonthRange(from: start, to: end)
        
        var results: [AttrValYear] = []
        results.reserveCapacity(values.count * months.count)
        
        for av in values {
            for m in months {
                results.append(AttrValYear(attr: av.attr, val: av.val, year: m))
            }
        }
        return results
    }
}
