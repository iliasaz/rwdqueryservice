//
//  PeopleIndex.swift
//  rwdquery
//
//  Created by Ilia Sazonov on 8/22/25.
//

import Foundation

// Compact integer keys for faster hashing and sharding
@inline(__always) func packKey(attr: Int, val: Int) -> Int64 {
    (Int64(attr) << 32) | Int64(UInt32(val))
}

// Set an offset to keep (year - offset) small & positive
public let YEAR_OFFSET = 2000

@inline(__always) func packYearKey(attr: Int, val: Int, year: Int) -> Int64 {
    // [15b year∆][16b attr][32b val] → fits in signed 64
    let y = Int64(year - YEAR_OFFSET) & 0x7FFF
    let a = Int64(attr) & 0xFFFF
    let v = Int64(UInt32(val))
    return (y << 48) | (a << 32) | v
}

public struct AttrVal: Hashable, CustomStringConvertible {
    public let attr: Int   // attribute id
    public let val:  Int   // value id
    public var description: String { "(\(attr)=\(val))" }
}

// Time-bucketed key: (attr, value, year). You can add month/day later if needed.
public struct AttrValYear: Hashable, CustomStringConvertible {
    public let attr: Int
    public let val:  Int
    public let year: Int
    public var description: String { "(\(attr)=\(val)@\(year))" }
}



public struct PostingFactory {
    public static func make(sortedUnique ids: [PersonID], universeSize: Int) -> Posting {
        if ids.isEmpty { return Posting(array: []) }
        let density = Double(ids.count) / Double(max(universeSize, 1))
        if density >= 0.02 || ids.count >= 4096 {
            return Posting(roaringOptimized: ids)
        } else {
            return Posting(array: ids)
        }
    }
}

public final class PeopleIndex: @unchecked Sendable {
    // ===== Sharded build buckets for parallel ingest =====
    private var shardCount: Int = 0
    private var buildValueShards: [ [Int64: [PersonID]] ] = []   // per-shard: (attr,val) -> [pid]
    private var buildYearShards:  [ [Int64: [PersonID]] ] = []   // per-shard: (attr,val,year) -> [pid]
    private var shardLocks: [NSLock] = []   // one lock per shard for parallel appends
    
    // Build buckets (value-level and year-bucketed)
    private var buildBucketsValue: [AttrVal: [PersonID]] = [:]
    private var buildBucketsYear:  [AttrValYear: [PersonID]] = [:]

    // Final postings
    private(set) var postingsValue: [AttrVal: Posting] = [:]
    private(set) var postingsYear:  [AttrValYear: Posting] = [:]

    private var cardinalityValue: [AttrVal: Int] = [:]
    private var cardinalityYear:  [AttrValYear: Int] = [:]

    private var sealed = false
    private var maxPersonID: PersonID = 0
    public var universeSize: Int { Int(maxPersonID) + 1 }

    public init() {}

    // ---- Ingestion ----

    /// Add timeless categorical assignments (supports multi-valued: pass multiple AttrVal with same attr).
    public func add(person id: PersonID, traits: [AttrVal?]) {
        precondition(!sealed, "Index is sealed; cannot add.")
        if id > maxPersonID { maxPersonID = id }
        for t in traits {
            if let t {
                buildBucketsValue[t, default: []].append(id)
            }
        }
    }

    /// Add a time-stamped event: (attr=value) observed at 'date'.
    /// This contributes to the value-level posting AND to the year bucket posting.
    public func addEvent(person id: PersonID, attrVal: AttrVal, date: Date) {
        precondition(!sealed, "Index is sealed; cannot add.")
        if id > maxPersonID { maxPersonID = id }
        buildBucketsValue[attrVal, default: []].append(id)
        let yr = PeopleIndex.extractYear(from: date)
        let tk = AttrValYear(attr: attrVal.attr, val: attrVal.val, year: yr)
        buildBucketsYear[tk, default: []].append(id)
    }
    
    /// Add a time-stamped event: (attr=value) observed at 'yyyymm'.
    /// This contributes to the value-level posting AND to the year-month bucket posting.
    public func addEvent(person id: PersonID, attrVal: AttrVal, yyyymm: Int) {
        precondition(!sealed, "Index is sealed; cannot add.")
        if id > maxPersonID { maxPersonID = id }
        buildBucketsValue[attrVal, default: []].append(id)
        let tk = AttrValYear(attr: attrVal.attr, val: attrVal.val, year: yyyymm)
        buildBucketsYear[tk, default: []].append(id)
    }

    public func seal() {
        precondition(!sealed, "Already sealed.")
        // finalize value postings
        for (key, var arr) in buildBucketsValue {
            arr.sort(); dedupInPlace(&arr)
            let p = PostingFactory.make(sortedUnique: arr, universeSize: universeSize)
            postingsValue[key] = p
            cardinalityValue[key] = p.count
        }
        // finalize time postings
        for (key, var arr) in buildBucketsYear {
            arr.sort(); dedupInPlace(&arr)
            let p = PostingFactory.make(sortedUnique: arr, universeSize: universeSize)
            postingsYear[key] = p
            cardinalityYear[key] = p.count
        }
        buildBucketsValue.removeAll(keepingCapacity: false)
        buildBucketsYear.removeAll(keepingCapacity: false)
        sealed = true
    }

    // ---- Queries ----

    /// AND of value-only predicates: (A=value1) ∧ (B=value2) ∧ ...
    public func query(allOf predicates: [AttrVal]) -> [PersonID] {
        guard !predicates.isEmpty else { return [] }
        var sets: [Posting] = []
        for p in predicates {
            guard let s = postingsValue[p], !s.isEmpty else { return [] }
            sets.append(s)
        }
        sets.sort { $0.count < $1.count }
        var acc = sets[0]
        for i in 1..<sets.count {
            acc = acc.intersect(sets[i])
            if acc.isEmpty { return [] }
        }
        return acc.toArray()
    }

    /// AND of mixed value + time-bucket predicates:
    /// (allOfValues) ∧ (for each time predicate: attr=value@year)
    public func query(allOf values: [AttrVal], andInYears timePreds: [AttrValYear]) -> [PersonID] {
        var sets: [Posting] = []
        for v in values {
            guard let s = postingsValue[v], !s.isEmpty else { return [] }
            sets.append(s)
        }
        for t in timePreds {
            guard let s = postingsYear[t], !s.isEmpty else { return [] }
            sets.append(s)
        }
        if sets.isEmpty { return [] }
        sets.sort { $0.count < $1.count }
        var acc = sets[0]
        for i in 1..<sets.count {
            acc = acc.intersect(sets[i])
            if acc.isEmpty { return [] }
        }
        return acc.toArray()
    }

    /// (OR within an attribute) AND others, optionally with year-bucketed terms,
    /// and optionally exclude a set of "ever" events.
    public func query(
        anyOf ors: [AttrVal] = [],
        and values: [AttrVal] = [],
        andInYears timePreds: [AttrValYear] = [],
        minus nots: [AttrVal] = []
    ) -> [PersonID] {
        var acc: Posting? = nil
        
        // ORs
        if !ors.isEmpty {
            for p in ors {
                guard let s = postingsValue[p], !s.isEmpty else { continue }
                acc = (acc == nil) ? s : acc!.union(s)
            }
            if acc == nil { return [] }
        }
        
        // AND timeless values
        for v in values {
            guard let s = postingsValue[v], !s.isEmpty else { return [] }
            acc = (acc == nil) ? s : acc!.intersect(s)
            if acc!.isEmpty { return [] }
        }
        
        // AND time predicates
        for t in timePreds {
            guard let s = postingsYear[t], !s.isEmpty else { return [] }
            acc = (acc == nil) ? s : acc!.intersect(s)
            if acc!.isEmpty { return [] }
        }
        
        // Subtract exclusions
        if !nots.isEmpty {
            var neg: Posting? = nil
            for p in nots {
                guard let s = postingsValue[p], !s.isEmpty else { continue }
                neg = (neg == nil) ? s : neg!.union(s)
            }
            if let neg { acc = acc?.subtract(neg) }
        }
        
        return acc?.toArray() ?? []
    }
    
    public func query(
        anyInYears timePreds: [AttrValYear],
        and values: [AttrVal] = [],
        minus nots: [AttrVal] = []
    ) -> [PersonID] {
        var acc: Posting? = nil
        
        // OR all year predicates
        for t in timePreds {
            guard let s = postingsYear[t], !s.isEmpty else { continue }
            acc = (acc == nil) ? s : acc!.union(s)
        }
        guard var result = acc else { return [] }
        
        // AND timeless values
        for v in values {
            guard let s = postingsValue[v], !s.isEmpty else { return [] }
            result = result.intersect(s)
            if result.isEmpty { return [] }
        }
        
        // Subtract exclusions
        if !nots.isEmpty {
            var neg: Posting? = nil
            for p in nots {
                guard let s = postingsValue[p], !s.isEmpty else { continue }
                neg = (neg == nil) ? s : neg!.union(s)
            }
            if let neg { result = result.subtract(neg) }
        }
        
        return result.toArray()
    }

    /// (AND allOf) \ (OR of nots) — value-only version
    public func query(allOf ands: [AttrVal], minus nots: [AttrVal]) -> [PersonID] {
        var res = Posting(array: query(allOf: ands))
        if res.isEmpty || nots.isEmpty { return res.toArray() }
        var neg: Posting? = nil
        for p in nots {
            guard let s = postingsValue[p], !s.isEmpty else { continue }
            neg = (neg == nil) ? s : neg!.union(s)
        }
        if let neg { res = res.subtract(neg) }
        return res.toArray()
    }
    
    public func query(anyOf ors: [AttrVal], minus nots: [AttrVal]) -> [PersonID] {
        // First, OR together all postings for ors
        var res: Posting? = nil
        for p in ors {
            guard let s = postingsValue[p], !s.isEmpty else { continue }
            res = (res == nil) ? s : res!.union(s)
        }
        // If no ors matched, return empty
        guard var result = res else { return [] }

        // Subtract postings for nots
        if !nots.isEmpty {
            var neg: Posting? = nil
            for p in nots {
                guard let s = postingsValue[p], !s.isEmpty else { continue }
                neg = (neg == nil) ? s : neg!.union(s)
            }
            if let neg { result = result.subtract(neg) }
        }

        return result.toArray()
    }

    // ---- Utilities ----
    private static func extractYear(from date: Date) -> Int {
        Calendar(identifier: .gregorian).dateComponents(in: TimeZone(secondsFromGMT: 0)!, from: date).year!
    }

    private func dedupInPlace(_ a: inout [PersonID]) {
        if a.isEmpty { return }
        var w = 1
        for i in 1..<a.count {
            if a[i] != a[w - 1] { a[w] = a[i]; w += 1 }
        }
        if w < a.count { a.removeLast(a.count - w) }
    }
    
    /// Prepare sharded, lock-free build buckets. Use a power-of-two shard count (e.g., 32/64/128).
    public func beginIngest(shards: Int) {
        precondition(!sealed, "Already sealed.")
        precondition(shards > 0 && (shards & (shards - 1) == 0), "shards must be a power of two")
        shardCount = shards
        buildValueShards = Array(repeating: [:], count: shards)
        buildYearShards  = Array(repeating: [:], count: shards)
        shardLocks = (0..<shards).map { _ in NSLock() }
    }

    /// Append a worker's batch into the sharded buckets (no dedup here).
    /// - Parameters:
    ///   - timeless: (pid, gender, race, ethnicity) tuples
    ///   - events:   (pid, health_condition=value, year) tuples
    public func ingestShard(
        timeless: [(PersonID, AttrVal, AttrVal, AttrVal)],
        events:   [(PersonID, AttrVal, Int)]
    ) {
        precondition(shardCount > 0, "beginIngest(shards:) must be called first")

        // Create per-shard local maps to avoid write contention while packing keys
        var localValueMaps = Array(repeating: [Int64: [PersonID]](), count: shardCount)
        var localYearMaps  = Array(repeating: [Int64: [PersonID]](), count: shardCount)

        // Timeless triples: 3 postings per person
        for (pid, g, r, e) in timeless {
            if pid > maxPersonID { maxPersonID = pid }
            let k1 = packKey(attr: g.attr, val: g.val)
            let k2 = packKey(attr: r.attr, val: r.val)
            let k3 = packKey(attr: e.attr, val: e.val)
            localValueMaps[Int(truncatingIfNeeded: k1) & (shardCount - 1)][k1, default: []].append(pid)
            localValueMaps[Int(truncatingIfNeeded: k2) & (shardCount - 1)][k2, default: []].append(pid)
            localValueMaps[Int(truncatingIfNeeded: k3) & (shardCount - 1)][k3, default: []].append(pid)
        }

        // Events: add both value-level and year-bucketed postings
        for (pid, av, year) in events {
            if pid > maxPersonID { maxPersonID = pid }
            let kv = packKey(attr: av.attr, val: av.val)
            let ky = packYearKey(attr: av.attr, val: av.val, year: year)

            localValueMaps[Int(truncatingIfNeeded: kv) & (shardCount - 1)][kv, default: []].append(pid)
            localYearMaps[Int(truncatingIfNeeded: ky) & (shardCount - 1)][ky, default: []].append(pid)
        }

        // Merge locals into global shard maps (serialize per shard to minimize contention)
        for s in 0..<shardCount {
            if !localValueMaps[s].isEmpty {
                for (k, ids) in localValueMaps[s] {
                    buildValueShards[s][k, default: []].append(contentsOf: ids)
                }
            }
            if !localYearMaps[s].isEmpty {
                for (k, ids) in localYearMaps[s] {
                    buildYearShards[s][k, default: []].append(contentsOf: ids)
                }
            }
        }
    }

//    /// Sort+dedup per shard in parallel, build Postings, then publish to the final maps.
//    public func sealParallel() {
//        precondition(!sealed, "Already sealed.")
//        let shards = shardCount > 0 ? shardCount : 1
//        if shardCount == 0 {
//            // Fall back to single-thread seal() for legacy path
//            self.seal()
//            return
//        }
//
//        let queue = DispatchQueue.global(qos: .userInitiated)
//        let group = DispatchGroup()
//
//        // Per-shard outputs to avoid cross-thread writes into final dictionaries
//        var shardValueOut = Array(repeating: [(Int64, Posting)](), count: shards)
//        var shardYearOut  = Array(repeating: [(Int64, Posting)](), count: shards)
//
//        for s in 0..<shards {
//            group.enter()
//            queue.async {
//                // Value postings
//                var outV: [(Int64, Posting)] = []
//                outV.reserveCapacity(self.buildValueShards[s].count)
//                for (k, var ids) in self.buildValueShards[s] {
//                    ids.sort()
//                    // in-place dedup
//                    if !ids.isEmpty {
//                        var w = 1
//                        for i in 1..<ids.count {
//                            if ids[i] != ids[w - 1] { ids[w] = ids[i]; w += 1 }
//                        }
//                        if w < ids.count { ids.removeLast(ids.count - w) }
//                    }
//                    let post = PostingFactory.make(sortedUnique: ids, universeSize: self.universeSize)
//                    outV.append((k, post))
//                }
//                shardValueOut[s] = outV
//
//                // Year postings
//                var outY: [(Int64, Posting)] = []
//                outY.reserveCapacity(self.buildYearShards[s].count)
//                for (k, var ids) in self.buildYearShards[s] {
//                    ids.sort()
//                    if !ids.isEmpty {
//                        var w = 1
//                        for i in 1..<ids.count {
//                            if ids[i] != ids[w - 1] { ids[w] = ids[i]; w += 1 }
//                        }
//                        if w < ids.count { ids.removeLast(ids.count - w) }
//                    }
//                    let post = PostingFactory.make(sortedUnique: ids, universeSize: self.universeSize)
//                    outY.append((k, post))
//                }
//                shardYearOut[s] = outY
//
//                group.leave()
//            }
//        }
//
//        group.wait()
//
//        // Publish into your existing query-time maps (postingsValue/postingsYear/…)
//        for s in 0..<shards {
//            for (k, p) in shardValueOut[s] {
//                let attr = Int((k >> 32) & 0xFFFF_FFFF)
//                let val  = Int(UInt32(truncatingIfNeeded: k & 0xFFFF_FFFF))
//                let av = AttrVal(attr: attr, val: val)
//                postingsValue[av] = p
//                cardinalityValue[av] = p.count
//            }
//            for (k, p) in shardYearOut[s] {
//                let year = Int((k >> 48) & 0x7FFF) + YEAR_OFFSET
//                let attr = Int((k >> 32) & 0xFFFF)
//                let val  = Int(UInt32(truncatingIfNeeded: k & 0xFFFF_FFFF))
//                let avy = AttrValYear(attr: attr, val: val, year: year)
//                postingsYear[avy] = p
//                cardinalityYear[avy] = p.count
//            }
//        }
//
//        // Cleanup
//        buildValueShards.removeAll(keepingCapacity: false)
//        buildYearShards.removeAll(keepingCapacity: false)
//        shardCount = 0
//        sealed = true
//    }
    
    @inline(__always) private func shardIndex(forKey k: Int64) -> Int {
        Int(truncatingIfNeeded: k) & (shardCount - 1)
    }

    @inline(__always) public func appendValue(_ key: Int64, _ pid: PersonID) {
        let s = shardIndex(forKey: key)
        let lock = shardLocks[s]
        lock.lock()
        buildValueShards[s][key, default: []].append(pid)
        lock.unlock()
    }

    @inline(__always) public func appendYear(_ key: Int64, _ pid: PersonID) {
        let s = shardIndex(forKey: key)
        let lock = shardLocks[s]
        lock.lock()
        buildYearShards[s][key, default: []].append(pid)
        lock.unlock()
    }
}

// MARK: - Load & Introspection API for IndexStore
public extension PeopleIndex {
    func beginLoad(universeSize: Int) {
        self.sealed = false
        self.maxPersonID = PersonID(universeSize - 1)
        self.postingsValue.removeAll(keepingCapacity: true)
        self.postingsYear.removeAll(keepingCapacity: true)
        self.cardinalityValue.removeAll(keepingCapacity: true)
        self.cardinalityYear.removeAll(keepingCapacity: true)
        self.shardCount = 0
    }
    func endLoad() { self.sealed = true }

    func addValuePosting(_ av: AttrVal, posting: Posting) {
        self.postingsValue[av] = posting
        self.cardinalityValue[av] = posting.count
    }
    func addYearPosting(_ avy: AttrValYear, posting: Posting) {
        self.postingsYear[avy] = posting
        self.cardinalityYear[avy] = posting.count
    }

    func getUniverseSize() -> Int { self.universeSize }
    var valuePostingCount: Int { self.postingsValue.count }
    var yearPostingCount: Int { self.postingsYear.count }

    func enumerateValuePostings(_ body: (AttrVal, Posting) -> Void) {
        for (k,v) in self.postingsValue { body(k,v) }
    }
    func enumerateYearPostings(_ body: (AttrValYear, Posting) -> Void) {
        for (k,v) in self.postingsYear { body(k,v) }
    }
}

// MARK: - Universe hint (used by Benchmark before ingest)
public extension PeopleIndex {
    /// Sets the expected universe size (number of people). This updates the underlying maxPersonID.
    /// Call before ingest/build so metadata saved to disk reflects the correct universe.
    func setUniverseHint(_ size: Int) {
        precondition(size > 0, "Universe size must be positive")
        self.maxPersonID = PersonID(size - 1)
    }
}

