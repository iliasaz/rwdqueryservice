//
//  Posting.swift
//  rwdquery
//
//  Created by Ilia Sazonov on 8/22/25.
//

/*
 Overview
 ========
 This file implements the posting-list layer used by our categorical inverted index.
 It provides two concrete posting representations and a small type-erasure wrapper so
 the rest of the code can work uniformly regardless of the underlying storage:
 
   • ArrayPosting  – a compact, sorted, unique array of PersonID values (sparse-friendly)
   • RoaringPosting – a wrapper over CRoaring bitmaps for medium/dense sets (auto-fallback
                      to ArrayPosting when CRoaring is not available)
   • Posting       – a type-erased façade exposing intersect/union/subtract/toArray
 
 Why two representations?
 ------------------------
 Different value distributions favor different encodings. Very sparse sets intersect faster
 as sorted arrays (two-pointer or galloping). Medium/dense sets are better as Roaring bitmaps
 because intersections are container-wise bit operations. By abstracting behind `Posting`,
 the query engine can mix and match without changing call sites.
 
 Key operations
 --------------
 - intersect(A, B):  logical AND of two postings
 - union(A, B):      logical OR of two postings
 - subtract(A, B):   logical A \ B (useful for NOT predicates)
 
 Performance notes
 -----------------
 • We keep arrays sorted & unique. Intersections use either classic two-pointer merge
   or galloping search when sizes are highly skewed (small << large).
 • The Roaring wrapper is a reference type owning a C pointer. When CRoaring is not
   present, we transparently fall back to ArrayPosting so the project still compiles.
 
 Safety notes
 ------------
 • RoaringPosting is a class so it can properly free the underlying C bitmap in `deinit`.
 • We never `import CRoaring` inside a type; the conditional import is at the top-level.
 */

import Foundation

#if canImport(SwiftRoaring)
import SwiftRoaring
#endif

public typealias PersonID = UInt32

// MARK: - ArrayPosting

/// Posting representation backed by a sorted, unique array of `PersonID`.
///
/// Pros: excellent for very sparse sets; simple & cache-friendly intersections.
/// Cons: less compact/faster than Roaring for medium/dense ranges.
/// Invariants: `ids` is always sorted ascending with no duplicates.
public struct ArrayPosting: CustomStringConvertible {
    // Invariant: sorted, unique
    public var ids: [PersonID] = []
    public init(_ ids: [PersonID] = []) { self.ids = ids }
    public var count: Int { ids.count }
    public var isEmpty: Bool { ids.isEmpty }
    public var description: String { "ArrayPosting(\(count))" }

    @inline(__always)
    public func toArray() -> [PersonID] { ids }

    @inline(__always)
    public func intersect(_ other: ArrayPosting) -> ArrayPosting {
        // Classic two-pointer merge intersection: O(|A| + |B|) comparisons bounded by
        // the shorter list; emits only matches into `out`.
        if isEmpty || other.isEmpty { return ArrayPosting() }
        var i = 0, j = 0
        var out: [PersonID] = []
        out.reserveCapacity(min(self.ids.count, other.ids.count))
        while i < self.ids.count && j < other.ids.count {
            let va = self.ids[i], vb = other.ids[j]
            if va == vb { out.append(va); i += 1; j += 1 }
            else if va < vb { i += 1 }
            else { j += 1 }
        }
        return ArrayPosting(out)
    }

    @inline(__always)
    public func gallopIntersect(_ other: ArrayPosting) -> ArrayPosting {
        /*
         Galloping (aka exponential) search intersection: efficient when one list is
         much smaller than the other. For each element in the small list, exponentially
         advance a window in the large list, then binary search within that window.
         Amortized complexity is O(|small| log (|large|/|small|)).
         */
        // Make 'small' the left
        let (small, large) = (self.count <= other.count) ? (self, other) : (other, self)
        if small.isEmpty || large.isEmpty { return ArrayPosting() }
        let a = small.ids, b = large.ids
        var out: [PersonID] = []
        out.reserveCapacity(min(a.count, b.count))
        var lo = 0
        for x in a {
            var hi = 1
            var base = lo
            while base + hi < b.count && b[base + hi] < x {
                base += hi
                hi <<= 1
            }
            let upper = min(b.count - 1, base + hi)
            var l = base, r = upper
            var found = false
            while l <= r {
                let m = (l + r) >> 1
                let v = b[m]
                if v == x { out.append(x); lo = m + 1; found = true; break }
                if v < x { l = m + 1 } else { r = m - 1 }
            }
            if !found { lo = l }
        }
        return ArrayPosting(out)
    }

    @inline(__always)
    public func union(_ other: ArrayPosting) -> ArrayPosting {
        // Standard merge of two sorted unique arrays, preserving uniqueness.
        if isEmpty { return other }
        if other.isEmpty { return self }
        var i = 0, j = 0
        var out: [PersonID] = []
        out.reserveCapacity(self.ids.count + other.ids.count)
        while i < self.ids.count && j < other.ids.count {
            let va = self.ids[i], vb = other.ids[j]
            if va == vb { out.append(va); i += 1; j += 1 }
            else if va < vb { out.append(va); i += 1 }
            else { out.append(vb); j += 1 }
        }
        if i < self.ids.count { out.append(contentsOf: self.ids[i...]) }
        if j < other.ids.count { out.append(contentsOf: other.ids[j...]) }
        return ArrayPosting(out)
    }

    @inline(__always)
    public func subtract(_ other: ArrayPosting) -> ArrayPosting {
        // Computes A \ B by walking both lists; drops elements present in B.
        if isEmpty { return self }
        if other.isEmpty { return self }
        var i = 0, j = 0
        var out: [PersonID] = []
        out.reserveCapacity(self.ids.count)
        while i < self.ids.count && j < other.ids.count {
            let va = self.ids[i], vb = other.ids[j]
            if va == vb { i += 1; j += 1 } // drop matches
            else if va < vb { out.append(va); i += 1 }
            else { j += 1 }
        }
        if i < self.ids.count { out.append(contentsOf: self.ids[i...]) }
        return ArrayPosting(out)
    }
}

// MARK: - RoaringPosting (optional CRoaring)

/// Posting representation backed by a Roaring bitmap when CRoaring is available.
/// Falls back to an internal `ArrayPosting` implementation when CRoaring is not
/// linked, so callers can compile/run without the dependency.
public final class RoaringPosting: CustomStringConvertible {
#if canImport(SwiftRoaring)

    private var rb: RoaringBitmap

    public init() {
        self.rb = RoaringBitmap()
    }

    public convenience init(_ ids: [PersonID]) {
        self.init()
        // Bulk add by iterating; SwiftRoaring optimizes sorted inserts.
        // If `ids` is not guaranteed sorted/unique, SwiftRoaring still accepts them; we rely on upstream to canonicalize.
        rb.addMany(values: ids)
        // Optional: enable run optimization if the wrapper exposes it (no-op when not beneficial).
        optimize()
    }
    
    public func optimize() {
        _ = rb.runOptimize()
        _ = rb.shrink()
    }

    /// Number of elements in the bitmap.
    public var count: Int {
        // Prefer O(1) cardinality if exposed by SwiftRoaring; otherwise fallback to iteration.
        // SwiftRoaring exposes `cardinality` as an Int-returning property.
        return Int(rb.count)
    }

    /// True if the bitmap is empty.
    public var isEmpty: Bool { rb.isEmpty }

    public var description: String { "RoaringPosting(\(count))" }

    /// Materialize the bitmap into a sorted array of `PersonID`.
    public func toArray() -> [PersonID] {
        var out: [PersonID] = []
        out.reserveCapacity(self.count)
        for v in rb { out.append(v) }
        return out
    }

    /// Compute the intersection (logical AND) of two Roaring bitmaps.
    public func intersect(_ other: RoaringPosting) -> RoaringPosting {
        let out = self.rb & other.rb
        let p = RoaringPosting()
        p.rb = out
        return p
    }

    /// Compute the union (logical OR) of two Roaring bitmaps.
    public func union(_ other: RoaringPosting) -> RoaringPosting {
        let out = self.rb | other.rb
        let p = RoaringPosting()
        p.rb = out
        return p
    }

    /// Compute the subtraction (logical AND NOT) of two Roaring bitmaps.
    public func subtract(_ other: RoaringPosting) -> RoaringPosting {
        let out = self.rb - other.rb
        let p = RoaringPosting()
        p.rb = out
        return p
    }

    /// Serialize using SwiftRoaring's native format into a Data blob (length not included).
    /// Use `IndexStore` to length‑prefix when writing to disk.
    public func serializeNative() -> Data {
        let sz = Int(rb.sizeInBytes())
        var buf = [Int8](repeating: 0, count: sz)
        let written = rb.serialize(buffer: &buf)
        precondition(written == sz, "Roaring serialize wrote unexpected size: \(written) vs \(sz)")
        return Data(buf.map { UInt8(bitPattern: $0) })
    }

    /// Build a RoaringPosting from SwiftRoaring's native bytes.
    public static func deserializeNative(_ bytes: Data) -> RoaringPosting {
        let i8 = bytes.map { Int8(bitPattern: $0) }
        let bm = RoaringBitmap.deserialize(buffer: i8)
        let p = RoaringPosting()
        p.rb = bm
        return p
    }
#else
    // Fallback so code compiles without CRoaring; delegates to ArrayPosting
    private var underlying = ArrayPosting()

    public init() {}
    public convenience init(_ ids: [PersonID]) { self.init(); self.underlying = ArrayPosting(ids) }
    public var count: Int { underlying.count }
    public var isEmpty: Bool { underlying.isEmpty }
    public var description: String { "RoaringPosting(fallback:\(count))" }
    public func toArray() -> [PersonID] { underlying.toArray() }
    public func intersect(_ other: RoaringPosting) -> RoaringPosting { RoaringPosting(underlying.intersect(other.underlying).ids) }
    public func union(_ other: RoaringPosting) -> RoaringPosting { RoaringPosting(underlying.union(other.underlying).ids) }
    public func subtract(_ other: RoaringPosting) -> RoaringPosting { RoaringPosting(underlying.subtract(other.underlying).ids) }
#endif
}

// MARK: - Type-erased Posting

/// Type-erased façade over `ArrayPosting` and `RoaringPosting`.
///
/// Mixed-mode semantics: when operands have different backends, we conservatively
/// fall back to array-based operations by materializing the smaller side to an array.
/// This keeps correctness and simplicity; performance-sensitive paths can be tuned
/// later by adding cross-backend fast paths if needed.
public struct Posting: CustomStringConvertible {
    enum Rep {
        case array(ArrayPosting)
        case roaring(RoaringPosting)
    }
    var rep: Rep

    init(rep: Rep) { self.rep = rep }
    public init(roaringPosting rp: RoaringPosting) { self.rep = .roaring(rp) }
    public init(array ids: [PersonID]) { self.rep = .array(ArrayPosting(ids)) }
    public init(roaring ids: [PersonID]) { self.rep = .roaring(RoaringPosting(ids)) }
    
    public init(roaringOptimized ids: [PersonID]) {
    #if canImport(SwiftRoaring)
        let rp = RoaringPosting(ids)
        rp.optimize()          // calls runOptimize + shrinkToFit
        self.rep = .roaring(rp)
    #else
        self.rep = .array(ArrayPosting(ids))
    #endif
    }

    public var count: Int {
        switch rep {
        case .array(let a): return a.count
        case .roaring(let r): return r.count
        }
    }
    public var isEmpty: Bool {
        switch rep {
        case .array(let a): return a.isEmpty
        case .roaring(let r): return r.isEmpty
        }
    }
    public var description: String {
        switch rep {
        case .array(let a): return a.description
        case .roaring(let r): return r.description
        }
    }

    @inline(__always)
    public func toArray() -> [PersonID] {
        switch rep {
        case .array(let a): return a.toArray()
        case .roaring(let r): return r.toArray()
        }
    }

    @inline(__always)
    public func intersect(_ other: Posting) -> Posting {
        // Strategy:
        // 1) Same-backend fast paths (array↔array with skew-aware choice; roaring↔roaring via CRoaring).
        // 2) Mixed backends: materialize the smaller operand to an array and intersect.
        switch (rep, other.rep) {
        case (.array(let a), .array(let b)):
            // choose algorithm based on skew
            if a.count * 16 < b.count { return Posting(array: a.gallopIntersect(b).ids) }
            if b.count * 16 < a.count { return Posting(array: b.gallopIntersect(a).ids) }
            return Posting(array: a.intersect(b).ids)
        case (.roaring(let a), .roaring(let b)):
            return Posting(roaringPosting: a.intersect(b))
        default:
            // mixed → convert smaller to array and intersect
            let (small, large) = (self.count <= other.count) ? (self, other) : (other, self)
            return Posting(array: small.toArray()).intersect(Posting(array: large.toArray()))
        }
    }

    @inline(__always)
    public func union(_ other: Posting) -> Posting {
        switch (rep, other.rep) {
        case (.array(let a), .array(let b)):
            return Posting(array: a.union(b).ids)
        case (.roaring(let a), .roaring(let b)):
            return Posting(roaringPosting: a.union(b))
        default:
            // Mixed backends: fall back to arrays by materializing both, concatenating,
            // sorting, and uniquing. This is correct but may be slower; acceptable for the
            // prototype and can be optimized later.
            return Posting(array: (self.toArray() + other.toArray()).sorted().unique())
        }
    }

    @inline(__always)
    public func subtract(_ other: Posting) -> Posting {
        switch (rep, other.rep) {
        case (.array(let a), .array(let b)):
            return Posting(array: a.subtract(b).ids)
        case (.roaring(let a), .roaring(let b)):
            return Posting(roaringPosting: a.subtract(b))
        default:
            // Mixed backends: fall back to arrays by materializing both and subtracting.
            let a = self.toArray(), b = other.toArray()
            return Posting(array: ArrayPosting(a).subtract(ArrayPosting(b)).ids)
        }
    }
}

// MARK: - Helpers

/// Returns a new array with duplicates removed, assuming the input is already sorted.
extension Array where Element: Comparable {
    // In-place unique for sorted arrays
    fileprivate func unique() -> [Element] {
        if isEmpty { return [] }
        var out = self
        var w = 1
        for i in 1..<out.count {
            if out[i] != out[w - 1] {
                out[w] = out[i]
                w += 1
            }
        }
        if w < out.count { out.removeLast(out.count - w) }
        return out
    }
}
