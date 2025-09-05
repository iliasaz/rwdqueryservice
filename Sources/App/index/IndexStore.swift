//
//  IndexStore.swift
//  rwdquery
//
//  Created by Ilia Sazonov on 8/24/25.
//

import Foundation

#if canImport(SwiftRoaring)
import SwiftRoaring
#endif

private let RWDX_MAGIC: UInt32 = 0x52574458 // "RWDX" in ASCII
private struct SectionEntry { let kind: UInt32; let offset: UInt64; let length: UInt64 }

public enum IndexStoreError: Error { case badMagic, unsupportedVersion, io, corrupt }

public final class IndexStore {
    public struct Paths {
        public let fileURL: URL
        public init(fileURL: URL) { self.fileURL = fileURL }
    }

    public init() {}

    private func createFresh(_ url: URL) throws -> URL {
        let fm = FileManager.default
        try fm.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        if fm.fileExists(atPath: url.path) { try fm.removeItem(at: url) }
        guard fm.createFile(atPath: url.path, contents: Data()) else { throw IndexStoreError.io }
        return url
    }

    private func writeContainer(headerVersion: UInt32,
                                sections: [(kind: UInt32, data: Data)],
                                to fh: FileHandle) throws {
        // Compute directory with offsets
        let headerSize = 4 /*magic*/ + 4 /*ver*/ + 4 /*flags*/ + 4 /*count*/
        let dirEntrySize = 4 + 8 + 8
        let dirSize = sections.count * dirEntrySize
        var offset = UInt64(headerSize + dirSize)
        var entries: [SectionEntry] = []
        entries.reserveCapacity(sections.count)
        for s in sections {
            entries.append(SectionEntry(kind: s.kind, offset: offset, length: UInt64(s.data.count)))
            offset += UInt64(s.data.count)
        }

        // Build header + directory
        var header = Data()
        header.appendLE(RWDX_MAGIC)
        header.appendLE(headerVersion)
        header.appendLE(UInt32(0)) // flags
        header.appendLE(UInt32(sections.count))
        for e in entries {
            header.appendLE(e.kind)
            header.appendLE(e.offset)
            header.appendLE(e.length)
        }

        // Write header then payloads
        try fh.write(contentsOf: header)
        for s in sections { try fh.write(contentsOf: s.data) }
    }

    private func mmapWhole(_ url: URL) throws -> Data {
        let fd = open(url.path, O_RDONLY)
        if fd < 0 { throw IndexStoreError.io }
        var st = stat(); if fstat(fd, &st) != 0 { close(fd); throw IndexStoreError.io }
        let size = Int(st.st_size)
        if size == 0 { close(fd); return Data() }
        let prot = PROT_READ
        #if os(macOS) || os(iOS) || os(tvOS) || os(watchOS)
        let flags = MAP_FILE | MAP_SHARED
        #else
        let flags = MAP_SHARED
        #endif
        guard let base = mmap(nil, size, prot, flags, fd, 0), base != MAP_FAILED else {
            close(fd); throw IndexStoreError.io
        }
        // `Data` will unmap via the custom deallocator
        let data = Data(bytesNoCopy: base, count: size, deallocator: .custom({ ptr, _ in
            munmap(ptr, size)
        }))
        close(fd)
        return data
    }

    private func parseHeaderAndDirectory(_ data: Data) throws -> [SectionEntry] {
        var rd = BytesReader(data)
        let magic = rd.readU32LE()
        guard magic == RWDX_MAGIC else { throw IndexStoreError.badMagic }
        let ver = rd.readU32LE()
        guard ver == 1 else { throw IndexStoreError.unsupportedVersion }
        _ = rd.readU32LE() // flags
        let count = Int(rd.readU32LE())
        var out: [SectionEntry] = []
        out.reserveCapacity(count)
        for _ in 0..<count {
            let kind = rd.readU32LE()
            let off  = rd.readU64LE()
            let len  = rd.readU64LE()
            out.append(SectionEntry(kind: kind, offset: off, length: len))
        }
        return out
    }

    // MARK: Save
    public func save(index: PeopleIndex,
                     dict: DictionaryEncoder,
                     to paths: Paths) throws
    {
        let fh = try FileHandle(forWritingTo: createFresh(paths.fileURL))
        defer { try? fh.close() }

        // 1) Build section payloads in memory (could stream to temp files if huge)
        let dictBlob = try serializeDicts(dict)
        let metaBlob = try serializeMeta(index: index)
        let (pvBlob, pvCount) = try serializePostingsValue(index)
        let (pyBlob, pyCount) = try serializePostingsYear(index)

        // 2) Write header + directory + payloads
        let sections: [(kind: UInt32, data: Data)] = [
            (1, dictBlob),
            (2, metaBlob),
            (3, pvBlob),
            (4, pyBlob),
        ]
        try writeContainer(headerVersion: 1, sections: sections, to: fh)
        print("Saved postings: value=\(pvCount), year=\(pyCount)")
    }

    // MARK: Load (fast path uses mmap)
    public func load(into index: PeopleIndex,
                     dict: DictionaryEncoder,
                     from paths: Paths) throws
    {
        let fh = try FileHandle(forReadingFrom: paths.fileURL)
        defer { try? fh.close() }

        let map = try mmapWhole(paths.fileURL)  // returns Data backed by mmap
        let dir = try parseHeaderAndDirectory(map)
        for entry in dir {
            let slice = map.subdata(in: Int(entry.offset) ..< Int(entry.offset+entry.length))
            switch entry.kind {
            case 1: try deserializeDicts(slice, into: dict)
            case 2: try deserializeMeta(slice, into: index)
            case 3: try deserializePostingsValue(slice, into: index)
            case 4: try deserializePostingsYear(slice, into: index)
            default: break
            }
        }
        index.endLoad()
    }

    // ===== Serialization helpers =====

    private func serializeDicts(_ dict: DictionaryEncoder) throws -> Data {
        // Compact: [attrCount][attrTable][valueCount][valueTable] + id maps.
        // Use a string table (length-prefixed UTF-8) + a parallel id->offset array.
        var out = Data()
        DictCodec.write(dict, into: &out)
        return out
    }

    private func deserializeDicts(_ data: Data, into dict: DictionaryEncoder) throws {
        DictCodec.read(data, into: dict)
    }

    private func serializeMeta(index: PeopleIndex) throws -> Data {
        var out = Data()
        MetaCodec.write(universeSize: index.getUniverseSize(),
                        counts: (value: index.valuePostingCount,
                                 year: index.yearPostingCount),
                        into: &out)
        return out
    }
    
    private func deserializeMeta(_ data: Data, into index: PeopleIndex) throws {
        let meta = MetaCodec.read(data)
        index.beginLoad(universeSize: meta.universe)
        // counts are informational; postings maps are filled by deserializePostings*
    }

    private func serializePostingsValue(_ index: PeopleIndex) throws -> (Data, Int) {
        var out = Data()
        var n = 0
        index.enumerateValuePostings { (av, posting) in
            n += 1
            out.appendVarUInt(UInt64(av.attr))
            out.appendVarUInt(UInt64(av.val))
            switch posting.rep {
            case .array(let ap):
                out.appendVarUInt(1)
                out.appendArrayPosting(ap.ids)
            case .roaring(let rb):
                out.appendVarUInt(2)
                out.appendRoaring(rb)
            }
        }
        return (out, n)
    }

    private func serializePostingsYear(_ index: PeopleIndex) throws -> (Data, Int) {
        var out = Data(); var n = 0
        index.enumerateYearPostings { (avy, posting) in
            n += 1
            out.appendVarUInt(UInt64(avy.attr))
            out.appendVarUInt(UInt64(avy.val))
            out.appendVarUInt(UInt64(avy.year))
            switch posting.rep {
            case .array(let ap):
                out.appendVarUInt(1)
                out.appendArrayPosting(ap.ids)
            case .roaring(let rb):
                out.appendVarUInt(2)
                out.appendRoaring(rb)
            }
        }
        return (out, n)
    }

    private func deserializePostingsValue(_ data: Data, into index: PeopleIndex) throws {
        var rd = PostingReader(data)
        while rd.vr.i < data.count {
            let attr = Int(rd.vr.readVarUInt())
            let val  = Int(rd.vr.readVarUInt())
            let codec = rd.vr.readVarUInt()
            let av = AttrVal(attr: attr, val: val)
            let posting: Posting
            if codec == 1 {
                let ids = rd.readArrayPosting()
                posting = Posting(array: ids)
            } else {
                let rb = rd.readRoaring()
                posting = Posting(roaringPosting: rb)
            }
            index.addValuePosting(av, posting: posting)
        }
    }

    private func deserializePostingsYear(_ data: Data, into index: PeopleIndex) throws {
        var rd = PostingReader(data)
        while rd.vr.i < data.count {
            let attr = Int(rd.vr.readVarUInt())
            let val  = Int(rd.vr.readVarUInt())
            let year = Int(rd.vr.readVarUInt())
            let codec = rd.vr.readVarUInt()
            let avy = AttrValYear(attr: attr, val: val, year: year)
            let posting: Posting
            if codec == 1 {
                let ids = rd.readArrayPosting()
                posting = Posting(array: ids)
            } else {
                let rb = rd.readRoaring()
                posting = Posting(roaringPosting: rb)
            }
            index.addYearPosting(avy, posting: posting)
        }
    }
}

private struct BytesReader {
    let data: Data; var i: Int = 0
    init(_ d: Data) { self.data = d }
    mutating func readU32LE() -> UInt32 { let v: UInt32 = data.readLE(at: i); i += 4; return v }
    mutating func readU64LE() -> UInt64 { let v: UInt64 = data.readLE(at: i); i += 8; return v }
}

private extension Data {
    mutating func appendLE(_ x: UInt32) {
        var v = x.littleEndian
        Swift.withUnsafeBytes(of: &v) { raw in
            append(contentsOf: raw.bindMemory(to: UInt8.self))
        }
    }

    mutating func appendLE(_ x: UInt64) {
        var v = x.littleEndian
        Swift.withUnsafeBytes(of: &v) { raw in
            append(contentsOf: raw.bindMemory(to: UInt8.self))
        }
    }

    func readLE<T>(at idx: Int) -> T where T: FixedWidthInteger {
        let sz = MemoryLayout<T>.size
        return self.subdata(in: idx..<(idx+sz)).withUnsafeBytes { ptr in
            ptr.load(as: T.self).littleEndian
        }
    }
}

// MARK: - VarUInt (LEB128 variant where last byte has MSB=1)
private extension Data {
    mutating func appendVarUInt(_ x: UInt64) {
        var v = x
        while v >= 0x80 { append(UInt8(v & 0x7F)); v >>= 7 }
        append(UInt8(v | 0x80))
    }
}

private struct VarReader {
    let data: Data; var i: Int
    init(_ d: Data, start: Int = 0) { data = d; i = start }
    mutating func readVarUInt() -> UInt64 {
        var shift: UInt64 = 0, out: UInt64 = 0
        while true {
            let b = data[i]; i += 1
            if (b & 0x80) != 0 { out |= UInt64(b & 0x7F) << shift; break }
            out |= UInt64(b) << shift; shift += 7
        }
        return out
    }
}

// MARK: - Small string helpers and compatible u32 reads
private extension Data {
    mutating func appendString(_ s: String) {
        let bytes = [UInt8](s.utf8)
        appendVarUInt(UInt64(bytes.count))
        append(contentsOf: bytes)
    }
}

private extension VarReader {
    mutating func readU32LECompat() -> UInt32 {
        // Read as 4‑byte little endian from the underlying data
        let val: UInt32 = data.readLE(at: i)
        i += 4
        return val
    }
    mutating func readString() -> String {
        let n = Int(readVarUInt())
        let start = i
        let end = start + n
        let sub = data.subdata(in: start..<end)
        i = end
        return String(decoding: sub, as: UTF8.self)
    }
}

// MARK: - Posting payload codecs
private extension Data {
    mutating func appendArrayPosting(_ ids: [UInt32]) {
        // require sorted unique ids
        appendVarUInt(UInt64(ids.count))
        var prev: UInt32 = 0
        for id in ids {
            let gap = UInt64(id &- prev)
            appendVarUInt(gap)
            prev = id
        }
    }
}

private struct PostingReader {
    var vr: VarReader
    init(_ d: Data, start: Int = 0) { vr = VarReader(d, start: start) }
    mutating func readArrayPosting() -> [UInt32] {
        let n = Int(vr.readVarUInt())
        var out: [UInt32] = []; out.reserveCapacity(n)
        var acc: UInt64 = 0
        for _ in 0..<n { acc &+= vr.readVarUInt(); out.append(UInt32(acc)) }
        return out
    }
}

#if canImport(SwiftRoaring)
// Note: We use SwiftRoaring's native serialization format here (not the portable format).
// A portable format is also available via `portableSizeInBytes` / `portableSerialize` and `portableDeserialize`,
// and we can flip to it later if cross-language compatibility is required.
private extension Data {
    mutating func appendRoaring(_ rp: RoaringPosting) {
        // Serialize via SwiftRoaring native format and length-prefix it
        let payload = rp.serializeNative()
        self.appendVarUInt(UInt64(payload.count))
        self.append(payload)
    }
}

private extension PostingReader {
    mutating func readRoaring() -> RoaringPosting {
        let byteCount = Int(vr.readVarUInt())
        let start = vr.i
        let end = start + byteCount
        let slice = vr.data.subdata(in: start..<end)
        vr.i = end
        return RoaringPosting.deserializeNative(slice)
    }
}
#else
private extension Data {
    mutating func appendRoaring(_ rp: RoaringPosting) {
        // No SwiftRoaring; should not be called in this build, but keep symmetry.
        fatalError("Roaring not available in this build")
    }
}
private extension PostingReader {
    mutating func readRoaring() -> RoaringPosting { fatalError("Roaring not available") }
}
#endif

// Expected API on DictionaryEncoder:
//   func exportFullSnapshot() -> (attrNames: [String], valueTables: [[String]], personGuids: [String])
//   func importFullSnapshot(attrNames: [String], valueTables: [[String]], personGuids: [String])
//   (Legacy) func exportSnapshot() / importSnapshot(...) without persons
// IDs are positional: attrID == index in attrNames; valueID == index in valueTables[attrID]; personID == index in personGuids.

// MARK: - Dictionary (attr/value/person) codec
// Binary layout (v2 with persons; backward compatible reader):
// [attrCount u32]
//   repeat attrCount times:
//     [attrName utf8 string]
//     [valueCount u32]
//       repeat valueCount times:
//         [valueName utf8 string]
// [personCount u32]            <-- present in v2; omitted in legacy v1
//   repeat personCount times:
//     [personGUID utf8 string]
// Attribute, value, and person IDs are implied by position (0-based).

enum DictCodec {
    static func write(_ dict: DictionaryEncoder, into out: inout Data) {
        let snap = dict.exportFullSnapshot()
        // attrs
        out.appendLE(UInt32(snap.attrNames.count))
        for (attrID, attrName) in snap.attrNames.enumerated() {
            out.appendString(attrName)
            let values = snap.valueTables[attrID]
            out.appendLE(UInt32(values.count))
            for v in values { out.appendString(v) }
        }
        // persons (v2)
        out.appendLE(UInt32(snap.personGuids.count))
        for g in snap.personGuids { out.appendString(g) }
    }

    static func read(_ data: Data, into dict: DictionaryEncoder) {
        var vr = VarReader(data)
        // attrs & values (legacy + v2)
        let attrCount = Int(vr.readU32LECompat())
        var attrs: [String] = []; attrs.reserveCapacity(attrCount)
        var valueTables: [[String]] = Array(repeating: [], count: attrCount)
        for a in 0..<attrCount {
            let attrName = vr.readString()
            attrs.append(attrName)
            let valueCount = Int(vr.readU32LECompat())
            var values: [String] = []; values.reserveCapacity(valueCount)
            for _ in 0..<valueCount { values.append(vr.readString()) }
            valueTables[a] = values
        }
        // persons (v2): present only if there are unread bytes
        var personGuids: [String] = []
        if vr.i < data.count {
            let personCount = Int(vr.readU32LECompat())
            personGuids.reserveCapacity(personCount)
            for _ in 0..<personCount { personGuids.append(vr.readString()) }
        }
        // Import using the most complete API available
        if personGuids.isEmpty {
            dict.importSnapshot(attrNames: attrs, valueTables: valueTables)
        } else {
            dict.importFullSnapshot(attrNames: attrs, valueTables: valueTables, personGuids: personGuids)
        }
    }
}

enum MetaCodec {
    static func write(universeSize: Int, counts: (value: Int, year: Int), into out: inout Data) {
        out.appendLE(UInt64(universeSize))
        out.appendLE(UInt32(counts.value))
        out.appendLE(UInt32(counts.year))
    }
    static func read(_ data: Data) -> (universe: Int, value: Int, year: Int) {
        var br = BytesReader(data)
        let uni = Int(br.readU64LE())
        let v = Int(br.readU32LE())
        let y = Int(br.readU32LE())
        return (uni, v, y)
    }
}
