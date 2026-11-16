#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <numeric>
#include <vector>

#include "z5/types/types.hxx"
#include "z5/util/crc32c.hxx"

// Helpers for the zarr v3 ``sharding_indexed`` codec.
//
// On-disk layout of a shard file (index_location == "end", the default):
//
//   [ inner chunk blob 0 ][ inner chunk blob 1 ] ... [ inner chunk blob k ]
//   [ shard index: nSlots * (offset uint64, nbytes uint64) ]
//   [ crc32c of the index bytes, uint32 LE ]
//
// With index_location == "start" the index (+ checksum) comes first and the
// blobs follow it. Offsets are always counted from the start of the shard.
// The index integers are little-endian unless the index ``bytes`` codec says
// "big"; the crc32c is present only if the index codecs include ``crc32c``.
//
// The index has one entry per inner-chunk slot of the shard, in C order over
// the chunks-per-shard grid; ``nSlots`` is always the full product (out-of-array
// / never-written slots are stored as "empty" = both fields all-ones). The inner
// chunk blobs are exactly what a non-sharded chunk file would contain (the
// output of the inner codec pipeline), so the dataset's existing compressor can
// produce / consume them unchanged.

namespace z5 {
namespace util {

    constexpr uint64_t SHARD_EMPTY = std::numeric_limits<uint64_t>::max();

    struct ShardEntry {
        uint64_t offset = SHARD_EMPTY;
        uint64_t nbytes = SHARD_EMPTY;
        inline bool empty() const {
            return offset == SHARD_EMPTY || nbytes == SHARD_EMPTY;
        }
    };

    // chunks-per-shard along each dimension (shardShape / innerChunkShape)
    inline types::ShapeType chunksPerShard(const types::ShapeType & shardShape,
                                           const types::ShapeType & innerShape) {
        types::ShapeType cps(shardShape.size());
        for(std::size_t d = 0; d < shardShape.size(); ++d) {
            cps[d] = shardShape[d] / innerShape[d];
        }
        return cps;
    }

    inline std::size_t numShardSlots(const types::ShapeType & cps) {
        return std::accumulate(cps.begin(), cps.end(),
                               std::size_t(1), std::multiplies<std::size_t>());
    }

    // outer shard id of an inner chunk id
    inline types::ShapeType shardId(const types::ShapeType & innerId,
                                    const types::ShapeType & cps) {
        types::ShapeType sid(innerId.size());
        for(std::size_t d = 0; d < innerId.size(); ++d) {
            sid[d] = innerId[d] / cps[d];
        }
        return sid;
    }

    // C-order ravel of the inner chunk's position within its shard
    inline std::size_t shardSlot(const types::ShapeType & innerId,
                                 const types::ShapeType & cps) {
        std::size_t slot = 0;
        for(std::size_t d = 0; d < innerId.size(); ++d) {
            slot = slot * cps[d] + (innerId[d] % cps[d]);
        }
        return slot;
    }

    // how the shard index is stored (the sharding codec's index_location and
    // index_codecs); the default is what z5 writes for new arrays
    struct ShardIndexLayout {
        bool atStart = false;     // index_location "start"
        bool bigEndian = false;   // index bytes codec endian "big"
        bool checksum = true;     // index codecs end with crc32c

        // bytes taken by the index and its checksum
        inline std::size_t size(const std::size_t nSlots) const {
            return 16 * nSlots + (checksum ? 4 : 0);
        }
    };

    // little-endian (de)serialization
    inline uint64_t readLE64(const char * p) {
        uint64_t v = 0;
        for(int i = 0; i < 8; ++i) {
            v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
        }
        return v;
    }
    inline uint32_t readLE32(const char * p) {
        uint32_t v = 0;
        for(int i = 0; i < 4; ++i) {
            v |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
        }
        return v;
    }
    inline void writeLE64(std::vector<char> & out, uint64_t v) {
        for(int i = 0; i < 8; ++i) {
            out.push_back(static_cast<char>((v >> (8 * i)) & 0xFFu));
        }
    }
    inline void writeLE32(std::vector<char> & out, uint32_t v) {
        for(int i = 0; i < 4; ++i) {
            out.push_back(static_cast<char>((v >> (8 * i)) & 0xFFu));
        }
    }
    inline uint64_t readBE64(const char * p) {
        uint64_t v = 0;
        for(int i = 0; i < 8; ++i) {
            v = (v << 8) | static_cast<uint64_t>(static_cast<unsigned char>(p[i]));
        }
        return v;
    }
    inline void writeBE64(std::vector<char> & out, uint64_t v) {
        for(int i = 7; i >= 0; --i) {
            out.push_back(static_cast<char>((v >> (8 * i)) & 0xFFu));
        }
    }

    // parse the shard index; returns false if the shard is too small, the
    // index checksum does not match, or an entry points outside of the data
    // region (i.e. the shard is corrupt)
    inline bool parseShardIndex(const std::vector<char> & shard, std::size_t nSlots,
                                std::vector<ShardEntry> & entries,
                                const ShardIndexLayout & layout=ShardIndexLayout()) {
        const std::size_t indexSize = 16 * nSlots;
        const std::size_t footer = layout.size(nSlots);
        if(shard.size() < footer) {
            return false;
        }
        // the data region is everything outside of the index + checksum
        const std::size_t indexStart = layout.atStart ? 0 : shard.size() - footer;
        const std::size_t dataBegin = layout.atStart ? footer : 0;
        const std::size_t dataEnd = layout.atStart ? shard.size() : indexStart;
        // validate the crc32c stored behind the index
        if(layout.checksum) {
            const uint32_t storedCrc = readLE32(shard.data() + indexStart + indexSize);
            if(crc32c(shard.data() + indexStart, indexSize) != storedCrc) {
                return false;
            }
        }
        const auto read64 = layout.bigEndian ? readBE64 : readLE64;
        entries.resize(nSlots);
        for(std::size_t s = 0; s < nSlots; ++s) {
            const char * p = shard.data() + indexStart + 16 * s;
            entries[s].offset = read64(p);
            entries[s].nbytes = read64(p + 8);
            // bound non-empty entries by the data region so corrupt offsets can
            // never cause out-of-bounds reads downstream
            if(!entries[s].empty() &&
               (entries[s].offset < dataBegin ||
                entries[s].offset > dataEnd ||
                entries[s].nbytes > dataEnd - entries[s].offset)) {
                return false;
            }
        }
        return true;
    }

    // extract the per-slot inner chunk blobs from a shard (empty slots -> empty vector)
    inline void extractShardBlobs(const std::vector<char> & shard,
                                  const std::vector<ShardEntry> & entries,
                                  std::vector<std::vector<char>> & blobs) {
        // clear slot-by-slot (not assign) so a caller-reused `blobs` keeps the slot
        // vectors' capacity across shards
        blobs.resize(entries.size());
        for(auto & blob : blobs) {
            blob.clear();
        }
        for(std::size_t s = 0; s < entries.size(); ++s) {
            if(!entries[s].empty()) {
                const auto off = entries[s].offset;
                const auto len = entries[s].nbytes;
                blobs[s].assign(shard.begin() + off, shard.begin() + off + len);
            }
        }
    }

    // build a shard file from per-slot inner chunk blobs (empty vector -> empty slot)
    inline void buildShard(const std::vector<std::vector<char>> & blobs,
                           std::vector<char> & out,
                           const ShardIndexLayout & layout=ShardIndexLayout()) {
        const std::size_t nSlots = blobs.size();
        out.clear();
        std::vector<ShardEntry> entries(nSlots);

        // offsets count from the start of the shard, so they skip a leading index
        std::size_t pos = layout.atStart ? layout.size(nSlots) : 0;
        std::size_t dataSize = 0;
        for(std::size_t s = 0; s < nSlots; ++s) {
            if(!blobs[s].empty()) {
                entries[s].offset = pos;
                entries[s].nbytes = blobs[s].size();
                pos += blobs[s].size();
                dataSize += blobs[s].size();
            }
        }

        // index + crc32c over the index bytes
        std::vector<char> index;
        index.reserve(layout.size(nSlots));
        const auto write64 = layout.bigEndian ? writeBE64 : writeLE64;
        for(std::size_t s = 0; s < nSlots; ++s) {
            write64(index, entries[s].offset);
            write64(index, entries[s].nbytes);
        }
        if(layout.checksum) {
            const uint32_t crc = crc32c(index.data(), index.size());
            writeLE32(index, crc);
        }

        out.reserve(index.size() + dataSize);
        if(layout.atStart) {
            out.insert(out.end(), index.begin(), index.end());
        }
        for(const auto & blob : blobs) {
            out.insert(out.end(), blob.begin(), blob.end());
        }
        if(!layout.atStart) {
            out.insert(out.end(), index.begin(), index.end());
        }
    }

    // true if every slot is empty (used to decide whether to delete the shard file)
    inline bool allSlotsEmpty(const std::vector<std::vector<char>> & blobs) {
        for(const auto & b : blobs) {
            if(!b.empty()) {
                return false;
            }
        }
        return true;
    }

} // namespace util
} // namespace z5
