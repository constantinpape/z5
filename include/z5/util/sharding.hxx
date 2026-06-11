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
//   [ shard index: nSlots * (offset uint64 LE, nbytes uint64 LE) ]
//   [ crc32c of the index bytes, uint32 LE ]
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

    // parse the index located at the end of the shard buffer; returns false if
    // the shard is too small, the index checksum does not match, or an entry
    // points outside of the data region (i.e. the shard is corrupt)
    inline bool parseShardIndex(const std::vector<char> & shard, std::size_t nSlots,
                                std::vector<ShardEntry> & entries) {
        const std::size_t footer = 16 * nSlots + 4;  // index + crc32c
        if(shard.size() < footer) {
            return false;
        }
        const std::size_t indexStart = shard.size() - footer;
        // validate the crc32c that buildShard stores behind the index
        const uint32_t storedCrc = readLE32(shard.data() + shard.size() - 4);
        if(crc32c(shard.data() + indexStart, 16 * nSlots) != storedCrc) {
            return false;
        }
        entries.resize(nSlots);
        for(std::size_t s = 0; s < nSlots; ++s) {
            const char * p = shard.data() + indexStart + 16 * s;
            entries[s].offset = readLE64(p);
            entries[s].nbytes = readLE64(p + 8);
            // bound non-empty entries by the data region so corrupt offsets can
            // never cause out-of-bounds reads downstream
            if(!entries[s].empty() &&
               (entries[s].offset > indexStart ||
                entries[s].nbytes > indexStart - entries[s].offset)) {
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
                           std::vector<char> & out) {
        const std::size_t nSlots = blobs.size();
        out.clear();
        std::vector<ShardEntry> entries(nSlots);

        // data region
        for(std::size_t s = 0; s < nSlots; ++s) {
            if(blobs[s].empty()) {
                entries[s] = ShardEntry{SHARD_EMPTY, SHARD_EMPTY};
            } else {
                entries[s].offset = out.size();
                entries[s].nbytes = blobs[s].size();
                out.insert(out.end(), blobs[s].begin(), blobs[s].end());
            }
        }

        // index region + crc32c over the index bytes
        std::vector<char> index;
        index.reserve(16 * nSlots);
        for(std::size_t s = 0; s < nSlots; ++s) {
            writeLE64(index, entries[s].offset);
            writeLE64(index, entries[s].nbytes);
        }
        const uint32_t crc = crc32c(index.data(), index.size());
        out.insert(out.end(), index.begin(), index.end());
        writeLE32(out, crc);
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
