#pragma once

#include "z5/s3/handle.hxx"
#include "z5/generic/store.hxx"


namespace z5 {
namespace s3 {

    // Thin byte-IO layer over chunk / shard objects on S3. Everything
    // format-related (codec, shard index, read-modify-write) lives in the
    // generic dataset implementations in z5/generic/, which are parameterized
    // on this policy. The S3 client is a process-wide cached shared_ptr per
    // configuration, so acquiring it per call costs no round trips.
    struct ChunkStore {

        typedef handle::Dataset DatasetHandleType;
        typedef handle::Chunk ChunkHandleType;

        // varlen (n5) header reads are not supported over s3
        static constexpr bool supportsN5HeaderIO = false;
        static constexpr const char * shardedName = "z5::s3::ShardedDataset";

        // GET the object; the outcome itself tells us whether it exists
        // (no separate exists() round trip, no TOCTOU window)
        static inline bool read(const ChunkHandleType & chunk, std::vector<char> & buffer,
                                const char * = "chunk") {
            auto client = chunk.makeClient();
            // getObject reads the raw bytes binary-safe (sized by Content-Length)
            return detail::getObject(*client, chunk.bucketName(), chunk.nameInBucket(), buffer);
        }

        static inline void write(const ChunkHandleType & chunk, const std::vector<char> & buffer,
                                 const char * = "chunk") {
            auto client = chunk.makeClient();
            detail::putObject(*client, chunk.bucketName(), chunk.nameInBucket(),
                              buffer.data(), buffer.size());
        }

        static inline void erase(const ChunkHandleType & chunk) {
            auto client = chunk.makeClient();
            detail::deleteObject(*client, chunk.bucketName(), chunk.nameInBucket());
        }

        // dummy path impls: there are no filesystem paths on s3
        static inline const fs::path & path(const DatasetHandleType &) {
            static const fs::path p;
            return p;
        }

        static inline void chunkPath(const ChunkHandleType &, fs::path &) {}

    };

    static_assert(z5::generic::ChunkStorePolicy<ChunkStore>);

}
}
