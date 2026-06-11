#pragma once

#include <concepts>
#include <filesystem>
#include <vector>

namespace z5 {
namespace generic {

    // Compile-time interface of a backend's chunk store: the thin byte-IO layer
    // (read / write / erase one chunk or shard object) that the generic dataset
    // implementations in z5/generic/ are parameterized on. Each backend's store
    // header static_asserts this concept, so policy drift is caught at parse time
    // even in builds that never instantiate that backend's dataset templates
    // (the default CI builds the filesystem backend only).
    //
    // The 'what' parameter of read/write names the object kind ("chunk"/"shard")
    // for error messages; object-store backends may ignore it.
    template<class STORE>
    concept ChunkStorePolicy = requires(const typename STORE::ChunkHandleType & chunk,
                                        const typename STORE::DatasetHandleType & ds,
                                        std::vector<char> & buffer,
                                        std::filesystem::path & path,
                                        const char * what) {
        // byte IO; read returns false if the object is absent, erase is idempotent
        { STORE::read(chunk, buffer, what) } -> std::convertible_to<bool>;
        STORE::write(chunk, buffer, what);
        STORE::erase(chunk);
        // path API (object-store backends return an empty path / no-op)
        { STORE::path(ds) } -> std::same_as<const std::filesystem::path &>;
        STORE::chunkPath(chunk, path);
        // traits
        { STORE::supportsN5HeaderIO } -> std::convertible_to<bool>;
        { STORE::shardedName } -> std::convertible_to<const char *>;
    };

}
}
