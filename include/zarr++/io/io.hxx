#pragma once

#include <fstream>
#include <ios>

#include "zarr++/handle/handle.cxx"
#include "zarr++/types/types.cxx"

namespace zarr {
namespace io {

    template<typename T>
    class ChunkIo {

    public:

        ChunkIo(const size_t chunkSize, const T fillValue)
            : chunkSize_(chunkSize), fillValue_(fillValue) {
        }

        // TODO how do we handle the bytesize ?
        inline bool read(const handle::Chunk & chunk, T * data) {

            // if the chunk exists, we read it,
            // otherwise, we write the fill value
            if(chunk.exists()) {
                // TODO read with fstream
                return true; // true, because the chunk is actually true
            } else {
                std::fill(&data, &data + chunkSize_, fillValue_);
                return false; // false, because the chunk is only fill value
            }
        }

        inline void write(const handle::Chunk & chunk, const T * data, const int byteSize) {
            std::ios_base::sync_with_stdio(false) ;
            auto file = std::fstream(chunk.path.cstr(), std::ios::out | std::ios::binary);
            file.write((char*) data, byteSize);
            file.close();
        }

    private:
        size_t chunkSize_;
        T fillValue_;
    };

}
}
