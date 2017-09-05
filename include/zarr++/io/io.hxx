#pragma once

#include <ios>
#define BOOST_FILESYSTEM_NO_DEPERECATED
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "zarr++/handle/handle.hxx"
#include "zarr++/types/types.hxx"

namespace fs = boost::filesystem;

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

                // open input stream and read the filesize
                fs::ifstream file(chunk.path(), std::ios::binary);
                file.seekg(0, std::ios::end);
                size_t fileSize = file.tellg();
                file.seekg(0, std::ios::beg);

                // read the file
                file.read((char*) data, fileSize);

                // return true, because we have read an existing chunk
                return true;

            } else {

                // return chunk-size filled with zeros and return false,
                // because we have read a non-existent chunk
                std::fill(data, data + chunkSize_, fillValue_);
                return false;
            }
        }

        inline void write(const handle::Chunk & chunk, const T * data, const int byteSize) {
            //std::ios_base::sync_with_stdio(false);
            fs::ofstream file(chunk.path(), std::ios::binary);
            file.write((char*) data, byteSize);
            file.close();
        }

    private:
        size_t chunkSize_;
        T fillValue_;
    };

}
}
