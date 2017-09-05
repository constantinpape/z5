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

        inline bool read(const handle::Chunk & chunk, std::vector<T> & data) {

            // if the chunk exists, we read it,
            // otherwise, we write the fill value
            if(chunk.exists()) {

                // open input stream and read the filesize
                fs::ifstream file(chunk.path(), std::ios::binary);
                file.seekg(0, std::ios::end);
                size_t fileSize = file.tellg();
                file.seekg(0, std::ios::beg);

                // resize the data vector
                size_t vectorSize = fileSize / sizeof(T);
                data.resize(vectorSize);

                // read the file
                file.read((char*) &data[0], fileSize);

                // return true, because we have read an existing chunk
                return true;

            } else {

                // return chunk-size filled with zeros and return false,
                // because we have read a non-existent chunk
                data.clear();
                data.resize(chunkSize_, fillValue_);
                return false;
            }
        }

        inline void write(const handle::Chunk & chunk, const std::vector<T> & data) {
            //std::ios_base::sync_with_stdio(false);
            fs::ofstream file(chunk.path(), std::ios::binary);
            file.write((char*) &data[0], data.size() * sizeof(T));
            file.close();
        }

    private:
        size_t chunkSize_;
        T fillValue_;
    };

}
}
