#pragma once

#include <ios>

#ifndef BOOST_FILESYSTEM_NO_DEPERECATED
#define BOOST_FILESYSTEM_NO_DEPERECATED
#endif
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "z5/io/io_base.hxx"

namespace fs = boost::filesystem;

namespace z5 {
namespace io {

    class ChunkIoZarr : public ChunkIoBase {

    public:

        ChunkIoZarr() {
        }

        inline bool read(const handle::Chunk & chunk, std::vector<char> & data) const {

            // if the chunk exists, we read it
            if(chunk.exists()) {

                // this might speed up the I/O by decoupling C++ buffers from C buffers
                std::ios_base::sync_with_stdio(false);
                // open input stream and read the filesize
                fs::ifstream file(chunk.path(), std::ios::binary);
                file.seekg(0, std::ios::end);
                const std::size_t fileSize = file.tellg();
                file.seekg(0, std::ios::beg);

                // resize the data vector
                data.resize(fileSize);

                // read the file
                file.read(&data[0], fileSize);
                file.close();

                // return true, because we have read an existing chunk
                return true;

            } else {
                // return false, because the chunk does not exist
                return false;
            }
        }

        inline void write(const handle::Chunk & chunk, const char * data, const std::size_t fileSize) const {
            // this might speed up the I/O by decoupling C++ buffers from C buffers
            std::ios_base::sync_with_stdio(false);
            fs::ofstream file(chunk.path(), std::ios::binary);
            file.write(data, fileSize);
            file.close();
        }

        inline void getChunkShape(const handle::Chunk &, types::ShapeType &) const {}
        inline std::size_t getChunkSize(const handle::Chunk &) const {}

        inline void findMinimumChunk(const unsigned, const fs::path &, const std::size_t, types::ShapeType & minOut) const {
            std::cout << "WARNING: findMinimumChunk not implemented for zarr, returning zeros" << std::endl;
            minOut.push_back(0);
        }

        inline void findMaximumChunk(const unsigned, const fs::path &, types::ShapeType & maxOut) const {
            std::cout << "WARNING: findMaximumChunk not implemented for zarr, returning zeros" << std::endl;
            maxOut.push_back(0);
        }

    };

}
}
