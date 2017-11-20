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

    template<typename T>
    class ChunkIoZarr : public ChunkIoBase<T> {

    public:

        ChunkIoZarr() {
        }

        inline bool read(const handle::Chunk & chunk, std::vector<T> & data) const {

            // if the chunk exists, we read it
            if(chunk.exists()) {
                

                // this might speed up the I/O by decoupling C++ buffers from C buffers
                std::ios_base::sync_with_stdio(false);
                // open input stream and read the filesize
                fs::ifstream file(chunk.path(), std::ios::binary);
                file.seekg(0, std::ios::end);
                size_t fileSize = file.tellg();
                file.seekg(0, std::ios::beg);
                

                // resize the data vector
                size_t vectorSize = fileSize / sizeof(T) + (fileSize % sizeof(T) == 0 ? 0 : sizeof(T));
                data.resize(vectorSize);

                // read the file
                file.read((char*) &data[0], fileSize);
                file.close();

                // return true, because we have read an existing chunk
                return true;

            } else {
                // return false, because the chunk does not exist
                return false;
            }
        }

        inline void write(const handle::Chunk & chunk, const std::vector<T> & data) const {
            // this might speed up the I/O by decoupling C++ buffers from C buffers
            std::ios_base::sync_with_stdio(false);
            fs::ofstream file(chunk.path(), std::ios::binary);
            file.write((char*) &data[0], data.size() * sizeof(T));
            file.close();
        }

        inline void getChunkShape(const handle::Chunk &, types::ShapeType &) const {}
        inline size_t getChunkSize(const handle::Chunk &) const {}
        
        inline void findMinimumChunk(types::ShapeType & minOut, const fs::path &, const size_t) {
            std::cout << "WARNING: findMinimumChunk not implemented for zarr, returning zeros" << std::endl;
            minOut.push_back(0);
        }
        
        inline void findMaximumChunk(types::ShapeType & maxOut, const fs::path &) {
            std::cout << "WARNING: findMaximumChunk not implemented for zarr, returning zeros" << std::endl;
            maxOut.push_back(0);
        }

    };

}
}
