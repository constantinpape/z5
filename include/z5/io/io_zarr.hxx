#pragma once

#include <ios>
#include "z5/io/io_base.hxx"

namespace z5 {
namespace io {

    class ChunkIoZarr : public ChunkIoBase {

    public:

        ChunkIoZarr() {
            // disable sync of c++ and c streams for potentially faster I/O
            std::ios_base::sync_with_stdio(false);
        }

        inline bool read(const handle::Chunk & chunk, std::vector<char> & data) const {

            // if the chunk exists, we read it
            if(chunk.exists()) {

                // open input stream and read the filesize
                #ifdef WITH_BOOST_FS
                fs::ifstream file(chunk.path(), std::ios::binary);
                #else
                std::ifstream file(chunk.path(), std::ios::binary);
                #endif
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

        // last two arguments are dummy for the varlen mode of n5
        // which is not supported in zarr
        inline void write(const handle::Chunk & chunk, const char * data,
                          const std::size_t fileSize, const bool=false,
                          const std::size_t=0) const {
            #ifdef WITH_BOOST_FS
            fs::ofstream file(chunk.path(), std::ios::binary);
            #else
            std::ofstream file(chunk.path(), std::ios::binary);
            #endif
            file.write(data, fileSize);
            file.close();
        }

        inline void findMinimumChunk(const unsigned, const fs::path &,
                                     const std::size_t, types::ShapeType & minOut) const {
            std::cout << "WARNING: findMinimumChunk not implemented for zarr, returning zeros" << std::endl;
            minOut.push_back(0);
        }

        inline void findMaximumChunk(const unsigned, const fs::path &,
                                     types::ShapeType & maxOut) const {
            std::cout << "WARNING: findMaximumChunk not implemented for zarr, returning zeros" << std::endl;
            maxOut.push_back(0);
        }

    };

}
}
