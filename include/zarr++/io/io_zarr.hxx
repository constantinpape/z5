#pragma once

#include <ios>

#ifndef BOOST_FILESYSTEM_NO_DEPERECATED
#define BOOST_FILESYSTEM_NO_DEPERECATED
#endif
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/copy.hpp>

#include "zarr++/io/io_base.hxx"

namespace fs = boost::filesystem;
namespace ios = boost::iostreams;

namespace zarr {
namespace io {

    template<typename T, typename COMPRESSOR, typename DECOMPRESSOR>
    class ChunkIoZarr : public ChunkIoBase<T, COMPRESSOR, DECOMPRESSOR> {

    public:

        typedef COMPRESSOR Comprressor;
        typedef DECOMPRESSOR Decompressor;

        ChunkIoZarr() {
        }

        inline bool read(const handle::Chunk & chunk, std::vector<T> & data) const {

            // if the chunk exists, we read it,
            // otherwise, we write the fill value
            if(chunk.exists()) {

                // open input stream and read the filesize
                fs::ifstream file(chunk.path(), std::ios::binary);
                file.seekg(0, std::ios::end);
                size_t fileSize = file.tellg();
                file.seekg(0, std::ios::beg);

                // resize the data vector
                size_t vectorSize = fileSize / sizeof(T) + (fileSize % sizeof(T) == 0 ? 0 : sizeof(T));
                data.resize(vectorSize);

                // read the file
                //file.read((char*) &data[0], fileSize);

                // read the file with iostreams
                ios::filtering_streambuf<ios::input> in;
                in.push(Decompressor());
                in.push(file);

                ios::basic_array_sink sink{(data.begin(), data.end()};
                ios::copy(in, sink);

                // return true, because we have read an existing chunk
                return true;

            } else {
                // return false, because the chunk does not exist
                return false;
            }
        }

        inline void write(const handle::Chunk & chunk, const std::vector<T> & data) const {
            //std::ios_base::sync_with_stdio(false);

            ios::array_source source{data.begin(), data.end()};
            ios::filtering_streambuf<ios::output> out;

            out.push(source);
            out.push(Comprressor());
            
            fs::ofstream file(chunk.path(), std::ios::binary);
            ios::copy(out, file);

            file.close();
        }

    };

}
}
