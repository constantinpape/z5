#pragma once

#include <ios>

#ifndef BOOST_FILESYSTEM_NO_DEPERECATED
#define BOOST_FILESYSTEM_NO_DEPERECATED
#endif
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "zarr++/io/io_base.hxx"

namespace fs = boost::filesystem;

namespace zarr {
namespace io {

    template<typename T>
    class ChunkIoN5 {

    public:

        inline bool read(const handle::Chunk & chunk, std::vector<T> & data) const;
        inline void write(const handle::Chunk & chunk, const std::vector<T> & data) const;
    };


}
}
