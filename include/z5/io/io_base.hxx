#pragma once

#ifdef WITH_BOOST_FS
    #ifndef BOOST_FILESYSTEM_NO_DEPERECATED
        #define BOOST_FILESYSTEM_NO_DEPERECATED
    #endif
    #include <boost/filesystem.hpp>
    namespace fs = boost::filesystem;
#else
    #if __GCC__ > 7
        #include <filesystem>
        namespace fs = std::filesystem;
    #else
        #include <experimental/filesystem>
        namespace fs = std::experimental::filesystem;
    #endif
#endif

#include "z5/handle/handle.hxx"


namespace z5 {
namespace io {

    class ChunkIoBase {

    public:
        virtual bool read(const handle::Chunk &, std::vector<char> &) const = 0;
        virtual void write(const handle::Chunk &, const char *, const std::size_t,
                           const bool=false, const std::size_t=0) const = 0;
        virtual void getChunkShape(const handle::Chunk &, types::ShapeType &) const {}
        virtual std::size_t getChunkSize(const handle::Chunk &) const {return 0;}
        virtual void findMinimumChunk(const unsigned, const fs::path &,
                                      const std::size_t, types::ShapeType &) const = 0;
        virtual void findMaximumChunk(const unsigned, const fs::path &,
                                      types::ShapeType &) const = 0;
        // dummy implementation for no header
        virtual inline std::size_t writeHeader(const types::ShapeType &,
                                               std::vector<char> &) const {
            return 0;
        }
        // dummy implementation for zarr, for which physical and spatial chunk size agree
        // for n5 these can be different due to varlength mode
        virtual std::size_t getDiscChunkSize(const handle::Chunk & chunk,
                                             bool & isVarlen) const {
            isVarlen = false;
            if(chunk.exists()) {
                return getChunkSize(chunk);
            } else {
                return 0;
            }
        }
    };

}
}
