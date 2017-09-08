#pragma once

#include "zarr++/handle/handle.hxx"

namespace zarr {
namespace io {

    template<typename T>
    class ChunkIoBase {

    public:

        virtual bool read(const handle::Chunk &, std::vector<T> &) const = 0;
        virtual void write(const handle::Chunk &, const std::vector<T> &) const = 0;
    };


}
}
