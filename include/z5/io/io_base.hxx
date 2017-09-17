#pragma once

#include "z5/handle/handle.hxx"

namespace z5 {
namespace io {

    template<typename T>
    class ChunkIoBase {

    public:
        virtual bool read(const handle::Chunk &, std::vector<T> &) const = 0;
        virtual void write(const handle::Chunk &, const std::vector<T> &) const = 0;
        virtual void getChunkShape(const handle::Chunk &, types::ShapeType &) const = 0;
    };


}
}
