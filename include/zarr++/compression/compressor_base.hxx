#pragma once

#include <vector>

namespace zarr {
namespace compression {

    // abstract basis class for compression
    template<typename T>
    class CompressorBase {

    public:
        //
        // API -> must be implemented by child classes
        //

        virtual void compress(const T *, std::vector<T> &, size_t) const = 0;

        virtual void decompress(const std::vector<T> &, T *, size_t) const = 0;

    };


}
} // namespace::zarr
