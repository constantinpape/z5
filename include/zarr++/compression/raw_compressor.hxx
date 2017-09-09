#pragma once

#include "zarr++/compression/compressor_base.hxx"

namespace zarr {
namespace compression {

    // dummy compressor if no compression is activated
    template<typename T>
    class RawCompressor : public CompressorBase<T> {

    public:
        RawCompressor() {
        }

        void compress(const T * dataIn, std::vector<T> & dataOut, size_t sizeIn) const {
            // TODO FIXME don't copy data - swap pointers?
            dataOut.assign(dataIn, dataIn + sizeIn);
        }

        void decompress(const std::vector<T> & dataIn, T * dataOut, size_t) const {
            // TODO FIXME don't copy data - swap pointers?
            std::copy(dataIn.begin(), dataIn.end(), dataOut);
        }
    };

}
}
