#pragma once

#include "zarr++/compression/compressor_base.hxx"

namespace zarr {

    // dummy compressor if we don't have any compressin activated
    class RawCompressor : public CompressorBase {

    public:
        RawCompressor() {
        }

        template<typename T>
        int compress(const T * dataIn, T * dataOut, size_t sizeIn, size_t) const {
            dataOut = dataIn;
            return sizeIn;
        }

        template<typename T>
        int decompress(const T * dataIn, T * dataOut, size_t sizeIn) const {
            dataOut = dataIn;
            return sizeIn;
        }
    };


}
