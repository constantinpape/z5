#pragma once

#include <blosc.h>
#include "zarr++/compression/compressor_base.hxx"
#include "zarr++/metadata.hxx"

namespace zarr {

    class BloscCompressor : public CompressorBase {

    public:
        BloscCompressor(const Metadata & metadata) {
            init(metadata);
        }

        template<typename T>
        int64_t compress(const T * dataIn, T * dataOut, size_t sizeIn, size_t sizeOut) const {
            int64_t sizeCompressed = blosc_compress_ctx(
                clevel_, shuffle_,
                sizeof(T),
                sizeIn, dataIn,
                dataOut, sizeOut,
                compressor_,
                0, // blosc blocksize, 0 means automatic value
                1  // number of internal threads -> we set this to 1 for now
            );
            if(sizeCompressed <= 0) {
                throw std::runtime_error("Blosc compression failed");
            }
            return sizeCompressed;
        }

        template<typename T>
        int64_t decompress(const T * dataIn, T * dataOut, size_t sizeOut) const {
            int64_t sizeDecompressed = blosc_decompress_ctx(
                dataIn, dataOut,
                sizeOut, 1 // number of internal threads
            );
            if(sizeDecompressed <= 0) {
                throw std::runtime_error("Blosc decompression failed");
            }
            return sizeDecompressed;
        }

    private:
        void init(const Metadata & metadata) {
            // TODO set the compression parameters from metadata

        }

        // the blosc compressor
        const char * compressor_;
        // compression level
        int clevel_;
        // blsoc shuffle
        int shuffle_;
    };
}
