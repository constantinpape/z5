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
        void compress(const T * dataIn, std::vector<T> & dataOut, size_t sizeIn, size_t sizeOut) const {

            // compress the data
            int sizeCompressed = blosc_compress_ctx(
                clevel_, shuffle_,
                sizeof(T),
                sizeIn, dataIn,
                &dataOut[0], sizeOut,
                compressor_,
                0, // blosc blocksize, 0 means automatic value
                1  // number of internal threads -> we set this to 1 for now
            );

            // check for errors
            if(sizeCompressed <= 0) {
                throw std::runtime_error("Blosc compression failed");
            }

            // resize the out data
            dataOut.resize(sizeCompressed);
        }

        template<typename T>
        void decompress(const std::vector<T> & dataIn, T * dataOut, size_t sizeOut) const {

            // decompress the data
            int sizeDecompressed = blosc_decompress_ctx(
                &dataIn[0], dataOut,
                sizeOut, 1 // number of internal threads
            );

            // check for errors
            if(sizeDecompressed <= 0) {
                throw std::runtime_error("Blosc decompression failed");
            }
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
