#pragma once

#ifdef WITH_BLOSC

#include <blosc.h>
#include "zarr++/compression/compressor_base.hxx"
#include "zarr++/metadata.hxx"

namespace zarr {
namespace compression {

    template<typename T>
    class BloscCompressor : public CompressorBase<T> {

    public:
        BloscCompressor(const ArrayMetadata & metadata) {
            init(metadata);
        }

        void compress(const T * dataIn, std::vector<T> & dataOut, size_t sizeIn) const {

            size_t sizeOut = sizeIn * sizeof(T) + BLOSC_MAX_OVERHEAD;
            dataOut.clear();
            dataOut.resize(sizeOut / sizeof(T));

            // compress the data
            int sizeCompressed = blosc_compress_ctx(
                clevel_, shuffle_,
                sizeof(T),
                sizeIn * sizeof(T), dataIn,
                &dataOut[0], sizeOut,
                compressor_.c_str(),
                0, // blosc blocksize, 0 means automatic value
                1  // number of internal threads -> we set this to 1 for now
            );

            // check for errors
            if(sizeCompressed <= 0) {
                throw std::runtime_error("Blosc compression failed");
            }

            // resize the out data
            dataOut.resize(sizeCompressed / sizeof(T) + BLOSC_MAX_OVERHEAD);
        }

        void decompress(const std::vector<T> & dataIn, T * dataOut, size_t sizeOut) const {

            // decompress the data
            int sizeDecompressed = blosc_decompress_ctx(
                &dataIn[0], dataOut,
                sizeOut * sizeof(T), 1 // number of internal threads
            );

            // check for errors
            if(sizeDecompressed <= 0) {
                throw std::runtime_error("Blosc decompression failed");
            }
        }

    private:
        // set the compression parameters from metadata
        void init(const ArrayMetadata & metadata) {
            clevel_ = metadata.compressorLevel;
            shuffle_ = metadata.compressorShuffle;
            compressor_ = metadata.codec;
        }

        // the blosc compressor
        std::string compressor_;
        // compression level
        int clevel_;
        // blsoc shuffle
        int shuffle_;
    };

} // namespace compression
} // namespace zarr

#endif
