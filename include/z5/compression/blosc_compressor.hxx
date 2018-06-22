#pragma once

#ifdef WITH_BLOSC

#include <blosc.h>
#include "z5/compression/compressor_base.hxx"
#include "z5/metadata.hxx"

namespace z5 {
namespace compression {

    template<typename T>
    class BloscCompressor : public CompressorBase<T> {

    public:
        BloscCompressor(const DatasetMetadata & metadata) {
            init(metadata);
        }

        void compress(const T * dataIn, std::vector<char> & dataOut, std::size_t sizeIn) const {

            const std::size_t sizeOut = sizeIn * sizeof(T) + BLOSC_MAX_OVERHEAD;
            dataOut.clear();
            dataOut.resize(sizeOut);

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
            dataOut.resize(sizeCompressed + BLOSC_MAX_OVERHEAD);
        }

        void decompress(const std::vector<char> & dataIn, T * dataOut, std::size_t sizeOut) const {

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

        virtual types::Compressor type() const {
            return types::blosc;
        }

    private:
        // set the compression parameters from metadata
        void init(const DatasetMetadata & metadata) {
            clevel_     = boost::any_cast<int>(metadata.compressionOptions.at("level"));
            shuffle_    = boost::any_cast<int>(metadata.compressionOptions.at("shuffle"));
            compressor_ = boost::any_cast<std::string>(metadata.compressionOptions.at("codec"));
        }

        // the blosc compressor
        std::string compressor_;
        // compression level
        int clevel_;
        // blsoc shuffle
        int shuffle_;
    };

} // namespace compression
} // namespace z5

#endif
