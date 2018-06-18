#pragma once

#ifdef WITH_LZ4

#include <zlib.h>

#include "z5/compression/compressor_base.hxx"
#include "z5/metadata.hxx"


namespace z5 {
namespace compression {

    template<typename T>
    class Lz4Compressor : public CompressorBase<T> {

    public:
        Lz4Compressor(const DatasetMetadata & metadata) {
            init(metadata);
        }

        void compress(const T * dataIn, std::vector<char> & dataOut, size_t sizeIn) const {
        }

        void decompress(const std::vector<char> & dataIn, T * dataOut, size_t sizeOut) const {
		}

        virtual types::Compressor type() const {
            return types::lz4;
        }

    private:
        void init(const DatasetMetadata & metadata) {
            level_ = boost::any_cast<int>(metadata.compressionOptions.at("level"));
        }

        // compression level
        // TODO I don't know how to set the compression level properly
        int level_;
    };

} // namespace compression
} // namespace z5

#endif
