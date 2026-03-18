#pragma once

#ifdef WITH_ZSTD

#include <zstd.h>

#include "z5/compression/compressor_base.hxx"
#include "z5/metadata.hxx"


namespace z5 {
namespace compression {

    template<typename T>
    class ZstdCompressor : public CompressorBase<T> {

    public:
        ZstdCompressor(const DatasetMetadata & metadata) {
            init(metadata);
        }

        void compress(const T * dataIn, std::vector<char> & dataOut, std::size_t sizeIn) const {
            const size_t outSize = ZSTD_compressBound(sizeIn * sizeof(T));
            dataOut.resize(outSize);

            const size_t compressed = ZSTD_compress(&dataOut[0], outSize,
                                                   (const char *) dataIn,
                                                   sizeIn * sizeof(T),
                                                   clevel_);

            if(ZSTD_isError(compressed)) {
                std::string err = "Exception during zstd compression: " +
                                 std::string(ZSTD_getErrorName(compressed));
                throw std::runtime_error(err);
            }

            dataOut.resize(compressed);
        }

        void decompress(const std::vector<char> & dataIn, T * dataOut, std::size_t sizeOut) const {
            const size_t decompressed = ZSTD_decompress((char *) dataOut, sizeOut * sizeof(T),
                                                       &dataIn[0], dataIn.size());

            if(ZSTD_isError(decompressed)) {
                std::string err = "Exception during zstd decompression: " +
                                 std::string(ZSTD_getErrorName(decompressed));
                throw std::runtime_error(err);
            }
		}

        inline types::Compressor type() const {
            return types::zstd;
        }

        inline void getOptions(types::CompressionOptions & opts) const {
            opts["level"] = clevel_;
        }

    private:
        void init(const DatasetMetadata & metadata) {
            clevel_ = std::get<int>(metadata.compressionOptions.at("level"));
        }

        // compression level
        int clevel_;
    };

} // namespace compression
} // namespace z5

#endif
