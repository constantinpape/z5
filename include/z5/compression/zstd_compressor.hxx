#pragma once

#ifdef WITH_ZSTD

#include <memory>
#include <zstd.h>

#include "z5/compression/compressor_base.hxx"
#include "z5/metadata.hxx"


namespace z5 {
namespace compression {

    template<typename T>
    class ZstdCompressor : public CompressorBase<T> {

    public:
        // the override of the virtual decompress hides the base class'
        // std::vector overload; re-expose the full overload set
        using CompressorBase<T>::decompress;

        ZstdCompressor(const DatasetMetadata & metadata) {
            init(metadata);
        }

        void compress(const T * dataIn, std::vector<char> & dataOut, std::size_t sizeIn) const {
            const size_t outSize = ZSTD_compressBound(sizeIn * sizeof(T));
            dataOut.resize(outSize);

            size_t compressed;
            if(checksum_) {
                // the content checksum needs the advanced API; it is verified
                // automatically by ZSTD_decompress
                std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> ctx(ZSTD_createCCtx(), &ZSTD_freeCCtx);
                if(!ctx) {
                    throw std::runtime_error("Exception during zstd compression: cannot create context");
                }
                ZSTD_CCtx_setParameter(ctx.get(), ZSTD_c_compressionLevel, clevel_);
                ZSTD_CCtx_setParameter(ctx.get(), ZSTD_c_checksumFlag, 1);
                compressed = ZSTD_compress2(ctx.get(), &dataOut[0], outSize,
                                            (const char *) dataIn, sizeIn * sizeof(T));
            } else {
                compressed = ZSTD_compress(&dataOut[0], outSize,
                                           (const char *) dataIn,
                                           sizeIn * sizeof(T),
                                           clevel_);
            }

            if(ZSTD_isError(compressed)) {
                std::string err = "Exception during zstd compression: " +
                                 std::string(ZSTD_getErrorName(compressed));
                throw std::runtime_error(err);
            }

            dataOut.resize(compressed);
        }

        void decompress(const char * dataIn, std::size_t nBytesIn, T * dataOut, std::size_t sizeOut) const {
            const size_t decompressed = ZSTD_decompress((char *) dataOut, sizeOut * sizeof(T),
                                                       dataIn, nBytesIn);

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
            // only zarr v3 has the setting; don't add it to the other formats' options
            if(hasChecksumOption_) {
                opts["checksum"] = checksum_;
            }
        }

    private:
        void init(const DatasetMetadata & metadata) {
            const auto & cOpts = metadata.compressionOptions;
            clevel_ = std::get<int>(cOpts.at("level"));
            const auto checksum = cOpts.find("checksum");
            hasChecksumOption_ = checksum != cOpts.end();
            checksum_ = hasChecksumOption_ && std::get<bool>(checksum->second);
        }

        // compression level
        int clevel_;
        // write a content checksum (zarr v3 ``checksum`` setting)
        bool checksum_;
        bool hasChecksumOption_;
    };

} // namespace compression
} // namespace z5

#endif
