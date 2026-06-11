#pragma once

#ifdef WITH_LZ4

#include <lz4.h>
#include <lz4hc.h>

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

        void compress(const T * dataIn, std::vector<char> & dataOut, std::size_t sizeIn) const {
            const std::size_t bytesIn = sizeIn * sizeof(T);
            // LZ4_compressBound works on int and returns 0 for inputs that exceed
            // LZ4_MAX_INPUT_SIZE (~2GB); fail with a clear error instead
            if(bytesIn > static_cast<std::size_t>(LZ4_MAX_INPUT_SIZE)) {
                throw std::runtime_error("z5: chunk is too large for lz4 compression");
            }
            const int outSize = LZ4_compressBound(bytesIn);
            dataOut.resize(outSize);
            const int compressed = LZ4_compress_default((const char *) dataIn,
                                                        &dataOut[0],
                                                        bytesIn,
                                                        outSize);
            if(compressed <= 0) {
                std::string err = "Exception during lz4 compression: (" + std::to_string(compressed)  + ")";
    		    throw std::runtime_error(err);
            }

            dataOut.resize(compressed);
        }

        void decompress(const char * dataIn, std::size_t nBytesIn, T * dataOut, std::size_t sizeOut) const {
            const int compressed = LZ4_decompress_safe(dataIn, (char *) dataOut,
                                                       nBytesIn, sizeOut * sizeof(T));
            if(compressed <= 0) {
                std::string err = "Exception during lz4 decompression: (" + std::to_string(compressed)  + ")";
    		    throw std::runtime_error(err);
            }
		}

        inline types::Compressor type() const {
            return types::lz4;
        }

        inline void getOptions(types::CompressionOptions & opts) const {
            opts["level"] = level_;
        }

    private:
        void init(const DatasetMetadata & metadata) {
            // store the level exactly as given in the metadata: getOptions reports it
            // back, so any transformation here would corrupt metadata round trips.
            // (the level is currently not wired into LZ4_compress_default, which
            // takes no level parameter)
            level_ = std::get<int>(metadata.compressionOptions.at("level"));
        }

        // compression level
        int level_;
    };

} // namespace compression
} // namespace z5

#endif
