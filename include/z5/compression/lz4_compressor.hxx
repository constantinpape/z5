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
        // the override of the virtual decompress hides the base class'
        // std::vector overload; re-expose the full overload set
        using CompressorBase<T>::decompress;

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
            const std::size_t header = sizeHeader_ ? 4 : 0;
            const int outSize = LZ4_compressBound(bytesIn);
            dataOut.resize(header + outSize);
            if(sizeHeader_) {
                for(int b = 0; b < 4; ++b) {
                    dataOut[b] = static_cast<char>((bytesIn >> (8 * b)) & 0xFFu);
                }
            }
            // the zarr v3 (numcodecs) codec honours its acceleration; other formats
            // keep their output unchanged
            const int compressed = sizeHeader_ ?
                LZ4_compress_fast((const char *) dataIn, &dataOut[header], bytesIn, outSize, level_) :
                LZ4_compress_default((const char *) dataIn, &dataOut[0], bytesIn, outSize);
            if(compressed <= 0) {
                std::string err = "Exception during lz4 compression: (" + std::to_string(compressed)  + ")";
    		    throw std::runtime_error(err);
            }

            dataOut.resize(header + compressed);
        }

        void decompress(const char * dataIn, std::size_t nBytesIn, T * dataOut, std::size_t sizeOut) const {
            std::size_t capacity = sizeOut * sizeof(T);
            if(sizeHeader_) {
                // numcodecs framing: the uncompressed size as uint32 LE, then the block
                if(nBytesIn < 4) {
                    throw std::runtime_error("Exception during lz4 decompression: missing size header");
                }
                const unsigned char * p = reinterpret_cast<const unsigned char *>(dataIn);
                const std::size_t stored = std::size_t(p[0]) | (std::size_t(p[1]) << 8) |
                                           (std::size_t(p[2]) << 16) | (std::size_t(p[3]) << 24);
                if(stored > capacity) {
                    throw std::runtime_error("Exception during lz4 decompression: chunk is larger than expected");
                }
                capacity = stored;
                dataIn += 4;
                nBytesIn -= 4;
            }
            const int compressed = LZ4_decompress_safe(dataIn, (char *) dataOut,
                                                       nBytesIn, capacity);
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
            sizeHeader_ = metadata.isZarr && metadata.zarrFormat == 3;
        }

        // compression level; for zarr v3, the numcodecs acceleration
        int level_;
        // zarr v3 ``numcodecs.lz4``: blocks are prefixed with their uncompressed
        // size (uint32 LE). zarr v2 and n5 keep z5's bare blocks.
        bool sizeHeader_;
    };

} // namespace compression
} // namespace z5

#endif
