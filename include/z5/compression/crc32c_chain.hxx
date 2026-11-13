#pragma once

#include <cstring>
#include <memory>
#include <stdexcept>
#include <vector>

#include "z5/compression/compressor_base.hxx"
#include "z5/util/crc32c.hxx"

namespace z5 {
namespace compression {

    // A zarr v3 bytes-to-bytes codec chain made of one compressor (possibly raw)
    // and ``crc32c`` codecs around it:
    //
    //     [crc32c x before] -> compressor -> [crc32c x after]
    //
    // Each crc32c stage appends the CRC32C of the bytes it gets (uint32,
    // little-endian) on encode, and verifies and strips it on decode. Chains
    // with two compressors are refused by the metadata parser: decoding the
    // inner one would need its decompressed size, which isn't stored anywhere.
    //
    // Only zarr v3 datasets with a crc32c codec use this class; every other
    // dataset keeps the bare compressor (no extra copy or indirection).
    template<typename T>
    class Crc32cChain : public CompressorBase<T> {

    public:
        using CompressorBase<T>::decompress;

        // `compressor` encodes whole elements and is used when before == 0;
        // `byteCompressor` encodes the elements plus `before` checksums, which
        // are not a whole number of elements, and is required when before > 0.
        // A raw compressor must come with before == 0 (crc32c stages around a
        // raw compressor all count as `after`).
        Crc32cChain(std::unique_ptr<CompressorBase<T>> compressor,
                    std::unique_ptr<CompressorBase<char>> byteCompressor,
                    const unsigned before, const unsigned after)
            : compressor_(std::move(compressor)), byteCompressor_(std::move(byteCompressor)),
              before_(before), after_(after) {
            if(before_ > 0 && (!byteCompressor_ || byteCompressor_->isRaw())) {
                throw std::runtime_error("z5: crc32c before a raw compressor must be counted as after it");
            }
        }

        void compress(const T * dataIn, std::vector<char> & dataOut, std::size_t sizeIn) const {
            const std::size_t nBytes = sizeIn * sizeof(T);
            if(before_ == 0) {
                if(compressor_->isRaw()) {
                    dataOut.resize(nBytes);
                    std::memcpy(dataOut.data(), dataIn, nBytes);
                } else {
                    compressor_->compress(dataIn, dataOut, sizeIn);
                }
            } else {
                std::vector<char> tmp(nBytes);
                std::memcpy(tmp.data(), dataIn, nBytes);
                for(unsigned i = 0; i < before_; ++i) {
                    appendChecksum(tmp);
                }
                byteCompressor_->compress(tmp.data(), dataOut, tmp.size());
            }
            for(unsigned i = 0; i < after_; ++i) {
                appendChecksum(dataOut);
            }
        }

        void decompress(const char * dataIn, std::size_t nBytesIn, T * dataOut, std::size_t sizeOut) const {
            // the outer checksums are at the end: verifying them only shortens the input
            for(unsigned i = 0; i < after_; ++i) {
                nBytesIn = stripChecksum(dataIn, nBytesIn);
            }
            const std::size_t nBytesOut = sizeOut * sizeof(T);
            if(before_ == 0) {
                if(compressor_->isRaw()) {
                    // same bound as the bare raw path in util::decompress
                    if(nBytesIn > nBytesOut) {
                        throw std::runtime_error("z5: raw chunk is larger than the expected chunk size");
                    }
                    std::memcpy(dataOut, dataIn, nBytesIn);
                } else {
                    compressor_->decompress(dataIn, nBytesIn, dataOut, sizeOut);
                }
                return;
            }
            std::vector<char> tmp(nBytesOut + 4 * before_);
            byteCompressor_->decompress(dataIn, nBytesIn, tmp.data(), tmp.size());
            std::size_t n = tmp.size();
            for(unsigned i = 0; i < before_; ++i) {
                n = stripChecksum(tmp.data(), n);
            }
            std::memcpy(dataOut, tmp.data(), nBytesOut);
        }

        // report the wrapped compressor, so the dataset's compression API is unchanged
        inline types::Compressor type() const {return compressor_->type();}
        inline void getOptions(types::CompressionOptions & opts) const {compressor_->getOptions(opts);}
        inline bool isRaw() const {return false;}

        inline unsigned checksumsBefore() const {return before_;}
        inline unsigned checksumsAfter() const {return after_;}

    private:
        static void appendChecksum(std::vector<char> & buffer) {
            const uint32_t crc = util::crc32c(buffer.data(), buffer.size());
            const std::size_t n = buffer.size();
            buffer.resize(n + 4);
            for(int b = 0; b < 4; ++b) {
                buffer[n + b] = static_cast<char>((crc >> (8 * b)) & 0xFFu);
            }
        }

        // verify the checksum at the end of the first `n` bytes; returns the length without it
        static std::size_t stripChecksum(const char * buffer, const std::size_t n) {
            if(n < 4) {
                throw std::runtime_error("z5: zarr v3 crc32c codec: chunk is too short for a checksum");
            }
            const unsigned char * p = reinterpret_cast<const unsigned char *>(buffer + n - 4);
            const uint32_t stored = uint32_t(p[0]) | (uint32_t(p[1]) << 8) |
                                    (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
            if(util::crc32c(buffer, n - 4) != stored) {
                throw std::runtime_error("z5: zarr v3 crc32c codec: checksum mismatch, the chunk is corrupted");
            }
            return n - 4;
        }

        std::unique_ptr<CompressorBase<T>> compressor_;
        std::unique_ptr<CompressorBase<char>> byteCompressor_;
        unsigned before_;
        unsigned after_;
    };

} // namespace compression
} // namespace z5
