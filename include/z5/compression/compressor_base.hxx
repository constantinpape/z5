#pragma once

#include <vector>
#include "z5/types/types.hxx"

namespace z5 {
namespace compression {

    // abstract basis class for compression
    template<typename T>
    class CompressorBase {

    public:
        //
        // API -> must be implemented by child classes
        //

        virtual ~CompressorBase() {}
        virtual void compress(const T *, std::vector<char> &, std::size_t) const = 0;
        // primary decompress: decode a raw byte range into the typed output
        virtual void decompress(const char * dataIn, std::size_t nBytesIn,
                                T * dataOut, std::size_t sizeOut) const = 0;
        // convenience overload for a full buffer (forwards to the pointer form, so the
        // shard read path can decode straight from the shard buffer without a per-slot copy)
        inline void decompress(const std::vector<char> & dataIn, T * dataOut,
                               std::size_t sizeOut) const {
            decompress(dataIn.data(), dataIn.size(), dataOut, sizeOut);
        }
        virtual types::Compressor type() const = 0;
        virtual void getOptions(types::CompressionOptions &) const = 0;
    };


}
} // namespace::z5
