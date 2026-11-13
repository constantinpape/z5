#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace z5 {
namespace util {

    // elements of variable length (strings), which the vlen codecs encode
    template<class T>
    inline constexpr bool isVlenElement = std::is_same_v<T, std::string>;

    // The numcodecs / zarr ``vlen-utf8`` and ``vlen-bytes`` framing of a chunk of
    // variable-length items: a uint32 LE item count, then for each item a uint32
    // LE byte length followed by its bytes. Both codecs use the same framing;
    // vlen-utf8 items are UTF-8 text.

    namespace vlen_detail {
        inline uint32_t readU32(const char * p) {
            const unsigned char * u = reinterpret_cast<const unsigned char *>(p);
            return uint32_t(u[0]) | (uint32_t(u[1]) << 8) | (uint32_t(u[2]) << 16) | (uint32_t(u[3]) << 24);
        }

        inline void appendU32(std::vector<char> & out, const std::size_t value) {
            if(value > 0xFFFFFFFFull) {
                throw std::runtime_error("z5: vlen codec: count or item length exceeds 2^32 - 1");
            }
            for(int b = 0; b < 4; ++b) {
                out.push_back(static_cast<char>((value >> (8 * b)) & 0xFFu));
            }
        }
    }


    // encode `n` items
    inline void encodeVlen(const std::string * items, const std::size_t n, std::vector<char> & out) {
        std::size_t total = 4 + 4 * n;
        for(std::size_t i = 0; i < n; ++i) {
            total += items[i].size();
        }
        out.clear();
        out.reserve(total);
        vlen_detail::appendU32(out, n);
        for(std::size_t i = 0; i < n; ++i) {
            vlen_detail::appendU32(out, items[i].size());
            out.insert(out.end(), items[i].begin(), items[i].end());
        }
    }


    // decode a chunk that must hold exactly `n` items; every length is checked
    // against the buffer, so a corrupt chunk raises instead of reading past it
    inline void decodeVlen(const char * in, const std::size_t nBytes, std::string * items, const std::size_t n) {
        if(nBytes < 4) {
            throw std::runtime_error("z5: vlen codec: chunk is too short for the item count");
        }
        const std::size_t count = vlen_detail::readU32(in);
        if(count != n) {
            throw std::runtime_error("z5: vlen codec: chunk holds " + std::to_string(count) +
                                     " items, expected " + std::to_string(n));
        }
        std::size_t pos = 4;
        for(std::size_t i = 0; i < n; ++i) {
            if(nBytes - pos < 4) {
                throw std::runtime_error("z5: vlen codec: chunk is truncated");
            }
            const std::size_t len = vlen_detail::readU32(in + pos);
            pos += 4;
            if(nBytes - pos < len) {
                throw std::runtime_error("z5: vlen codec: chunk is truncated");
            }
            items[i].assign(in + pos, len);
            pos += len;
        }
        if(pos != nBytes) {
            throw std::runtime_error("z5: vlen codec: unexpected bytes after the last item");
        }
    }

}
}
