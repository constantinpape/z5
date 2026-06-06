#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

// CRC32C (Castagnoli) - used by the zarr v3 sharding_indexed codec to checksum
// the shard index. Reflected polynomial 0x82F63B78 (== bit-reverse of the
// Castagnoli polynomial 0x1EDC6F41), init 0xFFFFFFFF, final XOR 0xFFFFFFFF.
// This matches the "crc32c" codec used by zarr-python and tensorstore.

namespace z5 {
namespace util {

namespace crc32c_detail {

    inline const std::array<uint32_t, 256> & table() {
        static const std::array<uint32_t, 256> tbl = [](){
            std::array<uint32_t, 256> t{};
            for(uint32_t i = 0; i < 256; ++i) {
                uint32_t crc = i;
                for(int k = 0; k < 8; ++k) {
                    crc = (crc & 1u) ? (crc >> 1) ^ 0x82F63B78u : (crc >> 1);
                }
                t[i] = crc;
            }
            return t;
        }();
        return tbl;
    }

} // namespace crc32c_detail


    // compute the CRC32C of n bytes, optionally continuing from a previous crc
    inline uint32_t crc32c(const char * data, std::size_t n, uint32_t seed = 0) {
        const auto & tbl = crc32c_detail::table();
        uint32_t crc = ~seed;
        const unsigned char * p = reinterpret_cast<const unsigned char *>(data);
        for(std::size_t i = 0; i < n; ++i) {
            crc = tbl[(crc ^ p[i]) & 0xFFu] ^ (crc >> 8);
        }
        return ~crc;
    }

} // namespace util
} // namespace z5
