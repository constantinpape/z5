#pragma once

#include <bit>
#include <cmath>
#include <cstdint>

namespace z5 {
namespace types {

    // IEEE 754 half precision (zarr's float16), held as its 16 bits. z5 only
    // copies float16 elements (datasets use uint16_t elements); these
    // conversions are for fill values and scalar writes. (std::float16_t is
    // C++23, and z5 builds as C++20.)

    // the value of half-precision bits; every half is exact as a double
    inline double float16ToDouble(const uint16_t bits) {
        const bool negative = bits & 0x8000u;
        const int exponent = (bits >> 10) & 0x1f;
        const unsigned mantissa = bits & 0x3ffu;
        double value;
        if(exponent == 0x1f) {
            value = mantissa ? std::nan("") : INFINITY;
        } else if(exponent == 0) {
            // zero or subnormal: mantissa * 2^-24
            value = std::ldexp(static_cast<double>(mantissa), -24);
        } else {
            value = std::ldexp(static_cast<double>(mantissa | 0x400u), exponent - 25);
        }
        return negative ? -value : value;
    }

    // the half-precision bits nearest to `value` (round to nearest, ties to
    // even, as numpy); out of range values become infinity and any NaN the
    // default quiet NaN
    inline uint16_t doubleToFloat16(const double value) {
        const uint64_t d = std::bit_cast<uint64_t>(value);
        const uint16_t sign = static_cast<uint16_t>((d >> 48) & 0x8000u);
        if(std::isnan(value)) {
            return sign | 0x7e00u;
        }
        const double magnitude = std::fabs(value);
        if(magnitude == 0.) {
            return sign;
        }
        // the largest half is 65504; from 65520 on, values round to infinity
        if(magnitude >= 65520.) {
            return sign | 0x7c00u;
        }
        int exponent;
        std::frexp(magnitude, &exponent);  // magnitude = m * 2^exponent, m in [0.5, 1)
        // the value in units of the half's last place: 2^-24 for subnormals
        // (exponent < -13), else 2^(exponent - 11), so 11 significant bits
        const int shift = exponent < -13 ? -24 : exponent - 11;
        const double scaled = std::ldexp(magnitude, -shift);
        // exact: scaled < 2^11, so its fraction is representable
        double units = std::floor(scaled);
        const double rest = scaled - units;
        if(rest > .5 || (rest == .5 && std::fmod(units, 2.) == 1.)) {
            units += 1.;
        }
        // the integer value at the last-place unit 2^shift; rounding up may
        // carry into the next exponent, which the bit layout absorbs: the
        // biased exponent and mantissa are contiguous
        const unsigned u = static_cast<unsigned>(units);
        if(shift == -24) {
            // subnormal (or, if u == 0x400, the smallest normal)
            return sign | static_cast<uint16_t>(u);
        }
        // normal: u in [2^10, 2^11]; biased exponent = shift + 25
        const unsigned biased = static_cast<unsigned>(shift + 25);
        return sign | static_cast<uint16_t>((biased << 10) + (u - 0x400u));
    }

}
}
