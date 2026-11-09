#pragma once

#include <cmath>
#include <complex>
#include <cstdint>
#include <bit>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>

#include "nlohmann/json.hpp"
#include "z5/types/types.hxx"
#include "z5/types/float16.hxx"
#include "z5/util/base64.hxx"

namespace z5 {
namespace types {

    // A dataset's fill value. After normalizeFillValue (which metadata parsing
    // and dataset creation apply), it holds int64_t for signed integer types
    // and uint64_t for unsigned ones, so every 64-bit value is exact, bool for
    // bool, double for float types, std::complex<double> for complex types,
    // and std::string for variable-length string and byte string types (the
    // raw bytes; the metadata stores a bytes fill value base64-encoded). Read
    // the typed value with fillValueAs<T>().
    //
    // NaN payloads are not kept: every NaN becomes the default quiet NaN and is
    // written as "NaN", as zarr-python does.
    using FillValue = std::variant<double, int64_t, uint64_t, std::string, bool, std::complex<double>>;

    inline bool isIntegerDtype(const Datatype dtype) {
        switch(dtype) {
            case int8: case int16: case int32: case int64:
            case uint8: case uint16: case uint32: case uint64:
                return true;
            default:
                return false;
        }
    }

    inline bool isSignedDtype(const Datatype dtype) {
        return dtype == int8 || dtype == int16 || dtype == int32 || dtype == int64;
    }

    inline bool isFloatDtype(const Datatype dtype) {
        return dtype == float16 || dtype == float32 || dtype == float64;
    }

    inline bool isBoolDtype(const Datatype dtype) {
        return dtype == boolean;
    }

    inline bool isComplexDtype(const Datatype dtype) {
        return dtype == complex64 || dtype == complex128 || dtype == complex256;
    }

    // the data type of one component of a complex type
    inline Datatype complexComponentDtype(const Datatype dtype) {
        return dtype == complex64 ? float32 : float64;
    }

    // a readable form of a fill value, for error messages
    inline std::string fillValueToString(const FillValue & v) {
        return std::visit([](const auto & x) {
            if constexpr(isComplexType<std::decay_t<decltype(x)>>) {
                return nlohmann::json::array({x.real(), x.imag()}).dump();
            } else {
                return nlohmann::json(x).dump();
            }
        }, v);
    }

    // a fill value that is a real number (not a string, bool or complex)
    inline bool isRealFillValue(const FillValue & v) {
        return std::holds_alternative<double>(v) || std::holds_alternative<int64_t>(v) ||
               std::holds_alternative<uint64_t>(v);
    }

    // a real fill value as a double
    inline double realFillValue(const FillValue & v) {
        if(const auto * i = std::get_if<int64_t>(&v)) {
            return static_cast<double>(*i);
        }
        if(const auto * u = std::get_if<uint64_t>(&v)) {
            return static_cast<double>(*u);
        }
        return std::get<double>(v);
    }

    inline double canonicalNaN(const double d) {
        return std::isnan(d) ? std::numeric_limits<double>::quiet_NaN() : d;
    }

    namespace fill_value_detail {

        inline int64_t signedMin(const std::size_t size) {
            return size == 8 ? std::numeric_limits<int64_t>::min() : -(int64_t(1) << (8 * size - 1));
        }

        inline int64_t signedMax(const std::size_t size) {
            return size == 8 ? std::numeric_limits<int64_t>::max() : (int64_t(1) << (8 * size - 1)) - 1;
        }

        inline uint64_t unsignedMax(const std::size_t size) {
            return size == 8 ? std::numeric_limits<uint64_t>::max() : (uint64_t(1) << (8 * size)) - 1;
        }

        inline std::string dtypeName(const Datatype dtype) {
            // the n5 / v3 names lack the complex types
            const auto & names = Datatypes::dtypeToN5();
            const auto it = names.find(dtype);
            return it != names.end() ? it->second : Datatypes::dtypeToZarr().at(dtype);
        }

        [[noreturn]] inline void invalid(const std::string & value, const Datatype dtype,
                                         const std::string & why) {
            throw std::runtime_error("z5: invalid fill_value " + value + " for data type " +
                                     dtypeName(dtype) + ": " + why);
        }

        [[noreturn]] inline void invalid(const FillValue & v, const Datatype dtype, const std::string & why) {
            invalid(fillValueToString(v), dtype, why);
        }

        // an integer fill value as the alternative for `dtype`, range checked
        inline FillValue integerFillValue(const FillValue & v, const Datatype dtype, const bool strict) {
            if(std::holds_alternative<bool>(v) || std::holds_alternative<std::complex<double>>(v)) {
                invalid(v, dtype, "expected an integer");
            }
            const std::size_t size = datatypeSize(dtype);
            const bool isSigned = isSignedDtype(dtype);
            if(const auto * u = std::get_if<uint64_t>(&v)) {
                if(*u > (isSigned ? static_cast<uint64_t>(signedMax(size)) : unsignedMax(size))) {
                    invalid(v, dtype, "out of range");
                }
                return isSigned ? FillValue(static_cast<int64_t>(*u)) : FillValue(*u);
            }
            if(const auto * i = std::get_if<int64_t>(&v)) {
                if(*i < 0) {
                    if(!isSigned || *i < signedMin(size)) {
                        invalid(v, dtype, "out of range");
                    }
                    return *i;
                }
                return integerFillValue(static_cast<uint64_t>(*i), dtype, strict);
            }
            if(const auto * d = std::get_if<double>(&v)) {
                if(!std::isfinite(*d)) {
                    invalid(v, dtype, "expected an integer");
                }
                double t = std::trunc(*d);
                if(t != *d && strict) {
                    invalid(v, dtype, "not an integer");
                }
                // all bounds are powers of two, so the comparisons are exact
                const double lo = isSigned ? static_cast<double>(signedMin(size)) : 0.;
                const double hiExclusive = std::ldexp(1., 8 * static_cast<int>(size) - (isSigned ? 1 : 0));
                if(!(t >= lo && t < hiExclusive)) {
                    invalid(v, dtype, "out of range");
                }
                return isSigned ? FillValue(static_cast<int64_t>(t)) : FillValue(static_cast<uint64_t>(t));
            }
            invalid(v, dtype, "expected an integer");
        }

        // a bool fill value: true / false, or the numbers 0 and 1
        inline FillValue boolFillValue(const FillValue & v, const Datatype dtype) {
            if(const auto * b = std::get_if<bool>(&v)) {
                return *b;
            }
            if(!isRealFillValue(v) || (realFillValue(v) != 0 && realFillValue(v) != 1)) {
                invalid(v, dtype, "expected true or false");
            }
            return realFillValue(v) == 1;
        }

        // a variable-length string fill value; the numeric default 0 (what
        // DatasetOptions starts with) means the empty string
        inline FillValue vlenFillValue(const FillValue & v, const Datatype dtype) {
            if(std::holds_alternative<std::string>(v)) {
                return v;
            }
            if(!isRealFillValue(v) || realFillValue(v) != 0) {
                invalid(v, dtype, "expected a string");
            }
            return std::string();
        }
    }


    // a float given by its bits (from a hex fill value or z5py), as a double;
    // any NaN becomes the default quiet NaN
    inline double floatFromBits(const uint64_t bits, const Datatype dtype) {
        const double d = dtype == float16 ? float16ToDouble(static_cast<uint16_t>(bits))
                       : dtype == float32 ? std::bit_cast<float>(static_cast<uint32_t>(bits))
                                          : std::bit_cast<double>(bits);
        return std::isnan(d) ? std::numeric_limits<double>::quiet_NaN() : d;
    }


    // Bring a fill value into the form described at FillValue, checking it
    // fits the data type. `strict` (zarr v3) refuses a non-integral value for
    // an integer type; otherwise (zarr v2, n5) it is truncated, as before.
    inline FillValue normalizeFillValue(const FillValue & v, const Datatype dtype, const bool strict) {
        if(isIntegerDtype(dtype)) {
            return fill_value_detail::integerFillValue(v, dtype, strict);
        }
        if(isBoolDtype(dtype)) {
            return fill_value_detail::boolFillValue(v, dtype);
        }
        if(isVlenDtype(dtype)) {
            return fill_value_detail::vlenFillValue(v, dtype);
        }
        if(isComplexDtype(dtype)) {
            // a real number is the real part
            if(const auto * c = std::get_if<std::complex<double>>(&v)) {
                return std::complex<double>(canonicalNaN(c->real()), canonicalNaN(c->imag()));
            }
            if(!isRealFillValue(v)) {
                fill_value_detail::invalid(v, dtype, "expected a number");
            }
            return std::complex<double>(canonicalNaN(realFillValue(v)), 0.);
        }
        if(!isRealFillValue(v)) {
            fill_value_detail::invalid(v, dtype, "expected a real number");
        }
        return canonicalNaN(realFillValue(v));
    }


    // A fill value from the element's bits (the element's bytes read as a
    // little-endian unsigned integer), e.g. as computed by numpy in z5py.
    inline FillValue fillValueFromBits(const Datatype dtype, const uint64_t bits) {
        using namespace fill_value_detail;
        const std::size_t size = datatypeSize(dtype);
        if(!isIntegerDtype(dtype) && !isFloatDtype(dtype) && !isBoolDtype(dtype)) {
            throw std::runtime_error("z5: fill value bits are not supported for data type " + dtypeName(dtype));
        }
        if(size < 8 && (bits >> (8 * size)) != 0) {
            invalid(std::to_string(bits), dtype, "the bits don't fit the data type");
        }
        if(isFloatDtype(dtype)) {
            return floatFromBits(bits, dtype);
        }
        if(isBoolDtype(dtype)) {
            return boolFillValue(bits, dtype);
        }
        if(isSignedDtype(dtype)) {
            // sign-extend the low `size` bytes
            const unsigned shift = 64 - 8 * size;
            return static_cast<int64_t>(bits << shift) >> shift;
        }
        return bits;
    }


    // The fill value as a dataset's element type. (A float16 dataset's
    // elements are uint16_t bits: use fillValueElement.)
    template<class T>
    T fillValueAs(const FillValue & v) {
        return std::visit([](const auto & x) -> T {
            using X = std::decay_t<decltype(x)>;
            if constexpr(std::is_same_v<T, std::string> || std::is_same_v<X, std::string>) {
                if constexpr(std::is_same_v<T, X>) {
                    return x;
                } else {
                    // normalizeFillValue keeps strings and numbers apart
                    throw std::runtime_error("z5: fill value type does not match the data type");
                }
            } else if constexpr(isComplexType<T>) {
                if constexpr(isComplexType<X>) {
                    return static_cast<T>(x);
                } else {
                    return T(static_cast<typename T::value_type>(x));
                }
            } else if constexpr(isComplexType<X>) {
                // normalizeFillValue gives complex fill values to complex types only
                throw std::runtime_error("z5: fill value type does not match the data type");
            } else {
                return static_cast<T>(x);
            }
        }, v);
    }


    // The fill value as an element of a `dtype` dataset: as fillValueAs, but
    // float16 elements are the half-precision bits
    template<class T>
    T fillValueElement(const FillValue & v, const Datatype dtype) {
        if constexpr(std::is_same_v<T, uint16_t>) {
            if(dtype == float16) {
                return doubleToFloat16(fillValueAs<double>(v));
            }
        }
        return fillValueAs<T>(v);
    }


    namespace fill_value_detail {

        // a float fill value of `floatDtype` (one component, for a complex
        // `dtype`): a number, "NaN", "Infinity" or "-Infinity", and in v3 also
        // a hex string of the value's bits
        inline double floatFromJson(const nlohmann::json & j, const Datatype floatDtype, const bool v3,
                                    const Datatype dtype) {
            if(j.is_number()) {
                return j.get<double>();
            }
            if(!j.is_string()) {
                invalid(j.dump(), dtype, "expected a number or a string");
            }
            const std::string s = j.get<std::string>();
            if(s == "NaN") {
                return std::numeric_limits<double>::quiet_NaN();
            } else if(s == "Infinity") {
                return std::numeric_limits<double>::infinity();
            } else if(s == "-Infinity") {
                return -std::numeric_limits<double>::infinity();
            } else if(v3 && s.size() > 2 && s[0] == '0' && s[1] == 'x') {
                // the value's bits, most significant first, two digits per byte
                const std::size_t size = datatypeSize(floatDtype);
                if(s.size() != 2 + 2 * size) {
                    invalid(j.dump(), dtype, "a hex fill value needs " + std::to_string(2 * size) + " digits");
                }
                uint64_t b = 0;
                for(std::size_t i = 2; i < s.size(); ++i) {
                    const char c = s[i];
                    unsigned digit;
                    if(c >= '0' && c <= '9') {
                        digit = c - '0';
                    } else if(c >= 'a' && c <= 'f') {
                        digit = c - 'a' + 10;
                    } else if(c >= 'A' && c <= 'F') {
                        digit = c - 'A' + 10;
                    } else {
                        invalid(j.dump(), dtype, "not a hex number");
                    }
                    b = (b << 4) | digit;
                }
                return floatFromBits(b, floatDtype);
            }
            invalid(j.dump(), dtype, "unknown string");
        }

        // the inverse of floatFromJson
        inline nlohmann::json floatToJson(const double d) {
            if(std::isnan(d)) {
                return "NaN";
            } else if(std::isinf(d)) {
                return d > 0 ? "Infinity" : "-Infinity";
            }
            return d;
        }
    }


    // Parse a fill_value from zarr v2 (.zarray) or v3 (zarr.json) metadata.
    //
    // Integer types take a JSON integer within the type's range (v3 also
    // accepts an integral float like 2.0, as zarr-python does). Float types take
    // a number, "NaN", "Infinity" or "-Infinity", and in v3 also a hex string of
    // the value's bits ("0x3f800000" is 1.0 for float32; NaN payloads are
    // dropped). v3 refuses null, as the spec and zarr-python do.
    //
    // Bool takes true or false (zarr v2 also accepts 0 and 1, and null for false).
    //
    // Complex types take [real, imag], each part as for a float type of half
    // the size (zarr v2 also accepts a number, the real part).
    //
    // zarr v2 is read leniently, as before, so that existing arrays still
    // open: null means NaN for float types and 0 for integer types, a
    // non-integral number for an integer type is truncated, and a string for an
    // integer type (which old z5 versions could write) reads as 0.
    inline FillValue fillValueFromJson(const nlohmann::json & j, const Datatype dtype, const int zarrFormat) {
        using namespace fill_value_detail;
        const bool v3 = zarrFormat == 3;

        if(isVlenDtype(dtype)) {
            // a bytes fill value is stored base64-encoded, as zarr-python does
            if(j.is_string() && dtype == vlen_bytes) {
                try {
                    return util::base64Decode(j.get<std::string>());
                } catch(const std::runtime_error & e) {
                    invalid(j.dump(), dtype, e.what());
                }
            }
            if(j.is_string()) {
                return j.get<std::string>();
            }
            // zarr v2 object arrays may have no fill value
            if(j.is_null() && !v3) {
                return std::string();
            }
            invalid(j.dump(), dtype, "expected a string");
        }

        if(isBoolDtype(dtype)) {
            if(j.is_boolean()) {
                return j.get<bool>();
            }
            if(!v3 && j.is_null()) {
                return false;
            }
            if(!v3 && j.is_number()) {
                return normalizeFillValue(j.get<double>(), dtype, false);
            }
            invalid(j.dump(), dtype, "expected true or false");
        }

        if(isComplexDtype(dtype) && j.is_array()) {
            if(j.size() != 2) {
                invalid(j.dump(), dtype, "expected [real, imag]");
            }
            const Datatype component = complexComponentDtype(dtype);
            return normalizeFillValue(std::complex<double>(floatFromJson(j[0], component, v3, dtype),
                                                           floatFromJson(j[1], component, v3, dtype)),
                                      dtype, false);
        }
        if(isComplexDtype(dtype) && v3) {
            invalid(j.dump(), dtype, "expected [real, imag]");
        }

        if(j.is_null()) {
            if(v3) {
                invalid(j.dump(), dtype, "zarr v3 requires a fill value");
            }
            return normalizeFillValue(isIntegerDtype(dtype) ? 0. : std::numeric_limits<double>::quiet_NaN(),
                                      dtype, false);
        }

        if(isIntegerDtype(dtype)) {
            FillValue v;
            if(j.is_number_unsigned()) {
                v = j.get<uint64_t>();
            } else if(j.is_number_integer()) {
                v = j.get<int64_t>();
            } else if(j.is_number_float()) {
                v = j.get<double>();
            } else if(v3) {
                invalid(j.dump(), dtype, "expected an integer");
            } else {
                v = 0.;
            }
            return normalizeFillValue(v, dtype, v3);
        }

        // float types, and a zarr v2 complex fill value given by its real part
        const Datatype floatDtype = isComplexDtype(dtype) ? complexComponentDtype(dtype) : dtype;
        return normalizeFillValue(floatFromJson(j, floatDtype, v3, dtype), dtype, false);
    }


    // Write a fill_value for zarr v2 or v3 metadata (the inverse of
    // fillValueFromJson). Integers are written exactly.
    inline void fillValueToJson(const Datatype dtype, const FillValue & v, nlohmann::json & j) {
        if(isBoolDtype(dtype)) {
            j = fillValueAs<uint8_t>(normalizeFillValue(v, dtype, false)) != 0;
            return;
        }
        if(isComplexDtype(dtype)) {
            const auto c = std::get<std::complex<double>>(normalizeFillValue(v, dtype, false));
            j = nlohmann::json::array({fill_value_detail::floatToJson(c.real()),
                                       fill_value_detail::floatToJson(c.imag())});
            return;
        }
        if(const auto * d = std::get_if<double>(&v)) {
            if(std::isnan(*d) || std::isinf(*d)) {
                j = fill_value_detail::floatToJson(*d);
            } else if(isIntegerDtype(dtype)) {
                // not normalized (a DatasetMetadata built by hand): write an
                // integer, which zarr v3 requires for integer types
                fillValueToJson(dtype, normalizeFillValue(*d, dtype, false), j);
            } else {
                j = *d;
            }
            return;
        }
        if(const auto * s = std::get_if<std::string>(&v); s && dtype == vlen_bytes) {
            j = util::base64Encode(*s);
            return;
        }
        std::visit([&](const auto & x) {
            if constexpr(isComplexType<std::decay_t<decltype(x)>>) {
                j = nlohmann::json::array({fill_value_detail::floatToJson(x.real()),
                                           fill_value_detail::floatToJson(x.imag())});
            } else {
                j = x;
            }
        }, v);
    }

} // namespace types
} // namespace z5
