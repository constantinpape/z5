#pragma once

#include <complex>
#include <memory>
#include <string>

#include "z5/dataset.hxx"


namespace z5 {
namespace generic {

    // dispatch a dataset factory callable over the scalar datatypes: 'make' is a
    // generic lambda invoked with a value tag (make(int8_t()), ...); 'fallback'
    // handles the remaining, backend-specific datatypes (complex256, zarr v2
    // only, on the filesystem; unsupported elsewhere)
    template<class MAKE, class FALLBACK>
    inline std::unique_ptr<z5::Dataset> dispatchDtype(const types::Datatype dtype,
                                                      MAKE && make, FALLBACK && fallback) {
        switch(dtype) {
            case types::int8:    return make(int8_t());
            case types::int16:   return make(int16_t());
            case types::int32:   return make(int32_t());
            case types::int64:   return make(int64_t());
            case types::uint8:   return make(uint8_t());
            case types::uint16:  return make(uint16_t());
            case types::uint32:  return make(uint32_t());
            case types::uint64:  return make(uint64_t());
            case types::float32: return make(float());
            case types::float64: return make(double());
            // bool: one byte per element, 0 or 1 (numpy's layout); a
            // std::vector<bool> buffer couldn't hold it
            case types::boolean: return make(uint8_t());
            // float16: the half-precision bits (types/float16.hxx)
            case types::float16: return make(uint16_t());
            case types::complex64:  return make(std::complex<float>());
            case types::complex128: return make(std::complex<double>());
            // variable-length strings: chunks are std::string elements, encoded by
            // the vlen codecs (util/vlen.hxx)
            case types::vlen_utf8: return make(std::string());
            case types::vlen_bytes: return make(std::string());
            default:             return fallback(dtype);
        }
    }

}
}
