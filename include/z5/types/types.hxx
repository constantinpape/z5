#pragma once

#include <vector>
#include <string>
#include <map>

namespace z5 {
namespace types {

    //
    // Coordinates
    //

    // TODO implement class that inherits froms std::vector and overloads useful operators (+, -, etc.)
    // TODO rename to coordinate type
    // type for array shapes
    typedef std::vector<size_t> ShapeType;

    //
    // Datatypes
    //

    // TODO add bool ?
    // dtype enum and map
    enum Datatype {
        int8, int16, int32, int64,
        uint8, uint16, uint32, uint64,
        float32, float64
    };

    // TODO handle endianness differently ?
    std::map<std::string, Datatype> zarrToDtype({
        { {"<i1", int8}, {"<i2", int16}, {"<i4", int32}, {"<i8", int64},
          {"<u1", uint8}, {"<u2", uint16}, {"<u4", uint32}, {"<u8", uint64},
          {"<f4", float32}, {"<f8", float64}
        }
    });

    // TODO handle endianness differently
    std::map<Datatype, std::string> dtypeToZarr({
        { {int8   , "<i1"}, {int16,  "<i2"}, {int32, "<i4"}, {int64, "<i8"},
          {uint8  , "<u1"}, {uint16, "<u2"}, {uint32, "<u4"},{uint64,"<u8"},
          {float32, "<f4"}, {float64,"<f8"}
        }
    });


    // n5 dtypes
    std::map<std::string, Datatype> n5ToDtype({
        { {"int8", int8},  {"int16", int16},  {"int32", int32},  {"int64", int64},
          {"uint8", uint8}, {"uint16", uint16}, {"uint32", uint32}, {"uint64", uint64},
          {"float32", float32}, {"float64", float64}
        }
    });

    std::map<Datatype, std::string> dtypeToN5({
        { {int8   ,"int8"},    {int16,   "int16"},  {int32, "int32"},  {int64, "int64"},
          {uint8  ,"uint8"},   {uint16,  "uint16"}, {uint32,"uint32"}, {uint64, "uint64"},
          {float32,"float32"}, {float64, "float64"}
        }
    });


    //
    // Compressors
    //

    // the different compressors
    // that are supported
    enum Compressor {
        raw,
        #ifdef WITH_BLOSC
        blosc,
        #endif
        #ifdef WITH_ZLIB
        zlib,
        #endif
        #ifdef WITH_BZIP2
        bzip2,
        #endif
        #ifdef WITH_LZ4
        lz4,
        #endif
        #ifdef WITH_XZ
        xz
        #endif
    };

    std::map<std::string, Compressor> stringToCompressor({{
        {"raw", raw},
        #ifdef WITH_BLOSC
        {"blosc", blosc},
        #endif
        #ifdef WITH_ZLIB
        {"zlib", zlib},
        {"gzip", zlib},
        #endif
        #ifdef WITH_BZIP2
        {"bzip2", bzip2},
        #endif
        #ifdef WITH_LZ4
        {"lz4", lz4},
        #endif
        #ifdef WITH_XZ
        {"xz", xz}
        #endif
    }});

    std::map<std::string, Compressor> zarrToCompressor({{
        #ifdef WITH_BLOSC
        {"blosc", blosc},
        #endif
        #ifdef WITH_ZLIB
        {"zlib", zlib},
        #endif
        #ifdef WITH_BZIP2
        {"bzip2", bzip2},
        #endif
        #ifdef WITH_LZ4
        {"lz4", lz4},
        #endif
    }});

    std::map<Compressor, std::string> compressorToZarr({{
        #ifdef WITH_BLOSC
        {blosc, "blosc"},
        #endif
        #ifdef WITH_ZLIB
        {zlib, "zlib"},
        #endif
        #ifdef WITH_BZIP2
        {bzip2, "bzip2"},
        #endif
        #ifdef WITH_LZ4
        {lz4, "lz4"},
        #endif
    }});

    std::map<std::string, Compressor> n5ToCompressor({{
        {"raw", raw},
        #ifdef WITH_ZLIB
        {"gzip", zlib},
        #endif
        #ifdef WITH_BZIP2
        {"bzip2", bzip2},
        #endif
        #ifdef WITH_XZ
        {"xz", xz}
        #endif
    }});

    std::map<Compressor, std::string> compressorToN5({{
        {raw, "raw"},
        #ifdef WITH_ZLIB
        {zlib, "gzip"},
        #endif
        #ifdef WITH_BZIP2
        {bzip2, "bzip2"},
        #endif
        #ifdef WITH_XZ
        {xz, "xz"}
        #endif
    }});

} // namespace::types
    // overload ostream operator for ShapeType (a.k.a) vector for convinience
    inline std::ostream & operator << (std::ostream & os, const types::ShapeType & coord) {
        os << "Coordinates(";
        for(const auto & cc: coord) {
            os << " " << cc;
        }
        os << " )";
        return os;
    }
} // namespace::z5
