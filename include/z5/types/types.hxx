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
    struct Datatypes {
        typedef std::map<std::string, Datatype> DtypeMap;
        typedef std::map<Datatype, std::string> InverseDtypeMap;

        static DtypeMap & zarrToDtype() {
            static DtypeMap dtypeMap({{{"<i1", int8}, {"<i2", int16}, {"<i4", int32}, {"<i8", int64},
                                       {"<u1", uint8}, {"<u2", uint16}, {"<u4", uint32}, {"<u8", uint64},
                                       {"<f4", float32}, {"<f8", float64}}});
            return dtypeMap;
        }

        static InverseDtypeMap & dtypeToZarr() {

            static InverseDtypeMap dtypeMap({{{int8   , "<i1"}, {int16,  "<i2"}, {int32, "<i4"}, {int64, "<i8"},
                                              {uint8  , "<u1"}, {uint16, "<u2"}, {uint32, "<u4"},{uint64,"<u8"},
                                              {float32, "<f4"}, {float64,"<f8"}}});
            return dtypeMap;
        }

        static DtypeMap & n5ToDtype() {
            static DtypeMap dtypeMap({{{"int8", int8},  {"int16", int16},  {"int32", int32},  {"int64", int64},
                                       {"uint8", uint8}, {"uint16", uint16}, {"uint32", uint32}, {"uint64", uint64},
                                       {"float32", float32}, {"float64", float64}}});
            return dtypeMap;
        }

        static InverseDtypeMap & dtypeToN5() {
            static InverseDtypeMap dtypeMap({{{int8   ,"int8"},    {int16,   "int16"},  {int32, "int32"},  {int64, "int64"},
                                              {uint8  ,"uint8"},   {uint16,  "uint16"}, {uint32,"uint32"}, {uint64, "uint64"},
                                              {float32,"float32"}, {float64, "float64"}}});
            return dtypeMap;
        }
    };


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


    struct Compressors {

        typedef std::map<std::string, Compressor> CompressorMap;
        typedef std::map<Compressor, std::string> InverseCompressorMap;

        static CompressorMap & stringToCompressor() {
            static CompressorMap cMap({{
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
            return cMap;
        }

        static CompressorMap & zarrToCompressor() {
            static CompressorMap cMap({{
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
            return cMap;
        }

        static InverseCompressorMap & compressorToZarr() {
            static InverseCompressorMap cMap({{
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
            return cMap;
        }

        static CompressorMap & n5ToCompressor() {
            static CompressorMap cMap({{
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
            return cMap;
        }

        static InverseCompressorMap & compressorToN5() {
            static InverseCompressorMap cMap({{
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
            return cMap;
        }
    };

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
