#pragma once

#include <vector>
#include <string>
#include <map>
#include <boost/any.hpp>

#include "nlohmann/json.hpp"


namespace z5 {
namespace types {

    //
    // Coordinates
    //

    // TODO implement class that inherits from std::vector and overloads useful operators (+, -, etc.)
    // TODO rename to coordinate type
    // type for array shapes
    typedef std::vector<std::size_t> ShapeType;

    //
    // Datatypes
    //

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
            static DtypeMap dtypeMap({{{"|i1", int8}, {"<i2", int16}, {"<i4", int32}, {"<i8", int64},
                                       {"|u1", uint8}, {"<u2", uint16}, {"<u4", uint32}, {"<u8", uint64},
                                       {"<f4", float32}, {"<f8", float64}}});
            return dtypeMap;
        }

        static InverseDtypeMap & dtypeToZarr() {

            static InverseDtypeMap dtypeMap({{{int8   , "|i1"}, {int16,  "<i2"}, {int32, "<i4"}, {int64, "<i8"},
                                              {uint8  , "|u1"}, {uint16, "<u2"}, {uint32, "<u4"},{uint64,"<u8"},
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
                {"bz2", bzip2},
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
                {bzip2, "bz2"},
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
                {"xz", xz},
                #endif
                #ifdef WITH_LZ4
                {"lz4", lz4}
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
                {xz, "xz"},
                #endif
                #ifdef WITH_LZ4
                {lz4, "lz4"}
                #endif
            }});
            return cMap;
        }
    };


    //
    // Compression Options
    //

    typedef std::map<std::string, boost::any> CompressionOptions;


    inline void readZarrCompressionOptionsFromJson(Compressor compressor,
                                                   const nlohmann::json & jOpts,
                                                   CompressionOptions & options) {
        std::string codec;
        switch(compressor) {
            #ifdef WITH_BLOSC
            case blosc: codec = jOpts["cname"];
                        options["codec"] = codec;
                        options["level"] = static_cast<int>(jOpts["clevel"]);
                        options["shuffle"] = static_cast<int>(jOpts["shuffle"]);
                        break;
            #endif
            #ifdef WITH_ZLIB
            case zlib: options["level"] = static_cast<int>(jOpts["level"]);
                       options["useZlib"] = true;
                       break;
            #endif
            #ifdef WITH_BZIP2
            case bzip2: options["level"] = static_cast<int>(jOpts["level"]);
            #endif
            // raw compression has no parameters
            default: break;
        }
    }


    inline void writeZarrCompressionOptionsToJson(Compressor compressor,
                                                  const CompressionOptions & options,
                                                  nlohmann::json & jOpts) {
        try {
            if(compressor == types::raw) {
                jOpts = nullptr;
            } else {
                jOpts["id"] = types::Compressors::compressorToZarr().at(compressor);
            }
        } catch(std::out_of_range) {
            throw std::runtime_error("z5.DatasetMetadata.toJsonZarr: wrong compressor for zarr format");
        }

        switch(compressor) {
            #ifdef WITH_BLOSC
            case blosc: jOpts["cname"]   = boost::any_cast<std::string>(options.at("codec"));
                        jOpts["clevel"]  = boost::any_cast<int>(options.at("level"));
                        jOpts["shuffle"] = boost::any_cast<int>(options.at("shuffle"));
                        break;
            #endif
            #ifdef WITH_ZLIB
            case zlib: jOpts["level"] = boost::any_cast<int>(options.at("level"));
                       break;
            #endif
            // raw compression has no parameters
            default: break;
        }
    }


    inline void readN5CompressionOptionsFromJson(Compressor compressor,
                                                 const nlohmann::json & jOpts,
                                                 CompressionOptions & options) {
        std::string codec;
        switch(compressor) {
            // TODO blosc in n5
            #ifdef WITH_BLOSC
            case blosc: codec = jOpts["codec"];
                        options["codec"] = codec;
                        options["level"] = static_cast<int>(jOpts["level"]);
                        options["shuffle"] = static_cast<int>(jOpts["shuffle"]);
                        break;
            #endif
            #ifdef WITH_ZLIB
            case zlib: options["level"] = static_cast<int>(jOpts["level"]);
                       options["useZlib"] = false;
                       break;
            #endif
            #ifdef WITH_BZIP2
            case bzip2: options["level"] = static_cast<int>(jOpts["blockSize"]); break;
            #endif
            #ifdef WITH_XZ
            case xz: options["level"] = static_cast<int>(jOpts["preset"]); break;
            #endif
            #ifdef WITH_LZ4
            case lz4: options["level"] = static_cast<int>(jOpts["blockSize"]); break;
            #endif
            // raw compression has no parameters
            default: break;
        }
    }


    inline void writeN5CompressionOptionsToJson(Compressor compressor,
                                                const CompressionOptions & options,
                                                nlohmann::json & jOpts) {
        try {
            jOpts["type"] = types::Compressors::compressorToN5().at(compressor);
        } catch(std::out_of_range) {
            throw std::runtime_error("z5.DatasetMetadata.toJsonN5: wrong compressor for N5 format");
        }

        switch(compressor) {
            // TODO blosc in n5
            #ifdef WITH_BLOSC
            case blosc: jOpts["name"] = boost::any_cast<std::string>(options.at("codec"));
                        jOpts["level"] = boost::any_cast<int>(options.at("level"));
                        jOpts["shuffle"] = boost::any_cast<int>(options.at("shuffle"));
                        break;
            #endif
            #ifdef WITH_ZLIB
            case zlib: jOpts["level"] = boost::any_cast<int>(options.at("level"));
                       break;
            #endif
            #ifdef WITH_BZIP2
            case bzip2: jOpts["blockSize"] = boost::any_cast<int>(options.at("level"));
                        break;
            #endif
            #ifdef WITH_XZ
            case xz: jOpts["preset"] = boost::any_cast<int>(options.at("level")); break;
            #endif
            #ifdef WITH_LZ4
            case lz4: jOpts["blockSize"] = boost::any_cast<int>(options.at("level")); break;
            #endif
            // raw compression has no parameters
            default: break;
        }
    }


    inline void defaultCompressionOptions(Compressor compressor,
                                          CompressionOptions & options,
                                          const bool isZarr) {

        switch(compressor) {
            #ifdef WITH_BLOSC
            case blosc: if(options.find("name") == options.end()){options["name"] = "lzf";}
                        if(options.find("level") == options.end()){options["level"] = 5;}
                        if(options.find("shuffle") == options.end()){options["shuffle"] = 1;}
                        break;
            #endif
            #ifdef WITH_ZLIB
            case zlib: if(options.find("level") == options.end()){options["level"] = 5;}
                       if(options.find("useZlib") == options.end()){options["useZlib"] = isZarr;}
                       break;
            #endif
            #ifdef WITH_BZIP2
            case bzip2: if(options.find("level") == options.end()){options["level"] = 5;}
                        break;
            #endif
            // raw compression has no parameters
            default: break;
        }
    }

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
