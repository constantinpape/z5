#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <numeric>
#include <variant>
#include <complex>
#include <limits>
#include <set>

#include "nlohmann/json.hpp"



namespace z5 {
namespace types {

    //
    // Coordinates
    //

    // type for array shapes
    typedef std::vector<std::size_t> ShapeType;

    //
    // Datatypes
    //

    // dtype enum and map
    enum Datatype {
        int8, int16, int32, int64,
        uint8, uint16, uint32, uint64,
        float32, float64,
        complex64, complex128, complex256
    };

    struct Datatypes {
        typedef std::map<std::string, Datatype> DtypeMap;
        typedef std::map<Datatype, std::string> InverseDtypeMap;

        static DtypeMap & zarrToDtype() {
            static DtypeMap dtypeMap({{{"|i1", int8}, {"<i2", int16}, {"<i4", int32}, {"<i8", int64},
                                       {"|u1", uint8}, {"<u2", uint16}, {"<u4", uint32}, {"<u8", uint64},
                                       {"<f4", float32}, {"<f8", float64},
                                       {"<c8", complex64}, {"<c16", complex128}, {"<c32", complex256}}});
            return dtypeMap;
        }

        static InverseDtypeMap & dtypeToZarr() {

            static InverseDtypeMap dtypeMap({{{int8   , "|i1"}, {int16,  "<i2"}, {int32, "<i4"}, {int64, "<i8"},
                                              {uint8  , "|u1"}, {uint16, "<u2"}, {uint32, "<u4"},{uint64,"<u8"},
                                              {float32, "<f4"}, {float64,"<f8"},
                                              {complex64, "<c8"}, {complex128, "<c16"}, {complex256, "<c32"}}});
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
        xz,
        #endif
        #ifdef WITH_ZSTD
        zstd
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
                {"xz", xz},
                #endif
                #ifdef WITH_ZSTD
                {"zstd", zstd}
                #endif
            }});
            return cMap;
        }

        static CompressorMap & zarrToCompressor() {
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
                {"bz2", bzip2},
                #endif
                #ifdef WITH_LZ4
                {"lz4", lz4},
                #endif
                #ifdef WITH_ZSTD
                {"zstd", zstd}
                #endif
            }});
            return cMap;
        }

        static InverseCompressorMap & compressorToZarr() {
            static InverseCompressorMap cMap({{
                {raw, "raw"},
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
                #ifdef WITH_ZSTD
                {zstd, "zstd"}
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
                {"lz4", lz4},
                #endif
                #ifdef WITH_BLOSC
                {"blosc", blosc},
                #endif
                #ifdef WITH_ZSTD
                {"zstd", zstd}
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
                {lz4, "lz4"},
                #endif
                #ifdef WITH_BLOSC
                {blosc, "blosc"},
                #endif
                #ifdef WITH_ZSTD
                {zstd, "zstd"}
                #endif
            }});
            return cMap;
        }
    };


    //
    // Compression Options and Fill Value
    //
    typedef std::map<std::string, std::variant<int, bool, std::string>> CompressionOptions;

    inline void readZarrCompressionOptionsFromJson(Compressor compressor,
                                                   const nlohmann::json & jOpts,
                                                   CompressionOptions & options) {
        std::string codec;
        switch(compressor) {
            #ifdef WITH_BLOSC
            case blosc: options["codec"] = jOpts["cname"].get<std::string>();
                        options["level"] = jOpts["clevel"].get<int>();
                        options["shuffle"] = jOpts["shuffle"].get<int>();
                        // load blocksize with default value 0
                        options["blocksize"] = (jOpts.find("blocksize") == jOpts.end()) ? 0 : jOpts["blocksize"].get<int>();
                        break;
            #endif
            #ifdef WITH_ZLIB
            case zlib: options["level"] = jOpts["level"].get<int>();
                       options["useZlib"] = jOpts["id"].get<std::string>() == "zlib";
                       break;
            #endif
            #ifdef WITH_BZIP2
            case bzip2: options["level"] = jOpts["level"].get<int>(); break;
            #endif
            #ifdef WITH_LZ4
            case lz4: options["level"] = jOpts["acceleration"].get<int>(); break;
            #endif
            #ifdef WITH_ZSTD
            case zstd: options["level"] = jOpts["level"].get<int>(); break;
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
            case blosc: jOpts["cname"]   = std::get<std::string>(options.at("codec"));
                        jOpts["clevel"]  = std::get<int>(options.at("level"));
                        jOpts["shuffle"] = std::get<int>(options.at("shuffle"));
                        jOpts["blocksize"] = std::get<int>(options.at("blocksize"));
                        break;
            #endif
            #ifdef WITH_ZLIB
            case zlib: jOpts["id"] = std::get<bool>(options.at("useZlib")) ? "zlib" : "gzip";
                       jOpts["level"] = std::get<int>(options.at("level"));
                       break;
            #endif
            #ifdef WITH_BZIP2
            case bzip2: jOpts["level"] = std::get<int>(options.at("level")); break;
            #endif
            #ifdef WITH_LZ4
            case lz4: jOpts["acceleration"] = std::get<int>(options.at("level")); break;
            #endif
            #ifdef WITH_ZSTD
            case zstd: jOpts["level"] = std::get<int>(options.at("level")); break;
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
            #ifdef WITH_ZLIB
            case zlib: options["level"] = jOpts["level"].get<int>();
                       options["useZlib"] = false;
                       break;
            #endif
            #ifdef WITH_BZIP2
            case bzip2: options["level"] = jOpts["blockSize"].get<int>(); break;
            #endif
            #ifdef WITH_XZ
            case xz: options["level"] = jOpts["preset"].get<int>(); break;
            #endif
            #ifdef WITH_LZ4
            case lz4: options["level"] = jOpts["blockSize"].get<int>(); break;
            #endif
            #ifdef WITH_BLOSC
            case blosc: options["codec"] = jOpts["cname"].get<std::string>();
                        options["level"] = jOpts["clevel"].get<int>();
                        options["shuffle"] = jOpts["shuffle"].get<int>();
                        // load blocksize with default value 0
                        options["blocksize"] = (jOpts.find("blocksize") == jOpts.end()) ? 0 : jOpts["blocksize"].get<int>();
                        // load nthreads with default value 1
                        options["nthreads"] = (jOpts.find("nthreads") == jOpts.end()) ? 1 : jOpts["nthreads"].get<int>();
                        break;
            #endif
            #ifdef WITH_ZSTD
            case zstd: options["level"] = jOpts["level"].get<int>(); break;
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
            #ifdef WITH_ZLIB
            case zlib: jOpts["level"] = std::get<int>(options.at("level"));
                       break;
            #endif
            #ifdef WITH_BZIP2
            case bzip2: jOpts["blockSize"] = std::get<int>(options.at("level"));
                        break;
            #endif
            #ifdef WITH_XZ
            case xz: jOpts["preset"] = std::get<int>(options.at("level")); break;
            #endif
            #ifdef WITH_LZ4
            case lz4: jOpts["blockSize"] = std::get<int>(options.at("level")); break;
            #endif
            #ifdef WITH_BLOSC
            case blosc: jOpts["cname"] = std::get<std::string>(options.at("codec"));
                        jOpts["clevel"] = std::get<int>(options.at("level"));
                        jOpts["shuffle"] = std::get<int>(options.at("shuffle"));
                        jOpts["blocksize"] = std::get<int>(options.at("blocksize"));
                        jOpts["nthreads"] = std::get<int>(options.at("nthreads"));
                        break;
            #endif
            #ifdef WITH_ZSTD
            case zstd: jOpts["level"] = std::get<int>(options.at("level")); break;
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
            case blosc: if(options.find("codec") == options.end()){options["codec"] = std::string("lz4");}
                        if(options.find("level") == options.end()){options["level"] = 5;}
                        if(options.find("shuffle") == options.end()){options["shuffle"] = 1;}
                        if(options.find("blocksize") == options.end()){options["blocksize"] = 0;}
                        // n5 metadata serialization reads "nthreads" unconditionally
                        if(options.find("nthreads") == options.end()){options["nthreads"] = 1;}
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
            #ifdef WITH_LZ4
            case lz4: if(options.find("level") == options.end()){options["level"] = 6;}
                      break;
            #endif
            #ifdef WITH_XZ
            case xz: if(options.find("level") == options.end()){options["level"] = 6;}
                     break;
            #endif
            #ifdef WITH_ZSTD
            case zstd: if(options.find("level") == options.end()){options["level"] = 3;}
                       break;
            #endif
            // raw compression has no parameters
            default: break;
        }
    }

    //
    // zarr v3 codec (de)serialization
    //
    // size of a datatype in bytes (used for the blosc ``typesize`` field)
    inline std::size_t datatypeSize(Datatype dt) {
        switch(dt) {
            case int8: case uint8: return 1;
            case int16: case uint16: return 2;
            case int32: case uint32: case float32: return 4;
            case int64: case uint64: case float64: return 8;
            case complex64: return 8;
            case complex128: return 16;
            case complex256: return 32;
            default: return 1;
        }
    }

    inline std::string bloscShuffleToString(int s) {
        return s == 0 ? "noshuffle" : (s == 2 ? "bitshuffle" : "shuffle");
    }
    inline int bloscShuffleFromJson(const nlohmann::json & s) {
        if(s.is_number_integer() || s.is_number_unsigned()) {
            return s.get<int>();
        }
        const std::string v = s.get<std::string>();
        if(v == "noshuffle") return 0;
        if(v == "bitshuffle") return 2;
        return 1;  // "shuffle"
    }

    // build the zarr v3 codec list: optional transpose, the array-to-bytes ``bytes``
    // codec, then the bytes-to-bytes codecs (``crc32cBefore`` crc32c, the optional
    // compressor, ``crc32cAfter`` crc32c)
    inline void writeV3CodecsToJson(Compressor compressor,
                                    const CompressionOptions & options,
                                    std::size_t typesize,
                                    nlohmann::json & jCodecs,
                                    const bool bigEndian=false,
                                    const std::vector<std::size_t> & transposeOrder={},
                                    const unsigned crc32cBefore=0,
                                    const unsigned crc32cAfter=0) {
        jCodecs = nlohmann::json::array();
        nlohmann::json crcCodec;
        crcCodec["name"] = "crc32c";

        if(!transposeOrder.empty()) {
            nlohmann::json transposeCodec;
            transposeCodec["name"] = "transpose";
            transposeCodec["configuration"]["order"] = transposeOrder;
            jCodecs.push_back(transposeCodec);
        }

        nlohmann::json bytesCodec;
        bytesCodec["name"] = "bytes";
        bytesCodec["configuration"]["endian"] = bigEndian ? "big" : "little";
        jCodecs.push_back(bytesCodec);

        for(unsigned i = 0; i < crc32cBefore; ++i) {
            jCodecs.push_back(crcCodec);
        }

        switch(compressor) {
            case raw: break;
            #ifdef WITH_ZLIB
            case zlib: {
                // the core gzip codec is gzip-framed only; zlib framing is numcodecs.zlib
                const auto useZlib = options.find("useZlib");
                const bool isZlib = useZlib != options.end() && std::get<bool>(useZlib->second);
                nlohmann::json c;
                c["name"] = isZlib ? "numcodecs.zlib" : "gzip";
                c["configuration"]["level"] = std::get<int>(options.at("level"));
                jCodecs.push_back(c);
                break;
            }
            #endif
            #ifdef WITH_BZIP2
            case bzip2: {
                nlohmann::json c;
                c["name"] = "numcodecs.bz2";
                c["configuration"]["level"] = std::get<int>(options.at("level"));
                jCodecs.push_back(c);
                break;
            }
            #endif
            #ifdef WITH_XZ
            case xz: {
                nlohmann::json c;
                c["name"] = "numcodecs.lzma";
                c["configuration"]["preset"] = std::get<int>(options.at("level"));
                const auto check = options.find("check");
                if(check != options.end()) {
                    c["configuration"]["check"] = std::get<int>(check->second);
                }
                jCodecs.push_back(c);
                break;
            }
            #endif
            #ifdef WITH_LZ4
            case lz4: {
                nlohmann::json c;
                c["name"] = "numcodecs.lz4";
                c["configuration"]["acceleration"] = std::get<int>(options.at("level"));
                jCodecs.push_back(c);
                break;
            }
            #endif
            #ifdef WITH_ZSTD
            case zstd: {
                nlohmann::json c;
                c["name"] = "zstd";
                c["configuration"]["level"] = std::get<int>(options.at("level"));
                const auto checksum = options.find("checksum");
                c["configuration"]["checksum"] = checksum != options.end() && std::get<bool>(checksum->second);
                jCodecs.push_back(c);
                break;
            }
            #endif
            #ifdef WITH_BLOSC
            case blosc: {
                nlohmann::json c;
                c["name"] = "blosc";
                c["configuration"]["cname"] = std::get<std::string>(options.at("codec"));
                c["configuration"]["clevel"] = std::get<int>(options.at("level"));
                c["configuration"]["shuffle"] = bloscShuffleToString(std::get<int>(options.at("shuffle")));
                c["configuration"]["typesize"] = static_cast<int>(typesize);
                c["configuration"]["blocksize"] = std::get<int>(options.at("blocksize"));
                jCodecs.push_back(c);
                break;
            }
            #endif
            default:
                throw std::runtime_error("z5: unsupported compressor for zarr v3");
        }

        for(unsigned i = 0; i < crc32cAfter; ++i) {
            jCodecs.push_back(crcCodec);
        }
    }

    // parse the ``endian`` of a zarr v3 ``bytes`` codec; returns true for big-endian.
    // The field is required for multi-byte data types and may be omitted for 1-byte ones.
    inline bool readV3BytesEndian(const nlohmann::json & cfg, const std::size_t typesize) {
        auto it = cfg.find("endian");
        if(it == cfg.end() || it->is_null()) {
            if(typesize > 1) {
                throw std::runtime_error("z5: zarr v3 bytes codec requires 'endian' for multi-byte data types");
            }
            return false;
        }
        const std::string endian = it->get<std::string>();
        if(endian == "little") {
            return false;
        } else if(endian == "big") {
            return true;
        }
        throw std::runtime_error("z5: invalid endian for zarr v3 bytes codec: " + endian);
    }

    // parse the ``order`` of a zarr v3 ``transpose`` codec: a permutation of the
    // ndim axes (or the legacy "C" / "F" shorthands for identity / reversed)
    inline std::vector<std::size_t> readV3TransposeOrder(const nlohmann::json & cfg,
                                                         const std::size_t ndim) {
        const auto it = cfg.find("order");
        if(it == cfg.end()) {
            throw std::runtime_error("z5: zarr v3 transpose codec requires 'order'");
        }
        std::vector<std::size_t> order(ndim);
        if(it->is_string()) {
            const std::string s = it->get<std::string>();
            if(s != "C" && s != "F") {
                throw std::runtime_error("z5: invalid order for zarr v3 transpose codec: " + s);
            }
            for(std::size_t d = 0; d < ndim; ++d) {
                order[d] = (s == "C") ? d : ndim - 1 - d;
            }
            return order;
        }
        if(!it->is_array() || it->size() != ndim) {
            throw std::runtime_error("z5: zarr v3 transpose order must list every axis once");
        }
        std::vector<bool> seen(ndim, false);
        for(std::size_t d = 0; d < ndim; ++d) {
            const auto & v = (*it)[d];
            if(!(v.is_number_unsigned() || v.is_number_integer()) || v.get<long long>() < 0 ||
               v.get<long long>() >= static_cast<long long>(ndim) || seen[v.get<std::size_t>()]) {
                throw std::runtime_error("z5: zarr v3 transpose order is not a permutation of the axes");
            }
            order[d] = v.get<std::size_t>();
            seen[order[d]] = true;
        }
        return order;
    }

    //
    // zarr v3 compressors: the core codecs (gzip, zstd, blosc) and the
    // ``numcodecs.*`` codecs zarr-python writes when given a numcodecs compressor
    //
    inline bool isV3Compressor(const std::string & name) {
        static const std::set<std::string> names{
            "gzip", "zstd", "blosc",
            "numcodecs.gzip", "numcodecs.zlib", "numcodecs.zstd", "numcodecs.blosc",
            "numcodecs.bz2", "numcodecs.lzma", "numcodecs.lz4"};
        return names.count(name) > 0;
    }

    // refuse configuration keys outside `allowed`: z5 would silently ignore them
    inline void checkV3CodecConfig(const std::string & name, const nlohmann::json & cfg,
                                   const std::set<std::string> & allowed) {
        for(const auto & item : cfg.items()) {
            if(allowed.count(item.key()) == 0) {
                throw std::runtime_error("z5: zarr v3 " + name + " codec: unsupported setting '" +
                                         item.key() + "'");
            }
        }
    }

    // an integer setting in [lo, hi]; absent or null gives `fallback`
    inline int v3IntSetting(const std::string & name, const nlohmann::json & cfg,
                            const std::string & key, const int fallback,
                            const int lo, const int hi) {
        const auto it = cfg.find(key);
        if(it == cfg.end() || it->is_null()) {
            return fallback;
        }
        if(!(it->is_number_integer() || it->is_number_unsigned()) ||
           it->get<long long>() < lo || it->get<long long>() > hi) {
            throw std::runtime_error("z5: zarr v3 " + name + " codec: invalid " + key + ": " + it->dump());
        }
        return it->get<int>();
    }

    // Call before naming a codec's Compressor: the enum only declares the
    // codecs compiled in, so readV3Compressor looks them up by name
    // (Compressors::stringToCompressor) after this check.
    inline void requireV3Codec(const bool compiledIn, const std::string & name, const std::string & flag) {
        if(!compiledIn) {
            throw std::runtime_error("z5: zarr v3 " + name + " codec requires " + flag);
        }
    }

    #ifdef WITH_ZLIB
    constexpr bool haveV3Zlib = true;
    #else
    constexpr bool haveV3Zlib = false;
    #endif
    #ifdef WITH_ZSTD
    constexpr bool haveV3Zstd = true;
    #else
    constexpr bool haveV3Zstd = false;
    #endif
    #ifdef WITH_BLOSC
    constexpr bool haveV3Blosc = true;
    #else
    constexpr bool haveV3Blosc = false;
    #endif
    #ifdef WITH_BZIP2
    constexpr bool haveV3Bzip2 = true;
    #else
    constexpr bool haveV3Bzip2 = false;
    #endif
    #ifdef WITH_XZ
    constexpr bool haveV3Xz = true;
    #else
    constexpr bool haveV3Xz = false;
    #endif
    #ifdef WITH_LZ4
    constexpr bool haveV3Lz4 = true;
    #else
    constexpr bool haveV3Lz4 = false;
    #endif

    // translate a zarr v3 compressor (isV3Compressor(name)) into z5's Compressor
    // and CompressionOptions. Missing numcodecs settings take numcodecs' defaults,
    // since zarr-python only writes the settings that differ from them.
    inline void readV3Compressor(const std::string & name, const nlohmann::json & cfg,
                                 const std::size_t typesize,
                                 Compressor & compressor, CompressionOptions & options) {
        if(name == "gzip") {
            requireV3Codec(haveV3Zlib, name, "WITH_ZLIB");
            compressor = Compressors::stringToCompressor().at("zlib");
            options["level"] = cfg.value("level", 5);
            options["useZlib"] = false;
        } else if(name == "zstd") {
            requireV3Codec(haveV3Zstd, name, "WITH_ZSTD");
            compressor = Compressors::stringToCompressor().at("zstd");
            options["level"] = cfg.value("level", 3);
            options["checksum"] = cfg.value("checksum", false);
        } else if(name == "blosc") {
            requireV3Codec(haveV3Blosc, name, "WITH_BLOSC");
            compressor = Compressors::stringToCompressor().at("blosc");
            options["codec"] = cfg.value("cname", std::string("lz4"));
            options["level"] = cfg.value("clevel", 5);
            options["shuffle"] = cfg.contains("shuffle") ? bloscShuffleFromJson(cfg.at("shuffle")) : 1;
            options["blocksize"] = cfg.value("blocksize", 0);
        } else if(name == "numcodecs.gzip" || name == "numcodecs.zlib") {
            requireV3Codec(haveV3Zlib, name, "WITH_ZLIB");
            checkV3CodecConfig(name, cfg, {"level"});
            const bool isZlib = name == "numcodecs.zlib";
            compressor = Compressors::stringToCompressor().at("zlib");
            // zlib's -1 is its default level, 6
            const int level = v3IntSetting(name, cfg, "level", 1, isZlib ? -1 : 0, 9);
            options["level"] = level < 0 ? 6 : level;
            options["useZlib"] = isZlib;
        } else if(name == "numcodecs.zstd") {
            requireV3Codec(haveV3Zstd, name, "WITH_ZSTD");
            checkV3CodecConfig(name, cfg, {"level", "checksum"});
            compressor = Compressors::stringToCompressor().at("zstd");
            // level 0 is zstd's default level
            options["level"] = v3IntSetting(name, cfg, "level", 0, -131072, 22);
            const auto checksum = cfg.find("checksum");
            if(checksum != cfg.end() && !checksum->is_null() && !checksum->is_boolean()) {
                throw std::runtime_error("z5: zarr v3 " + name + " codec: invalid checksum: " + checksum->dump());
            }
            options["checksum"] = checksum != cfg.end() && checksum->is_boolean() && checksum->get<bool>();
        } else if(name == "numcodecs.blosc") {
            requireV3Codec(haveV3Blosc, name, "WITH_BLOSC");
            checkV3CodecConfig(name, cfg, {"cname", "clevel", "shuffle", "blocksize", "typesize"});
            compressor = Compressors::stringToCompressor().at("blosc");
            const auto cname = cfg.find("cname");
            options["codec"] = (cname == cfg.end() || cname->is_null()) ? std::string("lz4") : cname->get<std::string>();
            options["level"] = v3IntSetting(name, cfg, "clevel", 5, 0, 9);
            // -1 is numcodecs' AUTOSHUFFLE: bit shuffle for 1-byte items, else byte shuffle
            const int shuffle = v3IntSetting(name, cfg, "shuffle", 1, -1, 2);
            options["shuffle"] = shuffle >= 0 ? shuffle : (typesize == 1 ? 2 : 1);
            options["blocksize"] = v3IntSetting(name, cfg, "blocksize", 0, 0, std::numeric_limits<int>::max());
        } else if(name == "numcodecs.bz2") {
            requireV3Codec(haveV3Bzip2, name, "WITH_BZIP2");
            checkV3CodecConfig(name, cfg, {"level"});
            compressor = Compressors::stringToCompressor().at("bzip2");
            options["level"] = v3IntSetting(name, cfg, "level", 1, 1, 9);
        } else if(name == "numcodecs.lzma") {
            requireV3Codec(haveV3Xz, name, "WITH_XZ");
            checkV3CodecConfig(name, cfg, {"format", "check", "preset", "filters"});
            // z5 reads and writes the xz container only (lzma.FORMAT_XZ == 1),
            // with the default filter chain
            if(v3IntSetting(name, cfg, "format", 1, 0, 3) != 1) {
                throw std::runtime_error("z5: zarr v3 " + name + " codec: only format 1 (xz) is supported");
            }
            const auto filters = cfg.find("filters");
            if(filters != cfg.end() && !filters->is_null()) {
                throw std::runtime_error("z5: zarr v3 " + name + " codec: custom filters are not supported");
            }
            // check: -1 is the xz default (CRC64); the others are liblzma's lzma_check
            const int check = v3IntSetting(name, cfg, "check", -1, -1, 10);
            if(check != -1 && check != 0 && check != 1 && check != 4 && check != 10) {
                throw std::runtime_error("z5: zarr v3 " + name + " codec: invalid check: " + std::to_string(check));
            }
            compressor = Compressors::stringToCompressor().at("xz");
            options["level"] = v3IntSetting(name, cfg, "preset", 6, 0, 9);
            options["check"] = check == -1 ? 4 : check;
        } else if(name == "numcodecs.lz4") {
            requireV3Codec(haveV3Lz4, name, "WITH_LZ4");
            checkV3CodecConfig(name, cfg, {"acceleration"});
            compressor = Compressors::stringToCompressor().at("lz4");
            // lz4 treats an acceleration below 1 as 1
            const int acceleration = v3IntSetting(name, cfg, "acceleration", 1,
                                                  std::numeric_limits<int>::min(),
                                                  std::numeric_limits<int>::max());
            options["level"] = acceleration < 1 ? 1 : acceleration;
        } else {
            throw std::runtime_error("z5: unsupported zarr v3 codec: " + name);
        }
    }

    // parse a zarr v3 codec list into a Compressor + CompressionOptions, the byte
    // order requested by the ``bytes`` codec (bigEndian), the composition of
    // all ``transpose`` codecs (transposeOrder; empty if it is the identity) and
    // the number of ``crc32c`` codecs before and after the compressor. Without a
    // compressor, every crc32c counts as after it.
    inline void readV3CodecsFromJson(const nlohmann::json & jCodecs,
                                     const std::size_t typesize,
                                     const std::size_t ndim,
                                     Compressor & compressor,
                                     CompressionOptions & options,
                                     bool & bigEndian,
                                     std::vector<std::size_t> & transposeOrder,
                                     unsigned & crc32cBefore,
                                     unsigned & crc32cAfter) {
        compressor = raw;
        bigEndian = false;
        crc32cBefore = 0;
        crc32cAfter = 0;
        transposeOrder.resize(ndim);
        std::iota(transposeOrder.begin(), transposeOrder.end(), std::size_t(0));
        // the spec orders codecs as: array-to-array codecs (transpose), then
        // exactly one array-to-bytes codec (bytes), then bytes-to-bytes codecs
        // (compressors); any other order would be decoded wrongly
        bool seenBytes = false;
        bool seenCompressor = false;
        for(const auto & c : jCodecs) {
            const std::string name = c.at("name").get<std::string>();
            const nlohmann::json cfg = c.contains("configuration") ? c.at("configuration") : nlohmann::json::object();
            if(name == "bytes") {
                if(seenBytes) {
                    throw std::runtime_error("z5: zarr v3 codecs must contain exactly one bytes codec");
                }
                bigEndian = readV3BytesEndian(cfg, typesize);
                seenBytes = true;
                continue;
            }
            if(name == "transpose") {
                if(seenBytes) {
                    throw std::runtime_error("z5: zarr v3 transpose codec must come before the bytes codec");
                }
                // applying p1 then p2 equals a single transpose by p1[p2[i]]
                const auto order = readV3TransposeOrder(cfg, ndim);
                std::vector<std::size_t> composed(ndim);
                for(std::size_t d = 0; d < ndim; ++d) {
                    composed[d] = transposeOrder[order[d]];
                }
                transposeOrder = composed;
                continue;
            }
            if(name != "crc32c" && !isV3Compressor(name)) {
                throw std::runtime_error("z5: unsupported zarr v3 codec: " + name);
            }
            if(!seenBytes) {
                throw std::runtime_error("z5: zarr v3 " + name + " codec must come after the bytes codec");
            }
            if(name == "crc32c") {
                // no configuration in the spec; refuse anything we would ignore
                if(!cfg.empty()) {
                    throw std::runtime_error("z5: zarr v3 crc32c codec has an unsupported configuration");
                }
                ++(seenCompressor ? crc32cAfter : crc32cBefore);
                continue;
            }
            // z5 applies a single compressor (plus crc32c around it): decoding the
            // inner of two compressors would need its decompressed size
            if(seenCompressor) {
                throw std::runtime_error("z5: zarr v3 codecs with more than one compressor "
                                         "are not supported");
            }
            seenCompressor = true;
            readV3Compressor(name, cfg, typesize, compressor, options);
        }
        if(!seenBytes) {
            throw std::runtime_error("z5: zarr v3 codecs must contain exactly one bytes codec");
        }
        // without a compressor, crc32c codecs simply follow each other
        if(!seenCompressor) {
            crc32cAfter += crc32cBefore;
            crc32cBefore = 0;
        }
        // store the identity as an empty order (no transpose work on IO)
        bool identity = true;
        for(std::size_t d = 0; d < ndim; ++d) {
            identity = identity && transposeOrder[d] == d;
        }
        if(identity) {
            transposeOrder.clear();
        }
    }

    // generic translation from compression type to json
    // for pybindings
    inline void jsonToCompressionType(const nlohmann::json & j, CompressionOptions & opts) {
        for(const auto & elem : j.items()) {
            const std::string & key = elem.key();
            const auto & val = elem.value();
            if(val.type() == nlohmann::json::value_t::boolean) {
                opts[key] = static_cast<bool>(val);
            } else if (val.type() == nlohmann::json::value_t::number_integer || val.type() == nlohmann::json::value_t::number_unsigned) {
                opts[key] = static_cast<int>(val);
            } else if (val.type() == nlohmann::json::value_t::string) {
                // msvc does not like the static cast here ....
                const std::string tmp = val;
                opts[key] = tmp;
            } else {
                throw std::runtime_error(std::string("Invalid type conversion for compression type: ") +
                                         val.type_name());
            }
        }
    }

    inline void compressionTypeToJson(const CompressionOptions & opts, nlohmann::json & j) {
        for(auto & elem : opts) {
            const auto & val = elem.second;
            if(std::holds_alternative<int>(val)){
                j[elem.first] = std::get<int>(val);
            } else if(std::holds_alternative<bool>(val)){
                j[elem.first] = std::get<bool>(val);
            } else if(std::holds_alternative<std::string>(val)){
                j[elem.first] = std::get<std::string>(val);
            } else {
                throw std::runtime_error("Invalid type conversion for compression type");
            }

        }
    }

} // namespace::types
    // overload ostream operator for ShapeType (a.k.a) vector for convenience
    inline std::ostream & operator << (std::ostream & os, const types::ShapeType & coord) {
        os << "Coordinates(";
        for(const auto & cc: coord) {
            os << " " << cc;
        }
        os << " )";
        return os;
    }
} // namespace::z5
