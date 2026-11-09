#pragma once

#include <cmath>
#include <limits>
#include <optional>
#include <set>
#include <string>
#include <vector>
#include <iomanip>

#include "z5/types/types.hxx"
#include "z5/types/fill_value.hxx"


namespace z5 {

    // general format
    struct Metadata {
        bool isZarr; // flag to specify whether we have a zarr or n5 array
        int zarrFormat;
        int n5Major;
        int n5Minor;
        int n5Patch;

        Metadata(const bool isZarr, const int zarrFormat=2) : isZarr(isZarr),
                                      zarrFormat(zarrFormat),
                                      n5Major(2),
                                      n5Minor(0),
                                      n5Patch(0)
        {}
        inline std::string n5Format() const {
            return std::to_string(n5Major) + "." + std::to_string(n5Minor) + "." + std::to_string(n5Patch);
        }
    };


    struct DatasetMetadata : public Metadata {

        //template<typename T>
        DatasetMetadata(
            const types::Datatype dtype,
            const types::ShapeType & shape,
            const types::ShapeType & chunkShape,
            const bool isZarr,
            const types::Compressor compressor=types::raw,
            const types::CompressionOptions & compressionOptions=types::CompressionOptions(),
            const types::FillValue & fillValue=0.,
            const std::string & zarrDelimiter=".",
            const int zarrFormat=2,
            const std::string & chunkKeyEncoding="default",
            const types::ShapeType & shardShape=types::ShapeType(),
            const std::vector<std::optional<std::string>> & dimensionNames={}
            ) : Metadata(isZarr, zarrFormat),
                dtype(dtype),
                shape(shape),
                chunkShape(chunkShape),
                compressor(compressor),
                compressionOptions(compressionOptions),
                fillValue(types::normalizeFillValue(fillValue, dtype, false)),
                zarrDelimiter(zarrDelimiter),
                chunkKeyEncoding(chunkKeyEncoding),
                shardShape(shardShape),
                dimensionNames(dimensionNames)
        {
            checkShapes();
        }


        // empty constructor
        DatasetMetadata() : Metadata(true)
        {}


        //
        void toJson(nlohmann::json & j) const {
            if(isZarr && zarrFormat == 3) {
                toJsonV3(j);
            } else if(isZarr) {
                toJsonZarr(j);
            } else {
                toJsonN5(j);
            }
        }


        //
        void fromJson(nlohmann::json & j, const bool isZarrDs) {
            isZarr = isZarrDs;
            if(isZarr) {
                // distinguish zarr v2 (.zarray) from v3 (zarr.json) by the version field
                auto it = j.find("zarr_format");
                if(it != j.end() && it->get<int>() == 3) {
                    zarrFormat = 3;
                    fromJsonV3(j);
                } else {
                    zarrFormat = 2;
                    fromJsonZarr(j);
                }
            } else {
                fromJsonN5(j);
            }
            checkShapes();
        }

    private:
        void toJsonZarr(nlohmann:: json & j) const {

            nlohmann::json compressionOpts;
            types::writeZarrCompressionOptionsToJson(compressor, compressionOptions, compressionOpts);
            j["compressor"] = compressionOpts;

            // object arrays name their item codec as a filter (numcodecs' layout)
            if(types::isVlenDtype(dtype)) {
                j["dtype"] = "|O";
            } else {
                j["dtype"] = types::Datatypes::dtypeToZarr().at(dtype);
            }
            j["shape"] = shape;
            j["chunks"] = chunkShape;

            types::fillValueToJson(dtype, fillValue, j["fill_value"]);

            if(types::isVlenDtype(dtype)) {
                j["filters"] = nlohmann::json::array({{{"id", types::vlenCodecName(dtype)}}});
            } else {
                j["filters"] = nullptr;
            }
            j["order"] = "C";
            j["zarr_format"] = zarrFormat;
            j["dimension_separator"] = zarrDelimiter;
        }

        void toJsonN5(nlohmann::json & j) const {

            // N5-Axis order: we need to reverse the shape when writing to metadata
            types::ShapeType rshape(shape.rbegin(), shape.rend());
            j["dimensions"] = rshape;

            // N5-Axis order: we need to reverse the block-size when writing to metadata
            types::ShapeType rchunks(chunkShape.rbegin(), chunkShape.rend());
            j["blockSize"] = rchunks;

            j["dataType"] = types::Datatypes::dtypeToN5().at(dtype);

            // write the new format
            nlohmann::json jOpts;
            types::writeN5CompressionOptionsToJson(compressor, compressionOptions, jOpts);
            j["compression"] = jOpts;
        }


        void fromJsonZarr(const nlohmann::json & j) {
            checkJson(j);
            if(j.at("dtype") == "|O") {
                // object arrays: the filter names how the items are encoded
                dtype = objectDtypeFromFilters(j.contains("filters") ? j.at("filters") : nlohmann::json());
            } else {
                try {
                    dtype = types::Datatypes::zarrToDtype().at(j["dtype"]);
                } catch(std::out_of_range) {
                    throw std::runtime_error("Unsupported zarr dtype: " + static_cast<std::string>(j["dtype"]));
                }
            }
            shape = types::ShapeType(j["shape"].begin(), j["shape"].end());
            chunkShape = types::ShapeType(j["chunks"].begin(), j["chunks"].end());

            fillValue = types::fillValueFromJson(j["fill_value"], dtype, 2);

            auto jIt = j.find("dimension_separator");
            if(jIt != j.end()) {
                zarrDelimiter = *jIt;
            } else {
                zarrDelimiter = ".";
            }

            const auto & compressionOpts = j["compressor"];

            std::string zarrCompressorId = compressionOpts.is_null() ? "raw" : compressionOpts["id"];
            try {
                compressor = types::Compressors::zarrToCompressor().at(zarrCompressorId);
            } catch(std::out_of_range) {
                throw std::runtime_error("z5.DatasetMetadata.fromJsonZarr: wrong compressor for zarr format: " + zarrCompressorId);
            }

            types::readZarrCompressionOptionsFromJson(compressor, compressionOpts, compressionOptions);
        }


        void fromJsonN5(const nlohmann::json & j) {

            dtype = types::Datatypes::n5ToDtype().at(j["dataType"]);
            // n5's own "string" type is encoded differently from zarr's vlen
            // codecs, and the zarr-only names aren't n5 types
            if(!types::isN5Dtype(dtype)) {
                throw std::runtime_error("z5: n5 data type " + j["dataType"].get<std::string>() +
                                         " is not supported");
            }

            // N5-Axis order: we need to reverse the shape when reading from metadata
            shape = types::ShapeType(j["dimensions"].rbegin(), j["dimensions"].rend());

            // N5-Axis order: we need to reverse the chunk shape when reading from metadata
            chunkShape = types::ShapeType(j["blockSize"].rbegin(), j["blockSize"].rend());

            // we need to deal with two different encodings for compression in n5:
            // in the old format, we only have the field 'compressionType', indicating which compressor should be used
            // in the new format, we have the field 'type', which indicates the compressor
            // and can have additional attributes for options

            std::string n5Compressor;
            auto jIt = j.find("compression");

            if(jIt != j.end()) {
                const auto & jOpts = *jIt;
                auto j2It = jOpts.find("type");
                if(j2It != jOpts.end()) {
                    n5Compressor = *j2It;
                } else {
                    throw std::runtime_error("z5.DatasetMetadata.fromJsonN5: wrong compression format");
                }

                // get the actual compressor
                try {
                    compressor = types::Compressors::n5ToCompressor().at(n5Compressor);
                } catch(std::out_of_range) {
                    throw std::runtime_error("z5.DatasetMetadata.fromJsonN5: wrong compressor for n5 format");
                }

                readN5CompressionOptionsFromJson(compressor, jOpts, compressionOptions);
            }

            else {
                auto j2It = j.find("compressionType");
                if(j2It != j.end()) {
                    n5Compressor = *j2It;
                } else {
                    throw std::runtime_error("z5.DatasetMetadata.fromJsonN5: wrong compression format");
                }

                // get the actual compressor
                try {
                    compressor = types::Compressors::n5ToCompressor().at(n5Compressor);
                } catch(std::out_of_range) {
                    throw std::runtime_error("z5.DatasetMetadata.fromJsonN5: wrong compressor for n5 format");
                }

                // for the old compression, we just write the default gzip options
                compressionOptions["level"] = 5;
                compressionOptions["useZlib"] = false;
            }

            // n5 has no fill value
            fillValue = types::normalizeFillValue(0., dtype, false);
        }


        void toJsonV3(nlohmann::json & j) const {
            j["zarr_format"] = 3;
            j["node_type"] = "array";
            j["data_type"] = types::Datatypes::dtypeToN5().at(dtype);
            j["shape"] = shape;

            const bool sharded = !shardShape.empty();

            // the chunk grid addresses shards when sharding, else (inner) chunks
            j["chunk_grid"]["name"] = "regular";
            j["chunk_grid"]["configuration"]["chunk_shape"] = sharded ? shardShape : chunkShape;

            // chunk key encoding ("default" -> nested under 'c/', "v2" -> flat)
            j["chunk_key_encoding"]["name"] = chunkKeyEncoding;
            j["chunk_key_encoding"]["configuration"]["separator"] = zarrDelimiter;

            types::fillValueToJson(dtype, fillValue, j["fill_value"]);

            nlohmann::json innerCodecs;
            types::writeV3CodecsToJson(compressor, compressionOptions,
                                       types::datatypeSize(dtype), innerCodecs, bigEndian,
                                       transposeOrder, crc32cBeforeCompressor,
                                       crc32cAfterCompressor,
                                       types::isVlenDtype(dtype) ? types::vlenCodecName(dtype) : "bytes");

            if(sharded) {
                nlohmann::json sharding;
                sharding["name"] = "sharding_indexed";
                sharding["configuration"]["chunk_shape"] = chunkShape;  // inner chunk shape
                sharding["configuration"]["codecs"] = innerCodecs;
                nlohmann::json indexCodecs = nlohmann::json::array();
                nlohmann::json bytesCodec;
                bytesCodec["name"] = "bytes";
                bytesCodec["configuration"]["endian"] = shardIndexBigEndian ? "big" : "little";
                indexCodecs.push_back(bytesCodec);
                if(shardIndexChecksum) {
                    nlohmann::json crcCodec;
                    crcCodec["name"] = "crc32c";
                    indexCodecs.push_back(crcCodec);
                }
                sharding["configuration"]["index_codecs"] = indexCodecs;
                sharding["configuration"]["index_location"] = shardIndexAtStart ? "start" : "end";
                j["codecs"] = nlohmann::json::array({sharding});
            } else {
                j["codecs"] = innerCodecs;
            }

            // optional names of the dimensions; an unnamed dimension is stored as null
            if(!dimensionNames.empty()) {
                nlohmann::json names = nlohmann::json::array();
                for(const auto & name : dimensionNames) {
                    names.push_back(name ? nlohmann::json(*name) : nlohmann::json(nullptr));
                }
                j["dimension_names"] = names;
            }

            // user attributes are stored inline; preserved/merged by the attribute layer
            if(j.find("attributes") == j.end()) {
                j["attributes"] = nlohmann::json::object();
            }
        }


        // The spec requires refusing an array with a metadata field we don't
        // know, unless it is an extension object marked "must_understand": false.
        static void checkV3Fields(const nlohmann::json & j) {
            static const std::set<std::string> known = {
                "zarr_format", "node_type", "shape", "data_type", "chunk_grid",
                "chunk_key_encoding", "fill_value", "codecs", "attributes",
                "storage_transformers", "dimension_names"
            };
            for(const auto & item : j.items()) {
                if(known.count(item.key())) {
                    continue;
                }
                const auto & val = item.value();
                const auto mu = val.is_object() ? val.find("must_understand") : val.end();
                if(val.is_object() && mu != val.end() && mu->is_boolean() && !mu->get<bool>()) {
                    continue;
                }
                throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: unsupported metadata field: " +
                                         item.key());
            }
            // storage transformers change where and how chunks are stored; z5 supports none
            const auto st = j.find("storage_transformers");
            if(st != j.end() && !st->is_null() && !st->empty()) {
                throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: storage_transformers are not supported");
            }
        }

        // read the shard index layout: index_location "end" (the default) or
        // "start", and index_codecs = bytes (either endian) optionally followed
        // by crc32c; any other index codec can't be decoded and is refused
        void readV3ShardIndex(const nlohmann::json & scfg) {
            const std::string loc = scfg.value("index_location", std::string("end"));
            if(loc != "end" && loc != "start") {
                throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: invalid shard index_location: " + loc);
            }
            shardIndexAtStart = loc == "start";

            const auto & ic = scfg.at("index_codecs");
            const bool supported = ic.is_array() && (ic.size() == 1 || ic.size() == 2) &&
                ic[0].at("name") == "bytes" &&
                (ic.size() == 1 || ic[1].at("name") == "crc32c");
            if(!supported) {
                throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: shard index_codecs other than "
                                         "bytes, optionally followed by crc32c, are not supported: " +
                                         ic.dump());
            }
            const auto & bytesCfg = ic[0].contains("configuration") ? ic[0].at("configuration")
                                                                     : nlohmann::json::object();
            shardIndexBigEndian = types::readV3BytesEndian(bytesCfg, sizeof(uint64_t));
            shardIndexChecksum = ic.size() == 2;
        }

        void fromJsonV3(const nlohmann::json & j) {
            if(j.at("node_type") != "array") {
                throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: node_type is not 'array'");
            }
            checkV3Fields(j);
            try {
                dtype = types::Datatypes::n5ToDtype().at(j.at("data_type"));
            } catch(std::out_of_range) {
                throw std::runtime_error("Unsupported zarr v3 data_type: " + j.at("data_type").get<std::string>());
            }
            shape = types::ShapeType(j.at("shape").begin(), j.at("shape").end());

            // outer chunk grid (== shard shape when sharded, else inner chunk shape)
            const auto & grid = j.at("chunk_grid");
            if(grid.at("name") != "regular") {
                throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: unsupported chunk_grid: " +
                                         grid.at("name").dump());
            }
            const auto & gridShape = grid.at("configuration").at("chunk_shape");
            types::ShapeType outerShape(gridShape.begin(), gridShape.end());

            // chunk key encoding
            const auto & cke = j.at("chunk_key_encoding");
            chunkKeyEncoding = cke.at("name").get<std::string>();
            if(chunkKeyEncoding != "default" && chunkKeyEncoding != "v2") {
                throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: unsupported chunk_key_encoding: " +
                                         chunkKeyEncoding);
            }
            if(cke.contains("configuration") && cke.at("configuration").contains("separator")) {
                zarrDelimiter = cke.at("configuration").at("separator").get<std::string>();
                if(zarrDelimiter != "/" && zarrDelimiter != ".") {
                    throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: invalid chunk key separator: " +
                                             zarrDelimiter);
                }
            } else {
                zarrDelimiter = (chunkKeyEncoding == "v2") ? "." : "/";
            }

            // fill value
            fillValue = types::fillValueFromJson(j.at("fill_value"), dtype, 3);

            // codecs: detect sharding, recover inner chunk shape + inner compressor
            const auto & codecs = j.at("codecs");
            const nlohmann::json * innerCodecs = &codecs;
            shardShape.clear();
            bool sharded = false;
            for(const auto & c : codecs) {
                if(c.at("name") == "sharding_indexed") {
                    sharded = true;
                    const auto & scfg = c.at("configuration");
                    const auto & innerShapeJson = scfg.at("chunk_shape");
                    chunkShape = types::ShapeType(innerShapeJson.begin(), innerShapeJson.end());
                    shardShape = outerShape;
                    innerCodecs = &scfg.at("codecs");
                    readV3ShardIndex(scfg);
                    break;
                }
            }
            if(sharded && shape.empty()) {
                // z5 marks unsharded datasets by an empty shard shape, so it can't
                // tell a zero-dimensional shard apart; reading the shard as a plain
                // chunk would return wrong data
                throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: sharded zero-dimensional arrays "
                                         "are not supported");
            }
            if(!sharded) {
                chunkShape = outerShape;
            } else if(codecs.size() != 1) {
                // e.g. a transpose of the whole shard: ignoring it would silently
                // misread the data
                throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: codecs before or after "
                                         "sharding_indexed are not supported");
            }

            types::readV3CodecsFromJson(*innerCodecs, types::datatypeSize(dtype), shape.size(),
                                        compressor, compressionOptions, bigEndian,
                                        transposeOrder, crc32cBeforeCompressor,
                                        crc32cAfterCompressor,
                                        types::isVlenDtype(dtype) ? types::vlenCodecName(dtype) : "bytes");

            // optional names of the dimensions; a null name is an unnamed dimension
            dimensionNames.clear();
            auto namesIt = j.find("dimension_names");
            if(namesIt != j.end() && !namesIt->is_null()) {
                if(!namesIt->is_array()) {
                    throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: dimension_names must be an array");
                }
                for(const auto & name : *namesIt) {
                    if(name.is_null()) {
                        dimensionNames.emplace_back(std::nullopt);
                    } else if(name.is_string()) {
                        dimensionNames.emplace_back(name.get<std::string>());
                    } else {
                        throw std::runtime_error("z5.DatasetMetadata.fromJsonV3: a dimension name must be a string or null");
                    }
                }
            }
        }

    public:
        // metadata values that can be set
        types::Datatype dtype;
        types::ShapeType shape;
        types::ShapeType chunkShape;

        // compressor name and options
        types::Compressor compressor;
        types::CompressionOptions compressionOptions;

        // the fill value: an exact integer for integer types, a double for
        // float types; see types/fill_value.hxx. Read the typed value with
        // fillValueAs<T>().
        types::FillValue fillValue;
        std::string zarrDelimiter;

        // zarr v3 specific: chunk key encoding ("default"/"v2") and the shard
        // shape (empty unless the sharding_indexed codec is used)
        std::string chunkKeyEncoding = "default";
        types::ShapeType shardShape;

        // zarr v3 specific: chunk elements are stored big-endian (``bytes`` codec
        // with endian "big"); z5 always writes little-endian for new datasets
        bool bigEndian = false;
        // zarr v3 specific: the names of the dimensions, std::nullopt for an
        // unnamed dimension; empty if the array has no dimension names
        std::vector<std::optional<std::string>> dimensionNames;

        // zarr v3 specific: layout of the shard index (sharding codec's
        // index_location and index_codecs); new arrays use the defaults
        bool shardIndexAtStart = false;
        bool shardIndexBigEndian = false;
        bool shardIndexChecksum = true;

        // zarr v3 specific: number of ``crc32c`` codecs before and after the
        // compressor (without a compressor, all count as after it)
        unsigned crc32cBeforeCompressor = 0;
        unsigned crc32cAfterCompressor = 0;

        // zarr v3 specific: composed permutation of the ``transpose`` codecs
        // (numpy ``transpose`` semantics); empty == no transpose
        std::vector<std::size_t> transposeOrder;

        // the fill value as the dataset's element type
        template<class T>
        T fillValueAs() const {
            return types::fillValueAs<T>(fillValue);
        }

        // metadata values that are fixed for now
        // zarr format is fixed to 2
        // const std::string order = "C";
        // const std::nullptr_t filters = nullptr;

    private:

        // make sure that shapes agree
        void checkShapes() {
            if(shape.size() != chunkShape.size()) {
                throw std::runtime_error("Dimension of shape and chunks does not agree");
            }
            if(!dimensionNames.empty() && dimensionNames.size() != shape.size()) {
                throw std::runtime_error("Dimension of shape and dimension names does not agree");
            }
            // n5 stores chunks in a directory per axis, so a zero-dimensional
            // chunk would be the dataset directory itself
            if(!isZarr && shape.empty()) {
                throw std::runtime_error("z5: zero-dimensional datasets are only supported for zarr");
            }
        }


        // the data type of a zarr v2 object array (``|O``), from its single
        // vlen filter; other filters (or none) can't be decoded
        static types::Datatype objectDtypeFromFilters(const nlohmann::json & filters) {
            if(filters.is_array() && filters.size() == 1 && filters[0].is_object()) {
                const auto & f = filters[0];
                const std::string id = f.value("id", std::string());
                for(const auto candidate : {types::vlen_utf8, types::vlen_bytes}) {
                    if(id == types::vlenCodecName(candidate)) {
                        // numcodecs' vlen codecs have no settings
                        if(f.size() != 1) {
                            throw std::runtime_error("z5: zarr v2 filter " + id + " has unsupported settings");
                        }
                        return candidate;
                    }
                }
            }
            throw std::runtime_error("z5: zarr v2 object arrays (|O) are only supported with a single "
                                     "vlen-utf8 or vlen-bytes filter, got " + filters.dump());
        }

        // make sure that fixed metadata values agree
        void checkJson(const nlohmann::json & j) {

            // check if order exists and check for the correct value
            auto jIt = j.find("order");
            if(jIt != j.end()) {
                if(*jIt != "C") {
                    throw std::runtime_error(
                        "Invalid Order: Z5 only supports C order"
                    );
                }
            }

            jIt = j.find("zarr_format");
            if(jIt != j.end()) {
                if(*jIt != zarrFormat) {
                    throw std::runtime_error(
                        "Invalid Zarr format: Z5 only supports zarr format 2"
                    );
                }
            }

            // object arrays (``|O``) carry their item codec as a filter, see
            // objectDtypeFromFilters
            jIt = j.find("filters");
            if(jIt != j.end() && j.value("dtype", std::string()) != "|O") {
                if(!j["filters"].is_null() && j["filters"].size() > 0) {
                    throw std::runtime_error(
                        "Invalid Filters: Z5 does not support filters"
                    );
                }
            }
        }
    };


    // Options for creating a dataset, besides its dtype, shape and chunk shape.
    // New creation features (e.g. zarr v3 codecs) are added as fields here, not
    // as further trailing parameters of createDataset / createDatasetMetadata.
    struct DatasetOptions {
        std::string compressor = "raw";
        types::CompressionOptions compressionOptions;
        // any alternative is accepted and converted to the data type's form
        // (see types::FillValue); int64_t / uint64_t keep 64-bit integers exact
        types::FillValue fillValue = 0.;
        std::string zarrDelimiter = ".";
        int zarrFormat = 2;
        // zarr v3 only
        std::string chunkKeyEncoding = "default";
        // zarr v3 only; empty: not sharded
        types::ShapeType shardShape;
        // zarr v3 only; the names of the dimensions, std::nullopt for an
        // unnamed dimension; empty: no dimension names
        std::vector<std::optional<std::string>> dimensionNames;
        // zarr v3 only; the transpose codec's permutation, empty: no transpose
        std::vector<std::size_t> transposeOrder;
        // zarr v3 only; add a crc32c codec after the compressor
        bool crc32c = false;
    };


    inline void createDatasetMetadata(
        const std::string & dtype,
        const types::ShapeType & shape,
        const types::ShapeType & chunkShape,
        const bool createAsZarr,
        const DatasetOptions & options,
        DatasetMetadata & metadata)
    {
        const auto & compressor = options.compressor;
        const auto & zarrFormat = options.zarrFormat;
        const auto & shardShape = options.shardShape;
        const auto & dimensionNames = options.dimensionNames;
        const auto & transposeOrder = options.transposeOrder;

        // get the internal data type
        types::Datatype internalDtype;
        try {
            internalDtype = types::Datatypes::n5ToDtype().at(dtype);
        } catch(const std::out_of_range & e) {
            throw std::runtime_error("z5::createDatasetMetadata: Invalid dtype for dataset");
        }
        // the types n5 lacks (bool, strings, ...) are zarr only
        if(!createAsZarr && !types::isN5Dtype(internalDtype)) {
            throw std::runtime_error("z5::createDatasetMetadata: " + dtype + " datasets are only supported for zarr");
        }
        // variable-length strings: not bare lz4 blocks (zarr v2), which don't
        // record the decoded size. Transpose is checked below.
        if(types::isVlenDtype(internalDtype)) {
            if(zarrFormat != 3 && compressor == "lz4") {
                throw std::runtime_error("z5::createDatasetMetadata: lz4 is not supported for zarr v2 " + dtype +
                                         " datasets");
            }
        }

        // get the compressor
        types::Compressor internalCompressor;
        try {
            internalCompressor = types::Compressors::stringToCompressor().at(compressor);
        } catch(const std::out_of_range & e) {
            throw std::runtime_error("z5::createDatasetMetadata: Invalid compressor for dataset");
        }

        // add the default compression options if necessary
        // we need to make a copy of the compression options, because they are const
        auto internalCompressionOptions = options.compressionOptions;
        // zarr v3 names the framing: "gzip" is the core gzip codec, "zlib" is
        // numcodecs.zlib. (The generic default, zlib framing for zarr, would store
        // zlib chunks under the gzip codec.)
        if(createAsZarr && zarrFormat == 3 && internalCompressor == types::zlib &&
           internalCompressionOptions.find("useZlib") == internalCompressionOptions.end()) {
            internalCompressionOptions["useZlib"] = compressor == "zlib";
        }
        types::defaultCompressionOptions(internalCompressor, internalCompressionOptions, createAsZarr);

        // sharding is only valid for zarr v3, and the shard shape must be a
        // whole multiple of the (inner) chunk shape along every axis
        if(!shardShape.empty()) {
            if(!(createAsZarr && zarrFormat == 3)) {
                throw std::runtime_error("z5::createDatasetMetadata: sharding is only supported for zarr v3");
            }
            if(shardShape.size() != chunkShape.size()) {
                throw std::runtime_error("z5::createDatasetMetadata: shard shape and chunk shape dimension mismatch");
            }
            for(std::size_t d = 0; d < shardShape.size(); ++d) {
                if(shardShape[d] < chunkShape[d] || shardShape[d] % chunkShape[d] != 0) {
                    throw std::runtime_error("z5::createDatasetMetadata: shard shape must be a multiple of the chunk shape");
                }
            }
        }

        // dimension names are only valid for zarr v3
        if(!dimensionNames.empty() && !(createAsZarr && zarrFormat == 3)) {
            throw std::runtime_error("z5::createDatasetMetadata: dimension names are only supported for zarr v3");
        }

        // the transpose codec is only valid for zarr v3 and must permute all axes;
        // the identity is stored as "no transpose codec"
        std::vector<std::size_t> order;
        if(!transposeOrder.empty()) {
            if(!(createAsZarr && zarrFormat == 3)) {
                throw std::runtime_error("z5::createDatasetMetadata: transpose is only supported for zarr v3");
            }
            std::vector<bool> seen(shape.size(), false);
            if(transposeOrder.size() != shape.size()) {
                throw std::runtime_error("z5::createDatasetMetadata: transpose order must list every axis once");
            }
            for(const auto axis : transposeOrder) {
                if(axis >= shape.size() || seen[axis]) {
                    throw std::runtime_error("z5::createDatasetMetadata: transpose order must list every axis once");
                }
                seen[axis] = true;
            }
            for(std::size_t d = 0; d < transposeOrder.size(); ++d) {
                if(transposeOrder[d] != d) {
                    order = transposeOrder;
                    break;
                }
            }
        }

        // the parser refuses a transpose of variable-length elements too
        if(!order.empty() && types::isVlenDtype(internalDtype)) {
            throw std::runtime_error("z5::createDatasetMetadata: transpose is not supported for " + dtype +
                                     " datasets");
        }

        if(options.crc32c && !(createAsZarr && zarrFormat == 3)) {
            throw std::runtime_error("z5::createDatasetMetadata: crc32c is only supported for zarr v3");
        }

        // zarr v3 refuses a non-integral fill value for integer types; zarr v2
        // and n5 truncate it, as before
        const types::FillValue fillValue = types::normalizeFillValue(options.fillValue, internalDtype,
                                                                     createAsZarr && zarrFormat == 3);

        metadata = DatasetMetadata(internalDtype, shape,
                                   chunkShape, createAsZarr,
                                   internalCompressor, internalCompressionOptions,
                                   fillValue, options.zarrDelimiter,
                                   zarrFormat, options.chunkKeyEncoding, shardShape,
                                   dimensionNames);
        metadata.transposeOrder = order;
        metadata.crc32cAfterCompressor = options.crc32c ? 1 : 0;
    }


    // positional form, kept for compatibility; new options are only in DatasetOptions
    inline void createDatasetMetadata(
        const std::string & dtype,
        const types::ShapeType & shape,
        const types::ShapeType & chunkShape,
        const bool createAsZarr,
        const std::string & compressor,
        const types::CompressionOptions & compressionOptions,
        const double fillValue,
        const std::string & zarrDelimiter,
        DatasetMetadata & metadata,
        const int zarrFormat=2,
        const std::string & chunkKeyEncoding="default",
        const types::ShapeType & shardShape=types::ShapeType())
    {
        DatasetOptions options;
        options.compressor = compressor;
        options.compressionOptions = compressionOptions;
        options.fillValue = fillValue;
        options.zarrDelimiter = zarrDelimiter;
        options.zarrFormat = zarrFormat;
        options.chunkKeyEncoding = chunkKeyEncoding;
        options.shardShape = shardShape;
        createDatasetMetadata(dtype, shape, chunkShape, createAsZarr, options, metadata);
    }


} // namespace::z5
