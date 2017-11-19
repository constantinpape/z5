#pragma once

#include <string>
#include <vector>
#include <boost/any.hpp>
#include "json.hpp"

#ifndef BOOST_FILESYSTEM_NO_DEPERECATED
#define BOOST_FILESYSTEM_NO_DEPERECATED
#endif
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

namespace fs = boost::filesystem;

#include "z5/handle/handle.hxx"
#include "z5/types/types.hxx"

namespace z5 {

    struct Metadata {
        const int zarrFormat = 2;
    };


    struct DatasetMetadata : public Metadata {

        //template<typename T>
        DatasetMetadata(
            const types::Datatype dtype,
            const types::ShapeType & shape,
            const types::ShapeType & chunkShape,
            const bool isZarr,
            const double fillValue=0, // FIXME this should be a template if we use boost::any
            const types::Compressor compressor=types::blosc,
            const std::string & codec="lz4",
            const int compressorLevel=5,
            const int compressorShuffle=1
            ) : dtype(dtype),
                shape(shape),
                chunkShape(chunkShape),
                isZarr(isZarr),
                fillValue(fillValue), // FIXME boost::any
                compressor(compressor),
                codec(codec),
                compressorLevel(compressorLevel),
                compressorShuffle(compressorShuffle)
        {
            checkShapes();
        }


        // empty constructur
        DatasetMetadata()
        {}


        //
        void toJson(nlohmann::json & j) const {
            if(isZarr) {
                toJsonZarr(j);
            } else {
                toJsonN5(j);
            }
        }


        //
        void fromJson(nlohmann::json & j, const bool isZarrDs) {
            isZarr = isZarrDs;
            isZarr ? fromJsonZarr(j) : fromJsonN5(j);
            checkShapes();
        }

    private:
        void toJsonZarr(nlohmann:: json & j) const {

            nlohmann::json compressionOpts;
            try {
                compressionOpts["id"] = (compressor == types::raw) ? nullptr : types::compressorToZarr.at(compressor);
            } catch(std::out_of_range) {
                throw std::runtime_error("z5.DatasetMetadata.toJsonZarr: wrong compressor for zarr format");
            }
            compressionOpts["cname"] = codec;
            compressionOpts["clevel"] = compressorLevel;
            compressionOpts["shuffle"] = compressorShuffle;
            j["compressor"] = compressionOpts;

            j["dtype"] = types::dtypeToZarr.at(dtype);
            j["shape"] = shape;
            j["chunks"] = chunkShape;

            // FIXME boost::any isn't doing what it's supposed to :(
            //j["fill_value"] = types::isRealType(.dtype) ? boost::any_cast<float>(.fillValue)
            //    : (types::isUnsignedType(.dtype) ? boost::any_cast<uint64_t>(.fillValue)
            //        : boost::any_cast<int64_t>(.fillValue));
            j["fill_value"] = fillValue;

            j["filters"] = filters;
            j["order"] = order;
            j["zarr_format"] = zarrFormat;
        }

        void toJsonN5(nlohmann::json & j) const {

            // N5-Axis order: we need to reverse the shape when writing to metadata
            types::ShapeType rshape(shape.rbegin(), shape.rend());
            j["dimensions"] = rshape;

            // N5-Axis order: we need to reverse the block-size when writing to metadata
            types::ShapeType rchunks(chunkShape.rbegin(), chunkShape.rend());
            j["blockSize"] = rchunks;

            j["dataType"] = types::dtypeToN5.at(dtype);

            try {
                j["compressionType"] = types::compressorToN5.at(compressor);
            } catch(std::out_of_range) {
                throw std::runtime_error("z5.DatasetMetadata.toJsonN5: wrong compressor for N5 format");
            }
        }


        void fromJsonZarr(const nlohmann::json & j) {
            checkJson(j);
            dtype = types::zarrToDtype.at(j["dtype"]);
            shape = types::ShapeType(j["shape"].begin(), j["shape"].end());
            chunkShape = types::ShapeType(j["chunks"].begin(), j["chunks"].end());
            fillValue = static_cast<double>(j["fill_value"]); // FIXME boost::any
            const auto & compressionOpts = j["compressor"];

            try {
                compressor = compressionOpts["id"].is_null() ?
                    types::raw : types::zarrToCompressor.at(compressionOpts["id"]);
            } catch(std::out_of_range) {
                throw std::runtime_error("z5.DatasetMetadata.fromJsonZarr: wrong compressor for zarr format");
            }

            codec    = compressionOpts["cname"];
            compressorLevel   = compressionOpts["clevel"];
            compressorShuffle = compressionOpts["shuffle"];
        }


        void fromJsonN5(const nlohmann::json & j) {

            dtype = types::n5ToDtype.at(j["dataType"]);

            // N5-Axis order: we need to reverse the shape when reading from metadata
            shape = types::ShapeType(j["dimensions"].rbegin(), j["dimensions"].rend());

            // N5-Axis order: we need to reverse the chunk shape when reading from metadata
            chunkShape = types::ShapeType(j["blockSize"].rbegin(), j["blockSize"].rend());

            try {
                compressor = types::n5ToCompressor.at(j["compressionType"]);
            } catch(std::out_of_range) {
                throw std::runtime_error("z5.DatasetMetadata.fromJsonN5: wrong compressor for N5 format");
            }

            codec = (compressor == types::zlib) ? "gzip" : "";
            // TODO these should become parameters in N5
            compressorLevel = 5;
            fillValue = 0;
        }

    public:
        // metadata values that can be set
        types::Datatype dtype;
        types::ShapeType shape;
        types::ShapeType chunkShape;

        // FIXME boost::any isn't doing what it's supposed to :(
        // for now we just store the fill value as double as a hacky workaround
        //boost::any fillValue;
        double fillValue;

        int compressorLevel;
        types::Compressor compressor;
        std::string codec;
        int compressorShuffle;
        bool isZarr; // flag to specify whether we have a zarr or n5 array

        // metadata values that are fixed for now
        // zarr format is fixed to 2
        const std::string order = "C";
        const std::nullptr_t filters = nullptr;

    private:

        // make sure that shapes agree
        void checkShapes() {
            if(shape.size() != chunkShape.size()) {
                throw std::runtime_error("Dimension of shape and chunks does not agree");
            }
            for(unsigned d = 0; d < shape.size(); ++d) {
                if(chunkShape[d] > shape[d]) {
                    throw std::runtime_error("Chunkshape cannot be bigger than shape");
                }
            }
        }


        // make sure that fixed metadata values agree
        void checkJson(const nlohmann::json & j) {

            // check if order exists and check for the correct value
            auto jIt = j.find("order");
            if(jIt != j.end()) {
                if(*jIt != order) {
                    throw std::runtime_error(
                        "Invalid Order: Zarr++ only supports C order"
                    );
                }
            }

            jIt = j.find("zarr_format");
            if(jIt != j.end()) {
                if(*jIt != zarrFormat) {
                    throw std::runtime_error(
                        "Invalid Zarr format: Zarr++ only supports zarr format 2"
                    );
                }
            }

            jIt = j.find("filters");
            if(jIt != j.end()) {
                if(!j["filters"].is_null()) {
                    throw std::runtime_error(
                        "Invalid Filters: Zarr++ does not support filters"
                    );
                }
            }
        }
    };


    //
    // helper functions
    //

    void writeMetadata(const handle::Group & handle, const Metadata & metadata) {
        auto filePath = handle.path();
        filePath /= ".zgroup";
        nlohmann::json j;
        j["zarr_format"] = metadata.zarrFormat;
        fs::ofstream file(filePath);
        file << std::setw(4) << j << std::endl;
        file.close();
    }

    void writeMetadata(const handle::Dataset & handle, const DatasetMetadata & metadata) {
        fs::path filePath = handle.path();
        nlohmann::json j;
        metadata.toJson(j);
        filePath /= metadata.isZarr ? ".zarray" : "attributes.json";
        fs::ofstream file(filePath);
        file << std::setw(4) << j << std::endl;
        file.close();
    }


    bool getMetadataPath(const handle::Dataset & handle, fs::path & path) {
        fs::path zarrPath = handle.path();
        fs::path n5Path = handle.path();
        zarrPath /= ".zarray";
        n5Path /= "attributes.json";
        if(fs::exists(zarrPath) && fs::exists(n5Path)) {
            throw std::runtime_error("Zarr and N5 specification are not both supported");
        }
        if(!fs::exists(zarrPath) && !fs::exists(n5Path)){
            throw std::runtime_error("Invalid path: no metadata existing");
        }
        bool isZarr = fs::exists(zarrPath);
        path = isZarr ? zarrPath : n5Path;
        return isZarr;
    }


    void readMetadata(const handle::Dataset & handle, DatasetMetadata & metadata) {
        nlohmann::json j;
        fs::path filePath;
        auto isZarr = getMetadataPath(handle, filePath);
        fs::ifstream file(filePath);
        file >> j;
        metadata.fromJson(j, isZarr);
        file.close();
    }


    types::Datatype readDatatype(const handle::Dataset & handle) {
        fs::path filePath;
        bool isZarr = getMetadataPath(handle, filePath);
        fs::ifstream file(filePath);
        nlohmann::json j;
        file >> j;
        file.close();
        return isZarr ? types::zarrToDtype.at(j["dtype"]) : types::n5ToDtype.at(j["dataType"]);
    }

} // namespace::z5
