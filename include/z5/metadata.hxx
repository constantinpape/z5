#pragma once

#include <string>
#include <vector>

#ifndef BOOST_FILESYSTEM_NO_DEPERECATED
#define BOOST_FILESYSTEM_NO_DEPERECATED
#endif
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

namespace fs = boost::filesystem;

#include "z5/handle/handle.hxx"
#include "z5/types/types.hxx"

namespace z5 {

    // TODO general format strig
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
            const types::Compressor compressor=types::raw,
            const types::CompressionOptions & compressionOptions=types::CompressionOptions(),
            const double fillValue=0 // FIXME the fill value should not be used here
            ) : dtype(dtype),
                shape(shape),
                chunkShape(chunkShape),
                isZarr(isZarr),
                compressor(compressor),
                compressionOptions(compressionOptions),
                fillValue(fillValue) // FIXME fill value should not be used here
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
            types::writeZarrCompressionOptionsToJson(compressor, compressionOptions, compressionOpts);
            j["compressor"] = compressionOpts;

            j["dtype"] = types::Datatypes::dtypeToZarr().at(dtype);
            j["shape"] = shape;
            j["chunks"] = chunkShape;

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

            j["dataType"] = types::Datatypes::dtypeToN5().at(dtype);

            // write the new format
            nlohmann::json jOpts;
            types::writeN5CompressionOptionsToJson(compressor, compressionOptions, jOpts);
            j["compression"] = jOpts;
        }


        void fromJsonZarr(const nlohmann::json & j) {
            checkJson(j);
            dtype = types::Datatypes::zarrToDtype().at(j["dtype"]);
            shape = types::ShapeType(j["shape"].begin(), j["shape"].end());
            chunkShape = types::ShapeType(j["chunks"].begin(), j["chunks"].end());
            fillValue = static_cast<double>(j["fill_value"]); // FIXME fill value should not be used at this level
            const auto & compressionOpts = j["compressor"];

            try {
                compressor = compressionOpts.is_null() ?
                    types::raw : types::Compressors::zarrToCompressor().at(compressionOpts["id"]);
            } catch(std::out_of_range) {
                throw std::runtime_error("z5.DatasetMetadata.fromJsonZarr: wrong compressor for zarr format");
            }

            types::readZarrCompressionOptionsFromJson(compressor, compressionOpts, compressionOptions);
        }


        void fromJsonN5(const nlohmann::json & j) {

            dtype = types::Datatypes::n5ToDtype().at(j["dataType"]);

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

            // FIXME fill value should not be used here
            fillValue = 0;
        }

    public:
        // metadata values that can be set
        types::Datatype dtype;
        types::ShapeType shape;
        types::ShapeType chunkShape;
        bool isZarr; // flag to specify whether we have a zarr or n5 array

        // compressor name and opyions
        types::Compressor compressor;
        types::CompressionOptions compressionOptions;

        // FIXME fill value should not be used at this level of implementation
        double fillValue;

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
                    std::cout << chunkShape <<  " " << shape << std::endl;
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

            jIt = j.find("filters");
            if(jIt != j.end()) {
                if(!j["filters"].is_null()) {
                    throw std::runtime_error(
                        "Invalid Filters: Z5 does not support filters"
                    );
                }
            }
        }
    };


    //
    // helper functions
    //

    inline void writeMetadata(const handle::Group & handle, const Metadata & metadata) {
        auto filePath = handle.path();
        filePath /= ".zgroup";
        nlohmann::json j;
        j["zarr_format"] = metadata.zarrFormat;
        fs::ofstream file(filePath);
        file << std::setw(4) << j << std::endl;
        file.close();
    }

    inline void writeMetadata(const handle::Dataset & handle, const DatasetMetadata & metadata) {
        fs::path filePath = handle.path();
        nlohmann::json j;
        metadata.toJson(j);
        filePath /= metadata.isZarr ? ".zarray" : "attributes.json";
        fs::ofstream file(filePath);
        file << std::setw(4) << j << std::endl;
        file.close();
    }


    inline bool getMetadataPath(const handle::Dataset & handle, fs::path & path) {
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


    inline void readMetadata(const handle::Dataset & handle, DatasetMetadata & metadata) {
        nlohmann::json j;
        fs::path filePath;
        auto isZarr = getMetadataPath(handle, filePath);
        fs::ifstream file(filePath);
        file >> j;
        metadata.fromJson(j, isZarr);
        file.close();
    }


    inline types::Datatype readDatatype(const handle::Dataset & handle) {
        fs::path filePath;
        bool isZarr = getMetadataPath(handle, filePath);
        fs::ifstream file(filePath);
        nlohmann::json j;
        file >> j;
        file.close();
        return isZarr ? types::Datatypes::zarrToDtype().at(j["dtype"]) : types::Datatypes::n5ToDtype().at(j["dataType"]);
    }

} // namespace::z5
