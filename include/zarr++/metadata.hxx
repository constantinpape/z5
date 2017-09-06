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

#include "zarr++/handle/handle.hxx"
#include "zarr++/types/types.hxx"

namespace zarr {

    struct Metadata {
        const static int zarrFormat = 2;
    };


    struct ArrayMetadata : public Metadata {

        template<typename T>
        ArrayMetadata(
            const std::string & dtype,
            const types::ShapeType & shape,
            const types::ShapeType & chunkShape,
            T fillValue=0,
            int compressorLevel=5,
            const std::string & compressorName="lz4",
            const std::string & compressorId="blosc",
            int compressorShuffle=1
            ) : dtype(types::parseDtype(dtype)),
                shape(shape),
                chunkShape(chunkShape),
                fillValue(fillValue),
                compressorLevel(compressorLevel),
                compressorName(compressorName),
                compressorId(compressorId),
                compressorShuffle(compressorShuffle)
        {}

        // empty constructur
        ArrayMetadata()
        {}

        // metadata values that can be set
        std::string dtype;
        types::ShapeType shape;
        types::ShapeType chunkShape;
        boost::any fillValue;
        int compressorLevel;
        std::string compressorName;
        std::string compressorId;
        int compressorShuffle;

        // metadata values that are fixed for now
        // zarr format is fixed to 2
        const std::string order = "C";
        const std::nullptr_t filters = nullptr;
    };

    void writeMetadata(
        const handle::Array & handle, const ArrayMetadata & metadata
    ) {

        nlohmann::json compressor;
        compressor["clevel"] = metadata.compressorLevel;
        compressor["cname"] = metadata.compressorName;
        compressor["id"] = metadata.compressorId;
        compressor["shuffle"] = metadata.compressorShuffle;

        nlohmann::json j;
        j["chunks"] = metadata.chunkShape;
        j["compressor"] = compressor;
        j["dtype"] = metadata.dtype;
        // need to cast to correct dtype
        j["fill_value"] = types::isRealType(metadata.dtype) ? boost::any_cast<float>(metadata.fillValue) 
            : boost::any_cast<int>(metadata.fillValue);
        j["filters"] = metadata.filters;
        j["order"] = metadata.order;
        j["shape"] = metadata.shape;
        j["zarr_format"] = metadata.zarrFormat;

        fs::path metaFile = handle.path();
        metaFile /= ".zarray";
        fs::ofstream file(metaFile);
        file << std::setw(4) << j << std::endl;
    }

    void readMetadata(
        const handle::Array & handle, ArrayMetadata & metadata
    ) {

        fs::path metaFile = handle.path();
        metaFile /= ".zarray";
        fs::ifstream file(metaFile);
        nlohmann::json j;
        file >> j;

        // make sure that fixed metadata values agree
        std::string order = j["order"];
        if(order != "C") {
            throw std::runtime_error(
                "Invalid Order: Zarr++ only supports C order"
            );
        }

        int zarrFormat = j["zarr_format"];
        if(zarrFormat != 2) {
            throw std::runtime_error(
                "Invalid Zarr format: Zarr++ only supports zarr format 2"
            );
        }

        if(!j["filters"].is_null()) {
            throw std::runtime_error(
                "Invalid Filters: Zarr++ does not support filters"
            );
        }

        // dtype
        metadata.dtype = types::parseDtype(j["dtype"]);

        // set shapes
        types::ShapeType shape(j["shape"].begin(), j["shape"].end());
        metadata.shape = shape;
        types::ShapeType chunkShape(j["chunkShape"].begin(), j["chunkShape"].end());
        metadata.chunkShape = chunkShape;

        // set compression options
        const auto & compressor = j["compressor"];
        metadata.compressorLevel = compressor["clevel"];
        metadata.compressorName = compressor["cname"];
        metadata.compressorId = compressor["id"];
        metadata.compressorShuffle = compressor["shuffle"];

        // TODO support undefined fill-values (encoded by 'null')
        // set fill value
        if(j["fill_value"].is_null()) {
            throw std::runtime_error("Invalid fill_value: Zarr++ does not support null");
        }
        metadata.fillValue = j["fill_value"];
    }

} // namespace::zarr
