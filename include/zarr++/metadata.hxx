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
                fillValue(static_cast<double>(fillValue)),
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

        // FIXME boost::any isn't doing what it's supposed to :(
        // for now we just store the fill value as double as a hacky workaround
        //boost::any fillValue;
        double fillValue;

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

        // FIXME boost::any isn't doing what it's supposed to :(
        //j["fill_value"] = types::isRealType(metadata.dtype) ? boost::any_cast<float>(metadata.fillValue)
        //    : (types::isUnsignedType(metadata.dtype) ? boost::any_cast<uint64_t>(metadata.fillValue)
        //        : boost::any_cast<int64_t>(metadata.fillValue));
        j["fill_value"] = metadata.fillValue;

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
        if(!fs::exists(metaFile)) {
            throw std::runtime_error("Invalid path in readMetadata: Does not have a .zarray");
        }

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
        types::ShapeType chunkShape(j["chunks"].begin(), j["chunks"].end());
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

        // FIXME boost::any isn't doing what it's supposed to :(
        //metadata.fillValue = types::isRealType(metadata.dtype) ? static_cast<float>(fillVal)
        //    : (types::isUnsignedType(metadata.dtype) ? static_cast<uint64_t>(fillVal)
        //        : static_cast<int64_t>(fillVal));
        metadata.fillValue = j["fill_value"];
    }

} // namespace::zarr
