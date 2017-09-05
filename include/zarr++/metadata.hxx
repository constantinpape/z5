#pragma once

#include <string>
#include <vector>
#include "json.hpp"

#include "zarr++/handle/handle.hxx"
#include "zarr++/types/types.hxx"

namespace zarr {

    class Metadata {

    public:
        Metadata(const int zarr_format) : zarr_format_(zarr_format) {

        }

    private:
        int zarr_format_;
    };

    class ArrayMetadata : public Metadata {

    public:

    private:
        types::ShapeType shape_;
        types::ShapeType chunks_;
        // FIXME this should be defined in types
        std::string dtype_;
        nlohman::json::json compressor_;
        // TODO
        FillValue fill_value_;
        std::string order_;
        nlohman::json::json filter_;
    }

    void writeMetaData(const handle::Handle & handle, const Metadata & metadata) {

    }

    void readMetaData(const handle::Handle & handle, Metadata & metadata) {

    }

} // namespace::zarr
