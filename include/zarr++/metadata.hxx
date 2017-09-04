#pragma once

#include <string>
#include <vector>
#include "json.hpp"

#include "zarr++/handle/handle.hxx"
#include "zarr++/types/types.hxx"

namespace zarr {

    struct Metadata {

    private:
        types::ShapeType shape_;
    };

    void writeMetaData(const handle::Handle & handle, const Metadata & metadata) {
        
    }

    void readMetaData(const handle::Handle & handle, Metadata & metadata) {

    }

} // namespace::zarr
