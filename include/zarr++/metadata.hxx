#pragma once

#include <string>
#include <vector>
#include "json.hpp"

#include "zarr++/handles.hxx"

namespace zarr {

    struct Metadata {
        typedef std::vector<size_t> ShapeType;
    
    private:
        ShapeType shape_;
    };

    void writeMetaData(const Handle & handle, const Metadata & metadata) {
        
    }

} // namespace::zarr
