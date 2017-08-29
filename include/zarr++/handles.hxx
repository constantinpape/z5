#pragma once

#include <string>

namespace zarr {

    class Handle {

        Handle(const std::string & pathOnFilesystem) :
            pathOnFilesystem_(pathOnFilesystem) {
        }

        const std::string & path() const {
            return pathOnFilesystem_;
        }

    private:
        std::string pathOnFilesystem_;

    };

    class Array : public Handle {

    };

    class Group : public Handle {

    };

    class Chunk : public Handle {

    };

} // namespace::zarr
