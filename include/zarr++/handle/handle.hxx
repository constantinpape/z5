#pragma once

#include <string>
#include <sys/stat.h>

namespace zarr {
namespace handle {

    class Handle {

        Handle(const std::string & pathOnFilesystem) :
            pathOnFilesystem_(pathOnFilesystem) {
        }

        // check if the file managed by this handle exists
        bool exists() {
            // compare SO:
            //https://stackoverflow.com/questions/12774207/fastest-way-to-check-if-a-file-exist-using-standard-c-c11-c
            struct stat buffer;
            return (stat (pathOnFilesystem_.c_str(), &buffer) == 0;
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

} // namespace::handle
} // namespace::zarr
