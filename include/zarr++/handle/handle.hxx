#pragma once

#include <string>
#include <sys/stat.h>

#define BOOST_FILESYSTEM_NO_DEPERECATED
#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

namespace zarr {
namespace handle {

    class Handle {

    public:
        Handle(const std::string & pathOnFilesystem) :
            pathOnFilesystem_(pathOnFilesystem) {
        }

        // check if the file managed by this handle exists
        virtual bool exists() const {
            return fs::exists(pathOnFilesystem_);
        }

        virtual const fs::path & path() const {
            return pathOnFilesystem_;
        }

    private:
        fs::path pathOnFilesystem_;

    };

    class Array : public Handle {
    
    public:
        Array(const std::string & pathOnFilesystem_)
            : Handle(pathOnFilesystem_) {
        }

    };

    class Group : public Handle {
    
    public:
        Group(const std::string & pathOnFilesystem_)
            : Handle(pathOnFilesystem_) {
        }

    };

    class Chunk : public Handle {

    public:
        Chunk(const std::string & pathOnFilesystem_)
            : Handle(pathOnFilesystem_) {
        }

    };

} // namespace::handle
} // namespace::zarr
