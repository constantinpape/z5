#pragma once

#include <string>

#ifndef BOOST_FILESYSTEM_NO_DEPERECATED
#define BOOST_FILESYSTEM_NO_DEPERECATED
#endif
#include <boost/filesystem.hpp>

#include "z5/util/util.hxx"
#include "z5/types/types.hxx"

namespace fs = boost::filesystem;

namespace z5 {
namespace handle {

    class Handle {

    public:
        Handle(const std::string & pathOnFilesystem) :
            pathOnFilesystem_(pathOnFilesystem) {
        }

        Handle(const fs::path & pathOnFilesystem) :
            pathOnFilesystem_(pathOnFilesystem) {
        }

        // check if the file managed by this handle exists
        virtual bool exists() const {
            return fs::exists(pathOnFilesystem_);
        }

        virtual const fs::path & path() const {
            return pathOnFilesystem_;
        }

        virtual bool createDir() const {
            return fs::create_directory(pathOnFilesystem_);
        }

        virtual bool isZarrArray() const {
            fs::path tmp(pathOnFilesystem_);
            tmp /= ".zarray";
            return fs::exists(tmp);
        }

        virtual bool isZarrGroup() const {
            fs::path tmp(pathOnFilesystem_);
            tmp /= ".zgroup";
            return fs::exists(tmp);
        }

        virtual bool isZarr() const {
            return isZarrArray() || isZarrGroup();
        }

        virtual bool isN5() const {
            fs::path tmp(pathOnFilesystem_);
            tmp /= "attributes.json";
            return fs::exists(tmp);
        }

    private:
        fs::path pathOnFilesystem_;

    };

    class Dataset : public Handle {

    public:
        Dataset(const std::string & pathOnFilesystem_)
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
        Chunk(const fs::path & pathOnFilesystem, const bool zarrFormat=true)
            : Handle(pathOnFilesystem), chunkIndices_(indicesFromPath(pathOnFilesystem, zarrFormat))  , zarrFormat_(zarrFormat) {
        }

        Chunk(const Dataset & handle, const types::ShapeType & chunkIndices, const bool zarrFormat=true)
            : Handle(pathFromDatasetAndIndices(handle, chunkIndices, zarrFormat)), chunkIndices_(chunkIndices), zarrFormat_(zarrFormat){

        }

        const types::ShapeType & chunkIndices() const {
            return chunkIndices_;
        }

        fs::path pathFromDataset(const Dataset & handle) const {
            return pathFromDatasetAndIndices(handle, chunkIndices_, zarrFormat_);
        }

    private:

        // static method to produce the filesystem path from parent handle,
        // chunk indices and flag for the format
        static fs::path pathFromDatasetAndIndices(
            const Dataset & handle,
            const types::ShapeType & chunkIndices,
            const bool zarrFormat
        ) {
            fs::path ret(handle.path());

            // if we have the zarr-format, chunk indices
            // are seperated by a '.'
            if(zarrFormat) {
				std::string name;
                std::string delimiter = ".";
                util::join(chunkIndices.begin(), chunkIndices.end(), name, delimiter);
                ret /= name;
            }

            // otherwise (n5-format), each chunk index has
            // its own directory
            else {
                for(auto idx : chunkIndices) {
                    ret /= std::to_string(idx);
                }
            }
            return ret;
        }


        // static method to get the chunk indices from the path on fs
        static types::ShapeType indicesFromPath(const fs::path & path, const bool zarrFormat) {

            types::ShapeType ret;
            // if we have zarr format, the chunk is stored as "Z.Y.X"
            if(zarrFormat) {
                auto name = path.leaf().string();
                std::string delimiter = ".";
                util::split(name, delimiter, ret);
            }

            // else (n5-format), the chunk is stored as "Z/Y/X"
            else {
                // reverse iterate the path
                for(auto it = path.rbegin(); it != path.rend(); ++it) {
                    auto name = it->leaf().string();
                    // we stop going up the path, as soon as the filename is not all names
                    if(!std::all_of(name.begin(), name.end(), ::isdigit)) {
                        break;
                    }
                    ret.push_back(
                        static_cast<size_t>(std::stoull(name))
                    );
                }
                // reverse the return vector
                std::reverse(ret.begin(), ret.end());
            }

            return ret;
        }


        types::ShapeType chunkIndices_;
        bool zarrFormat_;
    };

} // namespace::handle
} // namespace::zarr
