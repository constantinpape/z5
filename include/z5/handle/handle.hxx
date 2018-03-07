#pragma once

#include <string>

#ifndef BOOST_FILESYSTEM_NO_DEPERECATED
#define BOOST_FILESYSTEM_NO_DEPERECATED
#endif
#include <boost/filesystem.hpp>

#include "z5/util/util.hxx"
#include "z5/handle/file_mode.hxx"
#include "z5/types/types.hxx"

namespace fs = boost::filesystem;

namespace z5 {
namespace handle {

    class Handle {

    public:
        Handle(const std::string & pathOnFilesystem, const FileMode::modes mode=FileMode::a) :
            pathOnFilesystem_(pathOnFilesystem), mode_(mode) {
        }

        Handle(const fs::path & pathOnFilesystem, const FileMode::modes mode=FileMode::a) :
            pathOnFilesystem_(pathOnFilesystem), mode_(mode) {
        }

        // check if the file managed by this handle exists
        virtual bool exists() const {
            return fs::exists(pathOnFilesystem_);
        }

        virtual const fs::path & path() const {
            return pathOnFilesystem_;
        }

        virtual bool createDir() const {
            return fs::create_directories(pathOnFilesystem_);
        }

        virtual bool isZarrDataset() const {
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
            return isZarrDataset() || isZarrGroup();
        }

        // TODO check the keys to determine if we have a dataset
        // virtual bool isN5Dataset() const {
        //     fs::path tmp(pathOnFilesystem_);
        //     tmp /= "attributes.json";
        //     if(!fs::exists(tmp)) {
        //         return false;
        //     }
        // }

        virtual const FileMode & mode() const {
            return mode_;
        }


    private:
        fs::path pathOnFilesystem_;
        FileMode mode_;

    };


    class Dataset : public Handle {

    public:
        Dataset(const std::string & pathOnFilesystem_, const FileMode::modes mode=FileMode::a)
            : Handle(pathOnFilesystem_, mode) {
        }

    };


    class Group : public Handle {

    public:
        Group(const std::string & pathOnFilesystem_, const FileMode::modes mode=FileMode::a)
            : Handle(pathOnFilesystem_, mode) {
        }

    };


    class File : public Handle {

    public:
        File(const std::string & pathOnFilesystem_, const FileMode::modes mode=FileMode::a)
            : Handle(pathOnFilesystem_, mode) {
        }

    };


    class Chunk : public Handle {

    public:

        Chunk(const Dataset & handle, const types::ShapeType & chunkIndices, const bool zarrFormat)
            : Handle(pathFromDatasetAndIndices(handle, chunkIndices, zarrFormat)), chunkIndices_(chunkIndices), zarrFormat_(zarrFormat){

        }

        inline const types::ShapeType & chunkIndices() const {
            return chunkIndices_;
        }

        inline fs::path pathFromDataset(const Dataset & handle) const {
            return pathFromDatasetAndIndices(handle, chunkIndices_, zarrFormat_);
        }

        // compute the chunk shape, clipped if overhanging the boundary
        template<typename T> // need to template this to work with arbitrary index out vectors
        inline void boundedChunkShape(
            const types::ShapeType & shape, const types::ShapeType & chunkShape, std::vector<T> & shapeOut
        ) const {
            int nDim = shape.size();
            // we trust that all other dimensions are correct
            if(nDim != chunkIndices_.size()) {
                throw std::runtime_error("z5.Chunk.boundedChunkShape: invalid dimension in request");
            }
            shapeOut.resize(nDim);
            for(int d = 0; d < nDim; ++d) {
                shapeOut[d] = ((chunkIndices_[d] + 1) * chunkShape[d] <= shape[d]) ? chunkShape[d] :
                    shape[d] - chunkIndices_[d] * chunkShape[d];
            }
        }

        // make the top level directories for a n5 chunk
        inline void createTopDir() const {
            // don't need to do anything for zarr format
            if(zarrFormat_) {
                return;
            }

            fs::path topDir = path().parent_path();
            if(!fs::exists(topDir)) {
                fs::create_directories(topDir);
            }
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
                // N5-Axis order: we need to read the chunks in reverse order
                for(auto it = chunkIndices.rbegin(); it != chunkIndices.rend(); ++it) {
                    ret /= std::to_string(*it);
                }
            }
            return ret;
        }


        types::ShapeType chunkIndices_;
        bool zarrFormat_;
    };

} // namespace::handle
} // namespace::zarr
