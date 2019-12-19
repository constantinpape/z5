#pragma once

#include <set>
#include "z5/types/types.hxx"
#include "z5/util/file_mode.hxx"
#include "z5/util/util.hxx"

namespace z5 {
namespace handle {


    class Handle {
    public:
        Handle(const FileMode mode) : mode_(mode){}
        virtual ~Handle() {}

        // must impl API for all Handles

        virtual bool isZarr() const = 0;
        virtual bool isS3() const = 0;
        virtual bool isGcs() const = 0;

        virtual bool exists() const = 0;
        virtual void create() const = 0;
        virtual void remove() const = 0;

        virtual const fs::path & path() const = 0;
        virtual const std::string & bucketName() const = 0;
        virtual const std::string & nameInBucket() const = 0;

        const FileMode & mode() const {
            return mode_;
        }

    private:
        FileMode mode_;
    };


    //
    // We use CRTP desing for Group, File, Dataset and Chunk handles
    // in order to infer the handle type from the base class
    // via 'derived_cast'. or call the derived class implementations (for Chunk)
    //

    template<class GROUP>
    class Group : public Handle {
    public:
        Group(const FileMode mode) : Handle(mode){}
        virtual ~Group() {}

        // must impl API for group handles
        virtual void keys(std::vector<std::string> &) const = 0;
        virtual bool in(const std::string &) const = 0;
    };


    template<class GROUP>
    class File : public Group<GROUP> {
    public:
        File(const FileMode mode) : Group<GROUP>(mode){}
        virtual ~File() {}
    };


    template<class DATASET>
    class Dataset : public Handle {
    public:
        Dataset(const FileMode mode) : Handle(mode){}
        virtual ~Dataset() {}
    };


    template<class CHUNK>
    class Chunk : public Handle {
    public:
        Chunk(const types::ShapeType & chunkIndices,
              const types::ShapeType & defaultShape,
              const types::ShapeType & datasetShape,
              const FileMode mode) : chunkIndices_(chunkIndices),
                                     defaultShape_(defaultShape),
                                     datasetShape_(datasetShape),
                                     boundedShape_(computeBoundedShape()),
                                     Handle(mode){}
        virtual ~Chunk() {}

        // expose relevant part of the derived's class API
        inline bool exists() const {
            return static_cast<const CHUNK &>(*this).exists();
        }

        inline void create() const {
            static_cast<const CHUNK &>(*this).create();
        }

        inline bool isZarr() const {
            return static_cast<const CHUNK &>(*this).isZarr();
        }

        // API implemented in Base

        inline const types::ShapeType & chunkIndices() const {
            return chunkIndices_;
        }

        inline const types::ShapeType & shape() const {
            return boundedShape_;
        }

        inline std::size_t size() const {
            return std::accumulate(boundedShape_.begin(), boundedShape_.end(), 1, std::multiplies<std::size_t>());
        }

        inline const types::ShapeType & defaultShape() const {
            return defaultShape_;
        }

        inline std::size_t defaultSize() const {
            return std::accumulate(defaultShape_.begin(), defaultShape_.end(), 1, std::multiplies<std::size_t>());
        }

    protected:
        inline std::string getChunkKey(const bool isZarr) const {
            const auto & indices = chunkIndices();
			std::string name;

            // if we have the zarr-format, chunk indices
            // are seperated by a '.'
            if(isZarr) {
                std::string delimiter = ".";
                util::join(indices.begin(), indices.end(), name, delimiter);
            }

            // otherwise (n5-format), each chunk index has
            // its own directory
            else {
                // N5-Axis order: we need to read the chunks in reverse order
                for(auto it = indices.rbegin(); it != indices.rend(); ++it) {
                    name += ("/" + std::to_string(*it));
                }
            }
            return name;
        }


    private:
        // compute the chunk shape, clipped if overhanging the boundary
        inline types::ShapeType computeBoundedShape() const {
            const int ndim = defaultShape_.size();
            types::ShapeType out(ndim);
            for(int d = 0; d < ndim; ++d) {
                out[d] = ((chunkIndices_[d] + 1) * defaultShape_[d] <= datasetShape_[d]) ? defaultShape_[d] :
                    datasetShape_[d] - chunkIndices_[d] * defaultShape_[d];
            }
            return out;
        }

    private:
        const types::ShapeType & chunkIndices_;
        const types::ShapeType & defaultShape_;
        const types::ShapeType & datasetShape_;
        types::ShapeType boundedShape_;
    };


    inline bool hasAllN5DatasetAttributes(nlohmann::json & j) {
        const std::set<std::string> protectedAttributes = {"dimensions", "blockSize", "dataType",
                                                           "compressionType", "compression"};
        int nFound = 0;
        for(auto jIt = j.begin(); jIt != j.end(); ++jIt) {
            if(protectedAttributes.find(jIt.key()) != protectedAttributes.end()) {
                ++nFound;
            }
        }
        // NOTE we only expect to find one of "compressionType" and "compression"
        return nFound == (protectedAttributes.size() - 1);
    }
}
}
