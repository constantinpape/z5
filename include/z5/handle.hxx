#pragma once

#include <set>
#include "z5/types/types.hxx"
#include "z5/util/file_mode.hxx"
#include "z5/util/util.hxx"

namespace z5 {
namespace handle {

    // TODO this should go into docstrings and then a documentation should be generated from them
    // (using doxygen?!)
    /*
     * This header includes the base classes for the z5 handle objects.
     * The handles represent the different objects in the zarr/n5 spec:
     * - File: the root group (= top level directory) for a zarr/n5 container
     * - Group: a directory that is not a dataset, i.e. does not contain nd-data
     * - Dataset: a directory that contains nd-data stored in chunks (individual files)
     * - Chunk: an individual chunk file in a dataset
     *
     * The common functionality for all handles is defined in the class Handle.
     * It includes checking whether the current object is part of a zarr or an n5 container,
     * checking if the object exists, creating or removing the object, etc.
     *
     * The handles are passed to the functions in the header "factory.hxx", which should be
     * used to create or load groups / datasets and to the functions in the header "attributes.hxx",
     * which enable reading and writing additional metadata.
     *
     * All classes defined in this header are abstract and they need to be implemented to provide
     * an implementation for a specific backend.
     * For example, "filesystem/handle.hxx" contains the implementations that are used to represent
     * zarr/n5 containers on the local filesystem, "s3/handle.hxx" the implementations for an s3 object store
     * (still work in progress) and "gcs/handle.hxx" the implementations for a google cloud object store (mock implementation).
     *
     * Note that for the datasets, additional logic is defined in the header "dataset.hxx", which also has to
     * be implemented for each backend, see for example "filesystem/dataset.hxx".
     */

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

        // S3 (object-store) client configuration. These have sensible defaults so
        // the filesystem / gcs backends don't need to implement them; the s3 handles
        // override them and propagate the config down the handle hierarchy (the same
        // way bucketName() / nameInBucket() are propagated).
        virtual const std::string & endpoint() const {
            static const std::string empty;
            return empty;
        }
        virtual const std::string & region() const {
            static const std::string defaultRegion = "us-east-1";
            return defaultRegion;
        }
        virtual bool anon() const {
            return false;
        }
        virtual const std::string & accessKey() const {
            static const std::string empty;
            return empty;
        }
        virtual const std::string & secretKey() const {
            static const std::string empty;
            return empty;
        }

        const FileMode & mode() const {
            return mode_;
        }

    private:
        FileMode mode_;
    };


    //
    // We use CRTP design for Group, File, Dataset and Chunk handles
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
        Dataset(const FileMode mode, const std::string zarrDelimiter=".",
                const int zarrFormat=2, const std::string chunkKeyEncoding="default")
            : Handle(mode), zarrDelimiter_(zarrDelimiter),
              zarrFormat_(zarrFormat), chunkKeyEncoding_(chunkKeyEncoding){}
        virtual ~Dataset() {}

        const std::string & zarrDelimiter() const {return zarrDelimiter_;}
        int zarrFormat() const {return zarrFormat_;}
        const std::string & chunkKeyEncoding() const {return chunkKeyEncoding_;}

    private:
        std::string zarrDelimiter_;
        int zarrFormat_;
        std::string chunkKeyEncoding_;
    };


    template<class CHUNK>
    class Chunk : public Handle {
    public:
        // the shapes are stored BY VALUE: chunk handles are frequently constructed
        // from temporaries (e.g. the shard coordinate returned by util::shardId), so
        // reference members would dangle as soon as the constructing statement ends
        Chunk(const types::ShapeType & chunkIndices,
              const types::ShapeType & defaultShape,
              const types::ShapeType & datasetShape,
              const FileMode mode) : Handle(mode),
                                     chunkIndices_(chunkIndices),
                                     defaultShape_(defaultShape),
                                     datasetShape_(datasetShape),
                                     boundedShape_(computeBoundedShape()){}
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
            return std::accumulate(boundedShape_.begin(), boundedShape_.end(), std::size_t(1), std::multiplies<std::size_t>());
        }

        inline const types::ShapeType & defaultShape() const {
            return defaultShape_;
        }

        inline std::size_t defaultSize() const {
            return std::accumulate(defaultShape_.begin(), defaultShape_.end(), std::size_t(1), std::multiplies<std::size_t>());
        }

    protected:
        inline std::string getChunkKey(const bool isZarr, const std::string & zarrDelimiter=".",
                                       const int zarrFormat=2,
                                       const std::string & chunkKeyEncoding="default") const {
            const auto & indices = chunkIndices();
			std::string name;

            if(isZarr) {
                if(zarrFormat == 3 && chunkKeyEncoding == "default") {
                    // zarr v3 default encoding: chunk keys are nested under 'c' and
                    // joined by the separator (default '/'), e.g. "c/0/1/2"
                    name = "c";
                    for(const auto idx : indices) {
                        name += zarrDelimiter + std::to_string(idx);
                    }
                } else {
                    // zarr v2 (or zarr v3 "v2" chunk key encoding): flat key joined
                    // by the delimiter, e.g. "0.1.2"
                    util::join(indices.begin(), indices.end(), name, zarrDelimiter);
                }
            }

            // in n5 each chunk index has its own directory, i.e. the delimiter is '/'
            else {
                std::string delimiter = "/";
                // N5-Axis order: we need to read the chunks in reverse order
                util::join(indices.rbegin(), indices.rend(), name, delimiter);
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
        types::ShapeType chunkIndices_;
        types::ShapeType defaultShape_;
        types::ShapeType datasetShape_;
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
