#pragma once
#include "z5/handle.hxx"


namespace z5 {
namespace gcs {

namespace detail {
    // The gcs backend is a scaffold: every operation that would need to talk to
    // google cloud storage fails loudly. (The previous stubs were value-returning
    // functions without a return statement - undefined behavior when called.)
    [[noreturn]] inline void notImplemented() {
        throw std::runtime_error("z5: the gcs backend is not implemented");
    }
}

namespace handle {

    // TODO implement for gcs
    class GcsHandleImpl {
    public:
        inline const std::string & bucketNameImpl() const {
            detail::notImplemented();
        }

        inline const std::string & nameInBucketImpl() const {
            detail::notImplemented();
        }

    };

    class File : public z5::handle::File<File>, private GcsHandleImpl {
    public:
        typedef z5::handle::File<File> BaseType;

        File(const FileMode mode=FileMode())
            : BaseType(mode) {
        }

        // Implement the handle API
        inline bool isS3() const {return false;}
        inline bool isGcs() const {return true;}
        inline bool exists() const {detail::notImplemented();}
        inline bool isZarr() const {detail::notImplemented();}
        const fs::path & path() const {detail::notImplemented();}
        inline const std::string & bucketName() const {return bucketNameImpl();}
        inline const std::string & nameInBucket() const {return nameInBucketImpl();}

        inline void create() const {
            if(!mode().canCreate()) {
                const std::string err = "Cannot create new file in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            detail::notImplemented();
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove file in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            detail::notImplemented();
        }

        // Implement the group handle API
        inline void keys(std::vector<std::string> & out) const {
            detail::notImplemented();
        }
        inline bool in(const std::string & key) const {
            detail::notImplemented();
        }
    };


    class Group : public z5::handle::Group<Group>, private GcsHandleImpl {
    public:
        typedef z5::handle::Group<Group> BaseType;

        template<class GROUP>
        Group(const z5::handle::Group<GROUP> & group, const std::string & key)
            : BaseType(group.mode()) {
        }

        // Implement th handle API
        inline bool isS3() const {return false;}
        inline bool isGcs() const {return true;}
        inline bool exists() const {detail::notImplemented();}
        inline bool isZarr() const {detail::notImplemented();}
        const fs::path & path() const {detail::notImplemented();}
        inline const std::string & bucketName() const {return bucketNameImpl();}
        inline const std::string & nameInBucket() const {return nameInBucketImpl();}

        inline void create() const {
            if(mode().mode() == FileMode::modes::r) {
                const std::string err = "Cannot create new group in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            detail::notImplemented();
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove group in group mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            detail::notImplemented();
        }

        // Implement the group handle API
        inline void keys(std::vector<std::string> & out) const {
            detail::notImplemented();
        }
        inline bool in(const std::string & key) const {
            detail::notImplemented();
        }
    };


    class Dataset : public z5::handle::Dataset<Dataset>, private GcsHandleImpl {
    public:
        typedef z5::handle::Dataset<Dataset> BaseType;

        template<class GROUP>
        Dataset(const z5::handle::Group<GROUP> & group, const std::string & key)
            : BaseType(group.mode()) {
        }

        // Implement th handle API
        inline bool isS3() const {return false;}
        inline bool isGcs() const {return true;}
        inline bool exists() const {detail::notImplemented();}
        inline bool isZarr() const {detail::notImplemented();}
        const fs::path & path() const {detail::notImplemented();}
        inline const std::string & bucketName() const {return bucketNameImpl();}
        inline const std::string & nameInBucket() const {return nameInBucketImpl();}

        inline void create() const {
            // check if we have permissions to create a new dataset
            if(mode().mode() == FileMode::modes::r) {
                const std::string err = "Cannot create new dataset in mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            detail::notImplemented();
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove dataset in dataset mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            detail::notImplemented();
        }
    };


    class Chunk : public z5::handle::Chunk<Chunk>, private GcsHandleImpl {
    public:
        typedef z5::handle::Chunk<Chunk> BaseType;

        Chunk(const Dataset & ds,
              const types::ShapeType & chunkIndices,
              const types::ShapeType & chunkShape,
              const types::ShapeType & shape) : BaseType(chunkIndices, chunkShape, shape, ds.mode()),
                                                dsHandle_(ds){}

        // make the top level directories for a n5 chunk
        inline void create() const {
            detail::notImplemented();
        }

        inline const Dataset & datasetHandle() const {
            return dsHandle_;
        }

        inline bool isZarr() const {
            return dsHandle_.isZarr();
        }

        inline bool exists() const {detail::notImplemented();}
        const fs::path & path() const {detail::notImplemented();}

        inline void remove() const {
            detail::notImplemented();
        }

        inline bool isS3() const {return false;}
        inline bool isGcs() const {return true;}
        inline const std::string & bucketName() const {return bucketNameImpl();}
        inline const std::string & nameInBucket() const {return nameInBucketImpl();}

    private:

        const Dataset & dsHandle_;
    };


}
}
}
