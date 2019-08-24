#pragma once
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/Object.h>
#include "z5/handle.hxx"


namespace z5 {
namespace s3 {
namespace handle {

    // TODO implement for s3

    class File : public z5::handle::File<File> {
    public:
        typedef z5::handle::File<File> BaseType;

        // TODO support files at keys in bucket
        // for now we only support vanilla SDKOptions
        File(const std::string & bucketName, const FileMode mode=FileMode())
            : BaseType(mode),
              bucketName_(bucketName.c_str(), bucketName.size()),
              options_(){
            Aws::InitAPI(options_);
        }

        ~File() {
            Aws::ShutdownAPI(options_);
        }

        // Implement the handle API
        inline bool isS3() const {return true;}
        inline bool isGcs() const {return false;}
        inline bool exists() const {}
        inline bool isZarr() const {}
        const fs::path & path() const {}

        inline void create() const {
            if(!mode().canCreate()) {
                const std::string err = "Cannot create new file in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            // make sure that the file does not exist already
            if(exists()) {
                throw std::invalid_argument("Creating new file failed because it already exists.");
            }
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove file in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            if(!exists()) {
                throw std::invalid_argument("Cannot remove non-existing file.");
            }
        }

        // Implement the group handle API
        inline void keys(std::vector<std::string> & out) const {
            Aws::S3::S3Client client;
            Aws::S3::Model::ListObjectsRequest request;
            request.WithBucket(bucketName_);

            const auto object_list = client.ListObjects(request);

            if(object_list.IsSuccess()) {
                const auto list = object_list.GetResult().GetContents();
                for(const auto & obj : list) {
                    auto key = obj.GetKey();
                    out.emplace_back(key.c_str(), key.size());
                }
            } else {
                // TODO handle !IsSuccess properly
                throw std::runtime_error("No success");
            }

        }
        inline bool in(const std::string & key) const {
        }

    private:
        Aws::SDKOptions options_;
        Aws::String bucketName_;
    };


    class Group : public z5::handle::Group<Group> {
    public:
        typedef z5::handle::Group<Group> BaseType;

        template<class GROUP>
        Group(const z5::handle::Group<GROUP> & group, const std::string & key)
            : BaseType(group.mode()) {
        }

        // Implement th handle API
        inline bool isS3() const {return true;}
        inline bool isGcs() const {return false;}
        inline bool exists() const {}
        inline bool isZarr() const {}
        const fs::path & path() const {}

        inline void create() const {
            if(mode().mode() == FileMode::modes::r) {
                const std::string err = "Cannot create new group in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            // make sure that the file does not exist already
            if(exists()) {
                throw std::invalid_argument("Creating new group failed because it already exists.");
            }
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove group in group mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            if(!exists()) {
                throw std::invalid_argument("Cannot remove non-existing group.");
            }
        }

        // Implement the group handle API
        inline void keys(std::vector<std::string> & out) const {
        }
        inline bool in(const std::string & key) const {
        }
    };


    class Dataset : public z5::handle::Dataset<Dataset> {
    public:
        typedef z5::handle::Dataset<Dataset> BaseType;

        template<class GROUP>
        Dataset(const z5::handle::Group<GROUP> & group, const std::string & key)
            : BaseType(group.mode()) {
        }

        // Implement th handle API
        inline bool isS3() const {return true;}
        inline bool isGcs() const {return false;}
        inline bool exists() const {}
        inline bool isZarr() const {}
        const fs::path & path() const {}

        inline void create() const {
            // check if we have permissions to create a new dataset
            if(mode().mode() == FileMode::modes::r) {
                const std::string err = "Cannot create new dataset in mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            // make sure that the file does not exist already
            if(exists()) {
                throw std::invalid_argument("Creating new dataset failed because it already exists.");
            }
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove dataset in dataset mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            if(!exists()) {
                throw std::invalid_argument("Cannot remove non-existing dataset.");
            }
        }
    };


    class Chunk : public z5::handle::Chunk<Chunk> {
    public:
        typedef z5::handle::Chunk<Chunk> BaseType;

        Chunk(const Dataset & ds,
              const types::ShapeType & chunkIndices,
              const types::ShapeType & chunkShape,
              const types::ShapeType & shape) : BaseType(chunkIndices, chunkShape, shape, ds.mode()),
                                                dsHandle_(ds){}

        // make the top level directories for a n5 chunk
        inline void create() const {
        }

        inline void remove() const {
        }

        inline const Dataset & datasetHandle() const {
            return dsHandle_;
        }

        inline bool isZarr() const {
            return dsHandle_.isZarr();
        }

        inline bool exists() const {}
        const fs::path & path() const {}

        inline bool isS3() const {return true;}
        inline bool isGcs() const {return false;}

    private:

        const Dataset & dsHandle_;
    };


}
}
}
