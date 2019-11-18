#pragma once

#include <string>

#include "z5/handle.hxx"
#include "z5/types/types.hxx"


namespace z5 {
namespace filesystem {
namespace handle {


    class HandleImpl {

    public:
        HandleImpl(const fs::path & path) : path_(path) {}
        ~HandleImpl() {}

        // check if the file managed by this handle exists
        inline bool pathExists() const {
            return fs::exists(path_);
        }

        inline const fs::path & getPath() const {
            return path_;
        }

        inline bool createDir() const {
            return fs::create_directories(path_);
        }

        inline bool isZarrDataset() const {
            if(!pathExists()) {
               throw std::runtime_error("Cannot infer zarr format because the dataset has not been created yet.");
            }
            return fs::exists(path_ / ".zarray");
        }

        inline bool isZarrGroup() const {
            if(!pathExists()) {
               throw std::runtime_error("Cannot infer zarr format because the group has not been created yet.");
            }
            return fs::exists(path_ / ".zgroup");
        }

        inline void listSubDirs(std::vector<std::string> & out) const {
            for(const auto & p : fs::directory_iterator(path_)) {
                const auto & elem = p.path();
                if(fs::is_directory(elem)) {
                    out.emplace_back(elem.filename().string());
                }
            }
        }

        inline bool elementExists(const std::string & name) const {
            fs::path p = path_ / name;
            return fs::exists(p) && fs::is_directory(p);
        }

        inline void removeDir() const {
            fs::remove_all(path_);
        }

        inline const std::string & dummy() const {
            return dummy_;
        }

    private:
        fs::path path_;
        // empty string for dummy return value
        std::string dummy_;
    };


    class Group : public z5::handle::Group<Group>, private HandleImpl {
    public:
        typedef z5::handle::Group<Group> BaseType;
        typedef Group GroupType;

        template<class GROUP>
        Group(const z5::handle::Group<GROUP> & group, const std::string & key)
            : BaseType(group.mode()), HandleImpl(group.path() / key) {
        }

        // Implement th handle API
        inline bool isS3() const {return false;}
        inline bool isGcs() const {return false;}
        inline bool exists() const {return pathExists();}
        inline bool isZarr() const {return isZarrGroup();}
        inline const fs::path & path() const {return getPath();}

        inline void create() const {
            if(mode().mode() == FileMode::modes::r) {
                const std::string err = "Cannot create new group in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            if(exists()) {
                throw std::invalid_argument("Creating new group failed because it already exists.");
            }
            createDir();
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove group in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            if(!exists()) {
                throw std::invalid_argument("Cannot remove non-existing group.");
            }
            removeDir();

        }

        // Implement the group handle API
        inline void keys(std::vector<std::string> & out) const {
            listSubDirs(out);
        }
        inline bool in(const std::string & key) const {
            return elementExists(key);
        }

        // dummy implementation
        const std::string & bucketName() const {return dummy();}
        const std::string & nameInBucket() const {return dummy();}
    };


    class File : public z5::handle::File<File>, private HandleImpl {
    public:
        typedef z5::handle::File<File> BaseType;
        typedef Group GroupType;

        File(const fs::path & path, const FileMode mode=FileMode())
            : BaseType(mode), HandleImpl(path) {
        }

        // Implement th handle API
        inline bool isS3() const {return false;}
        inline bool isGcs() const {return false;}
        inline bool exists() const {return pathExists();}
        inline bool isZarr() const {return isZarrGroup();}
        inline const fs::path & path() const {return getPath();}

        inline void create() const {
            if(!mode().canCreate()) {
                const std::string err = "Cannot create new file in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            if(exists()) {
                throw std::invalid_argument("Creating new file failed because it already exists.");
            }
            createDir();
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove file in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            if(!exists()) {
                throw std::invalid_argument("Cannot remove non-existing file.");
            }
            removeDir();
        }

        // Implement the group handle API
        inline void keys(std::vector<std::string> & out) const {
            listSubDirs(out);
        }
        inline bool in(const std::string & key) const {
            return elementExists(key);
        }

        // dummy implementation
        const std::string & bucketName() const {return dummy();}
        const std::string & nameInBucket() const {return dummy();}
    };


    class Dataset : public z5::handle::Dataset<Dataset>, private HandleImpl {
    public:
        typedef z5::handle::Dataset<Dataset> BaseType;

        template<class GROUP>
        Dataset(const z5::handle::Group<GROUP> & group, const std::string & key)
            : BaseType(group.mode()), HandleImpl(group.path() / key) {
        }

        Dataset(const fs::path & path, const FileMode & mode)
            : BaseType(mode), HandleImpl(path) {}

        inline bool isS3() const {return false;}
        inline bool isGcs() const {return false;}
        inline bool exists() const {return pathExists();}
        inline bool isZarr() const {return isZarrDataset();}
        inline const fs::path & path() const {return getPath();}

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
            createDir();
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove dataset in dataset mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            if(!exists()) {
                throw std::invalid_argument("Cannot remove non-existing dataset.");
            }
            removeDir();
        }

        // dummy implementation
        const std::string & bucketName() const {return dummy();}
        const std::string & nameInBucket() const {return dummy();}
    };


    class Chunk : public z5::handle::Chunk<Chunk> {
    public:
        typedef z5::handle::Chunk<Chunk> BaseType;

        Chunk(const Dataset & ds,
              const types::ShapeType & chunkIndices,
              const types::ShapeType & chunkShape,
              const types::ShapeType & shape) : BaseType(chunkIndices, chunkShape, shape, ds.mode()),
                                                dsHandle_(ds),
                                                path_(ds.path() / getChunkKey(ds.isZarr())){}

        // make the top level directories for a n5 chunk
        inline void create() const {
            // don't need to do anything for zarr format
            if(dsHandle_.isZarr()) {
                return;
            }

            fs::path root = path_.parent_path();
            if(!fs::exists(root)) {
                fs::create_directories(root);
            }
        }

        inline const Dataset & datasetHandle() const {
            return dsHandle_;
        }

        inline bool isZarr() const {
            return dsHandle_.isZarr();
        }

        inline const fs::path & path() const {
            return path_;
        }

        inline bool exists() const {
            return fs::exists(path_);
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove chunk in mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            fs::remove(path_);
        }

        inline bool isS3() const {return false;}
        inline bool isGcs() const {return false;}

        // dummy implementation
        const std::string & bucketName() const {return dummy_;}
        const std::string & nameInBucket() const {return dummy_;}

    private:

        const Dataset & dsHandle_;
        fs::path path_;
        // empty string for dummy return value
        std::string dummy_;
    };

} // namespace::handle
} // namespace::fs
} // namespace::z5
