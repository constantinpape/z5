#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "z5/handle.hxx"
#include "z5/attributes.hxx"
#include "z5/factory.hxx"

#include "z5/filesystem/handle.hxx"
#include "z5/filesystem/metadata.hxx"

#ifdef WITH_S3
#include "z5/s3/handle.hxx"
#include "z5/s3/metadata.hxx"
#endif

namespace py = pybind11;

namespace z5 {


    template<class GROUP, class GROUP1, class DATASET>
    auto getGroupHandle(py::module & m, const std::string & name) {
        py::class_<GROUP> g(m, name.c_str());
        g
            .def("exists", &GROUP::exists)
            .def("has", &GROUP::in)
            .def("keys", [](const GROUP & self){
                std::vector<std::string> keys;
                self.keys(keys);
                return keys;
            })
            .def("path", [](const GROUP & self){return self.path().string();})
            .def("mode", &GROUP::mode)
            .def("is_zarr", &GROUP::isZarr)
            .def("is_sub_group", [](const GROUP & self, const std::string & name){
                return isSubGroup(self, name);
            })
            .def("relative_path", [](const GROUP & self, const GROUP & to){
                return relativePath(self, to);
            })
            .def("relative_path", [](const GROUP & self, const GROUP1 & to){
                return relativePath(self, to);
            })
            .def("relative_path", [](const GROUP & self, const DATASET & to){
                return relativePath(self, to);
            })
            .def("remove", &GROUP::remove)
            .def("get_dataset_handle", [](const GROUP & self, const std::string & name){
                return DATASET(self, name);
            })
        ;
        return g;
    }


    void exportFilesystem(py::module & m) {
        typedef filesystem::handle::File File;
        typedef filesystem::handle::Group Group;
        typedef filesystem::handle::Dataset Dataset;

        auto g = getGroupHandle<Group, File, Dataset>(m, "Group");
        g
            .def(py::init<Group, const std::string &>(),
                 py::arg("group"), py::arg("key"))
            .def(py::init<File, const std::string &>(),
                 py::arg("file"), py::arg("key"))
            .def(py::pickle(
                // __getstate__ -> we simply pickle the path,
                // the rest will be read from the attributes
                [](const Group & self) {
                    return py::make_tuple(self.path().string(), self.mode().mode());
                },

                // __setstate__
                [](py::tuple tup) {
                    if(tup.size() != 2) { // the serialization size is 2, because we pickle path and mode
                        throw std::runtime_error("Invalid state for unpickling handle.");
                    }
                    fs::path path(tup[0].cast<std::string>());
                    FileMode mode(tup[1].cast<FileMode::modes>());
                    return filesystem::handle::Group(path, mode);
                }
            ))
        ;

        auto f = getGroupHandle<File, Group, Dataset>(m, "File");
        f
            .def(py::init<const std::string &, FileMode>(),
                 py::arg("path"), py::arg("mode"))
            .def("read_metadata", [](const File & self){
                nlohmann::json j;
                filesystem::readMetadata(self, j);
                return j.dump();
            })
            .def(py::pickle(
                // __getstate__ -> we simply pickle the path,
                // the rest will be read from the attributes
                [](const File & self) {
                    return py::make_tuple(self.path().string(), self.mode().mode());
                },

                // __setstate__
                [](py::tuple tup) {
                    if(tup.size() != 2) { // the serialization size is 2, because we pickle path and mode
                        throw std::runtime_error("Invalid state for unpickling handle.");
                    }
                    fs::path path(tup[0].cast<std::string>());
                    FileMode mode(tup[1].cast<FileMode::modes>());
                    return filesystem::handle::File(path, mode);
                }
            ))
        ;

        py::class_<Dataset>(m, "DatasetHandle")
            .def(py::init<Group, const std::string &>())
            .def(py::init<File, const std::string &>())
            .def(py::pickle(
                // __getstate__ -> we simply pickle the path,
                // the rest will be read from the attributes
                [](const Dataset & ds) {
                    return py::make_tuple(ds.path().string(), ds.mode().mode());
                },

                // __setstate__
                [](py::tuple tup) {
                    if(tup.size() != 2) { // the serialization size is 2, because we pickle path and mode
                        throw std::runtime_error("Invalid state for unpickling handle.");
                    }
                    fs::path path(tup[0].cast<std::string>());
                    FileMode mode(tup[1].cast<FileMode::modes>());
                    return filesystem::handle::Dataset(path, mode);
                }
            ))
        ;
    }


    // need actual constructor for S3File, but need to know params for s3 first
    #ifdef WITH_S3
    void exportS3(py::module & m) {
        typedef s3::handle::File File;
        typedef s3::handle::Group Group;
        typedef s3::handle::Dataset Dataset;

        auto g = getGroupHandle<Group, File, Dataset>(m, "S3Group");
        g
            .def(py::init<Group, const std::string &>(),
                 py::arg("group"), py::arg("key"))
            .def(py::init<File, const std::string &>(),
                 py::arg("file"), py::arg("key"))
        ;

        auto f = getGroupHandle<File, File, Dataset>(m, "S3File");
        f
            // dummy construnctor
            .def(py::init<const std::string &, const std::string &, FileMode>(),
                 py::arg("bucket_name"), py::arg("name_in_bucket"), py::arg("mode"))
        ;

        py::class_<Dataset>(m, "S3DatasetHandle")
            .def(py::init<Group, const std::string &>())
            .def(py::init<File, const std::string &>())
        ;

    }
    #endif


    void exportHandles(py::module & m) {
        exportFilesystem(m);
        #ifdef WITH_S3
        exportS3(m);
        #endif
    }

}
