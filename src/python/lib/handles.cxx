#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include "z5/handle.hxx"
#include "z5/attributes.hxx"
#include "z5/factory.hxx"

#include "z5/filesystem/handle.hxx"
#include "z5/filesystem/metadata.hxx"

#ifdef WITH_S3
#include "z5/s3/handle.hxx"
#include "z5/s3/metadata.hxx"
#endif

namespace nb = nanobind;

namespace z5 {


    template<class GROUP, class GROUP1, class DATASET>
    auto getGroupHandle(nb::module_ & m, const std::string & name) {
        nb::class_<GROUP> g(m, name.c_str());
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


    void exportFilesystem(nb::module_ & m) {
        typedef filesystem::handle::File File;
        typedef filesystem::handle::Group Group;
        typedef filesystem::handle::Dataset Dataset;

        auto g = getGroupHandle<Group, File, Dataset>(m, "Group");
        g
            .def(nb::init<Group, const std::string &>(),
                 nb::arg("group"), nb::arg("key"))
            .def(nb::init<File, const std::string &>(),
                 nb::arg("file"), nb::arg("key"))
        ;

        auto f = getGroupHandle<File, Group, Dataset>(m, "File");
        f
            .def(nb::init<const std::string &, FileMode>(),
                 nb::arg("path"), nb::arg("mode"))
            .def("read_metadata", [](const File & self){
                nlohmann::json j;
                filesystem::readMetadata(self, j);
                return j.dump();
            })
        ;

        nb::class_<Dataset>(m, "DatasetHandle")
            .def(nb::init<Group, const std::string &>())
            .def(nb::init<File, const std::string &>())
        ;
    }


    // need actual constructor for S3File, but need to know params for s3 first
    #ifdef WITH_S3
    void exportS3(nb::module_ & m) {
        typedef s3::handle::File File;
        typedef s3::handle::Group Group;
        typedef s3::handle::Dataset Dataset;

        auto g = getGroupHandle<Group, File, Dataset>(m, "S3Group");
        g
            .def(nb::init<Group, const std::string &>(),
                 nb::arg("group"), nb::arg("key"))
            .def(nb::init<File, const std::string &>(),
                 nb::arg("file"), nb::arg("key"))
        ;

        auto f = getGroupHandle<File, File, Dataset>(m, "S3File");
        f
            .def(nb::init<const std::string &, const std::string &, FileMode,
                          const std::string &, const std::string &, bool,
                          const std::string &, const std::string &>(),
                 nb::arg("bucket_name"), nb::arg("name_in_bucket"), nb::arg("mode"),
                 nb::arg("endpoint_url") = std::string(),
                 nb::arg("region") = std::string("us-east-1"),
                 nb::arg("anon") = false,
                 nb::arg("access_key") = std::string(),
                 nb::arg("secret_key") = std::string())
        ;

        nb::class_<Dataset>(m, "S3DatasetHandle")
            .def(nb::init<Group, const std::string &>())
            .def(nb::init<File, const std::string &>())
        ;

    }
    #endif


    void exportHandles(nb::module_ & m) {
        exportFilesystem(m);
        #ifdef WITH_S3
        exportS3(m);
        #endif
    }

}
