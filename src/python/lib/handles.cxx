#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "z5/handle.hxx"
#include "z5/filesystem/handle.hxx"

#ifdef WITH_S3
#include "z5/s3/handle.hxx"
#endif

namespace py = pybind11;

namespace z5 {


    void exportFilesystem(py::module & m) {
        namespace fhandle = filesystem::handle;

        py::class_<fhandle::File>(m, "File")
            .def(py::init<const std::string &, FileMode>(),
                 py::arg("path"), py::arg("mode"))
            .def("exists", &fhandle::File::exists)
            .def("in", &fhandle::File::in)
            .def("keys", [](const fhandle::File & self){
                std::vector<std::string> keys;
                self.keys(keys);
                return keys;
            })
            .def("path", [](const fhandle::File & self){return self.path().string();})
            .def("mode", &fhandle::File::mode)
        ;

        py::class_<fhandle::Group>(m, "Group")
            .def(py::init<fhandle::Group, const std::string &>(),
                 py::arg("group"), py::arg("key"))
            .def(py::init<fhandle::File, const std::string &>(),
                 py::arg("file"), py::arg("key"))
            .def("exists", &fhandle::Group::exists)
            .def("in", &fhandle::Group::in)
            .def("keys", [](const fhandle::Group & self){
                std::vector<std::string> keys;
                self.keys(keys);
                return keys;
            })
            .def("path", [](const fhandle::Group & self){return self.path().string();})
            .def("mode", &fhandle::Group::mode)
        ;
    }


    // need actual constructor for S3File, but need to know params for s3 first
    #ifdef WITH_S3
    void exportS3(py::module & m) {
        namespace shandle = s3::handle;
        py::class_<shandle::File>(m, "S3File")
            // dummy construnctor
            .def(py::init<FileMode>(), py::arg("mode"))
            .def("exists", &shandle::File::exists)
            .def("in", &shandle::File::in)
            .def("keys", [](const shandle::File & self){
                std::vector<std::string> keys;
                self.keys(keys);
                return keys;
            })
            .def("mode", &shandle::File::mode)
        ;

        py::class_<shandle::Group>(m, "S3Group")
            .def(py::init<shandle::Group, const std::string &>(),
                 py::arg("group"), py::arg("key"))
            .def(py::init<shandle::File, const std::string &>(),
                 py::arg("file"), py::arg("key"))
            .def("exists", &shandle::Group::exists)
            .def("in", &shandle::Group::in)
            .def("keys", [](const shandle::Group & self){
                std::vector<std::string> keys;
                self.keys(keys);
                return keys;
            })
            .def("mode", &shandle::Group::mode)
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
