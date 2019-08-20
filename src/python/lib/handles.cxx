#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "z5/handle.hxx"
#include "z5/filesystem/handle.hxx"

#ifdef WITH_S3
#include "z5/s3/handle.hxx"
#endif

namespace py = pybind11;

namespace z5 {


    /*
    inline void exportBase(py::module & m) {
        py::class_<handle::File>(m, "File");
        py::class_<handle::Group>(m, "Group");
    }
    */


    inline void exportFilesystem(py::module & m) {
        namespace fhandle = filesystem::handle;
        py::class_<fhandle::File>(m, "File")
            .def("exists", &fhandle::File::exists)
            .def("in", &fhandle::File::in)
            .def("keys", &fhandle::File::keys)
            .def("path", [](const fhandle::File & self){return self.path().string();});
        py::class_<fhandle::Group>(m, "Group")
            .def("exists", &fhandle::Group::exists)
            .def("in", &fhandle::Group::in)
            .def("keys", &fhandle::Group::keys)
            .def("path", [](const fhandle::Group & self){return self.path().string();});
    }


    #ifdef WITH_S3
    inline void exportS3(py::module & m) {
        namespace shandle = s3::handle;
        py::class_<shandle::File>(m, "S3File")
            .def("exists", &shandle::File::exists)
            .def("in", &shandle::File::in)
            .def("keys", &shandle::File::keys);
        py::class_<shandle::Group>(m, "S3Group")
            .def("exists", &shandle::Group::exists)
            .def("in", &shandle::Group::in)
            .def("keys", &shandle::Group::keys);
    }
    #endif


    inline void exportHandles(py::module & m) {
        exportFilesystem(m);
        #ifdef WITH_S3
        exportS3(m);
        #endif
    }

}
