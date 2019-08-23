#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <iostream>

// IMPORTANT: This define needs to happen the first time that pyarray is
// imported, i.e. RIGHT HERE !
#define FORCE_IMPORT_ARRAY
#include "xtensor-python/pyarray.hpp"

#include "z5/common.hxx"

namespace py = pybind11;


namespace z5 {
    void exportAttributes(py::module &);
    void exportDataset(py::module &);
    void exportFactory(py::module &);
    void exportHandles(py::module &);
    void exportUtils(py::module &);

    void exportCompilerFlags(py::module & m) {
        m.def("get_available_codecs", &getAvailableCodecs);
        m.def("get_available_backends", &getAvailableBackends);
    }
}


PYBIND11_MODULE(_z5py, module) {

    xt::import_numpy();
    module.doc() = "z5py: z5 python bindings";

    using namespace z5;
    exportAttributes(module);
    exportCompilerFlags(module);
    exportDataset(module);
    exportHandles(module);
    exportFactory(module);
    exportUtils(module);
}
