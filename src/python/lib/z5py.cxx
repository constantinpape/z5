#include <pybind11/pybind11.h>
#include <iostream>

// IMPORTANT: This define needs to happen the first time that pyarray is
// imported, i.e. RIGHT HERE !
#define FORCE_IMPORT_ARRAY
#include "xtensor-python/pyarray.hpp"

namespace py = pybind11;


namespace z5 {
    void exportAttributes(py::module &);
    void exportDataset(py::module &);
    void exportFactory(py::module &);
    void exportHandles(py::module &);
    void exportUtils(py::module &);
}


PYBIND11_MODULE(_z5py, module) {

    xt::import_numpy();
    module.doc() = "z5py: z5 python bindings";

    using namespace z5;
    exportAttributes(module);
    exportDataset(module);
    exportHandles(module);
    exportFactory(module);
    exportUtils(module);
}
