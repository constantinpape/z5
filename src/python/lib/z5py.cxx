#include <pybind11/pybind11.h>
#include <iostream>

// IMPORTANT: This define needs to happen the first time that pyarray is
// imported, i.e. RIGHT HERE !
#define FORCE_IMPORT_ARRAY
#include "xtensor-python/pyarray.hpp"

namespace py = pybind11;


namespace z5 {
    void exportDataset(py::module &);
    void exportGroups(py::module &);
    void exportFileMode(py::module &);
    void exportUtils(py::module &);
}


PYBIND11_MODULE(_z5py, module) {

    xt::import_numpy();
    module.doc() = "z5 pythonbindings";

    using namespace z5;
    exportDataset(module);
    exportGroups(module);
    exportFileMode(module);
    exportUtils(module);
}
