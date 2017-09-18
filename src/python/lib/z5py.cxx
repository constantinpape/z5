#include <pybind11/pybind11.h>
#include <iostream>

namespace py = pybind11;


namespace z5 {
    void exportDataset(py::module &);
    void exportGroups(py::module &);
}


PYBIND11_MODULE(_z5py, module) {

    module.doc() = "z5 pythonbindings";

    using namespace z5;
    exportDataset(module);
    exportGroups(module);
}

