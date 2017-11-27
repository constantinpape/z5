#include <pybind11/pybind11.h>
#include <iostream>
#include "xtensor-python/pyarray.hpp"

namespace py = pybind11;


namespace z5 {
    void exportDataset(py::module &);
    void exportGroups(py::module &);
    void exportAdd(py::module &);
}


PYBIND11_MODULE(_z5py, module) {

    xt::import_numpy();
    module.doc() = "z5 pythonbindings";

    using namespace z5;
    //exportDataset(module);
    //exportGroups(module);
    exportAdd(module);
}


/*
PYBIND11_PLUGIN(_z5py) {
    xt::import_numpy();

    py::module module("_z5py", R"docu(
        Pythonbindings for N5 and Zarr multidimensional
        chunked arrays.
    )docu");

    using namespace z5;
    exportDataset(module);
    exportGroups(module);

    return module.ptr();
}
*/
