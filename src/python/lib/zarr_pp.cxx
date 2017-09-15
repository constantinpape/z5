#include <pybind11/pybind11.h>
#include <iostream>

namespace py = pybind11;


namespace zarr {
    void exportZarrArray(py::module &);
}


PYBIND11_PLUGIN(_zarr_pp) {

    py::options options;
    py::module module("_zarr_pp", "Zarr++ pythonbindings");

    using namespace zarr;
    exportZarrArray(module);
    return module.ptr();
}

