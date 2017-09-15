#include <pybind11/pybind11.h>

#include "zarr++/array.hxx"
#include "zarr++/array_factory.hxx"
#include "zarr++/multiarray/marray_access.hxx"
// TODO copy from nifty
#include "zarr++/python/converter.hxx"


namespace zarr {

    void exportZarrArray(py::module & module) {

        auto zArrayImpl = py::class_<ZarrArray>("ZarrArrayImpl");

        zArrayImpl
            .def("writeSubarry")
            .def("readeSubarry")
        ;

    }
}
