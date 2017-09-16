#include <pybind11/pybind11.h>
#include <iostream>

#include "z5/dataset.hxx"
#include "z5/dataset_factory.hxx"
#include "z5/multiarray/marray_access.hxx"
#include "z5/python/converter.hxx"


namespace z5 {

    void exportDataset(py::module & module) {

        auto dsClass = py::class_<Dataset>(module, "DatasetImpl");

        // TODO do we really need to provide read / write for all datatypes ? / is there a way to
        // do the dtype inference at runtime
        // TODO export chunk access ?
        dsClass
            // writers
            .def("writeSubarry", [](const Dataset & ds, const andres::PyView<int8_t> in, const std::vector<size_t> & roiBegin){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })

            // readers
            .def("readSubarry", [](const Dataset & ds, andres::PyView<int8_t> out, const std::vector<size_t> & roiBegin){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray(ds, out, roiBegin.begin());
            })

            // shapes and stuff
            .def_property_readonly("shape", [](const Dataset & ds){return ds.shape();})
            .def_property_readonly("len", [](const Dataset & ds){return ds.shape(0);})
            .def_property_readonly("chunks", [](const Dataset & ds){return ds.maxChunkShape();})
            .def_property_readonly("ndim", [](const Dataset & ds){return ds.dimension();})
            .def_property_readonly("size", [](const Dataset & ds){return ds.size();})
            .def_property_readonly("dtype", [](const Dataset & ds){return types::dtypeToN5[ds.getDtype()];})

            // TODO
            // compression, compression_opts, fillvalue
        ;

    }
}
