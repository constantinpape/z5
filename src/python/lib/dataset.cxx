#include <pybind11/pybind11.h>
#include <iostream>

#include "z5/dataset.hxx"
#include "z5/dataset_factory.hxx"
#include "z5/multiarray/marray_access.hxx"
#include "z5/python/converter.hxx"
#include "z5/groups.hxx"


namespace z5 {

    void exportDataset(py::module & module) {

        auto dsClass = py::class_<Dataset>(module, "DatasetImpl");

        // TODO do we really need to provide read / write for all datatypes ? / is there a way to
        // do the dtype inference at runtime
        // TODO export chunk access ?
        dsClass
            // writers
            .def("write_subarry", [](const Dataset & ds, const andres::PyView<int8_t> in, const std::vector<size_t> & roiBegin){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })

            // readers
            .def("read_subarry", [](const Dataset & ds, andres::PyView<int8_t> out, const std::vector<size_t> & roiBegin){
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
            .def_property_readonly("is_zarr", [](const Dataset & ds){return ds.isZarr();})

            // TODO
            // compression, compression_opts, fillvalue
        ;

        module.def("open_dataset",[](const std::string & path){
            return openDataset(path);
        });

        // TODO params
        module.def(
            "create_dataset",[](
            const std::string & path,
            const std::string & dtype,
            const types::ShapeType & shape,
            const types::ShapeType & chunkShape,
            const bool createAsZarr,
            const double fillValue,
            const std::string & compressor,
            const std::string & codec,
            const int compressorLevel,
            const int compressorShuffle
        ){
            return createDataset(
                path, dtype, shape, chunkShape, createAsZarr, fillValue, compressor, codec, compressorLevel, compressorShuffle
            );
        });
    }


    void exportGroups(py::module & module) {
        module.def("create_group",[](const std::string & path, const bool isZarr){
            handle::Group h(path);
            createGroup(h, isZarr);
        });

        module.def("create_sub_group",[](const std::string & path, const std::string & key, const bool isZarr){
            handle::Group h(path);
            createGroup(h, key, isZarr);
        });
    }
}
