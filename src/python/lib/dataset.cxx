#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <iostream>

#include "z5/dataset.hxx"
#include "z5/dataset_factory.hxx"
#include "z5/multiarray/marray_access.hxx"
#include "z5/python/converter.hxx"
#include "z5/groups.hxx"
#include "z5/broadcast.hxx"


namespace z5 {

    void exportDataset(py::module & module) {

        auto dsClass = py::class_<Dataset>(module, "DatasetImpl");

        // TODO do we really need to provide read / write for all datatypes ? / is there a way to
        // do the dtype inference at runtime
        // TODO export chunk access ?
        dsClass

            //
            // writers
            //
            // int8
            .def("write_subarray", [](
                const Dataset & ds,
                const andres::PyView<int8_t> in,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })
            // int16
            .def("write_subarray", [](
                const Dataset & ds,
                const andres::PyView<int16_t> in,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })
            // int32
            .def("write_subarray", [](
                const Dataset & ds,
                const andres::PyView<int32_t> in,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })
            // int64
            .def("write_subarray", [](
                const Dataset & ds,
                const andres::PyView<int64_t> in,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })
            // uint8
            .def("write_subarray", [](
                const Dataset & ds,
                const andres::PyView<uint8_t> in,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })
            // uint16
            .def("write_subarray", [](
                const Dataset & ds,
                const andres::PyView<uint16_t> in,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })
            // uint32
            .def("write_subarray", [](
                const Dataset & ds,
                const andres::PyView<uint32_t> in,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })
            // uint64
            .def("write_subarray", [](
                const Dataset & ds,
                const andres::PyView<uint64_t> in,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })
            // float32
            .def("write_subarray", [](
                const Dataset & ds,
                const andres::PyView<float> in,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })
            // float64
            .def("write_subarray", [](
                const Dataset & ds,
                const andres::PyView<double> in,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray(ds, in, roiBegin.begin());
            })

            //
            // readers
            //
            // int 8
            .def("read_subarray", [](
                const Dataset & ds,
                andres::PyView<int8_t> out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray(ds, out, roiBegin.begin());
            })
            // int 16
            .def("read_subarray", [](
                const Dataset & ds,
                andres::PyView<int16_t> out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray(ds, out, roiBegin.begin());
            })
            // int 32
            .def("read_subarray", [](
                const Dataset & ds,
                andres::PyView<int32_t> out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray(ds, out, roiBegin.begin());
            })
            // int 64
            .def("read_subarray", [](
                const Dataset & ds,
                andres::PyView<int64_t> out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray(ds, out, roiBegin.begin());
            })
            // uint 8
            .def("read_subarray", [](
                const Dataset & ds,
                andres::PyView<uint8_t> out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray(ds, out, roiBegin.begin());
            })
            // uint 16
            .def("read_subarray", [](
                const Dataset & ds,
                andres::PyView<uint16_t> out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray(ds, out, roiBegin.begin());
            })
            // uint 32
            .def("read_subarray", [](
                const Dataset & ds,
                andres::PyView<uint32_t> out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray(ds, out, roiBegin.begin());
            })
            // uint 64
            .def("read_subarray", [](
                const Dataset & ds,
                andres::PyView<uint64_t> out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray(ds, out, roiBegin.begin());
            })
            // float 32
            .def("read_subarray", [](
                const Dataset & ds,
                andres::PyView<float> out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray(ds, out, roiBegin.begin());
            })
            // flat 64
            .def("read_subarray", [](
                const Dataset & ds,
                andres::PyView<double> out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray(ds, out, roiBegin.begin());
            })

            //
            // scalar broadcsting
            //
            // TODO which ones do we have to define here?
            .def("write_scalar", [](
                const Dataset & ds,
                const std::vector<size_t> & roiBegin,
                const std::vector<size_t> & roiShape,
                int val
            ){
                py::gil_scoped_release allowThreads;
                writeScalar(ds, roiBegin.begin(), roiShape.begin(), val);
            })
            .def("write_scalar", [](
                const Dataset & ds,
                const std::vector<size_t> & roiBegin,
                const std::vector<size_t> & roiShape,
                double val
            ){
                py::gil_scoped_release allowThreads;
                writeScalar(ds, roiBegin.begin(), roiShape.begin(), val);
            })

            //
            // shapes and stuff
            //
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
