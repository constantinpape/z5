#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <iostream>

#include "z5/dataset.hxx"
#include "z5/dataset_factory.hxx"
#include "z5/groups.hxx"
#include "z5/multiarray/broadcast.hxx"

// for marray numpy bindings
//#include "z5/multiarray/marray_access.hxx"
//#include "z5/python/converter.hxx"

#include "z5/multiarray/xtensor_access.hxx"

// for xtensor numpy bindings
#ifndef FORCE_IMPORT_ARRAY
#define FORCE_IMPORT_ARRAY
#endif
#include "xtensor-python/pyarray.hpp"

namespace py = pybind11;

namespace z5 {

    template<class T>
    inline void writePySubarray(const Dataset & ds, const xt::pyarray<T> & in, const std::vector<size_t> & roiBegin) {
        multiarray::writeSubarray<T>(ds, in, roiBegin.begin());
    }

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
                const xt::pyarray<int8_t> in,
                const std::vector<size_t> & roiBegin
            ){
                std::cout << "Write int8" << std::endl;
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray<int8_t>(ds, in, roiBegin.begin());
            })
            // int16
            .def("write_subarray", [](
                const Dataset & ds,
                const xt::pyarray<int16_t> in,
                const std::vector<size_t> & roiBegin
            ){
                std::cout << "Write int16" << std::endl;
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray<int16_t>(ds, in, roiBegin.begin());
            })
            // int32
            .def("write_subarray", [](
                const Dataset & ds,
                const xt::pyarray<int32_t> in,
                const std::vector<size_t> & roiBegin
            ){
                std::cout << "Write int32" << std::endl;
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray<int32_t>(ds, in, roiBegin.begin());
            }, py::arg("in").noconvert(), py::arg("roiBegin"))
            // int64
            .def("write_subarray", [](
                const Dataset & ds,
                const xt::pyarray<int64_t> in,
                const std::vector<size_t> & roiBegin
            ){
                std::cout << "Write int64" << std::endl;
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray<int64_t>(ds, in, roiBegin.begin());
            })
            // uint8
            .def("write_subarray", [](
                const Dataset & ds,
                const xt::pyarray<uint8_t> in,
                const std::vector<size_t> & roiBegin
            ){
                std::cout << "Write uint8" << std::endl;
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray<uint8_t>(ds, in, roiBegin.begin());
            })
            // uint16
            .def("write_subarray", [](
                const Dataset & ds,
                const xt::pyarray<uint16_t> in,
                const std::vector<size_t> & roiBegin
            ){
                std::cout << "Write uint16" << std::endl;
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray<uint16_t>(ds, in, roiBegin.begin());
            })
            // uint32
            .def("write_subarray", [](
                const Dataset & ds,
                const xt::pyarray<uint32_t> in,
                const std::vector<size_t> & roiBegin
            ){
                std::cout << "Write uint32" << std::endl;
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray<uint32_t>(ds, in, roiBegin.begin());
            })
            // uint64
            .def("write_subarray", [](
                const Dataset & ds,
                const xt::pyarray<uint64_t> in,
                const std::vector<size_t> & roiBegin
            ){
                std::cout << "Write uint64" << std::endl;
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray<uint64_t>(ds, in, roiBegin.begin());
            })
            // float32
            .def("write_subarray", [](
                const Dataset & ds,
                const xt::pyarray<float> in,
                const std::vector<size_t> & roiBegin
            ){
                std::cout << "Write float" << std::endl;
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray<float>(ds, in, roiBegin.begin());
            })
            // float64
            .def("write_subarray", [](
                const Dataset & ds,
                const xt::pyarray<double> in,
                const std::vector<size_t> & roiBegin
            ){
                std::cout << "Write double" << std::endl;
                py::gil_scoped_release allowThreads;
                multiarray::writeSubarray<double>(ds, in, roiBegin.begin());
            })

            //
            // readers
            //
            // int 8
            .def("read_subarray", [](
                const Dataset & ds,
                xt::pyarray<int8_t> & out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray<int8_t>(ds, out, roiBegin.begin());
            })
            // int 16
            .def("read_subarray", [](
                const Dataset & ds,
                xt::pyarray<int16_t> & out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray<int16_t>(ds, out, roiBegin.begin());
            })
            // int 32
            .def("read_subarray", [](
                const Dataset & ds,
                xt::pyarray<int32_t> & out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray<int32_t>(ds, out, roiBegin.begin());
            })
            // int 64
            .def("read_subarray", [](
                const Dataset & ds,
                xt::pyarray<int64_t> & out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray<int64_t>(ds, out, roiBegin.begin());
            })
            // uint 8
            .def("read_subarray", [](
                const Dataset & ds,
                xt::pyarray<uint8_t> & out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray<uint8_t>(ds, out, roiBegin.begin());
            })
            // uint 16
            .def("read_subarray", [](
                const Dataset & ds,
                xt::pyarray<uint16_t> & out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray<uint16_t>(ds, out, roiBegin.begin());
            })
            // uint 32
            .def("read_subarray", [](
                const Dataset & ds,
                xt::pyarray<uint32_t> & out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray<uint32_t>(ds, out, roiBegin.begin());
            })
            // uint 64
            .def("read_subarray", [](
                const Dataset & ds,
                xt::pyarray<uint64_t> & out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray<uint64_t>(ds, out, roiBegin.begin());
            })
            // float 32
            .def("read_subarray", [](
                const Dataset & ds,
                xt::pyarray<float> & out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray<float>(ds, out, roiBegin.begin());
            })
            // flat 64
            .def("read_subarray", [](
                const Dataset & ds,
                xt::pyarray<double> & out,
                const std::vector<size_t> & roiBegin
            ){
                py::gil_scoped_release allowThreads;
                multiarray::readSubarray<double>(ds, out, roiBegin.begin());
            })

            //
            // scalar broadcsting
            //
            .def("write_scalar", [](
                const Dataset & ds,
                const std::vector<size_t> & roiBegin,
                const std::vector<size_t> & roiShape,
                int val
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeScalar(ds, roiBegin.begin(), roiShape.begin(), val);
            })
            .def("write_scalar", [](
                const Dataset & ds,
                const std::vector<size_t> & roiBegin,
                const std::vector<size_t> & roiShape,
                double val
            ){
                py::gil_scoped_release allowThreads;
                multiarray::writeScalar(ds, roiBegin.begin(), roiShape.begin(), val);
            })

            //
            // find min and max chunks along given dimension
            //
            .def("findMinimumCoordinates", [](const Dataset & ds, const unsigned dim){
                types::ShapeType chunk;
                {
                    py::gil_scoped_release allowThreads;
                    ds.findMinimumCoordinates(dim, chunk);
                }
                return chunk;
            })
            .def("findMaximumCoordinates", [](const Dataset & ds, const unsigned dim){
                types::ShapeType chunk;
                {
                    py::gil_scoped_release allowThreads;
                    ds.findMaximumCoordinates(dim, chunk);
                }
                return chunk;
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

        module.def("write_subarray",
                   &writePySubarray<int32_t>,
                   py::arg("ds"), py::arg("in").noconvert(), py::arg("roi_begin"),
                   py::call_guard<py::gil_scoped_release>());

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
