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
#include "xtensor-python/pyarray.hpp"
#include "xtensor-python/pytensor.hpp"

namespace py = pybind11;

namespace z5 {

    template<class T>
    inline void writePySubarray(const Dataset & ds, const xt::pyarray<T> & in, const std::vector<size_t> & roiBegin) {
        multiarray::writeSubarray<T>(ds, in, roiBegin.begin());
    }

    template<class T>
    inline void readPySubarray(const Dataset & ds, xt::pyarray<T> & out, const std::vector<size_t> & roiBegin) {
        multiarray::readSubarray<T>(ds, out, roiBegin.begin());
    }

    template<class T>
    inline void writePyScalar(const Dataset & ds,
                              const std::vector<size_t> & roiBegin,
                              const std::vector<size_t> & roiShape,
                              const T val) {
        multiarray::writeScalar(ds, roiBegin.begin(), roiShape.begin(), val);
    }

    template<class T>
    inline void convertPyArrayToFormat(const Dataset & ds,
                                       const xt::pyarray<T> & in,
                                       xt::pytensor<uint8_t, 1> & out) {
        multiarray::convertArrayToFormat<T>(ds, in, out);
    }


    template<class T>
    void exportIoT(py::module & module) {

        // export writing subarrays
        module.def("write_subarray",
                   &writePySubarray<T>,
                   py::arg("ds"), py::arg("in").noconvert(), py::arg("roi_begin"),
                   py::call_guard<py::gil_scoped_release>());

        // export reading subarrays
        module.def("read_subarray",
                   &readPySubarray<T>,
                   py::arg("ds"), py::arg("out").noconvert(), py::arg("roi_begin"),
                   py::call_guard<py::gil_scoped_release>());

        // export writing scalars
        module.def("write_scalar",
                   &writePyScalar<T>,
                   py::arg("ds"), py::arg("roi_begin"), py::arg("roi_shape"), py::arg("val").noconvert(),
                   py::call_guard<py::gil_scoped_release>());

        // export conversions
        module.def("convert_array_to_format",
                   &convertPyArrayToFormat<T>,
                   py::arg("ds"), py::arg("in").noconvert(), py::arg("out").noconvert(),
                   py::call_guard<py::gil_scoped_release>());
    }


    void exportDataset(py::module & module) {

        auto dsClass = py::class_<Dataset>(module, "DatasetImpl");

        dsClass
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
            .def_property_readonly("number_of_chunks", [](const Dataset & ds){return ds.numberOfChunks();})
            .def_property_readonly("chunks_per_dimension", [](const Dataset & ds){
                return ds.chunksPerDimension();
            })
            .def_property_readonly("compressor", [](const Dataset & ds){
                std::string compressor;
                ds.getCompressor(compressor);
                return compressor;
            })
            .def_property_readonly("codec", [](const Dataset & ds){
                std::string codec;
                ds.getCodec(codec);
                return codec;
            })
            .def_property_readonly("level", [](const Dataset & ds){
                return ds.getCLevel();
            })
            .def_property_readonly("shuffle", [](const Dataset & ds){
                return ds.getCShuffle();
            })

            // compression, compression_opts, fillvalue
        ;

        module.def("open_dataset",[](const std::string & path){
            return openDataset(path);
        });


        // TODO params !!!
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

        // export I/O for all dtypes
        // integer types
        exportIoT<int8_t>(module);
        exportIoT<int16_t>(module);
        exportIoT<int32_t>(module);
        exportIoT<int64_t>(module);
        // unsigned integer types
        exportIoT<uint8_t>(module);
        exportIoT<uint16_t>(module);
        exportIoT<uint32_t>(module);
        exportIoT<uint64_t>(module);
        // float types
        exportIoT<float>(module);
        exportIoT<double>(module);
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
