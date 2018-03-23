#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <iostream>

#include "z5/dataset.hxx"
#include "z5/dataset_factory.hxx"
#include "z5/groups.hxx"
#include "z5/multiarray/broadcast.hxx"

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
    inline xt::pytensor<char, 1> convertPyArrayToFormat(const Dataset & ds,
                                       const xt::pyarray<T> & in) {
        xt::pytensor<char, 1> out = xt::zeros<char>({1});
        multiarray::convertArrayToFormat<T>(ds, in, out);
        return out;
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
                   py::arg("ds"), py::arg("in").noconvert(),
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
            .def("chunkExists", [](const Dataset & ds, const types::ShapeType & chunkIndices){
                return ds.chunkExists(chunkIndices);
            })

            //
            // shapes and stuff
            //
            .def_property_readonly("shape", [](const Dataset & ds){return ds.shape();})
            .def_property_readonly("len", [](const Dataset & ds){return ds.shape(0);})
            .def_property_readonly("chunks", [](const Dataset & ds){return ds.maxChunkShape();})
            .def_property_readonly("ndim", [](const Dataset & ds){return ds.dimension();})
            .def_property_readonly("size", [](const Dataset & ds){return ds.size();})
            .def_property_readonly("dtype", [](const Dataset & ds){return types::Datatypes::dtypeToN5()[ds.getDtype()];})
            .def_property_readonly("is_zarr", [](const Dataset & ds){return ds.isZarr();})
            .def_property_readonly("number_of_chunks", [](const Dataset & ds){return ds.numberOfChunks();})
            .def_property_readonly("chunks_per_dimension", [](const Dataset & ds){
                return ds.chunksPerDimension();
            })

            // compression, compression_opts, fillvalue
            .def_property_readonly("compressor", [](const Dataset & ds){
                std::string compressor;
                ds.getCompressor(compressor);
                return compressor;
            })

            // pickle support
            .def(py::pickle(
                // __getstate__ -> we simply pickle the path,
                // the rest will be read from the attributes
                [](const Dataset & ds) {
                    return py::make_tuple(ds.handle().path().string());
                },
                // __setstate__
                [](py::tuple tup) {
                    if(tup.size() != 1) { // the serialization size is 1, because we only pickle the path
                        std::cout << tup.size() << std::endl;
                        throw std::runtime_error("Invalid state for unpickling z5::Dataset");
                    }

                    return openDataset(tup[0].cast<std::string>());
                }
            ))
        ;

        module.def("open_dataset",[](const std::string & path, const FileMode::modes mode){
            return openDataset(path, mode);
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
        module.def("create_group",[](const std::string & path, const bool isZarr, const FileMode::modes mode){
            handle::Group h(path, mode);
            createGroup(h, isZarr);
        });

        module.def("create_sub_group",[](const std::string & path, const std::string & key, const bool isZarr){
            handle::Group h(path);
            createGroup(h, key, isZarr);
        });
    }


    // TODO expose file mode to python
    void exportFileMode(py::module & module) {
        py::class_<FileMode> pyFileMode(module, "FileMode");

        // expose class
        pyFileMode
            .def(py::init<FileMode::modes>())
            .def("can_write", &FileMode::canWrite)
            .def("can_create", &FileMode::canCreate)
            .def("must_not_exist", &FileMode::mustNotExist)
            .def("should_truncate", &FileMode::shouldTruncate)
            .def("mode", &FileMode::printMode)
        ;

        // expose enum
        py::enum_<FileMode::modes>(pyFileMode, "modes")
            .value("r", FileMode::modes::r)
            .value("r_p", FileMode::modes::r_p)
            .value("w", FileMode::modes::w)
            .value("w_m", FileMode::modes::w_m)
            .value("a", FileMode::modes::a)
            .export_values()
        ;
    }
}
