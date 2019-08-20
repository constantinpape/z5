#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <iostream>

#include "z5/dataset.hxx"

#include "z5/multiarray/broadcast.hxx"
#include "z5/multiarray/xtensor_access.hxx"

// for xtensor numpy bindings
#include "xtensor-python/pyarray.hpp"
#include "xtensor-python/pytensor.hpp"

namespace py = pybind11;

namespace z5 {

    template<class T>
    inline void writePySubarray(const Dataset & ds,
                                // TODO specifying the strides might speed provide some speed-up
                                // TODO but it prevents singleton dimensions in the shapes
                                // const xt::pyarray<T, xt::layout_type::row_major> & in,
                                const xt::pyarray<T> & in,
                                const std::vector<size_t> & roiBegin,
                                const int numberOfThreads) {
        multiarray::writeSubarray<T>(ds, in, roiBegin.begin(), numberOfThreads);
    }


    template<class T>
    inline void readPySubarray(const Dataset & ds,
                               // TODO specifying the strides might speed provide some speed-up
                               // TODO but it prevents singleton dimensions in the shapes
                               // xt::pyarray<T, xt::layout_type::row_major> & out,
                               xt::pyarray<T> & out,
                               const std::vector<size_t> & roiBegin,
                               const int numberOfThreads) {
        multiarray::readSubarray<T>(ds, out, roiBegin.begin(), numberOfThreads);
    }


    template<class T>
    inline void writePyScalar(const Dataset & ds,
                              const std::vector<size_t> & roiBegin,
                              const std::vector<size_t> & roiShape,
                              const T val,
                              const int numberOfThreads) {
        multiarray::writeScalar<T>(ds, roiBegin.begin(), roiShape.begin(), val, numberOfThreads);
    }


    template<class T>
    inline xt::pyarray<T> readPyChunk(const Dataset & ds, const types::ShapeType & chunkId) {
        typedef typename xt::pyarray<T>::shape_type ShapeType;
        // check if the chunk exists
        bool chunkExists;
        {
            py::gil_scoped_release lift_gil;
            chunkExists = ds.chunkExists(chunkId);
        }
        // if the chunk does not exist, return None
        if(!chunkExists) {
            return py::none();
        }
        // get the shape of the output data
        ShapeType shape;
        {
            // TODO can't lift gil here ?!
            // py::gil_scoped_release lift_gil;
            // varlen: return data as 1D array otherwise return ND array
            std::size_t chunkSize;
            const bool isVarlen = ds.checkVarlenChunk(chunkId, chunkSize);
            if(isVarlen) {
                shape = ShapeType({chunkSize});
            } else {
                types::ShapeType chunkShape;
                ds.getChunkShape(chunkId, chunkShape);
                shape.resize(chunkShape.size());
                std::copy(chunkShape.begin(), chunkShape.end(), shape.begin());
            }
        }
        // read the chunk
        xt::pyarray<T> out(shape);
        {
            py::gil_scoped_release lift_gil;
            ds.readChunk(chunkId, &out[0]);
        }
        return out;
    }


    template<class T>
    inline void writePyChunk(const Dataset & ds,
                             const types::ShapeType & chunkId,
                             const xt::pyarray<T> & in,
                             const bool isVarlen) {
        ds.writeChunk(chunkId, &in[0], isVarlen,
                      isVarlen ? in.size() : 0);
    }


    template<class T>
    void exportIoT(py::module & module, const std::string & dtype) {

        // export writing subarrays
        module.def("write_subarray",
                   &writePySubarray<T>,
                   py::arg("ds"),
                   py::arg("in").noconvert(),
                   py::arg("roi_begin"),
                   py::arg("n_threads")=1,
                   py::call_guard<py::gil_scoped_release>());

        // export reading subarrays
        module.def("read_subarray",
                   &readPySubarray<T>,
                   py::arg("ds"),
                   py::arg("out").noconvert(),
                   py::arg("roi_begin"),
                   py::arg("n_threads")=1,
                   py::call_guard<py::gil_scoped_release>());

        // export write_chunk
        module.def("write_chunk",
                   &writePyChunk<T>,
                   py::arg("ds"), py::arg("chunkId"),
                   py::arg("in").noconvert(), py::arg("isVarlen")=false,
                   py::call_guard<py::gil_scoped_release>());

        // export chunk reading for all datatypes
        // integer types
        const std::string readChunkName = "read_chunk_" + dtype;
        module.def(readChunkName.c_str(), &readPyChunk<T>,
                   py::arg("ds"), py::arg("chunkId"));
    }


    void exportDataset(py::module & module) {

        auto dsClass = py::class_<Dataset>(module, "DatasetImpl");

        dsClass
            .def("chunkExists", [](const Dataset & ds, const types::ShapeType & chunkIndices){
                return ds.chunkExists(chunkIndices);
            })

            //
            // shapes and stuff
            //
            .def_property_readonly("shape", [](const Dataset & ds){return ds.shape();})
            .def_property_readonly("len", [](const Dataset & ds){return ds.shape(0);})
            .def_property_readonly("chunks", [](const Dataset & ds){return ds.defaultChunkShape();})
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

            // TODO need to rethink this
            /*
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
            */
        ;

        // export I/O for all dtypes
        // integer types
        exportIoT<int8_t>(module, "int8");
        exportIoT<int16_t>(module, "int16");
        exportIoT<int32_t>(module, "int32");
        exportIoT<int64_t>(module, "int64");
        // unsigned integer types
        exportIoT<uint8_t>(module, "uint8");
        exportIoT<uint16_t>(module, "uint16");
        exportIoT<uint32_t>(module, "uint32");
        exportIoT<uint64_t>(module, "uint64");
        // float types
        exportIoT<float>(module, "float32");
        exportIoT<double>(module, "float64");

        // export writing scalars
        // The overloads cannot be properly resolved,
        // that's why we give the datatype as additional argument
        // and then cast to the correct type
        module.def("write_scalar", [](const Dataset & ds,
                                      const std::vector<size_t> & roiBegin,
                                      const std::vector<size_t> & roiShape,
                                      const double val,
                                      const std::string & dtype,
                                      const int numberOfThreads) {
                    auto internalDtype = types::Datatypes::n5ToDtype().at(dtype);
                    switch(internalDtype) {
                        case types::Datatype::int8 : writePyScalar<int8_t>(ds, roiBegin, roiShape,
                                                                           static_cast<int8_t>(val),
                                                                           numberOfThreads);
                                                     break;
                        case types::Datatype::int16 : writePyScalar<int16_t>(ds, roiBegin, roiShape,
                                                                             static_cast<int16_t>(val),
                                                                             numberOfThreads);
                                                     break;
                        case types::Datatype::int32 : writePyScalar<int32_t>(ds, roiBegin, roiShape,
                                                                             static_cast<int32_t>(val),
                                                                             numberOfThreads);
                                                     break;
                        case types::Datatype::int64 : writePyScalar<int64_t>(ds, roiBegin, roiShape,
                                                                             static_cast<int64_t>(val),
                                                                             numberOfThreads);
                                                     break;
                        case types::Datatype::uint8 : writePyScalar<uint8_t>(ds, roiBegin, roiShape,
                                                                             static_cast<uint8_t>(val),
                                                                             numberOfThreads);
                                                     break;
                        case types::Datatype::uint16 : writePyScalar<uint16_t>(ds, roiBegin, roiShape,
                                                                               static_cast<uint16_t>(val),
                                                                               numberOfThreads);
                                                     break;
                        case types::Datatype::uint32 : writePyScalar<uint32_t>(ds, roiBegin, roiShape,
                                                                               static_cast<uint32_t>(val),
                                                                               numberOfThreads);
                                                     break;
                        case types::Datatype::uint64 : writePyScalar<uint64_t>(ds, roiBegin, roiShape,
                                                                               static_cast<uint64_t>(val),
                                                                               numberOfThreads);
                                                     break;
                        case types::Datatype::float32 : writePyScalar<float>(ds, roiBegin, roiShape,
                                                                             static_cast<float>(val),
                                                                             numberOfThreads);
                                                        break;
                        case types::Datatype::float64 : writePyScalar<double>(ds, roiBegin, roiShape,
                                                                              static_cast<double>(val),
                                                                              numberOfThreads);
                                                        break;
                        default: throw(std::runtime_error("Invalid datatype"));

                    }
                },py::arg("ds"),
                  py::arg("roi_begin"),
                  py::arg("roi_shape"),
                  py::arg("val"),
                  py::arg("dtype"),
                  py::arg("n_threads")=1,
                  py::call_guard<py::gil_scoped_release>());
    }

}
