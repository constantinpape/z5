#include <complex>
#include <numeric>

#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/complex.h>

#include "z5/dataset.hxx"

#include "z5/multiarray/array_view.hxx"
#include "z5/multiarray/array_access.hxx"
#include "z5/multiarray/broadcast.hxx"


namespace nb = nanobind;

namespace z5 {

    //
    // helpers to build (Const)ArrayView from a numpy ndarray
    // strides are reported by nanobind in elements, matching ArrayView's convention
    //
    template<class T>
    inline multiarray::ConstArrayView<T> constViewFromArray(
            const nb::ndarray<nb::numpy, const T, nb::c_contig> & arr) {
        const std::size_t nd = arr.ndim();
        types::ShapeType shape(nd), strides(nd);
        for(std::size_t d = 0; d < nd; ++d) {
            shape[d] = arr.shape(d);
            strides[d] = static_cast<std::size_t>(arr.stride(d));
        }
        return multiarray::ConstArrayView<T>(arr.data(), std::move(shape), std::move(strides));
    }

    template<class T>
    inline multiarray::ArrayView<T> viewFromArray(
            const nb::ndarray<nb::numpy, T, nb::c_contig> & arr) {
        const std::size_t nd = arr.ndim();
        types::ShapeType shape(nd), strides(nd);
        for(std::size_t d = 0; d < nd; ++d) {
            shape[d] = arr.shape(d);
            strides[d] = static_cast<std::size_t>(arr.stride(d));
        }
        return multiarray::ArrayView<T>(arr.data(), std::move(shape), std::move(strides));
    }


    template<class T>
    inline void writePySubarray(const Dataset & ds,
                                const nb::ndarray<nb::numpy, const T, nb::c_contig> in,
                                const std::vector<std::size_t> & roiBegin,
                                const int numberOfThreads) {
        const auto view = constViewFromArray<T>(in);
        multiarray::writeSubarray<T>(ds, view, roiBegin.begin(), numberOfThreads);
    }


    template<class T>
    inline void readPySubarray(const Dataset & ds,
                               nb::ndarray<nb::numpy, T, nb::c_contig> out,
                               const std::vector<std::size_t> & roiBegin,
                               const int numberOfThreads) {
        const auto view = viewFromArray<T>(out);
        multiarray::readSubarray<T>(ds, view, roiBegin.begin(), numberOfThreads);
    }


    template<class T>
    inline void writePyScalar(const Dataset & ds,
                              const std::vector<std::size_t> & roiBegin,
                              const std::vector<std::size_t> & roiShape,
                              const T val,
                              const int numberOfThreads) {
        multiarray::writeScalar<T>(ds, roiBegin.begin(), roiShape.begin(), val, numberOfThreads);
    }


    template<class T>
    inline nb::ndarray<nb::numpy, T> readPyChunk(const Dataset & ds, const types::ShapeType & chunkId) {

        types::ShapeType shape;

        // make sure the chunk exists and get the shape of the chunk
        {
            nb::gil_scoped_release lift_gil;
            // returning None does not work properly, so we raise
            // if the chunk does not exist and assume that this is checked
            // in python beforehand
            if(!ds.chunkExists(chunkId)) {
                throw std::runtime_error("Cannot read chunk because it does not exist.");
            }

            // get the shape of the output data
            // varlen: return data as 1D array otherwise return ND array
            std::size_t chunkSize;
            const bool isVarlen = ds.checkVarlenChunk(chunkId, chunkSize);
            if(isVarlen) {
                shape = types::ShapeType({chunkSize});
            } else {
                ds.getChunkShape(chunkId, shape);
            }
        }

        // allocate the data and read the chunk
        const std::size_t size = std::accumulate(shape.begin(), shape.end(),
                                                 static_cast<std::size_t>(1),
                                                 std::multiplies<std::size_t>());
        T * data = new T[size];
        {
            nb::gil_scoped_release lift_gil;
            ds.readChunk(chunkId, data);
        }

        // hand ownership of the buffer to numpy via a capsule
        nb::capsule owner(data, [](void * p) noexcept { delete[] static_cast<T*>(p); });
        return nb::ndarray<nb::numpy, T>(data, shape.size(), shape.data(), owner);
    }


    template<class T>
    inline void writePyChunk(const Dataset & ds,
                             const types::ShapeType & chunkId,
                             const nb::ndarray<nb::numpy, const T, nb::c_contig> in,
                             const bool isVarlen) {
        ds.writeChunk(chunkId, in.data(), isVarlen,
                      isVarlen ? in.size() : 0);
    }


    template<class T>
    void exportIoT(nb::module_ & module, const std::string & dtype) {

        // export writing subarrays
        module.def("write_subarray",
                   &writePySubarray<T>,
                   nb::arg("ds"),
                   nb::arg("in").noconvert(),
                   nb::arg("roi_begin"),
                   nb::arg("n_threads")=1,
                   nb::call_guard<nb::gil_scoped_release>());

        // export reading subarrays
        module.def("read_subarray",
                   &readPySubarray<T>,
                   nb::arg("ds"),
                   nb::arg("out").noconvert(),
                   nb::arg("roi_begin"),
                   nb::arg("n_threads")=1,
                   nb::call_guard<nb::gil_scoped_release>());

        // export write_chunk
        module.def("write_chunk",
                   &writePyChunk<T>,
                   nb::arg("ds"), nb::arg("chunkId"),
                   nb::arg("in").noconvert(), nb::arg("isVarlen")=false,
                   nb::call_guard<nb::gil_scoped_release>());

        // export chunk reading for all datatypes
        const std::string readChunkName = "read_chunk_" + dtype;
        module.def(readChunkName.c_str(), &readPyChunk<T>,
                   nb::arg("ds"), nb::arg("chunkId"));
    }


    void exportDataset(nb::module_ & module) {

        auto dsClass = nb::class_<Dataset>(module, "DatasetImpl");

        dsClass
            .def("chunkExists", [](const Dataset & ds, const types::ShapeType & chunkIndices){
                return ds.chunkExists(chunkIndices);
            })
            .def("getChunkShape", [](const Dataset & ds,
                                     const types::ShapeType & chunkIndices,
                                     const bool fromHeader){
                types::ShapeType chunkShape;
                ds.getChunkShape(chunkIndices, chunkShape, fromHeader);
                return chunkShape;
            })

            //
            // shapes and stuff
            //
            .def_prop_ro("shape", [](const Dataset & ds){return ds.shape();})
            .def_prop_ro("len", [](const Dataset & ds){return ds.shape(0);})
            .def_prop_ro("chunks", [](const Dataset & ds){return ds.defaultChunkShape();})
            .def_prop_ro("ndim", [](const Dataset & ds){return ds.dimension();})
            .def_prop_ro("size", [](const Dataset & ds){return ds.size();})
            .def_prop_ro("dtype", [](const Dataset & ds){return types::Datatypes::dtypeToN5()[ds.getDtype()];})
            .def_prop_ro("is_zarr", [](const Dataset & ds){return ds.isZarr();})
            .def_prop_ro("number_of_chunks", [](const Dataset & ds){return ds.numberOfChunks();})
            .def_prop_ro("chunks_per_dimension", [](const Dataset & ds){
                return ds.chunksPerDimension();
            })

            // compression, compression_opts, fillvalue
            .def_prop_ro("compressor", [](const Dataset & ds){
                std::string compressor;
                ds.getCompressor(compressor);
                return compressor;
            })
            .def_prop_ro("compression_options", [](const Dataset & ds){
                types::CompressionOptions opts;
                ds.getCompressionOptions(opts);
                nlohmann::json j;
                types::compressionTypeToJson(opts, j);
                return j.dump();
            })

            .def("remove_chunk", &Dataset::removeChunk, nb::arg("chunk_id"),
                 nb::call_guard<nb::gil_scoped_release>())
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
        // complex types
        exportIoT<std::complex<float>>(module, "complex64");
        exportIoT<std::complex<double>>(module, "complex128");

        // export writing scalars
        // The overloads cannot be properly resolved,
        // that's why we give the datatype as additional argument
        // and then cast to the correct type
        module.def("write_scalar", [](const Dataset & ds,
                                      const std::vector<std::size_t> & roiBegin,
                                      const std::vector<std::size_t> & roiShape,
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
                        case types::Datatype::complex64 : writePyScalar<std::complex<float>>(ds, roiBegin, roiShape,
                                                                              static_cast<std::complex<float>>(val),
                                                                              numberOfThreads);
                                                        break;
                        case types::Datatype::complex128 : writePyScalar<std::complex<double>>(ds, roiBegin, roiShape,
                                                                              static_cast<std::complex<double>>(val),
                                                                              numberOfThreads);
                                                        break;
                        default: throw(std::runtime_error("Invalid datatype"));

                    }
                },nb::arg("ds"),
                  nb::arg("roi_begin"),
                  nb::arg("roi_shape"),
                  nb::arg("val"),
                  nb::arg("dtype"),
                  nb::arg("n_threads")=1,
                  nb::call_guard<nb::gil_scoped_release>());
    }

}
