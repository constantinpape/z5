#include <complex>
#include <memory>
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


    // Extract the scalar as CAST_T (falling back to double, so float values can
    // still be written - truncating - to integer datasets), then write without the
    // GIL. Funneling every value through double would silently lose precision for
    // integers > 2^53.
    template<class T, class CAST_T>
    inline void writePyScalarCasted(const Dataset & ds,
                                    const std::vector<std::size_t> & roiBegin,
                                    const std::vector<std::size_t> & roiShape,
                                    const nb::object & val,
                                    const int numberOfThreads) {
        T v;
        try {
            v = static_cast<T>(nb::cast<CAST_T>(val));
        } catch(...) {
            v = static_cast<T>(nb::cast<double>(val));
        }
        nb::gil_scoped_release lift_gil;
        writePyScalar<T>(ds, roiBegin, roiShape, v, numberOfThreads);
    }


    template<class T>
    inline nb::object readPyChunk(const Dataset & ds, const types::ShapeType & chunkId) {

        types::ShapeType shape;
        bool exists;

        // check if the chunk exists and get its shape; the python layer previously
        // did its own pre-check, doubling the existence probe (an S3 round trip)
        {
            nb::gil_scoped_release lift_gil;
            exists = ds.chunkExists(chunkId);
            if(exists) {
                // get the shape of the output data
                // varlen: return data as 1D array otherwise return ND array
                std::size_t chunkSize;
                const bool isVarlen = ds.checkVarlenChunk(chunkId, chunkSize);
                if(isVarlen) {
                    shape = types::ShapeType({chunkSize});
                } else if(ds.isZarr()) {
                    // zarr chunks are always stored at the full chunk shape (edge chunks
                    // are padded); readChunk decompresses the full chunk, so the output
                    // must be allocated accordingly
                    shape = ds.defaultChunkShape();
                } else {
                    ds.getChunkShape(chunkId, shape);
                }
            }
        }
        if(!exists) {
            return nb::none();
        }

        // allocate the data and read the chunk; hold the buffer in a unique_ptr so
        // it is released if readChunk throws (corrupt chunk, decompression failure)
        const std::size_t size = std::accumulate(shape.begin(), shape.end(),
                                                 static_cast<std::size_t>(1),
                                                 std::multiplies<std::size_t>());
        std::unique_ptr<T[]> data(new T[size]);
        {
            nb::gil_scoped_release lift_gil;
            ds.readChunk(chunkId, data.get());
        }

        // hand ownership of the buffer to numpy via a capsule
        nb::capsule owner(data.get(), [](void * p) noexcept { delete[] static_cast<T*>(p); });
        return nb::cast(nb::ndarray<nb::numpy, T>(data.release(), shape.size(), shape.data(), owner));
    }


    template<class T>
    inline void writePyChunk(const Dataset & ds,
                             const types::ShapeType & chunkId,
                             const nb::ndarray<nb::numpy, const T, nb::c_contig> in,
                             const bool isVarlen) {
        // the chunk writer reinterprets the buffer as the DATASET's dtype and reads
        // the full chunk's worth of it, so both the dtype and the element count must
        // be validated here to prevent out-of-bounds reads of the numpy buffer
        ds.checkRequestType(typeid(T));
        if(!isVarlen) {
            // zarr chunks are stored at the full chunk shape, n5 chunks at the
            // boundary-clipped shape
            const std::size_t expected = ds.isZarr() ? ds.defaultChunkSize()
                                                     : ds.getChunkSize(chunkId);
            if(in.size() != expected) {
                throw std::runtime_error(
                    "Chunk data has the wrong number of elements: " +
                    std::to_string(in.size()) + " instead of " + std::to_string(expected));
            }
        }
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
            }, nb::call_guard<nb::gil_scoped_release>())
            .def("getChunkShape", [](const Dataset & ds,
                                     const types::ShapeType & chunkIndices,
                                     const bool fromHeader){
                types::ShapeType chunkShape;
                ds.getChunkShape(chunkIndices, chunkShape, fromHeader);
                return chunkShape;
            }, nb::call_guard<nb::gil_scoped_release>())

            //
            // shapes and stuff
            //
            .def_prop_ro("shape", [](const Dataset & ds){return ds.shape();})
            .def_prop_ro("len", [](const Dataset & ds){return ds.shape(0);})
            .def_prop_ro("chunks", [](const Dataset & ds){return ds.defaultChunkShape();})
            .def_prop_ro("shards", [](const Dataset & ds){return ds.shardShape();})
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
        // and then cast to the correct type. The value is taken as a python
        // object and extracted per dtype (integers via [u]int64, so values
        // beyond 2^53 don't lose precision in a double round trip).
        module.def("write_scalar", [](const Dataset & ds,
                                      const std::vector<std::size_t> & roiBegin,
                                      const std::vector<std::size_t> & roiShape,
                                      const nb::object & val,
                                      const std::string & dtype,
                                      const int numberOfThreads) {
                    auto internalDtype = types::Datatypes::n5ToDtype().at(dtype);
                    switch(internalDtype) {
                        case types::Datatype::int8 :
                            writePyScalarCasted<int8_t, int64_t>(ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        case types::Datatype::int16 :
                            writePyScalarCasted<int16_t, int64_t>(ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        case types::Datatype::int32 :
                            writePyScalarCasted<int32_t, int64_t>(ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        case types::Datatype::int64 :
                            writePyScalarCasted<int64_t, int64_t>(ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        case types::Datatype::uint8 :
                            writePyScalarCasted<uint8_t, uint64_t>(ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        case types::Datatype::uint16 :
                            writePyScalarCasted<uint16_t, uint64_t>(ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        case types::Datatype::uint32 :
                            writePyScalarCasted<uint32_t, uint64_t>(ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        case types::Datatype::uint64 :
                            writePyScalarCasted<uint64_t, uint64_t>(ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        case types::Datatype::float32 :
                            writePyScalarCasted<float, double>(ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        case types::Datatype::float64 :
                            writePyScalarCasted<double, double>(ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        case types::Datatype::complex64 :
                            writePyScalarCasted<std::complex<float>, std::complex<double>>(
                                ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        case types::Datatype::complex128 :
                            writePyScalarCasted<std::complex<double>, std::complex<double>>(
                                ds, roiBegin, roiShape, val, numberOfThreads);
                            break;
                        default: throw(std::runtime_error("Invalid datatype"));

                    }
                },nb::arg("ds"),
                  nb::arg("roi_begin"),
                  nb::arg("roi_shape"),
                  nb::arg("val"),
                  nb::arg("dtype"),
                  nb::arg("n_threads")=1);
    }

}
