#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/unique_ptr.h>
#include <nanobind/stl/complex.h>

#include "z5/factory.hxx"

namespace nb = nanobind;

namespace z5 {


    template<class GROUP>
    void exportDsFactories(nb::module_ & m) {
        m.def("open_dataset", [](const GROUP & root, const std::string & key){
            return openDataset(root, key);
        },
        nb::arg("root"), nb::arg("key"),
        nb::call_guard<nb::gil_scoped_release>());

        m.def("create_dataset", [](const GROUP & root, const std::string & key,
                                   const std::string & dtype,
                                   const std::vector<std::size_t>  & shape,
                                   const std::vector<std::size_t> & chunk_shape,
                                   const std::string & compression,
                                   const std::string & copts,
                                   const double fill_value,
                                   const std::string & dimension_separator,
                                   const int zarr_format,
                                   const std::vector<std::size_t> & shards,
                                   const std::string & chunk_key_encoding,
                                   const std::vector<std::optional<std::string>> & dimension_names,
                                   const std::vector<std::size_t> & transpose,
                                   const bool crc32c,
                                   const std::optional<uint64_t> fill_value_bits,
                                   const std::optional<std::string> fill_value_string,
                                   const std::optional<std::string> fill_value_base64,
                                   const std::optional<std::complex<double>> fill_value_complex){
                DatasetOptions options;
                options.compressor = compression;
                types::jsonToCompressionType(nlohmann::json::parse(copts), options.compressionOptions);
                // z5py passes the exact bits of the fill value as the dtype, or
                // the string of a string dataset
                // (or, for a bytes dataset, its bytes base64-encoded, so they arrive as text),
                // or the parts of a complex fill value
                if(fill_value_complex) {
                    options.fillValue = *fill_value_complex;
                } else if(fill_value_string) {
                    options.fillValue = *fill_value_string;
                } else if(fill_value_base64) {
                    options.fillValue = util::base64Decode(*fill_value_base64);
                } else if(fill_value_bits) {
                    options.fillValue = types::fillValueFromBits(types::Datatypes::n5ToDtype().at(dtype),
                                                                 *fill_value_bits);
                } else {
                    options.fillValue = fill_value;
                }
                options.zarrDelimiter = dimension_separator;
                options.zarrFormat = zarr_format;
                options.chunkKeyEncoding = chunk_key_encoding;
                options.shardShape = shards;
                options.dimensionNames = dimension_names;
                options.transposeOrder = transpose;
                options.crc32c = crc32c;
                return createDataset(root, key, dtype, shape, chunk_shape, options);
            },
            nb::arg("root"), nb::arg("key"),
            nb::arg("dtype"), nb::arg("shape"), nb::arg("chunks"),
            nb::arg("compression"),
            nb::arg("compression_options")=std::string(),
            nb::arg("fill_value")=0,
            nb::arg("dimension_separator")=".",
            nb::arg("zarr_format")=2,
            nb::arg("shards")=std::vector<std::size_t>(),
            nb::arg("chunk_key_encoding")=std::string("default"),
            nb::arg("dimension_names")=std::vector<std::optional<std::string>>(),
            nb::arg("transpose")=std::vector<std::size_t>(),
            nb::arg("crc32c")=false,
            nb::arg("fill_value_bits")=nb::none(),
            nb::arg("fill_value_string")=nb::none(),
            nb::arg("fill_value_base64")=nb::none(),
            nb::arg("fill_value_complex")=nb::none(),
            nb::call_guard<nb::gil_scoped_release>());
    }


    template<class GROUP, class FILE_>
    void exportFactoriesT(nb::module_ & m) {
        // file factories
        m.def("create_file", [](const FILE_ & file, const bool is_zarr, const int zarr_format){
            createFile(file, is_zarr, zarr_format);
        }, nb::arg("file"), nb::arg("is_zarr"), nb::arg("zarr_format")=2,
           nb::call_guard<nb::gil_scoped_release>());

        // group factories
        m.def("create_group", [](const FILE_ & file, const std::string & key, const int zarr_format){
            createGroup(file, key, zarr_format);
            return std::unique_ptr<GROUP>(new GROUP(file, key));
        }, nb::arg("file"), nb::arg("key"), nb::arg("zarr_format")=2,
           nb::call_guard<nb::gil_scoped_release>());
        m.def("create_group", [](const GROUP & group, const std::string & key, const int zarr_format){
            createGroup(group, key, zarr_format);
            return std::unique_ptr<GROUP>(new GROUP(group, key));
        }, nb::arg("group"), nb::arg("key"), nb::arg("zarr_format")=2,
           nb::call_guard<nb::gil_scoped_release>());

        // dataset factories
        exportDsFactories<GROUP>(m);
        exportDsFactories<FILE_>(m);
    }


    void exportFactory(nb::module_ & m) {
        // for filesystem
        exportFactoriesT<filesystem::handle::Group, filesystem::handle::File>(m);
        // for s3
        #ifdef WITH_S3
        exportFactoriesT<s3::handle::Group, s3::handle::File>(m);
        #endif
    }

}
