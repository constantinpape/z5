#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "z5/factory.hxx"
#include "variant_cast.hxx"

namespace py = pybind11;

namespace z5 {


    template<class GROUP>
    void exportDsFactories(py::module & m) {
        m.def("open_dataset", [](const GROUP & root, const std::string & key){
            return openDataset(root, key);
        },
        py::arg("root"), py::arg("key"));

        m.def("create_dataset", [](const GROUP & root, const std::string & key,
                                   const std::string & dtype,
                                   const std::vector<std::size_t>  & shape,
                                   const std::vector<std::size_t> & chunk_shape,
                                   const std::string & compression,
                                   const types::CompressionOptions & copts,
                                   const double fill_value){
                return createDataset(root, key, dtype, shape, chunk_shape, compression, copts, fill_value);
            },
            py::arg("root"), py::arg("key"),
            py::arg("dtype"), py::arg("shape"), py::arg("chunks"),
            py::arg("compression"),
            py::arg("compression_options")=types::CompressionOptions(),
            py::arg("fill_value")=0);
    }


    template<class GROUP, class FILE_>
    void exportFactoriesT(py::module & m) {
        // file factories
        m.def("create_file", [](const FILE_ & file, const bool is_zarr){
            createFile(file, is_zarr);
        }, py::arg("file"), py::arg("is_zarr"));

        // group factories
        m.def("create_group", [](const FILE_ & file, const std::string & key){
            createGroup(file, key);
            return std::unique_ptr<GROUP>(new GROUP(file, key));
        });
        m.def("create_group", [](const GROUP & group, const std::string & key){
            createGroup(group, key);
            return std::unique_ptr<GROUP>(new GROUP(group, key));
        });

        // dataset factories
        exportDsFactories<GROUP>(m);
        exportDsFactories<FILE_>(m);
    }


    void exportFactory(py::module & m) {
        // for filesystem
        exportFactoriesT<filesystem::handle::Group, filesystem::handle::File>(m);
        // for s3
        #ifdef WITH_S3
        exportFactoriesT<s3::handle::Group, s3::handle::File>(m);
        #endif
    }

}
