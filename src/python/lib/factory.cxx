#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/unique_ptr.h>

#include "z5/factory.hxx"

namespace nb = nanobind;

namespace z5 {


    template<class GROUP>
    void exportDsFactories(nb::module_ & m) {
        m.def("open_dataset", [](const GROUP & root, const std::string & key){
            return openDataset(root, key);
        },
        nb::arg("root"), nb::arg("key"));

        m.def("create_dataset", [](const GROUP & root, const std::string & key,
                                   const std::string & dtype,
                                   const std::vector<std::size_t>  & shape,
                                   const std::vector<std::size_t> & chunk_shape,
                                   const std::string & compression,
                                   const std::string & copts,
                                   const double fill_value,
                                   const std::string & dimension_separator){
                const nlohmann::json j = nlohmann::json::parse(copts);
                return createDataset(root, key, dtype, shape, chunk_shape, compression, j, fill_value, dimension_separator);
            },
            nb::arg("root"), nb::arg("key"),
            nb::arg("dtype"), nb::arg("shape"), nb::arg("chunks"),
            nb::arg("compression"),
            nb::arg("compression_options")=std::string(),
            nb::arg("fill_value")=0,
            nb::arg("dimension_separator")=".");
    }


    template<class GROUP, class FILE_>
    void exportFactoriesT(nb::module_ & m) {
        // file factories
        m.def("create_file", [](const FILE_ & file, const bool is_zarr){
            createFile(file, is_zarr);
        }, nb::arg("file"), nb::arg("is_zarr"));

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


    void exportFactory(nb::module_ & m) {
        // for filesystem
        exportFactoriesT<filesystem::handle::Group, filesystem::handle::File>(m);
        // for s3
        #ifdef WITH_S3
        exportFactoriesT<s3::handle::Group, s3::handle::File>(m);
        #endif
    }

}
