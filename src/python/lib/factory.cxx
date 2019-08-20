#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "z5/factory.hxx"

namespace py = pybind11;

namespace z5 {


    // TODO exporting create_dataset fails right now.
    // I think this is due to boost::any. try boost::variant / std::variant instead
    template<class GROUP>
    void exportDsFactories(py::module & m) {
        m.def("open_dataset", &openDataset<GROUP>, py::arg("root"), py::arg("key"));
        /*
        m.def("create_Dataset", &createDataset<GROUP>, py::arg("root"), py::arg("key"),
              py::arg("dtype"), py::arg("shape"), py::arg("chunk_shape"),
              py::arg("compression"), py::arg("compression_options"), py::arg("fill_value"));
        */
        /*
        m.def("create_dataset", [](const GROUP & root, const std::string & key,
                                   const std::string & dtype, const std::vector<uint64_t>  & shape,
                                   const std::vector<uint64_t> & chunk_shape,
                                   const std::string & compression,
                                   // TODO need boost variant?
                                   const std::map<> copts,
                                   ),
              py::arg("root"), py::arg("key"),
              py::arg("dtype"), py::arg("shape"), py::arg("chunk_shape"),
              py::arg("compression"), py::arg("compression_options"), py::arg("fill_value"));
        */
    }


    // TODO do we need to give different names for the functions to resolve calls in python
    template<class GROUP, class FILE_>
    void exportFactoriesT(py::module & m) {
        // file factories
        m.def("create_file", &createFile<FILE_>);

        // group factories
        m.def("create_group", &createGroup<FILE_>);
        m.def("create_group", &createGroup<GROUP>);

        // dataset factories
        exportDsFactories<GROUP>(m);
        exportDsFactories<FILE_>(m);
    }


    void exportFactory(py::module & m) {
        // for filesystem
        exportFactoriesT<filesystem::handle::Group, filesystem::handle::File>(m);
        // for s3
        #ifdef WITH_S3
        // exportFactoriesT<s3::handle::Group, s3::handle::File>(m);
        #endif
    }

}
