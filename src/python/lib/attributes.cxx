#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "z5/attributes.hxx"


namespace py = pybind11;

namespace z5 {


    template<class OBJECT>
    void exportAttributesT(py::module & m) {
        m.def("write_attributes", [](const OBJECT & g, const std::string & attrs){
            const nlohmann::json j = nlohmann::json::parse(attrs);
            writeAttributes(g, j);
        });

        m.def("read_attributes", [](const OBJECT & g){
            nlohmann::json j;
            readAttributes(g, j);
            return j.dump();
        });

        m.def("remove_attribute", [](const OBJECT & g, const std::string & key) {
            removeAttribute(g, key);
        });
    }


    void exportAttributes(py::module & m) {

        exportAttributesT<filesystem::handle::File>(m);
        exportAttributesT<filesystem::handle::Group>(m);
        exportAttributesT<filesystem::handle::Dataset>(m);

        #ifdef WITH_S3
        exportAttributesT<s3::handle::File>(m);
        exportAttributesT<s3::handle::Group>(m);
        exportAttributesT<s3::handle::Dataset>(m);
        #endif
    }

}
