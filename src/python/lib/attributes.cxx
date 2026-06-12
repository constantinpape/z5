#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include "z5/attributes.hxx"


namespace nb = nanobind;

namespace z5 {


    template<class OBJECT>
    void exportAttributesT(nb::module_ & m) {
        // attribute IO does file / network round trips (one per call for the S3
        // backend) and only handles C++ types -> release the GIL
        m.def("write_attributes", [](const OBJECT & g, const std::string & attrs){
            const nlohmann::json j = nlohmann::json::parse(attrs);
            writeAttributes(g, j);
        }, nb::call_guard<nb::gil_scoped_release>());

        m.def("read_attributes", [](const OBJECT & g){
            nlohmann::json j;
            readAttributes(g, j);
            return j.dump();
        }, nb::call_guard<nb::gil_scoped_release>());

        m.def("remove_attribute", [](const OBJECT & g, const std::string & key) {
            removeAttribute(g, key);
        }, nb::call_guard<nb::gil_scoped_release>());
    }


    void exportAttributes(nb::module_ & m) {

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
