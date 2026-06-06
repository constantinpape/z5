#include <nanobind/nanobind.h>
#include <nanobind/stl/map.h>
#include <nanobind/stl/string.h>

#include "z5/common.hxx"

namespace nb = nanobind;


namespace z5 {

    // export functions for the submodules
    void exportAttributes(nb::module_ &);
    void exportDataset(nb::module_ &);
    void exportFactory(nb::module_ &);
    void exportHandles(nb::module_ &);
    void exportUtils(nb::module_ &);

    void exportCompilerFlags(nb::module_ & m) {
        m.def("get_available_codecs", &getAvailableCodecs);
        m.def("get_available_backends", &getAvailableBackends);
    }
}


NB_MODULE(_z5py, module) {
    module.doc() = "z5py: z5 python bindings";

    using namespace z5;
    exportAttributes(module);
    exportCompilerFlags(module);
    exportDataset(module);
    exportHandles(module);
    exportFactory(module);
    exportUtils(module);
}
