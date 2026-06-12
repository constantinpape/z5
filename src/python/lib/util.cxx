#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include "z5/dataset.hxx"
#include "z5/util/functions.hxx"

namespace nb = nanobind;

namespace z5 {

    // expose file mode to python
    void exportFileMode(nb::module_ & module) {
        nb::class_<FileMode> pyFileMode(module, "FileMode");

        // expose class
        pyFileMode
            .def(nb::init<FileMode::modes>(), nb::arg("mode"))
            .def("can_write", &FileMode::canWrite)
            .def("can_create", &FileMode::canCreate)
            .def("must_not_exist", &FileMode::mustNotExist)
            .def("should_truncate", &FileMode::shouldTruncate)
            .def("mode", &FileMode::printMode)
        ;

        // expose enum
        nb::enum_<FileMode::modes>(pyFileMode, "modes")
            .value("r", FileMode::modes::r)
            .value("r_p", FileMode::modes::r_p)
            .value("w", FileMode::modes::w)
            .value("w_m", FileMode::modes::w_m)
            .value("a", FileMode::modes::a)
            .export_values()
        ;
    }


    void exportUtils(nb::module_ & module) {
        // remove dataset (used by Group.__delitem__)
        module.def("remove_dataset", &util::removeDataset,
                   nb::arg("ds"), nb::arg("n_threads"),
                   nb::call_guard<nb::gil_scoped_release>());

        exportFileMode(module);
    }


}
