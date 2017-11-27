#include <pybind11/pybind11.h>

// for xtensor numpy bindings
#ifndef FORCE_IMPORT_ARRAY
#define FORCE_IMPORT_ARRAY
#endif
#include "xtensor-python/pyarray.hpp"

namespace py = pybind11;

namespace z5 {
    
    void addArrays(xt::pyarray<int> & a, xt::pyarray<int> & b) {
        auto c = a + b;
        std::cout << "Added up arrays" << std::endl;;
    }

    void exportAdd(py::module & module) {
        module.def("add_arrays", &addArrays);
    }

}
