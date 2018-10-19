#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <iostream>

#include "z5/dataset.hxx"
#include "z5/util/functions.hxx"

// for xtensor numpy bindings
#include "xtensor-python/pyarray.hpp"
#include "xtensor-python/pytensor.hpp"

namespace py = pybind11;

namespace z5 {

    template<class T>
    void exportUtilsT(py::module & module, const std::string & dtype) {


        // export remove trivial chunks functionality
        std::string fname = "remove_trivial_chunks_" + dtype;
        module.def(fname.c_str(), [](const Dataset & ds,
                                     const int n_threads,
                                     const bool remove_specific_value,
                                     const T value){
            util::removeTrivialChunks(ds, n_threads, remove_specific_value, value);
        }, py::arg("ds"), py::arg("n_threads"),
           py::arg("remove_trivial_chunks")=false,
           py::arg("value")=0,
           py::call_guard<py::gil_scoped_release>());


        // export unique functionality
        fname = "unique_" + dtype;
        module.def(fname.c_str(), [](const Dataset & ds,
                                     const int n_threads){
            std::set<T> unique_set;
            util::unique(ds, n_threads, unique_set);

            typedef typename xt::pytensor<T, 1, xt::layout_type::row_major>::shape_type ShapeType;
            const ShapeType shape = {static_cast<int64_t>(unique_set.size())};
            xt::pytensor<T, 1, xt::layout_type::row_major> uniques = xt::zeros<T>(shape);
            std::copy(unique_set.begin(), unique_set.end(), uniques.begin());
            return uniques;
        }, py::arg("ds"), py::arg("n_threads"));


        // export unique with counts functionality
        fname = "unique_with_counts_" + dtype;
        module.def(fname.c_str(), [](const Dataset & ds,
                                     const int n_threads){
            std::map<T, std::size_t> unique_map;
            util::uniqueWithCounts(ds, n_threads, unique_map);
            typedef typename xt::pytensor<T, 1, xt::layout_type::row_major>::shape_type ShapeType;
            const ShapeType shape = {static_cast<int64_t>(unique_map.size())};

            xt::pytensor<T, 1, xt::layout_type::row_major> uniques = xt::zeros<T>(shape);
            xt::pytensor<std::size_t, 1, xt::layout_type::row_major> counts = xt::zeros<std::size_t>(shape);
            std::size_t index = 0;
            for(auto it = unique_map.begin(); it != unique_map.end(); ++it, ++index) {
                uniques[index] = it->first;
                counts[index] = it->second;
            }
            return std::make_pair(uniques, counts);
        }, py::arg("ds"), py::arg("n_threads"));
    }


    void exportUtils(py::module & module) {
        exportUtilsT<uint8_t>(module, "uint8");
        exportUtilsT<uint16_t>(module, "uint16");
        exportUtilsT<uint32_t>(module, "uint32");
        exportUtilsT<uint64_t>(module, "uint64");

        exportUtilsT<int8_t>(module, "int8");
        exportUtilsT<int16_t>(module, "int16");
        exportUtilsT<int32_t>(module, "int32");
        exportUtilsT<int64_t>(module, "int64");

        exportUtilsT<float>(module, "float32");
        exportUtilsT<double>(module, "float64");

        // export remove dataset
        module.def("remove_dataset", &util::removeDataset,
                   py::arg("ds"), py::arg("n_threads"),
                   py::call_guard<py::gil_scoped_release>());
    }



}
