#include <set>
#include <map>
#include <algorithm>

#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/string.h>

#include "z5/dataset.hxx"
#include "z5/util/functions.hxx"

namespace nb = nanobind;

namespace z5 {

    template<class T>
    void exportUtilsT(nb::module_ & module, const std::string & dtype) {


        // export remove trivial chunks functionality
        std::string fname = "remove_trivial_chunks_" + dtype;
        module.def(fname.c_str(), [](const Dataset & ds,
                                     const int n_threads,
                                     const bool remove_specific_value,
                                     const T value){
            util::removeTrivialChunks(ds, n_threads, remove_specific_value, value);
        }, nb::arg("ds"), nb::arg("n_threads"),
           nb::arg("remove_trivial_chunks")=false,
           nb::arg("value")=0,
           nb::call_guard<nb::gil_scoped_release>());


        // export unique functionality
        fname = "unique_" + dtype;
        module.def(fname.c_str(), [](const Dataset & ds,
                                     const int n_threads){
            std::set<T> unique_set;
            util::unique(ds, n_threads, unique_set);

            const std::size_t n = unique_set.size();
            T * data = new T[n];
            std::copy(unique_set.begin(), unique_set.end(), data);

            nb::capsule owner(data, [](void * p) noexcept { delete[] static_cast<T*>(p); });
            const std::size_t shape[1] = {n};
            return nb::ndarray<nb::numpy, T>(data, 1, shape, owner);
        }, nb::arg("ds"), nb::arg("n_threads"));


        // export unique with counts functionality
        fname = "unique_with_counts_" + dtype;
        module.def(fname.c_str(), [](const Dataset & ds,
                                     const int n_threads){
            std::map<T, std::size_t> unique_map;
            util::uniqueWithCounts(ds, n_threads, unique_map);

            const std::size_t n = unique_map.size();
            T * uniques = new T[n];
            std::size_t * counts = new std::size_t[n];
            std::size_t index = 0;
            for(auto it = unique_map.begin(); it != unique_map.end(); ++it, ++index) {
                uniques[index] = it->first;
                counts[index] = it->second;
            }

            nb::capsule uniques_owner(uniques, [](void * p) noexcept { delete[] static_cast<T*>(p); });
            nb::capsule counts_owner(counts, [](void * p) noexcept { delete[] static_cast<std::size_t*>(p); });
            const std::size_t shape[1] = {n};
            return std::make_pair(
                nb::ndarray<nb::numpy, T>(uniques, 1, shape, uniques_owner),
                nb::ndarray<nb::numpy, std::size_t>(counts, 1, shape, counts_owner)
            );
        }, nb::arg("ds"), nb::arg("n_threads"));
    }


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

        // export remove dataset and remove chunk
        module.def("remove_dataset", &util::removeDataset,
                   nb::arg("ds"), nb::arg("n_threads"),
                   nb::call_guard<nb::gil_scoped_release>());

        exportFileMode(module);
    }


}
