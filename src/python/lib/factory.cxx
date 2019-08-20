#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "z5/factory.hxx"


namespace z5 {

    /*
    void exportGroups(py::module & module) {
        module.def("create_group",[](const std::string & path,
                                     const bool isZarr,
                                     const FileMode::modes mode){
            handle::Group h(path, mode);
            createGroup(h, isZarr);
        });

        module.def("create_sub_group",[](const std::string & path,
                                         const std::string & key,
                                         const bool isZarr){
            handle::Group h(path);
            createGroup(h, key, isZarr);
        });
    }

    module.def("open_dataset",[](const std::string & path, const FileMode::modes mode){
        return openDataset(path, mode);
    });
    */


    void exportBaseHandles(py::module & module) {

    }


    void exportFactory(py::module & module) {
    }

}
