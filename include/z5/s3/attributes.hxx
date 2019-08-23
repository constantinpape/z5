#pragma once

#include "z5/s3/handle.hxx"


namespace z5 {
namespace s3 {

    // TODO implement for s3

    template<class GROUP>
    inline void readAttributes(const z5::handle::Group<GROUP> & group, nlohmann::json & j
    ) {
    }

    template<class GROUP>
    inline void writeAttributes(const z5::handle::Group<GROUP> & group, const nlohmann::json & j) {
    }

    template<class GROUP>
    inline void removeAttribute(const z5::handle::Group<GROUP> & group, const std::string & key) {
    }


    template<class DATASET>
    inline void readAttributes(const z5::handle::Dataset<DATASET> & ds, nlohmann::json & j
    ) {
    }

    template<class DATASET>
    inline void writeAttributes(const z5::handle::Dataset<DATASET> & ds, const nlohmann::json & j) {
    }

    template<class DATASET>
    inline void removeAttribute(const z5::handle::Dataset<DATASET> & ds, const std::string & key) {
    }


    template<class GROUP>
    inline bool isSubGroup(const z5::handle::Group<GROUP> & group, const std::string & key){
    }

}
}
