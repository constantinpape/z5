#pragma once

#include "z5/s3/handle.hxx"


namespace z5 {
namespace s3 {

namespace attrs_detail {

}

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
        if(group.isZarr()) {
            return group.in(key + "/.zgroup");
        } else {
            // TODO implement for n5
            throw std::logic_error("N5 S3 dataset not implemented yet");
            /*
            path /= "attributes.json";
            if(!fs::exists(path)) {
                return true;
            }
            nlohmann::json j;
            attrs_detail::readAttributes(path, j);
            return !z5::handle::hasAllN5DatasetAttributes(j);
            */
        }
    }

}
}
