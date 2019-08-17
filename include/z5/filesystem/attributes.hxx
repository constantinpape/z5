#pragma once

#include <fstream>
#include "z5/filesystem/handle.hxx"


namespace z5 {
namespace filesystem {


namespace attrs_detail {

    inline void readAttributes(const fs::path & path, nlohmann::json & j) {
        if(!fs::exists(path)) {
            throw std::runtime_error("Cannot read attributes: no attributes exist");
        }
        #ifdef WITH_BOOST_FS
        fs::ifstream file(path);
        #else
        std::ifstream file(path);
        #endif
        file >> j;
        file.close();
    }

    inline void writeAttributes(const fs::path & path, const nlohmann::json & j) {
        nlohmann::json jOut;
        // if we already have attributes, read them
        if(fs::exists(path)) {
            #ifdef WITH_BOOST_FS
            fs::ifstream file(path);
            #else
            std::ifstream file(path);
            #endif
            file >> jOut;
            file.close();
        }
        for(auto jIt = j.begin(); jIt != j.end(); ++jIt) {
            jOut[jIt.key()] = jIt.value();
        }
        #ifdef WITH_BOOST_FS
        fs::ofstream file(path);
        #else
        std::ofstream file(path);
        #endif
        file << jOut;
        file.close();
    }
}

    template<class GROUP>
    inline void readAttributes(const z5::handle::Group<GROUP> & group, nlohmann::json & j
    ) {
        const auto path = group.path() / (group.isZarr() ? ".zattrs" : "attributes.json");
        attrs_detail::readAttributes(path, j);
    }

    template<class GROUP>
    inline void writeAttributes(const z5::handle::Group<GROUP> & group, const nlohmann::json & j) {
        const auto path = group.path() / (group.isZarr() ? ".zattrs" : "attributes.json");
        attrs_detail::writeAttributes(path, j);
    }


    template<class DATASET>
    inline void readAttributes(const z5::handle::Dataset<DATASET> & ds, nlohmann::json & j
    ) {
        const auto path = ds.path() / (ds.isZarr() ? ".zattrs" : "attributes.json");
        attrs_detail::readAttributes(path, j);
    }

    template<class DATASET>
    inline void writeAttributes(const z5::handle::Dataset<DATASET> & ds, const nlohmann::json & j) {
        const auto path = ds.path() / (ds.isZarr() ? ".zattrs" : "attributes.json");
        attrs_detail::writeAttributes(path, j);
    }

}
}
