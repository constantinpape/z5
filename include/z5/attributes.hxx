#pragma once

#include <fstream>
#include "nlohmann/json.hpp"
#include "z5/handle/handle.hxx"

#ifdef WITH_BOOST_FS
    namespace fs = boost::filesystem;
#else
    #if __GCC__ > 7
        namespace fs = std::filesystem;
    #else
        namespace fs = std::experimental::filesystem;
    #endif
#endif

namespace z5 {

namespace attrs_detail {

    inline void readAttributes(fs::path & path, const std::vector<std::string> & keys, nlohmann::json & j) {
        nlohmann::json jTmp;
        #ifdef WITH_BOOST_FS
        fs::ifstream file(path);
        #else
        std::ifstream file(path);
        #endif
        file >> jTmp;
        file.close();
        for(const auto & key : keys) {
            try {
                j[key] = jTmp[key];
            }
            catch(std::out_of_range) {
                throw std::runtime_error("Cannot read attribute: Key does not exist");
            }
        }
    }

    inline void readAttributes(fs::path & path, nlohmann::json & j) {
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

    inline void readAttributes(
        const handle::Handle & handle,
        const std::vector<std::string> & keys,
        nlohmann::json & j
    ) {
        bool isZarr = handle.isZarr();
        auto path = handle.path();
        path /= isZarr ? ".zattrs" : "attributes.json";
        if(!fs::exists(path)) {
            throw std::runtime_error("Cannot read attributes: no attributes exist");
        }
        attrs_detail::readAttributes(path, keys, j);
    }

    inline void readAttributes(
        const handle::Handle & handle,
        nlohmann::json & j
    ) {
        bool isZarr = handle.isZarr();
        auto path = handle.path();
        path /= isZarr ? ".zattrs" : "attributes.json";
        if(!fs::exists(path)) {
            throw std::runtime_error("Cannot read attributes: no attributes exist");
        }
        attrs_detail::readAttributes(path, j);
    }

    inline void writeAttributes(const handle::Handle & handle, const nlohmann::json & j) {
        bool isZarr = handle.isZarr();
        auto path = handle.path();
        path /= isZarr ? ".zattrs" : "attributes.json";
        attrs_detail::writeAttributes(path, j);
    }
}
