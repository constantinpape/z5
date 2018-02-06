#pragma once

#include "nlohmann/json.hpp"
#include "z5/handle/handle.hxx"

namespace fs = boost::filesystem;

namespace z5 {

namespace attrs_detail {

    inline void readAttributes(fs::path & path, const std::vector<std::string> & keys, nlohmann::json & j) {
        nlohmann::json jTmp;
        fs::ifstream file(path);
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


    inline void writeAttributes(const fs::path & path, const nlohmann::json & j) {
        nlohmann::json jOut;
        // if we already have attributes, read them
        if(fs::exists(path)) {
            fs::ifstream file(path);
            file >> jOut;
            file.close();
        }
        for(auto jIt = j.begin(); jIt != j.end(); ++jIt) {
            jOut[jIt.key()] = jIt.value();
        }
        fs::ofstream file(path);
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


    inline void writeAttributes(const handle::Handle & handle, const nlohmann::json & j) {
        bool isZarr = handle.isZarr();
        auto path = handle.path();
        path /= isZarr ? ".zattrs" : "attributes.json";
        attrs_detail::writeAttributes(path, j);
    }
}
