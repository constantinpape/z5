#pragma once

#include "json.hpp"
#include "zarr++/handle/handle.hxx"

namespace fs = boost::filesystem;

namespace zarr {

namespace attrs_detail {

    bool checkHandle(const handle::Handle & handle) {

        bool isZarr = handle.isZarr();
        bool isN5 = handle.isN5();

        if(isZarr && isN5) {
            throw std::runtime_error("Same file with both Zarr and N5 specification is not supported");
        }

        if( !(isZarr || isN5) ) {
            throw std::runtime_error("File was not initialised to either Zarr or N5 format");
        }

        return isZarr;
    }


    void readAttributes(fs::path & path, const std::vector<std::string> & keys, nlohmann::json & j) {
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


    void writeAttributes(const fs::path & path, const nlohmann::json & j) {
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

    void readAttributes(
        const handle::Handle & handle,
        const std::vector<std::string> & keys,
        nlohmann::json & j
    ) {
        bool isZarr = attrs_detail::checkHandle(handle);
        auto path = handle.path();
        path /= isZarr ? ".zattrs" : "attributes.json";
        if(!fs::exists(path)) {
            throw std::runtime_error("Cannot read attributes: no attributes exist");
        }
        attrs_detail::readAttributes(path, keys, j);
    }


    void writeAttributes(const handle::Handle & handle, const nlohmann::json & j) {      
        bool isZarr = attrs_detail::checkHandle(handle);
        auto path = handle.path();
        path /= isZarr ? ".zattrs" : "attributes.json";
        attrs_detail::writeAttributes(path, j);
    }
}
