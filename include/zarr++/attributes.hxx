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

    // N5 only supports attributes for arrays ?!
    // TODO implement
    template<typename T>
    void readAttributeN5(fs::path & path, const std::string & key, T & outVal) {
    }

    // N5 only supports attributes for arrays ?!
    // TODO implement
    template<typename T>
    void writeAttributeN5(fs::path & path, const std::string & key, const T & inVal) {
    }


    // Zarr supports attributes for arrays and groups
    template<typename T>
    void readAttributeZarr(fs::path & path, const std::string & key, T & outVal) {
        path /= ".zattrs";
        if(!fs::exists(path)) {
            throw std::runtime_error("Cannot read attribute: Array does not have any attributes");
        }
        nlohmann::json j;
        fs::ifstream file(path);
        file >> j;
        file.close();
        try {
            outVal = j[key];
        }
        catch(std::out_of_range) {
            throw std::runtime_error("Cannot read attribute: Key does not exist");
        }
    }

    // Zarr supports attributes for arrays and groups
    template<typename T>
    void writeAttributeZarr(fs::path & path, const std::string & key, const T & inVal) {
        path /= ".zattrs";
        nlohmann::json j;
        // if we already have attributes, read them
        if(fs::exists(path)) {
            fs::ifstream file(path);
            file >> j;
            file.close();
        }
        j[key] = inVal;
        fs::ofstream file(path);
        file << j;
        file.close();
    }
}

    // TODO if I figure out boost::any, could use it instead of templates
    template<typename T>
    void readAttribute(const handle::Handle & handle, const std::string & key, T & outVal) {
        bool isZarr = attrs_detail::checkHandle(handle);
        if(isZarr) {
            attrs_detail::readAttributeZarr(handle.path(), key, outVal);
        } else {
            attrs_detail::readAttributeN5(handle.path(), key, outVal);
        }
    }

    // TODO if I figure out boost::any, could use it instead of templates
    template<typename T>
    void writeAttribute(const handle::Handle & handle, const std::string & key, const T & inVal) {
        bool isZarr = attrs_detail::checkHandle(handle);
        if(isZarr) {
            attrs_detail::writeAttributeZarr(handle.path(), key, inVal);
        } else {
            attrs_detail::writeAttributeN5(handle.path(), key, inVal);
        }
    }

}
