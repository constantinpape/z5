#pragma once

#include <fstream>
#include <iomanip>
#include "z5/filesystem/handle.hxx"


namespace z5 {
namespace filesystem {


namespace attrs_detail {

    inline void readAttributes(const fs::path & path, nlohmann::json & j) {
        if(!fs::exists(path)) {
            return;
        }
        std::ifstream file(path);
        file >> j;
        file.close();
    }

    inline void writeAttributes(const fs::path & path, const nlohmann::json & j) {
        nlohmann::json jOut;
        // if we already have attributes, read them
        if(fs::exists(path)) {
            std::ifstream file(path);
            file >> jOut;
            file.close();
        }
        for(auto jIt = j.begin(); jIt != j.end(); ++jIt) {
            jOut[jIt.key()] = jIt.value();
        }
        std::ofstream file(path);
        file << jOut;
        file.close();
    }

    inline void removeAttribute(const fs::path & path, const std::string & key) {
        nlohmann::json jOut;
        // if we already have attributes, read them
        if(fs::exists(path)) {
            std::ifstream file(path);
            file >> jOut;
            file.close();
        }
        else {
            return;
        }
        jOut.erase(key);
        std::ofstream file(path);
        file << jOut;
        file.close();
    }

    //
    // zarr v3: user attributes live inline in zarr.json under the "attributes" key
    //
    inline void readV3Attributes(const fs::path & zarrJson, nlohmann::json & j) {
        if(!fs::exists(zarrJson)) {
            return;
        }
        nlohmann::json full;
        std::ifstream file(zarrJson);
        file >> full;
        file.close();
        if(full.contains("attributes")) {
            j = full["attributes"];
        }
    }

    inline void writeV3Attributes(const fs::path & zarrJson, const nlohmann::json & j) {
        nlohmann::json full;
        if(fs::exists(zarrJson)) {
            std::ifstream file(zarrJson);
            file >> full;
            file.close();
        }
        nlohmann::json attrs = full.contains("attributes") ? full["attributes"] : nlohmann::json::object();
        for(auto jIt = j.begin(); jIt != j.end(); ++jIt) {
            attrs[jIt.key()] = jIt.value();
        }
        full["attributes"] = attrs;
        std::ofstream file(zarrJson);
        file << std::setw(4) << full << std::endl;
        file.close();
    }

    inline void removeV3Attribute(const fs::path & zarrJson, const std::string & key) {
        if(!fs::exists(zarrJson)) {
            return;
        }
        nlohmann::json full;
        {
            std::ifstream file(zarrJson);
            file >> full;
            file.close();
        }
        if(full.contains("attributes")) {
            full["attributes"].erase(key);
        }
        std::ofstream file(zarrJson);
        file << std::setw(4) << full << std::endl;
        file.close();
    }

    // true if this zarr container path uses zarr v3 (zarr.json present)
    inline bool isV3(const fs::path & containerPath) {
        return fs::exists(containerPath / "zarr.json");
    }
}

    template<class GROUP>
    inline void readAttributes(const z5::handle::Group<GROUP> & group, nlohmann::json & j
    ) {
        if(group.isZarr() && attrs_detail::isV3(group.path())) {
            attrs_detail::readV3Attributes(group.path() / "zarr.json", j);
            return;
        }
        const auto path = group.path() / (group.isZarr() ? ".zattrs" : "attributes.json");
        attrs_detail::readAttributes(path, j);
    }

    template<class GROUP>
    inline void writeAttributes(const z5::handle::Group<GROUP> & group, const nlohmann::json & j) {
        if(group.isZarr() && attrs_detail::isV3(group.path())) {
            attrs_detail::writeV3Attributes(group.path() / "zarr.json", j);
            return;
        }
        const auto path = group.path() / (group.isZarr() ? ".zattrs" : "attributes.json");
        attrs_detail::writeAttributes(path, j);
    }

    template<class GROUP>
    inline void removeAttribute(const z5::handle::Group<GROUP> & group, const std::string & key) {
        if(group.isZarr() && attrs_detail::isV3(group.path())) {
            attrs_detail::removeV3Attribute(group.path() / "zarr.json", key);
            return;
        }
        const auto path = group.path() / (group.isZarr() ? ".zattrs" : "attributes.json");
        attrs_detail::removeAttribute(path, key);
    }


    template<class DATASET>
    inline void readAttributes(const z5::handle::Dataset<DATASET> & ds, nlohmann::json & j
    ) {
        if(ds.isZarr() && attrs_detail::isV3(ds.path())) {
            attrs_detail::readV3Attributes(ds.path() / "zarr.json", j);
            return;
        }
        const auto path = ds.path() / (ds.isZarr() ? ".zattrs" : "attributes.json");
        attrs_detail::readAttributes(path, j);
    }

    template<class DATASET>
    inline void writeAttributes(const z5::handle::Dataset<DATASET> & ds, const nlohmann::json & j) {
        if(ds.isZarr() && attrs_detail::isV3(ds.path())) {
            attrs_detail::writeV3Attributes(ds.path() / "zarr.json", j);
            return;
        }
        const auto path = ds.path() / (ds.isZarr() ? ".zattrs" : "attributes.json");
        attrs_detail::writeAttributes(path, j);
    }

    template<class DATASET>
    inline void removeAttribute(const z5::handle::Dataset<DATASET> & ds, const std::string & key) {
        if(ds.isZarr() && attrs_detail::isV3(ds.path())) {
            attrs_detail::removeV3Attribute(ds.path() / "zarr.json", key);
            return;
        }
        const auto path = ds.path() / (ds.isZarr() ? ".zattrs" : "attributes.json");
        attrs_detail::removeAttribute(path, key);
    }


    template<class GROUP>
    inline bool isSubGroup(const z5::handle::Group<GROUP> & group, const std::string & key){
        fs::path path = group.path() / key;
        if(!fs::exists(path)) {
            return false;
        }
        if(group.isZarr()) {
            // zarr v3: zarr.json distinguishes group vs array via node_type
            const fs::path v3 = path / "zarr.json";
            if(fs::exists(v3)) {
                nlohmann::json j;
                attrs_detail::readAttributes(v3, j);
                return j.value("node_type", std::string("group")) == "group";
            }
            // zarr v2
            return fs::exists(path / ".zgroup");
        } else {
            path /= "attributes.json";
            if(!fs::exists(path)) {
                return true;
            }
            nlohmann::json j;
            attrs_detail::readAttributes(path, j);
            return !z5::handle::hasAllN5DatasetAttributes(j);
        }
    }

}
}
