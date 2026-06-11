#pragma once

#include "z5/metadata.hxx"
#include "z5/filesystem/attributes.hxx"
#include "z5/generic/metadata.hxx"

namespace z5 {
namespace filesystem {

namespace metadata_detail {

    inline void writeMetadata(const fs::path & path, const nlohmann::json & j) {
        std::ofstream file(path);
        file << std::setw(4) << j << std::endl;
        file.close();
    }

    inline void readMetadata(const fs::path & path, nlohmann::json & j) {
        std::ifstream file(path);
        file >> j;
        file.close();
    }

    inline bool getMetadataPath(const handle::Dataset & handle, fs::path & path) {
        const fs::path zarrPath = handle.path() / ".zarray";      // zarr v2
        const fs::path v3Path = handle.path() / "zarr.json";      // zarr v3
        const fs::path n5Path = handle.path() / "attributes.json"; // n5
        const bool hasZarr = fs::exists(zarrPath);
        const bool hasV3 = fs::exists(v3Path);
        const bool hasN5 = fs::exists(n5Path);
        if((hasZarr || hasV3) && hasN5) {
            throw std::runtime_error("Zarr and N5 specification are not both supported");
        }
        if(!hasZarr && !hasV3 && !hasN5){
            throw std::runtime_error("Invalid path: no metadata existing");
        }
        // zarr v3 takes precedence over v2 if both are somehow present
        const bool isZarr = hasZarr || hasV3;
        path = hasV3 ? v3Path : (hasZarr ? zarrPath : n5Path);
        return isZarr;
    }
}

    template<class GROUP>
    inline void writeMetadata(const z5::handle::File<GROUP> & handle, const Metadata & metadata) {
        attrs_detail::JsonIO io(handle.path());
        generic::writeFileMetadata(io, metadata);
    }


    template<class GROUP>
    inline void writeMetadata(const z5::handle::Group<GROUP> & handle, const Metadata & metadata) {
        attrs_detail::JsonIO io(handle.path());
        generic::writeGroupMetadata(io, metadata);
    }


    inline void writeMetadata(const handle::Dataset & handle, const DatasetMetadata & metadata) {
        attrs_detail::JsonIO io(handle.path());
        generic::writeDatasetMetadata(io, metadata);
    }


    template<class GROUP>
    inline void readMetadata(const z5::handle::Group<GROUP> & handle, nlohmann::json & j) {
        const bool isZarr = handle.isZarr();
        if(isZarr) {
            const fs::path v3Path = handle.path() / "zarr.json";
            if(fs::exists(v3Path)) {
                nlohmann::json jTmp;
                metadata_detail::readMetadata(v3Path, jTmp);
                j["zarr_format"] = jTmp["zarr_format"];
                if(jTmp.contains("node_type")) {
                    j["node_type"] = jTmp["node_type"];
                }
            } else {
                nlohmann::json jTmp;
                metadata_detail::readMetadata(handle.path() / ".zgroup", jTmp);
                j["zarr_format"] = jTmp["zarr_format"];
            }
        } else {
            nlohmann::json jTmp;
            metadata_detail::readMetadata(handle.path() / "attributes.json", jTmp);
            auto jIt = jTmp.find("n5");
            if(jIt != jTmp.end()) {
                j["n5"] = jIt.value();
            }
        }
    }


    inline void readMetadata(const handle::Dataset & handle, DatasetMetadata & metadata) {
        nlohmann::json j;
        fs::path path;
        auto isZarr = metadata_detail::getMetadataPath(handle, path);
        metadata_detail::readMetadata(path, j);
        metadata.fromJson(j, isZarr);
    }

}
}
