#pragma once

#include "z5/metadata.hxx"


namespace z5 {
namespace generic {

    // Backend-independent metadata WRITE logic, parameterized on the same
    // per-operation JsonIO objects as z5/generic/attributes.hxx. The read side
    // stays per-backend: the filesystem probes with cheap stats and guards
    // against mixed zarr+n5 metadata, while s3 minimizes HEAD round trips.

    // write zarr v3 group metadata (zarr.json with node_type "group"),
    // preserving any inline user attributes already on store
    template<class IO>
    inline void writeV3GroupMetadata(const IO & io, const Metadata & metadata) {
        nlohmann::json j;
        io.read("zarr.json", j);
        j["zarr_format"] = 3;
        j["node_type"] = "group";
        if(j.find("attributes") == j.end()) {
            j["attributes"] = nlohmann::json::object();
        }
        io.writePretty("zarr.json", j);
    }


    template<class IO>
    inline void writeFileMetadata(const IO & io, const Metadata & metadata) {
        if(metadata.isZarr && metadata.zarrFormat == 3) {
            writeV3GroupMetadata(io, metadata);
            return;
        }
        nlohmann::json j;
        if(metadata.isZarr) {
            j["zarr_format"] = metadata.zarrFormat;
            io.writePretty(".zgroup", j);
        } else {
            // n5 stores attributes and metadata in the same file,
            // so we need to make sure that we don't overwrite attributes
            // (read leaves j untouched if there are none)
            io.read("attributes.json", j);
            j["n5"] = metadata.n5Format();
            io.writePretty("attributes.json", j);
        }
    }


    template<class IO>
    inline void writeGroupMetadata(const IO & io, const Metadata & metadata) {
        if(metadata.isZarr && metadata.zarrFormat == 3) {
            writeV3GroupMetadata(io, metadata);
            return;
        }
        if(metadata.isZarr) {
            nlohmann::json j;
            j["zarr_format"] = metadata.zarrFormat;
            io.writePretty(".zgroup", j);
        }
        // we don't need to write metadata for n5 groups
    }


    template<class IO>
    inline void writeDatasetMetadata(const IO & io, const DatasetMetadata & metadata) {
        if(metadata.isZarr && metadata.zarrFormat == 3) {
            // preserve inline user attributes already on store
            nlohmann::json existing;
            const bool hasExisting = io.read("zarr.json", existing);
            nlohmann::json j;
            metadata.toJson(j);
            if(hasExisting && existing.contains("attributes")) {
                j["attributes"] = existing["attributes"];
            }
            io.writePretty("zarr.json", j);
            return;
        }
        nlohmann::json j;
        metadata.toJson(j);
        io.writePretty(metadata.isZarr ? ".zarray" : "attributes.json", j);
    }

}
}
