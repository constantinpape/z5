#pragma once

#include "z5/s3/handle.hxx"
#include "z5/s3/attributes.hxx"


namespace z5 {
namespace s3 {

namespace meta_detail {

    // write zarr v3 group metadata (zarr.json with node_type "group"), preserving
    // any inline user attributes already stored there.
    inline void writeV3GroupMetadata(Aws::S3::S3Client & client, const std::string & bucket,
                                     const std::string & base, const Metadata & metadata) {
        const std::string key = detail::joinKey(base, "zarr.json");
        nlohmann::json j = nlohmann::json::object();
        attrs_detail::readJson(client, bucket, key, j);
        j["zarr_format"] = 3;
        j["node_type"] = "group";
        if(j.find("attributes") == j.end()) {
            j["attributes"] = nlohmann::json::object();
        }
        attrs_detail::writeJson(client, bucket, key, j);
    }

}  // namespace meta_detail


    template<class GROUP>
    inline void writeMetadata(const z5::handle::File<GROUP> & handle, const Metadata & metadata) {
        auto client = detail::makeClient(handle);
        const auto & bucket = handle.bucketName();
        const std::string & base = handle.nameInBucket();
        const bool isZarr = metadata.isZarr;
        if(isZarr && metadata.zarrFormat == 3) {
            meta_detail::writeV3GroupMetadata(client, bucket, base, metadata);
            return;
        }
        if(isZarr) {
            nlohmann::json j;
            j["zarr_format"] = metadata.zarrFormat;
            attrs_detail::writeJson(client, bucket, detail::joinKey(base, ".zgroup"), j);
        } else {
            // n5 stores attributes and metadata in the same file: preserve attributes
            nlohmann::json j = nlohmann::json::object();
            attrs_detail::readJson(client, bucket, detail::joinKey(base, "attributes.json"), j);
            j["n5"] = metadata.n5Format();
            attrs_detail::writeJson(client, bucket, detail::joinKey(base, "attributes.json"), j);
        }
    }


    template<class GROUP>
    inline void writeMetadata(const z5::handle::Group<GROUP> & handle, const Metadata & metadata) {
        const bool isZarr = metadata.isZarr;
        if(isZarr && metadata.zarrFormat == 3) {
            auto client = detail::makeClient(handle);
            meta_detail::writeV3GroupMetadata(client, handle.bucketName(), handle.nameInBucket(), metadata);
            return;
        }
        if(isZarr) {
            auto client = detail::makeClient(handle);
            nlohmann::json j;
            j["zarr_format"] = metadata.zarrFormat;
            attrs_detail::writeJson(client, handle.bucketName(),
                                    detail::joinKey(handle.nameInBucket(), ".zgroup"), j);
        }
        // we don't need to write metadata for n5 groups
    }


    inline void writeMetadata(const handle::Dataset & handle, const DatasetMetadata & metadata) {
        auto client = detail::makeClient(handle);
        const auto & bucket = handle.bucketName();
        const std::string & base = handle.nameInBucket();

        if(metadata.isZarr && metadata.zarrFormat == 3) {
            const std::string key = detail::joinKey(base, "zarr.json");
            // preserve inline user attributes already on store
            nlohmann::json existing;
            const bool hasExisting = attrs_detail::readJson(client, bucket, key, existing);
            nlohmann::json j;
            metadata.toJson(j);
            if(hasExisting && existing.contains("attributes")) {
                j["attributes"] = existing["attributes"];
            }
            attrs_detail::writeJson(client, bucket, key, j);
            return;
        }

        const std::string key = detail::joinKey(base, metadata.isZarr ? ".zarray" : "attributes.json");
        nlohmann::json j;
        metadata.toJson(j);
        attrs_detail::writeJson(client, bucket, key, j);
    }


    inline void readMetadata(const handle::Dataset & handle, DatasetMetadata & metadata) {
        auto client = detail::makeClient(handle);
        const auto & bucket = handle.bucketName();
        const std::string & base = handle.nameInBucket();

        // detect the metadata format by object existence (zarr v3 takes precedence)
        const bool hasV3 = detail::objectExists(client, bucket, detail::joinKey(base, "zarr.json"));
        const bool hasV2 = !hasV3 && detail::objectExists(client, bucket, detail::joinKey(base, ".zarray"));
        const bool isZarr = hasV3 || hasV2;

        std::string key;
        if(hasV3) {
            key = detail::joinKey(base, "zarr.json");
        } else if(hasV2) {
            key = detail::joinKey(base, ".zarray");
        } else {
            key = detail::joinKey(base, "attributes.json");
        }

        nlohmann::json j;
        if(!attrs_detail::readJson(client, bucket, key, j)) {
            throw std::runtime_error("z5::s3::readMetadata: no dataset metadata found at " + base);
        }
        metadata.fromJson(j, isZarr);
    }


}
}
