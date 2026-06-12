#pragma once

#include "z5/s3/handle.hxx"
#include "z5/s3/attributes.hxx"
#include "z5/generic/metadata.hxx"


namespace z5 {
namespace s3 {

    template<class GROUP>
    inline void writeMetadata(const z5::handle::File<GROUP> & handle, const Metadata & metadata) {
        attrs_detail::JsonIO io(handle);
        generic::writeFileMetadata(io, metadata);
    }


    template<class GROUP>
    inline void writeMetadata(const z5::handle::Group<GROUP> & handle, const Metadata & metadata) {
        attrs_detail::JsonIO io(handle);
        generic::writeGroupMetadata(io, metadata);
    }


    inline void writeMetadata(const handle::Dataset & handle, const DatasetMetadata & metadata) {
        attrs_detail::JsonIO io(handle);
        generic::writeDatasetMetadata(io, metadata);
    }


    inline void readMetadata(const handle::Dataset & handle, DatasetMetadata & metadata) {
        auto client = detail::makeClient(handle);
        const auto & bucket = handle.bucketName();
        const std::string & base = handle.nameInBucket();

        // detect the metadata format by object existence (zarr v3 takes precedence)
        const bool hasV3 = detail::objectExists(*client, bucket, detail::joinKey(base, "zarr.json"));
        const bool hasV2 = !hasV3 && detail::objectExists(*client, bucket, detail::joinKey(base, ".zarray"));
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
        if(!attrs_detail::readJson(*client, bucket, key, j)) {
            throw std::runtime_error("z5::s3::readMetadata: no dataset metadata found at " + base);
        }
        metadata.fromJson(j, isZarr);
    }


}
}
