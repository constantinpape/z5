#pragma once

#include "z5/s3/handle.hxx"


namespace z5 {
namespace s3 {

namespace attrs_detail {

    // read a JSON object stored at 'key'; leaves 'j' untouched if the object is absent
    inline bool readJson(Aws::S3::S3Client & client, const std::string & bucket,
                         const std::string & key, nlohmann::json & j) {
        std::string content;
        if(!detail::getObjectString(client, bucket, key, content)) {
            return false;
        }
        if(content.empty()) {
            j = nlohmann::json::object();
        } else {
            j = nlohmann::json::parse(content);
        }
        return true;
    }

    inline void writeJson(Aws::S3::S3Client & client, const std::string & bucket,
                          const std::string & key, const nlohmann::json & j) {
        detail::putObjectString(client, bucket, key, j.dump(4));
    }

    //
    // separate-file attributes (zarr v2 '.zattrs', n5 'attributes.json')
    //

    inline void readAttributes(Aws::S3::S3Client & client, const std::string & bucket,
                               const std::string & key, nlohmann::json & j) {
        readJson(client, bucket, key, j);
    }

    inline void writeAttributes(Aws::S3::S3Client & client, const std::string & bucket,
                                const std::string & key, const nlohmann::json & j) {
        // read existing attributes (if any), merge in the new ones, write back
        nlohmann::json out = nlohmann::json::object();
        readJson(client, bucket, key, out);
        for(auto it = j.begin(); it != j.end(); ++it) {
            out[it.key()] = it.value();
        }
        writeJson(client, bucket, key, out);
    }

    inline void removeAttribute(Aws::S3::S3Client & client, const std::string & bucket,
                                const std::string & key, const std::string & attr) {
        nlohmann::json out;
        if(!readJson(client, bucket, key, out)) {
            return;
        }
        out.erase(attr);
        writeJson(client, bucket, key, out);
    }

    //
    // inline attributes (zarr v3, under zarr.json["attributes"])
    //

    inline void readV3Attributes(Aws::S3::S3Client & client, const std::string & bucket,
                                 const std::string & zarrJsonKey, nlohmann::json & j) {
        nlohmann::json full;
        if(!readJson(client, bucket, zarrJsonKey, full)) {
            return;
        }
        if(full.contains("attributes")) {
            j = full["attributes"];
        }
    }

    inline void writeV3Attributes(Aws::S3::S3Client & client, const std::string & bucket,
                                  const std::string & zarrJsonKey, const nlohmann::json & j) {
        nlohmann::json full = nlohmann::json::object();
        readJson(client, bucket, zarrJsonKey, full);
        nlohmann::json attrs = full.contains("attributes") ? full["attributes"] : nlohmann::json::object();
        for(auto it = j.begin(); it != j.end(); ++it) {
            attrs[it.key()] = it.value();
        }
        full["attributes"] = attrs;
        writeJson(client, bucket, zarrJsonKey, full);
    }

    inline void removeV3Attribute(Aws::S3::S3Client & client, const std::string & bucket,
                                  const std::string & zarrJsonKey, const std::string & attr) {
        nlohmann::json full;
        if(!readJson(client, bucket, zarrJsonKey, full)) {
            return;
        }
        if(full.contains("attributes")) {
            full["attributes"].erase(attr);
        }
        writeJson(client, bucket, zarrJsonKey, full);
    }

    // does this node use zarr v3 metadata (a 'zarr.json' object at its prefix)?
    inline bool isV3(Aws::S3::S3Client & client, const std::string & bucket,
                     const std::string & base) {
        return detail::objectExists(client, bucket, detail::joinKey(base, "zarr.json"));
    }

}  // namespace attrs_detail


    template<class GROUP>
    inline void readAttributes(const z5::handle::Group<GROUP> & group, nlohmann::json & j) {
        auto client = detail::makeClient(group);
        const auto & bucket = group.bucketName();
        const std::string & base = group.nameInBucket();
        if(group.isZarr()) {
            if(attrs_detail::isV3(*client, bucket, base)) {
                attrs_detail::readV3Attributes(*client, bucket, detail::joinKey(base, "zarr.json"), j);
                return;
            }
            attrs_detail::readAttributes(*client, bucket, detail::joinKey(base, ".zattrs"), j);
        } else {
            attrs_detail::readAttributes(*client, bucket, detail::joinKey(base, "attributes.json"), j);
        }
    }

    template<class GROUP>
    inline void writeAttributes(const z5::handle::Group<GROUP> & group, const nlohmann::json & j) {
        auto client = detail::makeClient(group);
        const auto & bucket = group.bucketName();
        const std::string & base = group.nameInBucket();
        if(group.isZarr()) {
            if(attrs_detail::isV3(*client, bucket, base)) {
                attrs_detail::writeV3Attributes(*client, bucket, detail::joinKey(base, "zarr.json"), j);
                return;
            }
            attrs_detail::writeAttributes(*client, bucket, detail::joinKey(base, ".zattrs"), j);
        } else {
            attrs_detail::writeAttributes(*client, bucket, detail::joinKey(base, "attributes.json"), j);
        }
    }

    template<class GROUP>
    inline void removeAttribute(const z5::handle::Group<GROUP> & group, const std::string & key) {
        auto client = detail::makeClient(group);
        const auto & bucket = group.bucketName();
        const std::string & base = group.nameInBucket();
        if(group.isZarr()) {
            if(attrs_detail::isV3(*client, bucket, base)) {
                attrs_detail::removeV3Attribute(*client, bucket, detail::joinKey(base, "zarr.json"), key);
                return;
            }
            attrs_detail::removeAttribute(*client, bucket, detail::joinKey(base, ".zattrs"), key);
        } else {
            attrs_detail::removeAttribute(*client, bucket, detail::joinKey(base, "attributes.json"), key);
        }
    }


    template<class DATASET>
    inline void readAttributes(const z5::handle::Dataset<DATASET> & ds, nlohmann::json & j) {
        auto client = detail::makeClient(ds);
        const auto & bucket = ds.bucketName();
        const std::string & base = ds.nameInBucket();
        if(ds.isZarr()) {
            if(attrs_detail::isV3(*client, bucket, base)) {
                attrs_detail::readV3Attributes(*client, bucket, detail::joinKey(base, "zarr.json"), j);
                return;
            }
            attrs_detail::readAttributes(*client, bucket, detail::joinKey(base, ".zattrs"), j);
        } else {
            attrs_detail::readAttributes(*client, bucket, detail::joinKey(base, "attributes.json"), j);
        }
    }

    template<class DATASET>
    inline void writeAttributes(const z5::handle::Dataset<DATASET> & ds, const nlohmann::json & j) {
        auto client = detail::makeClient(ds);
        const auto & bucket = ds.bucketName();
        const std::string & base = ds.nameInBucket();
        if(ds.isZarr()) {
            if(attrs_detail::isV3(*client, bucket, base)) {
                attrs_detail::writeV3Attributes(*client, bucket, detail::joinKey(base, "zarr.json"), j);
                return;
            }
            attrs_detail::writeAttributes(*client, bucket, detail::joinKey(base, ".zattrs"), j);
        } else {
            attrs_detail::writeAttributes(*client, bucket, detail::joinKey(base, "attributes.json"), j);
        }
    }

    template<class DATASET>
    inline void removeAttribute(const z5::handle::Dataset<DATASET> & ds, const std::string & key) {
        auto client = detail::makeClient(ds);
        const auto & bucket = ds.bucketName();
        const std::string & base = ds.nameInBucket();
        if(ds.isZarr()) {
            if(attrs_detail::isV3(*client, bucket, base)) {
                attrs_detail::removeV3Attribute(*client, bucket, detail::joinKey(base, "zarr.json"), key);
                return;
            }
            attrs_detail::removeAttribute(*client, bucket, detail::joinKey(base, ".zattrs"), key);
        } else {
            attrs_detail::removeAttribute(*client, bucket, detail::joinKey(base, "attributes.json"), key);
        }
    }


    template<class GROUP>
    inline bool isSubGroup(const z5::handle::Group<GROUP> & group, const std::string & key){
        auto client = detail::makeClient(group);
        const auto & bucket = group.bucketName();
        const std::string childBase = detail::joinKey(group.nameInBucket(), key);
        if(group.isZarr()) {
            // zarr v3: the child is a group iff its zarr.json declares node_type "group"
            const std::string v3Key = detail::joinKey(childBase, "zarr.json");
            if(detail::objectExists(*client, bucket, v3Key)) {
                nlohmann::json j;
                attrs_detail::readJson(*client, bucket, v3Key, j);
                return j.value("node_type", std::string("group")) == "group";
            }
            // zarr v2: a group has a .zgroup object
            return detail::objectExists(*client, bucket, detail::joinKey(childBase, ".zgroup"));
        } else {
            nlohmann::json j;
            attrs_detail::readAttributes(*client, bucket,
                                         detail::joinKey(childBase, "attributes.json"), j);
            return !z5::handle::hasAllN5DatasetAttributes(j);
        }
    }

}
}
