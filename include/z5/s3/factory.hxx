#pragma once

#include "z5/s3/dataset.hxx"
#include "z5/s3/sharded_dataset.hxx"
#include "z5/s3/metadata.hxx"


namespace z5 {
namespace s3 {

namespace factory_detail {

    // construct the typed (sharded or plain) dataset for a given dtype
    template<typename T>
    inline std::unique_ptr<z5::Dataset> makeDataset(const handle::Dataset & handle,
                                                    const DatasetMetadata & metadata) {
        if(!metadata.shardShape.empty()) {
            return std::unique_ptr<z5::Dataset>(new ShardedDataset<T>(handle, metadata));
        }
        return std::unique_ptr<z5::Dataset>(new Dataset<T>(handle, metadata));
    }

    inline std::unique_ptr<z5::Dataset> makeDatasetTyped(const handle::Dataset & handle,
                                                         const DatasetMetadata & metadata) {
        switch(metadata.dtype) {
            case types::int8: return makeDataset<int8_t>(handle, metadata);
            case types::int16: return makeDataset<int16_t>(handle, metadata);
            case types::int32: return makeDataset<int32_t>(handle, metadata);
            case types::int64: return makeDataset<int64_t>(handle, metadata);
            case types::uint8: return makeDataset<uint8_t>(handle, metadata);
            case types::uint16: return makeDataset<uint16_t>(handle, metadata);
            case types::uint32: return makeDataset<uint32_t>(handle, metadata);
            case types::uint64: return makeDataset<uint64_t>(handle, metadata);
            case types::float32: return makeDataset<float>(handle, metadata);
            case types::float64: return makeDataset<double>(handle, metadata);
            default: throw std::runtime_error("Datatype is not supported by the s3 backend");
        }
    }

}  // namespace factory_detail


    // read the chunk-key configuration (delimiter, format, encoding) of an existing
    // zarr dataset from .zarray (v2) or zarr.json (v3) on the object store.
    template<class GROUP>
    inline void getZarrChunkConfig(const z5::handle::Group<GROUP> & root, const std::string & key,
                                   std::string & zarrDelimiter, int & zarrFormat,
                                   std::string & chunkKeyEncoding) {
        auto client = detail::makeClient(root);
        const auto & bucket = root.bucketName();
        const std::string base = detail::joinKey(root.nameInBucket(), key);

        std::string content;
        // zarr v3
        if(detail::getObjectString(client, bucket, detail::joinKey(base, "zarr.json"), content)) {
            const nlohmann::json j = nlohmann::json::parse(content);
            zarrFormat = 3;
            if(j.contains("chunk_key_encoding")) {
                const auto & cke = j["chunk_key_encoding"];
                chunkKeyEncoding = cke.value("name", std::string("default"));
                if(cke.contains("configuration") && cke["configuration"].contains("separator")) {
                    zarrDelimiter = cke["configuration"]["separator"].get<std::string>();
                } else {
                    zarrDelimiter = (chunkKeyEncoding == "v2") ? "." : "/";
                }
            } else {
                chunkKeyEncoding = "default";
                zarrDelimiter = "/";
            }
            return;
        }
        // zarr v2
        if(detail::getObjectString(client, bucket, detail::joinKey(base, ".zarray"), content)) {
            const nlohmann::json j = nlohmann::json::parse(content);
            zarrFormat = 2;
            const auto it = j.find("dimension_separator");
            if(it != j.end()) {
                zarrDelimiter = *it;
            }
        }
    }


    inline std::unique_ptr<z5::Dataset> openDataset(const handle::Dataset & dataset) {
        // make sure that the dataset exists
        if(!dataset.exists()) {
            throw std::runtime_error("Opening dataset failed because it does not exists.");
        }
        DatasetMetadata metadata;
        readMetadata(dataset, metadata);
        return factory_detail::makeDatasetTyped(dataset, metadata);
    }


    inline std::unique_ptr<z5::Dataset> createDataset(
        const handle::Dataset & dataset,
        const DatasetMetadata & metadata
    ) {
        dataset.create();
        writeMetadata(dataset, metadata);
        return factory_detail::makeDatasetTyped(dataset, metadata);
    }


    template<class GROUP>
    inline void createFile(const z5::handle::File<GROUP> & file, const bool isZarr,
                           const int zarrFormat=2) {
        file.create();
        Metadata fmeta(isZarr, zarrFormat);
        writeMetadata(file, fmeta);
    }


    template<class GROUP>
    inline void createGroup(const z5::handle::Group<GROUP> & group, const bool isZarr,
                            const int zarrFormat=2) {
        group.create();
        Metadata fmeta(isZarr, zarrFormat);
        writeMetadata(group, fmeta);
    }


    template<class GROUP1, class GROUP2>
    inline std::string relativePath(const z5::handle::Group<GROUP1> & g1,
                                    const GROUP2 & g2) {
        // both handles live in the same bucket; return g2's key relative to g1's
        const std::string & a = g1.nameInBucket();
        const std::string & b = g2.nameInBucket();
        if(a.empty()) {
            return b;
        }
        if(b.size() >= a.size() && b.compare(0, a.size(), a) == 0) {
            std::string rel = b.substr(a.size());
            if(!rel.empty() && rel.front() == '/') {
                rel.erase(0, 1);
            }
            return rel;
        }
        return b;
    }

}
}
