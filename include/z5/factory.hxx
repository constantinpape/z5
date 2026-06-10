#pragma once

#include "z5/filesystem/factory.hxx"

#ifdef WITH_S3
#include "z5/s3/factory.hxx"
#endif

#ifdef WITH_GCS
#include "z5/gcs/factory.hxx"
#endif


namespace z5 {
    namespace factory_detail {
        // read the chunk-key configuration (delimiter, format, encoding) of an
        // existing zarr dataset, from .zarray (v2) or zarr.json (v3)
        inline void getZarrChunkConfig(const fs::path & root, const std::string & key,
                                       std::string & zarrDelimiter, int & zarrFormat,
                                       std::string & chunkKeyEncoding) {
            // zarr v3
            const fs::path v3 = root / key / "zarr.json";
            if(fs::exists(v3)) {
                nlohmann::json j;
                std::ifstream file(v3);
                file >> j;
                file.close();
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
            const fs::path v2 = root / key / ".zarray";
            if(fs::exists(v2)) {
                nlohmann::json j;
                std::ifstream file(v2);
                file >> j;
                file.close();
                zarrFormat = 2;
                const auto it = j.find("dimension_separator");
                if(it != j.end()) {
                    zarrDelimiter = *it;
                }
            }
        }
    }


    template<class GROUP>
    inline std::unique_ptr<Dataset> openDataset(const handle::Group<GROUP> & root,
                                                const std::string & key) {
        std::string zarrDelimiter = ".";
        int zarrFormat = 2;
        std::string chunkKeyEncoding = "default";

        // check if this is a s3 group
        #ifdef WITH_S3
        if(root.isS3()) {
            if(root.isZarr()) {
                s3::getZarrChunkConfig(root, key, zarrDelimiter, zarrFormat, chunkKeyEncoding);
            }
            s3::handle::Dataset ds(root, key, zarrDelimiter, zarrFormat, chunkKeyEncoding);
            return s3::openDataset(ds);
        }
        #endif
        #ifdef WITH_GCS
        if(root.isGcs()) {
            // TODO support zarr dataset with dimension separator by reading this from gcs
            gcs::handle::Dataset ds(root, key);
            return gcs::openDataset(ds);
        }
        #endif

        if(root.isZarr()) {
            factory_detail::getZarrChunkConfig(root.path(), key, zarrDelimiter, zarrFormat, chunkKeyEncoding);
        }
        filesystem::handle::Dataset ds(root, key, zarrDelimiter, zarrFormat, chunkKeyEncoding);
        return filesystem::openDataset(ds);
    }


    // TODO support passing zarr delimiter (need to also adapt this upstream)
    template<class GROUP>
    inline std::unique_ptr<Dataset> createDataset(
        const handle::Group<GROUP> & root,
        const std::string & key,
        const DatasetMetadata & metadata
    ) {
        #ifdef WITH_S3
        if(root.isS3()) {
            s3::handle::Dataset ds(root, key, metadata.zarrDelimiter,
                                   metadata.zarrFormat, metadata.chunkKeyEncoding);
            return s3::createDataset(ds, metadata);
        }
        #endif
        #ifdef WITH_GCS
        if(root.isGcs()) {
            // TODO support zarr dataset with dimension separator in gcs
            gcs::handle::Dataset ds(root, key);
            return gcs::createDataset(ds, metadata);
        }
        #endif

        filesystem::handle::Dataset ds(root, key, metadata.zarrDelimiter,
                                       metadata.zarrFormat, metadata.chunkKeyEncoding);
        return filesystem::createDataset(ds, metadata);
    }


    template<class GROUP>
    inline std::unique_ptr<Dataset> createDataset(
        const handle::Group<GROUP> & root,
        const std::string & key,
        const std::string & dtype,
        const types::ShapeType & shape,
        const types::ShapeType & chunkShape,
        const std::string & compressor="raw",
        const types::CompressionOptions & compressionOptions=types::CompressionOptions(),
        const double fillValue=0,
        const std::string & zarrDelimiter=".",
        const int zarrFormat=2,
        const std::string & chunkKeyEncoding="default",
        const types::ShapeType & shardShape=types::ShapeType()
    ) {
        DatasetMetadata metadata;
        createDatasetMetadata(dtype, shape, chunkShape, root.isZarr(),
                              compressor, compressionOptions, fillValue,
                              zarrDelimiter, metadata,
                              zarrFormat, chunkKeyEncoding, shardShape);

        #ifdef WITH_S3
        if(root.isS3()) {
            s3::handle::Dataset ds(root, key, metadata.zarrDelimiter,
                                   metadata.zarrFormat, metadata.chunkKeyEncoding);
            return s3::createDataset(ds, metadata);
        }
        #endif
        #ifdef WITH_GCS
        if(root.isGcs()) {
            // TODO support zarr dataset with dimension separator in gcs
            gcs::handle::Dataset ds(root, key);
            return gcs::createDataset(ds, metadata);
        }
        #endif

        filesystem::handle::Dataset ds(root, key, zarrDelimiter, zarrFormat, chunkKeyEncoding);
        return filesystem::createDataset(ds, metadata);
    }


    // dataset creation from json, because wrapping the CompressionOptions type
    // to python is very brittle
    template<class GROUP>
    inline std::unique_ptr<Dataset> createDataset(
        const handle::Group<GROUP> & root,
        const std::string & key,
        const std::string & dtype,
        const types::ShapeType & shape,
        const types::ShapeType & chunkShape,
        const std::string & compressor,
        const nlohmann::json & compressionOptions,
        const double fillValue=0,
        const std::string & zarrDelimiter=".",
        const int zarrFormat=2,
        const std::string & chunkKeyEncoding="default",
        const types::ShapeType & shardShape=types::ShapeType()
    ) {
        types::Compressor internalCompressor;
        try {
            internalCompressor = types::Compressors::stringToCompressor().at(compressor);
        } catch(const std::out_of_range & e) {
            throw std::runtime_error("z5::createDataset: Invalid compressor for dataset");
        }

        types::CompressionOptions cOpts;
        types::jsonToCompressionType(compressionOptions, cOpts);

        return createDataset(root, key, dtype, shape, chunkShape, compressor, cOpts, fillValue,
                             zarrDelimiter, zarrFormat, chunkKeyEncoding, shardShape);
    }


    template<class GROUP>
    inline void createFile(const handle::File<GROUP> & file, const bool isZarr, const int zarrFormat=2) {
        #ifdef WITH_S3
        if(file.isS3()) {
            s3::createFile(file, isZarr, zarrFormat);
            return;
        }
        #endif
        #ifdef WITH_GCS
        if(file.isGcs()) {
            gcs::createFile(file, isZarr);
            return;
        }
        #endif
        filesystem::createFile(file, isZarr, zarrFormat);
    }


    template<class GROUP>
    inline void createGroup(const handle::Group<GROUP> & root, const std::string & key, const int zarrFormat=2) {
        #ifdef WITH_S3
        if(root.isS3()) {
            s3::handle::Group newGroup(root, key);
            s3::createGroup(newGroup, root.isZarr(), zarrFormat);
            return;
        }
        #endif
        #ifdef WITH_GCS
        if(root.isGcs()) {
            gcs::handle::Group newGroup(root, key);
            gcs::createGroup(newGroup, root.isZarr());
            return;
        }
        #endif
        filesystem::handle::Group newGroup(root, key);
        filesystem::createGroup(newGroup, root.isZarr(), zarrFormat);
    }


    template<class GROUP1, class GROUP2>
    inline std::string relativePath(const handle::Group<GROUP1> & g1,
                                    const GROUP2 & g2) {
        #ifdef WITH_S3
        if(g1.isS3()) {
            if(!g2.isS3()) {
                throw std::runtime_error("Can't get relative path of different backends.");
            }
            return s3::relativePath(g1, g2);
        }
        #endif
        #ifdef WITH_GCS
        if(g1.isGcs()) {
            if(!g2.isGcs()) {
                throw std::runtime_error("Can't get relative path of different backends.");
            }
            return gcs::relativePath(g1, g2);
        }
        #endif
        return filesystem::relativePath(g1, g2);
    }

}
