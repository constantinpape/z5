#pragma once

#include "z5/filesystem/metadata.hxx"
#include "z5/filesystem/dataset.hxx"
#include "z5/filesystem/sharded_dataset.hxx"
#include "z5/generic/factory.hxx"


namespace z5 {
namespace filesystem {

namespace factory_detail {

    // instantiate the right concrete dataset for the element type:
    // a sharded dataset if the metadata carries a shard shape, else the plain one
    inline std::unique_ptr<z5::Dataset> makeDataset(const handle::Dataset & dataset,
                                                    const DatasetMetadata & metadata) {
        return generic::dispatchDtype(metadata.dtype,
            [&](auto tag) -> std::unique_ptr<z5::Dataset> {
                using T = decltype(tag);
                if(!metadata.shardShape.empty()) {
                    return std::unique_ptr<z5::Dataset>(new ShardedDataset<T>(dataset, metadata));
                }
                return std::unique_ptr<z5::Dataset>(new Dataset<T>(dataset, metadata));
            },
            [&](const types::Datatype dtype) -> std::unique_ptr<z5::Dataset> {
                // complex datasets are only supported as plain (non-sharded) datasets
                switch(dtype) {
                    case types::complex64:
                        return std::unique_ptr<z5::Dataset>(new Dataset<std::complex<float>>(dataset, metadata));
                    case types::complex128:
                        return std::unique_ptr<z5::Dataset>(new Dataset<std::complex<double>>(dataset, metadata));
                    case types::complex256:
                        return std::unique_ptr<z5::Dataset>(new Dataset<std::complex<long double>>(dataset, metadata));
                    default:
                        return nullptr;
                }
            });
    }

}

    // factory function to open an existing dataset
    inline std::unique_ptr<z5::Dataset> openDataset(const handle::Dataset & dataset) {

        // make sure that the file exists
        if(!dataset.exists()) {
            throw std::runtime_error("Opening dataset failed because it does not exists.");
        }

        DatasetMetadata metadata;
        readMetadata(dataset, metadata);
        return factory_detail::makeDataset(dataset, metadata);
    }


    // factory function to create a dataset
    inline std::unique_ptr<z5::Dataset> createDataset(
        const handle::Dataset & dataset,
        const DatasetMetadata & metadata
    ) {
        dataset.create();
        writeMetadata(dataset, metadata);
        return factory_detail::makeDataset(dataset, metadata);
    }


    template<class GROUP>
    inline void createFile(const z5::handle::File<GROUP> & file, const bool isZarr, const int zarrFormat=2) {
        file.create();
        Metadata fmeta(isZarr, zarrFormat);
        writeMetadata(file, fmeta);
    }


    inline void createGroup(const handle::Group & group, const bool isZarr, const int zarrFormat=2) {
        group.create();
        Metadata fmeta(isZarr, zarrFormat);
        writeMetadata(group, fmeta);
    }


    template<class GROUP1, class GROUP2>
    inline std::string relativePath(const z5::handle::Group<GROUP1> & g1,
                                    const GROUP2 & g2) {
        // generic_string: hierarchy names are always '/'-separated (h5py
        // semantics, and consistent with the s3 backend), even on windows,
        // where fs::path::string() would yield backslashes
        return relativeImpl(g1.path(), g2.path()).generic_string();
    }

}
}
