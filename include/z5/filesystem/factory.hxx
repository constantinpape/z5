#pragma once

#include "z5/filesystem/metadata.hxx"
#include "z5/filesystem/dataset.hxx"
#include "z5/filesystem/sharded_dataset.hxx"


namespace z5 {
namespace filesystem {

namespace factory_detail {

    // instantiate the right concrete dataset for the element type:
    // a sharded dataset if the metadata carries a shard shape, else the plain one
    template<class T>
    inline z5::Dataset * makeTyped(const handle::Dataset & dataset, const DatasetMetadata & metadata) {
        if(!metadata.shardShape.empty()) {
            return new ShardedDataset<T>(dataset, metadata);
        }
        return new Dataset<T>(dataset, metadata);
    }

    inline std::unique_ptr<z5::Dataset> makeDataset(const handle::Dataset & dataset,
                                                    const DatasetMetadata & metadata) {
        std::unique_ptr<z5::Dataset> ptr;
        switch(metadata.dtype) {
            case types::int8:
                ptr.reset(makeTyped<int8_t>(dataset, metadata)); break;
            case types::int16:
                ptr.reset(makeTyped<int16_t>(dataset, metadata)); break;
            case types::int32:
                ptr.reset(makeTyped<int32_t>(dataset, metadata)); break;
            case types::int64:
                ptr.reset(makeTyped<int64_t>(dataset, metadata)); break;
            case types::uint8:
                ptr.reset(makeTyped<uint8_t>(dataset, metadata)); break;
            case types::uint16:
                ptr.reset(makeTyped<uint16_t>(dataset, metadata)); break;
            case types::uint32:
                ptr.reset(makeTyped<uint32_t>(dataset, metadata)); break;
            case types::uint64:
                ptr.reset(makeTyped<uint64_t>(dataset, metadata)); break;
            case types::float32:
                ptr.reset(makeTyped<float>(dataset, metadata)); break;
            case types::float64:
                ptr.reset(makeTyped<double>(dataset, metadata)); break;
            case types::complex64:
                ptr.reset(new Dataset<std::complex<float>>(dataset, metadata)); break;
            case types::complex128:
                ptr.reset(new Dataset<std::complex<double>>(dataset, metadata)); break;
            case types::complex256:
                ptr.reset(new Dataset<std::complex<long double>>(dataset, metadata)); break;
        }
        return ptr;
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
        return relativeImpl(g1.path(), g2.path()).string();
    }

}
}
