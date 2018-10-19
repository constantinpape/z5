#pragma once

#include "z5/dataset.hxx"
#include "z5/metadata.hxx"
#include "z5/handle/handle.hxx"

namespace fs = boost::filesystem;
namespace z5 {

    // factory function to open an existing zarr-array
    inline std::unique_ptr<Dataset> openDataset(const std::string & path, const FileMode::modes mode=FileMode::a) {

        // read the data type from the metadata
        handle::Dataset h(path, mode);
        auto dtype = readDatatype(h);

        // make the ptr to the DatasetTyped of appropriate dtype
        std::unique_ptr<Dataset> ptr;
        switch(dtype) {
            case types::int8:
                ptr.reset(new DatasetTyped<int8_t>(h)); break;
            case types::int16:
                ptr.reset(new DatasetTyped<int16_t>(h)); break;
            case types::int32:
                ptr.reset(new DatasetTyped<int32_t>(h)); break;
            case types::int64:
                ptr.reset(new DatasetTyped<int64_t>(h)); break;
            case types::uint8:
                ptr.reset(new DatasetTyped<uint8_t>(h)); break;
            case types::uint16:
                ptr.reset(new DatasetTyped<uint16_t>(h)); break;
            case types::uint32:
                ptr.reset(new DatasetTyped<uint32_t>(h)); break;
            case types::uint64:
                ptr.reset(new DatasetTyped<uint64_t>(h)); break;
            case types::float32:
                ptr.reset(new DatasetTyped<float>(h)); break;
            case types::float64:
                ptr.reset(new DatasetTyped<double>(h)); break;
        }
        return ptr;
    }


    inline std::unique_ptr<Dataset> openDataset(
        const handle::Group & group,
        const std::string & key
    ) {
        auto path = group.path();
        path /= key;
        return openDataset(path.string(), group.mode().mode());
    }


    inline std::unique_ptr<Dataset> createDataset(
        const std::string & path,
        const std::string & dtype,
        const types::ShapeType & shape,
        const types::ShapeType & chunkShape,
        const bool createAsZarr,
        const std::string & compressor="raw",
        const types::CompressionOptions & compressionOptions=types::CompressionOptions(),
        const double fillValue=0,
        const FileMode::modes mode=FileMode::a
    ) {
        // get the internal data type
        types::Datatype internalDtype;
        try {
            internalDtype = types::Datatypes::n5ToDtype().at(dtype);
        } catch(const std::out_of_range & e) {
            throw std::runtime_error("z5py.createDataset: Invalid dtype for dataset");
        }

        // get the compressor
        types::Compressor internalCompressor;
        try {
            internalCompressor = types::Compressors::stringToCompressor().at(compressor);
        } catch(const std::out_of_range & e) {
            throw std::runtime_error("z5py.createDataset: Invalid compressor for dataset");
        }

        // add the default compression options if necessary
        // we need to make a compy of the compression options, because
        // they are const
        auto internalCompressionOptions = compressionOptions;
        types::defaultCompressionOptions(internalCompressor, internalCompressionOptions, createAsZarr);

        // make metadata
        DatasetMetadata metadata(
            internalDtype, shape,
            chunkShape, createAsZarr,
            internalCompressor, internalCompressionOptions,
            fillValue);

        // make array handle
        handle::Dataset h(path, mode);

        // make the ptr to the DatasetTyped of appropriate dtype
        std::unique_ptr<Dataset> ptr;
        switch(internalDtype) {
            case types::int8:
                ptr.reset(new DatasetTyped<int8_t>(h, metadata)); break;
            case types::int16:
                ptr.reset(new DatasetTyped<int16_t>(h, metadata)); break;
            case types::int32:
                ptr.reset(new DatasetTyped<int32_t>(h, metadata)); break;
            case types::int64:
                ptr.reset(new DatasetTyped<int64_t>(h, metadata)); break;
            case types::uint8:
                ptr.reset(new DatasetTyped<uint8_t>(h, metadata)); break;
            case types::uint16:
                ptr.reset(new DatasetTyped<uint16_t>(h, metadata)); break;
            case types::uint32:
                ptr.reset(new DatasetTyped<uint32_t>(h, metadata)); break;
            case types::uint64:
                ptr.reset(new DatasetTyped<uint64_t>(h, metadata)); break;
            case types::float32:
                ptr.reset(new DatasetTyped<float>(h, metadata)); break;
            case types::float64:
                ptr.reset(new DatasetTyped<double>(h, metadata)); break;
        }
        return ptr;
    }


    inline std::unique_ptr<Dataset> createDataset(
        const handle::Group & group,
        const std::string & key,
        const std::string & dtype,
        const types::ShapeType & shape,
        const types::ShapeType & chunkShape,
        const bool createAsZarr,
        const std::string & compressor="raw",
        const types::CompressionOptions & compressionOptions=types::CompressionOptions(),
        const double fillValue=0
    ) {
        auto path = group.path();
        path /= key;
        return createDataset(path.string(),
            dtype, shape, chunkShape,
            createAsZarr, compressor,
            compressionOptions, fillValue, group.mode().mode());
    }

}
