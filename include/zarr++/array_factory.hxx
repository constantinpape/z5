#pragma once

#include "zarr++/array.hxx"
#include "zarr++/metadata.hxx"
#include "zarr++/handle/handle.hxx"

namespace zarr {

    // factory function to open an existing zarr-array
    std::unique_ptr<ZarrArray> openZarrArray(const std::string & path) {

        // read the data type from the metadata
        handle::Array h(path);
        ArrayMetadata metadata;
        readMetadata(h, metadata);
        types::Datatypes dtype;

        try {
            dtype = types::stringToDtype.at(metadata.dtype);
        } catch(const std::out_of_range & e) {
            throw std::runtime_error("Invalid dtype for zarr array");
        }

        // make the ptr to the ZarrArrayTyped of appropriate dtype
        std::unique_ptr<ZarrArray> ptr; 
        switch(dtype) {
            case types::int8:
                ptr.reset(new ZarrArrayTyped<int8_t>(h)); break;
            case types::int16:
                ptr.reset(new ZarrArrayTyped<int16_t>(h)); break;
            case types::int32:
                ptr.reset(new ZarrArrayTyped<int32_t>(h)); break;
            case types::int64:
                ptr.reset(new ZarrArrayTyped<int64_t>(h)); break;
            case types::uint8:
                ptr.reset(new ZarrArrayTyped<uint8_t>(h)); break;
            case types::uint16:
                ptr.reset(new ZarrArrayTyped<uint16_t>(h)); break;
            case types::uint32:
                ptr.reset(new ZarrArrayTyped<uint32_t>(h)); break;
            case types::uint64:
                ptr.reset(new ZarrArrayTyped<uint64_t>(h)); break;
            case types::float32:
                ptr.reset(new ZarrArrayTyped<float>(h)); break;
            case types::float64:
                ptr.reset(new ZarrArrayTyped<double>(h)); break;
        }

        return ptr;
    }


    std::unique_ptr<ZarrArray> createZarrArray(
        const std::string & path,
        const std::string & dtype,
        const types::ShapeType & shape,
        const types::ShapeType & chunkShape,
        const double fillValue=0,
        const int compressorLevel=5,
        const std::string & compressorName="lz4",
        const std::string & compressorId="blosc",
        const int compressorShuffle=1
    ) {
        
        // make metadata
        ArrayMetadata metadata(
            dtype, shape, chunkShape, fillValue, compressorLevel, compressorName, compressorId, compressorShuffle
        );
        
        types::Datatypes internalDtype;
        try {
            internalDtype = types::stringToDtype.at(metadata.dtype);
        } catch(const std::out_of_range & e) {
            throw std::runtime_error("Invalid dtype for zarr array");
        }

        // make array handle
        handle::Array h(path);

        // make the ptr to the ZarrArrayTyped of appropriate dtype
        std::unique_ptr<ZarrArray> ptr; 
        switch(internalDtype) {
            case types::int8:
                ptr.reset(new ZarrArrayTyped<int8_t>(h, metadata)); break;
            case types::int16:
                ptr.reset(new ZarrArrayTyped<int16_t>(h, metadata)); break;
            case types::int32:
                ptr.reset(new ZarrArrayTyped<int32_t>(h, metadata)); break;
            case types::int64:
                ptr.reset(new ZarrArrayTyped<int64_t>(h, metadata)); break;
            case types::uint8:
                ptr.reset(new ZarrArrayTyped<uint8_t>(h, metadata)); break;
            case types::uint16:
                ptr.reset(new ZarrArrayTyped<uint16_t>(h, metadata)); break;
            case types::uint32:
                ptr.reset(new ZarrArrayTyped<uint32_t>(h, metadata)); break;
            case types::uint64:
                ptr.reset(new ZarrArrayTyped<uint64_t>(h, metadata)); break;
            case types::float32:
                ptr.reset(new ZarrArrayTyped<float>(h, metadata)); break;
            case types::float64:
                ptr.reset(new ZarrArrayTyped<double>(h, metadata)); break;
        }

        return ptr;
    }

}
