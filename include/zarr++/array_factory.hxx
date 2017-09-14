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

        // make the ptr to the ZarrArrayTyped of appropriate dtype
        std::unique_ptr<ZarrArray> ptr;
        switch(metadata.dtype) {
            case types::int8:
                std::cout << "opening int8" << std::endl;
                ptr.reset(new ZarrArrayTyped<int8_t>(h)); break;
            case types::int16:
                std::cout << "opening int16" << std::endl;
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
        const bool createAsZarr=true,
        const double fillValue=0,
        const std::string & compressor="blosc",
        const std::string & codec="lz4",
        const int compressorLevel=5,
        const int compressorShuffle=1
    ) {

        // get the internal data type
        types::Datatype internalDtype;
        try {
            internalDtype = types::n5ToDtype.at(dtype);
        } catch(const std::out_of_range & e) {
            throw std::runtime_error("Invalid dtype for zarr array");
        }

        types::Compressor internalCompressor = types::stringToCompressor.at(compressor);
        // make metadata
        ArrayMetadata metadata(
            internalDtype, shape,
            chunkShape, createAsZarr,
            fillValue, internalCompressor,
            codec, compressorLevel, compressorShuffle
        );

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
