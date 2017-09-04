#pragma once

#include <typeinfo>

#include "zarr++/metadata.hxx"
#include "zarr++/handle/handle.hxx"
#include "zarr++/types/types.hxx"

// different compression backends
#include "zarr++/compression/compressor_base.hxx"
#include "zarr++/compression/blosc_compressor.hxx"

namespace zarr {

    class ZarrArray {

    public:

        // create a new array with metadata
        ZarrArray(
            const handle::Array & handle,
            const Metadata & metadata) {

            // make sure that the file does not exist already
            if(handle.exists()) {
                throw std::runtime_error("Creating a new ZarrArray failed because file already exists.");
            }
            init(metadata);
        }

        // open existing array
        ZarrArray(const handle::Array & handle) {

            // make sure that the file exists
            if(!handle.exists()) {
                throw std::runtime_error("Opening an existing ZarrArray failed because file does not exists.");
            }
            Metadata metadata;
            readMetaData(handle, metadata);
            init(metadata);
        }

        // write a chunk
        template<typename T>
        void writeChunk(size_t chunkId, const T * dataIn) {
            // make sure that typeids match
            if(typeid(T) != typeid(typeid_)) {
                throw std::runtime_error("Typeids of ZarrArray and dataIn do not match.");
            }
        }

        // read a chunk
        template<typename T>
        void readChunk(size_t chunkId, T * dataOut) const {
            // make sure that typeids match
            if(typeid(T) != typeid(typeid_)) {
                throw std::runtime_error("Typeids of ZarrArray and dataOut do not match.");
            }

        }

    private:
        //
        // member functions
        //
        void init(const Metadata & metadata) {
            // read the metadata
            shape_ = metadata.shape();
            chunkShape_ = metadata.chunkShape();
        }

        //
        // member variables
        //

        // the shape of the array
        ShapeType shape_;
        // the chunk-shape of the arrays
        ShapeType chunkShape_;

        // TODO wrap generic typeids
        // typeid_;
        // TODO
        // unique ptr to hold child classes of compressor
        // std::unique_ptr<Compressor> compressor_;
    };

} // namespace::zarr
