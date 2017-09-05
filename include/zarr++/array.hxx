#pragma once

#include <memory>

#include "zarr++/metadata.hxx"
#include "zarr++/handle/handle.hxx"
#include "zarr++/types/types.hxx"
#include "zarr++/io/io.hxx"

// different compression backends
#include "zarr++/compression/compressor_base.hxx"
#include "zarr++/compression/blosc_compressor.hxx"

namespace zarr {

    // Abstract basis class for the zarr-arrays
    class ZarrArray {

    public:

        //
        // API
        //

        // write a chunk
        virtual inline void writeChunk(size_t chunkId, const T * dataIn) = 0;
        // read a chunk
        virtual inline void readChunk(size_t chunkId, T * dataOut) const = 0;

    };


    template<typename T>
    class ZarrArrayTyped : public ZarrArray {

    public:

        // create a new array with metadata
        ZarrArrayTyped(
            const handle::Array & handle,
            const Metadata & metadata) {

            // make sure that the file does not exist already
            if(handle.exists()) {
                throw std::runtime_error(
                    "Creating a new ZarrArray failed because file already exists."
                );
            }
            init(metadata);
        }

        // open existing array
        ZarrArrayTyped(const handle::Array & handle) {

            // make sure that the file exists
            if(!handle.exists()) {
                throw std::runtime_error(
                    "Opening an existing ZarrArray failed because file does not exists."
                );
            }
            Metadata metadata;
            readMetaData(handle, metadata);
            init(metadata);
        }

        // write a chunk
        inline void writeChunk(size_t chunkId, const T * dataIn) {

            // compress the data
            // TODO allocate dataOut properly
            int sizeCompressed = compressor_->compress(
                dataIn, dataOut, byteSize_, //TODO outSize_
            );

            // get the chunk handle from ID TODO
            handle::Chunk chunk;

            // write the data
            io_->write(chunk, dataOut, sizeCompressed);

        }

        // read a chunk
        inline void readChunk(size_t chunkId, T * dataOut) const {

            // make sure that typeids match
            if(typeid(T) != typeid(typeid_)) {
                throw std::runtime_error("Typeids of ZarrArray and dataOut do not match.");
            }

            // get the chunk handle from ID TODO
            handle::Chunk chunk;

            // read the data
            auto chunkExists = io_->read(chunk, );

            // if the chunk exists, decompress it
            // otherwise we return the chunk with fill value
            if(chunkExists) {
               decompresser_->decompress(); 
            } else {

            }

        }
    
    private:
        //
        // member functions
        //
        void init(const Metadata & metadata) {
            // read the metadata
            // TODO figure out when to read the shapes
            //shape_ = metadata.shape();
            //chunkShape_ = metadata.chunkShape();
            // TODO need a way to encode the dtype properly
            //chunkSize_ = std::accumulate(chunkShape_.begin(), chunkShape_.end(), 1, std::multiplies<>);
            //byteSize_ = sizeof() * chunkSize_;

        }

        //
        // member variables
        //

        // unique ptr to hold child classes of compressor
        std::unique_ptr<Compressor> compressor_;
        // unique ptr to hold the chunk writer
        std::unique_ptr<ChunkIo<T>> io_;

        // the shape of the array
        types::ShapeType shape_;
        // the chunk-shape of the arrays
        types::ShapeType chunkShape_;
        // the chunk size and the chunk size in bytes
        size_t chunkSize_;
        size_t byteSize_;
        // the fill value
        T fillValue_;
    };

} // namespace::zarr
