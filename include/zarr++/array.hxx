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

        // we need to use void pointer here to have a generic API
        // write a chunk
        virtual inline void writeChunk(size_t chunkId, const void * dataIn) = 0;
        // read a chunk
        virtual inline void readChunk(size_t chunkId, void * dataOut) const = 0;

    };


    template<typename T>
    class ZarrArrayTyped : public ZarrArray {

    public:

        // create a new array with metadata
        ZarrArrayTyped(
            const handle::Array & handle,
            const ArrayMetadata & metadata) : io_() {

            // make sure that the file does not exist already
            if(handle.exists()) {
                throw std::runtime_error(
                    "Creating a new ZarrArray failed because file already exists."
                );
            }
            init(metadata);
            handle.createDir();
            writeMetadata(handle, metadata);
        }

        // open existing array
        ZarrArrayTyped(const handle::Array & handle) : io_() {

            // make sure that the file exists
            if(!handle.exists()) {
                throw std::runtime_error(
                    "Opening an existing ZarrArray failed because file does not exists."
                );
            }
            ArrayMetadata metadata;
            readMetadata(handle, metadata);
            init(metadata);
        }

        // write a chunk
        virtual inline void writeChunk(size_t chunkId, const void * dataIn) {

            // compress the data
            std::vector<T> dataOut;
            compressor_->compress(static_cast<const T*>(dataIn), dataOut, chunkSize_);

            // get the chunk handle from ID TODO
            handle::Chunk chunk("");

            // write the data
            io_.write(chunk, dataOut);

        }

        // read a chunk
        // IMPORTANT we assume that the data pointer is already initialized up to chunkSize_
        virtual inline void readChunk(size_t chunkId, void * dataOut) const {

            // get the chunk handle from ID TODO
            handle::Chunk chunk("");

            // read the data
            std::vector<T> dataTmp;
            auto chunkExists = io_.read(chunk, dataTmp);

            // if the chunk exists, decompress it
            // otherwise we return the chunk with fill value
            if(chunkExists) {
               compressor_->decompress(dataTmp, static_cast<T*>(dataOut), chunkSize_);
            } else {
                std::fill(static_cast<T*>(dataOut), static_cast<T*>(dataOut) + chunkSize_, fillValue_);
            }

        }

        // delete copy constructor and assignment operator
        // because the compressor cannot be copied by default
        // and we don't really need this to be copyable afaik
        // if this changes at some point, we need to provide a proper
        // implementation here
        ZarrArrayTyped(const ZarrArrayTyped & that) = delete;
        ZarrArrayTyped & operator=(const ZarrArrayTyped & that) = delete;

    private:
        //
        // member functions
        //
        void init(const ArrayMetadata & metadata) {

            // get shapes and fillvalue
            shape_ = metadata.shape;
            chunkShape_ = metadata.chunkShape;
            chunkSize_ = std::accumulate(
                    chunkShape_.begin(), chunkShape_.end(), 1, std::multiplies<T>()
            );
            fillValue_ = static_cast<T>(metadata.fillValue);

            // get compressor and initialize the compressor pointer
            auto compressorId = metadata.compressorId;
            // TODO we want to support different compression libraries based on the compressor id
            // but for now, we only have blosc
            if(compressorId != "blosc") {
                throw std::runtime_error("Invalid compressor: Zarr++ only supports blosc (for now)");
            }

            // would be nice to have make_unique, but this is C++14
            // TODO write emulator that falls back if compiler only supports 11
            compressor_ = std::unique_ptr<compression::CompressorBase<T>>(
                new compression::BloscCompressor<T>(metadata)
            );

        }

        //
        // member variables
        //

        // unique ptr to hold child classes of compressor
        std::unique_ptr<compression::CompressorBase<T>> compressor_;

        // chunk writer
        io::ChunkIo<T> io_;

        // the shape of the array
        types::ShapeType shape_;
        // the chunk-shape of the arrays
        types::ShapeType chunkShape_;
        // the chunk size and the chunk size in bytes
        size_t chunkSize_;
        // the fill value
        T fillValue_;
    };

} // namespace::zarr
