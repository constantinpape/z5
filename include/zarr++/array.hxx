#pragma once

#include <memory>

#include "zarr++/metadata.hxx"
#include "zarr++/handle/handle.hxx"
#include "zarr++/types/types.hxx"
#include "zarr++/util/util.hxx"

// different compression backends
#include "zarr++/compression/raw_compressor.hxx"
#include "zarr++/compression/blosc_compressor.hxx"
#include "zarr++/compression/zlib_compressor.hxx"
#include "zarr++/compression/bzip_compressor.hxx"

// different io backends
#include "zarr++/io/io_zarr.hxx"
#include "zarr++/io/io_n5.hxx"

namespace zarr {

    // Abstract basis class for the zarr-arrays
    class ZarrArray {

    public:

        //
        // API
        //

        // we need to use void pointer here to have a generic API
        // write a chunk
        virtual inline void writeChunk(const handle::Chunk &, const void *) = 0;
        virtual void writeChunk(const types::ShapeType &, const void *) = 0;
        // read a chunk
        virtual inline void readChunk(const handle::Chunk &, void *) const = 0;
        virtual void readChunk(const types::ShapeType &, void *) const = 0;

        // helper functions for multiarray API
        virtual void checkRequestShape(const types::ShapeType &, const types::ShapeType &) const = 0;
        // TODO checkRequestType
        virtual void getChunkRequests(
            const types::ShapeType &,
            const types::ShapeType &,
            std::vector<types::ShapeType> &) const = 0;
        virtual bool getCoordinatesInRequest(
            const types::ShapeType &,
            const types::ShapeType &,
            const types::ShapeType &,
            types::ShapeType &,
            types::ShapeType &,
            types::ShapeType &) const = 0;
        virtual void getChunkShape(const handle::Chunk &, types::ShapeType &) const = 0;
        virtual void getChunkShape(const types::ShapeType &, types::ShapeType &) const = 0;

        // shapes and dimension
        virtual unsigned dimension() const = 0;
        virtual const types::ShapeType & shape() const = 0;
        virtual size_t shape(const unsigned) const = 0;
        virtual const types::ShapeType & chunkShape() const = 0;
        virtual size_t maxChunkShape(const unsigned) const = 0;

        virtual size_t numberOfChunks() const = 0;
        virtual const types::ShapeType & chunksPerDimension() const = 0;
        virtual size_t chunksPerDimension(const unsigned) const = 0;
        virtual size_t maxChunkSize() const = 0;
    };



    template<typename T>
    class ZarrArrayTyped : public ZarrArray {

    public:

        // create a new array with metadata
        ZarrArrayTyped(
            const handle::Array & handle,
            const ArrayMetadata & metadata) : handle_(handle) {

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
        ZarrArrayTyped(const handle::Array & handle) : handle_(handle) {

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


        virtual inline void writeChunk(const types::ShapeType & chunkIndices, const void * dataIn) {
            handle::Chunk chunk(handle_, chunkIndices);
            writeChunk(chunk, dataIn);
        }


        // write a chunk
        virtual inline void writeChunk(const handle::Chunk & chunk, const void * dataIn) {

            // make sure that we have a valid chunk
            checkChunk(chunk);

            // compress the data
            std::vector<T> dataOut;
            compressor_->compress(static_cast<const T*>(dataIn), dataOut, chunkSize_);

            // write the data
            io_->write(chunk, dataOut);

        }


        // read a chunk
        virtual inline void readChunk(const types::ShapeType & chunkIndices, void * dataOut) const {
            handle::Chunk chunk(handle_, chunkIndices);
            readChunk(chunk, dataOut);
        }


        // IMPORTANT we assume that the data pointer is already initialized up to chunkSize_
        virtual inline void readChunk(const handle::Chunk & chunk, void * dataOut) const {

            // make sure that we have a valid chunk
            checkChunk(chunk);

            // read the data
            std::vector<T> dataTmp;
            auto chunkExists = io_->read(chunk, dataTmp);

            // if the chunk exists, decompress it
            // otherwise we return the chunk with fill value
            if(chunkExists) {
                compressor_->decompress(dataTmp, static_cast<T*>(dataOut), chunkSize_);
            } else {
                std::fill(static_cast<T*>(dataOut), static_cast<T*>(dataOut) + chunkSize_, fillValue_);
            }
        }

        virtual void checkRequestShape(const types::ShapeType & offset, const types::ShapeType & shape) const {
            if(offset.size() != shape_.size()) {
                throw std::runtime_error("Request has wrong dimension");
            }
            for(int d = 0; d < shape_.size(); ++d) {
                if(offset[d] + shape[d] >= shape_[d]) {
                    throw std::runtime_error("Request is out of range");
                }
            }
        }

        virtual void getChunkRequests(
            const types::ShapeType & offset,
            const types::ShapeType & shape,
            std::vector<types::ShapeType> & chunkRequests) const {

            size_t nDim = offset.size();
            // iterate over the dimension and find the min and max chunk ids
            types::ShapeType minChunkIds(nDim);
            types::ShapeType maxChunkIds(nDim);
            for(int d = 0; d < nDim; ++d) {
                // integer division is ok for both min and max-id, because
                // the chunk is labeled by it's lowest coordinate
                minChunkIds[d] = offset[d] / chunkShape_[d];
                maxChunkIds[d] = (offset[d] + shape[d]) / chunkShape_[d];
            }

            util::makeRegularGrid(minChunkIds, maxChunkIds, chunkRequests);
        }

        virtual bool getCoordinatesInRequest(
            const types::ShapeType & chunkId,
            const types::ShapeType & offset,
            const types::ShapeType & shape,
            types::ShapeType & localOffset,
            types::ShapeType & localShape,
            types::ShapeType & inChunkOffset) const {

            localOffset.resize(offset.size());
            localShape.resize(offset.size());
            inChunkOffset.resize(offset.size());

            types::ShapeType chunkShape;
            getChunkShape(chunkId, chunkShape);

            bool completeOvlp = true;
            size_t chunkBegin, chunkEnd;
            int offDiff, endDiff;
            for(int d = 0; d < offset.size(); ++d) {

                // calculate the offset in the request block
                // first we need the begin coordinate of the chunk
                chunkBegin = chunkId[d] * chunkShape_[d];
                // then we calculate the difference between the chunk begin
                // and the request offset
                offDiff = chunkBegin - offset[d];
                // if the chunk begin is smaller, we are not completely overlapping
                // otherwise we save the offset difference
                localOffset[d] = (offDiff >= 0) ? offDiff : 0;
                if(offDiff < 0) {
                    completeOvlp = false;
                    inChunkOffset[d] = -offDiff;
                }

                // calculate the local shape in the request block
                // first we need the end coordinate off the chunk
                chunkEnd = chunkBegin + chunkShape[d];
                // then we calculate the difference between the request end and
                // the chunk end
                endDiff = offset[d] + shape[d] - chunkEnd;
                // if the chunk end is bigger, we are not completely overlapping and substract the difference
                // from the chunkShape. otherwise we return the chunk shape
                localShape[d] = (endDiff >= 0) ? chunkShape[d] : chunkShape[d] + endDiff;
                if(endDiff < 0) {
                    completeOvlp = false;
                }
                // if the offset difference was smaller than 0, we need to substract it from the shape
                if(offDiff < 0) {
                    localShape[d] += offDiff;
                }
            }
            return completeOvlp;
        }

        virtual void getChunkShape(const types::ShapeType & chunkId, types::ShapeType & chunkShape) const {
            handle::Chunk chunk(handle_, chunkId);
            getChunkShape(chunk, chunkShape);
        }

        virtual void getChunkShape(const handle::Chunk & chunk, types::ShapeType & chunkShape) const {
            chunkShape.resize(shape_.size());
            // zarr has a fixed chunkShpae, whereas n5 has variable chunk shape
            if(isZarr_) {
                std::copy(chunkShape_.begin(), chunkShape_.end(), chunkShape.begin());
            } else {
                io_->getChunkShape(chunk, chunkShape);
            }
        }

        // shapes and dimension
        virtual unsigned dimension() const {return shape_.size();}
        virtual const types::ShapeType & shape() const {return shape_;}
        virtual size_t shape(const unsigned d) const {return shape_[d];}
        virtual const types::ShapeType & chunkShape() const {return chunkShape_;}
        virtual size_t maxChunkShape(const unsigned d) const {return chunkShape_[d];}

        virtual size_t numberOfChunks() const {return numberOfChunks_;}
        virtual const types::ShapeType & chunksPerDimension() const {return chunksPerDimension_;}
        virtual size_t chunksPerDimension(const unsigned d) const {return chunksPerDimension_[d];}
        virtual size_t maxChunkSize() const {return chunkSize_;}

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

            // zarr or n5 array?
            isZarr_ = metadata.isZarr;

            // get shapes and fillvalue
            shape_ = metadata.shape;
            chunkShape_ = metadata.chunkShape;
            chunkSize_ = std::accumulate(
                chunkShape_.begin(), chunkShape_.end(), 1, std::multiplies<size_t>()
            );
            fillValue_ = static_cast<T>(metadata.fillValue);

            types::Compressor compressor;
            try {
                compressor = types::stringToCompressor.at(metadata.compressorId);
            }
            catch(std::out_of_range) {
                throw std::runtime_error("Compressor is not supported / installed");
            }

            // TODO add more compressors
            switch(compressor) {
                case types::raw:
            	    compressor_.reset(new compression::RawCompressor<T>()); break;
                case types::blosc:
            	    compressor_.reset(new compression::BloscCompressor<T>(metadata)); break;
                case types::gzip:
            	    compressor_.reset(new compression::ZlibCompressor<T>(metadata)); break;
                case types::zlib:
            	    compressor_.reset(new compression::ZlibCompressor<T>(metadata)); break;
                case types::bzip:
            	    compressor_.reset(new compression::BzipCompressor<T>(metadata)); break;
            }

            // chunk writer TODO enable N5 writer
            if(isZarr_) {
                io_.reset(new io::ChunkIoZarr<T>());
            } else {
                io_.reset(new io::ChunkIoN5<T>());
            }

            // get chunk specifications
            for(size_t d = 0; d < shape_.size(); ++d) {
                chunksPerDimension_.push_back(
                    shape_[d] / chunkShape_[d] + (shape_[d] % chunkShape_[d] == 0 ? 0 : 1)
                );
            }
            numberOfChunks_ = std::accumulate(
                chunksPerDimension_.begin(), chunksPerDimension_.end(), 1, std::multiplies<size_t>()
            );
        }

        // check that the chunk handle is valid
        void checkChunk(const handle::Chunk & chunk) const {
            // check dimension
            const auto & chunkIndices = chunk.chunkIndices();
            if(chunkIndices.size() != shape_.size()) {
                throw std::runtime_error("Invalid chunk dimension");
            }

            // check chunk dimensions
            for(int d = 0; d < shape_.size(); ++d) {
                if(chunkIndices[d] >= chunksPerDimension_[d]) {
                    throw std::runtime_error("Invalid chunk index");
                }
            }
        }

        //
        // member variables
        //

        // our handle
        handle::Array handle_;

        // unique ptr to hold child classes of compressor
        std::unique_ptr<compression::CompressorBase<T>> compressor_;

        // unique prtr chunk writer
        std::unique_ptr<io::ChunkIoBase<T>> io_;

        // flag to store whether the chunks are in zarr or n5 encoding
        bool isZarr_;

        // the shape of the array
        types::ShapeType shape_;
        // the chunk-shape of the arrays
        types::ShapeType chunkShape_;
        // the chunk size and the chunk size in bytes
        size_t chunkSize_;
        // the fill value
        T fillValue_;
        // the number of chunks and chunks per dimension
        size_t numberOfChunks_;
        types::ShapeType chunksPerDimension_;
    };

} // namespace::zarr
