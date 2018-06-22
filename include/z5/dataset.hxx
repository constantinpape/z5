#pragma once

#include <memory>

#include "z5/metadata.hxx"
#include "z5/handle/handle.hxx"
#include "z5/types/types.hxx"
#include "z5/util/util.hxx"

// different compression backends
#include "z5/compression/raw_compressor.hxx"
#include "z5/compression/blosc_compressor.hxx"
#include "z5/compression/zlib_compressor.hxx"
#include "z5/compression/bzip2_compressor.hxx"
#include "z5/compression/xz_compressor.hxx"
#include "z5/compression/lz4_compressor.hxx"

// different io backends
#include "z5/io/io_zarr.hxx"
#include "z5/io/io_n5.hxx"

namespace z5 {

    // Abstract basis class for the zarr-arrays
    class Dataset {

    public:

        //
        // API
        //

        // we need to use void pointer here to have a generic API
        // write a chunk
        virtual void writeChunk(const types::ShapeType &, const void *) const = 0;
        // read a chunk
        virtual void readChunk(const types::ShapeType &, void *) const = 0;

        virtual bool chunkExists(const types::ShapeType &) const = 0;

        // convert in-memory data to / from data format
        virtual void dataToFormat(const void *, std::vector<char> &, const types::ShapeType &) const = 0;
        virtual void formatToData(const char *, void *) const = 0;

        // helper functions for multiarray API
        virtual void checkRequestShape(const types::ShapeType &, const types::ShapeType &) const = 0;
        virtual void checkRequestType(const std::type_info &) const = 0;
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

        // find chunk index of coordinate
        virtual std::size_t coordinateToChunkId(const types::ShapeType &, types::ShapeType &) const = 0;
        // get offset of chunk

        // size and shape of an actual chunk
        virtual std::size_t getChunkSize(const types::ShapeType &) const = 0;
        virtual void getChunkShape(const types::ShapeType &, types::ShapeType &) const = 0;
        virtual std::size_t getChunkShape(const types::ShapeType &, const unsigned) const = 0;
        virtual void getChunkOffset(const types::ShapeType &, types::ShapeType &) const = 0;

        // maximal chunk size and shape
        virtual std::size_t maxChunkSize() const = 0;
        virtual const types::ShapeType & maxChunkShape() const = 0;
        virtual std::size_t maxChunkShape(const unsigned) const = 0;

        // shapes and dimension
        virtual unsigned dimension() const = 0;
        virtual const types::ShapeType & shape() const = 0;
        virtual std::size_t shape(const unsigned) const = 0;
        virtual std::size_t size() const = 0;

        virtual std::size_t numberOfChunks() const = 0;
        virtual const types::ShapeType & chunksPerDimension() const = 0;
        virtual std::size_t chunksPerDimension(const unsigned) const = 0;

        // dtype and compression options
        virtual types::Datatype getDtype() const = 0;
        virtual bool isZarr() const = 0;
        virtual types::Compressor getCompressor() const = 0;
        virtual void getCompressor(std::string &) const = 0;
        virtual const handle::Dataset & handle() const = 0;

        // find minimum / maximum existing coordinates
        // corresponding to the coordinates of the min / max chunk that was written
        virtual void findMinimumCoordinates(const unsigned, types::ShapeType &) const = 0;
        virtual void findMaximumCoordinates(const unsigned, types::ShapeType &) const = 0;
    };



    template<typename T>
    class DatasetTyped : public Dataset {

    public:

        typedef T value_type;
        typedef types::ShapeType shape_type;

        // create a new array with metadata
        DatasetTyped(
            const handle::Dataset & handle,
            const DatasetMetadata & metadata) : handle_(handle) {

            // check if we have permissions to create a new dataset
            if(!handle_.mode().canCreate()) {
                const std::string err = "Cannot create new dataset in file mode " + handle_.mode().printMode();
                throw std::invalid_argument(err.c_str());
            }

            // make sure that the file does not exist already
            if(handle.exists()) {
                throw std::runtime_error(
                    "Creating a new Dataset failed because file already exists."
                );
            }

            init(metadata);
            handle.createDir();
            writeMetadata(handle, metadata);
        }


        // open existing array
        DatasetTyped(const handle::Dataset & handle) : handle_(handle) {

            // make sure that the file exists
            if(!handle.exists()) {
                throw std::runtime_error(
                    "Opening an existing Dataset failed because file does not exists."
                );
            }
            DatasetMetadata metadata;
            readMetadata(handle, metadata);
            init(metadata);
        }


        virtual inline void writeChunk(const types::ShapeType & chunkIndices, const void * dataIn) const {
            // check if we are allowed to write
            if(!handle_.mode().canWrite()) {
                const std::string err = "Cannot write data in file mode " + handle_.mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            handle::Chunk chunk(handle_, chunkIndices, isZarr_);
            writeChunk(chunk, dataIn);
        }


        // read a chunk
        // IMPORTANT we assume that the data pointer is already initialized up to chunkSize_
        virtual inline void readChunk(const types::ShapeType & chunkIndices, void * dataOut) const {
            handle::Chunk chunk(handle_, chunkIndices, isZarr_);
            readChunk(chunk, dataOut);
        }

        virtual inline bool chunkExists(const types::ShapeType & chunkIndices) const {
            handle::Chunk chunk(handle_, chunkIndices, isZarr_);
            return chunk.exists();
        }

        // convert in-memory data to / from data format
        virtual void dataToFormat(const void * data, std::vector<char> & format, const types::ShapeType & dataShape) const {

            // data size from the shape
            const std::size_t dataSize = std::accumulate(dataShape.begin(), dataShape.end(), 1, std::multiplies<std::size_t>());

            // write header for N5
            if(!isZarr_) {
                io_->writeHeader(dataShape, format);
            }

            // reverse the endianness if necessary
            if(sizeof(T) > 1 && !isZarr_) {

                // copy the data and reverse endianness
                std::vector<T> dataTmp(static_cast<const T*>(data), static_cast<const T*>(data) + dataSize);
                util::reverseEndiannessInplace<T>(dataTmp.begin(), dataTmp.end());

                // if we have raw compression (== no compression), we bypass the compression step
                // and directly write to data
                // raw compression has enum-id 0
                if(compressor_->type() == 0) {
                    const std::size_t preSize = format.size();
                    format.resize(preSize + dataTmp.size());
                    memcpy(&format[preSize], &dataTmp[0], dataTmp.size());
                }
                // otherwise, we compress the data and then write to file
                else {
                    std::vector<char> compressed;
                    compressor_->compress(&dataTmp[0], compressed, dataSize);
                    format.insert(format.end(), compressed.begin(), compressed.end());
                }

            } else {

                // if we have raw compression (== no compression), we bypass the compression step
                // and directly write to data
                // raw compression has enum-id 0
                // compress the data
                if(compressor_->type() == 0) {
                    const std::size_t preSize = format.size();
                    format.resize(preSize + dataSize * sizeof(T));
                    memcpy(&format[preSize], (T*) data, dataSize * sizeof(T));
                }
                // otherwise, we compress the data and then write to file
                else {
                    std::vector<char> compressed;
                    compressor_->compress(static_cast<const T*>(data), compressed, dataSize);
                    format.insert(format.end(), compressed.begin(), compressed.end());
                }
            }
        }

        // TODO TODO
        virtual void formatToData(const char * format, void * data) const {

        }

        virtual void checkRequestShape(const types::ShapeType & offset, const types::ShapeType & shape) const {
            if(offset.size() != shape_.size() || shape.size() != shape_.size()) {
                throw std::runtime_error("Request has wrong dimension");
            }
            for(int d = 0; d < shape_.size(); ++d) {
                if(offset[d] + shape[d] > shape_[d]) {
                    std::cout << "Out of range: " << offset << " + " << shape << std::endl;
                    std::cout << " = " << offset[d] + shape[d] << " > " << shape_[d] << std::endl;;
                    throw std::runtime_error("Request is out of range");
                }
                if(shape[d] == 0) {
                    throw std::runtime_error("Request shape has a zero entry");
                }
            }
        }


        virtual void checkRequestType(const std::type_info & type) const {
            if(type != typeid(T)) {
                std::cout << "Mytype: " << typeid(T).name() << " your type: " << type.name() << std::endl;
                throw std::runtime_error("Request has wrong type");
            }
        }


        virtual void getChunkRequests(
            const types::ShapeType & offset,
            const types::ShapeType & shape,
            std::vector<types::ShapeType> & chunkRequests) const {

            std::size_t nDim = offset.size();
            // iterate over the dimension and find the min and max chunk ids
            types::ShapeType minChunkIds(nDim);
            types::ShapeType maxChunkIds(nDim);
            std::size_t endCoordinate, endId;
            for(int d = 0; d < nDim; ++d) {
                // integer division is ok for both min and max-id, because
                // the chunk is labeled by it's lowest coordinate
                minChunkIds[d] = offset[d] / chunkShape_[d];
                endCoordinate = (offset[d] + shape[d]);
                endId = endCoordinate / chunkShape_[d];
                maxChunkIds[d] = (endCoordinate % chunkShape_[d] == 0) ? endId - 1 : endId;
            }

            util::makeRegularGrid(minChunkIds, maxChunkIds, chunkRequests);
        }

        virtual bool getCoordinatesInRequest(
            const types::ShapeType & chunkId,
            const types::ShapeType & offset,
            const types::ShapeType & shape,
            types::ShapeType & offsetInRequest,
            types::ShapeType & shapeInRequest,
            types::ShapeType & offsetInChunk) const {

            offsetInRequest.resize(offset.size());
            shapeInRequest.resize(offset.size());
            offsetInChunk.resize(offset.size());

            types::ShapeType chunkShape;
            getChunkShape(chunkId, chunkShape);

            bool completeOvlp = true;
            std::size_t chunkBegin, chunkEnd, requestEnd;
            int offDiff, endDiff;
            for(int d = 0; d < offset.size(); ++d) {

                // first we calculate the chunk begin and end coordinates
                chunkBegin = chunkId[d] * chunkShape_[d];
                chunkEnd = chunkBegin + chunkShape[d];
                // next the request end
                requestEnd = offset[d] + shape[d];
                // then we calculate the difference between the chunk begin / end
                // and the request begin / end
                offDiff = chunkBegin - offset[d];
                endDiff = requestEnd - chunkEnd;

                // if the offset difference is negative, we are at a starting chunk
                // that is not completely overlapping
                // -> set all values accordingly
                if(offDiff < 0) {
                    offsetInRequest[d] = 0; // start chunk -> no local offset
                    offsetInChunk[d] = -offDiff;
                    completeOvlp = false;
                    // if this chunk is the beginning chunk as well as the end chunk,
                    // we need to adjust the local shape accordingly
                    shapeInRequest[d] = (chunkEnd <= requestEnd) ? chunkEnd - offset[d] : requestEnd - offset[d];
                }

                // if the end difference is negative, we are at a last chunk
                // that is not completely overlapping
                // -> set all values accordingly
                else if(endDiff < 0) {
                    offsetInRequest[d] = chunkBegin - offset[d];
                    offsetInChunk[d] = 0;
                    completeOvlp = false;
                    shapeInRequest[d] = requestEnd - chunkBegin;

                }

                // otherwise we are at a completely overlapping chunk
                else {
                    offsetInRequest[d] = chunkBegin - offset[d];
                    offsetInChunk[d] = 0;
                    shapeInRequest[d] = chunkShape[d];
                }

            }
            return completeOvlp;
        }

        // get full chunk shape from indices
        virtual void getChunkShape(const types::ShapeType & chunkId, types::ShapeType & chunkShape) const {
            handle::Chunk chunk(handle_, chunkId, isZarr_);
            getChunkShape(chunk, chunkShape);
        }

        // get individual chunk shape from indices
        virtual std::size_t getChunkShape(const types::ShapeType & chunkId, const unsigned dim) const {
            handle::Chunk chunk(handle_, chunkId, isZarr_);
            return getChunkShape(chunk, dim);
        }

        // get full chunk shape from handle
        inline void getChunkShape(const handle::Chunk & chunk, types::ShapeType & chunkShape) const {
            chunkShape.resize(shape_.size());
            // zarr has a fixed chunkShpae, whereas n5 has variable chunk shape
            if(isZarr_) {
                std::copy(chunkShape_.begin(), chunkShape_.end(), chunkShape.begin());
            } else {
                io_->getChunkShape(chunk, chunkShape);
            }
        }

        // get individual chunk shape from handle
        inline std::size_t getChunkShape(const handle::Chunk & chunk, const unsigned dim) const {
            // zarr has a fixed chunkShpae, whereas n5 has variable chunk shape
            if(isZarr_) {
                return maxChunkShape(dim);
            } else {
                std::vector<std::size_t> tmpShape;
                getChunkShape(chunk, tmpShape);
                return tmpShape[dim];
            }
        }

        inline void getChunkOffset(const types::ShapeType & chunkIds, types::ShapeType & chunkOffset) const {
            chunkOffset.resize(shape_.size());
            for(unsigned dim = 0; dim < shape_.size(); ++dim) {
                chunkOffset[dim] = chunkIds[dim] * chunkShape_[dim];
            }
        }

        inline std::size_t coordinateToChunkId(const types::ShapeType & coordinate, types::ShapeType & chunkId) const {
            chunkId.resize(shape_.size());
            for(unsigned dim = 0; dim < shape_.size(); ++dim) {
               chunkId[dim] = coordinate[dim] / chunkShape_[dim];
            }
        }

        virtual std::size_t getChunkSize(const types::ShapeType & chunkId) const {
            handle::Chunk chunk(handle_, chunkId, isZarr_);
            return getChunkSize(chunk);
        }

        // shapes and dimension
        virtual unsigned dimension() const {return shape_.size();}
        virtual const types::ShapeType & shape() const {return shape_;}
        virtual std::size_t shape(const unsigned d) const {return shape_[d];}
        virtual const types::ShapeType & maxChunkShape() const {return chunkShape_;}
        virtual std::size_t maxChunkShape(const unsigned d) const {return chunkShape_[d];}

        virtual std::size_t numberOfChunks() const {return numberOfChunks_;}
        virtual const types::ShapeType & chunksPerDimension() const {return chunksPerDimension_;}
        virtual std::size_t chunksPerDimension(const unsigned d) const {return chunksPerDimension_[d];}
        virtual std::size_t maxChunkSize() const {return chunkSize_;}
        virtual std::size_t size() const {
            return std::accumulate(shape_.begin(), shape_.end(), 1, std::multiplies<std::size_t>());
        }

        // datatype, format and handle
        virtual types::Datatype getDtype() const {return dtype_;}
        virtual bool isZarr() const {return isZarr_;}
        virtual const handle::Dataset & handle() const {return handle_;}

        // compression options
        virtual types::Compressor getCompressor() const {return compressor_->type();}
        virtual void getCompressor(std::string & compressor) const {
            auto compressorType = getCompressor();
            compressor = isZarr_ ? types::Compressors::compressorToZarr()[compressorType] : types::Compressors::compressorToN5()[compressorType];
        }

        // find minimum / maximum existing coordinates
        // corresponding to the coordinates of the min / max chunk that was written
        virtual inline void findMinimumCoordinates(const unsigned dim, types::ShapeType & minOut) const {
            // TODO check that the array is non-empty and that the dimension is valid
            io_->findMinimumChunk(dim, handle_.path(), numberOfChunks_, minOut);
            // multiply with chunk shape
            for(int d = 0; d < chunkShape_.size(); ++d) {
                minOut[d] *= chunkShape_[d];
            }
        }

        virtual inline void findMaximumCoordinates(const unsigned dim, types::ShapeType & maxOut) const {
            // TODO check that the array is non-empty and that the dimension is valid
            io_->findMaximumChunk(dim, handle_.path(), maxOut);
            // multiply with chunk shape and check if it exceeds the maximum chunk shape
            for(int d = 0; d < chunkShape_.size(); ++d) {
                maxOut[d] *= chunkShape_[d];
                maxOut[d] += chunkShape_[d];
                if(maxOut[d] > shape_[d]) {
                    maxOut[d] = shape_[d];
                }
            }
        }

        // delete copy constructor and assignment operator
        // because the compressor cannot be copied by default
        // and we don't really need this to be copyable afaik
        // if this changes at some point, we need to provide a proper
        // implementation here
        DatasetTyped(const DatasetTyped & that) = delete;
        DatasetTyped & operator=(const DatasetTyped & that) = delete;

    private:

        //
        // member functions
        //
        void init(const DatasetMetadata & metadata) {

            // zarr or n5 array?
            isZarr_ = metadata.isZarr;

            // dtype
            dtype_ = metadata.dtype;
            // get shapes and fillvalue
            shape_ = metadata.shape;
            chunkShape_ = metadata.chunkShape;

            chunkSize_ = std::accumulate(
                chunkShape_.begin(), chunkShape_.end(), 1, std::multiplies<std::size_t>()
            );
            fillValue_ = static_cast<T>(metadata.fillValue);

            // TODO add more compressors
            switch(metadata.compressor) {
                case types::raw:
            	    compressor_.reset(new compression::RawCompressor<T>()); break;
                #ifdef WITH_BLOSC
                case types::blosc:
            	    compressor_.reset(new compression::BloscCompressor<T>(metadata)); break;
                #endif
                #ifdef WITH_ZLIB
                case types::zlib:
                    compressor_.reset(new compression::ZlibCompressor<T>(metadata)); break;
                #endif
                #ifdef WITH_BZIP2
                case types::bzip2:
                    compressor_.reset(new compression::Bzip2Compressor<T>(metadata)); break;
                #endif
                #ifdef WITH_XZ
                case types::xz:
                    compressor_.reset(new compression::XzCompressor<T>(metadata)); break;
                #endif
                #ifdef WITH_LZ4
                case types::lz4:
                    compressor_.reset(new compression::Lz4Compressor<T>(metadata)); break;
                #endif
            }

            // chunk writer
            if(isZarr_) {
                io_.reset(new io::ChunkIoZarr());
            } else {
                io_.reset(new io::ChunkIoN5(shape_, chunkShape_));
            }

            // get chunk specifications
            for(std::size_t d = 0; d < shape_.size(); ++d) {
                chunksPerDimension_.push_back(
                    shape_[d] / chunkShape_[d] + (shape_[d] % chunkShape_[d] == 0 ? 0 : 1)
                );
            }
            numberOfChunks_ = std::accumulate(chunksPerDimension_.begin(),
                                              chunksPerDimension_.end(),
                                              1, std::multiplies<std::size_t>());
        }


        // write a chunk
        inline void writeChunk(const handle::Chunk & chunk, const void * dataIn) const {

            // throw a runtime error if we don't have write permissions

            // make sure that we have a valid chunk
            checkChunk(chunk);

            // get the correct chunk size and declare the out data
            const std::size_t chunkSize = isZarr_ ? chunkSize_ : io_->getChunkSize(chunk);
            std::vector<char> dataOut;

            // check if the chunk is empty (i.e. all fillvalue)
            // we need the remporary reference to capture in the lambda
            const auto & fillValue = fillValue_;
            const bool isEmpty = std::all_of(static_cast<const T*>(dataIn),
                                             static_cast<const T*>(dataIn) + chunkSize,
                                             [fillValue](const T val){return val == fillValue;});
            // if we have data on disc for the chunk, delete it
            if(isEmpty) {
                const auto & path = chunk.path();
                if(fs::exists(path)) {
                    fs::remove(path);
                }
                return;
            }

            // reverse the endianness if necessary
            if(sizeof(T) > 1 && !isZarr_) {

                // copy the data and reverse endianness
                std::vector<T> dataTmp(static_cast<const T*>(dataIn),
                                       static_cast<const T*>(dataIn) + chunkSize);
                util::reverseEndiannessInplace<T>(dataTmp.begin(), dataTmp.end());

                // if we have raw compression (== no compression), we bypass the compression step
                // and directly write to data
                // raw compression has enum-id 0
                if(compressor_->type() == 0) {
                    io_->write(chunk, (const char *) &dataTmp[0], dataTmp.size() * sizeof(T));
                }
                // otherwise, we compress the data and then write to file
                else {
                    compressor_->compress(&dataTmp[0], dataOut, chunkSize);
                    io_->write(chunk, &dataOut[0], dataOut.size());
                }

            } else {

                // if we have raw compression (== no compression), we bypass the compression step
                // and directly write to data
                // raw compression has enum-id 0
                // compress the data
                if(compressor_->type() == 0) {
                    io_->write(chunk, (const char*) dataIn, chunkSize * sizeof(T));
                }
                // otherwise, we compress the data and then write to file
                else {
                    compressor_->compress(static_cast<const T*>(dataIn), dataOut, chunkSize);
                    io_->write(chunk, &dataOut[0], dataOut.size());
                }
            }
        }


        inline void readChunk(const handle::Chunk & chunk, void * dataOut) const {

            // make sure that we have a valid chunk
            checkChunk(chunk);

            // read the data
            std::vector<char> dataTmp;
            const bool chunkExists = io_->read(chunk, dataTmp);

            // if the chunk exists, decompress it
            // otherwise we return the chunk with fill value
            if(chunkExists) {

                std::size_t chunkSize = isZarr_ ? chunkSize_ : io_->getChunkSize(chunk);

                // we don't need to decompress for raw compression
                if(compressor_->type() == 0) {
                    // mem-copy the binary data that was read to typed out data
                    memcpy((T*) dataOut, &dataTmp[0], dataTmp.size());
                } else {
                    compressor_->decompress(dataTmp, static_cast<T*>(dataOut), chunkSize);
                }

                // reverse the endianness for N5 data
                // TODO check that the file endianness is different than the system endianness
                if(sizeof(T) > 1 && !isZarr_) { // we don't need to convert single bit numbers
                    util::reverseEndiannessInplace<T>(static_cast<T*>(dataOut),
                                                      static_cast<T*>(dataOut) + chunkSize);
                }

            }

            else {
                std::size_t chunkSize = isZarr_ ? chunkSize_ : io_->getChunkSize(chunk);
                std::fill(static_cast<T*>(dataOut),
                          static_cast<T*>(dataOut) + chunkSize,
                          fillValue_);
            }
        }


        // check that the chunk handle is valid
        inline void checkChunk(const handle::Chunk & chunk) const {
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


        inline std::size_t getChunkSize(const handle::Chunk & chunk) const {
            // zarr has a fixed chunkShpae, whereas n5 has variable chunk shape
            if(isZarr_) {
                return maxChunkSize();
            } else {
                std::vector<std::size_t> tmpShape;
                getChunkShape(chunk, tmpShape);
                return std::accumulate(tmpShape.begin(), tmpShape.end(), 1, std::multiplies<std::size_t>());
            }
        }


        //
        // member variables
        //

        // our handle
        handle::Dataset handle_;

        // unique ptr to hold child classes of compressor
        std::unique_ptr<compression::CompressorBase<T>> compressor_;

        // unique prtr chunk writer
        std::unique_ptr<io::ChunkIoBase> io_;

        // flag to store whether the chunks are in zarr or n5 encoding
        bool isZarr_;

        // the datatype encoding
        types::Datatype dtype_;
        // the shape of the array
        types::ShapeType shape_;
        // the chunk-shape of the arrays
        types::ShapeType chunkShape_;
        // the chunk size and the chunk size in bytes
        std::size_t chunkSize_;
        // the fill value
        T fillValue_;
        // the number of chunks and chunks per dimension
        std::size_t numberOfChunks_;
        types::ShapeType chunksPerDimension_;
    };

} // namespace::z5
