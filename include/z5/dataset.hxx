#pragma once

#include <memory>

#include "z5/metadata.hxx"
#include "z5/handle/handle.hxx"
#include "z5/types/types.hxx"
#include "z5/util/util.hxx"
#include "z5/util/blocking.hxx"

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
        virtual void writeChunk(const types::ShapeType &, const void *,
                                const bool=false, const std::size_t=0) const = 0;
        // read a chunk
        virtual bool readChunk(const types::ShapeType &, void *) const = 0;
        virtual bool chunkExists(const types::ShapeType &) const = 0;

        // convert in-memory data to / from data format
        virtual void dataToFormat(const void *, std::vector<char> &, const types::ShapeType &) const = 0;
        // virtual void formatToData(const char *, void *) const = 0;

        // helper functions for multiarray API
        virtual void checkRequestShape(const types::ShapeType &, const types::ShapeType &) const = 0;
        virtual void checkRequestType(const std::type_info &) const = 0;

        // size and shape of an actual chunk
        virtual std::size_t getChunkSize(const types::ShapeType &) const = 0;
        virtual void getChunkShape(const types::ShapeType &, types::ShapeType &) const = 0;
        virtual std::size_t getChunkShape(const types::ShapeType &, const unsigned) const = 0;
        virtual void getChunkOffset(const types::ShapeType &, types::ShapeType &) const = 0;
        virtual std::size_t getDiscChunkSize(const types::ShapeType &, bool &) const = 0;

        // get bounded chunk shape
        virtual void getBoundedChunkShape(const types::ShapeType &, types::ShapeType &) const = 0;

        // maximal chunk size and shape
        virtual std::size_t maxChunkSize() const = 0;
        virtual const types::ShapeType & maxChunkShape() const = 0;
        virtual std::size_t maxChunkShape(const unsigned) const = 0;

        // chunking
        virtual const util::Blocking & chunking() const = 0;

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

        // get fill value
        virtual void getFillValue(void *) const = 0;
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


        inline void writeChunk(const types::ShapeType & chunkIndices, const void * dataIn,
                               const bool isVarlen=false, const std::size_t varSize=0) const {
            // check if we are allowed to write
            if(!handle_.mode().canWrite()) {
                const std::string err = "Cannot write data in file mode " + handle_.mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            handle::Chunk chunk(handle_, chunkIndices, isZarr_);
            writeChunk(chunk, dataIn, isVarlen, varSize);
        }


        // read a chunk
        // IMPORTANT we assume that the data pointer is already initialized up to chunkSize_
        inline bool readChunk(const types::ShapeType & chunkIndices, void * dataOut) const {
            handle::Chunk chunk(handle_, chunkIndices, isZarr_);
            return readChunk(chunk, dataOut);
        }


        inline bool chunkExists(const types::ShapeType & chunkIndices) const {
            handle::Chunk chunk(handle_, chunkIndices, isZarr_);
            return chunk.exists();
        }


        // convert in-memory data to / from data format
        inline void dataToFormat(const void * data,
                                 std::vector<char> & format,
                                 const types::ShapeType & dataShape) const {

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

        /*
        inline void formatToData(const char * format, void * data) const {

        }
        */

        inline void checkRequestShape(const types::ShapeType & offset, const types::ShapeType & shape) const {
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


        inline void checkRequestType(const std::type_info & type) const {
            if(type != typeid(T)) {
                std::cout << "Mytype: " << typeid(T).name() << " your type: " << type.name() << std::endl;
                throw std::runtime_error("Request has wrong type");
            }
        }


        // get full chunk shape from indices
        inline void getChunkShape(const types::ShapeType & chunkId, types::ShapeType & chunkShape) const {
            handle::Chunk chunk(handle_, chunkId, isZarr_);
            getChunkShape(chunk, chunkShape);
        }


        // get individual chunk shape from indices
        inline std::size_t getChunkShape(const types::ShapeType & chunkId, const unsigned dim) const {
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


        // get bounded chunk shape
        inline void getBoundedChunkShape(const handle::Chunk & chunk, types::ShapeType & chunkShape) const {
            chunk.boundedChunkShape(shape_, chunkShape_, chunkShape);
        }


        inline void getBoundedChunkShape(const types::ShapeType & chunkId, types::ShapeType & chunkShape) const {
            handle::Chunk chunk(handle_, chunkId, isZarr_);
            getBoundedChunkShape(chunk, chunkShape);
        }


        inline void getChunkOffset(const types::ShapeType & chunkIds, types::ShapeType & chunkOffset) const {
            chunkOffset.resize(shape_.size());
            for(unsigned dim = 0; dim < shape_.size(); ++dim) {
                chunkOffset[dim] = chunkIds[dim] * chunkShape_[dim];
            }
        }


        inline std::size_t getChunkSize(const types::ShapeType & chunkId) const {
            handle::Chunk chunk(handle_, chunkId, isZarr_);
            return getChunkSize(chunk);
        }

        inline const util::Blocking & chunking() const {return chunking_;}

        // shapes and dimension
        inline unsigned dimension() const {return shape_.size();}
        inline const types::ShapeType & shape() const {return shape_;}
        inline std::size_t shape(const unsigned d) const {return shape_[d];}
        inline const types::ShapeType & maxChunkShape() const {return chunkShape_;}
        inline std::size_t maxChunkShape(const unsigned d) const {return chunkShape_[d];}

        inline std::size_t numberOfChunks() const {return chunking_.numberOfBlocks();}
        inline const types::ShapeType & chunksPerDimension() const {return chunking_.blocksPerDimension();}
        inline std::size_t chunksPerDimension(const unsigned d) const {return chunking_.blocksPerDimension()[d];}
        inline std::size_t maxChunkSize() const {return chunkSize_;}
        inline std::size_t size() const {
            return std::accumulate(shape_.begin(), shape_.end(), 1, std::multiplies<std::size_t>());
        }

        // datatype, format and handle
        inline types::Datatype getDtype() const {return dtype_;}
        inline bool isZarr() const {return isZarr_;}
        inline const handle::Dataset & handle() const {return handle_;}

        // compression options
        inline types::Compressor getCompressor() const {return compressor_->type();}
        inline void getCompressor(std::string & compressor) const {
            auto compressorType = getCompressor();
            compressor = isZarr_ ? types::Compressors::compressorToZarr()[compressorType] : types::Compressors::compressorToN5()[compressorType];
        }


        // find minimum / maximum existing coordinates
        // corresponding to the coordinates of the min / max chunk that was written
        inline void findMinimumCoordinates(const unsigned dim, types::ShapeType & minOut) const {
            // TODO check that the array is non-empty and that the dimension is valid
            io_->findMinimumChunk(dim, handle_.path(), numberOfChunks(), minOut);
            // multiply with chunk shape
            for(int d = 0; d < chunkShape_.size(); ++d) {
                minOut[d] *= chunkShape_[d];
            }
        }


        inline void findMaximumCoordinates(const unsigned dim, types::ShapeType & maxOut) const {
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


        inline void getFillValue(void * fillValue) const {
            *((T*) fillValue) = fillValue_;
        }


        inline std::size_t getDiscChunkSize(const types::ShapeType & chunkId,
                                            bool & isVarlen) const {
            isVarlen = false;
            const handle::Chunk chunk(handle_, chunkId, isZarr_);
            return isZarr_ ? chunkSize_ : io_->getDiscChunkSize(chunk, isVarlen);
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

            // initialize the blocking for chunks
            chunking_ = util::Blocking(shape_, chunkShape_);
        }


        // write a chunk
        inline void writeChunk(const handle::Chunk & chunk, const void * dataIn,
                               const bool isVarlen=false, const std::size_t varSize=0) const {

            // TODO throw a runtime error if we don't have write permissions

            // make sure that we have a valid chunk
            checkChunk(chunk, isVarlen);

            // get the correct chunk size and declare the out data
            const std::size_t chunkSize = isZarr_ ? chunkSize_ : io_->getChunkSize(chunk);
            std::vector<char> dataOut;

            // check if the chunk is empty (i.e. all fillvalue) and if so don't write it
            // this does not apply if we have a varlen dataset
            // for which the fillvalue is not defined
            if(!isVarlen) {
                // we need the temporary reference to capture in the lambda
                const auto & fillValue = fillValue_;
                const bool isEmpty = std::all_of(static_cast<const T*>(dataIn),
                                                 static_cast<const T*>(dataIn) + chunkSize,
                                                 [fillValue](const T val){return val == fillValue;});
                // don't write the chunk if it is empty
                if(isEmpty) {
                    // if we have data on disc for the chunk, delete it
                    const auto & path = chunk.path();
                    if(fs::exists(path)) {
                        fs::remove(path);
                    }
                    return;
                }
            }

            // the data size is just the chunk size in normal mode and the
            // varSize in varlength mode
            const std::size_t dataSize = isVarlen ? varSize : chunkSize;

            // reverse the endianness if necessary
            if(sizeof(T) > 1 && !isZarr_) {

                // copy the data and reverse endianness
                std::vector<T> dataTmp(static_cast<const T*>(dataIn),
                                       static_cast<const T*>(dataIn) + dataSize);
                util::reverseEndiannessInplace<T>(dataTmp.begin(), dataTmp.end());

                // if we have raw compression (== no compression), we bypass the compression step
                // and directly write to data
                // raw compression has enum-id 0
                if(compressor_->type() == 0) {
                    io_->write(chunk, (const char *) &dataTmp[0], dataTmp.size() * sizeof(T),
                               isVarlen, varSize);
                }
                // otherwise, we compress the data and then write to file
                else {
                    compressor_->compress(&dataTmp[0], dataOut, dataSize);
                    io_->write(chunk, &dataOut[0], dataOut.size(), isVarlen, varSize);
                }

            } else {

                // if we have raw compression (== no compression), we bypass the compression step
                // and directly write to data
                // raw compression has enum-id 0
                // compress the data
                if(compressor_->type() == 0) {
                    io_->write(chunk, (const char*) dataIn, dataSize * sizeof(T),
                               isVarlen, varSize);
                }
                // otherwise, we compress the data and then write to file
                else {
                    compressor_->compress(static_cast<const T*>(dataIn), dataOut, dataSize);
                    io_->write(chunk, &dataOut[0], dataOut.size(), isVarlen, varSize);
                }
            }
        }


        inline bool readChunk(const handle::Chunk & chunk, void * dataOut) const {

            // make sure that we have a valid chunk
            checkChunk(chunk);

            // read the data
            std::vector<char> dataTmp;
            const bool chunkExists = io_->read(chunk, dataTmp);

            bool isVarlen = false;
            // if the chunk exists, decompress it
            // otherwise raise runtime error
            if(chunkExists) {

                const std::size_t chunkSize = isZarr_ ? chunkSize_ : io_->getDiscChunkSize(chunk, isVarlen);

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
                throw std::runtime_error("Trying to read a chunk that does not exist");
            }

            // return whether this is a varlen chunk
            return isVarlen;
        }


        // check that the chunk handle is valid
        inline void checkChunk(const handle::Chunk & chunk,
                               const bool isVarlen=false) const {
            // check dimension
            const auto & chunkIndices = chunk.chunkIndices();
            if(!chunking_.checkBlockCoordinate(chunkIndices)) {
                throw std::runtime_error("Invalid chunk");
            }
            // varlen chunks are only supported in n5
            if(isVarlen && isZarr_) {
                throw std::runtime_error("Varlength chunks are not supported in zarr");
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

        // unique ptr to chunk writer
        std::unique_ptr<io::ChunkIoBase> io_;

        // blocking for the chunls
        util::Blocking chunking_;

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
    };

} // namespace::z5
