#pragma once

#include "z5/dataset.hxx"
#include "z5/generic/store.hxx"
#include "z5/util/format_data.hxx"


namespace z5 {
namespace generic {

    // Plain (non-sharded) dataset: one stored object per chunk. All storage
    // access goes through the STORE policy; the format logic here is
    // backend-independent. n5 chunk-header reads (varlen size, actual shape)
    // are only available on stores with supportsN5HeaderIO.
    template<typename T, class STORE>
    requires ChunkStorePolicy<STORE>
    class Dataset : public z5::Dataset, private z5::MixinTyped<T> {

    public:

        typedef T value_type;
        typedef types::ShapeType shape_type;
        typedef z5::MixinTyped<T> Mixin;
        typedef z5::Dataset BaseType;
        typedef typename STORE::DatasetHandleType DatasetHandleType;
        typedef typename STORE::ChunkHandleType ChunkHandleType;

        // create a new array with metadata
        Dataset(const DatasetHandleType & handle,
                const DatasetMetadata & metadata) : BaseType(metadata),
                                                    Mixin(metadata),
                                                    handle_(handle){
            // seed the handle's isZarr cache from the metadata, so chunk handles
            // never need to probe the metadata files / objects
            handle_.setIsZarr(metadata.isZarr);
        }

        //
        // Implement Dataset API
        //

        inline void writeChunk(const types::ShapeType & chunkIndices, const void * dataIn,
                               const bool isVarlen=false, const std::size_t varSize=0) const {

            // check if we are allowed to write
            if(!handle_.mode().canWrite()) {
                const std::string err = "Cannot write data in file mode " + handle_.mode().printMode();
                throw std::invalid_argument(err.c_str());
            }

            // create chunk handle and check if this chunk is valid
            ChunkHandleType chunk(handle_, chunkIndices, defaultChunkShape(), shape());
            checkChunk(chunk, isVarlen);

            // create the output buffer and format the data
            std::vector<char> buffer;
            // data_to_buffer will return false if there's nothing to write (all fill value)
            if(!util::data_to_buffer(chunk, dataIn, buffer, Mixin::compressor_,
                                     Mixin::fillValue_, isVarlen, varSize)) {
                // if we have data on store for the chunk, delete it
                STORE::erase(chunk);
                return;
            }

            // write the chunk to the store
            STORE::write(chunk, buffer);
        }


        // read a chunk
        // IMPORTANT we assume that the data pointer is already initialized up to chunkSize_
        inline bool readChunk(const types::ShapeType & chunkIndices, void * dataOut) const {
            // get the chunk handle
            ChunkHandleType chunk(handle_, chunkIndices, defaultChunkShape(), shape());

            // make sure that we have a valid chunk
            checkChunk(chunk);

            // load the data from the store; the read outcome itself tells us
            // whether the chunk exists
            std::vector<char> buffer;
            if(!STORE::read(chunk, buffer)) {
                throw std::runtime_error("Trying to read a chunk that does not exist");
            }

            // format the data
            const bool is_varlen = util::buffer_to_data<T>(chunk, buffer, dataOut, Mixin::compressor_);

            return is_varlen;
        }

        inline void readRawChunk(const types::ShapeType & chunkIndices,
                                 std::vector<char> & buffer) const {
            ChunkHandleType chunk(handle_, chunkIndices, defaultChunkShape(), shape());
            // a missing chunk yields an empty buffer
            if(!STORE::read(chunk, buffer)) {
                buffer.clear();
            }
        }

        inline void checkRequestType(const std::type_info & type) const {
            if(type != typeid(T)) {
                throw std::runtime_error(std::string("Request has wrong type: expected ") +
                                         typeid(T).name() + ", got " + type.name());
            }
        }

        inline bool chunkExists(const types::ShapeType & chunkId) const {
            ChunkHandleType chunk(handle_, chunkId, defaultChunkShape(), shape());
            return chunk.exists();
        }


        inline std::size_t getChunkSize(const types::ShapeType & chunkId) const {
            ChunkHandleType chunk(handle_, chunkId, defaultChunkShape(), shape());
            return chunk.size();
        }


        inline void getChunkShape(const types::ShapeType & chunkId,
                                  types::ShapeType & chunkShape,
                                  const bool fromHeader=false) const {
            ChunkHandleType chunk(handle_, chunkId, defaultChunkShape(), shape());
            if constexpr(STORE::supportsN5HeaderIO) {
                if(!isZarr_ && fromHeader) {
                    STORE::readShapeFromHeader(chunk, chunkShape);
                    return;
                }
            }
            const auto & cshape = chunk.shape();
            chunkShape.resize(cshape.size());
            std::copy(cshape.begin(), cshape.end(), chunkShape.begin());
        }


        inline std::size_t getChunkShape(const types::ShapeType & chunkId,
                                         const unsigned dim,
                                         const bool fromHeader=false) const {
            ChunkHandleType chunk(handle_, chunkId, defaultChunkShape(), shape());
            if constexpr(STORE::supportsN5HeaderIO) {
                if(!isZarr_ && fromHeader) {
                    types::ShapeType chunkShape;
                    STORE::readShapeFromHeader(chunk, chunkShape);
                    return chunkShape[dim];
                }
            }
            return chunk.shape()[dim];
        }


        // compression options
        inline types::Compressor getCompressor() const {return Mixin::compressor_->type();}
        inline void getCompressor(std::string & compressor) const {
            auto compressorType = getCompressor();
            compressor = isZarr_ ? types::Compressors::compressorToZarr()[compressorType] :
                types::Compressors::compressorToN5()[compressorType];
        }
        inline void getCompressionOptions(types::CompressionOptions & opts) const {
            Mixin::compressor_->getOptions(opts);
        }

        inline void decompress(const std::vector<char> & buffer,
                               void * dataOut,
                               const std::size_t data_size) const {
            util::decompress<T>(buffer, dataOut, data_size, Mixin::compressor_);
        }
        inline void decompress(const char * buffer, std::size_t nBytes,
                               void * dataOut,
                               const std::size_t data_size) const {
            util::decompress<T>(buffer, nBytes, dataOut, data_size, Mixin::compressor_);
        }

        inline void getFillValue(void * fillValue) const {
            *((T*) fillValue) = Mixin::fillValue_;
        }


        inline bool checkVarlenChunk(const types::ShapeType & chunkId, std::size_t & chunkSize) const {
            ChunkHandleType chunk(handle_, chunkId, defaultChunkShape(), shape());
            if constexpr(STORE::supportsN5HeaderIO) {
                if(isZarr_ || !chunk.exists()) {
                    chunkSize = chunk.size();
                    return false;
                }
                const bool is_varlen = STORE::readVarlenFromHeader(chunk, chunkSize);
                if(!is_varlen) {
                    chunkSize = chunk.size();
                }
                return is_varlen;
            } else {
                // varlen (n5) chunk headers cannot be read on this store
                chunkSize = chunk.size();
                return false;
            }
        }

        inline const FileMode & mode() const {
            return handle_.mode();
        }
        inline const fs::path & path() const {
            return STORE::path(handle_);
        }
        inline void chunkPath(const types::ShapeType & chunkId, fs::path & path) const {
            ChunkHandleType chunk(handle_, chunkId, defaultChunkShape(), shape());
            STORE::chunkPath(chunk, path);
        }
        inline void removeChunk(const types::ShapeType & chunkId) const {
            ChunkHandleType chunk(handle_, chunkId, defaultChunkShape(), shape());
            chunk.remove();
        }
        inline void remove() const {
            handle_.remove();
        }

        // delete copy constructor and assignment operator
        // because the compressor cannot be copied by default
        // and we don't really need this to be copyable afaik
        // if this changes at some point, we need to provide a proper
        // implementation here
        Dataset(const Dataset & that) = delete;
        Dataset & operator=(const Dataset & that) = delete;

    private:

        // check that the chunk handle is valid
        inline void checkChunk(const ChunkHandleType & chunk,
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

    private:
        DatasetHandleType handle_;
    };


}
}
