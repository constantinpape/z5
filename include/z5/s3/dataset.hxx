#pragma once

#include "z5/dataset.hxx"
#include "z5/s3/handle.hxx"
#include "z5/util/format_data.hxx"


namespace z5 {
namespace s3 {

    template<typename T>
    class Dataset : public z5::Dataset, private z5::MixinTyped<T> {

    public:
        typedef T value_type;
        typedef types::ShapeType shape_type;
        typedef z5::MixinTyped<T> Mixin;

        // create a new array with metadata
        Dataset(const handle::Dataset & handle,
                const DatasetMetadata & metadata) : z5::Dataset(metadata),
                                                    Mixin(metadata),
                                                    handle_(handle){
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
            handle::Chunk chunk(handle_, chunkIndices, defaultChunkShape(), shape());
            checkChunk(chunk, isVarlen);

            // create the output buffer and format the data
            std::vector<char> buffer;
            auto client = chunk.makeClient();
            // data_to_buffer returns false if there's nothing to write (all fill value)
            if(!util::data_to_buffer(chunk, dataIn, buffer, Mixin::compressor_,
                                     Mixin::fillValue_, isVarlen, varSize)) {
                // if we have data for the chunk, delete it
                detail::deleteObject(client, chunk.bucketName(), chunk.nameInBucket());
                return;
            }

            // write the chunk to s3
            detail::putObject(client, chunk.bucketName(), chunk.nameInBucket(),
                              buffer.data(), buffer.size());
        }


        // read a chunk
        // IMPORTANT we assume that the data pointer is already initialized up to chunkSize_
        inline bool readChunk(const types::ShapeType & chunkIndices, void * dataOut) const {
            // get the chunk handle
            handle::Chunk chunk(handle_, chunkIndices, defaultChunkShape(), shape());

            // make sure that we have a valid chunk
            checkChunk(chunk);

            // throw runtime error if trying to read non-existing chunk
            if(!chunk.exists()) {
                throw std::runtime_error("Trying to read a chunk that does not exist");
            }

            // load the data from s3
            std::vector<char> buffer;
            read(chunk, buffer);

            // format the data
            const bool is_varlen = util::buffer_to_data<T>(chunk, buffer, dataOut, Mixin::compressor_);

            return is_varlen;
        }


        inline void readRawChunk(const types::ShapeType & chunkIndices,
                                 std::vector<char> & buffer) const {
            handle::Chunk chunk(handle_, chunkIndices, defaultChunkShape(), shape());
            read(chunk, buffer);
        }


        inline void checkRequestType(const std::type_info & type) const {
            if(type != typeid(T)) {
                throw std::runtime_error("Request has wrong type");
            }
        }


        inline bool chunkExists(const types::ShapeType & chunkId) const {
            handle::Chunk chunk(handle_, chunkId, defaultChunkShape(), shape());
            return chunk.exists();
        }

        inline std::size_t getChunkSize(const types::ShapeType & chunkId) const {
            handle::Chunk chunk(handle_, chunkId, defaultChunkShape(), shape());
            return chunk.size();
        }

        inline void getChunkShape(const types::ShapeType & chunkId,
                                  types::ShapeType & chunkShape,
                                  const bool fromHeader=false) const {
            handle::Chunk chunk(handle_, chunkId, defaultChunkShape(), shape());
            const auto & cshape = chunk.shape();
            chunkShape.resize(cshape.size());
            std::copy(cshape.begin(), cshape.end(), chunkShape.begin());
        }

        inline std::size_t getChunkShape(const types::ShapeType & chunkId,
                                         const unsigned dim,
                                         const bool fromHeader=false) const {
            handle::Chunk chunk(handle_, chunkId, defaultChunkShape(), shape());
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

        inline void getFillValue(void * fillValue) const {
            *((T*) fillValue) = Mixin::fillValue_;
        }

        inline void decompress(const std::vector<char> & buffer,
                               void * dataOut,
                               const std::size_t data_size) const {
            util::decompress<T>(buffer, dataOut, data_size, Mixin::compressor_);
        }

        inline void decompress(const char * buffer, std::size_t nBytes,
                               void * dataOut, const std::size_t data_size) const {
            util::decompress<T>(buffer, nBytes, dataOut, data_size, Mixin::compressor_);
        }

        inline bool checkVarlenChunk(const types::ShapeType & chunkId, std::size_t & chunkSize) const {
            handle::Chunk chunk(handle_, chunkId, defaultChunkShape(), shape());
            // varlen (n5) chunks are not supported over s3 yet
            chunkSize = chunk.size();
            return false;
        }

        inline const FileMode & mode() const {
            return handle_.mode();
        }

        inline void removeChunk(const types::ShapeType & chunkId) const {
            handle::Chunk chunk(handle_, chunkId, defaultChunkShape(), shape());
            chunk.remove();
        }

        inline void remove() const {
            handle_.remove();
        }

        // dummy impls
        inline const fs::path & path() const {static const fs::path p; return p;}
        inline void chunkPath(const types::ShapeType & chunkId, fs::path & path) const {}

        // delete copy constructor and assignment operator
        Dataset(const Dataset & that) = delete;
        Dataset & operator=(const Dataset & that) = delete;

    private:

        inline void read(const handle::Chunk & chunk, std::vector<char> & buffer) const {
            auto client = chunk.makeClient();
            // getObject reads the raw bytes binary-safe (sized by Content-Length)
            detail::getObject(client, chunk.bucketName(), chunk.nameInBucket(), buffer);
        }

        inline void checkChunk(const handle::Chunk & chunk, const bool isVarlen=false) const {
            // check dimension
            const auto & chunkIndices = chunk.chunkIndices();
            if(!chunking_.checkBlockCoordinate(chunkIndices)) {
                throw std::runtime_error("Invalid chunk");
            }
            // varlen chunks are only supported in n5 (and not over s3)
            if(isVarlen && isZarr_) {
                throw std::runtime_error("Varlength chunks are not supported in zarr");
            }
        }

        handle::Dataset handle_;
    };


}
}
