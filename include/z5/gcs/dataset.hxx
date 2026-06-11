#pragma once
#include "z5/dataset.hxx"
#include "z5/gcs/handle.hxx"


namespace z5 {
namespace gcs {

    // TODO implement for gcs

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
            detail::notImplemented();
        }

        // read a chunk
        // IMPORTANT we assume that the data pointer is already initialized up to chunkSize_
        inline bool readChunk(const types::ShapeType & chunkIndices, void * dataOut) const {
            detail::notImplemented();
        }

        inline void readRawChunk(const types::ShapeType & chunkIndices,
                                 std::vector<char> & buffer) const {
            detail::notImplemented();
        }

        inline void checkRequestType(const std::type_info & type) const {
            if(type != typeid(T)) {
                throw std::runtime_error(std::string("Request has wrong type: expected ") +
                                         typeid(T).name() + ", got " + type.name());
            }
        }


        inline bool chunkExists(const types::ShapeType & chunkId) const {
            detail::notImplemented();
        }
        inline std::size_t getChunkSize(const types::ShapeType & chunkId) const {
            detail::notImplemented();
        }
        inline void getChunkShape(const types::ShapeType & chunkId,
                                  types::ShapeType & chunkShape,
                                  const bool fromHeader=false) const {
            detail::notImplemented();
        }
        inline std::size_t getChunkShape(const types::ShapeType & chunkId,
                                         const unsigned dim,
                                         const bool fromHeader=false) const {
            detail::notImplemented();
        }


        // compression options
        inline types::Compressor getCompressor() const {return Mixin::compressor_->type();}
        inline void getCompressor(std::string & compressor) const {
            auto compressorType = getCompressor();
            compressor = isZarr_ ? types::Compressors::compressorToZarr()[compressorType] : types::Compressors::compressorToN5()[compressorType];
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
                               void * dataOut,
                               const std::size_t data_size) const {
            util::decompress<T>(buffer, nBytes, dataOut, data_size, Mixin::compressor_);
        }

        inline bool checkVarlenChunk(const types::ShapeType & chunkId, std::size_t & chunkSize) const {
            detail::notImplemented();
        }

        inline const FileMode & mode() const {
            return handle_.mode();
        }
        inline const fs::path & path() const {
            return handle_.path();
        }
        inline void chunkPath(const types::ShapeType & chunkId, fs::path & path) const {
            handle::Chunk chunk(handle_, chunkId, defaultChunkShape(), shape());
            path = chunk.path();
        }
        inline void removeChunk(const types::ShapeType & chunkId) const {
            handle::Chunk chunk(handle_, chunkId, defaultChunkShape(), shape());
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
        handle::Dataset handle_;
    };


}
}
