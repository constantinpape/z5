#pragma once

#include <mutex>

#include "z5/dataset.hxx"
#include "z5/s3/handle.hxx"
#include "z5/util/format_data.hxx"
#include "z5/util/sharding.hxx"


namespace z5 {
namespace s3 {

    // zarr v3 sharded dataset over S3: the on-store object is the shard (an
    // ``sharding_indexed`` blob holding many inner chunks + a crc32c-checked
    // index). The chunk grid addresses INNER chunks, so readSubarray /
    // writeSubarray are oblivious to sharding -- exactly like the filesystem
    // ShardedDataset, but the shard is GET/PUT as a single S3 object.
    template<typename T>
    class ShardedDataset : public z5::Dataset, private z5::MixinTyped<T> {

    public:
        typedef T value_type;
        typedef types::ShapeType shape_type;
        typedef z5::MixinTyped<T> Mixin;
        typedef z5::Dataset BaseType;

        ShardedDataset(const handle::Dataset & handle,
                       const DatasetMetadata & metadata) : BaseType(metadata),
                                                           Mixin(metadata),
                                                           handle_(handle),
                                                           shardShape_(metadata.shardShape),
                                                           chunksPerShard_(util::chunksPerShard(metadata.shardShape, metadata.chunkShape)),
                                                           nSlots_(util::numShardSlots(chunksPerShard_)) {
        }

        //
        // chunk IO
        //

        inline void writeChunk(const types::ShapeType & chunkIndices, const void * dataIn,
                               const bool isVarlen=false, const std::size_t varSize=0) const {
            if(!handle_.mode().canWrite()) {
                throw std::invalid_argument("Cannot write data in file mode " + handle_.mode().printMode());
            }
            handle::Chunk innerChunk(handle_, chunkIndices, defaultChunkShape(), shape());
            checkChunk(innerChunk, isVarlen);

            std::vector<char> blob;
            const bool nonEmpty = makeChunkBlob(chunkIndices, dataIn, blob);
            writeInnerBlob(chunkIndices, blob, nonEmpty);
        }

        inline bool readChunk(const types::ShapeType & chunkIndices, void * dataOut) const {
            std::vector<char> buffer;
            readRawChunk(chunkIndices, buffer);
            util::decompress<T>(buffer, dataOut, defaultChunkSize(), Mixin::compressor_);
            return false;  // no varlen in zarr v3
        }

        inline void readRawChunk(const types::ShapeType & chunkIndices,
                                 std::vector<char> & buffer) const {
            std::vector<util::ShardEntry> entries;
            std::vector<char> shardBuf;
            if(!readShard(chunkIndices, shardBuf, entries)) {
                buffer.clear();
                return;
            }
            const auto & e = entries[util::shardSlot(chunkIndices, chunksPerShard_)];
            if(e.empty()) {
                buffer.clear();
                return;
            }
            buffer.assign(shardBuf.begin() + e.offset, shardBuf.begin() + e.offset + e.nbytes);
        }

        inline void checkRequestType(const std::type_info & type) const {
            if(type != typeid(T)) {
                throw std::runtime_error("Request has wrong type");
            }
        }

        inline bool chunkExists(const types::ShapeType & chunkId) const {
            std::vector<util::ShardEntry> entries;
            std::vector<char> shardBuf;
            if(!readShard(chunkId, shardBuf, entries)) {
                return false;
            }
            return !entries[util::shardSlot(chunkId, chunksPerShard_)].empty();
        }

        inline std::size_t getChunkSize(const types::ShapeType & chunkId) const {
            std::vector<util::ShardEntry> entries;
            std::vector<char> shardBuf;
            if(!readShard(chunkId, shardBuf, entries)) {
                return 0;
            }
            const auto & e = entries[util::shardSlot(chunkId, chunksPerShard_)];
            return e.empty() ? 0 : e.nbytes;
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

        inline bool checkVarlenChunk(const types::ShapeType & chunkId, std::size_t & chunkSize) const {
            handle::Chunk chunk(handle_, chunkId, defaultChunkShape(), shape());
            chunkSize = chunk.size();
            return false;
        }

        //
        // compression / fill value
        //

        inline types::Compressor getCompressor() const {return Mixin::compressor_->type();}
        inline void getCompressor(std::string & compressor) const {
            compressor = types::Compressors::compressorToZarr()[getCompressor()];
        }
        inline void getCompressionOptions(types::CompressionOptions & opts) const {
            Mixin::compressor_->getOptions(opts);
        }
        inline void decompress(const std::vector<char> & buffer,
                               void * dataOut, const std::size_t data_size) const {
            util::decompress<T>(buffer, dataOut, data_size, Mixin::compressor_);
        }
        inline void decompress(const char * buffer, std::size_t nBytes,
                               void * dataOut, const std::size_t data_size) const {
            util::decompress<T>(buffer, nBytes, dataOut, data_size, Mixin::compressor_);
        }
        inline void getFillValue(void * fillValue) const {
            *((T*) fillValue) = Mixin::fillValue_;
        }

        //
        // sharding info
        //

        inline bool isSharded() const {return true;}
        inline types::ShapeType shardShape() const {return shardShape_;}

        //
        // batched shard IO (used by the shard-aware sub-array paths)
        //

        inline void readShardBlobs(const types::ShapeType & shardCoord,
                                   std::vector<std::vector<char>> & blobs) const override {
            blobs.assign(nSlots_, std::vector<char>());
            std::vector<char> shardBuf;
            if(!readShardObject(shardCoord, shardBuf)) {
                return;
            }
            std::vector<util::ShardEntry> entries;
            if(util::parseShardIndex(shardBuf, nSlots_, entries)) {
                util::extractShardBlobs(shardBuf, entries, blobs);
            }
        }

        inline bool readShardRaw(const types::ShapeType & shardCoord,
                                 std::vector<char> & shardBuf,
                                 std::vector<std::size_t> & offsets,
                                 std::vector<std::size_t> & nbytes) const override {
            if(!readShardObject(shardCoord, shardBuf)) {
                return false;
            }
            std::vector<util::ShardEntry> entries;
            if(!util::parseShardIndex(shardBuf, nSlots_, entries)) {
                throw std::runtime_error("z5::s3::ShardedDataset: corrupt shard index");
            }
            offsets.resize(nSlots_);
            nbytes.resize(nSlots_);
            for(std::size_t s = 0; s < nSlots_; ++s) {
                offsets[s] = entries[s].empty() ? 0 : entries[s].offset;
                nbytes[s] = entries[s].empty() ? 0 : entries[s].nbytes;
            }
            return true;
        }

        inline void writeShardBlobs(const types::ShapeType & shardCoord,
                                    const std::vector<std::vector<char>> & blobs) const override {
            handle::Chunk shardChunk(handle_, shardCoord, shardShape_, shape());
            auto client = detail::makeClient(handle_);
            if(util::allSlotsEmpty(blobs)) {
                detail::deleteObject(client, shardChunk.bucketName(), shardChunk.nameInBucket());
                return;
            }
            std::vector<char> out;
            util::buildShard(blobs, out);
            detail::putObject(client, shardChunk.bucketName(), shardChunk.nameInBucket(),
                              out.data(), out.size());
        }

        inline bool makeChunkBlob(const types::ShapeType & chunkId, const void * dataIn,
                                  std::vector<char> & blob) const override {
            handle::Chunk innerChunk(handle_, chunkId, defaultChunkShape(), shape());
            return util::data_to_buffer(innerChunk, dataIn, blob,
                                        Mixin::compressor_, Mixin::fillValue_);
        }

        inline const FileMode & mode() const {return handle_.mode();}
        inline const fs::path & path() const {static const fs::path p; return p;}
        inline void chunkPath(const types::ShapeType & chunkId, fs::path & path) const {}
        inline void removeChunk(const types::ShapeType & chunkId) const {
            writeInnerBlob(chunkId, std::vector<char>(), false);
        }
        inline void remove() const {handle_.remove();}

        ShardedDataset(const ShardedDataset & that) = delete;
        ShardedDataset & operator=(const ShardedDataset & that) = delete;

    private:

        // GET the shard object into shardBuf; false if it does not exist
        inline bool readShardObject(const types::ShapeType & shardCoord,
                                    std::vector<char> & shardBuf) const {
            handle::Chunk shardChunk(handle_, shardCoord, shardShape_, shape());
            auto client = detail::makeClient(handle_);
            return detail::getObject(client, shardChunk.bucketName(),
                                     shardChunk.nameInBucket(), shardBuf);
        }

        inline bool readShard(const types::ShapeType & chunkId,
                              std::vector<char> & shardBuf,
                              std::vector<util::ShardEntry> & entries) const {
            if(!readShardObject(util::shardId(chunkId, chunksPerShard_), shardBuf)) {
                return false;
            }
            if(!util::parseShardIndex(shardBuf, nSlots_, entries)) {
                throw std::runtime_error("z5::s3::ShardedDataset: corrupt shard index");
            }
            return true;
        }

        // replace one inner chunk's blob in its shard (read-modify-write), serialized
        // so concurrent direct writeChunk calls to the same shard are safe.
        inline void writeInnerBlob(const types::ShapeType & chunkId,
                                   const std::vector<char> & blob,
                                   const bool nonEmpty) const {
            std::lock_guard<std::mutex> lock(shardMutex_);
            const auto shardCoord = util::shardId(chunkId, chunksPerShard_);
            const std::size_t slot = util::shardSlot(chunkId, chunksPerShard_);
            std::vector<std::vector<char>> blobs;
            readShardBlobs(shardCoord, blobs);
            blobs[slot] = nonEmpty ? blob : std::vector<char>();
            writeShardBlobs(shardCoord, blobs);
        }

        inline void checkChunk(const handle::Chunk & chunk, const bool isVarlen=false) const {
            if(!chunking_.checkBlockCoordinate(chunk.chunkIndices())) {
                throw std::runtime_error("Invalid chunk");
            }
            if(isVarlen) {
                throw std::runtime_error("Varlength chunks are not supported in zarr v3");
            }
        }

    private:
        handle::Dataset handle_;
        types::ShapeType shardShape_;
        types::ShapeType chunksPerShard_;
        std::size_t nSlots_;
        mutable std::mutex shardMutex_;
    };


}
}
