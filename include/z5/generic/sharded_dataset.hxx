#pragma once

#include <mutex>

#include "z5/dataset.hxx"
#include "z5/generic/store.hxx"
#include "z5/util/format_data.hxx"
#include "z5/util/sharding.hxx"


namespace z5 {
namespace generic {

    // zarr v3 dataset that uses the ``sharding_indexed`` codec: many inner chunks
    // are packed into a single shard object together with an index (and a crc32c
    // checksum of that index). The dataset's chunk grid addresses INNER chunks
    // (so readSubarray/writeSubarray are oblivious to sharding); the stored object
    // is the shard that contains the inner chunk.
    //
    // An inner chunk's bytes inside the shard are exactly what a non-sharded chunk
    // object would hold (the output of the inner codec pipeline), so the existing
    // compressor produces / consumes them unchanged.
    //
    // All storage access goes through the STORE policy (one read / write / erase
    // of a whole shard object); the format logic here is backend-independent.
    template<typename T, class STORE>
    requires ChunkStorePolicy<STORE>
    class ShardedDataset : public z5::Dataset, private z5::MixinTyped<T> {

    public:
        typedef T value_type;
        typedef types::ShapeType shape_type;
        typedef z5::MixinTyped<T> Mixin;
        typedef z5::Dataset BaseType;
        typedef typename STORE::DatasetHandleType DatasetHandleType;
        typedef typename STORE::ChunkHandleType ChunkHandleType;

        ShardedDataset(const DatasetHandleType & handle,
                       const DatasetMetadata & metadata) : BaseType(metadata),
                                                           Mixin(metadata),
                                                           handle_(handle),
                                                           shardShape_(metadata.shardShape),
                                                           chunksPerShard_(util::chunksPerShard(metadata.shardShape, metadata.chunkShape)),
                                                           nSlots_(util::numShardSlots(chunksPerShard_)) {
            // sharding implies zarr v3; seed the cache so chunk handles never probe
            handle_.setIsZarr(true);
        }

        //
        // chunk IO
        //

        inline void writeChunk(const types::ShapeType & chunkIndices, const void * dataIn,
                               const bool isVarlen=false, const std::size_t varSize=0) const {
            if(!handle_.mode().canWrite()) {
                throw std::invalid_argument("Cannot write data in file mode " + handle_.mode().printMode());
            }
            // the inner chunk handle is only used to validate the chunk coordinate
            ChunkHandleType innerChunk(handle_, chunkIndices, defaultChunkShape(), shape());
            checkChunk(innerChunk, isVarlen);

            // compress the inner chunk; empty (all-fill) chunks become empty slots
            std::vector<char> blob;
            const bool nonEmpty = makeChunkBlob(chunkIndices, dataIn, blob);
            writeInnerBlob(chunkIndices, std::move(blob), nonEmpty);
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
            // element count of the (bounded) chunk, like the plain datasets --
            // NOT the compressed byte count of the shard slot
            ChunkHandleType chunk(handle_, chunkId, defaultChunkShape(), shape());
            return chunk.size();
        }

        inline void getChunkShape(const types::ShapeType & chunkId,
                                  types::ShapeType & chunkShape,
                                  const bool fromHeader=false) const {
            ChunkHandleType chunk(handle_, chunkId, defaultChunkShape(), shape());
            const auto & cshape = chunk.shape();
            chunkShape.resize(cshape.size());
            std::copy(cshape.begin(), cshape.end(), chunkShape.begin());
        }

        inline std::size_t getChunkShape(const types::ShapeType & chunkId,
                                         const unsigned dim,
                                         const bool fromHeader=false) const {
            ChunkHandleType chunk(handle_, chunkId, defaultChunkShape(), shape());
            return chunk.shape()[dim];
        }

        inline bool checkVarlenChunk(const types::ShapeType & chunkId, std::size_t & chunkSize) const {
            ChunkHandleType chunk(handle_, chunkId, defaultChunkShape(), shape());
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

        // read all per-slot inner-chunk blobs of a shard; empty vector for empty / absent
        // slots. 'shardCoord' is the shard's coordinate in the (outer) shard grid.
        inline void readShardBlobs(const types::ShapeType & shardCoord,
                                   std::vector<std::vector<char>> & blobs) const override {
            // clear slot-by-slot (not assign) so a caller-reused `blobs` keeps the slot
            // vectors' capacity across shards
            blobs.resize(nSlots_);
            for(auto & blob : blobs) {
                blob.clear();
            }
            ChunkHandleType shardChunk(handle_, shardCoord, shardShape_, shape());
            std::vector<char> shardBuf;
            if(!STORE::read(shardChunk, shardBuf, "shard")) {
                return;
            }
            std::vector<util::ShardEntry> entries;
            // a present but corrupt shard must fail loudly: this function feeds the
            // write read-modify-write path, and treating corruption as "empty shard"
            // would silently discard all other inner chunks on the next write
            if(!util::parseShardIndex(shardBuf, nSlots_, entries)) {
                throw std::runtime_error(std::string(STORE::shardedName) + ": corrupt shard index");
            }
            util::extractShardBlobs(shardBuf, entries, blobs);
        }

        // read a shard once, reporting per-slot byte offset + length within shardBuf
        // (length 0 for empty/never-written slots). Lets the read path decode each touched
        // inner chunk straight from shardBuf with no per-slot copy. Returns false if absent.
        inline bool readShardRaw(const types::ShapeType & shardCoord,
                                 std::vector<char> & shardBuf,
                                 std::vector<std::size_t> & offsets,
                                 std::vector<std::size_t> & nbytes) const override {
            ChunkHandleType shardChunk(handle_, shardCoord, shardShape_, shape());
            if(!STORE::read(shardChunk, shardBuf, "shard")) {
                return false;
            }
            std::vector<util::ShardEntry> entries;
            if(!util::parseShardIndex(shardBuf, nSlots_, entries)) {
                throw std::runtime_error(std::string(STORE::shardedName) + ": corrupt shard index");
            }
            offsets.resize(nSlots_);
            nbytes.resize(nSlots_);
            for(std::size_t s = 0; s < nSlots_; ++s) {
                offsets[s] = entries[s].empty() ? 0 : entries[s].offset;
                nbytes[s] = entries[s].empty() ? 0 : entries[s].nbytes;
            }
            return true;
        }

        // build & write a shard from its per-slot blobs (remove the object if all empty).
        // NOTE: takes no lock itself; callers guarantee exclusivity for the shard object --
        // the shard-aware writeSubarray path uses one task per shard, and writeInnerBlob
        // (the direct per-chunk path) calls this while holding shardMutex_.
        inline void writeShardBlobs(const types::ShapeType & shardCoord,
                                    const std::vector<std::vector<char>> & blobs) const override {
            // single choke point for all sharded writes (writeChunk, removeChunk and
            // the shard-aware writeSubarray path) -> enforce the file mode here
            if(!handle_.mode().canWrite()) {
                throw std::invalid_argument("Cannot write data in file mode " + handle_.mode().printMode());
            }
            ChunkHandleType shardChunk(handle_, shardCoord, shardShape_, shape());
            if(util::allSlotsEmpty(blobs)) {
                STORE::erase(shardChunk);
                return;
            }
            std::vector<char> out;
            util::buildShard(blobs, out);
            STORE::write(shardChunk, out, "shard");
        }

        // compress one inner chunk to its on-disk blob; false => all-fill (empty slot).
        inline bool makeChunkBlob(const types::ShapeType & chunkId, const void * dataIn,
                                  std::vector<char> & blob) const override {
            ChunkHandleType innerChunk(handle_, chunkId, defaultChunkShape(), shape());
            return util::data_to_buffer(innerChunk, dataIn, blob,
                                        Mixin::compressor_, Mixin::fillValue_);
        }

        //
        // paths and removal
        //

        inline const FileMode & mode() const {return handle_.mode();}
        inline const fs::path & path() const {return STORE::path(handle_);}
        inline void chunkPath(const types::ShapeType & chunkId, fs::path & path) const {
            ChunkHandleType shardChunk(handle_, util::shardId(chunkId, chunksPerShard_), shardShape_, shape());
            STORE::chunkPath(shardChunk, path);
        }
        inline void removeChunk(const types::ShapeType & chunkId) const {
            writeInnerBlob(chunkId, std::vector<char>(), false);
        }
        inline void remove() const {handle_.remove();}

        // delete copy constructor and assignment operator
        // because the compressor cannot be copied by default
        ShardedDataset(const ShardedDataset & that) = delete;
        ShardedDataset & operator=(const ShardedDataset & that) = delete;

    private:

        // read a shard object and parse its index; returns false if the shard does not exist
        inline bool readShard(const types::ShapeType & chunkId,
                              std::vector<char> & shardBuf,
                              std::vector<util::ShardEntry> & entries) const {
            ChunkHandleType shardChunk(handle_, util::shardId(chunkId, chunksPerShard_), shardShape_, shape());
            if(!STORE::read(shardChunk, shardBuf, "shard")) {
                return false;
            }
            if(!util::parseShardIndex(shardBuf, nSlots_, entries)) {
                throw std::runtime_error(std::string(STORE::shardedName) + ": corrupt shard index");
            }
            return true;
        }

        // replace one inner chunk's blob in its shard (read-modify-write). Serialized by
        // shardMutex_ so concurrent direct writeChunk calls to the same shard are safe;
        // the read + rebuild themselves are shared with the batched shard-aware path.
        inline void writeInnerBlob(const types::ShapeType & chunkId,
                                   std::vector<char> && blob,
                                   const bool nonEmpty) const {
            std::lock_guard<std::mutex> lock(shardMutex_);

            const auto shardCoord = util::shardId(chunkId, chunksPerShard_);
            const std::size_t slot = util::shardSlot(chunkId, chunksPerShard_);

            std::vector<std::vector<char>> blobs;
            readShardBlobs(shardCoord, blobs);
            blobs[slot] = nonEmpty ? std::move(blob) : std::vector<char>();
            writeShardBlobs(shardCoord, blobs);
        }

        inline void checkChunk(const ChunkHandleType & chunk, const bool isVarlen=false) const {
            if(!chunking_.checkBlockCoordinate(chunk.chunkIndices())) {
                throw std::runtime_error("Invalid chunk");
            }
            if(isVarlen) {
                throw std::runtime_error("Varlength chunks are not supported in zarr v3");
            }
        }

    private:
        DatasetHandleType handle_;
        types::ShapeType shardShape_;
        types::ShapeType chunksPerShard_;
        std::size_t nSlots_;
        mutable std::mutex shardMutex_;
    };


}
}
