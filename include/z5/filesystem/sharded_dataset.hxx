#pragma once

#include <fstream>
#include <mutex>

#include "z5/dataset.hxx"
#include "z5/filesystem/handle.hxx"
#include "z5/util/format_data.hxx"
#include "z5/util/sharding.hxx"


namespace z5 {
namespace filesystem {

    // zarr v3 dataset that uses the ``sharding_indexed`` codec: many inner chunks
    // are packed into a single shard file together with an index (and a crc32c
    // checksum of that index). The dataset's chunk grid addresses INNER chunks
    // (so readSubarray/writeSubarray are oblivious to sharding); the on-disk file
    // is the shard that contains the inner chunk.
    //
    // An inner chunk's bytes inside the shard are exactly what a non-sharded chunk
    // file would hold (the output of the inner codec pipeline), so the existing
    // compressor produces / consumes them unchanged.
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
            // sharding implies zarr v3; seed the cache so chunk handles never probe
            handle_.setIsZarr(true);
            std::ios_base::sync_with_stdio(false);
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
            handle::Chunk innerChunk(handle_, chunkIndices, defaultChunkShape(), shape());
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
        // paths and removal
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
            blobs.assign(nSlots_, std::vector<char>());
            handle::Chunk shardChunk(handle_, shardCoord, shardShape_, shape());
            if(!fs::exists(shardChunk.path())) {
                return;
            }
            std::vector<char> shardBuf;
            readFile(shardChunk.path(), shardBuf);
            std::vector<util::ShardEntry> entries;
            // a present but corrupt shard must fail loudly: this function feeds the
            // write read-modify-write path, and treating corruption as "empty shard"
            // would silently discard all other inner chunks on the next write
            if(!util::parseShardIndex(shardBuf, nSlots_, entries)) {
                throw std::runtime_error("z5::ShardedDataset: corrupt shard index");
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
            handle::Chunk shardChunk(handle_, shardCoord, shardShape_, shape());
            if(!fs::exists(shardChunk.path())) {
                return false;
            }
            readFile(shardChunk.path(), shardBuf);
            std::vector<util::ShardEntry> entries;
            if(!util::parseShardIndex(shardBuf, nSlots_, entries)) {
                throw std::runtime_error("z5::ShardedDataset: corrupt shard index");
            }
            offsets.resize(nSlots_);
            nbytes.resize(nSlots_);
            for(std::size_t s = 0; s < nSlots_; ++s) {
                offsets[s] = entries[s].empty() ? 0 : entries[s].offset;
                nbytes[s] = entries[s].empty() ? 0 : entries[s].nbytes;
            }
            return true;
        }

        // build & write a shard from its per-slot blobs (remove the file if all empty).
        // NOTE: takes no lock itself; callers guarantee exclusivity for the shard file --
        // the shard-aware writeSubarray path uses one task per shard, and writeInnerBlob
        // (the direct per-chunk path) calls this while holding shardMutex_.
        inline void writeShardBlobs(const types::ShapeType & shardCoord,
                                    const std::vector<std::vector<char>> & blobs) const override {
            // single choke point for all sharded writes (writeChunk, removeChunk and
            // the shard-aware writeSubarray path) -> enforce the file mode here
            if(!handle_.mode().canWrite()) {
                throw std::invalid_argument("Cannot write data in file mode " + handle_.mode().printMode());
            }
            handle::Chunk shardChunk(handle_, shardCoord, shardShape_, shape());
            const auto & shardPath = shardChunk.path();
            if(util::allSlotsEmpty(blobs)) {
                if(fs::exists(shardPath)) {
                    fs::remove(shardPath);
                }
                return;
            }
            std::vector<char> out;
            util::buildShard(blobs, out);
            shardChunk.create();  // create nested 'c/...' directories if needed
            writeFile(shardPath, out);
        }

        // compress one inner chunk to its on-disk blob; false => all-fill (empty slot).
        inline bool makeChunkBlob(const types::ShapeType & chunkId, const void * dataIn,
                                  std::vector<char> & blob) const override {
            handle::Chunk innerChunk(handle_, chunkId, defaultChunkShape(), shape());
            return util::data_to_buffer(innerChunk, dataIn, blob,
                                        Mixin::compressor_, Mixin::fillValue_);
        }

        inline const FileMode & mode() const {return handle_.mode();}
        inline const fs::path & path() const {return handle_.path();}
        inline void chunkPath(const types::ShapeType & chunkId, fs::path & path) const {
            handle::Chunk shardChunk(handle_, util::shardId(chunkId, chunksPerShard_), shardShape_, shape());
            path = shardChunk.path();
        }
        inline void removeChunk(const types::ShapeType & chunkId) const {
            writeInnerBlob(chunkId, std::vector<char>(), false);
        }
        inline void remove() const {handle_.remove();}

        ShardedDataset(const ShardedDataset & that) = delete;
        ShardedDataset & operator=(const ShardedDataset & that) = delete;

    private:

        // read a shard file and parse its index; returns false if the shard does not exist
        inline bool readShard(const types::ShapeType & chunkId,
                              std::vector<char> & shardBuf,
                              std::vector<util::ShardEntry> & entries) const {
            handle::Chunk shardChunk(handle_, util::shardId(chunkId, chunksPerShard_), shardShape_, shape());
            if(!fs::exists(shardChunk.path())) {
                return false;
            }
            readFile(shardChunk.path(), shardBuf);
            if(!util::parseShardIndex(shardBuf, nSlots_, entries)) {
                throw std::runtime_error("z5::ShardedDataset: corrupt shard index");
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

        inline void writeFile(const fs::path & path, const std::vector<char> & buffer) const {
            std::ofstream file(path, std::ios::binary);
            if(!file.is_open()) {
                throw std::runtime_error("z5: cannot open shard file for writing: " + path.string());
            }
            file.write(buffer.data(), buffer.size());
            if(!file.good()) {
                throw std::runtime_error("z5: failed to write shard file: " + path.string());
            }
            file.close();
        }

        inline void readFile(const fs::path & path, std::vector<char> & buffer) const {
            std::ifstream file(path, std::ios::binary);
            if(!file.is_open()) {
                throw std::runtime_error("z5: cannot open shard file for reading: " + path.string());
            }
            file.seekg(0, std::ios::end);
            const std::size_t file_size = file.tellg();
            file.seekg(0, std::ios::beg);
            buffer.resize(file_size);
            file.read(buffer.data(), file_size);
            if(file.gcount() != static_cast<std::streamsize>(file_size)) {
                throw std::runtime_error("z5: failed to read shard file: " + path.string());
            }
            file.close();
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
