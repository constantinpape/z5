#pragma once

#include <memory>
#include <optional>
#include <string>
#include <type_traits>

#include "z5/metadata.hxx"
#include "z5/handle.hxx"
#include "z5/types/types.hxx"
#include "z5/util/util.hxx"
#include "z5/util/blocking.hxx"
#include "z5/util/format_data.hxx"

// different compression backends
#include "z5/compression/raw_compressor.hxx"
#include "z5/compression/blosc_compressor.hxx"
#include "z5/compression/zlib_compressor.hxx"
#include "z5/compression/bzip2_compressor.hxx"
#include "z5/compression/xz_compressor.hxx"
#include "z5/compression/lz4_compressor.hxx"
#include "z5/compression/zstd_compressor.hxx"
#include "z5/compression/crc32c_chain.hxx"


namespace z5 {

    // Abstract basis class for the dataset
    class Dataset {

    public:
        //
        // Constructor
        //
        Dataset(const DatasetMetadata & metadata) : isZarr_(metadata.isZarr),
                                                    dtype_(metadata.dtype),
                                                    shape_(metadata.shape),
                                                    chunkShape_(metadata.chunkShape),
                                                    chunkSize_(std::accumulate(chunkShape_.begin(), chunkShape_.end(), std::size_t(1), std::multiplies<std::size_t>())),
                                                    zarrDelimiter_(metadata.zarrDelimiter),
                                                    dimensionNames_(metadata.dimensionNames),
                                                    transposeOrder_(metadata.transposeOrder),
                                                    elementSize_(metadata.elementSize),
                                                    crc32c_(metadata.crc32cBeforeCompressor + metadata.crc32cAfterCompressor > 0),
                                                    chunking_(shape_, chunkShape_)
        {}

        // Destructor
        virtual ~Dataset(){}

        //
        // API - already implemented and SHOULD NOT be overwritten
        //

        inline void checkRequestShape(const types::ShapeType & offset, const types::ShapeType & shape) const {
            if(offset.size() != shape_.size() || shape.size() != shape_.size()) {
                throw std::runtime_error("Request has wrong dimension");
            }
            for(int d = 0; d < shape_.size(); ++d) {
                if(offset[d] + shape[d] > shape_[d]) {
                    throw std::runtime_error(
                        "Request is out of range: " + std::to_string(offset[d]) + " + " +
                        std::to_string(shape[d]) + " > " + std::to_string(shape_[d]) +
                        " (in dimension " + std::to_string(d) + ")");
                }
                if(shape[d] == 0) {
                    throw std::runtime_error("Request shape has a zero entry");
                }
            }
        }

        // maximal chunk size and shape
        inline std::size_t defaultChunkSize() const {return chunkSize_;}
        inline const types::ShapeType & defaultChunkShape() const {return chunkShape_;}
        inline std::size_t defaultChunkShape(const unsigned d) const {return chunkShape_[d];}

        // chunking
        inline const util::Blocking & chunking() const {return chunking_;}

        // shapes and dimension
        inline unsigned dimension() const {return shape_.size();}
        inline const types::ShapeType & shape() const {return shape_;}
        inline std::size_t shape(const unsigned d) const {return shape_[d];}
        inline std::size_t size() const {
            return std::accumulate(shape_.begin(), shape_.end(), std::size_t(1), std::multiplies<std::size_t>());
        }

        inline void getChunkOffset(const types::ShapeType & chunkId, types::ShapeType & chunkOffset) const {
            chunkOffset.resize(shape_.size());
            for(unsigned dim = 0; dim < shape_.size(); ++dim) {
                chunkOffset[dim] = chunkId[dim] * chunkShape_[dim];
            }
        }

        inline std::size_t numberOfChunks() const {return chunking_.numberOfBlocks();}
        inline const types::ShapeType & chunksPerDimension() const {return chunking_.blocksPerDimension();}
        inline std::size_t chunksPerDimension(const unsigned d) const {return chunking_.blocksPerDimension()[d];}

        inline types::Datatype getDtype() const {return dtype_;}
        inline bool isZarr() const {return isZarr_;}

        // names of the dimensions (zarr v3), std::nullopt for an unnamed
        // dimension - empty if the dataset has no dimension names
        inline const std::vector<std::optional<std::string>> & dimensionNames() const {return dimensionNames_;}

        // sharding (zarr v3) - default: not sharded; overridden by ShardedDataset
        virtual bool isSharded() const {return false;}
        virtual types::ShapeType shardShape() const {return types::ShapeType();}

        // zarr v3 transpose codec order (numpy ``transpose`` semantics); empty if none
        inline const std::vector<std::size_t> & transposeOrder() const {return transposeOrder_;}

        // zarr v3: the chunk codecs include at least one crc32c checksum
        inline bool hasCrc32c() const {return crc32c_;}

        // the byte size of a raw element (raw_bits, raw_bytes); 0 otherwise
        inline std::size_t elementSize() const {return elementSize_;}

        // batched shard IO (zarr v3 sharding) - used by the shard-aware sub-array paths to
        // do a single read-modify-write per shard and parallelize across shards. Default
        // bodies are no-ops; only ShardedDataset overrides them (and the sub-array code
        // only calls them when isSharded() is true).
        // read all per-slot inner-chunk blobs of a shard (empty vector for empty slots)
        virtual void readShardBlobs(const types::ShapeType &,
                                    std::vector<std::vector<char>> &) const {}
        // read a shard once into a raw buffer, reporting per-slot byte offset + length
        // (length 0 for empty slots) so the read path can decode in-place without copying
        // each blob out. Returns false if the shard file is absent.
        virtual bool readShardRaw(const types::ShapeType &, std::vector<char> &,
                                  std::vector<std::size_t> &,
                                  std::vector<std::size_t> &) const {return false;}
        // build and write a shard from its per-slot blobs (removes the file if all empty)
        virtual void writeShardBlobs(const types::ShapeType &,
                                     const std::vector<std::vector<char>> &) const {}
        // compress one inner chunk to its on-disk blob; returns false if it is all-fill
        // (-> empty slot), in which case 'blob' is left empty
        virtual bool makeChunkBlob(const types::ShapeType &, const void *,
                                   std::vector<char> &) const {return false;}

        //
        // API - MUST implement
        //

        // we need to use void pointer here to have a generic API
        // write a chunk
        virtual void writeChunk(const types::ShapeType &, const void *,
                                const bool=false, const std::size_t=0) const = 0;
        // read a chunk - returns True if this is a varlen chunk
        virtual bool readChunk(const types::ShapeType &, void *) const = 0;
        // read a chunk; return the unformatted data
        virtual void readRawChunk(const types::ShapeType &, std::vector<char> &) const = 0;

        // check the request type
        virtual void checkRequestType(const std::type_info &) const = 0;

        // size and shape of an actual chunk
        virtual bool chunkExists(const types::ShapeType &) const = 0;
        virtual std::size_t getChunkSize(const types::ShapeType &) const = 0;
        virtual void getChunkShape(const types::ShapeType &, types::ShapeType &, const bool=false) const = 0;
        virtual std::size_t getChunkShape(const types::ShapeType &, const unsigned, const bool=false) const = 0;
        virtual bool checkVarlenChunk(const types::ShapeType &, std::size_t &) const = 0;

        // compression options and fill value
        virtual types::Compressor getCompressor() const = 0;
        virtual void getCompressor(std::string &) const = 0;
        virtual void getFillValue(void *) const = 0;
        virtual void getCompressionOptions(types::CompressionOptions &) const = 0;
        virtual void decompress(const std::vector<char> &, void *, const std::size_t) const = 0;
        // decompress straight from a raw byte range (used by the sharded read path to decode
        // an inner chunk in-place from the shard buffer, without copying the blob out first)
        virtual void decompress(const char *, std::size_t, void *, const std::size_t) const = 0;

        // file paths, permissions and removal
        virtual const FileMode & mode() const = 0;
        virtual const fs::path & path() const = 0;
        virtual void chunkPath(const types::ShapeType &, fs::path &) const = 0;
        virtual void removeChunk(const types::ShapeType &) const = 0;
        virtual void remove() const = 0;

    protected:
        // private members:
        bool isZarr_;
        types::Datatype dtype_;
        types::ShapeType shape_;
        types::ShapeType chunkShape_;
        std::size_t chunkSize_;
        std::string zarrDelimiter_;
        std::vector<std::optional<std::string>> dimensionNames_;
        std::vector<std::size_t> transposeOrder_;
        std::size_t elementSize_;
        bool crc32c_;

        util::Blocking chunking_;
    };


    // build the compressor named by the metadata, for elements of type U
    template<class U>
    inline std::unique_ptr<compression::CompressorBase<U>> makeCompressor(const DatasetMetadata & metadata) {
        switch(metadata.compressor) {
            #ifdef WITH_BLOSC
            case types::blosc: return std::make_unique<compression::BloscCompressor<U>>(metadata);
            #endif
            #ifdef WITH_ZLIB
            case types::zlib: return std::make_unique<compression::ZlibCompressor<U>>(metadata);
            #endif
            #ifdef WITH_BZIP2
            case types::bzip2: return std::make_unique<compression::Bzip2Compressor<U>>(metadata);
            #endif
            #ifdef WITH_XZ
            case types::xz: return std::make_unique<compression::XzCompressor<U>>(metadata);
            #endif
            #ifdef WITH_LZ4
            case types::lz4: return std::make_unique<compression::Lz4Compressor<U>>(metadata);
            #endif
            #ifdef WITH_ZSTD
            case types::zstd: return std::make_unique<compression::ZstdCompressor<U>>(metadata);
            #endif
            default: return std::make_unique<compression::RawCompressor<U>>();
        }
    }


    // the element type a dataset's compressor works on: variable-length strings
    // (util/vlen.hxx) and raw elements are encoded to bytes before compression
    template<class T>
    using CodecElement = std::conditional_t<std::is_same_v<T, std::string>, char, T>;


    // mixin to provide some typed functionality for typing
    template<class T>
    class MixinTyped {
    public:
        using CodecT = CodecElement<T>;

        MixinTyped(const DatasetMetadata & metadata) : fillValue_(metadata.fillValueAs<T>()) {
            // strings and raw elements have no byte order
            arrayCodecs_.reverseBytes = metadata.bigEndian && sizeof(T) > 1 && !util::isVlenElement<T>;
            arrayCodecs_.order = metadata.transposeOrder;
            arrayCodecs_.fixedItemSize = metadata.elementSize;
            arrayCodecs_.chunkShape = metadata.chunkShape;
            init_compressor(metadata);
        }
        ~MixinTyped(){}

    protected:
        T fillValue_;
        // zarr v3 element codecs (transpose, big-endian ``bytes``) applied around
        // the compressor on encode / decode; identity for zarr v2 and n5
        util::ArrayCodecs arrayCodecs_;
        // unique ptr to hold child classes of compressor; for zarr v3 with crc32c
        // codecs, a Crc32cChain wrapping the compressor
        std::unique_ptr<compression::CompressorBase<CodecT>> compressor_;

    private:
        void init_compressor(const DatasetMetadata & metadata) {
            compressor_ = makeCompressor<CodecT>(metadata);
            const unsigned before = metadata.crc32cBeforeCompressor;
            const unsigned after = metadata.crc32cAfterCompressor;
            if(before == 0 && after == 0) {
                return;
            }
            // crc32c around a raw compressor: the order doesn't matter, all go after
            if(compressor_->isRaw()) {
                compressor_ = std::make_unique<compression::Crc32cChain<CodecT>>(
                    std::move(compressor_), nullptr, 0, before + after);
                return;
            }
            // checksums before the compressor make its input a byte stream that is
            // not a whole number of elements, so it needs a byte-typed compressor
            std::unique_ptr<compression::CompressorBase<char>> byteCompressor;
            if(before > 0) {
                byteCompressor = makeCompressor<char>(metadata);
            }
            compressor_ = std::make_unique<compression::Crc32cChain<CodecT>>(
                std::move(compressor_), std::move(byteCompressor), before, after);
        }
    };


} // namespace::z5
