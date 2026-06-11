#pragma once

#include <algorithm>
#include <cassert>
#include <map>
#include <numeric>

#include "z5/dataset.hxx"
#include "z5/types/types.hxx"
#include "z5/multiarray/array_view.hxx"
#include "z5/multiarray/array_util.hxx"
#include "z5/util/threadpool.hxx"
#include "z5/util/sharding.hxx"


namespace z5 {
namespace multiarray {


    // Run `body(buffer, unitIndex)` for each unit in [0, nUnits), single- or multi-threaded.
    // Each thread gets its own reusable buffer of `bufSize` elements (initialised to `init`),
    // so per-unit buffer reuse and the single-threaded (no-threadpool) fast path are kept.
    // `body` is a template parameter -> no std::function indirection / zero abstraction cost.
    template<typename T, typename BODY>
    inline void forEachBuffered(const int numberOfThreads, const std::size_t nUnits,
                                const std::size_t bufSize, const T init, BODY && body) {
        if(numberOfThreads == 1) {
            std::vector<T> buffer(bufSize, init);
            for(std::size_t i = 0; i < nUnits; ++i) {
                body(buffer, i);
            }
        } else {
            util::ThreadPool tp(numberOfThreads);
            // a pool created with n_threads <= 0 may have 0 workers; parallel_foreach then
            // runs synchronously and still calls the task with thread id 0, so we need at
            // least one per-thread buffer.
            const int nThreads = std::max(1, static_cast<int>(tp.nThreads()));
            std::vector<std::vector<T>> buffers(nThreads, std::vector<T>(bufSize, init));
            util::parallel_foreach(tp, nUnits, [&](const int tId, const std::size_t i){
                body(buffers[tId], i);
            });
        }
    }


    template<typename T>
    inline void readSubarrayPlain(const Dataset & ds,
                                  const ArrayView<T> & out,
                                  const types::ShapeType & offset,
                                  const types::ShapeType & shape,
                                  const std::vector<types::ShapeType> & chunkRequests,
                                  const int numberOfThreads) {

        const std::size_t maxChunkSize = ds.defaultChunkSize();
        const auto & maxChunkShape = ds.defaultChunkShape();
        const auto & chunking = ds.chunking();
        const bool isZarr = ds.isZarr();

        T fillValue;
        ds.getFillValue(&fillValue);

        // read buffer is always overwritten by decompress (or unused on the fill path), so
        // value-initialising to T() matches the previous default-constructed buffer.
        forEachBuffered<T>(numberOfThreads, chunkRequests.size(), maxChunkSize, T(),
                           [&](std::vector<T> & buffer, const std::size_t chunkIndex){
            const auto & chunkId = chunkRequests[chunkIndex];
            types::ShapeType offsetInRequest, requestShape, chunkShape, offsetInChunk;

            chunking.getCoordinatesInRoi(chunkId, offset, shape,
                                         offsetInRequest, requestShape, offsetInChunk);

            // get the view into our array
            const auto outView = subview(out, offsetInRequest, requestShape);

            // check if this chunk exists, if not fill output with fill value
            if(!ds.chunkExists(chunkId)) {
                fillView(outView, fillValue);
                return;
            }

            // get the shape and size of the chunk (in the actual grid)
            ds.getChunkShape(chunkId, chunkShape);
            std::size_t chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(),
                                                    std::size_t(1), std::multiplies<std::size_t>());

            // read the data from storage
            std::vector<char> dataBuffer;
            ds.readRawChunk(chunkId, dataBuffer);

            // get the shape of the chunk (as it is stored)
            std::size_t chunkStoreSize = maxChunkSize;
            std::size_t headerLength = 0;
            if(!isZarr) {
                if(util::read_n5_header(dataBuffer, chunkStoreSize, headerLength)) {
                    throw std::runtime_error("Can't read from varlen chunks to multiarray");
                }
            }

            // edge chunk stored at full size (zarr) -> use the default chunk shape; the
            // leading block of the buffer holds the valid data.
            if(chunkStoreSize != chunkSize) {
                chunkSize = maxChunkSize;
                chunkShape = maxChunkShape;
            }

            // resize the buffer if necessary
            if(chunkSize != buffer.size()) {
                buffer.resize(chunkSize);
            }

            // decompress the data, decoding straight past the n5 header (no memmove)
            ds.decompress(dataBuffer.data() + headerLength, dataBuffer.size() - headerLength,
                          &buffer[0], chunkSize);

            // reverse the endianness for N5 data (unless datatype is byte)
            if(!isZarr && sizeof(T) > 1) {
                util::reverseEndiannessInplace<T>(&buffer[0], &buffer[0] + chunkSize);
            }

            // copy the requested sub-block of the chunk buffer into the output view
            const ConstArrayView<T> chunkView(buffer.data(), chunkShape, cOrderStrides(chunkShape));
            copyView(subview(chunkView, offsetInChunk, requestShape), outView);
        });
    }


    template<typename T, typename ITER>
    inline void readSubarray(const Dataset & ds,
                             const ArrayView<T> & out,
                             ITER roiBeginIter,
                             const int numberOfThreads=1) {

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter + out.ndim());
        types::ShapeType shape(out.shape.begin(), out.shape.end());
        ds.checkRequestShape(offset, shape);
        ds.checkRequestType(typeid(T));
        assert(out.ndim() == ds.dimension());

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        const auto & chunking = ds.chunking();
        chunking.getBlocksOverlappingRoi(offset, shape, chunkRequests);

        // sharded datasets use a shard-aware path (one read per shard, parallel across shards)
        if(ds.isSharded()) {
            readSubarraySharded<T>(ds, out, offset, shape, chunkRequests, numberOfThreads);
        } else {
            readSubarrayPlain<T>(ds, out, offset, shape, chunkRequests, numberOfThreads);
        }
    }


    // Prepare the full inner-chunk write buffer for `chunkId`, handling complete overlap,
    // zarr edge-chunk padding and partial overlap. The request's data is written into the
    // prepared buffer by `fillRequest(destView, offsetInRequest, requestShape)` (the
    // subarray path copies from the input view, the scalar path fills a value). For
    // partial overlap the existing chunk contents are obtained via `readExisting(buffer)`
    // (must fill `buffer` and return true if the chunk already existed, false otherwise).
    // `chunkShape` is set to the shape to hand to the chunk writer. Shared by the
    // non-sharded and sharded write paths; the only difference between them is the
    // "read existing chunk" source.
    template<typename T, typename FILL_REQUEST, typename READ_EXISTING>
    inline void prepareChunkWriteBuffer(const Dataset & ds,
                                        const util::Blocking & chunking,
                                        const types::ShapeType & offset,
                                        const types::ShapeType & shape,
                                        const types::ShapeType & chunkId,
                                        const bool isZarr,
                                        const std::size_t maxChunkSize,
                                        const T fillValue,
                                        std::vector<T> & buffer,
                                        types::ShapeType & chunkShape,
                                        FILL_REQUEST && fillRequest,
                                        READ_EXISTING && readExisting) {
        types::ShapeType offsetInRequest, requestShape, offsetInChunk;
        bool completeOvlp = chunking.getCoordinatesInRoi(chunkId, offset, shape,
                                                         offsetInRequest, requestShape,
                                                         offsetInChunk);
        ds.getChunkShape(chunkId, chunkShape);
        std::size_t chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(),
                                                std::size_t(1), std::multiplies<std::size_t>());

        // edge chunk written by zarr is padded to the full chunk shape
        if(chunkSize != maxChunkSize && isZarr) {
            completeOvlp = false;
            chunkShape = ds.defaultChunkShape();
            chunkSize = maxChunkSize;
        }
        if(chunkSize != buffer.size()) {
            buffer.resize(chunkSize);
        }

        if(completeOvlp) {
            fillRequest(makeView(buffer.data(), chunkShape), offsetInRequest, requestShape);
        } else {
            // preserve the part of the chunk that is not covered by the request
            if(!readExisting(buffer)) {
                std::fill(buffer.begin(), buffer.end(), fillValue);
            }
            const auto bufferView = makeView(buffer.data(), chunkShape);
            fillRequest(subview(bufferView, offsetInChunk, requestShape),
                        offsetInRequest, requestShape);
        }
    }


    // copy the request region from the input view into the prepared chunk buffer;
    // this is the `fillRequest` of the subarray write paths
    template<typename T>
    inline auto makeCopyFillRequest(const ConstArrayView<T> & in) {
        return [&in](const ArrayView<T> & dest,
                     const types::ShapeType & offsetInRequest,
                     const types::ShapeType & requestShape) {
            copyView(subview(in, offsetInRequest, requestShape), dest);
        };
    }


    // chunk-by-chunk write driver for non-sharded datasets; the request data is
    // written into each chunk buffer by `fillRequest` (see prepareChunkWriteBuffer).
    // Shared by writeSubarray (copy from input view) and writeScalar (fill value).
    template<typename T, typename FILL_REQUEST>
    inline void writePlainGeneric(const Dataset & ds,
                                  const types::ShapeType & offset,
                                  const types::ShapeType & shape,
                                  const std::vector<types::ShapeType> & chunkRequests,
                                  const int numberOfThreads,
                                  FILL_REQUEST && fillRequest) {

        const std::size_t maxChunkSize = ds.defaultChunkSize();
        const auto & chunking = ds.chunking();
        const bool isZarr = ds.isZarr();

        T fillValue;
        ds.getFillValue(&fillValue);

        forEachBuffered<T>(numberOfThreads, chunkRequests.size(), maxChunkSize, fillValue,
                           [&](std::vector<T> & buffer, const std::size_t chunkIndex){
            const auto & chunkId = chunkRequests[chunkIndex];
            types::ShapeType chunkShape;
            // partial-overlap reads come from the chunk file (preserving the varlen guard)
            prepareChunkWriteBuffer<T>(ds, chunking, offset, shape, chunkId, isZarr,
                                       maxChunkSize, fillValue, buffer, chunkShape,
                                       fillRequest,
                                       [&](std::vector<T> & buf) -> bool {
                                           if(!ds.chunkExists(chunkId)) {
                                               return false;
                                           }
                                           if(ds.readChunk(chunkId, &buf[0])) {
                                               throw std::runtime_error(
                                                   "Can't write to varlen chunks from multiarray");
                                           }
                                           return true;
                                       });
            ds.writeChunk(chunkId, &buffer[0]);
        });
    }


    template<typename T>
    inline void writeSubarrayPlain(const Dataset & ds,
                                   const ConstArrayView<T> & in,
                                   const types::ShapeType & offset,
                                   const types::ShapeType & shape,
                                   const std::vector<types::ShapeType> & chunkRequests,
                                   const int numberOfThreads) {
        writePlainGeneric<T>(ds, offset, shape, chunkRequests, numberOfThreads,
                             makeCopyFillRequest(in));
    }


    //
    // shard-aware paths (zarr v3 sharding)
    //
    // For a sharded dataset the chunk grid addresses INNER chunks, but the on-disk unit is
    // the shard. The generic paths above would dispatch one task per inner chunk, each of
    // which read-modify-writes (and, for writes, locks) the whole shard -- so K inner
    // chunks in a shard cost K full shard reads/rebuilds and serialize. The paths below
    // instead group the request's inner chunks by shard, touch each shard file exactly
    // once, and parallelize ACROSS shards (one task per shard, so no shard-level locking is
    // needed within a call).

    // group the inner-chunk requests by their shard; returns stable pointers into the map.
    inline void groupChunksByShard(
            const std::vector<types::ShapeType> & chunkRequests,
            const types::ShapeType & chunksPerShard,
            std::map<types::ShapeType, std::vector<types::ShapeType>> & shardGroups) {
        for(const auto & chunkId : chunkRequests) {
            shardGroups[util::shardId(chunkId, chunksPerShard)].push_back(chunkId);
        }
    }

    // shard-grouped write driver: one read-modify-write per shard, parallel across
    // shards; the request data is written into each inner-chunk buffer by `fillRequest`.
    // Shared by writeSubarray (copy from input view) and writeScalar (fill value).
    template<typename T, typename FILL_REQUEST>
    inline void writeShardedGeneric(const Dataset & ds,
                                    const types::ShapeType & offset,
                                    const types::ShapeType & shape,
                                    const std::vector<types::ShapeType> & chunkRequests,
                                    const int numberOfThreads,
                                    FILL_REQUEST && fillRequest) {
        // this path bypasses writeChunk, so enforce the file mode here (fail fast,
        // before any shard is read); writeShardBlobs checks again as the choke point
        if(!ds.mode().canWrite()) {
            throw std::invalid_argument("Cannot write data in file mode " + ds.mode().printMode());
        }

        T fillValue;
        ds.getFillValue(&fillValue);

        const std::size_t maxChunkSize = ds.defaultChunkSize();
        const auto & chunking = ds.chunking();
        const bool isZarr = ds.isZarr();

        const auto cps = util::chunksPerShard(ds.shardShape(), ds.defaultChunkShape());

        std::map<types::ShapeType, std::vector<types::ShapeType>> shardGroups;
        groupChunksByShard(chunkRequests, cps, shardGroups);
        std::vector<const std::pair<const types::ShapeType,
                                    std::vector<types::ShapeType>> *> groups;
        groups.reserve(shardGroups.size());
        for(const auto & kv : shardGroups) {
            groups.push_back(&kv);
        }

        // process a single shard: read it once, update the touched slots, write it once
        auto processShard = [&](std::vector<T> & buffer,
                                const types::ShapeType & shardCoord,
                                const std::vector<types::ShapeType> & innerChunks) {
            std::vector<std::vector<char>> blobs;
            ds.readShardBlobs(shardCoord, blobs);  // preserves untouched slots; cheap if absent

            types::ShapeType chunkShape;
            std::vector<char> blob;
            for(const auto & chunkId : innerChunks) {
                const std::size_t slot = util::shardSlot(chunkId, cps);
                prepareChunkWriteBuffer<T>(
                    ds, chunking, offset, shape, chunkId, isZarr, maxChunkSize,
                    fillValue, buffer, chunkShape, fillRequest,
                    [&](std::vector<T> & buf) -> bool {
                        // partial overlap: serve the existing chunk from the in-memory shard
                        if(blobs[slot].empty()) {
                            return false;
                        }
                        ds.decompress(blobs[slot], &buf[0], buf.size());
                        return true;
                    });
                const bool nonEmpty = ds.makeChunkBlob(chunkId, &buffer[0], blob);
                // swap instead of copy; every compressor overwrites its output, so
                // reusing `blob` (now holding the slot's old bytes) is safe
                if(nonEmpty) {
                    blobs[slot].swap(blob);
                } else {
                    blobs[slot].clear();
                }
            }
            ds.writeShardBlobs(shardCoord, blobs);
        };

        // one task per shard (so each shard file has a single writer -> no lock needed)
        forEachBuffered<T>(numberOfThreads, groups.size(), maxChunkSize, fillValue,
                           [&](std::vector<T> & buffer, const std::size_t i){
            processShard(buffer, groups[i]->first, groups[i]->second);
        });
    }


    template<typename T>
    inline void writeSubarraySharded(const Dataset & ds,
                                     const ConstArrayView<T> & in,
                                     const types::ShapeType & offset,
                                     const types::ShapeType & shape,
                                     const std::vector<types::ShapeType> & chunkRequests,
                                     const int numberOfThreads) {
        writeShardedGeneric<T>(ds, offset, shape, chunkRequests, numberOfThreads,
                               makeCopyFillRequest(in));
    }


    template<typename T>
    inline void readSubarraySharded(const Dataset & ds,
                                    const ArrayView<T> & out,
                                    const types::ShapeType & offset,
                                    const types::ShapeType & shape,
                                    const std::vector<types::ShapeType> & chunkRequests,
                                    const int numberOfThreads) {
        T fillValue;
        ds.getFillValue(&fillValue);

        const std::size_t maxChunkSize = ds.defaultChunkSize();
        const auto & maxChunkShape = ds.defaultChunkShape();
        const auto & chunking = ds.chunking();

        const auto cps = util::chunksPerShard(ds.shardShape(), ds.defaultChunkShape());

        std::map<types::ShapeType, std::vector<types::ShapeType>> shardGroups;
        groupChunksByShard(chunkRequests, cps, shardGroups);
        std::vector<const std::pair<const types::ShapeType,
                                    std::vector<types::ShapeType>> *> groups;
        groups.reserve(shardGroups.size());
        for(const auto & kv : shardGroups) {
            groups.push_back(&kv);
        }

        // inner chunks are always stored at the full chunk shape (zarr v3); the strides are
        // constant across chunks, so compute them once.
        const auto chunkStrides = cOrderStrides(maxChunkShape);

        // process a single shard: read it once, decode the requested inner chunks in place
        auto processShard = [&](std::vector<T> & buffer,
                                const types::ShapeType & shardCoord,
                                const std::vector<types::ShapeType> & innerChunks) {
            std::vector<char> shardBuf;
            std::vector<std::size_t> offsets, nbytes;
            const bool exists = ds.readShardRaw(shardCoord, shardBuf, offsets, nbytes);

            types::ShapeType offsetInRequest, requestShape, offsetInChunk;
            for(const auto & chunkId : innerChunks) {
                const std::size_t slot = util::shardSlot(chunkId, cps);
                chunking.getCoordinatesInRoi(chunkId, offset, shape,
                                             offsetInRequest, requestShape, offsetInChunk);
                const auto outView = subview(out, offsetInRequest, requestShape);

                // empty / never-written slot -> fill value
                if(!exists || nbytes[slot] == 0) {
                    fillView(outView, fillValue);
                    continue;
                }

                // decode the inner chunk straight from the shard buffer (no per-slot copy)
                if(buffer.size() != maxChunkSize) {
                    buffer.resize(maxChunkSize);
                }
                ds.decompress(shardBuf.data() + offsets[slot], nbytes[slot],
                              &buffer[0], maxChunkSize);
                const ConstArrayView<T> chunkView(buffer.data(), maxChunkShape, chunkStrides);
                copyView(subview(chunkView, offsetInChunk, requestShape), outView);
            }
        };

        forEachBuffered<T>(numberOfThreads, groups.size(), maxChunkSize, T(),
                           [&](std::vector<T> & buffer, const std::size_t i){
            processShard(buffer, groups[i]->first, groups[i]->second);
        });
    }


    template<typename T, typename ITER>
    inline void writeSubarray(const Dataset & ds,
                              const ConstArrayView<T> & in,
                              ITER roiBeginIter,
                              const int numberOfThreads=1) {

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter + in.ndim());
        types::ShapeType shape(in.shape.begin(), in.shape.end());

        ds.checkRequestShape(offset, shape);
        ds.checkRequestType(typeid(T));
        assert(in.ndim() == ds.dimension());

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        const auto & chunking = ds.chunking();
        chunking.getBlocksOverlappingRoi(offset, shape, chunkRequests);

        // sharded datasets use a shard-aware path (one RMW per shard, parallel across shards)
        if(ds.isSharded()) {
            writeSubarraySharded<T>(ds, in, offset, shape, chunkRequests, numberOfThreads);
        } else {
            writeSubarrayPlain<T>(ds, in, offset, shape, chunkRequests, numberOfThreads);
        }
    }


    // unique ptr API
    template<typename T, typename ITER>
    inline void readSubarray(std::unique_ptr<Dataset> & ds,
                             const ArrayView<T> & out,
                             ITER roiBeginIter,
                             const int numberOfThreads=1) {
       readSubarray<T>(*ds, out, roiBeginIter, numberOfThreads);
    }


    template<typename T, typename ITER>
    inline void writeSubarray(std::unique_ptr<Dataset> & ds,
                              const ConstArrayView<T> & in,
                              ITER roiBeginIter,
                              const int numberOfThreads=1) {
        writeSubarray<T>(*ds, in, roiBeginIter, numberOfThreads);
    }

}
}
