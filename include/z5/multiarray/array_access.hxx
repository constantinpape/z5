#pragma once

#include <cassert>
#include <numeric>

#include "z5/dataset.hxx"
#include "z5/types/types.hxx"
#include "z5/multiarray/array_view.hxx"
#include "z5/multiarray/array_util.hxx"
#include "z5/util/threadpool.hxx"


namespace z5 {
namespace multiarray {


    template<typename T>
    inline void readSubarraySingleThreaded(const Dataset & ds,
                                           const ArrayView<T> & out,
                                           const types::ShapeType & offset,
                                           const types::ShapeType & shape,
                                           const std::vector<types::ShapeType> & chunkRequests) {

        types::ShapeType offsetInRequest, requestShape, chunkShape;
        types::ShapeType offsetInChunk;

        const std::size_t maxChunkSize = ds.defaultChunkSize();
        const auto & maxChunkShape = ds.defaultChunkShape();

        std::size_t chunkSize, chunkStoreSize;
        std::vector<T> buffer(maxChunkSize);

        const auto & chunking = ds.chunking();
        const bool isZarr = ds.isZarr();

        // get the fillvalue
        T fillValue;
        ds.getFillValue(&fillValue);

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

            chunking.getCoordinatesInRoi(chunkId, offset, shape,
                                         offsetInRequest, requestShape, offsetInChunk);

            // get the view into our array
            const auto outView = subview(out, offsetInRequest, requestShape);

            // check if this chunk exists, if not fill output with fill value
            if(!ds.chunkExists(chunkId)) {
                fillView(outView, fillValue);
                continue;
            }

            // get the shape and size of the chunk (in the actual grid)
            ds.getChunkShape(chunkId, chunkShape);
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(),
                                        1, std::multiplies<std::size_t>());

            // read the data from storage
            std::vector<char> dataBuffer;
            ds.readRawChunk(chunkId, dataBuffer);

            // get the shape of the chunk (as it is stored)
            chunkStoreSize = maxChunkSize;
            if(!isZarr) {
                if(util::read_n5_header(dataBuffer, chunkStoreSize)) {
                    throw std::runtime_error("Can't read from varlen chunks to multiarray");
                }
            }

            // if this is an edge chunk and the size of the chunk stored is different from
            // the size of the chunk in the grid (this is the case when written by zarr)
            // we need to use the full (default) chunk shape for the buffer; the leading
            // block of the buffer holds the valid data.
            if(chunkStoreSize != chunkSize) {
                chunkSize = maxChunkSize;
                chunkShape = maxChunkShape;
            }

            // resize the buffer if necessary
            if(chunkSize != buffer.size()) {
                buffer.resize(chunkSize);
            }

            // decompress the data
            ds.decompress(dataBuffer, &buffer[0], chunkSize);

            // reverse the endianness for N5 data (unless datatype is byte)
            if(!isZarr && sizeof(T) > 1) {
                util::reverseEndiannessInplace<T>(&buffer[0], &buffer[0] + chunkSize);
            }

            // copy the requested sub-block of the chunk buffer into the output view
            const ConstArrayView<T> chunkView(buffer.data(), chunkShape, cOrderStrides(chunkShape));
            copyView(subview(chunkView, offsetInChunk, requestShape), outView);
        }
    }


    template<typename T>
    inline void readSubarrayMultiThreaded(const Dataset & ds,
                                          const ArrayView<T> & out,
                                          const types::ShapeType & offset,
                                          const types::ShapeType & shape,
                                          const std::vector<types::ShapeType> & chunkRequests,
                                          const int numberOfThreads) {

        // construct threadpool and make a buffer for each thread
        util::ThreadPool tp(numberOfThreads);
        const int nThreads = tp.nThreads();

        const std::size_t maxChunkSize = ds.defaultChunkSize();
        const auto & maxChunkShape = ds.defaultChunkShape();

        typedef std::vector<T> Buffer;
        std::vector<Buffer> threadBuffers(nThreads, Buffer(maxChunkSize));

        const auto & chunking = ds.chunking();
        const bool isZarr = ds.isZarr();

        // get the fillvalue
        T fillValue;
        ds.getFillValue(&fillValue);

        // read the chunks in parallel
        const std::size_t nChunks = chunkRequests.size();
        util::parallel_foreach(tp, nChunks, [&](const int tId, const std::size_t chunkIndex){

            const auto & chunkId = chunkRequests[chunkIndex];
            auto & buffer = threadBuffers[tId];

            types::ShapeType offsetInRequest, requestShape, chunkShape;
            types::ShapeType offsetInChunk;

            chunking.getCoordinatesInRoi(chunkId, offset, shape,
                                         offsetInRequest, requestShape, offsetInChunk);

            // get the view into our array
            const auto outView = subview(out, offsetInRequest, requestShape);

            // check if this chunk exists, if not fill output with fill value
            if(!ds.chunkExists(chunkId)) {
                fillView(outView, fillValue);
                return;
            }

            // get the current chunk-shape
            ds.getChunkShape(chunkId, chunkShape);
            std::size_t chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(),
                                                    1, std::multiplies<std::size_t>());

            // read the data from storage
            std::vector<char> dataBuffer;
            ds.readRawChunk(chunkId, dataBuffer);

            // get the shape of the chunk (as it is stored)
            std::size_t chunkStoreSize = maxChunkSize;
            if(!isZarr) {
                if(util::read_n5_header(dataBuffer, chunkStoreSize)) {
                    throw std::runtime_error("Can't read from varlen chunks to multiarray");
                }
            }

            // edge chunk stored at full size (zarr) -> use the default chunk shape
            if(chunkStoreSize != chunkSize) {
                chunkSize = maxChunkSize;
                chunkShape = maxChunkShape;
            }

            // resize the buffer if necessary
            if(chunkSize != buffer.size()) {
                buffer.resize(chunkSize);
            }

            // decompress the data
            ds.decompress(dataBuffer, &buffer[0], chunkSize);

            // reverse the endianness for N5 data (unless datatype is byte)
            if(!isZarr && sizeof(T) > 1) {
                util::reverseEndiannessInplace<T>(&buffer[0], &buffer[0] + chunkSize);
            }

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

        // read single or multi-threaded
        if(numberOfThreads == 1) {
            readSubarraySingleThreaded<T>(ds, out, offset, shape, chunkRequests);
        } else {
            readSubarrayMultiThreaded<T>(ds, out, offset, shape, chunkRequests, numberOfThreads);
        }
    }


    template<typename T>
    inline void writeSubarraySingleThreaded(const Dataset & ds,
                                            const ConstArrayView<T> & in,
                                            const types::ShapeType & offset,
                                            const types::ShapeType & shape,
                                            const std::vector<types::ShapeType> & chunkRequests) {

        types::ShapeType offsetInRequest, requestShape, chunkShape;
        types::ShapeType offsetInChunk;

        // get the fillvalue
        T fillValue;
        ds.getFillValue(&fillValue);

        const std::size_t maxChunkSize = ds.defaultChunkSize();
        std::size_t chunkSize = maxChunkSize;
        std::vector<T> buffer(chunkSize, fillValue);

        const auto & chunking = ds.chunking();

        // if we have a zarr dataset, we always write the full chunk
        const bool isZarr = ds.isZarr();

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = chunking.getCoordinatesInRoi(chunkId, offset, shape,
                                                             offsetInRequest, requestShape,
                                                             offsetInChunk);
            // get shape and size of this chunk
            ds.getChunkShape(chunkId, chunkShape);
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(),
                                        1, std::multiplies<std::size_t>());

            // get the view into the in-array
            const auto inView = subview(in, offsetInRequest, requestShape);

            // if this is an edge chunk and we are writing zarr format,
            // we need to write the full chunk padded with the fill value
            if(chunkSize != maxChunkSize && isZarr) {
                completeOvlp = false;
                chunkShape = ds.defaultChunkShape();
                chunkSize = maxChunkSize;
                std::fill(buffer.begin(), buffer.end(), fillValue);
            }

            // resize the buffer if necessary
            if(chunkSize != buffer.size()) {
                buffer.resize(chunkSize);
            }

            // request and chunk overlap completely
            // -> we can write the whole chunk
            if(completeOvlp) {
                copyView(inView, makeView(buffer.data(), chunkShape));
                ds.writeChunk(chunkId, &buffer[0]);
            }

            // request and chunk overlap only partially
            // -> we can only write partial data and need
            // to preserve the data that will not be written
            else {

                // check if this chunk exists, and if it does, read the chunk's data
                // to preserve the part that is not written to
                if(ds.chunkExists(chunkId)) {
                    // load the current data into the buffer
                    if(ds.readChunk(chunkId, &buffer[0])) {
                        throw std::runtime_error("Can't write to varlen chunks from multiarray");
                    }
                } else {
                    std::fill(buffer.begin(), buffer.end(), fillValue);
                }

                // overwrite the data that is covered by the request
                const auto bufferView = makeView(buffer.data(), chunkShape);
                copyView(inView, subview(bufferView, offsetInChunk, requestShape));

                // write the chunk
                ds.writeChunk(chunkId, &buffer[0]);
            }
        }
    }


    template<typename T>
    inline void writeSubarrayMultiThreaded(const Dataset & ds,
                                           const ConstArrayView<T> & in,
                                           const types::ShapeType & offset,
                                           const types::ShapeType & shape,
                                           const std::vector<types::ShapeType> & chunkRequests,
                                           const int numberOfThreads) {

        // get the fillvalue
        T fillValue;
        ds.getFillValue(&fillValue);

        // construct threadpool and make a buffer for each thread
        util::ThreadPool tp(numberOfThreads);
        const int nThreads = tp.nThreads();

        const std::size_t maxChunkSize = ds.defaultChunkSize();
        typedef std::vector<T> Buffer;
        std::vector<Buffer> threadBuffers(nThreads, Buffer(maxChunkSize, fillValue));

        const auto & chunking = ds.chunking();
        const bool isZarr = ds.isZarr();

        // write the chunks in parallel
        const std::size_t nChunks = chunkRequests.size();
        util::parallel_foreach(tp, nChunks, [&](const int tId, const std::size_t chunkIndex){

            const auto & chunkId = chunkRequests[chunkIndex];
            auto & buffer = threadBuffers[tId];

            types::ShapeType offsetInRequest, requestShape, chunkShape;
            types::ShapeType offsetInChunk;

            bool completeOvlp = chunking.getCoordinatesInRoi(chunkId, offset, shape,
                                                             offsetInRequest, requestShape,
                                                             offsetInChunk);
            ds.getChunkShape(chunkId, chunkShape);
            std::size_t chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(),
                                                    1, std::multiplies<std::size_t>());

            // get the view into the in-array
            const auto inView = subview(in, offsetInRequest, requestShape);

            // if this is an edge chunk and we are writing zarr format,
            // we need to write the full chunk padded with the fill value
            if(chunkSize != maxChunkSize && isZarr) {
                completeOvlp = false;
                chunkSize = maxChunkSize;
                chunkShape = ds.defaultChunkShape();
                std::fill(buffer.begin(), buffer.end(), fillValue);
            }

            // resize buffer if necessary
            if(chunkSize != buffer.size()) {
                buffer.resize(chunkSize);
            }

            // request and chunk overlap completely
            // -> we can write the whole chunk
            if(completeOvlp) {
                copyView(inView, makeView(buffer.data(), chunkShape));
                ds.writeChunk(chunkId, &buffer[0]);
            }

            // request and chunk overlap only partially
            // -> we can only write partial data and need
            // to preserve the data that will not be written
            else {
                if(ds.chunkExists(chunkId)) {
                    // load the current data into the buffer
                    if(ds.readChunk(chunkId, &buffer[0])) {
                        throw std::runtime_error("Can't write to varlen chunks from multiarray");
                    }
                } else {
                    std::fill(buffer.begin(), buffer.end(), fillValue);
                }

                // overwrite the data that is covered by the request
                const auto bufferView = makeView(buffer.data(), chunkShape);
                copyView(inView, subview(bufferView, offsetInChunk, requestShape));

                // write the chunk
                ds.writeChunk(chunkId, &buffer[0]);
            }
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

        // write data multi or single threaded
        if(numberOfThreads == 1) {
            writeSubarraySingleThreaded<T>(ds, in, offset, shape, chunkRequests);
        } else {
            writeSubarrayMultiThreaded<T>(ds, in, offset, shape, chunkRequests, numberOfThreads);
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
