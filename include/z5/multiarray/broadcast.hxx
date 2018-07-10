#pragma once

#include "z5/dataset.hxx"
#include "z5/multiarray/xtensor_access.hxx"
#include "z5/util/threadpool.hxx"

#include "xtensor/xeval.hpp"


namespace z5 {
namespace multiarray {


    template<typename T>
    void writeScalarSingleThreaded(const Dataset & ds,
                                   const types::ShapeType & offset,
                                   const types::ShapeType & shape,
                                   const T val,
                                   const std::vector<types::ShapeType> & chunkRequests) {

        types::ShapeType offsetInRequest, requestShape, chunkShape, offsetInChunk;
        // out buffer holding data for a single chunk
        std::size_t chunkSize = ds.maxChunkSize();
        std::vector<T> buffer(chunkSize, val);

        // iterate over the chunks and write the buffer
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = ds.getCoordinatesInRequest(chunkId,
                                                           offset,
                                                           shape,
                                                           offsetInRequest,
                                                           requestShape,
                                                           offsetInChunk);


            ds.getChunkShape(chunkId, chunkShape);
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(), 1, std::multiplies<std::size_t>());

            // reshape buffer if necessary
            if(buffer.size() != chunkSize) {
                buffer.resize(chunkSize, val);
            }

            // request and chunk overlap completely
            // -> we can write the whole chunk
            if(completeOvlp) {
                ds.writeChunk(chunkId, &buffer[0]);
            }

            // request and chunk overlap only partially
            // -> we can only write partial data and need
            // to preserve the data that will not be written
            else {
                // load the current data into the buffer
                ds.readChunk(chunkId, &buffer[0]);
                // overwrite the data that is covered by the request
                auto fullBuffView = xt::adapt(buffer, chunkShape);
                xt::slice_vector bufSlice;
                sliceFromRoi(bufSlice, offsetInChunk, requestShape);
                auto bufView = xt::strided_view(fullBuffView, bufSlice);
                bufView = val;
                ds.writeChunk(chunkId, &buffer[0]);

                // need to reset the buffer to our fill value
                std::fill(buffer.begin(), buffer.end(), val);
            }
        }
    }


    template<typename T>
    void writeScalarMultiThreaded(const Dataset & ds,
                                  const types::ShapeType & offset,
                                  const types::ShapeType & shape,
                                  const T val,
                                  const std::vector<types::ShapeType> & chunkRequests,
                                  const int numberOfThreads) {

        // construct threadpool and make a buffer for each thread
        util::ThreadPool tp(numberOfThreads);
        const int nThreads = tp.nThreads();

        // out buffer holding data for a single chunk
        std::size_t chunkSize = ds.maxChunkSize();
        typedef std::vector<T> Buffer;
        std::vector<Buffer> threadBuffers(nThreads, Buffer(chunkSize, val));

        // write scalar to the chunks in parallel
        const std::size_t nChunks = chunkRequests.size();
        util::parallel_foreach(tp, nChunks, [&](const int tId, const std::size_t chunkIndex){

            const auto & chunkId = chunkRequests[chunkIndex];
            auto & buffer = threadBuffers[tId];

            types::ShapeType offsetInRequest, requestShape, chunkShape, offsetInChunk;
            bool completeOvlp = ds.getCoordinatesInRequest(chunkId,
                                                           offset,
                                                           shape,
                                                           offsetInRequest,
                                                           requestShape,
                                                           offsetInChunk);


            ds.getChunkShape(chunkId, chunkShape);
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(), 1, std::multiplies<std::size_t>());

            // reshape buffer if necessary
            if(buffer.size() != chunkSize) {
                buffer.resize(chunkSize, val);
            }

            // request and chunk overlap completely
            // -> we can write the whole chunk
            if(completeOvlp) {
                ds.writeChunk(chunkId, &buffer[0]);
            }

            // request and chunk overlap only partially
            // -> we can only write partial data and need
            // to preserve the data that will not be written
            else {
                // load the current data into the buffer
                ds.readChunk(chunkId, &buffer[0]);
                // overwrite the data that is covered by the request
                auto fullBuffView = xt::adapt(buffer, chunkShape);
                xt::slice_vector bufSlice;
                sliceFromRoi(bufSlice, offsetInChunk, requestShape);
                auto bufView = xt::strided_view(fullBuffView, bufSlice);
                bufView = val;
                ds.writeChunk(chunkId, &buffer[0]);

                // need to reset the buffer to our fill value
                std::fill(buffer.begin(), buffer.end(), val);
            }
        });
    }

    template<typename T, typename ITER>
    void writeScalar(const Dataset & ds,
                     ITER roiBeginIter,
                     ITER roiShapeIter,
                     const T val,
                     const int numberOfThreads=1) {

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter+ds.dimension());
        types::ShapeType shape(roiShapeIter, roiShapeIter+ds.dimension());
        ds.checkRequestShape(offset, shape);
        ds.checkRequestType(typeid(T));

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        ds.getChunkRequests(offset, shape, chunkRequests);

        // write scalar single or multi-threaded
        if(numberOfThreads == 1) {
            writeScalarSingleThreaded<T>(ds, offset, shape, val, chunkRequests);
        } else {
            writeScalarMultiThreaded<T>(ds, offset, shape, val, chunkRequests, numberOfThreads);
        }
    }


    // unique ptr API
    template<typename T, typename ITER>
    inline void writeScalar(std::unique_ptr<Dataset> & ds,
                            ITER roiBeginIter,
                            ITER roiShapeIter,
                            const T val,
                            const int numberOfThreads=1) {
       writeScalar(*ds, roiBeginIter, roiShapeIter, val, numberOfThreads);
    }
}
}
