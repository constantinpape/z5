#pragma once

#include "z5/dataset.hxx"
#include "z5/types/types.hxx"
#include "z5/multiarray/xtensor_util.hxx"
#include "z5/util/threadpool.hxx"

#include "xtensor/xarray.hpp"
#include "xtensor/xstrided_view.hpp"
#include "xtensor/xadapt.hpp"

// free functions to read and write from xtensor multiarrays
// (it's hard to have these as members due to dynamic type inference)

namespace z5 {
namespace multiarray {

    template<typename T, typename ARRAY>
    inline void readSubarraySingleThreaded(const Dataset & ds,
                                           xt::xexpression<ARRAY> & outExpression,
                                           const types::ShapeType & offset,
                                           const types::ShapeType & shape,
                                           const std::vector<types::ShapeType> & chunkRequests) {
        // need to cast to the actual xtensor implementation
        auto & out = outExpression.derived_cast();

        types::ShapeType offsetInRequest, requestShape, chunkShape;
        types::ShapeType offsetInChunk;

        std::size_t chunkSize = ds.maxChunkSize();
        std::vector<T> buffer(chunkSize);

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

            //std::cout << "Reading chunk " << chunkId << std::endl;
            bool completeOvlp = ds.getCoordinatesInRequest(chunkId,
                                                           offset,
                                                           shape,
                                                           offsetInRequest,
                                                           requestShape,
                                                           offsetInChunk);

            // get the view in our array
            xt::slice_vector offsetSlice(out);
            sliceFromRoi(offsetSlice, offsetInRequest, requestShape);
            auto view = xt::dynamic_view(out, offsetSlice);

            // get the current chunk-shape and resize the buffer if necessary
            ds.getChunkShape(chunkId, chunkShape);
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(), 1, std::multiplies<std::size_t>());
            if(chunkSize != buffer.size()) {
                buffer.resize(chunkSize);
            }

            // read the current chunk into the buffer
            ds.readChunk(chunkId, &buffer[0]);

            // request and chunk completely overlap
            // -> we can read all the data from the chunk
            if(completeOvlp) {
                copyBufferToView(buffer, view, out.strides());
                //auto bufferView = xt::adapt(buffer, view.shape());
                //view = bufferView;
            }
            // request and chunk overlap only partially
            // -> we can read the chunk data only partially
            else {
                // get a view to the part of the buffer we are interested in
                auto fullBuffView = xt::adapt(buffer, chunkShape);
                xt::slice_vector bufSlice(fullBuffView);
                sliceFromRoi(bufSlice, offsetInChunk, requestShape);
                auto bufView = xt::dynamic_view(fullBuffView, bufSlice);

                // could also implement smart view for this,
                // but this would be kind of hard and premature optimization
                view = bufView;
            }
        }
    }


    template<typename T, typename ARRAY>
    inline void readSubarrayMultiThreaded(const Dataset & ds,
                                          xt::xexpression<ARRAY> & outExpression,
                                          const types::ShapeType & offset,
                                          const types::ShapeType & shape,
                                          const std::vector<types::ShapeType> & chunkRequests,
                                          const int numberOfThreads) {
        // need to cast to the actual xtensor implementation
        auto & out = outExpression.derived_cast();

        // construct threadpool and make a buffer for each thread
        util::ThreadPool tp(numberOfThreads);
        const int nThreads = tp.nThreads();

        std::size_t chunkSize = ds.maxChunkSize();
        typedef std::vector<T> Buffer;
        std::vector<Buffer> threadBuffers(nThreads, Buffer(chunkSize));

        // read the chunks in parallel
        const std::size_t nChunks = chunkRequests.size();
        util::parallel_foreach(tp, nChunks, [&](const int tId, const std::size_t chunkIndex){

            const auto & chunkId = chunkRequests[chunkIndex];
            auto & buffer = threadBuffers[tId];

            types::ShapeType offsetInRequest, requestShape, chunkShape;
            types::ShapeType offsetInChunk;

            //std::cout << "Reading chunk " << chunkId << std::endl;
            bool completeOvlp = ds.getCoordinatesInRequest(chunkId,
                                                           offset,
                                                           shape,
                                                           offsetInRequest,
                                                           requestShape,
                                                           offsetInChunk);

            // get the view in our array
            xt::slice_vector offsetSlice(out);
            sliceFromRoi(offsetSlice, offsetInRequest, requestShape);
            auto view = xt::dynamic_view(out, offsetSlice);

            // get the current chunk-shape and resize the buffer if necessary
            ds.getChunkShape(chunkId, chunkShape);
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(), 1, std::multiplies<std::size_t>());
            if(chunkSize != buffer.size()) {
                buffer.resize(chunkSize);
            }

            // read the current chunk into the buffer
            ds.readChunk(chunkId, &buffer[0]);

            // request and chunk completely overlap
            // -> we can read all the data from the chunk
            if(completeOvlp) {
                copyBufferToView(buffer, view, out.strides());
                //auto bufferView = xt::adapt(buffer, view.shape());
                //view = bufferView;
            }
            // request and chunk overlap only partially
            // -> we can read the chunk data only partially
            else {
                // get a view to the part of the buffer we are interested in
                auto fullBuffView = xt::adapt(buffer, chunkShape);
                xt::slice_vector bufSlice(fullBuffView);
                sliceFromRoi(bufSlice, offsetInChunk, requestShape);
                auto bufView = xt::dynamic_view(fullBuffView, bufSlice);

                // could also implement smart view for this,
                // but this would be kind of hard and premature optimization
                view = bufView;
            }
        });
    }


    template<typename T, typename ARRAY, typename ITER>
    inline void readSubarray(const Dataset & ds,
                             xt::xexpression<ARRAY> & outExpression,
                             ITER roiBeginIter,
                             const int numberOfThreads=1) {

        // need to cast to the actual xtensor implementation
        auto & out = outExpression.derived_cast();

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter+out.dimension());
        types::ShapeType shape(out.shape().begin(), out.shape().end());
        ds.checkRequestShape(offset, shape);
        ds.checkRequestType(typeid(T));

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        ds.getChunkRequests(offset, shape, chunkRequests);

        // read single or multi-threaded
        if(numberOfThreads == 1) {
            readSubarraySingleThreaded<T>(ds, out, offset, shape, chunkRequests);
        } else {
            readSubarrayMultiThreaded<T>(ds, out, offset, shape, chunkRequests, numberOfThreads);
        }
    }


    template<typename T, typename ARRAY>
    inline void writeSubarraySingleThreaded(const Dataset & ds,
                                            const xt::xexpression<ARRAY> & inExpression,
                                            const types::ShapeType & offset,
                                            const types::ShapeType & shape,
                                            const std::vector<types::ShapeType> & chunkRequests) {

        const auto & in = inExpression.derived_cast();
        types::ShapeType offsetInRequest, requestShape, chunkShape;
        types::ShapeType offsetInChunk;

        std::size_t chunkSize = ds.maxChunkSize();
        std::vector<T> buffer(chunkSize);

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = ds.getCoordinatesInRequest(chunkId, offset, shape, offsetInRequest, requestShape, offsetInChunk);
            ds.getChunkShape(chunkId, chunkShape);
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(), 1, std::multiplies<std::size_t>());

            // get the view into the in-array
            xt::slice_vector offsetSlice(in);
            sliceFromRoi(offsetSlice, offsetInRequest, requestShape);
            const auto view = xt::dynamic_view(in, offsetSlice);

            // resize buffer if necessary
            if(chunkSize != buffer.size()) {
                buffer.resize(chunkSize);
            }

            // request and chunk overlap completely
            // -> we can write the whole chunk
            if(completeOvlp) {
                copyViewToBuffer(view, buffer, in.strides());
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
                xt::slice_vector bufSlice(fullBuffView);
                sliceFromRoi(bufSlice, offsetInChunk, requestShape);
                auto bufView = xt::dynamic_view(fullBuffView, bufSlice);

                // could also implement smart view for this,
                // but this would be kind of hard and premature optimization
                bufView = view;

                // write the chunk
                ds.writeChunk(chunkId, &buffer[0]);
            }
        }
    }


    template<typename T, typename ARRAY>
    inline void writeSubarrayMultiThreaded(const Dataset & ds,
                                           const xt::xexpression<ARRAY> & inExpression,
                                           const types::ShapeType & offset,
                                           const types::ShapeType & shape,
                                           const std::vector<types::ShapeType> & chunkRequests,
                                           const int numberOfThreads) {

        const auto & in = inExpression.derived_cast();

        // construct threadpool and make a buffer for each thread
        util::ThreadPool tp(numberOfThreads);
        const int nThreads = tp.nThreads();

        std::size_t chunkSize = ds.maxChunkSize();
        typedef std::vector<T> Buffer;
        std::vector<Buffer> threadBuffers(nThreads, Buffer(chunkSize));

        // write the chunks in parallel
        const std::size_t nChunks = chunkRequests.size();
        util::parallel_foreach(tp, nChunks, [&](const int tId, const std::size_t chunkIndex){

            const auto & chunkId = chunkRequests[chunkIndex];
            auto & buffer = threadBuffers[tId];

            types::ShapeType offsetInRequest, requestShape, chunkShape;
            types::ShapeType offsetInChunk;

            bool completeOvlp = ds.getCoordinatesInRequest(chunkId, offset, shape, offsetInRequest, requestShape, offsetInChunk);
            ds.getChunkShape(chunkId, chunkShape);
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(), 1, std::multiplies<std::size_t>());

            // get the view into the in-array
            xt::slice_vector offsetSlice(in);
            sliceFromRoi(offsetSlice, offsetInRequest, requestShape);
            const auto view = xt::dynamic_view(in, offsetSlice);

            // resize buffer if necessary
            if(chunkSize != buffer.size()) {
                buffer.resize(chunkSize);
            }

            // request and chunk overlap completely
            // -> we can write the whole chunk
            if(completeOvlp) {
                copyViewToBuffer(view, buffer, in.strides());
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
                xt::slice_vector bufSlice(fullBuffView);
                sliceFromRoi(bufSlice, offsetInChunk, requestShape);
                auto bufView = xt::dynamic_view(fullBuffView, bufSlice);

                // could also implement smart view for this,
                // but this would be kind of hard and premature optimization
                bufView = view;

                // write the chunk
                ds.writeChunk(chunkId, &buffer[0]);
            }
        });
    }


    template<typename T, typename ARRAY, typename ITER>
    inline void writeSubarray(const Dataset & ds,
                              const xt::xexpression<ARRAY> & inExpression,
                              ITER roiBeginIter,
                              const int numberOfThreads=1) {

        const auto & in = inExpression.derived_cast();

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter+in.dimension());
        types::ShapeType shape(in.shape().begin(), in.shape().end());

        ds.checkRequestShape(offset, shape);
        ds.checkRequestType(typeid(T));

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        ds.getChunkRequests(offset, shape, chunkRequests);

        // write data multi or single threaded
        if(numberOfThreads == 1) {
            writeSubarraySingleThreaded<T>(ds, in, offset, shape, chunkRequests);
        } else {
            writeSubarrayMultiThreaded<T>(ds, in, offset, shape, chunkRequests, numberOfThreads);
        }

    }


    // unique ptr API
    template<typename T, typename ARRAY, typename ITER>
    inline void readSubarray(std::unique_ptr<Dataset> & ds,
                             xt::xexpression<ARRAY> & out,
                             ITER roiBeginIter,
                             const int numberOfThreads=1) {
       readSubarray<T>(*ds, out, roiBeginIter, numberOfThreads);
    }


    template<typename T, typename ARRAY, typename ITER>
    inline void writeSubarray(std::unique_ptr<Dataset> & ds,
                              const xt::xexpression<ARRAY> & in,
                              ITER roiBeginIter,
                              const int numberOfThreads=1) {
        writeSubarray<T>(*ds, in, roiBeginIter, numberOfThreads);
    }


    template<typename T, typename ARRAY_IN, typename ARRAY_OUT>
    void convertArrayToFormat(const Dataset & ds,
                              const xt::xexpression<ARRAY_IN> & inExpression,
                              xt::xexpression<ARRAY_OUT> & outExpression) {
        const auto & in = inExpression.derived_cast();
        auto & out = outExpression.derived_cast();

        std::vector<char> tmp;
        types::ShapeType shape(in.shape().begin(), in.shape().end());
        ds.dataToFormat(&in(0), tmp, shape);

        out.resize({(int64_t) tmp.size()});
        // TODO use xadapt instead
        std::copy(tmp.begin(), tmp.end(), out.begin());
    }

    template<typename T, typename ARRAY_IN, typename ARRAY_OUT>
    inline auto convertArrayToFormat(std::unique_ptr<Dataset> & ds, const xt::xexpression<ARRAY_IN> & in, xt::xexpression<ARRAY_OUT> & out) {
        return convertArrayToFormat<T>(*ds, in, out);
    }

}
}
