#pragma once

#include "z5/dataset.hxx"
#include "z5/types/types.hxx"
#include "z5/multiarray/xtensor_util.hxx"

#include "xtensor/xarray.hpp"
#include "xtensor/xstridedview.hpp"
#include "xtensor/xadapt.hpp"

// free functions to read and write from xtensor multiarrays
// (it's hard to have these as members due to dynamic type inference)

namespace z5 {
namespace multiarray {

    template<typename T, typename ARRAY, typename ITER>
    inline void readSubarray(const Dataset & ds, xt::xexpression<ARRAY> & outExpression, ITER roiBeginIter) {

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

        types::ShapeType offsetInRequest, requestShape, chunkShape;
        types::ShapeType offsetInChunk;

        size_t chunkSize = ds.maxChunkSize();
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
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(), 1, std::multiplies<size_t>());
            if(chunkSize != buffer.size()) {
                buffer.resize(chunkSize);
            }

            // read the current chunk into the buffer
            ds.readChunk(chunkId, &buffer[0]);

            // request and chunk completely overlap
            // -> we can read all the data from the chunk
            if(completeOvlp) {
                copyBufferToView(buffer, view, out.strides());
            }
            // request and chunk overlap only partially
            // -> we can read the chunk data only partially
            else {
                // get a view to the part of the buffer we are interested in
                auto fullBuffView = xt::xadapt(buffer, chunkShape);
                xt::slice_vector bufSlice(fullBuffView);
                sliceFromRoi(bufSlice, offsetInChunk, requestShape);
                auto bufView = xt::dynamic_view(fullBuffView, bufSlice);

                // could also implement smart view for this,
                // but this would be kind of hard and premature optimization
                view = bufView;
            }
        }
    }


    template<typename T, typename ARRAY, typename ITER>
    void writeSubarray(const Dataset & ds, const xt::xexpression<ARRAY> & inExpression, ITER roiBeginIter) {

        const auto & in = inExpression.derived_cast();

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter+in.dimension());
        types::ShapeType shape(in.shape().begin(), in.shape().end());

        ds.checkRequestShape(offset, shape);
        ds.checkRequestType(typeid(T));

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        ds.getChunkRequests(offset, shape, chunkRequests);

        types::ShapeType offsetInRequest, requestShape, chunkShape;
        types::ShapeType offsetInChunk;

        size_t chunkSize = ds.maxChunkSize();
        std::vector<T> buffer(chunkSize);

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = ds.getCoordinatesInRequest(chunkId, offset, shape, offsetInRequest, requestShape, offsetInChunk);
            ds.getChunkShape(chunkId, chunkShape);
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(), 1, std::multiplies<size_t>());

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
                auto fullBuffView = xt::xadapt(buffer, chunkShape);
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


    // unique ptr API
    template<typename T, typename ARRAY, typename ITER>
    inline void readSubarray(std::unique_ptr<Dataset> & ds, xt::xexpression<ARRAY> & out, ITER roiBeginIter) {
       readSubarray<T>(*ds, out, roiBeginIter);
    }

    template<typename T, typename ARRAY, typename ITER>
    inline void writeSubarray(std::unique_ptr<Dataset> & ds, const xt::xexpression<ARRAY> & in, ITER roiBeginIter) {
        writeSubarray<T>(*ds, in, roiBeginIter);
    }

}
}
