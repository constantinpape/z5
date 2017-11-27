#pragma once

#include "z5/dataset.hxx"
#include "z5/types/types.hxx"

#include "xtensor/xarray.hpp"
#include "xtensor/xview.hpp"
#include "xtensor/xstridedview.hpp"
#include "xtensor/xadapt.hpp"

// free functions to read and write from xtensor multiarrays
// (it's hard to have these as members due to dynamic type inference)

namespace z5 {
namespace multiarray {


    // small helper function to convert a ROI given by (offset, shape) into
    // a proper xtensor sliceing
    inline void sliceFromRoi(xt::slice_vector & roiSlice, const types::ShapeType & offset, const types::ShapeType & shape) {
        for(int d = 0; d < offset.size(); ++d) {
            roiSlice.push_back(xt::range(offset[d], offset[d] + shape[d]));
        }
    }


    inline size_t offsetAndStridesFromRoi(const types::ShapeType & outStrides,
                                          const types::ShapeType & requestShape,
                                          const types::ShapeType & offsetInRequest,
                                          types::ShapeType & requestStrides) {
        // first, calculate the flat offset
        // -> sum( coordinate_offset * out_strides )
        size_t flatOffset = 1;
        for(int d = 0; d < outStrides.size(); ++d) {
            flatOffset += (outStrides[d] * offsetInRequest[d]);
        }

        // TODO this depends on the laout type.....
        // next, calculate the new strides
        requestStrides.resize(requestShape.size());
        for(int d = 0; d < requestShape.size(); ++d) {
            requestStrides[d] = std::accumulate(requestShape.rbegin(), requestShape.rbegin() + d, 1, std::multiplies<size_t>());
        }
        std::reverse(requestStrides.begin(), requestStrides.end());

        return flatOffset;
    }


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

        // TODO writing directly to a view does not work, probably because it is not continuous in memory (?!)
        // that's why we use the buffer for now.
        // In the end, it would be nice to do this without the buffer (which introduces an additional copy)
        // Benchmark and figure this out !

        // we can use 1d buffers and adapt to xtensor !
        size_t chunkSize = ds.maxChunkSize();
        std::vector<T> buffer(chunkSize);

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

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

            // get the strided view
            //size_t flatOffset = offsetAndStridesFromRoi(outStrides, requestShape, offsetInRequest, requestStrides);
            //auto view = xt::strided_view(out, requestShape, requestStrides, flatOffset, xt::layout_type::dynamic);

            // get the current chunk-shape and resize the buffer if necessary
            ds.getChunkShape(chunkId, chunkShape);
            // TODO accumulate directly, w/o further read from data
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(), 1, std::multiplies<size_t>());
            if(chunkSize != buffer.size()) {
                buffer.resize(chunkSize);
            }

            // read the current chunk into the buffer
            ds.readChunk(chunkId, &buffer[0]);

            // request and chunk completely overlap
            // -> we can read all the data from the chunk
            if(completeOvlp) {
                // TODO figure out which one is faster
                // copy the data from the buffer into the view

                // via xadapt and assignment operator
                auto buffView = xt::xadapt(buffer, chunkShape);
                view = buffView;

                // or via copy from flat buffer
                //std::copy(buffer.begin(), buffer.end(), view.begin());
            }
            // request and chunk overlap only partially
            // -> we can read the chunk data only partially
            else {

                // get a view to the part of the buffer we are interested in
                auto fullBuffView = xt::xadapt(buffer, chunkShape);
                xt::slice_vector bufSlice(fullBuffView);
                sliceFromRoi(bufSlice, offsetInChunk, requestShape);
                auto bufView = xt::dynamic_view(fullBuffView, bufSlice);

                // TODO figure out which one is faster
                view = bufView;
                //std::copy(bufView.begin(), bufView.end(), view.begin());
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

        // TODO try to figure out writing without memcopys for fully overlapping chunks
        // we can use 1d buffers and adapt to xtensor !
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

                // for now this does not work without copying,
                // because views are not contiguous in memory (I think ?!)
                // TODO would be nice to figure this out -> benchmark first !
                //ds.writeChunk(chunkId, &view(0));

                //buffer = view;
                std::copy(view.begin(), view.end(), buffer.begin());
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
                //bufView = view;
                std::copy(view.begin(), view.end(), bufView.begin());

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
