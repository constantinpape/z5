#pragma once

#include "z5/dataset.hxx"
#include "z5/types/types.hxx"

#include "xtensor/xarray.hpp"
#include "xtensor/xview.hpp"
#include "xtensor/xstridedview.hpp"

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


    template<typename T, typename ARRAY, typename ITER>
    void readSubarray(const Dataset & ds, xt::xexpression<ARRAY> & outExpression, ITER roiBeginIter) {

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

        types::ShapeType offsetInRequest, shapeInRequest, chunkShape, offsetInChunk;

        // TODO writing directly to a view does not work, probably because it is not continuous in memory (?!)
        // that's why we use the buffer for now.
        // In the end, it would be nice to do this without the buffer (which introduces an additional copy)
        // Benchmark and figure this out !
        types::ShapeType bufferShape;
        // N5-Axis order: we need to reverse the max chunk shape
        if(ds.isZarr()) {
           bufferShape = types::ShapeType(ds.maxChunkShape().begin(), ds.maxChunkShape().end());
        } else {
           bufferShape = types::ShapeType(ds.maxChunkShape().rbegin(), ds.maxChunkShape().rend());
        }
        xt::xarray<T> buffer(bufferShape);

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = ds.getCoordinatesInRequest(chunkId, offset, shape, offsetInRequest, shapeInRequest, offsetInChunk);

            // get the view in our array
            xt::slice_vector offsetSlice(out);
            sliceFromRoi(offsetSlice, offsetInRequest, shapeInRequest);
            auto view = xt::dynamic_view(out, offsetSlice);

            // get the current chunk-shape and resize the buffer if necessary
            ds.getChunkShape(chunkId, chunkShape);

            // N5-Axis order: we need to transpose the view and reverse the
            // chunk shape internally
            if(!ds.isZarr()) {
                view = xt::transpose(view);  // TODO does this work as expected ?
                std::reverse(chunkShape.begin(), chunkShape.end());
            }

            // reshape buffer if necessary
            if(bufferShape != chunkShape) {
                buffer.reshape(chunkShape);
                bufferShape = chunkShape;
            }

            // read the current chunk into the buffer
            ds.readChunk(chunkId, &buffer(0));
            //ds.readChunk(chunkId, bdata);

            // request and chunk completely overlap
            // -> we can read all the data from the chunk
            if(completeOvlp) {
                // FIXME can we do this with xtensor
                // without data copy: not working
                //ds.readChunk(chunkId, &view(0));

                std::cout << "Buffer: " << buffer(0, 0, 0) << " " << buffer(0, 0, 1) << std::endl;
                // copy the data from the buffer into the view
                view = buffer;
                //std::copy(buffer.begin(), buffer.end(), view.begin());
                std::cout << "Read complete ovlp: " << view(0, 0, 0) << " " << view(0, 0, 1) << std::endl;

            }
            // request and chunk overlap only partially
            // -> we can read the chunk data only partially
            else {

                // copy the data from the correct buffer-view to the out view
                xt::slice_vector bufSlice(buffer);
                sliceFromRoi(bufSlice, offsetInChunk, shapeInRequest);
                auto bufView = xt::dynamic_view(buffer, bufSlice);
                // TODO can't use assignment here, but copy compiles, why?
                //view = bufView;
                std::copy(bufView.begin(), bufView.end(), view.begin());
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
        // create marray buffer
        types::ShapeType bufferShape;
        // N5-Axis order: we need to reverse the max chunk shape
        if(ds.isZarr()) {
            bufferShape = types::ShapeType(ds.maxChunkShape().begin(), ds.maxChunkShape().end());
        } else {
            bufferShape = types::ShapeType(ds.maxChunkShape().rbegin(), ds.maxChunkShape().rend());
        }
        xt::xarray<T> buffer(bufferShape);

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = ds.getCoordinatesInRequest(chunkId, offset, shape, offsetInRequest, requestShape, offsetInChunk);
            ds.getChunkShape(chunkId, chunkShape);

            // get the view into the in-array
            xt::slice_vector offsetSlice(in);
            sliceFromRoi(offsetSlice, offsetInRequest, requestShape);
            auto view = xt::dynamic_view(in, offsetSlice);

            // N5-Axis order: we need to reverse the chunk shape internally
            if(!ds.isZarr()) {
                std::reverse(chunkShape.begin(), chunkShape.end());
            }

            // resize buffer if necessary
            if(bufferShape != chunkShape) {
                buffer.reshape(chunkShape);
                bufferShape = chunkShape;
            }

            // request and chunk overlap completely
            // -> we can write the whole chunk
            if(completeOvlp) {

                // for now this does not work without copying,
                // because views are not contiguous in memory (I think ?!)
                // TODO would be nice to figure this out -> benchmark first !
                //ds.writeChunk(chunkId, &view(0));

                buffer = view;
                ds.writeChunk(chunkId, &buffer(0));
            }

            // request and chunk overlap only partially
            // -> we can only write partial data and need
            // to preserve the data that will not be written
            else {

                // load the current data into the buffer
                ds.readChunk(chunkId, &buffer(0));

                // overwrite the data that is covered by the view
                xt::slice_vector bufSlice(buffer);
                sliceFromRoi(bufSlice, offsetInChunk, requestShape);
                auto bufView = xt::dynamic_view(buffer, bufSlice);

                // TODO can't use copy here, but assignment compiles, why?
                //std::copy(view.begin(), view.end(), bufView.begin());
                bufView = view;

                // wrte the chunk
                ds.writeChunk(chunkId, &buffer(0));
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
