#pragma once

#include "z5/dataset.hxx"
#include "z5/types/types.hxx"
#include "xtensor-python/pyarray.hpp"

// free functions to read and write from xtensor multiarrays
// (it's hard to have these as members due to dynamic type inference)

namespace z5 {
namespace multiarray {


    //
    template<typename T, typename ITER>
    void readSubarray(const Dataset & ds, xt::pyarray<T> & out, ITER roiBeginIter) {

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter+out.dimension());
        types::ShapeType shape(out.shape());
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
        xt::zeros<T> buffer(bufferShape);

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = ds.getCoordinatesInRequest(chunkId, offset, shape, offsetInRequest, shapeInRequest, offsetInChunk);

            // get the view in our array
            xt::slice_vector offsetSlice(out);
            for(int d = 0; d < out.dimension(); ++d) {
                offsetSlice.push_back(xt::range(offsetInRequest[d], shapeInRequest[d]));
            }
            auto view = xt::dynamic_view(out, offsetSlice);

            // get the current chunk-shape and resize the buffer if necessary
            ds.getChunkShape(chunkId, chunkShape);

            // N5-Axis order: we need to transpose the view and reverse the
            // chunk shape internally
            if(!ds.isZarr()) {
                view.transpose();
                std::reverse(chunkShape.begin(), chunkShape.end());
            }

            // reshape buffer if necessary
            if(bufferShape != chunkShape) {
                xt::slice_vector offsetSlice(buffer);
                for(int d = 0; d < out.dimension(); ++d) {
                    offsetSlice.push_back(xt::range(chunkShape[d], chunkShape[d]));
                }
                buffer.reshape(offsetSlice);
                bufferShape = chunkShape;
            }

            // read the current chunk into the buffer
            ds.readChunk(chunkId, &buffer(0));

            // request and chunk completely overlap
            // -> we can read all the data from the chunk
            if(completeOvlp) {
                // FIXME can we do this with xtensor
                // without data copy: not working
                //ds.readChunk(chunkId, &view(0));

                // copy the data from the buffer into the view
                view = buffer;

            }
            // request and chunk overlap only partially
            // -> we can read the chunk data only partially
            else {

                // how do we do this in xt ???
                // copy the data from the correct buffer-view to the out view
                view = buffer.view(offsetInChunk.begin(), shapeInRequest.begin());
            }
        }
    }


    template<typename T, typename ITER>
    void writeSubarray(const Dataset & ds, const andres::View<T> & in, ITER roiBeginIter) {

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter+in.dimension());
        types::ShapeType shape(in.shapeBegin(), in.shapeEnd());

        ds.checkRequestShape(offset, shape);
        ds.checkRequestType(typeid(T));

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        ds.getChunkRequests(offset, shape, chunkRequests);

        types::ShapeType localOffset, localShape, chunkShape;
        types::ShapeType inChunkOffset;

        // TODO try to figure out writing without memcopys for fully overlapping chunks
        // create marray buffer
        types::ShapeType bufferShape;
        // N5-Axis order: we need to reverse the max chunk shape
        if(ds.isZarr()) {
            bufferShape = types::ShapeType(ds.maxChunkShape().begin(), ds.maxChunkShape().end());
        } else {
            bufferShape = types::ShapeType(ds.maxChunkShape().rbegin(), ds.maxChunkShape().rend());
        }
        andres::Marray<T> buffer(andres::SkipInitialization, bufferShape.begin(), bufferShape.end());

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = ds.getCoordinatesInRequest(chunkId, offset, shape, localOffset, localShape, inChunkOffset);
            ds.getChunkShape(chunkId, chunkShape);

            auto view = in.constView(localOffset.begin(), localShape.begin());
            // N5-Axis order: we need to reverse the chunk shape internally
            if(!ds.isZarr()) {
                std::reverse(chunkShape.begin(), chunkShape.end());
            }

            // resize buffer if necessary
            if(bufferShape != chunkShape) {
                buffer.resize(andres::SkipInitialization, chunkShape.begin(), chunkShape.end());
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
                auto bufView = buffer.view(inChunkOffset.begin(), localShape.begin());
                bufView = view;
                ds.writeChunk(chunkId, &buffer(0));
            }
        }
    }


    // unique ptr API
    template<typename T, typename ITER>
    inline void readSubarray(std::unique_ptr<Dataset> & ds, andres::View<T> & out, ITER roiBeginIter) {
       readSubarray(*ds, out, roiBeginIter);
    }

    template<typename T, typename ITER>
    inline void writeSubarray(std::unique_ptr<Dataset> & ds, const andres::View<T> & in, ITER roiBeginIter) {
        writeSubarray(*ds, in, roiBeginIter);
    }

}
}
