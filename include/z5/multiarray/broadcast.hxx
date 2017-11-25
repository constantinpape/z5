#pragma once
#include "z5/dataset.hxx"
#include "z5/multiarray/xtensor_access.hxx"

namespace z5 {
namespace multiarray {

    template<typename T, typename ITER>
    void writeScalar(const Dataset & ds, ITER roiBeginIter, ITER roiShapeIter, const T val) {

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter+ds.dimension());
        types::ShapeType shape(roiShapeIter, roiShapeIter+ds.dimension());
        ds.checkRequestShape(offset, shape);
        ds.checkRequestType(typeid(T));

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        ds.getChunkRequests(offset, shape, chunkRequests);

        types::ShapeType localOffset, localShape, chunkShape;
        types::ShapeType inChunkOffset;
        // out buffer holding data for a single chunk
        types::ShapeType bufferShape;
        // N5-Axis order: we need to reverse the max chunk shape
        if(ds.isZarr()) {
           bufferShape = types::ShapeType(ds.maxChunkShape().begin(), ds.maxChunkShape().end());
        } else {
           bufferShape = types::ShapeType(ds.maxChunkShape().rbegin(), ds.maxChunkShape().rend());
        }
        xt::xarray<T> buffer(bufferShape, val);

        // iterate over the chunks and write the buffer
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = ds.getCoordinatesInRequest(
                chunkId, offset, shape, localOffset, localShape, inChunkOffset
            );

            // reshape buffer if necessary
            if(bufferShape != chunkShape) {
                buffer.reshape(chunkShape);
                bufferShape = chunkShape;
                buffer = val;
            }

            // request and chunk overlap completely
            // -> we can write the whole chunk
            if(completeOvlp) {
                std::cout << "Writing full buffer with val " << buffer(0, 0, 0) << " " << buffer(0, 0, 1) << std::endl;
                ds.writeChunk(chunkId, &buffer(0));
            }

            // request and chunk overlap only partially
            // -> we can only write partial data and need
            // to preserve the data that will not be written
            else {
                std::cout << "Here I am !" << std::endl;
                // load the current data into the buffer
                ds.readChunk(chunkId, &buffer(0));
                // overwrite the data that is covered by the view
                xt::slice_vector bufSlice(buffer);
                sliceFromRoi(bufSlice, inChunkOffset, localShape);
                auto bufView = xt::dynamic_view(buffer, bufSlice);
                bufView = val;
                ds.writeChunk(chunkId, buffer.raw_data());
            }
        }
    }


    // unique ptr API
    template<typename T, typename ITER>
    inline void writeScalar(std::unique_ptr<Dataset> & ds, ITER roiBeginIter, ITER roiShapeIter, const T val) {
       writeScalar(*ds, roiBeginIter, roiShapeIter, val);
    }
}
}
