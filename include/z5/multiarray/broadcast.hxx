#pragma once

#include "z5/dataset.hxx"
#include "z5/multiarray/xtensor_access.hxx"
#include "xtensor/xeval.hpp"


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

        types::ShapeType offsetInRequest, requestShape, chunkShape, offsetInChunk;
        // out buffer holding data for a single chunk
        size_t chunkSize = ds.maxChunkSize();
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
            // TODO accumulate directly, w/o further read from data
            chunkSize = std::accumulate(chunkShape.begin(), chunkShape.end(), 1, std::multiplies<size_t>());

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
                auto fullBuffView = xt::xadapt(buffer, chunkShape);
                xt::slice_vector bufSlice(fullBuffView);
                sliceFromRoi(bufSlice, offsetInChunk, requestShape);
                auto bufView = xt::dynamic_view(fullBuffView, bufSlice);
                bufView = val;
                ds.writeChunk(chunkId, &buffer[0]);
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
