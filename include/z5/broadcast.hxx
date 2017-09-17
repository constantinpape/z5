#pragma once
#include "z5/dataset.hxx"
#include "andres/marray.hxx"

namespace z5 {

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
        andres::Marray<T> buffer(ds.maxChunkShape().begin(), ds.maxChunkShape().end(), val);
        types::ShapeType bufferShape(buffer.shapeBegin(), buffer.shapeEnd());

        // iterate over the chunks and write the buffer
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = ds.getCoordinatesInRequest(
                chunkId, offset, shape, localOffset, localShape, inChunkOffset
            );

            // resize buffer if necessary
            ds.getChunkShape(chunkId, chunkShape);
            if(bufferShape != chunkShape) {
                buffer.resize(chunkShape.begin(), chunkShape.end(), val);
                bufferShape = chunkShape;
            }

            // request and chunk overlap completely
            // -> we can write the whole chunk
            if(completeOvlp) {
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
                bufView = val;
                ds.writeChunk(chunkId, &buffer(0));
            }
        }
    }

}
