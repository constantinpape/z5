#pragma once

#include "zarr++/array.hxx"
#include "andres/marray.hxx"

// free functions to read and write from multiarrays
// (it's hard to have these as members due to dynamic type inference)

namespace zarr {

    // TODO unique ptr API

    template<typename T, typename ITER>
    void readSubarray(const ZarrArray & array, andres::View<T> & out, ITER roiBeginIter) {

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter+out.dimension());
        types::ShapeType shape(out.shapeBegin(), out.shapeEnd());
        array.checkRequestShape(offset, shape);
        array.checkRequestType(typeid(T));

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        array.getChunkRequests(offset, shape, chunkRequests);

        // iterate over the chunks
        types::ShapeType localOffset, localShape, chunkShape;
        types::ShapeType inChunkOffset;
        // create marray to have a buffer for non-overlapping overlaps
        andres::Marray<T> buffer;
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = array.getCoordinatesInRequest(chunkId, offset, shape, localOffset, localShape, inChunkOffset);
            auto view = out.view(localOffset.begin(), localShape.begin());

            // request and chunk completely overlap
            // -> we can read all the data from the chunk
            if(completeOvlp) {
                // TODO FIXME I don't think this works without copying, because
                // the view memory is not continuous in memory
                // using the buffer similar to non overlapping chunk should fix this
                array.readChunk(chunkId, &view[0]);
            }

            // request and chunk overlap only partially
            // -> we can read the chunk data only partially
            else {
                // TODO skip initialisation
                array.getChunkShape(chunkId);
                buffer.resize(chunkShape.begin(), chunkShape.end());
                array.readChunk(chunkId, &buffer[0]);
                view = buffer.view(inChunkOffset.begin(), localShape.begin());
            }
        }
    }


    template<typename VIEW, typename ITER>
    void writeSubarray(const ZarrArray & array, const VIEW & in, ITER roiBeginIter) {

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter+out.dimension());
        types::ShapeType shape(out.shapeBegin(), out.shapeEnd());
        array.checkRequest(offset, shape);
        array.checkRequestType(typeid(T));

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        array.getChunkRequests(offset, shape, chunkRequests);

        // iterate over the chunks
        types::ShapeType localOffset, localShape, inChunkOffset;
        // create marray to have a buffer for non-overlapping overlaps
        andres::Marray<T> buffer;
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = array.getCoordinatesInRequest(chunkId, offset, shape, localOffset, localShape, inChunkOffset);
            auto view = out.constView(localOffset.begin(), localShape.begin());

            // request and chunk overlap completely
            // -> we can write the whole chunk
            if(completeOvlp) {

                // TODO FIXME I don't think this works without copying, because
                // the view memory is not continuous in memory
                // using the buffer similar to non overlapping chunk should fix this
                array.writeChunk(chunkId, &view[0]);
            }

            // request and chunk overlap only partially
            // -> we can only write partial data and need
            // to preserve the data that will not be written
            else {
                // TODO skip initialisation
                array.getChunkShape(chunkId);
                buffer.resize(chunkShape.begin(), chunkShape.end());
                array.readChunk(chunkId, &buffer[0]);
                auto bufView = buffer.view(inChunkOffset.begin(), localShape.begin());
                bufView = view;
                array.writeChunk(chunkId, &view[0]);
            }
        }

    }


}
