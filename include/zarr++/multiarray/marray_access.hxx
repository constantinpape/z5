#pragma once

#include "zarr++/array.hxx"
#include "andres/marray.hxx"

// free functions to read and write from multiarrays
// (it's hard to have these as members due to dynamic type inference)

namespace zarr {
namespace multiarray {

    //
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

        types::ShapeType localOffset, localShape, chunkShape;
        types::ShapeType inChunkOffset;

        // create marray to have a buffer for non-overlapping overlaps
        //andres::Marray<T> buffer;

        // TODO writing directly to a view does not work, probably because it is not continuous in memory (?!)
        // that's why we use the buffer for now.
        // In the end, it would be nice to do this without the buffer (which introduces an additional copy)
        // Benchmark and figure this out !
        andres::Marray<T> buffer(array.maxChunkShape().begin(), array.maxChunkShape().end());
        types::ShapeType bufferShape(buffer.shapeBegin(), buffer.shapeEnd());

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = array.getCoordinatesInRequest(chunkId, offset, shape, localOffset, localShape, inChunkOffset);
            auto view = out.view(localOffset.begin(), localShape.begin());

            // get the current chunk-shape and resize the buffer if necessary
            array.getChunkShape(chunkId, chunkShape);
            if(bufferShape != chunkShape) {
                // TODO skip initialisation
                buffer.resize(chunkShape.begin(), chunkShape.end());
                bufferShape = chunkShape;
            }

            // read the current chunk into the buffer
            array.readChunk(chunkId, &buffer(0));

            // request and chunk completely overlap
            // -> we can read all the data from the chunk
            if(completeOvlp) {
                // without data copy: not working
                //array.readChunk(chunkId, &view(0));

                // copy the data from the buffer into the view
                view = buffer;
            }

            // request and chunk overlap only partially
            // -> we can read the chunk data only partially
            else {
                // copy the data from the correct buffer-view to the out view
                view = buffer.view(inChunkOffset.begin(), localShape.begin());
            }
        }
    }


    template<typename T, typename ITER>
    void writeSubarray(const ZarrArray & array, const andres::View<T> & in, ITER roiBeginIter) {

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter+in.dimension());
        types::ShapeType shape(in.shapeBegin(), in.shapeEnd());
        array.checkRequestShape(offset, shape);
        array.checkRequestType(typeid(T));

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        array.getChunkRequests(offset, shape, chunkRequests);

        types::ShapeType localOffset, localShape, chunkShape;
        types::ShapeType inChunkOffset;
        // create marray to have a buffer for non-overlapping overlaps
        andres::Marray<T> buffer(array.maxChunkShape().begin(), array.maxChunkShape().end());
        types::ShapeType bufferShape(buffer.shapeBegin(), buffer.shapeEnd());

        // iterate over the chunks
        for(const auto & chunkId : chunkRequests) {

            bool completeOvlp = array.getCoordinatesInRequest(chunkId, offset, shape, localOffset, localShape, inChunkOffset);
            auto view = in.constView(localOffset.begin(), localShape.begin());

            // resize buffer if necessary
            array.getChunkShape(chunkId, chunkShape);
            if(bufferShape != chunkShape) {
                // TODO skip initialisation
                buffer.resize(chunkShape.begin(), chunkShape.end());
                bufferShape = chunkShape;
            }

            // request and chunk overlap completely
            // -> we can write the whole chunk
            if(completeOvlp) {
                // for now this does not work without copying,
                // because views are not contiguous in memory (I think ?!)
                // TODO would be nice to figure this out -> benchmark first !
                //array.writeChunk(chunkId, &view(0));
                buffer = view;
                array.writeChunk(chunkId, &buffer(0));
            }

            // request and chunk overlap only partially
            // -> we can only write partial data and need
            // to preserve the data that will not be written
            else {
                // load the current data into the buffer
                array.readChunk(chunkId, &buffer(0));
                // overwrite the data that is covered by the view
                auto bufView = buffer.view(inChunkOffset.begin(), localShape.begin());
                bufView = view;
                array.writeChunk(chunkId, &buffer(0));
            }
        }
    }


    // unique ptr API
    template<typename T, typename ITER>
    void readSubarray(std::unique_ptr<ZarrArray> & array, andres::View<T> & out, ITER roiBeginIter) {
       readSubarray(*array, out, roiBeginIter);
    }

    template<typename T, typename ITER>
    void writeSubarray(std::unique_ptr<ZarrArray> & array, const andres::View<T> & in, ITER roiBeginIter) {
        writeSubarray(*array, in, roiBeginIter);
    }

}
}
