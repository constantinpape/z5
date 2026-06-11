#pragma once

#include "z5/dataset.hxx"
#include "z5/multiarray/array_view.hxx"
#include "z5/multiarray/array_util.hxx"
#include "z5/multiarray/array_access.hxx"


namespace z5 {
namespace multiarray {

    // Broadcast a scalar to a region of the dataset. Implemented on the shared
    // write drivers (writePlainGeneric / writeShardedGeneric), which handle the
    // zarr edge-chunk padding (chunks are stored at the full chunk shape, so the
    // buffer must never be sized by the clipped shape), the partial-overlap
    // read-modify-write, single-/multi-threaded execution with one reusable
    // buffer per thread, and one read-modify-write per shard for sharded data.
    template<typename T, typename ITER>
    void writeScalar(const Dataset & ds,
                     ITER roiBeginIter,
                     ITER roiShapeIter,
                     const T val,
                     const int numberOfThreads=1) {

        // get the offset and shape of the request and check if it is valid
        types::ShapeType offset(roiBeginIter, roiBeginIter+ds.dimension());
        types::ShapeType shape(roiShapeIter, roiShapeIter+ds.dimension());
        ds.checkRequestShape(offset, shape);
        ds.checkRequestType(typeid(T));

        // get the chunks that are involved in this request
        std::vector<types::ShapeType> chunkRequests;
        const auto & chunking = ds.chunking();
        chunking.getBlocksOverlappingRoi(offset, shape, chunkRequests);

        // fill the request region of each chunk buffer with the scalar
        const auto fillRequest = [val](const ArrayView<T> & dest,
                                       const types::ShapeType &,
                                       const types::ShapeType &) {
            fillView(dest, val);
        };

        if(ds.isSharded()) {
            writeShardedGeneric<T>(ds, offset, shape, chunkRequests,
                                   numberOfThreads, fillRequest);
        } else {
            writePlainGeneric<T>(ds, offset, shape, chunkRequests,
                                 numberOfThreads, fillRequest);
        }
    }


    // unique ptr API
    template<typename T, typename ITER>
    inline void writeScalar(std::unique_ptr<Dataset> & ds,
                            ITER roiBeginIter,
                            ITER roiShapeIter,
                            const T val,
                            const int numberOfThreads=1) {
       writeScalar(*ds, roiBeginIter, roiShapeIter, val, numberOfThreads);
    }
}
}
