#pragma once


#include "z5/dataset.hxx"
#include "z5/util/threadpool.hxx"


namespace z5 {


    template<class F>
    void parallel_for_each_chunk(const Dataset & dataset, const int nThreads, F && f) {
        util::parallel_foreach(nThreads, dataset.numberOfChunks(), [&](const int tid, const size_t chunkId){
            types::ShapeType chunkCoord;
            dataset.chunkIndexToTuple(chunkId, chunkCoord);
            f(tid, dataset, chunkCoord);
        });
    }


    template<class F>
    void parallel_for_each_chunk_in_bb(const Dataset & dataset,
                                       const types::ShapeType & bb_start,
                                       const types::ShapeType & bb_stop,
                                       const int nThreads, F && f) {
        // get the (coordinate) chunk ids that overlap with the request
        std::vector<types::ShapeType> chunks;
        types::ShapeType bb_shape(bb_stop);
        for(unsigned d = 0; d < bb_shape.size(); ++d) {
            bb_shape[d] -= bb_start[d];
        }
        dataset.getChunkRequests(bb_start, bb_shape, chunks);

        // loop over chunks in parallel and call lambda
        const size_t nChunks = chunks.size();
        util::parallel_foreach(nThreads, nChunks, [&](const int tid, const size_t chunkId){
            f(tid, dataset, chunks[chunkId]);
        });
    }


    // TODO
    template<class F>
    void parallel_for_each_block(const Dataset & dataset, const types::ShapeType & blockShape,
                                 const int nThreads, F && f) {
        // get blocking for this block shape

        // loop over blocks in parallel
    }


    // TODO
    template<class F>
    void parallel_for_each_block_in_bb(const Dataset & dataset, const types::ShapeType & blockShape,
                                       const types::ShapeType & bb_start,
                                       const types::ShapeType & bb_stop,
                                       const int nThreads, F && f) {

    }

}
