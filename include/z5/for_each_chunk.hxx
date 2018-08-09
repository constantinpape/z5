#pragma once

#include <memory>

#ifndef BOOST_FILESYSTEM_NO_DEPERECATED
#define BOOST_FILESYSTEM_NO_DEPERECATED
#endif
#include <boost/filesystem.hpp>

#include "z5/dataset.hxx"
#include "z5/util/threadpool.hxx"

namespace fs = boost::filesystem;


namespace z5 {


    // TODO implement chunks in subvolume
    template<class F>
    void parallel_for_each_chunk(const Dataset & dataset, const int nThreads,  F && f) {
        const auto & chunksPerDim = dataset.chunksPerDimension();
        util::parallel_foreach(nThreads, dataset.numberOfChunks(), [&](const int tid, const size_t chunkId){
            types::ShapeType chunkCoord;
            dataset.chunkIndexToTuple(chunkId, chunkCoord);
            f(tid, dataset, chunkCoord);
        });
    }


    // remove chunks which contain only a single value
    // -> this is often some background value, that might`
    // be different from the global background value
    template<class T>
    void remove_trivial_chunks(const Dataset & dataset, const int nThreads,
                               const bool removeSpecificValue=false, const T value=0) {
        parallel_for_each_chunk(dataset, nThreads, [removeSpecificValue,
                                                    value](const int tid,
                                                           const Dataset & ds,
                                                           const types::ShapeType & chunk) {
            const size_t chunkSize = ds.getChunkSize(chunk);
            std::vector<T> data(chunkSize);
            ds.readChunk(chunk, &data[0]);
            // check vector for number of uniques and if we only have a single unique, remove it
            const auto uniques = std::set<T>(data.begin(), data.end());
            const bool remove = (uniques.size() == 1) ? (removeSpecificValue ? (*uniques.begin() == value ? true : false) : true) : false;
            if(remove) {
                const auto handle = handle::Chunk(ds.handle(), chunk, ds.isZarr());
                fs::remove(handle.path());
            }
        });
    }


    // TODO maybe it would be benefitial to have intermediate unordered sets
    template<class T>
    void unique(const Dataset & dataset, const int nThreads, std::set<T> & uniques) {
        // allocate the per thread data
        // TODO need to get actual number of threads here
        std::vector<std::set<T>> threadData(nThreads - 1);

        // iterate over all chunks
        parallel_for_each_chunk(dataset, nThreads, [&threadData, &uniques](const int tid,
                                                                           const Dataset & ds,
                                                                           const types::ShapeType & chunk) {
            const size_t chunkSize = ds.getChunkSize(chunk);
            std::vector<T> data(chunkSize);
            ds.readChunk(chunk, &data[0]);
            auto & threadSet = (tid == 0) ? uniques : threadData[tid];
            threadSet.insert(data.begin(), data.end());
        });

        // merge the per thread data
        // TODO can we parallelize ?
        for(const auto & threadSet : threadData) {
            uniques.insert(threadSet.begin(), threadSet.end());
        }
    }


    // TODO maybe it would be benefitial to have intermediate unordered maps
    template<class T>
    void uniqueWithCounts(const Dataset & dataset, const int nThreads, std::map<T, size_t> & uniques) {
        // allocate the per thread data
        // TODO need to get actual number of threads here
        std::vector<std::map<T, size_t>> threadData(nThreads - 1);

        // iterate over all chunks
        parallel_for_each_chunk(dataset, nThreads, [&threadData, &uniques](const int tid,
                                                                           const Dataset & ds,
                                                                           const types::ShapeType & chunk) {
            const size_t chunkSize = ds.getChunkSize(chunk);
            std::vector<T> data(chunkSize);
            ds.readChunk(chunk, &data[0]);
            auto & threadMap = (tid == 0) ? uniques : threadData[tid];
            for(const T val : data) {
                auto mIt = threadMap.find(val);
                if(mIt == threadMap.end()) {
                    threadMap.emplace(std::make_pair(val, 1));
                } else {
                    ++mIt->second;
                }
            }
        });

        // merge the per thread data
        // TODO can we parallelize ?
        for(const auto & threadMap : threadData) {
            for(auto tIt = threadMap.begin(); tIt != threadMap.end(); ++tIt) {
                auto uIt = uniques.find(tIt->first);
                if(uIt == uniques.end()) {
                    uniques.emplace(std::make_pair(tIt->first, tIt->second));
                } else {
                    uIt->second += tIt->second;
                }
            }
        }
    }

}
