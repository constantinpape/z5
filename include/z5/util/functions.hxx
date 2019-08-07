#pragma once

#ifdef WITH_BOOST_FS
    #ifndef BOOST_FILESYSTEM_NO_DEPERECATED
        #define BOOST_FILESYSTEM_NO_DEPERECATED
    #endif
    #include <boost/filesystem.hpp>
    namespace fs = boost::filesystem;
#else
    #if __GCC__ > 7
        #include <filesystem>
        namespace fs = std::filesystem;
    #else
        #include <experimental/filesystem>
        namespace fs = std::experimental::filesystem;
    #endif
#endif

#include "z5/util/for_each.hxx"


namespace z5 {
namespace util {


    // remove chunks which contain only a single value
    // -> this is often some background value, that might`
    // be different from the global background value
    template<class T>
    void removeTrivialChunks(const Dataset & dataset, const int nThreads,
                             const bool removeSpecificValue=false, const T value=0) {

        // check if we are allowed to delete
        if(!dataset.handle().mode().canWrite()) {
            const std::string err = "Cannot delete chunks in a dataset that was not opened with write permissions.";
            throw std::invalid_argument(err.c_str());
        }

        // delete trivial chunks in parallel
        parallel_for_each_chunk(dataset, nThreads, [removeSpecificValue,
                                                    value](const int tid,
                                                           const Dataset & ds,
                                                           const types::ShapeType & chunk) {
            if(!ds.chunkExists(chunk)) {
                return;
            }
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


    inline void removeChunk(const Dataset & ds, const types::ShapeType & chunkId) {
        if(ds.chunkExists(chunkId)) {
            const auto handle = handle::Chunk(ds.handle(), chunkId, ds.isZarr());
            fs::remove(handle.path());
        }
    }


    // remove dataset multithreaded
    inline void removeDataset(const Dataset & dataset, const int nThreads) {

        // check if we are allowed to delete
        if(!dataset.handle().mode().canWrite()) {
            const std::string err = "Cannot delete dataset that was not opened with write permissions.";
            throw std::invalid_argument(err.c_str());
        }

        // delete chunks in parallel
        parallel_for_each_chunk(dataset, nThreads, [](const int tid,
                                                      const Dataset & ds,
                                                      const types::ShapeType & chunk) {
            if(!ds.chunkExists(chunk)) {
                return;
            }
            const auto handle = handle::Chunk(ds.handle(), chunk, ds.isZarr());
            // const auto & path = handle.path();
            fs::remove(handle.path());
            // TODO instead of removing directories in the end, we could check
            // if the chunks directory is empty here adn then delete it
            // howeve, this would require some locking, as we don't want to deletes
            // of a folder at the same time
        });

        // delete all empty directories
        fs::remove_all(dataset.handle().path());
    }


    // TODO add option to ignore fill value
    // TODO maybe it would be benefitial to have intermediate unordered sets
    template<class T>
    void unique(const Dataset & dataset, const int nThreads, std::set<T> & uniques) {

        // allocate the per thread data
        // need to get actual number of threads here
        ParallelOptions pOpts(nThreads);
        const int nActualThreads = pOpts.getActualNumThreads();
        std::vector<std::set<T>> threadData(nActualThreads - 1);

        // iterate over all chunks
        parallel_for_each_chunk(dataset, nThreads, [&threadData, &uniques](const int tid,
                                                                           const Dataset & ds,
                                                                           const types::ShapeType & chunk) {
            // skip empty chunks, don't count them in unique values
            if(!ds.chunkExists(chunk)) {
                return;
            }
            const size_t chunkSize = ds.getChunkSize(chunk);
            std::vector<T> data(chunkSize);
            ds.readChunk(chunk, &data[0]);
            auto & threadSet = (tid == 0) ? uniques : threadData[tid - 1];
            threadSet.insert(data.begin(), data.end());
        });

        // merge the per thread data
        // TODO can we parallelize ?
        for(const auto & threadSet : threadData) {
            uniques.insert(threadSet.begin(), threadSet.end());
        }
    }


    // TODO add option to ignore fill value
    // TODO maybe it would be benefitial to have intermediate unordered maps
    template<class T>
    void uniqueWithCounts(const Dataset & dataset, const int nThreads, std::map<T, size_t> & uniques) {
        // allocate the per thread data
        ParallelOptions pOpts(nThreads);
        const int nActualThreads = pOpts.getActualNumThreads();
        std::vector<std::map<T, size_t>> threadData(nActualThreads - 1);

        // iterate over all chunks
        parallel_for_each_chunk(dataset, nThreads, [&threadData, &uniques](const int tid,
                                                                           const Dataset & ds,
                                                                           const types::ShapeType & chunk) {
            // skip empty chunks, don't count them in unique values
            if(!ds.chunkExists(chunk)) {
                return;
            }
            const size_t chunkSize = ds.getChunkSize(chunk);
            std::vector<T> data(chunkSize);
            ds.readChunk(chunk, &data[0]);
            auto & threadMap = (tid == 0) ? uniques : threadData[tid - 1];
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
}
