#pragma once

#include <ios>

#ifndef BOOST_FILESYSTEM_NO_DEPERECATED
#define BOOST_FILESYSTEM_NO_DEPERECATED
#endif
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "z5/io/io_base.hxx"
#include "z5/types/types.hxx"
#include "z5/util/util.hxx"

namespace fs = boost::filesystem;

namespace z5 {
namespace io {

    // TODO
    // endianess mess
    // the best way would be to handle this
    // in a streambuffer for boost::iostreams
    //

    class ChunkIoN5 : public ChunkIoBase {

    public:

        ChunkIoN5(const types::ShapeType & shape, const types::ShapeType & chunkShape) :
            shape_(shape), chunkShape_(chunkShape){
        }

        inline bool read(const handle::Chunk & chunk, std::vector<char> & data) const {

            // if the chunk exists, we read it
            if(chunk.exists()) {

                // this might speed up the I/O by decoupling C++ buffers from C buffers
                std::ios_base::sync_with_stdio(false);
                // open input stream and read the header
                fs::ifstream file(chunk.path(), std::ios::binary);
                types::ShapeType chunkShape;
                const std::size_t fileSize = readHeader(file, chunkShape);

                // resize the data vector
                data.resize(fileSize);

                // read the file
                file.read(&data[0], fileSize);
                file.close();

                // return true, because we have read an existing chunk
                return true;

            } else {
                return false;
            }

        }


        inline void write(const handle::Chunk & chunk, const char * data, const std::size_t fileSize) const {
            // create the parent folder
            chunk.createTopDir();
            // this might speed up the I/O by decoupling C++ buffers from C buffers
            std::ios_base::sync_with_stdio(false);
            fs::ofstream file(chunk.path(), std::ios::binary);
            // write the header
            writeHeader(chunk, file);
            file.write(data, fileSize);
            file.close();
        }


        inline void getChunkShape(const handle::Chunk & chunk, types::ShapeType & shape) const {
            if(chunk.exists()) {
                std::ios_base::sync_with_stdio(false);
                fs::ifstream file(chunk.path(), std::ios::binary);
                readHeader(file, shape);
                file.close();

            }
            else {
                chunk.boundedChunkShape(shape_, chunkShape_, shape);
            }
        }


        inline std::size_t getChunkSize(const handle::Chunk & chunk) const {
            types::ShapeType shape;
            getChunkShape(chunk, shape);
            return std::accumulate(shape.begin(), shape.end(), 1, std::multiplies<std::size_t>());
        }


        // TODO do we need to invert dim due to N5 axis conventions ?
        inline void findMinimumChunk(const unsigned dim, const fs::path & dsDir, const std::size_t nChunksTotal, types::ShapeType & minOut) const {
            minOut.clear();
            fs::path chunkDir(dsDir);

            // if our dimension is bigger than zero, we need to first find the chunk
            // with lowest entry in the corresponding dimension
            if(dim > 0) {
                // get lists of chunks at query dimension
                std::vector<types::ShapeType> chunkList;
                listChunks(dim, dsDir, chunkList);

                // find the lowest value in our dimension, while keeping the order of
                // higher dimensions for chunks with same value
                auto compareDim = [dim](types::ShapeType a, types::ShapeType b) {
                    return std::lexicographical_compare(a.rbegin(), a.rend(),
                                                        b.rbegin(), b.rend());
                };
                auto minChunkId = *(std::min_element(chunkList.begin(), chunkList.end(), compareDim));

                // start the out chunk with the min ID we found
                minOut.insert(minOut.end(), minChunkId.begin(), minChunkId.end());

                // update the chunk dir
                for(auto cc : minChunkId) {
                    chunkDir /= std::to_string(cc);
                }

                // check if we are already at the lowest chunk level
                // and if we are, return
                if(fs::is_regular_file(chunkDir)) {
                    // need to reverse due to n5 axis ordering
                    std::reverse(minOut.begin(), minOut.end());
                    return;
                }
            }

            // we need to wrap std::min in a lambda and a std::function to pass it to `iterateChunks`
            std::function<std::size_t (std::size_t, std::size_t)> minComp = [](std::size_t a, std::size_t b) {
                return std::min(a, b);
            };

            // starting from our current chunk position, find the downstream chunk with
            // lowest indices
            while(true) {
                // we need to pass something that is definetely bigger than any chunk id
                // as initial value here
                std::size_t chunkId = iterateChunks(chunkDir, nChunksTotal, minComp);
                minOut.push_back(chunkId);
                chunkDir /= std::to_string(chunkId);
                // we need to check if the next chunkDir is still a directory
                // or already a chunk file (and break)
                if(fs::is_regular_file(chunkDir)) {
                    break;
                }
            }
            // otherwise, we first need to list all chunks at the level corresponding to
            // the query dimension, find the lowest entry and then open up the sub directories
            // of this chunk

            // need to reverse due to n5 axis ordering
            std::reverse(minOut.begin(), minOut.end());
        }


        inline void findMaximumChunk(const unsigned dim, const fs::path & dsDir, types::ShapeType & maxOut) const {
            maxOut.clear();
            fs::path chunkDir(dsDir);

            // if our dimension is bigger than zero, we need to first find the chunk
            // with highest entry in the corresponding dimension
            if(dim > 0) {
                // get lists of chunks at query dimension
                std::vector<types::ShapeType> chunkList;
                listChunks(dim, dsDir, chunkList);

                // find the lowest value in our dimension, sort lexicographical for rest
                auto compareDim = [dim](types::ShapeType a, types::ShapeType b) {
                    return std::lexicographical_compare(a.rbegin(), a.rend(),
                                                        b.rbegin(), b.rend());
                };
                auto maxChunkId = *(std::max_element(chunkList.begin(), chunkList.end(), compareDim));

                // start the out chunk with the max ID we found
                maxOut.insert(maxOut.end(), maxChunkId.begin(), maxChunkId.end());

                // update the chunk dir
                for(auto cc : maxChunkId) {
                    chunkDir /= std::to_string(cc);
                }

                // check if we are already at the lowest chunk level
                // and if we are, return
                if(fs::is_regular_file(chunkDir)) {
                    // need to reverse due to n5 axis ordering
                    std::reverse(maxOut.begin(), maxOut.end());
                    return;
                }
            }

            // we need to wrap std::max in a lambda and a std::function to pass it to `iterateChunks`
            std::function<std::size_t (std::size_t, std::size_t)> maxComp = [](std::size_t a, std::size_t b) {
                return std::max(a, b);
            };

            // starting from our current chunk position, find the downstream chunk with
            // lowest indices
            while(true) {
                // we need to pass something that is definetely bigger than any chunk id
                // as initial value here
                std::size_t chunkId = iterateChunks(chunkDir, 0, maxComp);
                maxOut.push_back(chunkId);
                chunkDir /= std::to_string(chunkId);
                // we need to check if the next chunkDir is still a directory
                // or already a chunk file (and break)
                if(fs::is_regular_file(chunkDir)) {
                    break;
                }
            }
            // otherwise, we first need to list all chunks at the level corresponding to
            // the query dimension, find the lowest entry and then open up the sub directories
            // of this chunk

            // need to reverse due to n5 axis ordering
            std::reverse(maxOut.begin(), maxOut.end());
        }

        inline std::size_t writeHeader(const types::ShapeType & shape, std::vector<char> & data) const {

            // write the mode
            uint16_t mode = 0; // TODO support the varlength mode as well
            util::reverseEndiannessInplace(mode);
            std::size_t offset = 0;
            data.insert(data.begin() + offset, (char*) &mode, (char*) &mode + 2);
            offset += 2;

            // write the number of dimensions
            uint16_t nDimsOut = shape.size();
            util::reverseEndiannessInplace(nDimsOut);
            data.insert(data.begin() + offset, (char*) &nDimsOut, (char*) &nDimsOut + 2);
            offset += 2;

            // get the bounded chunk shape and write it to file
            std::vector<uint32_t> shapeOut(shape.begin(), shape.end());
            util::reverseEndiannessInplace<uint32_t>(shapeOut.begin(), shapeOut.end());

            // N5-Axis order: we need to reverse the chunk shape written to the header
            std::reverse(shapeOut.begin(), shapeOut.end());
            // write chunk shape to header
            for(int d = 0; d < shape.size(); ++d) {
                data.insert(data.begin() + offset, (char*) &shapeOut[d], (char*) &shapeOut[d] + 4);
                offset += 4;
            }
            return offset;

            // TODO need to write the actual size if we allow for varlength mode
        }



    private:

        inline void listChunks(const unsigned dim, const fs::path & chunkDir, std::vector<types::ShapeType> & chunkList) const {
            fs::recursive_directory_iterator dir(chunkDir), end;
            types::ShapeType chunkId(dim + 1);
            unsigned currentLevel;
            // recursive directory iterator iterates depth first
            // but thanks to level, we don't need to rely on this
            while(dir != end) {

                // if we are at the required depth, we don't open the directory further
                // and we push back the chunk id
                currentLevel = dir.level();

                if(currentLevel >= dim) {

                    // with this, we do not open the subfolders in iteration
                    dir.no_push();
                    int ii = 0;
                    // starting from the current chunk path, we walk up the filepath,
                    // until we hit the n5 root directory
                    fs::path chunkPath(*dir);
                    bool validChunk = true;

                    while(true) {
                        // we try to compare to this chunk index, however the file might not be
                        // a chunk folder / file, in that case stoull will fail,
                        // in this case we will invalidate the chunk
                        try {
                            chunkId[ii] = std::stoull(chunkPath.leaf().filename().string());
                        } catch(std::invalid_argument) {
                            validChunk = false;
                            break;
                        }
                        if(currentLevel <= 0) {
                            break;
                        }
                        ++ii;
                        --currentLevel;
                        chunkPath = chunkPath.parent_path();
                    }

                    if(validChunk) {
                        std::reverse(chunkId.begin(), chunkId.end());
                        // push back this chunk folder to the chunk list
                        chunkList.emplace_back(chunkId);
                    }
                }
                ++dir;
            }
        }

        // go through all chunks in this directory and return the chunk that is optimal w.r.t compare (max or min)
        inline std::size_t iterateChunks(const fs::path & chunkDir, const std::size_t init, std::function<std::size_t (std::size_t, std::size_t)> compare) const {
            fs::directory_iterator it(chunkDir);
            std::size_t ret = init;
            for(; it != fs::directory_iterator(); ++it) {
                // we try to compare to this chunk index, however the file might not be
                // a chunk folder / file, in that case stoull will fail, and we just continue
                try {
                    ret = compare(ret, std::stoull(it->path().filename().string()));
                } catch(std::invalid_argument) {
                    continue;
                }
            }
            return ret;
        }

        // TODO allow for reading the mode
        inline std::size_t readHeader(fs::ifstream & file, types::ShapeType & shape) const {

            /// keep track of the header length
            std::size_t headerLen = 0;

            // read the mode
            uint16_t mode;
            file.read((char *) &mode, 2);
            util::reverseEndiannessInplace(mode);
            headerLen += 2;

            // TODO support varlength mode
            if(mode != 0) {
                throw std::runtime_error("Z5 only supports reading N5 chunks in default mode");
            }

            // read the number of dimensions
            uint16_t nDims;
            file.read((char *) &nDims, 2);
            util::reverseEndiannessInplace(nDims);
            headerLen += 2;

            // read tempory shape with uint32 entries
            std::vector<uint32_t> shapeTmp(nDims);
            for(int d = 0; d < nDims; ++d) {
                file.read((char *) &shapeTmp[d], 4);
            }
            util::reverseEndiannessInplace<uint32_t>(shapeTmp.begin(), shapeTmp.end());
            headerLen += 4 * nDims;

            // N5-Axis order: we need to reverse the chunk shape read from the header
            std::reverse(shapeTmp.begin(), shapeTmp.end());

            // copy the tempory shape to out shape
            shape.resize(nDims);
            std::copy(shapeTmp.begin(), shapeTmp.end(), shape.begin());

            // TODO support varlen
            // if(mode != 0) {
            //     headerLen += ;
            // }

            // get the file length in byte (need to substract header len)
            file.seekg(0, std::ios::end);
            const std::size_t fileSize = static_cast<std::size_t>(file.tellg()) - headerLen;

            // move file back to the end of the header
            file.seekg(headerLen);

            return fileSize;
        }

        inline void writeHeader(const handle::Chunk & chunk, fs::ofstream & file) const {

            // write the mode
            uint16_t mode = 0; // TODO support the varlength mode as well
            util::reverseEndiannessInplace(mode);
            file.write((char*) &mode, 2);

            // write the number of dimensions
            uint16_t nDimsOut = shape_.size();
            util::reverseEndiannessInplace(nDimsOut);
            file.write((char *) &nDimsOut, 2);

            // TODO need to invert the dimensions here
            // get the bounded chunk shape and write it to file
            std::vector<uint32_t> shapeOut(shape_.size());
            chunk.boundedChunkShape(shape_, chunkShape_, shapeOut);
            util::reverseEndiannessInplace<uint32_t>(shapeOut.begin(), shapeOut.end());

            // N5-Axis order: we need to reverse the chunk shape written to the header
            std::reverse(shapeOut.begin(), shapeOut.end());
            // write chunk shape to header
            for(int d = 0; d < shape_.size(); ++d) {
                file.write((char *) &shapeOut[d], 4);
            }

            // TODO need to write the actual size if we allow for varlength mode
        }

        // members
        const types::ShapeType & shape_;
        const types::ShapeType & chunkShape_;

    };


}
}
