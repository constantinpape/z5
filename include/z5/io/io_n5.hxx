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

    // TODO TODO TODO
    // endianess mess
    // the best way would be to handle this
    // in a streambuffer for boost::iostreams
    //

    template<typename T>
    class ChunkIoN5 : public ChunkIoBase<T> {

    public:

        ChunkIoN5(/*const ArrayMetadata & metadata*/) {
        }

        inline bool read(const handle::Chunk & chunk, std::vector<T> & data) const {

            // if the chunk exists, we read it
            if(chunk.exists()) {

                // this might speed up the I/O by decoupling C++ buffers from C buffers
                std::ios_base::sync_with_stdio(false);
                // open input stream and read the header
                fs::ifstream file(chunk.path(), std::ios::binary);
                types::ShapeType chunkShape;
                size_t fileSize = readHeader(file, chunkShape);

                // resize the data vector
                size_t vectorSize = fileSize / sizeof(T) + (fileSize % sizeof(T) == 0 ? 0 : sizeof(T));
                data.resize(vectorSize);

                // read the file
                file.read((char*) &data[0], fileSize);
                file.close();

                // reverse the endianness
                // TODO actually check that the file endianness is different than the system endianness
                if(sizeof(T) > 1) { // we don't need to convert single bit numbers
                    std::cout << data[0] << std::endl;
                    std::cout << "Reversing endianness" << std::endl;
                    util::reverseEndiannessInplace(data);
                    std::cout << data[0] << std::endl;
                }

                // return true, because we have read an existing chunk
                return true;

            } else {
                return false;
            }

        }


        inline void write(const handle::Chunk & chunk, const std::vector<T> & data) const {
            // this might speed up the I/O by decoupling C++ buffers from C buffers
            std::ios_base::sync_with_stdio(false);
            fs::ofstream file(chunk.path(), std::ios::binary);
            // write the header
            writeHeader(chunk, file);
            // reverse the endiannessif necessary
            if(sizeof(T) > 1) {
                std::vector<T> dataTmp(data);
                util::reverseEndiannessInplace(dataTmp);
                file.write((char*) &dataTmp[0], dataTmp.size() * sizeof(T));
            } else {
                // write the data
                file.write((char*) &data[0], data.size() * sizeof(T));
            }
            file.close();
        }


        inline void getChunkShape(const handle::Chunk & chunk, types::ShapeType & shape) const {
            std::ios_base::sync_with_stdio(false);
            fs::ifstream file(chunk.path(), std::ios::binary);
            readHeader(file, shape);
            file.close();
        }

    private:

        // TODO allow for reading the mode
        size_t readHeader(fs::ifstream & file, types::ShapeType & shape) const {
            // read the mode
            uint16_t mode;
            file.read((char *) &mode, 2);
            util::reverseEndiannessInplace(mode);
            // TODO support varlength mode
            if(mode != 0) {
                throw std::runtime_error("Zarr++ only supports reading N5 chunks in default mode");
            }

            // read the number of dimensions
            uint16_t nDims;
            file.read((char *) &nDims, 2);
            util::reverseEndiannessInplace(nDims);

            // read tempory shape with uint32 entries
            std::vector<uint32_t> shapeTmp(nDims);
            for(int d = 0; d < nDims; ++d) {
                file.read((char *) &shapeTmp[d], 4);
            }
            util::reverseEndiannessInplace(shapeTmp);

            // copy the tempory shape to out shape
            shape.resize(nDims);
            std::copy(shapeTmp.begin(), shapeTmp.end(), shape.begin());

            // TODO need to read the actual size if we allow for varlength mode

            // calculate the file size
            size_t fileSize = std::accumulate(shape.begin(), shape.end(), 1, std::multiplies<size_t>());
            return fileSize * sizeof(T);
        }

        void writeHeader(const handle::Chunk & chunk, fs::ofstream & file) const {
            // write the mode
            uint16_t mode = 0; // TODO support the varlength mode as well
            util::reverseEndiannessInplace(mode);
            file.write((char*) &mode, 2);
            // TODO need to get the number of dims
            // write the number of dimensions
            //uint16_t nDims = numberOfDimensions_;
            //file.write((char*) &nDims, 2);
            //// TODO write chunk shape
            //for(int d = 0; d < nDims; ++d) {
            //    
            //}
            // TODO if we allow mode, need to be able to read another block here
        }

    };


}
}
