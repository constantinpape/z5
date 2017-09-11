#pragma once

#include <ios>

#ifndef BOOST_FILESYSTEM_NO_DEPERECATED
#define BOOST_FILESYSTEM_NO_DEPERECATED
#endif
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "zarr++/io/io_base.hxx"
#include "zarr++/types/types.hxx"

namespace fs = boost::filesystem;

namespace zarr {
namespace io {

    // TODO TODO TODO
    // endianess mess
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
            // write the data
            file.write((char*) &data[0], data.size() * sizeof(T));
            file.close();
        }

    private:

        void readHeader(const handle::Chunk & chunk) const {
            // TODO call the other readHeader with proper file stream
        }

        // TODO allow for reading the mode
        size_t readHeader(fs::ifstream & file, types::ShapeType & shape) const {
            // read the mode
            uint16_t mode;
            file.read((char *) &mode, 2);
            // TODO support varlength mode
            if(mode != 0) {
                throw std::runtime_error("Zarr++ only supports reading N5 chunks in default mode");
            }

            // read the number of dimensions
            uint16_t nDims;
            file.read((char *) &nDims, 2);

            // read the shape
            shape.resize(nDims);
            for(int d = 0; d < nDims; ++d) {
                file.read((char *) &shape[d], 4);
            }

            // TODO need to read the actual size if we allow for varlength mode

            // calculate the file size
            size_t fileSize = std::accumulate(shape.begin(), shape.end(), 1, std::multiplies<size_t>());
            return fileSize * sizeof(T);
        }

        void writeHeader(const handle::Chunk & chunk, fs::ofstream & file) const {
            // write the mode
            uint16_t mode = 0; // TODO support the varlength mode as well
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
