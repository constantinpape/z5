#pragma once

#include <fstream>
#include <ios>

#include "z5/filesystem/handle.hxx"
#include "z5/generic/store.hxx"
#include "z5/util/util.hxx"


namespace z5 {
namespace filesystem {

namespace store_detail {

    // 'what' names the object kind ("chunk" / "shard") in error messages
    inline void readFile(const fs::path & path, std::vector<char> & buffer, const char * what) {
        // open input stream and read the filesize
        std::ifstream file(path, std::ios::binary);
        if(!file.is_open()) {
            throw std::runtime_error(std::string("z5: cannot open ") + what + " file for reading: " + path.string());
        }

        file.seekg(0, std::ios::end);
        const std::size_t file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        // resize the data vector
        buffer.resize(file_size);

        // read the file
        file.read(buffer.data(), file_size);
        if(file.gcount() != static_cast<std::streamsize>(file_size)) {
            throw std::runtime_error(std::string("z5: failed to read ") + what + " file: " + path.string());
        }
        file.close();
    }

    inline void writeFile(const fs::path & path, const std::vector<char> & buffer, const char * what) {
        std::ofstream file(path, std::ios::binary);
        if(!file.is_open()) {
            throw std::runtime_error(std::string("z5: cannot open ") + what + " file for writing: " + path.string());
        }
        file.write(buffer.data(), buffer.size());
        if(!file.good()) {
            throw std::runtime_error(std::string("z5: failed to write ") + what + " file: " + path.string());
        }
        file.close();
    }

}  // namespace store_detail


    // Thin byte-IO layer over chunk / shard files. Everything format-related
    // (codec, shard index, read-modify-write) lives in the generic dataset
    // implementations in z5/generic/, which are parameterized on this policy.
    struct ChunkStore {

        typedef handle::Dataset DatasetHandleType;
        typedef handle::Chunk ChunkHandleType;

        // n5 header reads (varlen size, chunk shape) are only available on the filesystem
        static constexpr bool supportsN5HeaderIO = true;
        static constexpr const char * shardedName = "z5::ShardedDataset";

        static inline bool read(const ChunkHandleType & chunk, std::vector<char> & buffer,
                                const char * what = "chunk") {
            if(!fs::exists(chunk.path())) {
                return false;
            }
            store_detail::readFile(chunk.path(), buffer, what);
            return true;
        }

        static inline void write(const ChunkHandleType & chunk, const std::vector<char> & buffer,
                                 const char * what = "chunk") {
            // create nested chunk directories if needed (a no-op for non-nested chunks)
            chunk.create();
            store_detail::writeFile(chunk.path(), buffer, what);
        }

        static inline void erase(const ChunkHandleType & chunk) {
            if(fs::exists(chunk.path())) {
                fs::remove(chunk.path());
            }
        }

        static inline const fs::path & path(const DatasetHandleType & handle) {
            return handle.path();
        }

        static inline void chunkPath(const ChunkHandleType & chunk, fs::path & path) {
            path = chunk.path();
        }

        // read the varlen mode / size from an n5 chunk header; false if the chunk is not varlen
        static inline bool readVarlenFromHeader(const ChunkHandleType & chunk,
                                                std::size_t & chunkSize) {
            std::ifstream file(chunk.path(), std::ios::binary);

            // read the mode
            uint16_t mode;
            file.read((char *) &mode, 2);
            util::reverseEndiannessInplace(mode);

            if(mode == 0) {
                return false;
            }

            // read the number of dimensions
            uint16_t ndim;
            file.read((char *) &ndim, 2);
            util::reverseEndiannessInplace(ndim);

            // advance the file by ndim * 4 to skip the shape
            file.seekg((ndim + 1) * 4);

            uint32_t varlength;
            file.read((char*) &varlength, 4);
            util::reverseEndiannessInplace(varlength);
            chunkSize = varlength;

            file.close();
            return true;
        }

        // read the chunk shape from an n5 chunk header
        static inline void readShapeFromHeader(const ChunkHandleType & chunk,
                                               types::ShapeType & chunkShape) {
            std::ifstream file(chunk.path(), std::ios::binary);

            // advance the file by 2 to skip the mode
            file.seekg(2);

            // read the number of dimensions
            uint16_t ndim;
            file.read((char *) &ndim, 2);
            util::reverseEndiannessInplace(ndim);

            // read temporary shape with uint32 entries
            std::vector<uint32_t> shapeTmp(ndim);
            for(int d = 0; d < ndim; ++d) {
                file.read((char *) &shapeTmp[d], 4);
            }
            util::reverseEndiannessInplace<uint32_t>(shapeTmp.begin(), shapeTmp.end());

            // N5-Axis order: we need to reverse the chunk shape read from the header
            std::reverse(shapeTmp.begin(), shapeTmp.end());

            chunkShape.resize(ndim);
            std::copy(shapeTmp.begin(), shapeTmp.end(), chunkShape.begin());

            file.close();
        }

    };

    static_assert(z5::generic::ChunkStorePolicy<ChunkStore>);

}
}
