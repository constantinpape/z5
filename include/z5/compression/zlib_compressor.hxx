#pragma once

#ifdef WITH_ZLIB

#include <chrono>
#include <zlib.h>

#include "z5/compression/compressor_base.hxx"
#include "z5/metadata.hxx"

// zlib manual:
// https://zlib.net/manual.html

// calls to ZLIB interface following
// https://blog.cppse.nl/deflate-and-gzip-compress-and-decompress-functions

namespace z5 {
namespace compression {

    template<typename T>
    class ZlibCompressor : public CompressorBase<T> {

    public:
        ZlibCompressor(const DatasetMetadata & metadata) {
            init(metadata);
        }

        void compress(const T * dataIn, std::vector<char> & dataOut, std::size_t sizeIn) const {

            // open the zlib stream
            z_stream zs;
            memset(&zs, 0, sizeof(zs));

            // reserve the out data to input size
            dataOut.clear();
            dataOut.reserve(sizeIn * sizeof(T));

            // intermediate output buffer
            // size set to 256 kb, which is recommended in the zlib usage example:
            // http://www.gzip.org/zlib/zlib_how.html/
            const std::size_t bufferSize = 262144;
            std::vector<Bytef> outbuffer(bufferSize);

            // init the zlib or gzip stream
            if(useZlibEncoding_) {
                if(deflateInit(&zs, clevel_) != Z_OK){
                    throw(std::runtime_error("Initializing zLib deflate failed"));
                }
            } else {
                if(deflateInit2(&zs, clevel_,
                                Z_DEFLATED, MAX_WBITS + 16,
                                MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY) != Z_OK) {
                    throw(std::runtime_error("Initializing zLib deflate failed"));
                }

                // write additional gzip header
                write_gzip_header(dataOut);
            }

            // set the stream in-pointer to the input data and the input size
            // to the size of the input in bytes
            zs.next_in = (Bytef*) dataIn;
            zs.avail_in = sizeIn * sizeof(T);

            // let zlib compress the bytes blockwise
            int ret;
            std::size_t prevOutBytes = 0;
            std::size_t bytesCompressed;
            do {
                // set the stream out-pointer to the current position of the out-data
                // and set the available out size to the remaining size in the vector not written at

                zs.next_out = &outbuffer[0];
                zs.avail_out = outbuffer.size();

                ret = deflate(&zs, Z_FINISH);
                bytesCompressed = zs.total_out - prevOutBytes;
                prevOutBytes = zs.total_out;

                dataOut.insert(dataOut.end(),
                               outbuffer.begin(),
                               outbuffer.begin() + bytesCompressed);

            } while(ret == Z_OK);

            deflateEnd(&zs);

    		if (ret != Z_STREAM_END) {          // an error occurred that was not EOF
    		    std::ostringstream oss;
    		    oss << "Exception during zlib compression: (" << ret << ") " << zs.msg;
    		    throw(std::runtime_error(oss.str()));
    		}

            // gzip: append checksum to the data
            if(!useZlibEncoding_) {
                write_gzip_footer(dataIn, sizeIn, dataOut);
            }

        }


        void decompress(const std::vector<char> & dataIn, T * dataOut, std::size_t sizeOut) const {

            // open the zlib stream
            z_stream zs;
            memset(&zs, 0, sizeof(zs));

            // init the zlib stream with automatic header detection
            // for zlib and zip format (MAX_WBITS + 32)
            if(inflateInit2(&zs, MAX_WBITS + 32) != Z_OK){
                throw(std::runtime_error("Initializing zLib inflate failed"));
            }

            // set the stream input to the beginning of the input data
            zs.next_in = (Bytef*) &dataIn[0];
            zs.avail_in = dataIn.size();

            // let zlib decompress the bytes blockwise
            int ret;
            std::size_t currentPosition = 0;
            do {
                // set the stream outout to the output dat at the current position
                // and set the available size to the remaining bytes in the output data
                // reinterpret_cast because (Bytef*) throws warning
                zs.next_out = reinterpret_cast<Bytef*>(dataOut + currentPosition);
                zs.avail_out = (sizeOut - currentPosition) * sizeof(T);

                ret = inflate(&zs, Z_FINISH);

                // get the current position in the out data
                currentPosition = zs.total_out / sizeof(T);

            } while(ret == Z_OK);

            inflateEnd(&zs);

			if (ret != Z_STREAM_END) {          // an error occurred that was not EOF
    			std::ostringstream oss;
    			oss << "Exception during zlib decompression: (" << ret << ") " << zs.msg;
    			throw(std::runtime_error(oss.str()));
    		}
		}

        virtual types::Compressor type() const {
            return types::zlib;
        }

        // header format based on
        // https://github.com/python/cpython/blob/3.7/Lib/gzip.py#L221
        inline void write_gzip_header(std::vector<char> & dataOut) const {
            // TODO simplifty this by representing magicHeader, compressionMethod and
            // emptyFilename as a single hex

            // write magic header
            const std::string magicHeader = std::to_string(0x1f8b);
            for(char cc: magicHeader) {
                dataOut.push_back(cc);
            }

            // write compression method
            const std::string compressionMethod = std::to_string(0x08);
            for(char cc: compressionMethod) {
                dataOut.push_back(cc);
            }

            // we skip the filename, which is possible, see
            // https://github.com/python/cpython/blob/3.7/Lib/gzip.py#L232-L233
            const std::string emptyFilename = std::to_string(0x00);
            for(char cc: emptyFilename) {
                dataOut.push_back(cc);
            }

            // write current time stamp
            const uint64_t mtime = std::chrono::duration_cast<std::chrono::duration<uint64_t>>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            const std::string mTimeBytes = std::to_string(mtime);
            for(char cc: mTimeBytes) {
                dataOut.push_back(cc);
            }

            // write more magic bytes
            const std::string magic1 = std::to_string(0x02);
            for(char cc: magic1) {
                dataOut.push_back(cc);
            }
            const std::string magic2 = std::to_string(0xff);
            for(char cc: magic2) {
                dataOut.push_back(cc);
            }

        }

        inline void write_gzip_footer(const T * dataIn,
                                      const std::size_t sizeIn,
                                      std::vector<char> & dataOut) const {
            unsigned long crc = crc32(0L, Z_NULL, 0);
            const int64_t bufferSize = 262144;
            int64_t len = sizeIn;

            Bytef * p = (Bytef *) dataIn;

            // checksum data of bufferSize as long as it's good
            while(len > 0) {
                crc = crc32(crc, p, std::min(bufferSize, len));
                len -= bufferSize;
                p = (Bytef *) (dataIn + bufferSize);
            }

            // convert unsigned long to chars and append to dataOut
            const std::string crc_bytes = std::to_string(crc);
            for(char cc: crc_bytes) {
                dataOut.push_back(cc);
            }

        }

    private:
        void init(const DatasetMetadata & metadata) {
            clevel_ = boost::any_cast<int>(metadata.compressionOptions.at("level"));
            useZlibEncoding_ = boost::any_cast<bool>(metadata.compressionOptions.at("useZlib"));
        }

        // compression level
        int clevel_;
        // use zlib or gzip encoding
        bool useZlibEncoding_;
    };

} // namespace compression
} // namespace z5

#endif
