#pragma once

#ifdef WITH_ZLIB

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

            // resize the out data to input size
            dataOut.clear();
            dataOut.reserve(sizeIn * sizeof(T));

            // intermediate output buffer
            // size set to 256 kb, which is recommended in the zlib usage example:
            // http://www.gzip.org/zlib/zlib_how.html/
            const std::size_t bufferSize = 262144;
            std::vector<Bytef> outbuffer(bufferSize);

            // init the zlib or gzip stream
            // note that the gzip format fails for very small input sizes (<= 22)
            // so we use zlib for these no matter the input
            // TODO this might lead to issues with other n5 implementations
            if(!useZlibEncoding_ && sizeIn > 22) {
                if(deflateInit2(&zs, clevel_,
                                Z_DEFLATED, gzipWindowsize + 16,
                                gzipCFactor, Z_DEFAULT_STRATEGY) != Z_OK) {
                    throw(std::runtime_error("Initializing zLib deflate failed"));
                }
            } else {
                if(deflateInit(&zs, clevel_) != Z_OK){
                    throw(std::runtime_error("Initializing zLib deflate failed"));
                }
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

        }


        void decompress(const std::vector<char> & dataIn, T * dataOut, std::size_t sizeOut) const {

            // open the zlib stream
            z_stream zs;
            memset(&zs, 0, sizeof(zs));

            // init the zlib stream
            if(!useZlibEncoding_ && sizeOut > 22) {
                if(inflateInit2(&zs, gzipWindowsize + 16) != Z_OK){
                    throw(std::runtime_error("Initializing zLib inflate failed"));}
            } else {
                if(inflateInit(&zs) != Z_OK){
                    throw(std::runtime_error("Initializing zLib inflate failed"));
                }
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

    private:
        void init(const DatasetMetadata & metadata) {
            clevel_ = boost::any_cast<int>(metadata.compressionOptions.at("level"));
            useZlibEncoding_ = boost::any_cast<bool>(metadata.compressionOptions.at("useZlib"));
        }

        // compression level
        int clevel_;
        // use zlib or gzip encoding
        bool useZlibEncoding_;

        const static int gzipWindowsize = 15;
        //static int gzipBsize = 8096;
        const static int gzipCFactor = 9;
    };

} // namespace compression
} // namespace z5

#endif
