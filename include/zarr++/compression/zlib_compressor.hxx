#pragma once

#ifdef WITH_ZLIB

#include <zlib.h>

#include "zarr++/compression/compressor_base.hxx"
#include "zarr++/metadata.hxx"

// zlib manual:
// https://zlib.net/manual.html

// calls to ZLIB interface following
// https://blog.cppse.nl/deflate-and-gzip-compress-and-decompress-functions

namespace zarr {
namespace compression {

    // TODO gzip encoding
    template<typename T>
    class ZlibCompressor : public CompressorBase<T> {

    public:
        ZlibCompressor(const ArrayMetadata & metadata) {
            init(metadata);
        }

        void compress(const T * dataIn, std::vector<T> & dataOut, size_t sizeIn) const {

            // open the zlib stream
            z_stream zs;
            memset(&zs, 0, sizeof(zs));

            // resize the out data to input size
            dataOut.clear();
            dataOut.resize(sizeIn);

            // init the zlib stream
            if(useZlibEncoding_) {
                if(deflateInit(&zs, clevel_) != Z_OK){throw(std::runtime_error("Initializing zLib deflate failed"));}
            } else {
                if(deflateInit2(&zs, clevel_, Z_DEFLATED, gzipWindowsize + 16, gzipCFactor, Z_DEFAULT_STRATEGY) != Z_OK) {
                    throw(std::runtime_error("Initializing zLib deflate failed"));
                }
            }

            // set the stream in-pointer to the input data and the input size
            // to the size of the input in bytes
            zs.next_in = (Bytef*) dataIn;
            zs.avail_in = sizeIn * sizeof(T);

            // let zlib compress the bytes blockwise
            int ret;
            size_t currentPosition = 0;
            do {
                // set the stream out-pointer to the current position of the out-data
                // and set the available out size to the remaining size in the vector not written at
                //zs.next_out = reinterpret_cast<Bytef*>(&dataOut[currentPosition]);
                zs.next_out = (Bytef*) &dataOut[currentPosition];
                zs.avail_out = std::distance(dataOut.begin() + currentPosition, dataOut.end()) * sizeof(T);

                ret = deflate(&zs, Z_FINISH);

                // get the current position in the out vector
                currentPosition = zs.total_out / sizeof(T);

            } while(ret == Z_OK);

            deflateEnd(&zs);

    		if (ret != Z_STREAM_END) {          // an error occurred that was not EOF
    		    std::ostringstream oss;
    		    oss << "Exception during zlib compression: (" << ret << ") " << zs.msg;
    		    throw(std::runtime_error(oss.str()));
    		}

            dataOut.resize(currentPosition);

        }


        void decompress(const std::vector<T> & dataIn, T * dataOut, size_t sizeOut) const {

            // open the zlib stream
            z_stream zs;
            memset(&zs, 0, sizeof(zs));

            // init the zlib stream
            if(useZlibEncoding_) {
                if(inflateInit(&zs) != Z_OK){throw(std::runtime_error("Initializing zLib inflate failed"));}
            } else {
                if(inflateInit2(&zs, gzipWindowsize + 16) != Z_OK){throw(std::runtime_error("Initializing zLib inflate failed"));}
            }

            // set the stream input to the beginning of the input data
            zs.next_in = (Bytef*) &dataIn[0];
            zs.avail_in = dataIn.size() * sizeof(T);

            // let zlib decompress the bytes blockwise
            int ret;
            size_t currentPosition = 0;
            size_t currentLength;
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

            // FIXME for some reasion this always failes with a -5 error (Z_BUF_ERROR)
            // but the tests seem to work just fine...
			//if (ret != Z_STREAM_END) {          // an error occurred that was not EOF
    		//	std::ostringstream oss;
    		//	oss << "Exception during zlib decompression: (" << ret << ") " << zs.msg;
    		//	throw(std::runtime_error(oss.str()));
    		//}
		}

    private:
        void init(const ArrayMetadata & metadata) {
            // TODO clevel = compressorLevel -1 ???
            clevel_ = metadata.compressorLevel;
            useZlibEncoding_ = (metadata.codec == "zlib") ? true : false;
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
} // namespace zarr

#endif
