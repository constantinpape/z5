#pragma once

#ifdef WITH_BZIP

#include <bzlib.h>

#include "zarr++/compression/compressor_base.hxx"
#include "zarr++/metadata.hxx"

namespace zarr {
namespace compression {

    // TODO gzip encoding
    template<typename T>
    class BzipCompressor : public CompressorBase<T> {

    public:
        BzipCompressor(const ArrayMetadata & metadata) {
            init(metadata);
        }

        void compress(const T * dataIn, std::vector<T> & dataOut, size_t sizeIn) const {

            // resize the out data to input size
            dataOut.clear();
            dataOut.resize(sizeIn);

            // FIXME unfortunately the utility api does not support passing
            // const pointer for the in data
            // compress buffer to buffer with bzip
            // last two arguments:
            // verbosity: 0 -> not verbose
            // workFactor: 30 -> default value
            //unsigned int compressionSize = dataOut.size() * sizeof(T);
            //auto status = BZ2_bzBuffToBuffCompress(
            //    (char *) &dataOut[0], &compressionSize,
            //    (const char *) dataIn, sizeIn * sizeof(T),
            //    clevel_, 0, 30
            //);

            // low - level API
            // open the bzip2 stream and set pointer values
            bz_stream bzs;
            bzs.opaque = NULL;
            bzs.bzalloc = NULL;
            bzs.bzfree = NULL;

            if(BZ2_bzCompressInit(&bzs, clevel_, 0, 30) != BZ_OK) {
                throw(std::runtime_error("Initializing bzip compression failed"));
            }

            // set the stream in-pointer to the input data and the input size
            // to the size of the input in bytes
            bzs.next_in = (char*) dataIn;
            bzs.avail_in = sizeIn * sizeof(T);

            // let zlib compress the bytes blockwise
            int ret;
            size_t currentPosition = 0;
            size_t total_out;
            do {
                // set the stream out-pointer to the current position of the out-data
                // and set the available out size to the remaining size in the vector not written at
                //zs.next_out = reinterpret_cast<Bytef*>(&dataOut[currentPosition]);
                bzs.next_out = (char*) &dataOut[currentPosition];
                bzs.avail_out = std::distance(dataOut.begin() + currentPosition, dataOut.end()) * sizeof(T);

                ret = BZ2_bzCompress(&bzs, BZ_FINISH);

                // combine the 32bit counts to get thr total out number
                total_out = (static_cast<size_t>(bzs.total_out_hi32) << 32) + bzs.total_out_lo32;
                // get the current position in the out vector
                currentPosition = total_out / sizeof(T);

            } while(ret == BZ_OK);

            BZ2_bzCompressEnd(&bzs);

    		if (ret != BZ_STREAM_END) {          // an error occurred that was not EOF
    		    std::ostringstream oss;
    		    oss << "Exception during bzip compression: (" << ret << ") ";
    		    throw(std::runtime_error(oss.str()));
    		}

            dataOut.resize(currentPosition);
        }


        void decompress(const std::vector<T> & dataIn, T * dataOut, size_t sizeOut) const {

            unsigned int decompressionSize = dataIn.size() * sizeof(T);

            // FIXME unfortunately the utility api does not support passing
            // const pointer for the in data
            // decompress buffer to buffer with bzip
            // last two arguments:
            // small: if larger than 0, uses alternative algorithm with less performance
            // but smaller memory footprint (we use 0 here)
            // verbosity: 0 -> not verbose
            //auto status = BZ2_bzBuffToBuffDecompress(
            //    (char *) dataOut, &decompressionSize,
            //    (const char *) &dataIn[0], sizeOut * sizeof(T),
            //    0, 0
            //);

            // open the zlib stream
            bz_stream bzs;
            bzs.opaque = NULL;
            bzs.bzalloc = NULL;
            bzs.bzfree = NULL;

            // init the zlib stream
            // last two arguments:
            // small: if larger than 0, uses alternative algorithm with less performance
            // but smaller memory footprint (we use 0 here)
            // verbosity: 0 -> not verbose
            if(BZ2_bzDecompressInit(&bzs, 0, 0) != BZ_OK) {
                throw(std::runtime_error("Initializing bzip decompression failed"));
            }

            // set the stream input to the beginning of the input data
            bzs.next_in = (char*) &dataIn[0];
            bzs.avail_in = dataIn.size() * sizeof(T);

            // let zlib decompress the bytes blockwise
            int ret;
            size_t currentPosition = 0;
            size_t currentLength;
            size_t total_out;
            do {
                // set the stream outout to the output dat at the current position
                // and set the available size to the remaining bytes in the output data
                // reinterpret_cast because (Bytef*) throws warning
                bzs.next_out = reinterpret_cast<char*>(dataOut + currentPosition);
                bzs.avail_out = (sizeOut - currentPosition) * sizeof(T);

                ret = BZ2_bzDecompress(&bzs);

                // combine the 32bit counts to get thr total out number
                total_out = (static_cast<size_t>(bzs.total_out_hi32) << 32) + bzs.total_out_lo32;

                // get the current position in the out data
                currentPosition = total_out / sizeof(T);

            } while(ret == BZ_OK);

            BZ2_bzDecompressEnd(&bzs);

            if(ret != BZ_OK) {
    		    std::ostringstream oss;
    		    oss << "Exception during bzip compression: (" << ret << ") ";
    		    throw(std::runtime_error(oss.str()));
            }

		}

    private:
        void init(const ArrayMetadata & metadata) {
            // TODO clevel = compressorLevel -1 ???
            clevel_ = metadata.compressorLevel;
        }

        // compression level
        int clevel_;
    };

} // namespace compression
} // namespace zarr

#endif
