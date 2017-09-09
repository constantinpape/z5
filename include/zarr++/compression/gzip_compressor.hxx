#pragma once

#ifdef WITH_GZIP

#include <zlib.h>

#include "zarr++/compression/compressor_base.hxx"
#include "zarr++/metadata.hxx"

// calls to ZLIB interface following
// https://panthema.net/2007/0328-ZLibString.html

namespace zarr {
namespace compression {

    template<typename T>
    class GzipCompressor : public CompressorBase<T> {

    public:
        GzipCompressor(const ArrayMetadata & metadata) {
            init(metadata);
        }

        void compress(const T * dataIn, std::vector<T> & dataOut, size_t sizeIn) const {

            // open the zlib stream
            z_stream zs;
            memset(&zs, 0, sizeof(zs));

            // TODO bytesize ?
            dataOut.resize(sizeIn);

            if(deflateInit(&zs, clevel_) != Z_OK) {
                throw(std::runtime_error("Initializing zLib deflate failed"));
            }

            zs.next_in = (Bytef*) dataIn;
            zs.avail_in = sizeIn; // TODO do we need the bytesize here?

            int ret;
            T outbuffer[32768];
            size_t currentLength;
            size_t currentPosition = 0;

            // compress bytes blockwise
            do {
                zs.next_out = reinterpret_cast<Bytef*>(outbuffer);
                zs.avail_out = sizeof(outbuffer);

                ret = deflate(&zs, Z_FINISH);

                if(currentPosition < zs.total_out) { // TODO outsize in bytes

                    // append the data to the output
                    currentLength = (zs.total_out - currentPosition);

                    std::cout << currentPosition << " " << currentLength << std::endl;
                    std::cout << zs.total_out << " " << dataOut.size() << std::endl;

                    //dataOut.insert(dataOut.begin() + currentPosition, outbuffer, outbuffer + currentLength); // TODO bytesize?
                    std::cout << "Here" << std::endl;
                    std::copy(dataOut.begin() + currentPosition, dataOut.begin() + currentPosition + currentLength, outbuffer);
                    std::cout << "There" << std::endl;
                    currentPosition += currentLength;
                }

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
            z_stream zs;
            memset(&zs, 0, sizeof(zs));

            if(deflateInit(&zs, clevel_) != Z_OK) {
                throw(std::runtime_error("Initializing zLib inflate failed"));
            }

            zs.next_in = (Bytef*) &dataIn[0];
            zs.avail_in = dataIn.size(); // TODO bytesize ?!

            int ret;
            T outbuffer[32768];
            size_t currentOutSize = 0;
            size_t currentLength;

            // decompress bytes blockwise
            do {
                zs.next_out = reinterpret_cast<Bytef*>(outbuffer);
                zs.avail_out = sizeof(outbuffer);

                ret = inflate(&zs, 0);

                if(currentOutSize < zs.total_out) { // TODO is that correct size ? AND bytesize ?
                    // AAAARRRGH I don't want to use memcpy - hope this works
                    // TODO bytesize everywhere ?
                    currentLength = (zs.total_out - currentOutSize);
                    std::copy(
                        outbuffer, outbuffer + currentLength, dataOut + currentOutSize
                    );
                    currentOutSize += currentLength;
                }
            } while(ret == Z_OK);

            inflateEnd(&zs);

			if (ret != Z_STREAM_END) {          // an error occurred that was not EOF
    			std::ostringstream oss;
    			oss << "Exception during zlib decompression: (" << ret << ") " << zs.msg;
    			throw(std::runtime_error(oss.str()));
    		}
		}

    private:
        void init(const ArrayMetadata & metadata) {
            clevel_ = metadata.compressorLevel;
        }

       // compression level
       int clevel_;

    };

} // namespace compression
} // namespace zarr

#endif
