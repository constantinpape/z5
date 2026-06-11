#pragma once

#ifdef WITH_ZLIB

#ifdef WITH_LIBDEFLATE
#include <libdeflate.h>
#else
#include <zlib.h>
#endif

#include <cstring>
#include <stdexcept>
#include <string>

#include "z5/compression/compressor_base.hxx"
#include "z5/metadata.hxx"

// zlib manual:
// https://zlib.net/manual.html

// calls to ZLIB interface following
// https://blog.cppse.nl/deflate-and-gzip-compress-and-decompress-functions

// libdeflate (faster, SIMD-accelerated drop-in for the gzip/zlib codec) manual:
// https://github.com/ebiggers/libdeflate

namespace z5 {
namespace compression {

    template<typename T>
    class ZlibCompressor : public CompressorBase<T> {

    public:
        ZlibCompressor(const DatasetMetadata & metadata) {
            init(metadata);
        }

#ifdef WITH_LIBDEFLATE

        // libdeflate backend. The on-disk format is identical to the zlib backend
        // (RFC1950 zlib wrapper when useZlibEncoding_, RFC1952 gzip wrapper otherwise),
        // so files written by either backend are interchangeable.

        void compress(const T * dataIn, std::vector<char> & dataOut, std::size_t sizeIn) const {

            const std::size_t sizeInBytes = sizeIn * sizeof(T);

            // libdeflate levels are 1..12 and have no "store" mode; zlib levels are 0..9.
            // z5's default level is 5 and never selects 0, but clamp defensively so the
            // same metadata works for both backends (level_ itself stays the raw value
            // reported by getOptions, so metadata round-trips unchanged).
            int level = level_;
            if(level < 1) { level = 1; }
            if(level > 12) { level = 12; }

            // libdeflate compressors are not thread-safe and a single ZlibCompressor
            // instance is shared across worker threads, so allocate/free per call
            // (cheap: one malloc + table init, mirrors the per-call zlib z_stream).
            libdeflate_compressor * compressor = libdeflate_alloc_compressor(level);
            if(compressor == nullptr) {
                throw std::runtime_error("z5: libdeflate_alloc_compressor failed");
            }

            // size the output to the worst-case bound so we can compress in a single
            // pass directly into it (no scratch buffer, no per-block vector::insert).
            const std::size_t bound = useZlibEncoding_ ?
                libdeflate_zlib_compress_bound(compressor, sizeInBytes) :
                libdeflate_gzip_compress_bound(compressor, sizeInBytes);
            dataOut.resize(bound);

            const std::size_t compressedSize = useZlibEncoding_ ?
                libdeflate_zlib_compress(compressor, dataIn, sizeInBytes, dataOut.data(), bound) :
                libdeflate_gzip_compress(compressor, dataIn, sizeInBytes, dataOut.data(), bound);

            libdeflate_free_compressor(compressor);

            // libdeflate returns 0 if the output did not fit; we sized to the
            // worst-case bound, so 0 indicates a genuine failure.
            if(compressedSize == 0) {
                throw std::runtime_error("z5: libdeflate compression failed");
            }

            // shrink to the actual compressed size
            dataOut.resize(compressedSize);
        }


        void decompress(const char * dataIn, std::size_t nBytesIn, T * dataOut, std::size_t sizeOut) const {

            const std::size_t sizeOutBytes = sizeOut * sizeof(T);

            libdeflate_decompressor * decompressor = libdeflate_alloc_decompressor();
            if(decompressor == nullptr) {
                throw std::runtime_error("z5: libdeflate_alloc_decompressor failed");
            }

            // Pick the wrapper from the metadata flag (zlib for zarr-v2 "zlib", gzip
            // for n5 / zarr-v2 "gzip" / zarr-v3). Pass a non-null actual-size pointer so
            // that, like the previous zlib code, a stream decoding to fewer than the
            // expected bytes still succeeds (only an overflow is an error).
            std::size_t actualOut = 0;
            libdeflate_result res = useZlibEncoding_ ?
                libdeflate_zlib_decompress(decompressor, dataIn, nBytesIn, dataOut, sizeOutBytes, &actualOut) :
                libdeflate_gzip_decompress(decompressor, dataIn, nBytesIn, dataOut, sizeOutBytes, &actualOut);

            // Robustness fallback: retry with the other wrapper. The old zlib path used
            // automatic header detection (MAX_WBITS + 32); this preserves the ability to
            // read a foreign file whose on-disk wrapper disagrees with its declared metadata.
            if(res != LIBDEFLATE_SUCCESS) {
                res = useZlibEncoding_ ?
                    libdeflate_gzip_decompress(decompressor, dataIn, nBytesIn, dataOut, sizeOutBytes, &actualOut) :
                    libdeflate_zlib_decompress(decompressor, dataIn, nBytesIn, dataOut, sizeOutBytes, &actualOut);
            }

            libdeflate_free_decompressor(decompressor);

            if(res != LIBDEFLATE_SUCCESS) {
                throw std::runtime_error("z5: libdeflate decompression failed (" + std::to_string((int) res) + ")");
            }
        }

#else // WITH_LIBDEFLATE -- stock zlib backend

        void compress(const T * dataIn, std::vector<char> & dataOut, std::size_t sizeIn) const {

            // open the zlib stream
            z_stream zs;
            memset(&zs, 0, sizeof(zs));

            // init the zlib or gzip stream
            if(useZlibEncoding_) {
                if(deflateInit(&zs, level_) != Z_OK){
                    throw(std::runtime_error("Initializing zLib deflate failed"));
                }
            } else {
                if(deflateInit2(&zs, level_,
                                Z_DEFLATED, MAX_WBITS + 16,
                                MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY) != Z_OK) {
                    throw(std::runtime_error("Initializing zLib deflate failed"));
                }
            }

            // size the output to the worst-case bound so we can deflate in a single pass
            // directly into it (no 256kb scratch buffer, no per-block vector::insert).
            const std::size_t sizeInBytes = sizeIn * sizeof(T);
            const uLong bound = deflateBound(&zs, sizeInBytes);
            dataOut.resize(bound);

            zs.next_in = (Bytef*) dataIn;
            zs.avail_in = sizeInBytes;
            zs.next_out = reinterpret_cast<Bytef*>(dataOut.data());
            zs.avail_out = bound;

            // deflateBound guarantees the whole input fits, so a single Z_FINISH suffices
            const int ret = deflate(&zs, Z_FINISH);
            const std::size_t compressedSize = zs.total_out;
            deflateEnd(&zs);

    		if (ret != Z_STREAM_END) {          // an error occurred that was not EOF
                std::string err = "Exception during zlib compression: (" + std::to_string(ret)  + ")";
    		    throw std::runtime_error(err);
    		}

            // shrink to the actual compressed size
            dataOut.resize(compressedSize);
        }


        void decompress(const char * dataIn, std::size_t nBytesIn, T * dataOut, std::size_t sizeOut) const {

            // open the zlib stream
            z_stream zs;
            memset(&zs, 0, sizeof(zs));

            // init the zlib stream with automatic header detection
            // for zlib and zip format (MAX_WBITS + 32)
            if(inflateInit2(&zs, MAX_WBITS + 32) != Z_OK){
                throw(std::runtime_error("Initializing zLib inflate failed"));
            }

            // set the stream input to the beginning of the input data
            zs.next_in = (Bytef*) dataIn;
            zs.avail_in = nBytesIn;

            // let zlib decompress the bytes blockwise
            int ret;
            std::size_t currentPosition = 0;
            do {
                // set the stream output to the output dat at the current position
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
                std::string err = "Exception during zlib decompression: (" + std::to_string(ret)  + ")";
    		    throw std::runtime_error(err);
    		}
		}

#endif // WITH_LIBDEFLATE

        virtual types::Compressor type() const {
            return types::zlib;
        }

        inline void getOptions(types::CompressionOptions & opts) const {
            // report the full option set used by init: metadata serialization reads
            // "useZlib" back from these options and would throw if it were missing
            opts["level"] = level_;
            opts["useZlib"] = useZlibEncoding_;
        }

    private:
        void init(const DatasetMetadata & metadata) {
            level_ = std::get<int>(metadata.compressionOptions.at("level"));
            useZlibEncoding_ = std::get<bool>(metadata.compressionOptions.at("useZlib"));
        }

        // compression level
        int level_;
        // use zlib or gzip encoding
        bool useZlibEncoding_;
    };

} // namespace compression
} // namespace z5

#endif
