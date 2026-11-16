#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "z5/dataset.hxx"
#include "z5/util/vlen.hxx"

#ifdef WITH_ZSTD
#include <zstd.h>
#endif

// CompressorBase::decompressBytes decodes a byte stream whose decoded size isn't
// known in advance (chunks of variable-length strings), and the vlen framing
// that splits it into items.

namespace z5 {
namespace compression {

    // byte streams of several sizes: empty, tiny, and large and compressible
    // enough to need several buffer growths
    inline std::vector<std::vector<char>> testStreams() {
        std::vector<std::vector<char>> streams;
        streams.emplace_back();
        streams.emplace_back(std::vector<char>{'a'});
        std::default_random_engine generator;
        std::uniform_int_distribution<int> distr(0, 255);
        std::vector<char> random(10000);
        for(auto & c : random) {
            c = static_cast<char>(distr(generator));
        }
        streams.push_back(random);
        streams.emplace_back(std::vector<char>(1 << 22, 'x'));  // ~4000:1 for most codecs
        return streams;
    }

    template<class COMPRESSOR>
    void checkRoundTrip(const COMPRESSOR & compressor, const std::string & name) {
        for(const auto & stream : testStreams()) {
            std::vector<char> compressed, out;
            compressor.compress(stream.data(), compressed, stream.size());
            compressor.decompressBytes(compressed.data(), compressed.size(), out);
            ASSERT_EQ(out, stream) << name << " size " << stream.size();
        }
    }

    // a truncated stream must raise, not loop or return partial data
    template<class COMPRESSOR>
    void checkTruncated(const COMPRESSOR & compressor, const std::string & name) {
        std::vector<char> stream(100000);
        for(std::size_t i = 0; i < stream.size(); ++i) {
            stream[i] = static_cast<char>((i * 7919) % 251);
        }
        std::vector<char> compressed, out;
        compressor.compress(stream.data(), compressed, stream.size());
        ASSERT_THROW(compressor.decompressBytes(compressed.data(), compressed.size() / 2, out),
                     std::runtime_error) << name;
    }

    inline DatasetMetadata metadataWith(const types::Compressor compressor,
                                        const types::CompressionOptions & options,
                                        const int zarrFormat=3) {
        DatasetMetadata metadata;
        metadata.isZarr = true;
        metadata.zarrFormat = zarrFormat;
        metadata.compressor = compressor;
        metadata.compressionOptions = options;
        return metadata;
    }


    TEST(DecompressBytesTest, Raw) {
        RawCompressor<char> compressor;
        std::vector<char> in{'a', 'b', 'c'}, out;
        compressor.decompressBytes(in.data(), in.size(), out);
        ASSERT_EQ(out, in);
    }

#ifdef WITH_ZLIB
    TEST(DecompressBytesTest, Zlib) {
        for(const bool useZlib : {true, false}) {
            const std::string name = useZlib ? "zlib" : "gzip";
            ZlibCompressor<char> compressor(metadataWith(types::zlib, {{"level", 5}, {"useZlib", useZlib}}));
            checkRoundTrip(compressor, name);
            checkTruncated(compressor, name);
        }
    }
#endif

#ifdef WITH_ZSTD
    TEST(DecompressBytesTest, Zstd) {
        ZstdCompressor<char> compressor(metadataWith(types::zstd, {{"level", 3}}));
        checkRoundTrip(compressor, "zstd");
        checkTruncated(compressor, "zstd");
    }

    // a frame without its content size takes the streaming path
    TEST(DecompressBytesTest, ZstdWithoutContentSize) {
        ZstdCompressor<char> compressor(metadataWith(types::zstd, {{"level", 3}}));
        for(const auto & stream : testStreams()) {
            std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> ctx(ZSTD_createCCtx(), &ZSTD_freeCCtx);
            ZSTD_CCtx_setParameter(ctx.get(), ZSTD_c_contentSizeFlag, 0);
            std::vector<char> compressed(ZSTD_compressBound(stream.size()));
            ZSTD_outBuffer output = {compressed.data(), compressed.size(), 0};
            ZSTD_inBuffer input = {stream.data(), stream.size(), 0};
            ASSERT_EQ(ZSTD_compressStream2(ctx.get(), &output, &input, ZSTD_e_end), 0u);
            compressed.resize(output.pos);
            ASSERT_EQ(ZSTD_getFrameContentSize(compressed.data(), compressed.size()), ZSTD_CONTENTSIZE_UNKNOWN);

            std::vector<char> out;
            compressor.decompressBytes(compressed.data(), compressed.size(), out);
            ASSERT_EQ(out, stream) << "size " << stream.size();
            if(!stream.empty()) {
                ASSERT_THROW(compressor.decompressBytes(compressed.data(), compressed.size() - 1, out),
                             std::runtime_error);
            }
        }
    }
#endif

#ifdef WITH_BLOSC
    TEST(DecompressBytesTest, Blosc) {
        BloscCompressor<char> compressor(metadataWith(types::blosc, {{"codec", std::string("lz4")}, {"level", 5},
                                                                     {"shuffle", 1}, {"blocksize", 0}}));
        checkRoundTrip(compressor, "blosc");
        checkTruncated(compressor, "blosc");
    }
#endif

#ifdef WITH_BZIP2
    TEST(DecompressBytesTest, Bzip2) {
        Bzip2Compressor<char> compressor(metadataWith(types::bzip2, {{"level", 5}}));
        checkRoundTrip(compressor, "bzip2");
        checkTruncated(compressor, "bzip2");
    }
#endif

#ifdef WITH_XZ
    TEST(DecompressBytesTest, Xz) {
        XzCompressor<char> compressor(metadataWith(types::xz, {{"level", 6}}));
        checkRoundTrip(compressor, "xz");
        checkTruncated(compressor, "xz");
    }
#endif

#ifdef WITH_LZ4
    TEST(DecompressBytesTest, Lz4) {
        // zarr v3 (numcodecs framing) records the size
        Lz4Compressor<char> compressor(metadataWith(types::lz4, {{"level", 1}}));
        checkRoundTrip(compressor, "lz4");
        checkTruncated(compressor, "lz4");

        // bare blocks (zarr v2) don't, so they are refused
        Lz4Compressor<char> bare(metadataWith(types::lz4, {{"level", 1}}, 2));
        std::vector<char> in(100, 'a'), compressed, out;
        bare.compress(in.data(), compressed, in.size());
        ASSERT_THROW(bare.decompressBytes(compressed.data(), compressed.size(), out), std::runtime_error);
    }
#endif

    // crc32c stages around the compressor are verified and stripped
    TEST(DecompressBytesTest, Crc32cChain) {
#ifdef WITH_ZLIB
        const auto metadata = metadataWith(types::zlib, {{"level", 5}, {"useZlib", false}});
        for(const auto & counts : std::vector<std::pair<unsigned, unsigned>>{{0, 1}, {1, 0}, {2, 1}}) {
            Crc32cChain<char> chain(std::make_unique<ZlibCompressor<char>>(metadata),
                                    std::make_unique<ZlibCompressor<char>>(metadata),
                                    counts.first, counts.second);
            checkRoundTrip(chain, "crc32c chain");
            std::vector<char> in(1000, 'q'), compressed, out;
            chain.compress(in.data(), compressed, in.size());
            compressed[compressed.size() / 2] ^= 1;
            ASSERT_THROW(chain.decompressBytes(compressed.data(), compressed.size(), out), std::runtime_error);
        }
#endif
        Crc32cChain<char> raw(std::make_unique<RawCompressor<char>>(), nullptr, 0, 2);
        checkRoundTrip(raw, "crc32c raw");
    }

} // namespace compression


namespace util {

    TEST(VlenTest, RoundTrip) {
        const std::vector<std::string> items{"a", "", "h\xc3\xa9llo", std::string(100000, 'x'),
                                             std::string("nul\0inside", 10)};
        std::vector<char> encoded;
        encodeVlen(items.data(), items.size(), encoded);
        std::vector<std::string> decoded(items.size());
        decodeVlen(encoded.data(), encoded.size(), decoded.data(), decoded.size());
        ASSERT_EQ(decoded, items);
    }

    // the framing numcodecs writes: count, then length + bytes per item (LE)
    TEST(VlenTest, Layout) {
        const std::vector<std::string> items{"a", "bc"};
        std::vector<char> encoded;
        encodeVlen(items.data(), items.size(), encoded);
        const std::vector<char> expected{2, 0, 0, 0, 1, 0, 0, 0, 'a', 2, 0, 0, 0, 'b', 'c'};
        ASSERT_EQ(encoded, expected);
    }

    TEST(VlenTest, Corrupt) {
        const std::vector<std::string> items{"ab", "cde"};
        std::vector<char> encoded;
        encodeVlen(items.data(), items.size(), encoded);
        std::vector<std::string> out(2);
        // wrong item count
        std::vector<std::string> three(3);
        ASSERT_THROW(decodeVlen(encoded.data(), encoded.size(), three.data(), 3), std::runtime_error);
        // truncated at every possible length
        for(std::size_t n = 0; n < encoded.size(); ++n) {
            ASSERT_THROW(decodeVlen(encoded.data(), n, out.data(), 2), std::runtime_error) << n;
        }
        // trailing bytes
        encoded.push_back('x');
        ASSERT_THROW(decodeVlen(encoded.data(), encoded.size(), out.data(), 2), std::runtime_error);
        // an item length past the end
        encoded.pop_back();
        encoded[4] = 100;
        ASSERT_THROW(decodeVlen(encoded.data(), encoded.size(), out.data(), 2), std::runtime_error);
    }

} // namespace util
} // namespace z5
