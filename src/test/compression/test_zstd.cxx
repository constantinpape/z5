#include "gtest/gtest.h"

#include "z5/compression/zstd_compressor.hxx"
#include "z5/metadata.hxx"

#include "test_helper.hxx"

namespace z5 {
namespace compression {


    TEST_F(CompressionTest, ZstdCompressInt) {
        DatasetMetadata metadata;
        metadata.compressor = types::zstd;
        metadata.compressionOptions["level"] = 5;
        ZstdCompressor<int> compressor(metadata);

        std::vector<char> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);
        ASSERT_TRUE(dataOut.size() / sizeof(int) < SIZE);
    }


    TEST_F(CompressionTest, ZstdCompressFloat) {
        DatasetMetadata metadata;
        metadata.compressor = types::zstd;
        metadata.compressionOptions["level"] = 5;
        ZstdCompressor<float> compressor(metadata);

        std::vector<char> dataOut;
        compressor.compress(dataFloat_, dataOut, SIZE);
        ASSERT_TRUE(dataOut.size() / sizeof(float) < SIZE);
    }


    TEST_F(CompressionTest, ZstdDecompressInt) {
        DatasetMetadata metadata;
        metadata.compressor = types::zstd;
        metadata.compressionOptions["level"] = 5;
        ZstdCompressor<int> compressor(metadata);

        std::vector<char> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);
        ASSERT_TRUE(dataOut.size() / sizeof(int) < SIZE);

        int dataTmp[SIZE];
        compressor.decompress(dataOut, dataTmp, SIZE);
        for(std::size_t i = 0; i < SIZE; ++i) {
            ASSERT_EQ(dataTmp[i], dataInt_[i]);
        }
    }


    TEST_F(CompressionTest, ZstdDecompressFloat) {
        DatasetMetadata metadata;
        metadata.compressor = types::zstd;
        metadata.compressionOptions["level"] = 5;
        ZstdCompressor<float> compressor(metadata);

        std::vector<char> dataOut;
        compressor.compress(dataFloat_, dataOut, SIZE);
        ASSERT_TRUE(dataOut.size() / sizeof(float) < SIZE);

        float dataTmp[SIZE];
        compressor.decompress(dataOut, dataTmp, SIZE);
        for(std::size_t i = 0; i < SIZE; ++i) {
            ASSERT_EQ(dataTmp[i], dataFloat_[i]);
        }
    }

}
}
