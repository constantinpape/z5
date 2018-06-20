#include "gtest/gtest.h"

#include <random>

#include "z5/compression/xz_compressor.hxx"
#include "z5/metadata.hxx"

#include "test_helper.hxx"


namespace z5 {
namespace compression {


    TEST_F(CompressionTest, XzCompressInt) {

        // Test compression with default values
        DatasetMetadata metadata;
        metadata.compressionOptions["level"] = 5;
        XzCompressor<int> compressor(metadata);

        std::vector<char> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);

        ASSERT_TRUE(dataOut.size() / sizeof(int) < SIZE);
        std::cout << "Compression Int: " << dataOut.size() / sizeof(int) << " / " << SIZE << std::endl;
    }


    TEST_F(CompressionTest, XzCompressFloat) {

        // Test compression with default values
        DatasetMetadata metadata;
        metadata.compressionOptions["level"] = 5;
        XzCompressor<float> compressor(metadata);

        std::vector<char> dataOut;
        compressor.compress(dataFloat_, dataOut, SIZE);

        ASSERT_TRUE(dataOut.size() / sizeof(float) < SIZE);
        std::cout << "Compression Float: " << dataOut.size() / sizeof(float) << " / " << SIZE << std::endl;
    }


    TEST_F(CompressionTest, XzDecompressInt) {

        // Test compression with default values
        DatasetMetadata metadata;
        metadata.compressionOptions["level"] = 5;
        XzCompressor<int> compressor(metadata);

        std::vector<char> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);
        ASSERT_TRUE(dataOut.size() / sizeof(int) < SIZE);

        int dataTmp[SIZE];
        compressor.decompress(dataOut, dataTmp, SIZE);
        for(std::size_t i = 0; i < SIZE; ++i) {
            ASSERT_EQ(dataTmp[i], dataInt_[i]);
        }
    }


    TEST_F(CompressionTest, XzDecompressFloat) {

        // Test compression with default values
        DatasetMetadata metadata;
        metadata.compressionOptions["level"] = 5;
        XzCompressor<float> compressor(metadata);

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
