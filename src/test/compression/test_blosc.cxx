#include "gtest/gtest.h"

#include <random>

#include "z5/compression/blosc_compressor.hxx"
#include "z5/metadata.hxx"

#include "test_helper.hxx"


namespace z5 {
namespace compression {


    TEST_F(CompressionTest, BloscCompressInt) {

        // Test compression with default values
        DatasetMetadata metadata;
        metadata.compressor = types::blosc;
        metadata.compressionOptions["codec"] = "lz4";
        metadata.compressionOptions["level"] = 5;
        metadata.compressionOptions["shuffle"] = 1;
        BloscCompressor<int> compressor(metadata);

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);

        ASSERT_TRUE(dataOut.size() < SIZE);
        std::cout << "Compression Int: " << dataOut.size() << " / " << SIZE << std::endl;

    }


    TEST_F(CompressionTest, BloscCompressFloat) {

        // Test compression with default values
        DatasetMetadata metadata;
        metadata.compressor = types::blosc;
        metadata.compressionOptions["codec"] = "lz4";
        metadata.compressionOptions["level"] = 5;
        metadata.compressionOptions["shuffle"] = 1;
        BloscCompressor<float> compressor(metadata);

        std::vector<float> dataOut;
        compressor.compress(dataFloat_, dataOut, SIZE);

        ASSERT_TRUE(dataOut.size() < SIZE);
        std::cout << "Compression Float: " << dataOut.size() << " / " << SIZE << std::endl;

    }


    TEST_F(CompressionTest, BloscDecompressInt) {

        // Test compression with default values
        DatasetMetadata metadata;
        metadata.compressor = types::blosc;
        metadata.compressionOptions["codec"] = "lz4";
        metadata.compressionOptions["level"] = 5;
        metadata.compressionOptions["shuffle"] = 1;
        BloscCompressor<int> compressor(metadata);

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);
        ASSERT_TRUE(dataOut.size() < SIZE);

        int dataTmp[SIZE];
        compressor.decompress(dataOut, dataTmp, SIZE);
        for(size_t i = 0; i < SIZE; ++i) {
            ASSERT_EQ(dataTmp[i], dataInt_[i]);
        }
    }


    TEST_F(CompressionTest, BloscDecompressFloat) {

        // Test compression with default values
        DatasetMetadata metadata;
        metadata.compressor = types::blosc;
        metadata.compressionOptions["codec"] = "lz4";
        metadata.compressionOptions["level"] = 5;
        metadata.compressionOptions["shuffle"] = 1;
        BloscCompressor<float> compressor(metadata);

        std::vector<float> dataOut;
        compressor.compress(dataFloat_, dataOut, SIZE);
        ASSERT_TRUE(dataOut.size() < SIZE);

        float dataTmp[SIZE];
        compressor.decompress(dataOut, dataTmp, SIZE);
        for(size_t i = 0; i < SIZE; ++i) {
            ASSERT_EQ(dataTmp[i], dataFloat_[i]);
        }

    }

}
}
