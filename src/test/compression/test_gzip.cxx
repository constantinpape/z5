#include "gtest/gtest.h"

#include <random>

#include "zarr++/compression/gzip_compressor.hxx"
#include "zarr++/metadata.hxx"

#include "test_helper.hxx"

namespace zarr {
namespace compression {


    TEST_F(CompressionTest, GzipCompressInt) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorId = "gzip";
        metadata.compressorName = "gzip";
        metadata.compressorLevel = 5;
        GzipCompressor<int> compressor(metadata);

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);

        ASSERT_TRUE(dataOut.size() < SIZE);
        std::cout << "Compression Int: " << dataOut.size() << " / " << SIZE << std::endl;

    }


    TEST_F(CompressionTest, GzipCompressFloat) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorId = "gzip";
        metadata.compressorLevel = 5;
        GzipCompressor<float> compressor(metadata);

        std::vector<float> dataOut;
        compressor.compress(dataFloat_, dataOut, SIZE);

        ASSERT_TRUE(dataOut.size() < SIZE);
        std::cout << "Compression Float: " << dataOut.size() << " / " << SIZE << std::endl;

    }


    TEST_F(CompressionTest, GzipDecompressInt) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorId = "gzip";
        metadata.compressorLevel = 5;
        GzipCompressor<int> compressor(metadata);

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);
        ASSERT_TRUE(dataOut.size() < SIZE);

        int dataTmp[SIZE];
        compressor.decompress(dataOut, dataTmp, SIZE);
        for(size_t i = 0; i < SIZE; ++i) {
            ASSERT_EQ(dataTmp[i], dataInt_[i]);
        }
    }


    TEST_F(CompressionTest, GzipDecompressFloat) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorId = "gzip";
        metadata.compressorLevel = 5;
        GzipCompressor<float> compressor(metadata);

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
