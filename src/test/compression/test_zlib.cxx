#include "gtest/gtest.h"

#include <random>

#include "zarr++/compression/zlib_compressor.hxx"
#include "zarr++/metadata.hxx"

#include "test_helper.hxx"

namespace zarr {
namespace compression {


    TEST_F(CompressionTest, ZlibCompressInt) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorName = "zlib";
        metadata.compressorLevel = 5;
        ZlibCompressor<int> compressor(metadata);

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);

        ASSERT_TRUE(dataOut.size() < SIZE);
        std::cout << "Compression Int: " << dataOut.size() << " / " << SIZE << std::endl;

    }


    TEST_F(CompressionTest, ZlibCompressFloat) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorName = "zlib";
        metadata.compressorLevel = 5;
        ZlibCompressor<float> compressor(metadata);

        std::vector<float> dataOut;
        compressor.compress(dataFloat_, dataOut, SIZE);

        ASSERT_TRUE(dataOut.size() < SIZE);
        std::cout << "Compression Float: " << dataOut.size() << " / " << SIZE << std::endl;

    }


    TEST_F(CompressionTest, ZlibDecompressInt) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorName = "zlib";
        metadata.compressorLevel = 5;
        ZlibCompressor<int> compressor(metadata);

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);
        ASSERT_TRUE(dataOut.size() < SIZE);

        int dataTmp[SIZE];
        compressor.decompress(dataOut, dataTmp, SIZE);
        for(size_t i = 0; i < SIZE; ++i) {
            ASSERT_EQ(dataTmp[i], dataInt_[i]);
        }
    }


    TEST_F(CompressionTest, ZlibDecompressFloat) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorName = "zlib";
        metadata.compressorLevel = 5;
        ZlibCompressor<float> compressor(metadata);

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
