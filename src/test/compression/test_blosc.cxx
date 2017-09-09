#include "gtest/gtest.h"

#include <random>

#include "zarr++/compression/blosc_compressor.hxx"
#include "zarr++/metadata.hxx"

#include "test_helper.hxx"


namespace zarr {
namespace compression {


    TEST_F(CompressionTest, BloscCompressInt) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorId = "blosc";
        metadata.compressorName = "lz4";
        metadata.compressorLevel = 5;
        metadata.compressorShuffle = 1;
        BloscCompressor<int> compressor(metadata);

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, size_);

        ASSERT_TRUE(dataOut.size() < size_);
        std::cout << "Compression Int: " << dataOut.size() << " / " << size_ << std::endl;

    }


    TEST_F(CompressionTest, BloscCompressFloat) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorId = "blosc";
        metadata.compressorName = "lz4";
        metadata.compressorLevel = 5;
        metadata.compressorShuffle = 1;
        BloscCompressor<float> compressor(metadata);

        std::vector<float> dataOut;
        compressor.compress(dataFloat_, dataOut, size_);

        ASSERT_TRUE(dataOut.size() < size_);
        std::cout << "Compression Float: " << dataOut.size() << " / " << size_ << std::endl;

    }


    TEST_F(CompressionTest, BloscDecompressInt) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorId = "blosc";
        metadata.compressorName = "lz4";
        metadata.compressorLevel = 5;
        metadata.compressorShuffle = 1;
        BloscCompressor<int> compressor(metadata);

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, size_);
        ASSERT_TRUE(dataOut.size() < size_);

        int dataTmp[size_];
        compressor.decompress(dataOut, dataTmp, size_);
        for(size_t i = 0; i < size_; ++i) {
            ASSERT_EQ(dataTmp[i], dataInt_[i]);
        }
    }


    TEST_F(CompressionTest, BloscCompressDecompressFloat) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorId = "blosc";
        metadata.compressorName = "lz4";
        metadata.compressorLevel = 5;
        metadata.compressorShuffle = 1;
        BloscCompressor<float> compressor(metadata);

        std::vector<float> dataOut;
        compressor.compress(dataFloat_, dataOut, size_);
        ASSERT_TRUE(dataOut.size() < size_);

        float dataTmp[size_];
        compressor.decompress(dataOut, dataTmp, size_);
        for(size_t i = 0; i < size_; ++i) {
            ASSERT_EQ(dataTmp[i], dataFloat_[i]);
        }

    }

}
}
