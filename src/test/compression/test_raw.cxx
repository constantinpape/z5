#include "gtest/gtest.h"

#include "z5/compression/raw_compressor.hxx"
#include "z5/metadata.hxx"

#include "test_helper.hxx"


namespace z5 {
namespace compression {


    TEST_F(CompressionTest, RawCompressInt) {
        RawCompressor<int> compressor;
        std::vector<int> dataOut;
        ASSERT_THROW(compressor.compress(dataInt_, dataOut, SIZE), std::runtime_error);
    }


    TEST_F(CompressionTest, RawCompressFloat) {
        RawCompressor<float> compressor;
        std::vector<float> dataOut;
        ASSERT_THROW(compressor.compress(dataFloat_, dataOut, SIZE), std::runtime_error);
    }


    TEST_F(CompressionTest, RawDecompressInt) {
        RawCompressor<int> compressor;
        std::vector<int> dataOut;
        int dataTmp[SIZE];
        ASSERT_THROW(compressor.decompress(dataOut, dataTmp, SIZE), std::runtime_error);
    }


    TEST_F(CompressionTest, RawDecompressFloat) {
        RawCompressor<float> compressor;
        std::vector<float> dataOut;
        float dataTmp[SIZE];
        ASSERT_THROW(compressor.decompress(dataOut, dataTmp, SIZE), std::runtime_error);
    }

}
}
