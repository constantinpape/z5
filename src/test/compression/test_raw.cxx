#include "gtest/gtest.h"

#include "zarr++/compression/raw_compressor.hxx"
#include "zarr++/metadata.hxx"

#include "test_helper.hxx"


namespace zarr {
namespace compression {


    TEST_F(CompressionTest, RawCompressInt) {

        RawCompressor<int> compressor;

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);

        ASSERT_EQ(dataOut.size(), SIZE);
    }


    TEST_F(CompressionTest, RawCompressFloat) {

        RawCompressor<float> compressor;

        std::vector<float> dataOut;
        compressor.compress(dataFloat_, dataOut, SIZE);

        ASSERT_EQ(dataOut.size(), SIZE);
    }


    TEST_F(CompressionTest, RawDecompressInt) {

        RawCompressor<int> compressor;

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);
        ASSERT_EQ(dataOut.size(), SIZE);

        int dataTmp[SIZE];
        compressor.decompress(dataOut, dataTmp, SIZE);
        for(size_t i = 0; i < SIZE; ++i) {
            ASSERT_EQ(dataTmp[i], dataInt_[i]);
        }
    }


    TEST_F(CompressionTest, RawDecompressFloat) {

        RawCompressor<float> compressor;

        std::vector<float> dataOut;
        compressor.compress(dataFloat_, dataOut, SIZE);
        ASSERT_EQ(dataOut.size(), SIZE);

        float dataTmp[SIZE];
        compressor.decompress(dataOut, dataTmp, SIZE);
        for(size_t i = 0; i < SIZE; ++i) {
            ASSERT_EQ(dataTmp[i], dataFloat_[i]);
        }
    }

}
}
