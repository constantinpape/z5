#include "gtest/gtest.h"

#include "zarr++/compression/raw_compressor.hxx"
#include "zarr++/metadata.hxx"

#include "test_helper.hxx"


namespace zarr {
namespace compression {


    TEST_F(CompressionTest, RawCompressInt) {

        RawCompressor<int> compressor;

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, size_);

        ASSERT_EQ(dataOut.size(), size_);
    }


    TEST_F(CompressionTest, RawCompressFloat) {

        RawCompressor<float> compressor;

        std::vector<float> dataOut;
        compressor.compress(dataFloat_, dataOut, size_);

        ASSERT_EQ(dataOut.size(), size_);
    }


    TEST_F(CompressionTest, RawDecompressInt) {

        RawCompressor<int> compressor;

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, size_);
        ASSERT_EQ(dataOut.size(), size_);

        int dataTmp[size_];
        compressor.decompress(dataOut, dataTmp, size_);
        for(size_t i = 0; i < size_; ++i) {
            ASSERT_EQ(dataTmp[i], dataInt_[i]);
        }
    }


    TEST_F(CompressionTest, RawCompressDecompressFloat) {

        RawCompressor<float> compressor;

        std::vector<float> dataOut;
        compressor.compress(dataFloat_, dataOut, size_);
        ASSERT_EQ(dataOut.size(), size_);

        float dataTmp[size_];
        compressor.decompress(dataOut, dataTmp, size_);
        for(size_t i = 0; i < size_; ++i) {
            ASSERT_EQ(dataTmp[i], dataFloat_[i]);
        }
    }

}
}
