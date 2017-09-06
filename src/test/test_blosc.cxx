#include "gtest/gtest.h"

#include "zarr++/compression/blosc_compressor.hxx"
#include "zarr++/metadata.hxx"


namespace zarr {
namespace compression {

    // fixture for the blosc compressor
    class BloscTest : public ::testing::Test {

    protected:
        BloscTest() {

        }

        virtual ~BloscTest() {

        }

        virtual void SetUp() {

            std::default_random_engine generator;
            // fill 'dataInt_' with random values
            std::uniform_int_distribution<int> distributionInt(0, 1000);
            auto drawInt = std::bind(distributionInt, generator);
            for(size_t i = 0; i < size_; ++i) {
                dataInt_[i] = drawInt();
            }

            // fill 'dataFloat_' with random values
            std::uniform_real_distribution<float> distributionFloat(0., 1.);
            auto drawFloat = std::bind(distributionFloat, generator);
            for(size_t i = 0; i < size_; ++i) {
                dataFloat_[i] = drawFloat();
            }
        }

        virtual void TearDown() {

        }


        const static size_t size_ = 100*100*100;
        int dataInt_[size_];
        float dataFloat_[size_];

    };


    /*
    TEST_F(BloscTest, PureBlosc) {
        int dataOut[size_];
        int dataDest[size_];
        auto csize = blosc_compress(
            5, 1, sizeof(int), size_ * sizeof(int), dataInt_, dataOut, size_ * sizeof(int) + BLOSC_MAX_OVERHEAD
        );
        std::cout << csize / sizeof(int) << " / " << size_ << std::endl;
        ASSERT_TRUE((csize / sizeof(int)) < size_);
        auto dsize = blosc_decompress(
            dataOut, dataDest, size_ * sizeof(int)
        );
        for(size_t i = 0; i < size_; ++i) {
            ASSERT_EQ(dataDest[i], dataInt_[i]);
        }
    }
    */


    TEST_F(BloscTest, CompressInt) {

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


    TEST_F(BloscTest, CompressFloat) {

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


    TEST_F(BloscTest, CompressDecompressInt) {

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


    TEST_F(BloscTest, CompressDecompressFloat) {

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
