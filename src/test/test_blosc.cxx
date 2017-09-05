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
            auto drawInt = std::bind(distributioniInt, generator);
            for(size_t i = 0; i < size_; ++i) {
                dataInt_[i] = drawInt();
            }

            // fill 'dataFloat_' with random values
            std::uniform_real distributionFloat<float>(0., 1.);
            auto drawFloat = std::bind(distributionFloat, generator);
            for(size_t i = 0; i < size_; ++i) {
                data_[i] = drawFloat();
            }
        }

        virtual void TearDown() {

        }


        const static size_t size_ = 100*100*100;
        int dataInt[size_];
        float dataFloat[size_];

    };


    TEST_F(BloscTest, CompressInt) {

        // TODO metadata
        BloscCompressor compressor(metadata);
        compressor->compress();

    }


    TEST_F(BloscTest, CompressFloat) {

    }


    TEST_F(BloscTest, CompressDecompressInt) {

    }

    TEST_F(BloscTest, CompressDecompressFloat) {

    }

}
}
