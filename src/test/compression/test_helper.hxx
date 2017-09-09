#include "gtest/gtest.h"

#include <random>


namespace zarr {
namespace compression {

    // fixture for the compression tests
    class CompressionTest : public ::testing::Test {

    protected:
        CompressionTest() {

        }

        virtual ~CompressionTest() {

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

}
}
