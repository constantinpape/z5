#pragma once

#include "gtest/gtest.h"

#include <random>

#define SIZE 100*100*100

namespace z5 {
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
            for(std::size_t i = 0; i < SIZE; ++i) {
                dataInt_[i] = drawInt();
            }

            // fill 'dataFloat_' with random values
            std::uniform_real_distribution<float> distributionFloat(0., 1.);
            auto drawFloat = std::bind(distributionFloat, generator);
            for(std::size_t i = 0; i < SIZE; ++i) {
                dataFloat_[i] = drawFloat();
            }
        }

        virtual void TearDown() {

        }


        int dataInt_[SIZE];
        float dataFloat_[SIZE];
        // TODO
        std::vector<std::string> bloscCompressors;
    };

}
}
