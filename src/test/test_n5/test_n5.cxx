#include "gtest/gtest.h"

#include "z5/dataset_factory.hxx"

namespace z5 {

    TEST(testN5, testRead) {
        auto array = openDataset("array.n5");
        auto chunks = array->chunksPerDimension();

        // TODO asymmetric shapes
        ASSERT_EQ(array->maxChunkSize(), 1000);
        std::vector<double> dataOut(array->maxChunkSize());

        ASSERT_EQ(array->dimension(), 3);
        for(unsigned d = 0; d < array->dimension(); ++d) {
            ASSERT_EQ(array->shape(d), 100);
            ASSERT_EQ(array->maxChunkShape(d), 10);
        }

        for(size_t z = 0; z < chunks[0]; ++z) {
            for(size_t y = 0; y < chunks[1]; ++y) {
                for(size_t x = 0; x < chunks[2]; ++x) {

                    std::fill(dataOut.begin(), dataOut.end(), 0);
                    types::ShapeType chunk({z, y, x});

                    // read chunk shape and make sure it agrees
                    types::ShapeType cShape;
                    array->getChunkShape(chunk, cShape);
                    ASSERT_EQ(cShape.size(), 3);
                    for(int i = 0; i < 3; ++i) {
                        std::cout << cShape[i] << std::endl;
                        ASSERT_EQ(cShape[i], 10);
                    }

                    // read values and make sure they agree
                    array->readChunk(chunk, &dataOut[0]);
                    ASSERT_EQ(dataOut.size(), array->maxChunkSize());
                    for(size_t i = 0; i < dataOut.size(); i++) {
                        ASSERT_EQ(dataOut[i], 42);
                    }

                }
            }
        }
    }


    /*
    TEST(testN5, testReadFillvalue) {
        auto array = openZarrArray("array_fv.n5");
        auto chunks = array->chunksPerDimension();
        std::vector<double> dataOut(array->maxChunkSize());

        ASSERT_EQ(array->dimension(), 3);
        for(unsigned d = 0; d < array->dimension(); ++d) {
            ASSERT_EQ(array->shape(d), 100);
            ASSERT_EQ(array->maxChunkShape(d), 10);
        }

        for(size_t z = 0; z < chunks[0]; ++z) {
            for(size_t y = 0; y < chunks[1]; ++y) {
                for(size_t x = 0; x < chunks[2]; ++x) {
                    std::fill(dataOut.begin(), dataOut.end(), 0);
                    array->readChunk(types::ShapeType({z, y, x}), &dataOut[0]);
                    ASSERT_EQ(dataOut.size(), array->maxChunkSize());
                    for(size_t i = 0; i < dataOut.size(); i++) {
                        ASSERT_EQ(dataOut[i], 42);
                    }
                }
            }
        }
    }


    TEST(testN5, testWrite) {
        auto array = createZarrArray(
            "array1.n5", "float64", types::ShapeType({100, 100, 100}), types::ShapeType({10, 10, 10}), false
        );
        auto chunks = array->chunksPerDimension();
        std::vector<double> data(array->maxChunkSize(), 42.);

        for(size_t z = 0; z < chunks[0]; ++z) {
            for(size_t y = 0; y < chunks[1]; ++y) {
                for(size_t x = 0; x < chunks[2]; ++x) {
                    array->writeChunk(types::ShapeType({z, y, x}),&data[0]);
                }
            }
        }
    }
    */
}
