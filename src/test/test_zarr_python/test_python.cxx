#include "gtest/gtest.h"

#include "zarr++/array_factory.hxx"

namespace zarr {

    TEST(testPython, testRead) {
        auto array = openZarrArray("array.zr");
        auto chunks = array->chunksPerDimension();

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
                    array->readChunk(chunk, &dataOut[0]);
                    ASSERT_EQ(dataOut.size(), array->maxChunkSize());
                    for(size_t i = 0; i < dataOut.size(); i++) {
                        ASSERT_EQ(dataOut[i], 42);
                    }

                }
            }
        }
    }


    TEST(testPython, testReadFillvalue) {
        auto array = openZarrArray("array_fv.zr");
        const auto & chunks = array->chunksPerDimension();
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


    TEST(testPython, testWrite) {
        auto array = createZarrArray(
            "array1.zr", "float64", types::ShapeType({100, 100, 100}), types::ShapeType({10, 10, 10}), true
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
}
