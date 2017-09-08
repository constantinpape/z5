#include "gtest/gtest.h"

#include "zarr++/array_factory.hxx"

namespace zarr {

    TEST(testPython, testRead) {
        auto array = openZarrArray("array.zr");
        auto chunks = array->chunksPerDimension();
        auto chunkShape = array->chunkShape();
        std::vector dataOut(array->cunkSize());

        for(int z = 0; z < chunks[0]; ++z) {
            for(int y = 0; y < chunks[1]; ++y) {
                for(int x = 0; x < chunks[2]; ++x) {
                    std::fill(dataOut.begin(), dataOut.end(), 0);
                    array->readChunk(
                        types::ShapeType({z * chunkShape[0], y * chunkShape[1], x * chunkShape[2]}),
                        &dataOut[0]
                    );
                    ASSERT_EQ(dataOut.size(), array->chunkSize());
                    for(size_t i = 0; i < dataOut.size(); i++) {
                        ASSERT_EQ(dataOut[i], 42);
                    }
                }
            }
        }
    }
}
