#include "gtest/gtest.h"

#include "z5/dataset_factory.hxx"

namespace z5 {

    TEST(testPython, testRead) {
        // test array with default blosc compression
        {
            auto array = openDataset("array.zr");
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


        // test array with no compression
        {
            auto array = openDataset("array_raw.zr");
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
    }


    TEST(testPython, testReadFillvalue) {
        auto array = openDataset("array_fv.zr");
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
        {
            types::CompressionOptions cOpts;
            cOpts["level"] = 5;
            cOpts["shuffle"] = 1;
            std::string codec = "lz4";
            cOpts["codec"] = codec;
            auto array = createDataset("array1.zr", 
                                       "float64", 
                                       types::ShapeType({100, 100, 100}),
                                       types::ShapeType({10, 10, 10}),
                                       true,
                                       "blosc",
                                       cOpts);
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
        
        {
            auto array = createDataset("array1_raw.zr", 
                                       "float64", 
                                       types::ShapeType({100, 100, 100}),
                                       types::ShapeType({10, 10, 10}),
                                       true,
                                       "raw");
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
}
