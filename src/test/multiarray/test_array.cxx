#include "gtest/gtest.h"

#include <random>

#include "z5/factory.hxx"
#include "z5/multiarray/array_access.hxx"

#include "array_helper.hxx"

namespace z5 {
namespace multiarray {

    // fixture for the array IO test
    class ArrayTest : public ::testing::Test {

    protected:
        ArrayTest() : fZarr(fs::path("data.zr")), fN5(fs::path("data.n5")),
                      shape_({100, 100, 100}), chunkShapeRegular_({10, 10, 10}), chunkShapeIrregular_({23, 17, 11})
        {
        }

        ~ArrayTest() {
        }

        template<class T>
        void writeData(const std::unique_ptr<Dataset> & ds) {
            const auto & chunks = ds->chunksPerDimension();
            std::vector<T> data(ds->defaultChunkSize(), 42);
            for(std::size_t z = 0; z < chunks[0]; ++z) {
                for(std::size_t y = 0; y < chunks[1]; ++y) {
                    for(std::size_t x = 0; x < chunks[2]; ++x) {
                        ds->writeChunk(types::ShapeType({z, y, x}), &data[0]);
                    }
                }
            }
        }

        virtual void SetUp() {
            {
                filesystem::createFile(fZarr, true);
                auto intReg = createDataset(fZarr, "int_regular", "int32", shape_, chunkShapeRegular_, "raw");
                writeData<int>(intReg);
                auto intIrreg = createDataset(fZarr, "int_irregular", "int32", shape_, chunkShapeIrregular_, "raw");
                writeData<int>(intIrreg);
                auto floatReg = createDataset(fZarr, "float_regular", "float32", shape_, chunkShapeRegular_, "raw");
                writeData<float>(floatReg);
                auto floatIrreg = createDataset(fZarr, "float_irregular", "float32", shape_, chunkShapeIrregular_, "raw");
                writeData<float>(floatIrreg);
            }
            {
                filesystem::createFile(fN5, true);
                auto intReg = createDataset(fN5, "int_regular", "int32", shape_, chunkShapeRegular_, "raw");
                writeData<int>(intReg);
                auto intIrreg = createDataset(fN5, "int_irregular", "int32", shape_, chunkShapeIrregular_, "raw");
                writeData<int>(intIrreg);
                auto floatReg = createDataset(fN5, "float_regular", "float32", shape_, chunkShapeRegular_, "raw");
                writeData<float>(floatReg);
                auto floatIrreg = createDataset(fN5, "float_irregular", "float32", shape_, chunkShapeIrregular_, "raw");
                writeData<float>(floatIrreg);
            }
        }

        virtual void TearDown() {
            fs::remove_all(fZarr.path());
            fs::remove_all(fN5.path());
        }

        template<typename T>
        void testArrayRead(std::unique_ptr<Dataset> & array) {
            const auto & shape = array->shape();

            // load a completely overlapping array consisting of 8 chunks
            {
                types::ShapeType offset({0, 0, 0});
                TestArray<T> data(types::ShapeType({20, 20, 20}));
                readSubarray<T>(array, data.view(), offset.begin());

                for(int i = 0; i < 20; ++i) {
                    for(int j = 0; j < 20; ++j) {
                        for(int k = 0; k < 20; ++k) {
                            ASSERT_EQ(data(i, j, k), 42);
                        }
                    }
                }
            }

            // load the complete array
            {
                types::ShapeType offset({0, 0, 0});
                TestArray<T> data(shape);
                readSubarray<T>(array, data.view(), offset.begin());

                for(const auto elem : data.data) {
                    ASSERT_EQ(elem, 42);
                }
            }

            // load 25 random valid sub-arrays and make sure they contain the correct data
            std::default_random_engine gen;
            std::uniform_int_distribution<std::size_t> xx(0, shape[0] - 2);
            std::uniform_int_distribution<std::size_t> yy(0, shape[1] - 2);
            std::uniform_int_distribution<std::size_t> zz(0, shape[2] - 2);
            std::size_t N = 25;
            std::size_t x, y, z;
            std::size_t sx, sy, sz;
            for(int t = 0; t < N; ++t) {

                x = xx(gen);
                y = yy(gen);
                z = zz(gen);
                types::ShapeType offset({x, y, z});

                std::uniform_int_distribution<std::size_t> shape_xx(1, shape[0] - x);
                std::uniform_int_distribution<std::size_t> shape_yy(1, shape[1] - y);
                std::uniform_int_distribution<std::size_t> shape_zz(1, shape[2] - z);
                sx = shape_xx(gen);
                sy = shape_yy(gen);
                sz = shape_zz(gen);
                TestArray<T> data(types::ShapeType({sx, sy, sz}));
                readSubarray<T>(array, data.view(), offset.begin());

                for(const auto elem : data.data) {
                    ASSERT_EQ(elem, 42);
                }
            }
        }


        template<typename T, typename DISTR>
        void testArrayWriteRead(std::unique_ptr<Dataset> & array, DISTR & distr) {

            const auto & shape = array->shape();

            std::default_random_engine gen;
            auto draw = std::bind(distr, gen);

            // write and read a completely overlapping array consisting of 8 chunks
            {
                types::ShapeType offset({0, 0, 0});
                TestArray<T> dataIn(types::ShapeType({20, 20, 20}));
                for(auto & v : dataIn.data) {
                    v = draw();
                }
                writeSubarray<T>(array, dataIn.cview(), offset.begin());

                TestArray<T> dataOut(types::ShapeType({20, 20, 20}));
                readSubarray<T>(array, dataOut.view(), offset.begin());
                ASSERT_EQ(dataIn.data, dataOut.data);
            }

            // write and read the complete array
            {
                types::ShapeType offset({0, 0, 0});
                TestArray<T> dataIn(shape);
                for(auto & v : dataIn.data) {
                    v = draw();
                }
                writeSubarray<T>(array, dataIn.cview(), offset.begin());

                TestArray<T> dataOut(shape);
                readSubarray<T>(array, dataOut.view(), offset.begin());
                ASSERT_EQ(dataIn.data, dataOut.data);
            }

            // write and read 25 random valid sub-arrays
            std::uniform_int_distribution<std::size_t> xx(0, shape[0] - 2);
            std::uniform_int_distribution<std::size_t> yy(0, shape[1] - 2);
            std::uniform_int_distribution<std::size_t> zz(0, shape[2] - 2);
            std::size_t N = 25;
            std::size_t x, y, z;
            std::size_t sx, sy, sz;
            for(int t = 0; t < N; ++t) {

                x = xx(gen);
                y = yy(gen);
                z = zz(gen);
                types::ShapeType offset({x, y, z});

                std::uniform_int_distribution<std::size_t> shape_xx(1, shape[0] - x);
                std::uniform_int_distribution<std::size_t> shape_yy(1, shape[1] - y);
                std::uniform_int_distribution<std::size_t> shape_zz(1, shape[2] - z);
                sx = shape_xx(gen);
                sy = shape_yy(gen);
                sz = shape_zz(gen);

                TestArray<T> dataIn(types::ShapeType({sx, sy, sz}));
                for(auto & v : dataIn.data) {
                    v = draw();
                }
                writeSubarray<T>(array, dataIn.cview(), offset.begin());

                TestArray<T> dataOut(types::ShapeType({sx, sy, sz}));
                readSubarray<T>(array, dataOut.view(), offset.begin());
                ASSERT_EQ(dataIn.data, dataOut.data);
            }
        }

        z5::filesystem::handle::File fZarr;
        z5::filesystem::handle::File fN5;

        types::ShapeType shape_;
        types::ShapeType chunkShapeRegular_;
        types::ShapeType chunkShapeIrregular_;
    };


    TEST_F(ArrayTest, TestThrow) {
        auto array = openDataset(fZarr, "int_regular");

        // check for shape throws #0
        TestArray<int32_t> sub0(types::ShapeType({120, 120, 120}));
        types::ShapeType offset0({0, 0, 0});
        ASSERT_THROW(readSubarray<int32_t>(array, sub0.view(), offset0.begin()), std::runtime_error);

        // check for shape throws #1
        TestArray<int32_t> sub1(types::ShapeType({80, 80, 80}));
        types::ShapeType offset1({30, 30, 30});
        ASSERT_THROW(readSubarray<int32_t>(array, sub1.view(), offset1.begin()), std::runtime_error);

        // check for shape throws #2
        TestArray<int32_t> sub2(types::ShapeType({80, 80, 0}));
        types::ShapeType offset2({0, 0, 0});
        ASSERT_THROW(readSubarray<int32_t>(array, sub2.view(), offset2.begin()), std::runtime_error);

        types::ShapeType shape({80, 80, 80});
        types::ShapeType offset({0, 0, 0});
        // check for dtype throws #0
        TestArray<int64_t> sub64(shape);
        ASSERT_THROW(readSubarray<int64_t>(array, sub64.view(), offset.begin()), std::runtime_error);

        // check for dtype throws #1
        TestArray<uint32_t> subu(shape);
        ASSERT_THROW(readSubarray<uint32_t>(array, subu.view(), offset.begin()), std::runtime_error);

        // check for dtype throws #2
        TestArray<float> subf(shape);
        ASSERT_THROW(readSubarray<float>(array, subf.view(), offset.begin()), std::runtime_error);
    }

    TEST_F(ArrayTest, TestReadIntRegular) {
        auto array = openDataset(fZarr, "int_regular");
        testArrayRead<int32_t>(array);
        auto arrayN5 = openDataset(fN5, "int_regular");
        testArrayRead<int32_t>(arrayN5);
    }


    TEST_F(ArrayTest, TestReadFloatRegular) {
        auto array = openDataset(fZarr, "float_regular");
        testArrayRead<float>(array);
        auto arrayN5 = openDataset(fN5, "float_regular");
        testArrayRead<float>(arrayN5);
    }


    TEST_F(ArrayTest, TestReadIntIrregular) {
        auto array = openDataset(fZarr, "int_irregular");
        testArrayRead<int32_t>(array);
        auto arrayN5 = openDataset(fN5, "int_irregular");
        testArrayRead<int32_t>(arrayN5);
    }


    TEST_F(ArrayTest, TestReadFloatIrregular) {
        auto array = openDataset(fZarr, "float_irregular");
        testArrayRead<float>(array);
        auto arrayN5 = openDataset(fN5, "float_irregular");
        testArrayRead<float>(arrayN5);
    }


    TEST_F(ArrayTest, TestWriteReadIntRegular) {
        auto array = openDataset(fZarr, "int_regular");
        std::uniform_int_distribution<int32_t> distr(-100, 100);
        testArrayWriteRead<int32_t>(array, distr);

        auto arrayN5 = openDataset(fN5, "int_regular");
        testArrayWriteRead<int32_t>(arrayN5, distr);
    }


    TEST_F(ArrayTest, TestWriteReadFloatRegular) {
        std::uniform_real_distribution<float> distr(0., 1.);
        auto array = openDataset(fZarr, "float_regular");
        testArrayWriteRead<float>(array, distr);

        auto arrayN5 = openDataset(fN5, "float_regular");
        testArrayWriteRead<float>(arrayN5, distr);
    }


    TEST_F(ArrayTest, TestWriteReadIntIrregular) {
        auto array = openDataset(fZarr, "int_irregular");
        std::uniform_int_distribution<int32_t> distr(-100, 100);
        testArrayWriteRead<int32_t>(array, distr);

        auto arrayN5 = openDataset(fN5, "int_irregular");
        testArrayWriteRead<int32_t>(arrayN5, distr);
    }


    TEST_F(ArrayTest, TestWriteReadFloatIrregular) {
        auto array = openDataset(fZarr, "float_irregular");
        std::uniform_real_distribution<float> distr(0., 1.);
        testArrayWriteRead<float>(array, distr);

        auto arrayN5 = openDataset(fN5, "float_irregular");
        testArrayWriteRead<float>(arrayN5, distr);
    }
}
}
