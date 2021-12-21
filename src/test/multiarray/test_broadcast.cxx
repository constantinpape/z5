#include "gtest/gtest.h"

#include <random>
#include "xtensor/xarray.hpp"

#include "z5/factory.hxx"
#include "z5/multiarray/broadcast.hxx"

namespace z5 {
namespace multiarray {

    // fixture for the broadcasting test
    class BroadcastTest : public ::testing::Test {

    protected:
        BroadcastTest() : fZarr(fs::path("data.zr")), fN5(fs::path("data.n5")),
                          shape_({100, 100, 100}), chunkShapeRegular_({10, 10, 10}), chunkShapeIrregular_({23, 17, 11})
        {
        }

        ~BroadcastTest() {
        }

        virtual void SetUp() {
            filesystem::createFile(fZarr, true);
            createDataset(fZarr, "int_regular", "int32", shape_, chunkShapeRegular_, "raw");
            createDataset(fZarr, "int_irregular", "int32", shape_, chunkShapeIrregular_, "raw");
            createDataset(fZarr, "float_regular", "float32", shape_, chunkShapeRegular_, "raw");
            createDataset(fZarr, "float_irregular", "float32", shape_, chunkShapeIrregular_, "raw");

            filesystem::createFile(fN5, true);
            createDataset(fN5, "int_regular", "int32", shape_, chunkShapeRegular_, "raw");
            createDataset(fN5, "int_irregular", "int32", shape_, chunkShapeIrregular_, "raw");
            createDataset(fN5, "float_regular", "float32", shape_, chunkShapeRegular_, "raw");
            createDataset(fN5, "float_irregular", "float32", shape_, chunkShapeIrregular_, "raw");
        }

        virtual void TearDown() {
            fs::remove_all(fZarr.path());
            fs::remove_all(fN5.path());
        }

        template<typename T>
        void testBroadcast(std::unique_ptr<Dataset> & array) {
            typedef typename xt::xarray<T>::shape_type ArrayShape;
            types::ShapeType shape(array->shape());
            ArrayShape arrayShape(shape.begin(), shape.end());
            const T val = 42;

            // write scalar and load for completely overlapping array consisting of 8 chunks
            {
                ArrayShape offset({0, 0, 0});
                ArrayShape subShape({20, 20, 20});
                writeScalar(array, offset.begin(), subShape.begin(), val);
                xt::xarray<T> data(subShape);
                readSubarray<T>(array, data, offset.begin());

                for(int i = 0; i < subShape[0]; ++i) {
                    for(int j = 0; j < subShape[1]; ++j) {
                        for(int k = 0; k < subShape[2]; ++k) {
                            //std::cout << i << " " << j << " " << k << std::endl;
                            ASSERT_EQ(data(i, j, k), val);
                        }
                    }
                }
            }

            // load the complete array
            {
                ArrayShape offset({0, 0, 0});
                writeScalar(array, offset.begin(), arrayShape.begin(), val);
                xt::xarray<T> data(arrayShape);
                readSubarray<T>(array, data, offset.begin());

                for(int i = 0; i < shape[0]; ++i) {
                    for(int j = 0; j < shape[1]; ++j) {
                        for(int k = 0; k < shape[2]; ++k) {
                            //std::cout << i << " " << j << " " << k << std::endl;
                            ASSERT_EQ(data(i, j, k), val);
                        }
                    }
                }
            }

            // load 25 random valid chunks and make sure that they
            // contain the correct data
            std::default_random_engine gen;
            std::uniform_int_distribution<std::size_t> xx(0, shape[0] - 2);
            std::uniform_int_distribution<std::size_t> yy(0, shape[1] - 2);
            std::uniform_int_distribution<std::size_t> zz(0, shape[2] - 2);
            std::size_t N = 25;
            std::size_t x, y, z;
            std::size_t sx, sy, sz;
            for(int t = 0; t < N; ++t) {

                // draw the offset coordinates
                x = xx(gen);
                y = yy(gen);
                z = zz(gen);
                ArrayShape offset({x, y, z});

                // draw the shape coordinates
                std::uniform_int_distribution<std::size_t> shape_xx(1, shape[0] - x);
                std::uniform_int_distribution<std::size_t> shape_yy(1, shape[1] - y);
                std::uniform_int_distribution<std::size_t> shape_zz(1, shape[2] - z);
                sx = shape_xx(gen);
                sy = shape_yy(gen);
                sz = shape_zz(gen);
                ArrayShape subShape({sx, sy, sz});

                //std::cout << "Offset:" << std::endl;
                //std::cout << x << " " << y << " " << z << std::endl;
                //std::cout << "Shape:" << std::endl;
                //std::cout << sx << " " << sy << " " << sz << std::endl;

                writeScalar(array, offset.begin(), subShape.begin(), val);
                xt::xarray<T> data(subShape);
                readSubarray<T>(array, data, offset.begin());

                for(int i = 0; i < subShape[0]; ++i) {
                    for(int j = 0; j < subShape[1]; ++j) {
                        for(int k = 0; k < subShape[2]; ++k) {
                            ASSERT_EQ(data(i, j, k), val);
                        }
                    }
                }
            }
        }

        z5::filesystem::handle::File fZarr;
        z5::filesystem::handle::File fN5;

        types::ShapeType shape_;
        types::ShapeType chunkShapeRegular_;
        types::ShapeType chunkShapeIrregular_;
    };


    TEST_F(BroadcastTest, TestBroadcastIntRegular) {
        // load the regular array and run the test
        auto array = openDataset(fZarr, "int_regular");
        testBroadcast<int32_t>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(fN5, "int_regular");
        testBroadcast<int32_t>(arrayN5);
    }


    TEST_F(BroadcastTest, TestBroadcastFloatRegular) {
        // load the regular array and run the test
        auto array = openDataset(fZarr, "float_regular");
        testBroadcast<float>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(fN5, "float_regular");
        testBroadcast<float>(arrayN5);
    }


    TEST_F(BroadcastTest, TestBroadcastIntIrregular) {
        //// load the regular array and run the test
        auto array = openDataset(fZarr, "int_irregular");
        testBroadcast<int32_t>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(fN5, "int_irregular");
        testBroadcast<int32_t>(arrayN5);
    }


    TEST_F(BroadcastTest, TestBroadcastFloatIrregular) {
        //// load the regular array and run the test
        auto array = openDataset(fZarr, "float_irregular");
        testBroadcast<float>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(fN5, "float_irregular");
        testBroadcast<float>(arrayN5);
    }


}
}
