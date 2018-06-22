#include "gtest/gtest.h"

#include <random>
#include "xtensor/xarray.hpp"

#include "z5/dataset_factory.hxx"
#include "z5/multiarray/broadcast.hxx"

namespace fs = boost::filesystem;

namespace z5 {
namespace multiarray {

    // fixture for the broadcasting test
    class BroadcastTest : public ::testing::Test {

    protected:
        BroadcastTest() :
            pathIntRegular_("int_regular.zr"), pathIntIrregular_("int_irregular.zr"),
            pathFloatRegular_("float_regular.zr"), pathFloatIrregular_("float_irregular.zr"),
            pathIntRegularN5_("int_regular.n5"), pathIntIrregularN5_("int_irregular.n5"),
            pathFloatRegularN5_("float_regular.n5"), pathFloatIrregularN5_("float_irregular.n5"),
            shape_({100, 100, 100}), chunkShapeRegular_({10, 10, 10}), chunkShapeIrregular_({23, 17, 11})
        {
        }

        ~BroadcastTest() {
        }

        virtual void SetUp() {
            // create zarr arrays
            createDataset(pathIntRegular_, "int32", shape_, chunkShapeRegular_, true, "raw");
            createDataset(pathIntIrregular_, "int32", shape_, chunkShapeIrregular_, true, "raw");
            createDataset(pathFloatRegular_, "float32", shape_, chunkShapeRegular_, true, "raw");
            createDataset(pathFloatIrregular_, "float32", shape_, chunkShapeIrregular_, true, "raw");

            // create n5 arrays
            createDataset(pathIntRegularN5_, "int32", shape_, chunkShapeRegular_, false, "raw");
            createDataset(pathIntIrregularN5_, "int32", shape_, chunkShapeIrregular_, false, "raw");
            createDataset(pathFloatRegularN5_, "float32", shape_, chunkShapeRegular_, false, "raw");
            createDataset(pathFloatIrregularN5_, "float32", shape_, chunkShapeIrregular_, false, "raw");
        }

        virtual void TearDown() {
            // remove zarr arrays
            {
                // remove int arrays
                fs::path ireg(pathIntRegular_);
                fs::remove_all(ireg);
                fs::path iirreg(pathIntIrregular_);
                fs::remove_all(iirreg);
                // remove float arrays
                fs::path freg(pathFloatRegular_);
                fs::remove_all(freg);
                fs::path firreg(pathFloatIrregular_);
                fs::remove_all(firreg);
            }
            // remove n5 arrays
            {
                // remove int arrays
                fs::path ireg(pathIntRegularN5_);
                fs::remove_all(ireg);
                fs::path iirreg(pathIntIrregularN5_);
                fs::remove_all(iirreg);
                // remove float arrays
                fs::path freg(pathFloatRegularN5_);
                fs::remove_all(freg);
                fs::path firreg(pathFloatIrregularN5_);
                fs::remove_all(firreg);
            }
        }

        template<typename T>
        void testBroadcast(std::unique_ptr<Dataset> & array) {
            typedef typename xt::xarray<T>::shape_type ArrayShape;
            types::ShapeType shape(array->shape());
            ArrayShape arrayShape(shape.begin(), shape.end());
            const T val = 42;

            // write scalcar and load for completely overlapping array consisting of 8 chunks
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

        // zarr paths
        std::string pathIntRegular_;
        std::string pathIntIrregular_;
        std::string pathFloatRegular_;
        std::string pathFloatIrregular_;

        // n5 paths
        std::string pathIntRegularN5_;
        std::string pathIntIrregularN5_;
        std::string pathFloatRegularN5_;
        std::string pathFloatIrregularN5_;

        types::ShapeType shape_;
        types::ShapeType chunkShapeRegular_;
        types::ShapeType chunkShapeIrregular_;
    };


    TEST_F(BroadcastTest, TestBroadcastIntRegular) {
        // load the regular array and run the test
        auto array = openDataset(pathIntRegular_);
        testBroadcast<int32_t>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(pathIntRegularN5_);
        testBroadcast<int32_t>(arrayN5);
    }


    TEST_F(BroadcastTest, TestBroadcastFloatRegular) {
        // load the regular array and run the test
        auto array = openDataset(pathFloatRegular_);
        testBroadcast<float>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(pathFloatRegularN5_);
        testBroadcast<float>(arrayN5);
    }


    TEST_F(BroadcastTest, TestBroadcastIntIrregular) {
        //// load the regular array and run the test
        auto array = openDataset(pathIntIrregular_);
        testBroadcast<int32_t>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(pathIntIrregularN5_);
        testBroadcast<int32_t>(arrayN5);
    }


    TEST_F(BroadcastTest, TestBroadcastFloatIrregular) {
        //// load the regular array and run the test
        auto array = openDataset(pathFloatIrregular_);
        testBroadcast<float>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(pathFloatIrregularN5_);
        testBroadcast<float>(arrayN5);
    }


}
}
