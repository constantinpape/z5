#include "gtest/gtest.h"

#include <random>

#include "z5/factory.hxx"
#include "z5/multiarray/broadcast.hxx"
#include "z5/multiarray/array_access.hxx"

#include "array_helper.hxx"

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
            types::ShapeType shape(array->shape());
            const T val = 42;

            // write scalar and load for completely overlapping array consisting of 8 chunks
            {
                types::ShapeType offset({0, 0, 0});
                types::ShapeType subShape({20, 20, 20});
                writeScalar(array, offset.begin(), subShape.begin(), val);
                TestArray<T> data(subShape);
                readSubarray<T>(array, data.view(), offset.begin());

                for(const auto elem : data.data) {
                    ASSERT_EQ(elem, val);
                }
            }

            // load the complete array
            {
                types::ShapeType offset({0, 0, 0});
                writeScalar(array, offset.begin(), shape.begin(), val);
                TestArray<T> data(shape);
                readSubarray<T>(array, data.view(), offset.begin());

                for(const auto elem : data.data) {
                    ASSERT_EQ(elem, val);
                }
            }

            // write and load 25 random valid sub-arrays
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
                types::ShapeType subShape({sx, sy, sz});

                writeScalar(array, offset.begin(), subShape.begin(), val);
                TestArray<T> data(subShape);
                readSubarray<T>(array, data.view(), offset.begin());

                for(const auto elem : data.data) {
                    ASSERT_EQ(elem, val);
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
        auto array = openDataset(fZarr, "int_regular");
        testBroadcast<int32_t>(array);
        auto arrayN5 = openDataset(fN5, "int_regular");
        testBroadcast<int32_t>(arrayN5);
    }


    TEST_F(BroadcastTest, TestBroadcastFloatRegular) {
        auto array = openDataset(fZarr, "float_regular");
        testBroadcast<float>(array);
        auto arrayN5 = openDataset(fN5, "float_regular");
        testBroadcast<float>(arrayN5);
    }


    TEST_F(BroadcastTest, TestBroadcastIntIrregular) {
        auto array = openDataset(fZarr, "int_irregular");
        testBroadcast<int32_t>(array);
        auto arrayN5 = openDataset(fN5, "int_irregular");
        testBroadcast<int32_t>(arrayN5);
    }


    TEST_F(BroadcastTest, TestBroadcastFloatIrregular) {
        auto array = openDataset(fZarr, "float_irregular");
        testBroadcast<float>(array);
        auto arrayN5 = openDataset(fN5, "float_irregular");
        testBroadcast<float>(arrayN5);
    }


}
}
