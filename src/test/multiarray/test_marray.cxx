#include "gtest/gtest.h"

#include <random>

#include "zarr++/array_factory.hxx"
#include "zarr++/multiarray/marray_access.hxx"

namespace fs = boost::filesystem;

namespace zarr {
namespace multiarray {

    // fixture for the zarr array test
    class MarrayTest : public ::testing::Test {

    protected:
        MarrayTest() :
            pathIntRegular_("int_regular.zr"), pathIntIrregular_("int_irregular.zr"),
            pathFloatRegular_("float_regular.zr"), pathFloatIrregular_("float_irregular.zr"),
            shape_({100, 100, 100}), chunkShapeRegular_({10, 10, 10}), chunkShapeIrregular_({23, 17, 11})
        {
        }

        ~MarrayTest() {
        }

        virtual void SetUp() {
            // create arrays
            createZarrArray(pathIntRegular_, "<i4", shape_, chunkShapeRegular_);
            createZarrArray(pathIntIrregular_, "<i4", shape_, chunkShapeIrregular_);
            createZarrArray(pathFloatRegular_, "<f4", shape_, chunkShapeRegular_);
            createZarrArray(pathFloatIrregular_, "<f4", shape_, chunkShapeIrregular_);

            // write test data
        }

        virtual void TearDown() {
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

        std::string pathIntRegular_;
        std::string pathIntIrregular_;
        std::string pathFloatRegular_;
        std::string pathFloatIrregular_;

        types::ShapeType shape_;
        types::ShapeType chunkShapeRegular_;
        types::ShapeType chunkShapeIrregular_;
    };


    TEST_F(MarrayTest, TestThrow) {
        auto array = openZarrArray(pathIntRegular_);

        // check for shape throws #0
        types::ShapeType shape0({120, 120, 120});
        types::ShapeType offset0({0, 0, 0});
        andres::Marray<int32_t> sub0(shape0.begin(), shape0.end());
        ASSERT_THROW(readSubarray(array, sub0, offset0.begin()), std::runtime_error);

        // check for shape throws #1
        types::ShapeType shape1({80, 80, 80});
        types::ShapeType offset1({30, 30, 30});
        andres::Marray<int32_t> sub1(shape1.begin(), shape1.end());
        ASSERT_THROW(readSubarray(array, sub1, offset1.begin()), std::runtime_error);

        types::ShapeType shape({80, 80, 80});
        types::ShapeType offset({0, 0, 0});
        // check for dtype throws #0
        andres::Marray<int64_t> sub64(shape.begin(), shape.end());
        ASSERT_THROW(readSubarray(array, sub64, offset.begin()), std::runtime_error);

        // check for dtype throws #1
        andres::Marray<uint32_t> subu(shape.begin(), shape.end());
        ASSERT_THROW(readSubarray(array, subu, offset.begin()), std::runtime_error);

        // check for dtype throws #2
        andres::Marray<float> subf(shape.begin(), shape.end());
        ASSERT_THROW(readSubarray(array, subf, offset.begin()), std::runtime_error);
    }


    TEST_F(MarrayTest, TestReadIntRegular) {

    }
}
}
