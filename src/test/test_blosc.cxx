#include "gtest/gtest.h"

#include "zarr++/compression/blosc_compressor.hxx"
#include "zarr++/metadata.hxx"


namespace zarr {
namespace compression {

    // fixture for the blosc compressor
    class BloscTest : public ::testing::Test {

    protected:
        BloscTest() {

        }

        virtual ~BloscTest() {

        }

        virtual void SetUp() {

        }

        virtual void TearDown() {

        }

        BloscCompressor compressor_;
        float * dataFloatIn_;
        float * dataFloatOut_;
        int * intDataIn_;
        int * intDataOut_;
    };

    TEST_F(BloscTest, CompressDecompressInt) {

    }

    TEST_F(BloscTest, CompressDecompressFloat) {

    }

}
}
