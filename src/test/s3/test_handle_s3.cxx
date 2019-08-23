#include "gtest/gtest.h"
#include "z5/s3/handle.hxx"

namespace z5 {

    // fixture for the handle test
    class HandleTest : public ::testing::Test {

    protected:
        HandleTest(){}

        ~HandleTest(){
        }

        void SetUp() {
        }

        void TearDown() {
        }
    };


    TEST_F(HandleTest, TestFile) {
        // Make a file
        z5::s3::handle::File f;
        f.create();
    }


    TEST_F(HandleTest, TestGroup) {
        // Make a file
        z5::s3::handle::File f;

        // Make a group
        z5::s3::handle::Group g(f, "group");
        g.create();

        // Make a sub-group
        z5::s3::handle::Group g1(g, "sub");
        g1.create();
    }


    TEST_F(HandleTest, TestDataset) {

        // Make a file
        z5::s3::handle::File f;

        // Make a dataset
        z5::s3::handle::Dataset ds(f, "ds");
        ds.create();

        // Make a group
        z5::s3::handle::Group g(f, "group");

        // Make a dataset
        z5::s3::handle::Dataset ds1(g, "ds");
        ds1.create();
    }
}
