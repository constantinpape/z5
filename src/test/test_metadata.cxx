#include "gtest/gtest.h"

#include "zarr++/metadata.hxx"

namespace zarr {

    // fixture for the metadata
    class MetadataTest : public ::testing::Test {

    protected:
        MetadataTest(){
        }

        virtual ~MetadataTest() {
        }

        virtual void SetUp() {
        }

        virtual void TearDown() {
        }

    };


    TEST_F(MetadataTest, ReadMetadata) {

    }

}
