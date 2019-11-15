#include <random>
#include "gtest/gtest.h"

#include "z5/factory.hxx"

#define SIZE 10*10*10


namespace z5 {

    // fixture for the factory test
    class FactoryTest : public ::testing::Test {

    protected:
        FactoryTest(){
        }

        ~FactoryTest() {}

        void SetUp() {
        }

        void TearDown() {
        }

    };


    TEST_F(FactoryTest, CreateFile) {
        z5::s3::handle::File file("my-bucket");
        // z5::createFile(file, false);
    }


    /*
    TEST_F(FactoryTest, CreateGroup) {
        z5::s3::handle::File file;
        z5::createFile(file, false);

        z5::createGroup(file, "group");
        z5::s3::handle::Group group(file, "group");

        z5::createGroup(group, "sub");
        z5::s3::handle::Group group1(group, "sub");
    }


    TEST_F(FactoryTest, OpenAllDtypes) {
        std::vector<std::string> dtypes({
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "float64"
        });

        z5::s3::handle::File file;
        z5::createFile(file, false);

        for(const auto & dtype : dtypes) {
            z5::s3::handle::Dataset handle(file, dtype);
            handle.create();
            auto ds = openDataset(file, dtype);
        }
    }


    TEST_F(FactoryTest, CreateAllDtypes) {
        std::vector<std::string> dtypes({
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "float64"
        });

        z5::s3::handle::File file;
        z5::createFile(file, false);

        for(const auto & dtype : dtypes) {
            auto ds = createDataset(
                file, dtype,
                dtype,
                types::ShapeType({100, 100, 100}),
                types::ShapeType({10, 10, 10})
            );
        }

    }
    */

}
