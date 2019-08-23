#include <random>
#include "gtest/gtest.h"

#include "z5/factory.hxx"

#define SIZE 10*10*10


namespace z5 {

    // fixture for the factory test
    class FactoryTest : public ::testing::Test {

    protected:
        FactoryTest() : tmp("tmp_dir") {
        }

        ~FactoryTest() {}

        void SetUp() {
            fs::create_directories(tmp);
        }

        void TearDown() {
            fs::remove_all(tmp);
        }

        fs::path tmp;


        template<typename T>
        void checkDataset(std::unique_ptr<Dataset> & ds) {
            T data[SIZE];
            std::fill(data, data + SIZE, static_cast<T>(42));
            ds->checkRequestType(typeid(T));
            ds->writeChunk(types::ShapeType({0, 0, 0}), data);
            T dataOut[SIZE];
            ds->readChunk(types::ShapeType({0, 0, 0}), dataOut);
            for(std::size_t i = 0; i < SIZE; ++i) {
                ASSERT_EQ(data[i], dataOut[i]);
            }
        }


        void writeMetadataHelper(const filesystem::handle::Dataset & ds, const std::string & dtype) {
            DatasetMetadata meta(types::Datatypes::n5ToDtype()[dtype],
                                 types::ShapeType({100, 100, 100}),
                                 types::ShapeType({10, 10, 10}),
                                 false);
            z5::filesystem::writeMetadata(ds, meta);
        }
    };


    TEST_F(FactoryTest, CreateFile) {
        fs::path p = tmp / "f.n5";
        z5::filesystem::handle::File file(p);
        ASSERT_FALSE(file.exists());
        z5::createFile(file, false);
        ASSERT_TRUE(file.exists());
    }


    TEST_F(FactoryTest, CreateGroup) {
        fs::path p = tmp / "f.n5";
        z5::filesystem::handle::File file(p);
        z5::createFile(file, false);

        z5::createGroup(file, "group");
        z5::filesystem::handle::Group group(file, "group");
        ASSERT_TRUE(group.exists());

        z5::createGroup(group, "sub");
        z5::filesystem::handle::Group group1(group, "sub");
        ASSERT_TRUE(group1.exists());
    }


    TEST_F(FactoryTest, OpenAllDtypes) {
        std::vector<std::string> dtypes({
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "float64"
        });

        fs::path p = tmp / "f.n5";
        z5::filesystem::handle::File file(p);
        z5::createFile(file, false);

        for(const auto & dtype : dtypes) {

            z5::filesystem::handle::Dataset handle(file, dtype);
            handle.create();
            writeMetadataHelper(handle, dtype);

            auto ds = openDataset(file, dtype);
            switch(types::Datatypes::n5ToDtype()[dtype]) {
                case types::int8:
                    checkDataset<int8_t>(ds);
                    break;
                case types::int16:
                    checkDataset<int16_t>(ds);
                    break;
                case types::int32:
                    checkDataset<int32_t>(ds);
                    break;
                case types::int64:
                    checkDataset<int64_t>(ds);
                    break;
                case types::uint8:
                    checkDataset<uint8_t>(ds);
                    break;
                case types::uint16:
                    checkDataset<uint16_t>(ds);
                    break;
                case types::uint32:
                    checkDataset<uint32_t>(ds);
                    break;
                case types::uint64:
                    checkDataset<uint64_t>(ds);
                    break;
                case types::float32:
                    checkDataset<float>(ds);
                    break;
                case types::float64:
                    checkDataset<double>(ds);
                    break;
                default:
                    break;
            }
        }
    }


    TEST_F(FactoryTest, CreateAllDtypes) {
        std::vector<std::string> dtypes({
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "float64"
        });

        fs::path p = tmp / "f.n5";
        z5::filesystem::handle::File file(p);
        z5::createFile(file, false);

        for(const auto & dtype : dtypes) {
            auto ds = createDataset(
                file, dtype,
                dtype,
                types::ShapeType({100, 100, 100}),
                types::ShapeType({10, 10, 10})
            );
            switch(types::Datatypes::n5ToDtype().at(dtype)) {

                case types::int8:
                    checkDataset<int8_t>(ds);
                    break;
                case types::int16:
                    checkDataset<int16_t>(ds);
                    break;
                case types::int32:
                    checkDataset<int32_t>(ds);
                    break;
                case types::int64:
                    checkDataset<int64_t>(ds);
                    break;
                case types::uint8:
                    checkDataset<uint8_t>(ds);
                    break;
                case types::uint16:
                    checkDataset<uint16_t>(ds);
                    break;
                case types::uint32:
                    checkDataset<uint32_t>(ds);
                    break;
                case types::uint64:
                    checkDataset<uint64_t>(ds);
                    break;
                case types::float32:
                    checkDataset<float>(ds);
                    break;
                case types::float64:
                    checkDataset<double>(ds);
                    break;
                default:
                    break;
            }
        }

    }

}
