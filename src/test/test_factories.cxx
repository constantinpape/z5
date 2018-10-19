#include "gtest/gtest.h"

#include <random>

#include "z5/metadata.hxx"
#include "z5/dataset_factory.hxx"

namespace fs = boost::filesystem;

#define SIZE 10*10*10

namespace z5 {


    void writeMetadata(const handle::Dataset & h, const std::string & dtype) {
        DatasetMetadata meta(types::Datatypes::n5ToDtype()[dtype],
                             types::ShapeType({100, 100, 100}),
                             types::ShapeType({10, 10, 10}),
                             true);
        writeMetadata(h, meta);
    }


    // fixture for the zarr array test
    class FactoryTest : public ::testing::Test {

    protected:
        FactoryTest() : handle_("array.zr") {
        }

        virtual ~FactoryTest() {
            if(handle_.exists()) {
                fs::remove_all(handle_.path());
            }
        }


        template<typename T>
        void checkDataset(std::unique_ptr<Dataset> & array) {
            T data[SIZE];
            std::fill(data, data + SIZE, static_cast<T>(42));
            array->checkRequestType(typeid(T));
            array->writeChunk(types::ShapeType({0, 0, 0}), data);
            T dataOut[SIZE];
            array->readChunk(types::ShapeType({0, 0, 0}), dataOut);
            for(std::size_t i = 0; i < SIZE; ++i) {
                ASSERT_EQ(data[i], dataOut[i]);
            }
        }


        handle::Dataset handle_;

    };


    TEST_F(FactoryTest, OpenAllDtypes) {
        std::vector<std::string> dtypes({
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "float64"
        });

        for(const auto & dtype : dtypes) {
            handle_.createDir();
            writeMetadata(handle_, dtype);
            auto array = openDataset(handle_.path().string());

            switch(types::Datatypes::n5ToDtype()[dtype]) {

                case types::int8:
                    checkDataset<int8_t>(array);
                    break;
                case types::int16:
                    checkDataset<int16_t>(array);
                    break;
                case types::int32:
                    checkDataset<int32_t>(array);
                    break;
                case types::int64:
                    checkDataset<int64_t>(array);
                    break;
                case types::uint8:
                    checkDataset<uint8_t>(array);
                    break;
                case types::uint16:
                    checkDataset<uint16_t>(array);
                    break;
                case types::uint32:
                    checkDataset<uint32_t>(array);
                    break;
                case types::uint64:
                    checkDataset<uint64_t>(array);
                    break;
                case types::float32:
                    checkDataset<float>(array);
                    break;
                case types::float64:
                    checkDataset<double>(array);
                    break;
                default:
                    break;
            }

            fs::remove_all(handle_.path());
        }
    }


    TEST_F(FactoryTest, CreateAllDtypes) {
        std::vector<std::string> dtypes({
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "float64"
        });

        for(const auto & dtype : dtypes) {
            auto array = createDataset(
                handle_.path().string(),
                dtype,
                types::ShapeType({100, 100, 100}),
                types::ShapeType({10, 10, 10}),
                true
            );
            switch(types::Datatypes::n5ToDtype().at(dtype)) {

                case types::int8:
                    checkDataset<int8_t>(array);
                    break;
                case types::int16:
                    checkDataset<int16_t>(array);
                    break;
                case types::int32:
                    checkDataset<int32_t>(array);
                    break;
                case types::int64:
                    checkDataset<int64_t>(array);
                    break;
                case types::uint8:
                    checkDataset<uint8_t>(array);
                    break;
                case types::uint16:
                    checkDataset<uint16_t>(array);
                    break;
                case types::uint32:
                    checkDataset<uint32_t>(array);
                    break;
                case types::uint64:
                    checkDataset<uint64_t>(array);
                    break;
                case types::float32:
                    checkDataset<float>(array);
                    break;
                case types::float64:
                    checkDataset<double>(array);
                    break;
                default:
                    break;
            }

            fs::remove_all(handle_.path());
        }

    }

}
