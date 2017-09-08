#include "gtest/gtest.h"

#include <random>

#include "zarr++/metadata.hxx"
#include "zarr++/array_factory.hxx"

namespace fs = boost::filesystem;

#define SIZE 10*10*10

namespace zarr {


    void getMetadata(const std::string & dtype, ArrayMetadata & meta) {
        nlohmann::json j = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"fill_value\": 42, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;
        j["dtype"] = dtype;
        meta.fromJson(j);
    }


    void writeMetadata(const handle::Array & h, const std::string & dtype) {
        ArrayMetadata meta;
        getMetadata(dtype, meta);
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
        void checkArray(std::unique_ptr<ZarrArray> & array) {
            T data[SIZE];
            std::fill(data, data + SIZE, static_cast<T>(42));
            array->writeChunk(types::ShapeType({0, 0, 0}), data);
            T dataOut[SIZE];
            array->readChunk(types::ShapeType({0, 0, 0}), dataOut);
            for(size_t i = 0; i < SIZE; ++i) {
                ASSERT_EQ(data[i], dataOut[i]);
            }
        }


        handle::Array handle_;

    };


    TEST_F(FactoryTest, OpenAllDtypes) {
        std::vector<std::string> dtypes({
            "<i1", "<i2", "<i4", "<i8",
            "<u1", "<u2", "<u4", "<u8",
            "<f4", "<f8"
        });

        for(const auto & dtype : dtypes) {
            handle_.createDir();
            writeMetadata(handle_, dtype);
            auto array = openZarrArray(handle_.path().string());

            switch(types::stringToDtype[dtype]) {

                case types::int8:
                    checkArray<int8_t>(array);
                    break;
                case types::int16:
                    checkArray<int16_t>(array);
                    break;
                case types::int32:
                    checkArray<int32_t>(array);
                    break;
                case types::int64:
                    checkArray<int64_t>(array);
                    break;
                case types::uint8:
                    checkArray<uint8_t>(array);
                    break;
                case types::uint16:
                    checkArray<uint16_t>(array);
                    break;
                case types::uint32:
                    checkArray<uint32_t>(array);
                    break;
                case types::uint64:
                    checkArray<uint64_t>(array);
                    break;
                case types::float32:
                    checkArray<float>(array);
                    break;
                case types::float64:
                    checkArray<double>(array);
                    break;
                default:
                    break;
            }

            fs::remove_all(handle_.path());
        }

    }


    TEST_F(FactoryTest, CreateAllDtypes) {
        std::vector<std::string> dtypes({
            "<i1", "<i2", "<i4", "<i8",
            "<u1", "<u2", "<u4", "<u8",
            "<f4", "<f8"
        });

        for(const auto & dtype : dtypes) {
            auto array = createZarrArray(
                handle_.path().string(),
                dtype,
                types::ShapeType({100, 100, 100}),
                types::ShapeType({10, 10, 10})
            );
            switch(types::stringToDtype[dtype]) {

                case types::int8:
                    checkArray<int8_t>(array);
                    break;
                case types::int16:
                    checkArray<int16_t>(array);
                    break;
                case types::int32:
                    checkArray<int32_t>(array);
                    break;
                case types::int64:
                    checkArray<int64_t>(array);
                    break;
                case types::uint8:
                    checkArray<uint8_t>(array);
                    break;
                case types::uint16:
                    checkArray<uint16_t>(array);
                    break;
                case types::uint32:
                    checkArray<uint32_t>(array);
                    break;
                case types::uint64:
                    checkArray<uint64_t>(array);
                    break;
                case types::float32:
                    checkArray<float>(array);
                    break;
                case types::float64:
                    checkArray<double>(array);
                    break;
                default:
                    break;
            }

            fs::remove_all(handle_.path());
        }

    }




}
