#include "gtest/gtest.h"
#include "nlohmann/json.hpp"

#include "z5/attributes.hxx"
#include "z5/dataset_factory.hxx"

namespace fs = boost::filesystem;

namespace z5 {

    // fixture for the metadata
    class AttributesTest : public ::testing::Test {

    protected:
        AttributesTest() : hZarr("array.zr"), hN5("array.n5"){
            j["a"] = 42;
            j["b"] = 3.14;
            j["c"] = std::vector<int>({1, 2, 3});
            j["d"] = "blub";
        }

        ~AttributesTest(){
        }

        void SetUp() {
            types::ShapeType shape({100, 100, 100});
            types::ShapeType chunks({10, 10, 10});

            types::CompressionOptions cOpts;
            cOpts["level"] = 5;
            std::string codec = "lz4";
            cOpts["codec"] = codec;
            cOpts["shuffle"] = 1;
            createDataset(hZarr.path().string(), "int32",
                          shape, chunks, true,
                          "blosc", cOpts, 0);

            types::CompressionOptions cOptsN5;
            cOptsN5["level"] = 5;
            cOptsN5["useZlib"] = false;
            createDataset(hN5.path().string(), "int32",
                          shape, chunks, false,
                          "gzip", cOptsN5, 0);
        }

        void TearDown() {
            fs::remove_all(hZarr.path());
            fs::remove_all(hN5.path());
        }

        handle::Dataset hZarr;
        handle::Dataset hN5;
        nlohmann::json j;
    };


    TEST_F(AttributesTest, TestWriteReadZarr) {
        writeAttributes(hZarr, j);
        nlohmann::json jOut;
        std::vector<std::string> keys({"a", "b", "c", "d"});
        readAttributes(hZarr, keys, jOut);

        ASSERT_EQ(jOut["a"], 42);
        ASSERT_EQ(jOut["b"], 3.14);
        ASSERT_EQ(jOut["c"], std::vector<int>({1, 2, 3}));
        ASSERT_EQ(jOut["d"], "blub");
    }


    TEST_F(AttributesTest, TestWriteReadN5) {
        writeAttributes(hN5, j);
        nlohmann::json jOut;
        std::vector<std::string> keys({"a", "b", "c", "d"});
        readAttributes(hN5, keys, jOut);

        ASSERT_EQ(jOut["a"], 42);
        ASSERT_EQ(jOut["b"], 3.14);
        ASSERT_EQ(jOut["c"], std::vector<int>({1, 2, 3}));
        ASSERT_EQ(jOut["d"], "blub");
    }
}
