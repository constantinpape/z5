#include "gtest/gtest.h"
#include "json.hpp"

#include "zarr++/attributes.hxx"
#include "zarr++/array_factory.hxx"

namespace fs = boost::filesystem;

namespace zarr {

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
            createZarrArray(
                hZarr.path().string(), "<i4", shape,
                chunks, 0, 5,
                "lz4", "blosc", 1,
                true
            );
            // TODO fix array creation for n5
            //createZarrArray(
            //    hN5.path().string(), "<i4", shape,
            //    chunks, 0, 5,
            //    "zlib", "gzip", 1,
            //    false
            //);
        }

        void TearDown() {
            fs::remove_all(hZarr.path());
            //fs::remove_all(hN5.path());
        }

        handle::Array hZarr;
        handle::Array hN5;
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


}
