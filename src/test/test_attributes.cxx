#include "gtest/gtest.h"
#include "nlohmann/json.hpp"

#include "z5/attributes.hxx"
#include "z5/factory.hxx"

namespace z5 {

    // fixture for the metadata
    class AttributesTest : public ::testing::Test {

    protected:
        AttributesTest() : fZarr("data.zr"), fN5("data.n5"){
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

            fZarr.create();
            createDataset(fZarr, "data", "int32",
                          shape, chunks);

            fN5.create();
            createDataset(fN5, "data", "int32",
                          shape, chunks);
        }

        void TearDown() {
            fs::remove_all(fZarr.path());
            fs::remove_all(fN5.path());
        }

        filesystem::handle::File fZarr;
        filesystem::handle::File fN5;
        nlohmann::json j;
    };


    TEST_F(AttributesTest, TestAttributesDsZarr) {
        filesystem::handle::Dataset h(fZarr, "data");
        writeAttributes(h, j);
        nlohmann::json jOut;
        readAttributes(h, jOut);

        ASSERT_EQ(jOut["a"], 42);
        ASSERT_EQ(jOut["b"], 3.14);
        ASSERT_EQ(jOut["c"], std::vector<int>({1, 2, 3}));
        ASSERT_EQ(jOut["d"], "blub");
    }


    TEST_F(AttributesTest, TestAttributesDsN5) {
        filesystem::handle::Dataset h(fZarr, "data");
        writeAttributes(h, j);
        nlohmann::json jOut;
        readAttributes(h, jOut);

        ASSERT_EQ(jOut["a"], 42);
        ASSERT_EQ(jOut["b"], 3.14);
        ASSERT_EQ(jOut["c"], std::vector<int>({1, 2, 3}));
        ASSERT_EQ(jOut["d"], "blub");

        const std::vector<std::string> protectedAttributes = {"dimensions", "blockSize", "dataType",
                                                              "compressionType", "compression"};
        for(const auto & attr : protectedAttributes) {
            ASSERT_TRUE((jOut.find(attr) == jOut.end()));
        }

        for(const auto & attr : protectedAttributes) {
            j[attr] = "blob";
            ASSERT_THROW(writeAttributes(h, j), std::runtime_error);
            j.erase(attr);
        }
    }


    TEST_F(AttributesTest, TestAttributesFileZarr) {
        writeAttributes(fZarr, j);
        nlohmann::json jOut;
        readAttributes(fZarr, jOut);

        ASSERT_EQ(jOut["a"], 42);
        ASSERT_EQ(jOut["b"], 3.14);
        ASSERT_EQ(jOut["c"], std::vector<int>({1, 2, 3}));
        ASSERT_EQ(jOut["d"], "blub");
    }


    TEST_F(AttributesTest, TestAttributesFileN5) {
        writeAttributes(fN5, j);
        nlohmann::json jOut;
        readAttributes(fN5, jOut);

        ASSERT_EQ(jOut["a"], 42);
        ASSERT_EQ(jOut["b"], 3.14);
        ASSERT_EQ(jOut["c"], std::vector<int>({1, 2, 3}));
        ASSERT_EQ(jOut["d"], "blub");

        j["n5"] = "3.0.0";
        ASSERT_THROW(writeAttributes(fN5, j), std::runtime_error);
    }
}
