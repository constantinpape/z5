#include "gtest/gtest.h"
#include "json.hpp"

#include "zarr++/metadata.hxx"

namespace fs = boost::filesystem;

namespace zarr {

    // fixture for the metadata
    class MetadataTest : public ::testing::Test {

    protected:
        MetadataTest(){
        }

        virtual ~MetadataTest() {
            // standard zarray metadata
            j = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"dtype\": \"<f8\", \"fill_value\": 0, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;
        }

        virtual void SetUp() {
            fs::path mdata(".zarray");
            fs::ofstream file(mdata);
            j >> file;
        }

        virtual void TearDown() {
            fs::path mdata(".zarray");
            fs::remove(mdata);
        }

        nlohmann::json j;

    };


    TEST_F(MetadataTest, ReadMetadata) {
        handle::Array h(".");

        ArrayMetadata meta;
        readMetadata(h, meta);

        // TODO test for all fields
        ASSERT_EQ(meta.shape.size(), meta.chunkShape.size());
        ASSERT_EQ(meta.shape.size(), j["shape"].size());
        ASSERT_EQ(meta.chunkShape.size(), j["chunkShape"].size());
        for(int i = 0; i < meta.shape.size(); ++i) {
            ASSERT_EQ(meta.chunkShape[i], j["chunkShape"][i]);
            ASSERT_EQ(meta.shape[i], j["schunkShape"][i]);
        }
    }

}
