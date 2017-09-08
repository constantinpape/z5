#include "gtest/gtest.h"
#include "json.hpp"

#include "zarr++/metadata.hxx"

namespace fs = boost::filesystem;

namespace zarr {

    // fixture for the metadata
    class MetadataTest : public ::testing::Test {

    protected:
        MetadataTest(){
            // standard zarray metadata
            j = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"dtype\": \"<f8\", \"fill_value\": 0, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;
        }

        virtual ~MetadataTest() {
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

        ArrayMetadata metadata;
        readMetadata(h, metadata);

        ASSERT_EQ(metadata.shape.size(), metadata.chunkShape.size());
        ASSERT_EQ(metadata.shape.size(), j["shape"].size());
        ASSERT_EQ(metadata.chunkShape.size(), j["chunks"].size());
        for(int i = 0; i < metadata.shape.size(); ++i) {
            ASSERT_EQ(metadata.chunkShape[i], j["chunks"][i]);
            ASSERT_EQ(metadata.shape[i], j["shape"][i]);
        }
        const auto & compressor = j["compressor"];
        ASSERT_EQ(metadata.compressorLevel, compressor["clevel"]);
        ASSERT_EQ(metadata.compressorName, compressor["cname"]);
        ASSERT_EQ(metadata.compressorId, compressor["id"]);
        ASSERT_EQ(metadata.compressorShuffle, compressor["shuffle"]);
        ASSERT_EQ(metadata.dtype, j["dtype"]);
        // FIXME boost any is a bit tricky here
        ASSERT_EQ(metadata.fillValue, j["fill_value"]);
        ASSERT_EQ(metadata.order, j["order"]);
    }


    TEST_F(MetadataTest, WriteMetadata) {
        fs::path mdata(".zarray");
        fs::remove(mdata);

        ArrayMetadata metadata;
        metadata.fromJson(j);

        handle::Array h(".");
        writeMetadata(h, metadata);
        ASSERT_TRUE(fs::exists(mdata));
    }


    TEST_F(MetadataTest, WriteReadMetadat) {
        fs::path mdata(".zarray");
        fs::remove(mdata);

        ArrayMetadata metaWrite;
        metaWrite.fromJson(j);

        handle::Array h(".");
        writeMetadata(h, metaWrite);
        ASSERT_TRUE(fs::exists(mdata));

        ArrayMetadata metaRead;
        readMetadata(h, metaRead);

        ASSERT_EQ(metaRead.shape.size(), metaRead.chunkShape.size());
        ASSERT_EQ(metaRead.shape.size(), metaWrite.shape.size());
        ASSERT_EQ(metaRead.chunkShape.size(), metaWrite.chunkShape.size());
        for(int i = 0; i < metaRead.shape.size(); ++i) {
            ASSERT_EQ(metaRead.chunkShape[i], metaWrite.chunkShape[i]);
            ASSERT_EQ(metaRead.shape[i],      metaWrite.shape[i]);
        }
        ASSERT_EQ(metaRead.compressorLevel,   metaWrite.compressorLevel);
        ASSERT_EQ(metaRead.compressorName,    metaWrite.compressorName);
        ASSERT_EQ(metaRead.compressorId,      metaWrite.compressorId);
        ASSERT_EQ(metaRead.compressorShuffle, metaWrite.compressorShuffle);
        ASSERT_EQ(metaRead.dtype,             metaWrite.dtype);
        // FIXME boost any is a bit tricky here
        ASSERT_EQ(metaRead.fillValue, metaWrite.fillValue);
        ASSERT_EQ(metaRead.order, metaWrite.order);
    }

}
