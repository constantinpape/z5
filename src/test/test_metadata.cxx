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
            jZarr = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"dtype\": \"<f8\", \"fill_value\": 0, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;
            jN5 = "{ \"blockSize\": [10, 10, 10], \"compressionType\": \"gzip\", \"dataType\": \"float64\", \"dimensions\": [100, 100, 100] }"_json;
        }

        virtual ~MetadataTest() {
        }

        virtual void SetUp() {
            // write zarr metadata
            fs::path mdata("array.zr");
            fs::create_directory(mdata);
            mdata /= ".zarray";
            fs::ofstream file(mdata);
            jZarr >> file;
            file.close();

            // write N5 metadata
            fs::path mdataN5("array.n5");
            fs::create_directory(mdataN5);
            mdataN5 /= "attributes.json";
            fs::ofstream fileN5(mdataN5);
            jN5 >> fileN5;
            fileN5.close();
        }

        virtual void TearDown() {
            // remove zarr mdata
            fs::path mdata("array.zr");
            fs::remove_all(mdata);
            // remove n5 mdata
            fs::path mdataN5("array.n5");
            fs::remove_all(mdataN5);
        }

        nlohmann::json jZarr;
        nlohmann::json jN5;

    };


    TEST_F(MetadataTest, ReadMetadata) {
        handle::Array h("array.zr");

        ArrayMetadata metadata;
        readMetadata(h, metadata);

        ASSERT_EQ(metadata.shape.size(), metadata.chunkShape.size());
        ASSERT_EQ(metadata.shape.size(), jZarr["shape"].size());
        ASSERT_EQ(metadata.chunkShape.size(), jZarr["chunks"].size());
        for(int i = 0; i < metadata.shape.size(); ++i) {
            ASSERT_EQ(metadata.chunkShape[i], jZarr["chunks"][i]);
            ASSERT_EQ(metadata.shape[i], jZarr["shape"][i]);
        }
        const auto & compressor = jZarr["compressor"];
        ASSERT_EQ(metadata.compressorLevel, compressor["clevel"]);
        ASSERT_EQ(metadata.compressorName, compressor["cname"]);
        ASSERT_EQ(metadata.compressorId, compressor["id"]);
        ASSERT_EQ(metadata.compressorShuffle, compressor["shuffle"]);
        ASSERT_EQ(metadata.dtype, jZarr["dtype"]);
        // FIXME boost any is a bit tricky here
        ASSERT_EQ(metadata.fillValue, jZarr["fill_value"]);
        ASSERT_EQ(metadata.order, jZarr["order"]);
    }


    TEST_F(MetadataTest, ReadMetadataN5) {
        handle::Array h("array.n5");

        ArrayMetadata metadata;
        readMetadata(h, metadata);

        ASSERT_EQ(metadata.shape.size(), metadata.chunkShape.size());
        ASSERT_EQ(metadata.shape.size(), jN5["dimensions"].size());
        ASSERT_EQ(metadata.chunkShape.size(), jN5["blockSize"].size());
        for(int i = 0; i < metadata.shape.size(); ++i) {
            ASSERT_EQ(metadata.chunkShape[i], jN5["blockSize"][i]);
            ASSERT_EQ(metadata.shape[i], jN5["dimensions"][i]);
        }
        ASSERT_EQ(metadata.compressorId, "zlib");
        ASSERT_EQ(metadata.compressorName, jN5["compressionType"]);
        ASSERT_EQ(metadata.dtype, types::n5TypeToZarr[jN5["dataType"]]);
    }


    TEST_F(MetadataTest, WriteMetadata) {
        fs::path mdata("array.zr/.zarray");
        fs::remove(mdata);

        ArrayMetadata metadata;
        metadata.fromJson(jZarr);

        handle::Array h("array.zr");
        writeMetadata(h, metadata);
        ASSERT_TRUE(fs::exists(mdata));
    }


    TEST_F(MetadataTest, WriteMetadataN5) {
        fs::path mdata("array.n5/attributes.json");
        fs::remove(mdata);

        ArrayMetadata metadata;
        metadata.fromJsonN5(jN5);

        handle::Array h("array.n5");
        writeMetadata(h, metadata);
        ASSERT_TRUE(fs::exists(mdata));
    }


    TEST_F(MetadataTest, WriteReadMetadata) {
        fs::path mdata("array.zr/.zarray");
        fs::remove(mdata);

        ArrayMetadata metaWrite;
        metaWrite.fromJson(jZarr);

        handle::Array h("array.zr");
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


    TEST_F(MetadataTest, WriteReadMetadataN5) {
        fs::path mdata("array.n5/attributes.json");
        fs::remove(mdata);

        ArrayMetadata metaWrite;
        metaWrite.fromJsonN5(jN5);

        handle::Array h("array.n5");
        writeMetadata(h, metaWrite);
        ASSERT_TRUE(fs::exists(mdata));

        ArrayMetadata metaRead;
        readMetadata(h, metaRead);

        ASSERT_EQ(metaRead.shape.size(), metaRead.chunkShape.size());
        ASSERT_EQ(metaRead.shape.size(), jN5["dimensions"].size());
        ASSERT_EQ(metaRead.chunkShape.size(), jN5["blockSize"].size());
        for(int i = 0; i < metaRead.shape.size(); ++i) {
            ASSERT_EQ(metaRead.chunkShape[i], jN5["blockSize"][i]);
            ASSERT_EQ(metaRead.shape[i], jN5["dimensions"][i]);
        }
        ASSERT_EQ(metaRead.compressorId, "zlib");
        ASSERT_EQ(metaRead.compressorName, jN5["compressionType"]);
        ASSERT_EQ(metaRead.dtype, types::n5TypeToZarr[jN5["dataType"]]);
    }

}
