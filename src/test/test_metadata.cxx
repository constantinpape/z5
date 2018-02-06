#include "gtest/gtest.h"
#include "nlohmann/json.hpp"

#include "z5/metadata.hxx"

namespace fs = boost::filesystem;

namespace z5 {

    // fixture for the metadata
    class MetadataTest : public ::testing::Test {

    protected:
        MetadataTest(){
            // standard zarray metadata
            jZarr = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"dtype\": \"<f8\", \"fill_value\": 0, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;
            // TODO change to new compression format
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
            file << jZarr;
            file.close();

            // write N5 metadata
            fs::path mdataN5("array.n5");
            fs::create_directory(mdataN5);
            mdataN5 /= "attributes.json";
            fs::ofstream fileN5(mdataN5);
            fileN5 << jN5;
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
        handle::Dataset h("array.zr");

        DatasetMetadata metadata;
        readMetadata(h, metadata);

        ASSERT_EQ(metadata.shape.size(), metadata.chunkShape.size());
        ASSERT_EQ(metadata.shape.size(), jZarr["shape"].size());
        ASSERT_EQ(metadata.chunkShape.size(), jZarr["chunks"].size());
        for(int i = 0; i < metadata.shape.size(); ++i) {
            ASSERT_EQ(metadata.chunkShape[i], jZarr["chunks"][i]);
            ASSERT_EQ(metadata.shape[i], jZarr["shape"][i]);
        }
        const auto & compressor = jZarr["compressor"];
        // check compressr
        ASSERT_EQ(metadata.compressor, types::Compressors::zarrToCompressor()[compressor["id"]]);
        // check compression options
        ASSERT_EQ(boost::any_cast<int>(metadata.compressionOptions["level"]), compressor["clevel"]);
        ASSERT_EQ(boost::any_cast<std::string>(metadata.compressionOptions["codec"]), compressor["cname"]);
        ASSERT_EQ(boost::any_cast<int>(metadata.compressionOptions["shuffle"]), compressor["shuffle"]);
        // check dtype, fillvalue and order
        ASSERT_EQ(metadata.dtype, types::Datatypes::zarrToDtype()[jZarr["dtype"]]);
        ASSERT_EQ(metadata.fillValue, jZarr["fill_value"]);
        ASSERT_EQ(metadata.order, jZarr["order"]);
    }

    TEST_F(MetadataTest, ReadMetadataN5) {
        handle::Dataset h("array.n5");

        DatasetMetadata metadata;
        readMetadata(h, metadata);

        ASSERT_EQ(metadata.shape.size(), metadata.chunkShape.size());
        ASSERT_EQ(metadata.shape.size(), jN5["dimensions"].size());
        ASSERT_EQ(metadata.chunkShape.size(), jN5["blockSize"].size());
        for(int i = 0; i < metadata.shape.size(); ++i) {
            ASSERT_EQ(metadata.chunkShape[i], jN5["blockSize"][i]);
            ASSERT_EQ(metadata.shape[i], jN5["dimensions"][i]);
        }
        #ifdef WITH_ZLIB
        ASSERT_EQ(metadata.compressor, types::zlib);
        #endif
        ASSERT_EQ(boost::any_cast<bool>(metadata.compressionOptions.at("useZlib")), false);
        ASSERT_EQ(metadata.dtype, types::Datatypes::n5ToDtype()[jN5["dataType"]]);
    }


    TEST_F(MetadataTest, WriteMetadata) {
        fs::path mdata("array.zr/.zarray");
        fs::remove(mdata);

        DatasetMetadata metadata;
        metadata.fromJson(jZarr, true);

        handle::Dataset h("array.zr");
        writeMetadata(h, metadata);
        ASSERT_TRUE(fs::exists(mdata));
    }


    TEST_F(MetadataTest, WriteMetadataN5) {
        fs::path mdata("array.n5/attributes.json");
        fs::remove(mdata);

        DatasetMetadata metadata;
        metadata.fromJson(jN5, false);

        handle::Dataset h("array.n5");
        writeMetadata(h, metadata);
        ASSERT_TRUE(fs::exists(mdata));
    }


    TEST_F(MetadataTest, WriteReadMetadata) {
        fs::path mdata("array.zr/.zarray");
        fs::remove(mdata);

        DatasetMetadata metaWrite;
        metaWrite.fromJson(jZarr, true);

        handle::Dataset h("array.zr");
        writeMetadata(h, metaWrite);
        ASSERT_TRUE(fs::exists(mdata));

        DatasetMetadata metaRead;
        readMetadata(h, metaRead);

        // check shapes and chunks
        ASSERT_EQ(metaRead.shape.size(), metaRead.chunkShape.size());
        ASSERT_EQ(metaRead.shape.size(), metaWrite.shape.size());
        ASSERT_EQ(metaRead.chunkShape.size(), metaWrite.chunkShape.size());
        for(int i = 0; i < metaRead.shape.size(); ++i) {
            ASSERT_EQ(metaRead.chunkShape[i], metaWrite.chunkShape[i]);
            ASSERT_EQ(metaRead.shape[i],      metaWrite.shape[i]);
        }
        // check compression
        ASSERT_EQ(metaRead.compressor, metaWrite.compressor);
        // check compression options
        ASSERT_EQ(boost::any_cast<int>(metaRead.compressionOptions.at("level")),
                  boost::any_cast<int>(metaWrite.compressionOptions.at("level")));
        ASSERT_EQ(boost::any_cast<int>(metaRead.compressionOptions.at("shuffle")),
                  boost::any_cast<int>(metaWrite.compressionOptions.at("shuffle")));
        ASSERT_EQ(boost::any_cast<std::string>(metaRead.compressionOptions.at("codec")),
                  boost::any_cast<std::string>(metaWrite.compressionOptions.at("codec")));
        // check dtype, fill value, order
        ASSERT_EQ(metaRead.dtype,             metaWrite.dtype);
        ASSERT_EQ(metaRead.fillValue, metaWrite.fillValue);
        ASSERT_EQ(metaRead.order, metaWrite.order);
    }


    TEST_F(MetadataTest, WriteReadMetadataN5) {
        fs::path mdata("array.n5/attributes.json");
        fs::remove(mdata);

        DatasetMetadata metaWrite;
        metaWrite.fromJson(jN5, false);

        handle::Dataset h("array.n5");
        writeMetadata(h, metaWrite);
        ASSERT_TRUE(fs::exists(mdata));

        DatasetMetadata metaRead;
        readMetadata(h, metaRead);

        ASSERT_EQ(metaRead.shape.size(), metaRead.chunkShape.size());
        ASSERT_EQ(metaRead.shape.size(), jN5["dimensions"].size());
        ASSERT_EQ(metaRead.chunkShape.size(), jN5["blockSize"].size());
        for(int i = 0; i < metaRead.shape.size(); ++i) {
            ASSERT_EQ(metaRead.chunkShape[i], jN5["blockSize"][i]);
            ASSERT_EQ(metaRead.shape[i], jN5["dimensions"][i]);
        }
        #ifdef WITH_ZLIB
        ASSERT_EQ(metaRead.compressor, types::zlib);
        #endif
        ASSERT_EQ(boost::any_cast<bool>(metaRead.compressionOptions["useZlib"]), false);
        ASSERT_EQ(metaRead.dtype, types::Datatypes::n5ToDtype()[jN5["dataType"]]);
    }

}
