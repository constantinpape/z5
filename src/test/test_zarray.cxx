#include "gtest/gtest.h"

#include <random>

#include "zarr++/metadata.hxx"
#include "zarr++/array.hxx"

namespace fs = boost::filesystem;

namespace zarr {

    // fixture for the zarr array test
    class ArrayTest : public ::testing::Test {

    protected:
        ArrayTest() : floatHandle_("array_float.zr"), intHandle_("array_int.zr") {
            // int zarray metadata
            jInt_ = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"dtype\": \"<i4\", \"fill_value\": 0, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;
            jFloat_ = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"dtype\": \"<i4\", \"fill_value\": 0, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;

        }

        virtual ~ArrayTest() {

        }

        virtual void SetUp() {

            //
            // make test data
            //
            std::default_random_engine generator;
            // fill 'dataInt_' with random values
            std::uniform_int_distribution<int> distributionInt(0, 1000);
            auto drawInt = std::bind(distributionInt, generator);
            for(size_t i = 0; i < size_; ++i) {
                dataInt_[i] = drawInt();
            }

            // fill 'dataFloat_' with random values
            std::uniform_real_distribution<float> distributionFloat(0., 1.);
            auto drawFloat = std::bind(distributionFloat, generator);
            for(size_t i = 0; i < size_; ++i) {
                dataFloat_[i] = drawFloat();
            }

            //
            // create files for reading
            //
            floatHandle_.createDir();
            intHandle_.createDir();

            ArrayMetadata floatMeta;
            readMetadata(jFloat_, floatMeta);
            writeMetadata(floatHandle_, floatMeta);

            ArrayMetadata intMeta;
            readMetadata(jInt_, intMeta);
            writeMetadata(intHandle_, intMeta);
        }

        virtual void TearDown() {
            // remove stuff
            fs::remove_all(floatHandle_.path());
            fs::remove_all(intHandle_.path());
        }

        handle::Array floatHandle_;
        handle::Array intHandle_;

        nlohmann::json jInt_;
        nlohmann::json jFloat_;

        const static size_t size_ = 10*10*10;
        int dataInt_[size_];
        float dataFloat_[size_];

    };


    TEST_F(ArrayTest, OpenIntArray) {

        ZarrArrayTyped<int> array(intHandle_);
        const auto & chunksPerDim = array.chunksPerDimension();
        const auto & chunkShape = array.chunkShape();

        std::default_random_engine generator;

        // test for 10 random chuks
        for(unsigned _ = 0; _ < 10; ++_) {
            // get a random chunk
            types::ShapeType chunkId(array.dimension());
            for(unsigned d = 0; d < array.dimension(); ++d) {
                std::uniform_int_distribution<size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = chunkShape[d] * distr(generator);
            }

            array.writeChunk(chunkId, dataInt_);

            // read a chunk
            int dataTmp[size_];
            array.readChunk(chunkId, dataTmp);

            // check
            for(size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataInt_[i]);
            }
        }
    }


    TEST_F(ArrayTest, OpenFloatArray) {

        ZarrArrayTyped<float> array(floatHandle_);
        const auto & chunksPerDim = array.chunksPerDimension();
        const auto & chunkShape = array.chunkShape();

        std::default_random_engine generator;

        // test for 10 random chunks
        for(unsigned t = 0; t < 10; ++t) {

            // get a random chunk
            types::ShapeType chunkId(array.dimension());
            for(unsigned d = 0; d < array.dimension(); ++d) {
                std::uniform_int_distribution<size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = chunkShape[d] * distr(generator);
            }

            array.writeChunk(chunkId, dataFloat_);

            // read a chunk
            float dataTmp[size_];
            array.readChunk(chunkId, dataTmp);

            // check
            for(size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataFloat_[i]);
            }
        }
    }

}
