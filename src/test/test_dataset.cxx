#include "gtest/gtest.h"

#include <random>

#include "z5/metadata.hxx"
#include "z5/dataset.hxx"

namespace fs = boost::filesystem;

namespace z5 {

    // fixture for the zarr array test
    class DatasetTest : public ::testing::Test {

    protected:
        DatasetTest() : floatHandle_("array_float.zr"), intHandle_("array_int.zr") {
            // int zarray metadata
            jInt_ = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"dtype\": \"<i4\", \"fill_value\": 42, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;
            jFloat_ = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"dtype\": \"<i4\", \"fill_value\": 42, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;

        }

        virtual ~DatasetTest() {

        }

        virtual void SetUp() {

            //
            // make test data
            //
            std::default_random_engine generator;
            // fill 'dataInt_' with random values
            std::uniform_int_distribution<int> distributionInt(0, 1000);
            auto drawInt = std::bind(distributionInt, generator);
            for(std::size_t i = 0; i < size_; ++i) {
                dataInt_[i] = drawInt();
            }

            // fill 'dataFloat_' with random values
            std::uniform_real_distribution<float> distributionFloat(0., 1.);
            auto drawFloat = std::bind(distributionFloat, generator);
            for(std::size_t i = 0; i < size_; ++i) {
                dataFloat_[i] = drawFloat();
            }

            //
            // create files for reading
            //
            floatHandle_.createDir();
            intHandle_.createDir();

            DatasetMetadata floatMeta;
            floatMeta.fromJson(jFloat_, true);
            writeMetadata(floatHandle_, floatMeta);

            DatasetMetadata intMeta;
            intMeta.fromJson(jInt_, true);
            writeMetadata(intHandle_, intMeta);
        }

        virtual void TearDown() {
            // remove stuff
            fs::remove_all(floatHandle_.path());
            fs::remove_all(intHandle_.path());
            handle::Dataset hi("array_int1.zr");
            if(hi.exists()) {
                fs::remove_all(hi.path());
            }
            handle::Dataset hf("array_float1.zr");
            if(hf.exists()) {
                fs::remove_all(hf.path());
            }
        }

        handle::Dataset floatHandle_;
        handle::Dataset intHandle_;

        nlohmann::json jInt_;
        nlohmann::json jFloat_;

        const static std::size_t size_ = 10*10*10;
        int dataInt_[size_];
        float dataFloat_[size_];

    };


    TEST_F(DatasetTest, OpenIntDataset) {

        DatasetTyped<int> array(intHandle_);
        const auto & chunksPerDim = array.chunksPerDimension();

        std::default_random_engine generator;

        // test uninitialized chunk
        int dataTmp[size_];
        array.readChunk(types::ShapeType({0, 0, 0}), dataTmp);
        // check
        for(std::size_t i = 0; i < size_; ++i) {
            ASSERT_EQ(dataTmp[i], 42);
        }

        // test for 10 random chuks
        for(unsigned _ = 0; _ < 10; ++_) {
            // get a random chunk
            types::ShapeType chunkId(array.dimension());
            for(unsigned d = 0; d < array.dimension(); ++d) {
                std::uniform_int_distribution<std::size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = distr(generator);
            }

            array.writeChunk(chunkId, dataInt_);

            // read a chunk
            int dataTmp[size_];
            array.readChunk(chunkId, dataTmp);

            // check
            for(std::size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataInt_[i]);
            }
        }
    }


    TEST_F(DatasetTest, CreateIntDataset) {

        handle::Dataset h("array_int1.zr");
        DatasetMetadata intMeta;
        intMeta.fromJson(jInt_, true);

        DatasetTyped<int> array(h, intMeta);
        const auto & chunksPerDim = array.chunksPerDimension();

        std::default_random_engine generator;

        // test uninitialized chunk
        int dataTmp[size_];
        array.readChunk(types::ShapeType({0, 0, 0}), dataTmp);
        // check
        for(std::size_t i = 0; i < size_; ++i) {
            ASSERT_EQ(dataTmp[i], 42);
        }

        // test for 10 random chunks
        for(unsigned _ = 0; _ < 10; ++_) {
            // get a random chunk
            types::ShapeType chunkId(array.dimension());
            for(unsigned d = 0; d < array.dimension(); ++d) {
                std::uniform_int_distribution<std::size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = distr(generator);
            }

            array.writeChunk(chunkId, dataInt_);

            // read a chunk
            int dataTmp[size_];
            array.readChunk(chunkId, dataTmp);

            // check
            for(std::size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataInt_[i]);
            }
        }
    }


    TEST_F(DatasetTest, OpenFloatDataset) {

        DatasetTyped<float> array(floatHandle_);
        const auto & chunksPerDim = array.chunksPerDimension();

        std::default_random_engine generator;

        // test uninitialized chunk
        float dataTmp[size_];
        array.readChunk(types::ShapeType({0, 0, 0}), dataTmp);
        // check
        for(std::size_t i = 0; i < size_; ++i) {
            ASSERT_EQ(dataTmp[i], 42.);
        }

        // test for 10 random chunks
        for(unsigned t = 0; t < 10; ++t) {

            // get a random chunk
            types::ShapeType chunkId(array.dimension());
            for(unsigned d = 0; d < array.dimension(); ++d) {
                std::uniform_int_distribution<std::size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = distr(generator);
            }

            array.writeChunk(chunkId, dataFloat_);

            // read a chunk
            float dataTmp[size_];
            array.readChunk(chunkId, dataTmp);

            // check
            for(std::size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataFloat_[i]);
            }
        }
    }


    TEST_F(DatasetTest, CreateFloatDataset) {

        handle::Dataset h("array_float1.zr");
        DatasetMetadata floatMeta;
        floatMeta.fromJson(jFloat_, true);

        DatasetTyped<float> array(h, floatMeta);
        const auto & chunksPerDim = array.chunksPerDimension();

        std::default_random_engine generator;

        // test uninitialized chunk
        float dataTmp[size_];
        array.readChunk(types::ShapeType({0, 0, 0}), dataTmp);
        // check
        for(std::size_t i = 0; i < size_; ++i) {
            ASSERT_EQ(dataTmp[i], 42.);
        }

        // test for 10 random chunks
        for(unsigned t = 0; t < 10; ++t) {

            // get a random chunk
            types::ShapeType chunkId(array.dimension());
            for(unsigned d = 0; d < array.dimension(); ++d) {
                std::uniform_int_distribution<std::size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = distr(generator);
            }

            array.writeChunk(chunkId, dataFloat_);

            // read a chunk
            float dataTmp[size_];
            array.readChunk(chunkId, dataTmp);

            // check
            for(std::size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataFloat_[i]);
            }
        }
    }

}
