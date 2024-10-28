#include "gtest/gtest.h"

#include <random>

#include "z5/factory.hxx"
#include "z5/filesystem/metadata.hxx"
#include "z5/filesystem/dataset.hxx"


namespace z5 {

    // fixture for the zarr array test
    class DatasetTest : public ::testing::Test {

    protected:
        DatasetTest() : fileHandle_("data.zr"), intHandle_(fileHandle_, "int"), floatHandle_(fileHandle_, "float"), complexFloatHandle_(fileHandle_, "complexFloat") {
            // int zarray metadata
            jInt_ = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"dtype\": \"<i4\", \"fill_value\": 42, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;
            jFloat_ = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"dtype\": \"<f4\", \"fill_value\": 42, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;
            jComplexFloat_ = "{ \"chunks\": [10, 10, 10], \"compressor\": { \"clevel\": 5, \"cname\": \"lz4\", \"id\": \"blosc\", \"shuffle\": 1}, \"dtype\": \"<c8\", \"fill_value\": 42, \"filters\": null, \"order\": \"C\", \"shape\": [100, 100, 100], \"zarr_format\": 2}"_json;

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

            // fill 'dataComplexFloat_' with random values
            for(std::size_t i = 0; i < size_; ++i) {
                dataComplexFloat_[i].real(drawFloat());
                dataComplexFloat_[i].imag(drawFloat());
            }

            //
            // create files for reading
            //
            fileHandle_.create();
            intHandle_.create();
            floatHandle_.create();
            complexFloatHandle_.create();

            DatasetMetadata intMeta;
            intMeta.fromJson(jInt_, true);
            filesystem::writeMetadata(intHandle_, intMeta);

            DatasetMetadata floatMeta;
            floatMeta.fromJson(jFloat_, true);
            filesystem::writeMetadata(floatHandle_, floatMeta);
            
            DatasetMetadata complexFloatMeta;
            complexFloatMeta.fromJson(jComplexFloat_, true);
            filesystem::writeMetadata(complexFloatHandle_, complexFloatMeta);
        }

        virtual void TearDown() {
            // remove stuff
            fs::remove_all(fileHandle_.path());
        }

        filesystem::handle::File fileHandle_;
        filesystem::handle::Dataset intHandle_;
        filesystem::handle::Dataset floatHandle_;
        filesystem::handle::Dataset complexFloatHandle_;

        nlohmann::json jInt_;
        nlohmann::json jFloat_;
        nlohmann::json jComplexFloat_;

        static const std::size_t size_ = 10*10*10;
        int dataInt_[size_];
        float dataFloat_[size_];
        std::complex<float> dataComplexFloat_[size_];

    };


    TEST_F(DatasetTest, OpenIntDataset) {

        auto ds = openDataset(fileHandle_, "int");
        const auto & chunksPerDim = ds->chunksPerDimension();

        std::default_random_engine generator;

        // test uninitialized chunk -> this is expected to throw a runtime error
        int dataTmp[size_];
        ASSERT_THROW(ds->readChunk(types::ShapeType({0, 0, 0}), dataTmp), std::runtime_error);

        // test for 10 random chunks
        for(unsigned _ = 0; _ < 10; ++_) {
            // get a random chunk
            types::ShapeType chunkId(ds->dimension());
            for(unsigned d = 0; d < ds->dimension(); ++d) {
                std::uniform_int_distribution<std::size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = distr(generator);
            }

            ds->writeChunk(chunkId, dataInt_);

            // read a chunk
            int dataTmp[size_];
            ds->readChunk(chunkId, dataTmp);

            // check
            for(std::size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataInt_[i]);
            }
        }
    }


    TEST_F(DatasetTest, CreateIntDataset) {

        DatasetMetadata intMeta;
        intMeta.fromJson(jInt_, true);

        auto ds = createDataset(fileHandle_, "int1", intMeta);
        const auto & chunksPerDim = ds->chunksPerDimension();

        std::default_random_engine generator;

        // test uninitialized chunk -> this is expected to throw a runtime error
        int dataTmp[size_];
        ASSERT_THROW(ds->readChunk(types::ShapeType({0, 0, 0}), dataTmp), std::runtime_error);

        // test for 10 random chunks
        for(unsigned _ = 0; _ < 10; ++_) {
            // get a random chunk
            types::ShapeType chunkId(ds->dimension());
            for(unsigned d = 0; d < ds->dimension(); ++d) {
                std::uniform_int_distribution<std::size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = distr(generator);
            }

            ds->writeChunk(chunkId, dataInt_);

            // read a chunk
            int dataTmp[size_];
            ds->readChunk(chunkId, dataTmp);

            // check
            for(std::size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataInt_[i]);
            }
        }
    }


    TEST_F(DatasetTest, OpenFloatDataset) {

        auto ds = openDataset(fileHandle_, "float");
        const auto & chunksPerDim = ds->chunksPerDimension();

        std::default_random_engine generator;

        // test uninitialized chunk -> this is expected to throw a runtime error
        float dataTmp[size_];
        ASSERT_THROW(ds->readChunk(types::ShapeType({0, 0, 0}), dataTmp), std::runtime_error);

        // test for 10 random chunks
        for(unsigned t = 0; t < 10; ++t) {

            // get a random chunk
            types::ShapeType chunkId(ds->dimension());
            for(unsigned d = 0; d < ds->dimension(); ++d) {
                std::uniform_int_distribution<std::size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = distr(generator);
            }

            ds->writeChunk(chunkId, dataFloat_);

            // read a chunk
            float dataTmp[size_];
            ds->readChunk(chunkId, dataTmp);

            // check
            for(std::size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataFloat_[i]);
            }
        }
    }


    TEST_F(DatasetTest, CreateFloatDataset) {

        DatasetMetadata floatMeta;
        floatMeta.fromJson(jFloat_, true);

        auto ds = createDataset(fileHandle_, "float1", floatMeta);
        const auto & chunksPerDim = ds->chunksPerDimension();

        std::default_random_engine generator;

        // test uninitialized chunk -> this is expected to throw a runtime error
        float dataTmp[size_];
        ASSERT_THROW(ds->readChunk(types::ShapeType({0, 0, 0}), dataTmp), std::runtime_error);

        // test for 10 random chunks
        for(unsigned t = 0; t < 10; ++t) {

            // get a random chunk
            types::ShapeType chunkId(ds->dimension());
            for(unsigned d = 0; d < ds->dimension(); ++d) {
                std::uniform_int_distribution<std::size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = distr(generator);
            }

            ds->writeChunk(chunkId, dataFloat_);

            // read a chunk
            float dataTmp[size_];
            ds->readChunk(chunkId, dataTmp);

            // check
            for(std::size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataFloat_[i]);
            }
        }
    }

    TEST_F(DatasetTest, OpenComplexFloatDataset) {

        auto ds = openDataset(fileHandle_, "complexFloat");
        const auto & chunksPerDim = ds->chunksPerDimension();

        std::default_random_engine generator;

        // test uninitialized chunk -> this is expected to throw a runtime error
        std::complex<float> dataTmp[size_];
        ASSERT_THROW(ds->readChunk(types::ShapeType({0, 0, 0}), dataTmp), std::runtime_error);

        // test for 10 random chunks
        for(unsigned t = 0; t < 10; ++t) {

            // get a random chunk
            types::ShapeType chunkId(ds->dimension());
            for(unsigned d = 0; d < ds->dimension(); ++d) {
                std::uniform_int_distribution<std::size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = distr(generator);
            }

            ds->writeChunk(chunkId, dataComplexFloat_);

            // read a chunk
            std::complex<float> dataTmp[size_];
            ds->readChunk(chunkId, dataTmp);

            // check
            for(std::size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataComplexFloat_[i]);
            }
        }
    }

    TEST_F(DatasetTest, CreateComplexFloatDataset) {

        DatasetMetadata complexFloatMeta;
        complexFloatMeta.fromJson(jComplexFloat_, true);

        auto ds = createDataset(fileHandle_, "complexFloat1", complexFloatMeta);
        const auto & chunksPerDim = ds->chunksPerDimension();

        std::default_random_engine generator;

        // test uninitialized chunk -> this is expected to throw a runtime error
        std::complex<float> dataTmp[size_];
        ASSERT_THROW(ds->readChunk(types::ShapeType({0, 0, 0}), dataTmp), std::runtime_error);

        // test for 10 random chunks
        for(unsigned t = 0; t < 10; ++t) {

            // get a random chunk
            types::ShapeType chunkId(ds->dimension());
            for(unsigned d = 0; d < ds->dimension(); ++d) {
                std::uniform_int_distribution<std::size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = distr(generator);
            }

            ds->writeChunk(chunkId, dataComplexFloat_);

            // read a chunk
            std::complex<float> dataTmp[size_];
            ds->readChunk(chunkId, dataTmp);

            // check
            for(std::size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataComplexFloat_[i]);
            }
        }
    }

    TEST_F(DatasetTest, CreateBloscDataset) {

        types::ShapeType shape({100, 100, 100});
        types::ShapeType chunks({10, 10, 10});

        Metadata fMeta(true);
        filesystem::writeMetadata(fileHandle_, fMeta);
        auto ds = createDataset(fileHandle_, "blosc-ds", "float32", shape, chunks, "blosc");
        const auto & chunksPerDim = ds->chunksPerDimension();

        std::default_random_engine generator;

        // test uninitialized chunk -> this is expected to throw a runtime error
        float dataTmp[size_];
        ASSERT_THROW(ds->readChunk(types::ShapeType({0, 0, 0}), dataTmp), std::runtime_error);

        // test for 10 random chunks
        for(unsigned t = 0; t < 10; ++t) {

            // get a random chunk
            types::ShapeType chunkId(ds->dimension());
            for(unsigned d = 0; d < ds->dimension(); ++d) {
                std::uniform_int_distribution<std::size_t> distr(0, chunksPerDim[d] - 1);
                chunkId[d] = distr(generator);
            }

            ds->writeChunk(chunkId, dataFloat_);

            // read a chunk
            float dataTmp[size_];
            ds->readChunk(chunkId, dataTmp);

            // check
            for(std::size_t i = 0; i < size_; ++i) {
                ASSERT_EQ(dataTmp[i], dataFloat_[i]);
            }
        }
    }

}
