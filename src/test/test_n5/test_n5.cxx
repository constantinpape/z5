#include "gtest/gtest.h"

#include "z5/dataset_factory.hxx"

namespace fs = boost::filesystem;
namespace z5 {

    // fixture for the zarr array test
    class N5Test : public ::testing::Test {

    protected:

        template<typename T>
        void testRead(
            const std::string & ds_path,
            types::Compressor expectedCompressor,
            const std::string & expectedCodec,
            const T expectedValue
        ) {
            auto ds = openDataset(ds_path);
            ASSERT_EQ(ds->getCompressor(), expectedCompressor);
            std::string codec;

            // check for the correct shapes, chunk size and shapes
            ASSERT_EQ(ds->size(), 111*121*113);
            ASSERT_EQ(ds->shape(0), 113);
            ASSERT_EQ(ds->shape(1), 121);
            ASSERT_EQ(ds->shape(2), 111);
            // check for the correct shapes, chunk size and shapes
            ASSERT_EQ(ds->maxChunkSize(), 17*25*14);
            ASSERT_EQ(ds->maxChunkShape(0), 14);
            ASSERT_EQ(ds->maxChunkShape(1), 25);
            ASSERT_EQ(ds->maxChunkShape(2), 17);

            auto chunks = ds->chunksPerDimension();
            for(std::size_t z = 0; z < chunks[0]; ++z) {
                for(std::size_t y = 0; y < chunks[1]; ++y) {
                    for(std::size_t x = 0; x < chunks[2]; ++x) {

                        std::size_t chunkSize = ds->getChunkSize({z, y, x});
                        std::vector<T> dataOut(chunkSize);

                        // read chunk and make sure it agrees
                        types::ShapeType chunk({z, y, x});
                        ds->readChunk(chunk, &dataOut[0]);
                        for(std::size_t i = 0; i < dataOut.size(); i++) {
                            ASSERT_EQ(dataOut[i], expectedValue);
                        }

                    }
                }
            }
        }


        template<typename T>
        void testWrite(
            const std::string & ds_path,
            const std::string & dtype,
            const std::string & n5Compressor,
            const T value
        ) {

            // TODO asymmetric, overhanging chunk shapes
            types::ShapeType shape = {100, 100, 100};
            types::ShapeType chunkShape = {10, 10, 10};
            types::CompressionOptions cOpts;
            cOpts["level"] = 5;
            cOpts["useZlib"] = false;
            auto ds = createDataset(ds_path, dtype, shape, chunkShape, false, n5Compressor, cOpts);

            auto chunks = ds->chunksPerDimension();
            std::vector<T> data(ds->maxChunkSize(), value);

            for(std::size_t z = 0; z < chunks[0]; ++z) {
                for(std::size_t y = 0; y < chunks[1]; ++y) {
                    for(std::size_t x = 0; x < chunks[2]; ++x) {
                        types::ShapeType chunk({z, y, x});
                        ds->writeChunk(chunk, &data[0]);
                    }
                }
            }
        }
    };

    //
    // Read integer types with raw compression
    //
    TEST_F(N5Test, testReadRawInt8) {
        testRead<int8_t>("n5_test_data/array_byte_raw.n5", types::raw, "raw", 42);
    }

    TEST_F(N5Test, testReadRawInt16) {
        testRead<int16_t>("n5_test_data/array_short_raw.n5", types::raw, "raw", 42);
    }

    TEST_F(N5Test, testReadRawInt32) {
        testRead<int32_t>("n5_test_data/array_int_raw.n5", types::raw, "raw", 42);
    }

    TEST_F(N5Test, testReadRawInt64) {
        testRead<int64_t>("n5_test_data/array_long_raw.n5", types::raw, "raw", 42);
    }

    //
    // Read float types with raw compression
    //
    TEST_F(N5Test, testReadRawFloat32) {
        testRead<float>("n5_test_data/array_float_raw.n5", types::raw, "raw", 42);
    }

    TEST_F(N5Test, testReadRawFloat64) {
        testRead<double>("n5_test_data/array_double_raw.n5", types::raw, "raw", 42);
    }

    #ifdef WITH_ZLIB
    //
    // Read integer types with gzip compression
    //
    TEST_F(N5Test, testReadGzipInt8) {
        testRead<int8_t>("n5_test_data/array_byte_gzip.n5", types::zlib, "gzip", 42);
    }

    TEST_F(N5Test, testReadGzipInt16) {
        testRead<int16_t>("n5_test_data/array_short_gzip.n5", types::zlib, "gzip", 42);
    }

    TEST_F(N5Test, testReadGzipInt32) {
        testRead<int32_t>("n5_test_data/array_int_gzip.n5", types::zlib, "gzip", 42);
    }

    TEST_F(N5Test, testReadGzipInt64) {
        testRead<int64_t>("n5_test_data/array_long_gzip.n5", types::zlib, "gzip", 42);
    }

    //
    // Read float types with gzip compression
    //
    TEST_F(N5Test, testReadGzipFloat32) {
        testRead<float>("n5_test_data/array_float_gzip.n5", types::zlib, "gzip", 42);
    }

    TEST_F(N5Test, testReadGzipFloat64) {
        testRead<double>("n5_test_data/array_double_gzip.n5", types::zlib, "gzip", 42);
    }


    // test the fill value
    TEST_F(N5Test, testReadFillvalue) {
        testRead<int32_t>("n5_test_data/array_fv.n5", types::zlib, "gzip", 0);
    }
    #endif


    //
    // Write all types with raw compression
    //

    TEST_F(N5Test, writeTestDataRaw) {
        fs::path folder("n5_test_data_out");
        if(!fs::exists(folder)) {
            fs::create_directory(folder);
        }

        testWrite<int8_t>("n5_test_data_out/array_byte_raw.n5", "int8", "raw", 42);
        testWrite<int16_t>("n5_test_data_out/array_short_raw.n5", "int16", "raw", 42);
        testWrite<int32_t>("n5_test_data_out/array_int_raw.n5", "int32", "raw", 42);
        testWrite<int64_t>("n5_test_data_out/array_long_raw.n5", "int64", "raw", 42);
        testWrite<float>("n5_test_data_out/array_float_raw.n5", "float32", "raw", 42);
        testWrite<double>("n5_test_data_out/array_double_raw.n5", "float64", "raw", 42);
    }

    TEST_F(N5Test, writeTestDataGzip) {
        fs::path folder("n5_test_data_out");
        if(!fs::exists(folder)) {
            fs::create_directory(folder);
        }

        testWrite<int8_t>("n5_test_data_out/array_byte_gzip.n5", "int8", "zlib", 42);
        testWrite<int16_t>("n5_test_data_out/array_short_gzip.n5", "int16", "zlib", 42);
        testWrite<int32_t>("n5_test_data_out/array_int_gzip.n5", "int32", "zlib", 42);
        testWrite<int64_t>("n5_test_data_out/array_long_gzip.n5", "int64", "zlib", 42);
        testWrite<float>("n5_test_data_out/array_float_gzip.n5", "float32", "zlib", 42);
        testWrite<double>("n5_test_data_out/array_double_gzip.n5", "float64", "zlib", 42);
    }

}
