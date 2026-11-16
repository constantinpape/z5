#include <random>
#include "gtest/gtest.h"

#include "z5/factory.hxx"

#define SIZE 10*10*10


namespace z5 {

    // fixture for the factory test
    class FactoryTest : public ::testing::Test {

    protected:
        FactoryTest() : tmp("tmp_dir") {
        }

        ~FactoryTest() {}

        void SetUp() {
            fs::create_directories(tmp);
        }

        void TearDown() {
            fs::remove_all(tmp);
        }

        fs::path tmp;


        template<typename T>
        void checkDataset(std::unique_ptr<Dataset> & ds) {
            T data[SIZE];
            std::fill(data, data + SIZE, static_cast<T>(42));
            ds->checkRequestType(typeid(T));
            ds->writeChunk(types::ShapeType({0, 0, 0}), data);
            T dataOut[SIZE];
            ds->readChunk(types::ShapeType({0, 0, 0}), dataOut);
            for(std::size_t i = 0; i < SIZE; ++i) {
                ASSERT_EQ(data[i], dataOut[i]);
            }
        }


        void writeMetadataHelper(const filesystem::handle::Dataset & ds, const std::string & dtype) {
            DatasetMetadata meta(types::Datatypes::n5ToDtype()[dtype],
                                 types::ShapeType({100, 100, 100}),
                                 types::ShapeType({10, 10, 10}),
                                 false);
            z5::filesystem::writeMetadata(ds, meta);
        }
    };


    TEST_F(FactoryTest, CreateFile) {
        fs::path p = tmp / "f.n5";
        z5::filesystem::handle::File file(p);
        ASSERT_FALSE(file.exists());
        z5::createFile(file, false);
        ASSERT_TRUE(file.exists());
    }


    TEST_F(FactoryTest, CreateGroup) {
        fs::path p = tmp / "f.n5";
        z5::filesystem::handle::File file(p);
        z5::createFile(file, false);

        z5::createGroup(file, "group");
        z5::filesystem::handle::Group group(file, "group");
        ASSERT_TRUE(group.exists());

        z5::createGroup(group, "sub");
        z5::filesystem::handle::Group group1(group, "sub");
        ASSERT_TRUE(group1.exists());
    }


    TEST_F(FactoryTest, OpenAllDtypes) {
        std::vector<std::string> dtypes({
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "float64"
        });

        fs::path p = tmp / "f.n5";
        z5::filesystem::handle::File file(p);
        z5::createFile(file, false);

        for(const auto & dtype : dtypes) {

            z5::filesystem::handle::Dataset handle(file, dtype);
            handle.create();
            writeMetadataHelper(handle, dtype);

            auto ds = openDataset(file, dtype);
            switch(types::Datatypes::n5ToDtype()[dtype]) {
                case types::int8:
                    checkDataset<int8_t>(ds);
                    break;
                case types::int16:
                    checkDataset<int16_t>(ds);
                    break;
                case types::int32:
                    checkDataset<int32_t>(ds);
                    break;
                case types::int64:
                    checkDataset<int64_t>(ds);
                    break;
                case types::uint8:
                    checkDataset<uint8_t>(ds);
                    break;
                case types::uint16:
                    checkDataset<uint16_t>(ds);
                    break;
                case types::uint32:
                    checkDataset<uint32_t>(ds);
                    break;
                case types::uint64:
                    checkDataset<uint64_t>(ds);
                    break;
                case types::float32:
                    checkDataset<float>(ds);
                    break;
                case types::float64:
                    checkDataset<double>(ds);
                    break;
                default:
                    break;
            }
        }
    }


    TEST_F(FactoryTest, CreateDatasetOptions) {
        fs::path p = tmp / "f.zr";
        z5::filesystem::handle::File file(p);
        z5::createFile(file, true, 3);

        DatasetOptions options;
        options.compressor = "zlib";
        options.fillValue = 7;
        options.zarrFormat = 3;
        options.chunkKeyEncoding = "v2";
        options.shardShape = types::ShapeType({20, 20, 20});
        auto ds = createDataset(file, "data", "int32",
                                types::ShapeType({100, 100, 100}),
                                types::ShapeType({10, 10, 10}),
                                options);

        // the options round-trip through the metadata on disk
        DatasetMetadata meta;
        filesystem::readMetadata(filesystem::handle::Dataset(ds->path(), ds->mode()), meta);
        ASSERT_EQ(meta.zarrFormat, 3);
        ASSERT_EQ(meta.compressor, types::zlib);
        ASSERT_EQ(meta.fillValue, 7);
        ASSERT_EQ(meta.chunkKeyEncoding, "v2");
        ASSERT_EQ(meta.shardShape, types::ShapeType({20, 20, 20}));

        // the positional form gives the same metadata
        auto ds2 = createDataset(file, "data2", "int32",
                                 types::ShapeType({100, 100, 100}),
                                 types::ShapeType({10, 10, 10}),
                                 "zlib", types::CompressionOptions(), 7, ".", 3, "v2",
                                 types::ShapeType({20, 20, 20}));
        DatasetMetadata meta2;
        filesystem::readMetadata(filesystem::handle::Dataset(ds2->path(), ds2->mode()), meta2);
        nlohmann::json j, j2;
        meta.toJson(j);
        meta2.toJson(j2);
        ASSERT_EQ(j, j2);

        // v3-only options are rejected for other formats
        DatasetOptions bad;
        bad.shardShape = types::ShapeType({20, 20, 20});
        ASSERT_THROW(createDataset(file, "bad", "int32",
                                   types::ShapeType({100, 100, 100}),
                                   types::ShapeType({10, 10, 10}),
                                   bad), std::runtime_error);
    }


    TEST_F(FactoryTest, CreateDatasetCrc32c) {
        fs::path p = tmp / "f.zr";
        z5::filesystem::handle::File file(p);
        z5::createFile(file, true, 3);

        DatasetOptions options;
        options.compressor = "zlib";
        options.zarrFormat = 3;
        options.crc32c = true;
        auto ds = createDataset(file, "data", "int32",
                                types::ShapeType({100, 100, 100}),
                                types::ShapeType({10, 10, 10}),
                                options);
        ASSERT_TRUE(ds->hasCrc32c());
        checkDataset<int32_t>(ds);

        // the checksum follows the compressor in the stored codec list
        DatasetMetadata meta;
        filesystem::readMetadata(filesystem::handle::Dataset(ds->path(), ds->mode()), meta);
        ASSERT_EQ(meta.crc32cBeforeCompressor, 0u);
        ASSERT_EQ(meta.crc32cAfterCompressor, 1u);
        nlohmann::json j;
        meta.toJson(j);
        ASSERT_EQ(j["codecs"].size(), 3u);
        ASSERT_EQ(j["codecs"][2]["name"], "crc32c");

        // off by default
        DatasetOptions plain;
        plain.zarrFormat = 3;
        auto ds2 = createDataset(file, "plain", "int32",
                                 types::ShapeType({100, 100, 100}),
                                 types::ShapeType({10, 10, 10}),
                                 plain);
        ASSERT_FALSE(ds2->hasCrc32c());

        // zarr v3 only
        fs::path p2 = tmp / "f2.zr";
        z5::filesystem::handle::File file2(p2);
        z5::createFile(file2, true, 2);
        DatasetOptions bad;
        bad.crc32c = true;
        ASSERT_THROW(createDataset(file2, "bad", "int32",
                                   types::ShapeType({100, 100, 100}),
                                   types::ShapeType({10, 10, 10}),
                                   bad), std::runtime_error);
    }


    TEST_F(FactoryTest, CreateDatasetV3ZlibFraming) {
        // zarr v3: "gzip" must write gzip-framed chunks under the core gzip codec,
        // "zlib" zlib-framed chunks under numcodecs.zlib
        fs::path p = tmp / "f.zr";
        z5::filesystem::handle::File file(p);
        z5::createFile(file, true, 3);

        const std::vector<std::pair<std::string, std::string>> cases{
            {"gzip", "gzip"}, {"zlib", "numcodecs.zlib"}};
        for(const auto & c : cases) {
            DatasetOptions options;
            options.compressor = c.first;
            options.zarrFormat = 3;
            auto ds = createDataset(file, c.first, "int32",
                                    types::ShapeType({100, 100, 100}),
                                    types::ShapeType({10, 10, 10}),
                                    options);
            checkDataset<int32_t>(ds);

            DatasetMetadata meta;
            filesystem::readMetadata(filesystem::handle::Dataset(ds->path(), ds->mode()), meta);
            nlohmann::json j;
            meta.toJson(j);
            ASSERT_EQ(j["codecs"][1]["name"], c.second);

            // gzip streams start with 1f 8b, zlib streams with 78
            std::vector<char> raw;
            ds->readRawChunk(types::ShapeType({0, 0, 0}), raw);
            ASSERT_GE(raw.size(), 2u);
            if(c.first == "gzip") {
                ASSERT_EQ(static_cast<unsigned char>(raw[0]), 0x1fu);
                ASSERT_EQ(static_cast<unsigned char>(raw[1]), 0x8bu);
            } else {
                ASSERT_EQ(static_cast<unsigned char>(raw[0]), 0x78u);
            }
        }
    }


    TEST_F(FactoryTest, CreateAllDtypes) {
        std::vector<std::string> dtypes({
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "float32", "float64"
        });

        fs::path p = tmp / "f.n5";
        z5::filesystem::handle::File file(p);
        z5::createFile(file, false);

        for(const auto & dtype : dtypes) {
            auto ds = createDataset(
                file, dtype,
                dtype,
                types::ShapeType({100, 100, 100}),
                types::ShapeType({10, 10, 10})
            );
            switch(types::Datatypes::n5ToDtype().at(dtype)) {

                case types::int8:
                    checkDataset<int8_t>(ds);
                    break;
                case types::int16:
                    checkDataset<int16_t>(ds);
                    break;
                case types::int32:
                    checkDataset<int32_t>(ds);
                    break;
                case types::int64:
                    checkDataset<int64_t>(ds);
                    break;
                case types::uint8:
                    checkDataset<uint8_t>(ds);
                    break;
                case types::uint16:
                    checkDataset<uint16_t>(ds);
                    break;
                case types::uint32:
                    checkDataset<uint32_t>(ds);
                    break;
                case types::uint64:
                    checkDataset<uint64_t>(ds);
                    break;
                case types::float32:
                    checkDataset<float>(ds);
                    break;
                case types::float64:
                    checkDataset<double>(ds);
                    break;
                default:
                    break;
            }
        }

    }

}
