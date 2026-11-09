#include <bit>
#include <random>
#include "gtest/gtest.h"

#include "z5/factory.hxx"
#include "z5/multiarray/array_access.hxx"

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
        ASSERT_EQ(std::get<int64_t>(meta.fillValue), 7);
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


    // variable-length strings through the factory and the sub-array IO
    TEST_F(FactoryTest, CreateStringDataset) {
        for(const int zarrFormat : {2, 3}) {
            fs::path p = tmp / ("strings" + std::to_string(zarrFormat) + ".zr");
            z5::filesystem::handle::File file(p);
            z5::createFile(file, true, zarrFormat);

            DatasetOptions options;
            options.zarrFormat = zarrFormat;
            options.compressor = "zlib";
            options.fillValue = std::string("-");
            auto ds = createDataset(file, "s", "string", types::ShapeType({3, 5}),
                                    types::ShapeType({2, 2}), options);
            ASSERT_EQ(ds->getDtype(), types::vlen_utf8);
            std::string fill;
            ds->getFillValue(&fill);
            ASSERT_EQ(fill, "-");

            std::vector<std::string> in{"a", "", "h\xc3\xa9", std::string(1000, 'x'), "e", "f"};
            const types::ShapeType offset({1, 2});
            multiarray::writeSubarray<std::string>(*ds, multiarray::makeView(in.data(), types::ShapeType({2, 3})),
                                                   offset.begin());

            auto reopened = openDataset(file, "s");
            std::vector<std::string> out(15);
            const types::ShapeType zero({0, 0});
            multiarray::readSubarray<std::string>(*reopened, multiarray::makeView(out.data(), types::ShapeType({3, 5})),
                                                  zero.begin());
            const std::vector<std::string> expected{"-", "-", "-", "-", "-",
                                                    "-", "-", "a", "", "h\xc3\xa9",
                                                    "-", "-", std::string(1000, 'x'), "e", "f"};
            ASSERT_EQ(out, expected);

            // a numeric request on a string dataset is refused
            std::vector<int> wrong(15);
            ASSERT_THROW(multiarray::readSubarray<int>(*reopened, multiarray::makeView(wrong.data(), types::ShapeType({3, 5})),
                                                       zero.begin()), std::runtime_error);
        }

        // n5 has no such type; numeric fill values other than 0 are refused
        fs::path p = tmp / "strings.n5";
        z5::filesystem::handle::File n5(p);
        z5::createFile(n5, false);
        DatasetOptions plain;
        ASSERT_THROW(createDataset(n5, "s", "string", types::ShapeType({3}), types::ShapeType({2}), plain),
                     std::runtime_error);
        fs::path p2 = tmp / "strings_fill.zr";
        z5::filesystem::handle::File zr(p2);
        z5::createFile(zr, true, 3);
        DatasetOptions badFill;
        badFill.zarrFormat = 3;
        badFill.fillValue = 1.;
        ASSERT_THROW(createDataset(zr, "s", "string", types::ShapeType({3}), types::ShapeType({2}), badFill),
                     std::runtime_error);
    }


    // bool datasets hold uint8_t elements, 0 or 1; the fill value is a JSON bool
    TEST_F(FactoryTest, CreateBoolDataset) {
        for(const int zarrFormat : {2, 3}) {
            fs::path p = tmp / ("bool" + std::to_string(zarrFormat) + ".zr");
            z5::filesystem::handle::File file(p);
            z5::createFile(file, true, zarrFormat);

            DatasetOptions options;
            options.zarrFormat = zarrFormat;
            options.fillValue = true;
            auto ds = createDataset(file, "b", "bool", types::ShapeType({3, 5}),
                                    types::ShapeType({2, 2}), options);
            ASSERT_EQ(ds->getDtype(), types::boolean);
            uint8_t fill = 0;
            ds->getFillValue(&fill);
            ASSERT_EQ(fill, 1);

            std::vector<uint8_t> in{0, 1, 0, 0, 1, 0};
            const types::ShapeType offset({1, 2});
            multiarray::writeSubarray<uint8_t>(*ds, multiarray::makeView(in.data(), types::ShapeType({2, 3})),
                                               offset.begin());
            auto reopened = openDataset(file, "b");
            std::vector<uint8_t> out(15);
            const types::ShapeType zero({0, 0});
            multiarray::readSubarray<uint8_t>(*reopened, multiarray::makeView(out.data(), types::ShapeType({3, 5})),
                                              zero.begin());
            ASSERT_EQ(out, std::vector<uint8_t>({1, 1, 1, 1, 1,
                                                 1, 1, 0, 1, 0,
                                                 1, 1, 0, 1, 0}));

            DatasetMetadata meta;
            filesystem::readMetadata(filesystem::handle::Dataset(ds->path(), ds->mode()), meta);
            nlohmann::json j;
            meta.toJson(j);
            ASSERT_EQ(j["fill_value"], nlohmann::json(true));
            ASSERT_EQ(zarrFormat == 3 ? j["data_type"] : j["dtype"], zarrFormat == 3 ? "bool" : "|b1");

            // only 0 and 1 are bool values
            DatasetOptions bad;
            bad.zarrFormat = zarrFormat;
            bad.fillValue = int64_t(2);
            ASSERT_THROW(createDataset(file, "bad", "bool", types::ShapeType({3}), types::ShapeType({2}), bad),
                         std::runtime_error);
        }

        // fill_value parsing: zarr v3 takes only true / false, v2 also null, 0 and 1
        ASSERT_EQ(types::fillValueFromJson(false, types::boolean, 3), types::FillValue(false));
        ASSERT_THROW(types::fillValueFromJson(1, types::boolean, 3), std::runtime_error);
        ASSERT_THROW(types::fillValueFromJson(nullptr, types::boolean, 3), std::runtime_error);
        ASSERT_EQ(types::fillValueFromJson(nullptr, types::boolean, 2), types::FillValue(false));
        ASSERT_EQ(types::fillValueFromJson(1, types::boolean, 2), types::FillValue(true));
        ASSERT_THROW(types::fillValueFromJson(2, types::boolean, 2), std::runtime_error);

        // n5 has no bool type
        fs::path p = tmp / "bool.n5";
        z5::filesystem::handle::File n5(p);
        z5::createFile(n5, false);
        ASSERT_THROW(createDataset(n5, "b", "bool", types::ShapeType({3}), types::ShapeType({2}), DatasetOptions()),
                     std::runtime_error);
    }


    // float16 datasets hold uint16_t elements, the half-precision bits
    TEST_F(FactoryTest, CreateFloat16Dataset) {
        for(const int zarrFormat : {2, 3}) {
            fs::path p = tmp / ("half" + std::to_string(zarrFormat) + ".zr");
            z5::filesystem::handle::File file(p);
            z5::createFile(file, true, zarrFormat);

            DatasetOptions options;
            options.zarrFormat = zarrFormat;
            options.fillValue = -2.5;
            auto ds = createDataset(file, "h", "float16", types::ShapeType({4}), types::ShapeType({2}), options);
            ASSERT_EQ(ds->getDtype(), types::float16);
            uint16_t fill = 0;
            ds->getFillValue(&fill);
            ASSERT_EQ(fill, 0xc100);

            const std::vector<uint16_t> in{0x3c00, 0x7c00, 0x0001};
            const types::ShapeType offset({1});
            multiarray::writeSubarray<uint16_t>(*ds, multiarray::makeView(in.data(), types::ShapeType({3})),
                                                offset.begin());
            std::vector<uint16_t> out(4);
            const types::ShapeType zero({0});
            multiarray::readSubarray<uint16_t>(*openDataset(file, "h"),
                                               multiarray::makeView(out.data(), types::ShapeType({4})), zero.begin());
            ASSERT_EQ(out, std::vector<uint16_t>({0xc100, 0x3c00, 0x7c00, 0x0001}));
        }

        // a zarr v3 hex fill value has four digits
        ASSERT_EQ(types::fillValueFromJson("0x3c00", types::float16, 3), types::FillValue(1.));
        ASSERT_THROW(types::fillValueFromJson("0x3c", types::float16, 3), std::runtime_error);

        fs::path p = tmp / "half.n5";
        z5::filesystem::handle::File n5(p);
        z5::createFile(n5, false);
        ASSERT_THROW(createDataset(n5, "h", "float16", types::ShapeType({3}), types::ShapeType({2}), DatasetOptions()),
                     std::runtime_error);
    }


    // complex datasets, plain and sharded; the fill value is [real, imag]
    TEST_F(FactoryTest, CreateComplexDataset) {
        for(const bool sharded : {false, true}) {
            fs::path p = tmp / (sharded ? "complex_sharded.zr" : "complex.zr");
            z5::filesystem::handle::File file(p);
            z5::createFile(file, true, 3);

            DatasetOptions options;
            options.zarrFormat = 3;
            options.fillValue = std::complex<double>(1.5, -2.);
            if(sharded) {
                options.shardShape = types::ShapeType({4});
            }
            auto ds = createDataset(file, "c", "complex64", types::ShapeType({6}), types::ShapeType({2}), options);
            ASSERT_EQ(ds->getDtype(), types::complex64);
            ASSERT_EQ(ds->isSharded(), sharded);
            std::complex<float> fill;
            ds->getFillValue(&fill);
            ASSERT_EQ(fill, std::complex<float>(1.5f, -2.f));

            const std::vector<std::complex<float>> in{{1.f, 2.f}, {0.f, -3.f}, {4.f, 0.f}};
            const types::ShapeType offset({1});
            multiarray::writeSubarray<std::complex<float>>(*ds, multiarray::makeView(in.data(), types::ShapeType({3})),
                                                           offset.begin());
            std::vector<std::complex<float>> out(6);
            const types::ShapeType zero({0});
            multiarray::readSubarray<std::complex<float>>(*openDataset(file, "c"),
                                                          multiarray::makeView(out.data(), types::ShapeType({6})),
                                                          zero.begin());
            const std::complex<float> f(1.5f, -2.f);
            ASSERT_EQ(out, std::vector<std::complex<float>>({f, {1.f, 2.f}, {0.f, -3.f}, {4.f, 0.f}, f, f}));

            DatasetMetadata meta;
            filesystem::readMetadata(filesystem::handle::Dataset(ds->path(), ds->mode()), meta);
            nlohmann::json j;
            meta.toJson(j);
            ASSERT_EQ(j["fill_value"], nlohmann::json::array({1.5, -2.}));
            ASSERT_EQ(j["data_type"], "complex64");
        }

        // fill_value parsing: zarr v3 takes only [real, imag], v2 also a number
        ASSERT_EQ(types::fillValueFromJson(nlohmann::json::array({"NaN", 2}), types::complex128, 3).index(), 5u);
        ASSERT_EQ(types::fillValueFromJson(nlohmann::json::array({"0x3f800000", 0}), types::complex64, 3),
                  types::FillValue(std::complex<double>(1., 0.)));
        ASSERT_THROW(types::fillValueFromJson(1., types::complex64, 3), std::runtime_error);
        ASSERT_THROW(types::fillValueFromJson(nlohmann::json::array({1}), types::complex64, 3), std::runtime_error);
        ASSERT_EQ(types::fillValueFromJson(2., types::complex64, 2), types::FillValue(std::complex<double>(2., 0.)));
        // a complex fill value is refused for a real type
        ASSERT_THROW(types::normalizeFillValue(std::complex<double>(1., 1.), types::float32, false),
                     std::runtime_error);
    }


    // raw datasets hold std::string elements of the element size
    TEST_F(FactoryTest, CreateRawDataset) {
        types::Datatype dtype;
        std::size_t size;
        ASSERT_TRUE(types::parseDtypeName("r24", dtype, size));
        ASSERT_EQ(dtype, types::raw_bits);
        ASSERT_EQ(size, 3u);
        ASSERT_TRUE(types::parseDtypeName("V5", dtype, size));
        ASSERT_EQ(dtype, types::raw_bytes);
        ASSERT_EQ(size, 5u);
        for(const std::string bad : {"r0", "r12", "V0", "r", "raw_bits", "rx8"}) {
            ASSERT_FALSE(types::parseDtypeName(bad, dtype, size)) << bad;
        }

        for(const bool sharded : {false, true}) {
            fs::path p = tmp / (sharded ? "raw_sharded.zr" : "raw.zr");
            z5::filesystem::handle::File file(p);
            z5::createFile(file, true, 3);

            DatasetOptions options;
            options.zarrFormat = 3;
            options.fillValue = std::string("\x01\x02\x03");
            options.transposeOrder = {1, 0};
            if(sharded) {
                options.shardShape = types::ShapeType({4, 2});
            }
            auto ds = createDataset(file, "r", "r24", types::ShapeType({3, 2}), types::ShapeType({2, 1}), options);
            ASSERT_EQ(ds->getDtype(), types::raw_bits);
            ASSERT_EQ(ds->elementSize(), 3u);

            const std::vector<std::string> in{"abc", "def", "ghi", "jkl"};
            const types::ShapeType offset({1, 0});
            multiarray::writeSubarray<std::string>(*ds, multiarray::makeView(in.data(), types::ShapeType({2, 2})),
                                                   offset.begin());
            std::vector<std::string> out(6);
            const types::ShapeType zero({0, 0});
            multiarray::readSubarray<std::string>(*openDataset(file, "r"),
                                                  multiarray::makeView(out.data(), types::ShapeType({3, 2})),
                                                  zero.begin());
            const std::string f("\x01\x02\x03");
            ASSERT_EQ(out, std::vector<std::string>({f, f, "abc", "def", "ghi", "jkl"}));

            // an element of another size is refused
            const std::vector<std::string> bad{"ab"};
            ASSERT_THROW(multiarray::writeSubarray<std::string>(*ds, multiarray::makeView(bad.data(),
                                                                                         types::ShapeType({1, 1})),
                                                                zero.begin()), std::runtime_error);

            DatasetMetadata meta;
            filesystem::readMetadata(filesystem::handle::Dataset(ds->path(), ds->mode()), meta);
            nlohmann::json j;
            meta.toJson(j);
            ASSERT_EQ(j["data_type"], "r24");
            ASSERT_EQ(j["fill_value"], nlohmann::json::array({1, 2, 3}));
        }

        // a fill value of another size is refused
        DatasetOptions badFill;
        badFill.zarrFormat = 3;
        badFill.fillValue = std::string("ab");
        DatasetMetadata m;
        ASSERT_THROW(createDatasetMetadata("V3", types::ShapeType({4}), types::ShapeType({2}), true, badFill, m),
                     std::runtime_error);
    }


    TEST_F(FactoryTest, CreateDatasetExactFillValue) {
        for(const int zarrFormat : {2, 3}) {
            fs::path p = tmp / ("fill" + std::to_string(zarrFormat) + ".zr");
            z5::filesystem::handle::File file(p);
            z5::createFile(file, true, zarrFormat);

            // uint64 max has no exact double
            DatasetOptions options;
            options.zarrFormat = zarrFormat;
            options.fillValue = std::numeric_limits<uint64_t>::max();
            auto ds = createDataset(file, "u64", "uint64", types::ShapeType({10, 10}),
                                    types::ShapeType({5, 5}), options);
            uint64_t fill = 0;
            ds->getFillValue(&fill);
            ASSERT_EQ(fill, std::numeric_limits<uint64_t>::max());

            DatasetMetadata meta;
            filesystem::readMetadata(filesystem::handle::Dataset(ds->path(), ds->mode()), meta);
            ASSERT_EQ(std::get<uint64_t>(meta.fillValue), std::numeric_limits<uint64_t>::max());
            nlohmann::json j;
            meta.toJson(j);
            ASSERT_EQ(j["fill_value"].get<uint64_t>(), std::numeric_limits<uint64_t>::max());

            DatasetOptions neg;
            neg.zarrFormat = zarrFormat;
            neg.fillValue = std::numeric_limits<int64_t>::min() + 1;
            auto ds2 = createDataset(file, "i64", "int64", types::ShapeType({10, 10}),
                                     types::ShapeType({5, 5}), neg);
            int64_t fill2 = 0;
            ds2->getFillValue(&fill2);
            ASSERT_EQ(fill2, std::numeric_limits<int64_t>::min() + 1);

            // the fill value takes the data type's form: a double for an
            // integer type becomes an integer, an integer for a float type a double
            DatasetMetadata m;
            DatasetOptions asDouble;
            asDouble.zarrFormat = zarrFormat;
            asDouble.fillValue = 7.;
            createDatasetMetadata("uint8", types::ShapeType({10}), types::ShapeType({5}), true,
                                  asDouble, m);
            ASSERT_EQ(m.fillValue, types::FillValue(uint64_t(7)));
            DatasetOptions asInt;
            asInt.zarrFormat = zarrFormat;
            asInt.fillValue = int64_t(-3);
            createDatasetMetadata("float32", types::ShapeType({10}), types::ShapeType({5}), true,
                                  asInt, m);
            ASSERT_EQ(m.fillValue, types::FillValue(-3.));

            // a NaN payload becomes the default NaN
            DatasetOptions payload;
            payload.zarrFormat = zarrFormat;
            payload.fillValue = types::fillValueFromBits(types::float32, 0x7fc00001u);
            auto ds3 = createDataset(file, "nan", "float32", types::ShapeType({10}),
                                     types::ShapeType({5}), payload);
            float fill3 = 0;
            ds3->getFillValue(&fill3);
            ASSERT_EQ(std::bit_cast<uint32_t>(fill3), 0x7fc00000u);

            // out of range values, and bits wider than the data type, are refused
            DatasetOptions bad;
            bad.zarrFormat = zarrFormat;
            bad.fillValue = int64_t(256);
            ASSERT_THROW(createDataset(file, "bad", "uint8", types::ShapeType({10}),
                                       types::ShapeType({5}), bad), std::runtime_error);
            ASSERT_THROW(types::fillValueFromBits(types::float32, 0x100000000ull), std::runtime_error);
            bad.fillValue = std::numeric_limits<double>::quiet_NaN();
            ASSERT_THROW(createDataset(file, "bad", "int8", types::ShapeType({10}),
                                       types::ShapeType({5}), bad), std::runtime_error);
        }

        // zarr v3 refuses a non-integral fill value for an integer type; v2 truncates it
        fs::path p = tmp / "fill_frac.zr";
        z5::filesystem::handle::File file(p);
        z5::createFile(file, true, 3);
        DatasetOptions frac;
        frac.zarrFormat = 3;
        frac.fillValue = 0.5;
        ASSERT_THROW(createDataset(file, "frac", "int8", types::ShapeType({10}),
                                   types::ShapeType({5}), frac), std::runtime_error);
        frac.zarrFormat = 2;
        DatasetMetadata m;
        createDatasetMetadata("int8", types::ShapeType({10}), types::ShapeType({5}), true, frac, m);
        ASSERT_EQ(m.fillValue, types::FillValue(int64_t(0)));
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
