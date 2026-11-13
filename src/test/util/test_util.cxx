#include <random>
#include "gtest/gtest.h"
#include "z5/util/util.hxx"
#include "z5/util/blocking.hxx"
#include "z5/util/base64.hxx"
#include "z5/types/float16.hxx"


namespace test_util_detail {
	// explicit function to generate test data
    inline void makeRegularGridTest(const z5::types::ShapeType & minCoords,
                                    const z5::types::ShapeType & maxCoords,
                                    std::vector<z5::types::ShapeType> & grid) {
        size_t nDim = minCoords.size();
        if(nDim == 1) {
            for(size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                grid.emplace_back(z5::types::ShapeType({x}));
            }
        }

        else if(nDim == 2) {
            for(size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    grid.emplace_back(z5::types::ShapeType({x, y}));
                }
            }
        }

        else if(nDim == 3) {
            for(size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    for(size_t z = minCoords[2]; z <= maxCoords[2]; ++z) {
                        grid.emplace_back(z5::types::ShapeType({x, y, z}));
                    }
                }
            }
        }

        else if(nDim == 4) {
            for(size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    for(size_t z = minCoords[2]; z <= maxCoords[2]; ++z) {
                        for(size_t t = minCoords[3]; t <= maxCoords[3]; ++t) {
                            grid.emplace_back(z5::types::ShapeType({x, y, z, t}));
                        }
                    }
                }
            }
        }

        else if(nDim == 5) {
            for(size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    for(size_t z = minCoords[2]; z <= maxCoords[2]; ++z) {
                        for(size_t t = minCoords[3]; t <= maxCoords[3]; ++t) {
                            for(size_t c = minCoords[4]; c <= maxCoords[4]; ++c) {
                                grid.emplace_back(z5::types::ShapeType({x, y, z, t, c}));
                            }
                        }
                    }
                }
            }
        }
    }
}


namespace z5 {
namespace util {

    TEST(RegularGridTest, Test2DExplicit) {
        std::vector<size_t> minCoords = {0, 3};
        std::vector<size_t> maxCoords = {4, 5};

        std::vector<std::vector<size_t>> out;
        std::vector<std::vector<size_t>> expected = {{0, 3}, {0, 4}, {0, 5},
                                                     {1, 3}, {1, 4}, {1, 5},
                                                     {2, 3}, {2, 4}, {2, 5},
                                                     {3, 3}, {3, 4}, {3, 5},
                                                     {4, 3}, {4, 4}, {4, 5}};
        makeRegularGrid(minCoords, maxCoords, out);
        EXPECT_EQ(out.size(), expected.size());
        for(size_t i = 0; i < out.size(); ++i) {
            EXPECT_EQ(out[i], expected[i]);
        }
    }

    TEST(RegularGridTest, TestND) {
        const size_t nDimTest = 5;
        const size_t nReps = 10;

        std::default_random_engine generator;
        std::uniform_int_distribution<std::size_t> distr1(0, 100);

        for(size_t dim = 1; dim <= nDimTest; ++dim) {
            // bound the per-dimension extent so the grid size stays manageable
            // for the high-dimensional cases (8^5 = 32768 points at most)
            const std::size_t maxExtent = (dim <= 3) ? 102 : 8;
            for(size_t i = 0; i < nReps; ++i) {

                std::vector<size_t> minCoords(dim);
                std::vector<size_t> maxCoords(dim);
                for(int d = 0; d < dim; ++d) {
                    minCoords[d] = distr1(generator);
                    std::uniform_int_distribution<std::size_t> distr2(minCoords[d] + 1,
                                                                      minCoords[d] + maxExtent);
                    maxCoords[d] = distr2(generator);
                }

                std::vector<std::vector<size_t>> out;
                // std::cout << "Run function" << std::endl;
                makeRegularGrid(minCoords, maxCoords, out);

                std::vector<std::vector<size_t>> expected;
                // std::cout << "Run test function" << std::endl;
                test_util_detail::makeRegularGridTest(minCoords, maxCoords, expected);
                // std::cout << "done" << std::endl;

                EXPECT_EQ(out.size(), expected.size());
                for(size_t i = 0; i < out.size(); ++i) {
                    EXPECT_EQ(out[i], expected[i]);
                }
            }
        }
    }


    // a zero-dimensional grid is a single point with an empty coordinate
    TEST(RegularGridTest, ZeroDim) {
        std::vector<std::vector<size_t>> out;
        makeRegularGrid({}, {}, out);
        ASSERT_EQ(out.size(), 1u);
        EXPECT_TRUE(out[0].empty());
    }

    // a zero-dimensional shape has one block with an empty coordinate and shape
    TEST(BlockingTest, ZeroDim) {
        const Blocking blocking({}, {});
        EXPECT_EQ(blocking.numberOfBlocks(), 1u);
        EXPECT_TRUE(blocking.blocksPerDimension().empty());

        types::ShapeType coord{7}, begin{7}, shape{7};
        blocking.blockIdToBlockCoordinate(0, coord);
        EXPECT_TRUE(coord.empty());
        EXPECT_EQ(blocking.blockCoordinatesToBlockId(coord), 0u);
        EXPECT_TRUE(blocking.checkBlockCoordinate(coord));
        blocking.getBlockBeginAndShape(coord, begin, shape);
        EXPECT_TRUE(begin.empty());
        EXPECT_TRUE(shape.empty());

        std::vector<types::ShapeType> blocks;
        blocking.getBlocksOverlappingRoi({}, {}, blocks);
        ASSERT_EQ(blocks.size(), 1u);
        EXPECT_TRUE(blocks[0].empty());

        types::ShapeType beginInRoi, shapeInRoi, beginInBlock;
        EXPECT_TRUE(blocking.getCoordinatesInRoi(coord, {}, {}, beginInRoi, shapeInRoi, beginInBlock));
    }

    // base64 as zarr-python writes bytes fill values (RFC 4648, padded)
    TEST(Base64Test, RoundTrip) {
        const std::vector<std::pair<std::string, std::string>> cases{
            {"", ""}, {"a", "YQ=="}, {"ab", "YWI="}, {"abc", "YWJj"},
            {std::string("\x00\xff\x01", 3), "AP8B"}, {std::string(1, '\xfb'), "+w=="}, {"\xff\xff", "//8="}};
        for(const auto & c : cases) {
            EXPECT_EQ(base64Encode(c.first), c.second);
            EXPECT_EQ(base64Decode(c.second), c.first);
        }
        std::string all;
        for(int i = 0; i < 256; ++i) {
            all += static_cast<char>(i);
        }
        EXPECT_EQ(base64Decode(base64Encode(all)), all);
    }

    TEST(Base64Test, Invalid) {
        for(const std::string bad : {"YQ", "YQ=", "Y===", "YQ==YQ==", "YW!=", "=YWI", "YW=I"}) {
            EXPECT_THROW(base64Decode(bad), std::runtime_error) << bad;
        }
    }

    // every half survives a round trip through double (NaNs become the
    // default quiet NaN, keeping the sign)
    TEST(Float16Test, RoundTrip) {
        for(uint32_t bits = 0; bits <= 0xffffu; ++bits) {
            const uint16_t half = static_cast<uint16_t>(bits);
            const double d = types::float16ToDouble(half);
            const bool isNan = (half & 0x7c00u) == 0x7c00u && (half & 0x3ffu) != 0;
            ASSERT_EQ(std::isnan(d), isNan) << bits;
            const uint16_t expected = isNan ? static_cast<uint16_t>((half & 0x8000u) | 0x7e00u) : half;
            ASSERT_EQ(types::doubleToFloat16(d), expected) << bits;
        }
    }

    // round to nearest, ties to even (expected bits from numpy)
    TEST(Float16Test, Rounding) {
        const std::vector<std::pair<double, uint16_t>> cases{
            {1., 0x3c00}, {-2., 0xc000}, {0.1, 0x2e66}, {1. / 3., 0x3555},
            {1. + std::ldexp(1., -11), 0x3c00}, {1. + 3 * std::ldexp(1., -11), 0x3c02},
            {65504., 0x7bff}, {65519.99, 0x7bff}, {65520., 0x7c00}, {1e6, 0x7c00}, {-1e6, 0xfc00},
            {std::ldexp(1., -24), 0x0001}, {std::ldexp(1., -25), 0x0000},
            {std::ldexp(1., -25) + std::ldexp(1., -40), 0x0001}, {3 * std::ldexp(1., -26), 0x0001},
            {std::ldexp(1., -14), 0x0400}, {std::ldexp(1., -14) - std::ldexp(1., -25), 0x0400},
            {1e-10, 0x0000}, {-0., 0x8000}, {INFINITY, 0x7c00}, {-INFINITY, 0xfc00}};
        for(const auto & c : cases) {
            EXPECT_EQ(types::doubleToFloat16(c.first), c.second) << c.first;
        }
    }

}
}
