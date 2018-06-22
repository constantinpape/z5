#include <random>
#include "gtest/gtest.h"
#include "z5/util/util.hxx"


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
                            for(size_t c = minCoords[4]; c <= maxCoords[4]; ++t) {
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

        for(size_t dim = 1; dim < nDimTest; ++dim) {
            // std::cout << "Dim: " << dim << std::endl;
            for(size_t i = 0; i < nReps; ++i) {

                // std::cout << "Rep: " << std::endl;
                std::vector<size_t> minCoords(dim);
                std::vector<size_t> maxCoords(dim);
                for(int d = 0; d < dim; ++d) {
                    minCoords[d] = distr1(generator);
                    std::uniform_int_distribution<std::size_t> distr2(minCoords[d] + 1, 102);
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

}
}
