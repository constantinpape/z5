#ifdef WITH_MARRAY

#include "gtest/gtest.h"
#include <random>
#include "z5/dataset_factory.hxx"
#include "z5/multiarray/marray_access.hxx"

namespace fs = boost::filesystem;

namespace z5 {
namespace multiarray {

    // fixture for the zarr array test
    class MarrayTest : public ::testing::Test {

    protected:
        MarrayTest() :
            pathIntRegular_("int_regular.zr"), pathIntIrregular_("int_irregular.zr"),
            pathFloatRegular_("float_regular.zr"), pathFloatIrregular_("float_irregular.zr"),
            shape_({100, 100, 100}), chunkShapeRegular_({10, 10, 10}), chunkShapeIrregular_({23, 17, 11})
        {
        }

        ~MarrayTest() {
        }

        virtual void SetUp() {
            // create arrays
            auto intReg = createDataset(pathIntRegular_, "int32", shape_, chunkShapeRegular_, true);
            auto intIrreg = createDataset(pathIntIrregular_, "int32", shape_, chunkShapeIrregular_, true);
            auto floatReg = createDataset(pathFloatRegular_, "float32", shape_, chunkShapeRegular_, true);
            auto floatIrreg = createDataset(pathFloatIrregular_, "float32", shape_, chunkShapeIrregular_, true);

            // write regular test data
            {
                const auto & chunks = intReg->chunksPerDimension();
                const auto & chunkShape = intReg->maxChunkShape();
                std::vector<int32_t> dataInt(intReg->maxChunkSize(), 42);
                for(std::size_t x = 0; x < chunks[0]; ++x) {
                    for(std::size_t y = 0; y < chunks[1]; ++y) {
                        for(std::size_t z = 0; z < chunks[2]; ++z) {
                            intReg->writeChunk(types::ShapeType({x, y, z}), &dataInt[0]);
                        }
                    }
                }

                std::vector<float> dataFloat(floatReg->maxChunkSize(), 42.);
                for(std::size_t x = 0; x < chunks[0]; ++x) {
                    for(std::size_t y = 0; y < chunks[1]; ++y) {
                        for(std::size_t z = 0; z < chunks[2]; ++z) {
                            floatReg->writeChunk(types::ShapeType({x, y, z}), &dataFloat[0]);
                        }
                    }
                }
            }

            // write irregular test data
            {
                const auto & chunks = intIrreg->chunksPerDimension();
                const auto & chunkShape = intIrreg->maxChunkShape();
                std::vector<int32_t> dataInt(intIrreg->maxChunkSize(), 42);
                for(std::size_t x = 0; x < chunks[0]; ++x) {
                    for(std::size_t y = 0; y < chunks[1]; ++y) {
                        for(std::size_t z = 0; z < chunks[2]; ++z) {
                            intIrreg->writeChunk(types::ShapeType({x, y, z}), &dataInt[0]);
                        }
                    }
                }

                std::vector<float> dataFloat(floatIrreg->maxChunkSize(), 42.);
                for(std::size_t x = 0; x < chunks[0]; ++x) {
                    for(std::size_t y = 0; y < chunks[1]; ++y) {
                        for(std::size_t z = 0; z < chunks[2]; ++z) {
                            floatIrreg->writeChunk(types::ShapeType({x, y, z}), &dataFloat[0]);
                        }
                    }
                }
            }
        }

        virtual void TearDown() {
            // remove int arrays
            fs::path ireg(pathIntRegular_);
            fs::remove_all(ireg);
            fs::path iirreg(pathIntIrregular_);
            fs::remove_all(iirreg);
            // remove float arrays
            fs::path freg(pathFloatRegular_);
            fs::remove_all(freg);
            fs::path firreg(pathFloatIrregular_);
            fs::remove_all(firreg);
        }

        template<typename T>
        void testArrayRead(std::unique_ptr<Dataset> & array) {
            const auto & shape = array->shape();

            // load a completely overlapping array consisting of 8 chunks
            {
                types::ShapeType offset({0, 0, 0});
                types::ShapeType subShape({20, 20, 20});
                andres::Marray<T> data(subShape.begin(), subShape.end());
                readSubarray(array, data, offset.begin());

                for(int i = 0; i < subShape[0]; ++i) {
                    for(int j = 0; j < subShape[1]; ++j) {
                        for(int k = 0; k < subShape[2]; ++k) {
                            ASSERT_EQ(data(i, j, k), 42);
                        }
                    }
                }
            }

            // load the complete array
            {
                types::ShapeType offset({0, 0, 0});
                andres::Marray<T> data(shape.begin(), shape.end());
                readSubarray(array, data, offset.begin());

                for(int i = 0; i < shape[0]; ++i) {
                    for(int j = 0; j < shape[1]; ++j) {
                        for(int k = 0; k < shape[2]; ++k) {
                            ASSERT_EQ(data(i, j, k), 42);
                        }
                    }
                }
            }

            // load 25 random valid chunks and make sure that they
            // contain the correct data
            std::default_random_engine gen;
            std::uniform_int_distribution<std::size_t> xx(0, shape[0] - 2);
            std::uniform_int_distribution<std::size_t> yy(0, shape[1] - 2);
            std::uniform_int_distribution<std::size_t> zz(0, shape[2] - 2);
            std::size_t N = 25;
            std::size_t x, y, z;
            std::size_t sx, sy, sz;
            for(int t = 0; t < N; ++t) {

                // draw the offset coordinates
                x = xx(gen);
                y = yy(gen);
                z = zz(gen);
                types::ShapeType offset({x, y, z});

                // draw the shape coordinates
                std::uniform_int_distribution<std::size_t> shape_xx(1, shape[0] - x);
                std::uniform_int_distribution<std::size_t> shape_yy(1, shape[1] - y);
                std::uniform_int_distribution<std::size_t> shape_zz(1, shape[2] - z);
                sx = shape_xx(gen);
                sy = shape_yy(gen);
                sz = shape_zz(gen);
                types::ShapeType shape({sx, sy, sz});

                //std::cout << "Offset:" << std::endl;
                //std::cout << x << " " << y << " " << z << std::endl;
                //std::cout << "Shape:" << std::endl;
                //std::cout << sx << " " << sy << " " << sz << std::endl;

                andres::Marray<T> data(shape.begin(), shape.end());
                readSubarray(array, data, offset.begin());

                for(int i = 0; i < shape[0]; ++i) {
                    for(int j = 0; j < shape[1]; ++j) {
                        for(int k = 0; k < shape[2]; ++k) {
                            ASSERT_EQ(data(i, j, k), 42);
                        }
                    }
                }
            }
        }


        template<typename T, typename DISTR>
        void testArrayWriteRead(std::unique_ptr<Dataset> & array, DISTR & distr) {

            const auto & shape = array->shape();
            std::default_random_engine gen;
            auto draw = std::bind(distr, gen);

            // write and read a completely overlapping array consisting of 8 chunks
            {
                types::ShapeType offset({0, 0, 0});
                types::ShapeType subShape({20, 20, 20});

                // generate random in data
                andres::Marray<T> dataIn(subShape.begin(), subShape.end());
                for(auto it = dataIn.begin(); it != dataIn.end(); ++it) {
                    *it = draw();
                }
                writeSubarray(array, dataIn, offset.begin());

                // read the out data
                andres::Marray<T> dataOut(subShape.begin(), subShape.end());
                readSubarray(array, dataOut, offset.begin());
                for(int i = 0; i < subShape[0]; ++i) {
                    for(int j = 0; j < subShape[1]; ++j) {
                        for(int k = 0; k < subShape[2]; ++k) {
                            ASSERT_EQ(dataIn(i, j, k), dataOut(i, j, k));
                        }
                    }
                }
            }

            // load the complete array
            {
                types::ShapeType offset({0, 0, 0});

                // generate random in data
                andres::Marray<T> dataIn(shape.begin(), shape.end());
                for(auto it = dataIn.begin(); it != dataIn.end(); ++it) {
                    *it = draw();
                }
                writeSubarray(array, dataIn, offset.begin());

                // read the out data
                andres::Marray<T> dataOut(shape.begin(), shape.end());
                readSubarray(array, dataOut, offset.begin());

                for(int i = 0; i < shape[0]; ++i) {
                    for(int j = 0; j < shape[1]; ++j) {
                        for(int k = 0; k < shape[2]; ++k) {
                            ASSERT_EQ(dataIn(i, j, k), dataOut(i, j, k));
                        }
                    }
                }
            }

            // load 25 random valid chunks and make sure that they
            // contain the correct data
            std::uniform_int_distribution<std::size_t> xx(0, shape[0] - 2);
            std::uniform_int_distribution<std::size_t> yy(0, shape[1] - 2);
            std::uniform_int_distribution<std::size_t> zz(0, shape[2] - 2);
            std::size_t N = 25;
            std::size_t x, y, z;
            std::size_t sx, sy, sz;
            for(int t = 0; t < N; ++t) {

                // draw the offset coordinates
                x = xx(gen);
                y = yy(gen);
                z = zz(gen);
                types::ShapeType offset({x, y, z});

                // draw the shape coordinates
                std::uniform_int_distribution<std::size_t> shape_xx(1, shape[0] - x);
                std::uniform_int_distribution<std::size_t> shape_yy(1, shape[1] - y);
                std::uniform_int_distribution<std::size_t> shape_zz(1, shape[2] - z);
                sx = shape_xx(gen);
                sy = shape_yy(gen);
                sz = shape_zz(gen);
                types::ShapeType shape({sx, sy, sz});

                //std::cout << "Offset:" << std::endl;
                //std::cout << x << " " << y << " " << z << std::endl;
                //std::cout << "Shape:" << std::endl;
                //std::cout << sx << " " << sy << " " << sz << std::endl;
                // generate random in data
                andres::Marray<T> dataIn(shape.begin(), shape.end());
                for(auto it = dataIn.begin(); it != dataIn.end(); ++it) {
                    *it = draw();
                }
                writeSubarray(array, dataIn, offset.begin());

                // read the out data
                andres::Marray<T> dataOut(shape.begin(), shape.end());
                readSubarray(array, dataOut, offset.begin());

                for(int i = 0; i < shape[0]; ++i) {
                    for(int j = 0; j < shape[1]; ++j) {
                        for(int k = 0; k < shape[2]; ++k) {
                            ASSERT_EQ(dataIn(i, j, k), dataOut(i, j, k));
                        }
                    }
                }
            }
        }

        std::string pathIntRegular_;
        std::string pathIntIrregular_;
        std::string pathFloatRegular_;
        std::string pathFloatIrregular_;

        types::ShapeType shape_;
        types::ShapeType chunkShapeRegular_;
        types::ShapeType chunkShapeIrregular_;
    };


    TEST_F(MarrayTest, TestThrow) {
        auto array = openDataset(pathIntRegular_);

        // check for shape throws #0
        types::ShapeType shape0({120, 120, 120});
        types::ShapeType offset0({0, 0, 0});
        andres::Marray<int32_t> sub0(shape0.begin(), shape0.end());
        ASSERT_THROW(readSubarray(array, sub0, offset0.begin()), std::runtime_error);

        // check for shape throws #1
        types::ShapeType shape1({80, 80, 80});
        types::ShapeType offset1({30, 30, 30});
        andres::Marray<int32_t> sub1(shape1.begin(), shape1.end());
        ASSERT_THROW(readSubarray(array, sub1, offset1.begin()), std::runtime_error);

        // check for shape throws #2
        types::ShapeType shape2({80, 80, 0});
        types::ShapeType offset2({0, 0, 0});
        andres::Marray<int32_t> sub2(shape2.begin(), shape2.end());
        ASSERT_THROW(readSubarray(array, sub2, offset2.begin()), std::runtime_error);

        types::ShapeType shape({80, 80, 80});
        types::ShapeType offset({0, 0, 0});
        // check for dtype throws #0
        andres::Marray<int64_t> sub64(shape.begin(), shape.end());
        ASSERT_THROW(readSubarray(array, sub64, offset.begin()), std::runtime_error);

        // check for dtype throws #1
        andres::Marray<uint32_t> subu(shape.begin(), shape.end());
        ASSERT_THROW(readSubarray(array, subu, offset.begin()), std::runtime_error);

        // check for dtype throws #2
        andres::Marray<float> subf(shape.begin(), shape.end());
        ASSERT_THROW(readSubarray(array, subf, offset.begin()), std::runtime_error);
    }


    TEST_F(MarrayTest, TestReadIntRegular) {
        // load the regular array and run the test
        auto array = openDataset(pathIntRegular_);
        testArrayRead<int32_t>(array);
    }


    TEST_F(MarrayTest, TestReadFloatRegular) {
        // load the regular array and run the test
        auto array = openDataset(pathFloatRegular_);
        testArrayRead<float>(array);
    }


    TEST_F(MarrayTest, TestReadIntIrregular) {
        // load the regular array and run the test
        auto array = openDataset(pathIntIrregular_);
        testArrayRead<int32_t>(array);
    }


    TEST_F(MarrayTest, TestReadFloatIrregular) {
        // load the regular array and run the test
        auto array = openDataset(pathFloatIrregular_);
        testArrayRead<float>(array);
    }


    TEST_F(MarrayTest, TestWriteReadIntRegular) {
        auto array = openDataset(pathIntRegular_);
        std::uniform_int_distribution<int32_t> distr(-100, 100);
        testArrayWriteRead<int32_t>(array, distr);
    }


    TEST_F(MarrayTest, TestWriteReadFloatRegular) {
        auto array = openDataset(pathFloatRegular_);
        std::uniform_real_distribution<float> distr(0., 1.);
        testArrayWriteRead<float>(array, distr);
    }


    TEST_F(MarrayTest, TestWriteReadIntIrregular) {
        auto array = openDataset(pathIntIrregular_);
        std::uniform_int_distribution<int32_t> distr(-100, 100);
        testArrayWriteRead<int32_t>(array, distr);
    }


    TEST_F(MarrayTest, TestWriteReadFloatIrregular) {
        auto array = openDataset(pathFloatIrregular_);
        std::uniform_real_distribution<float> distr(0., 1.);
        testArrayWriteRead<float>(array, distr);
    }
}
}
#endif
