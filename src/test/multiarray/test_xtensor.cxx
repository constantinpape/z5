#include "gtest/gtest.h"

#include <random>
#include "xtensor/xarray.hpp"

#include "z5/dataset_factory.hxx"
#include "z5/multiarray/xtensor_access.hxx"

namespace fs = boost::filesystem;

namespace z5 {
namespace multiarray {

    // fixture for the zarr array test
    class XtensorTest : public ::testing::Test {

    protected:
        XtensorTest() :
            pathIntRegular_("int_regular.zr"), pathIntIrregular_("int_irregular.zr"),
            pathFloatRegular_("float_regular.zr"), pathFloatIrregular_("float_irregular.zr"),
            pathIntRegularN5_("int_regular.n5"), pathIntIrregularN5_("int_irregular.n5"),
            pathFloatRegularN5_("float_regular.n5"), pathFloatIrregularN5_("float_irregular.n5"),
            shape_({100, 100, 100}), chunkShapeRegular_({10, 10, 10}), chunkShapeIrregular_({23, 17, 11})
        {
        }

        ~XtensorTest() {
        }

        template<class T>
        void writeData(const std::unique_ptr<Dataset> & ds) {
            const auto & chunks = ds->chunksPerDimension();
            const auto & chunkShape = ds->maxChunkShape();
            std::vector<T> data(ds->maxChunkSize(), 42);
            for(std::size_t z = 0; z < chunks[0]; ++z) {
                for(std::size_t y = 0; y < chunks[1]; ++y) {
                    for(std::size_t x = 0; x < chunks[2]; ++x) {
                        ds->writeChunk(types::ShapeType({z, y, x}), &data[0]);
                    }
                }
            }
        }

        virtual void SetUp() {
            {
                // create arrays Zarr
                auto intReg = createDataset(pathIntRegular_, "int32", shape_, chunkShapeRegular_, true, "raw");
                writeData<int>(intReg);
                auto intIrreg = createDataset(pathIntIrregular_, "int32", shape_, chunkShapeIrregular_, true, "raw");
                writeData<int>(intIrreg);
                auto floatReg = createDataset(pathFloatRegular_, "float32", shape_, chunkShapeRegular_, true, "raw");
                writeData<float>(floatReg);
                auto floatIrreg = createDataset(pathFloatIrregular_, "float32", shape_, chunkShapeIrregular_, true, "raw");
                writeData<float>(floatIrreg);
            }
            {
                // create arrays n5
                auto intReg = createDataset(pathIntRegularN5_, "int32", shape_, chunkShapeRegular_, false, "raw");
                writeData<int>(intReg);
                auto intIrreg = createDataset(pathIntIrregularN5_, "int32", shape_, chunkShapeIrregular_, false, "raw");
                writeData<int>(intIrreg);
                auto floatReg = createDataset(pathFloatRegularN5_, "float32", shape_, chunkShapeRegular_, false, "raw");
                writeData<float>(floatReg);
                auto floatIrreg = createDataset(pathFloatIrregularN5_, "float32", shape_, chunkShapeIrregular_, false, "raw");
                writeData<float>(floatIrreg);
            }
        }

        virtual void TearDown() {
            // remove zarr paths
            {
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
            // remove n5 paths
            {
                // remove int arrays
                fs::path ireg(pathIntRegularN5_);
                fs::remove_all(ireg);
                fs::path iirreg(pathIntIrregularN5_);
                fs::remove_all(iirreg);
                // remove float arrays
                fs::path freg(pathFloatRegularN5_);
                fs::remove_all(freg);
                fs::path firreg(pathFloatIrregularN5_);
                fs::remove_all(firreg);
            }
        }

        template<typename T>
        void testArrayRead(std::unique_ptr<Dataset> & array) {
            typedef typename xt::xarray<int32_t>::shape_type ArrayShape;
            const auto & shape = array->shape();
            ArrayShape arrayShape(shape.begin(), shape.end());

            // load a completely overlapping array consisting of 8 chunks
            {
                types::ShapeType offset({0, 0, 0});
                ArrayShape subShape({20, 20, 20});
                //types::ShapeType subShape({10, 10, 10});
                xt::xarray<T> data(subShape);
                readSubarray<T>(array, data, offset.begin());

                for(int i = 0; i < subShape[0]; ++i) {
                    for(int j = 0; j < subShape[1]; ++j) {
                        for(int k = 0; k < subShape[2]; ++k) {
                            //std::cout << i << " " << j << " " << " " << k << std::endl;
                            ASSERT_EQ(data(i, j, k), 42);
                        }
                    }
                }
            }

            // load the complete array
            {
                types::ShapeType offset({0, 0, 0});
                xt::xarray<T> data(arrayShape);
                readSubarray<T>(array, data, offset.begin());

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
                //types::ShapeType offset({20, 30, 34});

                // draw the shape coordinates
                std::uniform_int_distribution<std::size_t> shape_xx(1, shape[0] - x);
                std::uniform_int_distribution<std::size_t> shape_yy(1, shape[1] - y);
                std::uniform_int_distribution<std::size_t> shape_zz(1, shape[2] - z);
                sx = shape_xx(gen);
                sy = shape_yy(gen);
                sz = shape_zz(gen);
                ArrayShape shape({sx, sy, sz});
                //types::ShapeType shape({40, 42, 56});

                //std::cout << "Random Request: " << t << " / " << N << std::endl;
                //std::cout << "Offset:" << std::endl;
                //std::cout << x << " " << y << " " << z << std::endl;
                //std::cout << "Shape:" << std::endl;
                //std::cout << sx << " " << sy << " " << sz << std::endl;

                xt::xarray<T> data(shape);
                readSubarray<T>(array, data, offset.begin());

                for(int i = 0; i < shape[0]; ++i) {
                    for(int j = 0; j < shape[1]; ++j) {
                        for(int k = 0; k < shape[2]; ++k) {
                            //std::cout << i << " " << j << " " << k << std::endl;
                            ASSERT_EQ(data(i, j, k), 42);
                        }
                    }
                }
            }
        }


        template<typename T, typename DISTR>
        void testArrayWriteRead(std::unique_ptr<Dataset> & array, DISTR & distr) {

            typedef typename xt::xarray<int32_t>::shape_type ArrayShape;
            const auto & shape = array->shape();
            ArrayShape arrayShape(shape.begin(), shape.end());

            std::default_random_engine gen;
            auto draw = std::bind(distr, gen);

            // write and read a completely overlapping array consisting of 8 chunks
            {
                types::ShapeType offset({0, 0, 0});
                ArrayShape subShape({20, 20, 20});

                // generate random in data
                xt::xarray<T> dataIn(subShape);
                for(auto it = dataIn.begin(); it != dataIn.end(); ++it) {
                    *it = draw();
                }
                writeSubarray<T>(array, dataIn, offset.begin());

                // read the out data
                xt::xarray<T> dataOut(subShape);
                readSubarray<T>(array, dataOut, offset.begin());
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
                xt::xarray<T> dataIn(arrayShape);
                for(auto it = dataIn.begin(); it != dataIn.end(); ++it) {
                    *it = draw();
                }
                writeSubarray<T>(array, dataIn, offset.begin());

                // read the out data
                xt::xarray<T> dataOut(arrayShape);
                readSubarray<T>(array, dataOut, offset.begin());

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
                ArrayShape shape({sx, sy, sz});

                //std::cout << "Offset:" << std::endl;
                //std::cout << x << " " << y << " " << z << std::endl;
                //std::cout << "Shape:" << std::endl;
                //std::cout << sx << " " << sy << " " << sz << std::endl;
                // generate random in data
                xt::xarray<T> dataIn(shape);
                for(auto it = dataIn.begin(); it != dataIn.end(); ++it) {
                    *it = draw();
                }
                writeSubarray<T>(array, dataIn, offset.begin());

                // read the out data
                xt::xarray<T> dataOut(shape);
                readSubarray<T>(array, dataOut, offset.begin());

                for(int i = 0; i < shape[0]; ++i) {
                    for(int j = 0; j < shape[1]; ++j) {
                        for(int k = 0; k < shape[2]; ++k) {
                            ASSERT_EQ(dataIn(i, j, k), dataOut(i, j, k));
                        }
                    }
                }
            }
        }

        // zarr paths
        std::string pathIntRegular_;
        std::string pathIntIrregular_;
        std::string pathFloatRegular_;
        std::string pathFloatIrregular_;
        // N5 paths
        std::string pathIntRegularN5_;
        std::string pathIntIrregularN5_;
        std::string pathFloatRegularN5_;
        std::string pathFloatIrregularN5_;

        types::ShapeType shape_;
        types::ShapeType chunkShapeRegular_;
        types::ShapeType chunkShapeIrregular_;
    };


    TEST_F(XtensorTest, TestThrow) {
        auto array = openDataset(pathIntRegular_);

        typedef typename xt::xarray<int32_t>::shape_type ArrayShape;
        // check for shape throws #0
        ArrayShape shape0({120, 120, 120});
        types::ShapeType offset0({0, 0, 0});
        xt::xarray<int32_t> sub0(shape0);
        ASSERT_THROW(readSubarray<int32_t>(array, sub0, offset0.begin()), std::runtime_error);

        // check for shape throws #1
        ArrayShape shape1({80, 80, 80});
        types::ShapeType offset1({30, 30, 30});
        xt::xarray<int32_t> sub1(shape1);
        ASSERT_THROW(readSubarray<int32_t>(array, sub1, offset1.begin()), std::runtime_error);

        // check for shape throws #2
        ArrayShape shape2({80, 80, 0});
        types::ShapeType offset2({0, 0, 0});
        xt::xarray<int32_t> sub2(shape2);
        ASSERT_THROW(readSubarray<int32_t>(array, sub2, offset2.begin()), std::runtime_error);

        ArrayShape shape({80, 80, 80});
        types::ShapeType offset({0, 0, 0});
        // check for dtype throws #0
        xt::xarray<int64_t> sub64(shape);
        ASSERT_THROW(readSubarray<int64_t>(array, sub64, offset.begin()), std::runtime_error);

        // check for dtype throws #1
        xt::xarray<uint32_t> subu(shape);
        ASSERT_THROW(readSubarray<uint32_t>(array, subu, offset.begin()), std::runtime_error);

        // check for dtype throws #2
        xt::xarray<float> subf(shape);
        ASSERT_THROW(readSubarray<float>(array, subf, offset.begin()), std::runtime_error);
    }

    TEST_F(XtensorTest, TestReadIntRegular) {
        // load the regular array and run the test
        auto array = openDataset(pathIntRegular_);
        testArrayRead<int32_t>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(pathIntRegularN5_);
        testArrayRead<int32_t>(arrayN5);
    }


    TEST_F(XtensorTest, TestReadFloatRegular) {
        // load the regular array and run the test
        auto array = openDataset(pathFloatRegular_);
        testArrayRead<float>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(pathFloatRegularN5_);
        testArrayRead<float>(arrayN5);
    }


    TEST_F(XtensorTest, TestReadIntIrregular) {
        // load the regular array and run the test
        auto array = openDataset(pathIntIrregular_);
        testArrayRead<int32_t>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(pathIntIrregularN5_);
        testArrayRead<int32_t>(arrayN5);
    }


    TEST_F(XtensorTest, TestReadFloatIrregular) {
        // load the regular array and run the test
        auto array = openDataset(pathFloatIrregular_);
        testArrayRead<float>(array);
        // load the regular array and run the test
        auto arrayN5 = openDataset(pathFloatIrregularN5_);
        testArrayRead<float>(arrayN5);
    }


    TEST_F(XtensorTest, TestWriteReadIntRegular) {
        auto array = openDataset(pathIntRegular_);
        std::uniform_int_distribution<int32_t> distr(-100, 100);
        testArrayWriteRead<int32_t>(array, distr);

        auto arrayN5 = openDataset(pathIntRegularN5_);
        testArrayWriteRead<int32_t>(arrayN5, distr);
    }


    TEST_F(XtensorTest, TestWriteReadFloatRegular) {
        auto array = openDataset(pathFloatRegular_);
        std::uniform_real_distribution<float> distr(0., 1.);
        testArrayWriteRead<float>(array, distr);

        auto arrayN5 = openDataset(pathFloatRegularN5_);
        testArrayWriteRead<float>(arrayN5, distr);
    }


    TEST_F(XtensorTest, TestWriteReadIntIrregular) {
        auto array = openDataset(pathIntIrregular_);
        std::uniform_int_distribution<int32_t> distr(-100, 100);
        testArrayWriteRead<int32_t>(array, distr);

        auto arrayN5 = openDataset(pathIntIrregularN5_);
        testArrayWriteRead<int32_t>(arrayN5, distr);
    }


    TEST_F(XtensorTest, TestWriteReadFloatIrregular) {
        auto array = openDataset(pathFloatIrregular_);
        std::uniform_real_distribution<float> distr(0., 1.);
        testArrayWriteRead<float>(array, distr);

        auto arrayN5 = openDataset(pathFloatIrregularN5_);
        testArrayWriteRead<float>(arrayN5, distr);
    }
}
}
