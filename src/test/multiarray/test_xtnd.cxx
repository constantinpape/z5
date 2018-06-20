#include "gtest/gtest.h"

#include <random>
#include "xtensor/xarray.hpp"

#include "z5/dataset_factory.hxx"
#include "z5/multiarray/xtensor_access.hxx"

namespace fs = boost::filesystem;

#define MIN_DIM 1
#define MAX_DIM 4

namespace z5 {
namespace multiarray {

    // fixture for the zarr array test
    class XtensorNDTest : public ::testing::Test {

    protected:
        XtensorNDTest() : path_("test.n5"), size_(100), chunkSize_(10){
        }

        auto writeData(const std::size_t dim) {
            // std::vector<std::size_t> shape(dim, (dim < 5) ? size_ : 20);
            std::vector<std::size_t> shape(dim, size_);
            std::vector<std::size_t> chunkShape(dim, chunkSize_);
            // create the dataset
            auto ds = createDataset(path_, "int32", shape, chunkShape, false, "raw");
            // write the data
            std::vector<int32_t> data(ds->maxChunkSize(), 42);
            const auto & chunksPerDim = ds->chunksPerDimension();
            types::ShapeType chunkId(dim);
            // write all the chunks (ND)
            for(int d = 0; d < dim;) {
                ds->writeChunk(chunkId, &data[0]);
                for(d = 0; d < dim; ++d) {
                    chunkId[d] += 1;
                    if(chunkId[d] < chunksPerDim[d]) {
                        break;
                    }
                    else {
                        chunkId[d] = 0;
                    }
                }
            }
            return ds;
        }

        void testArrayRead(const std::size_t dim) {
            typedef typename xt::xarray<int32_t>::shape_type ArrayShape;
            auto ds = writeData(dim);
            const auto & shape = ds->shape();
            ArrayShape arrayShape(shape.begin(), shape.end());

            std::cout << "Run test data " << dim << " d" << std::endl;
            // load a completely overlapping array consisting of 8 chunks
            {
                types::ShapeType offset(dim, 0);
                ArrayShape subShape(dim, 20);
                xt::xarray<int32_t> data(subShape);
                //std::cout << "Read..." << std::endl;
                readSubarray<int32_t>(ds, data, offset.begin());
                //std::cout << "... done" << std::endl;

                for(const auto elem: data) {
                    ASSERT_EQ(elem, 42);
                }
            }
            std::cout << "done" << std::endl;

            std::cout << "Run test data full" << dim << " d" << std::endl;
            // load the complete array
            {
                types::ShapeType offset(dim, 0);
                xt::xarray<int32_t> data(arrayShape);
                readSubarray<int32_t>(ds, data, offset.begin());

                for(const auto elem: data) {
                    ASSERT_EQ(elem, 42);
                }
            }
            std::cout << "done" << std::endl;

        }


        void testArrayWriteRead(const std::size_t dim) {

            typedef typename xt::xarray<int32_t>::shape_type ArrayShape;
            // create the dataset
            ArrayShape shape(dim, size_);
            types::ShapeType dsShape(dim, size_);
            std::vector<std::size_t> chunkShape(dim, chunkSize_);
            auto ds = createDataset(path_, "int32", dsShape, chunkShape, false, "raw");

            // random number generator
            std::uniform_int_distribution<int32_t> distr;
            std::default_random_engine gen;
            auto draw = std::bind(distr, gen);

            // write and read a completely overlapping array consisting of 8 chunks
            {
                types::ShapeType offset(dim, 0);
                ArrayShape subShape(dim, 20);

                // generate random in data
                xt::xarray<int32_t> dataIn(subShape);
                for(auto it = dataIn.begin(); it != dataIn.end(); ++it) {
                    *it = draw();
                }
                writeSubarray<int32_t>(ds, dataIn, offset.begin());

                // read the out data
                xt::xarray<int32_t> dataOut(subShape);
                readSubarray<int32_t>(ds, dataOut, offset.begin());

                auto itIn = dataIn.begin();
                auto itOut = dataOut.begin();
                for(; itIn != dataIn.end(); ++itIn, ++itOut) {
                    ASSERT_EQ(*itIn, *itOut);
                }
            }

            // load the complete array
            {
                types::ShapeType offset(dim, 0);

                // generate random in data
                xt::xarray<int32_t> dataIn(shape);
                for(auto it = dataIn.begin(); it != dataIn.end(); ++it) {
                    *it = draw();
                }
                writeSubarray<int32_t>(ds, dataIn, offset.begin());

                // read the out data
                xt::xarray<int32_t> dataOut(shape);
                readSubarray<int32_t>(ds, dataOut, offset.begin());

                auto itIn = dataIn.begin();
                auto itOut = dataOut.begin();
                for(; itIn != dataIn.end(); ++itIn, ++itOut) {
                    ASSERT_EQ(*itIn, *itOut);
                }
            }
        }

        std::string path_;
        std::size_t size_;
        std::size_t chunkSize_;
    };

    TEST_F(XtensorNDTest, TestRead) {
        for(std::size_t dim = MIN_DIM; dim <= MAX_DIM; ++dim) {
            testArrayRead(dim);
            // remove array
            fs::path path(path_);
            fs::remove_all(path);
        }
    }

    TEST_F(XtensorNDTest, TestWriteRead) {
        for(std::size_t dim = MIN_DIM; dim <= MAX_DIM; ++dim) {
            testArrayWriteRead(dim);
            // remove array
            fs::path path(path_);
            fs::remove_all(path);
        }
    }
}
}
