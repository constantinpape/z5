#include "gtest/gtest.h"

#include <random>
#include "xtensor/xarray.hpp"

#include "z5/dataset_factory.hxx"
#include "z5/multiarray/xtensor_access.hxx"

namespace fs = boost::filesystem;

namespace z5 {
namespace multiarray {

    // fixture for the zarr array test
    class XtensorNDTest : public ::testing::Test {

    protected:
        XtensorNDTest() : path_("test.n5"), size_(100), chunkSize_(10){
        }

        auto writeData(const size_t dim) {
            std::vector<size_t> shape(dim, size_);
            std::vector<size_t> chunkShape(dim, chunkSize_);
            // create the dataset
            auto ds = createDataset(path_, "int32", shape, chunkShape, false, 0, "raw");
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

        void testArrayRead(const size_t dim) {
            auto ds = writeData(dim);
            const auto & shape = ds->shape();

            std::cout << "Run test data " << dim << " d" << std::endl;
            // load a completely overlapping array consisting of 8 chunks
            {
                types::ShapeType offset(dim, 0);
                types::ShapeType subShape(dim, 20);
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
                xt::xarray<int32_t> data(shape);
                readSubarray<int32_t>(ds, data, offset.begin());

                for(const auto elem: data) {
                    ASSERT_EQ(elem, 42);
                }
            }
            std::cout << "done" << std::endl;

        }


        void testArrayWriteRead(const size_t dim) {

            // create the dataset
            std::vector<size_t> shape(dim, size_);
            std::vector<size_t> chunkShape(dim, chunkSize_);
            auto ds = createDataset(path_, "int32", shape, chunkShape, false, 0, "raw");

            // random number generator
            std::uniform_int_distribution<int32_t> distr;
            std::default_random_engine gen;
            auto draw = std::bind(distr, gen);

            // write and read a completely overlapping array consisting of 8 chunks
            {
                types::ShapeType offset(dim, 0);
                types::ShapeType subShape(dim, 20);

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
        size_t size_;
        size_t chunkSize_;
    };

    // FIXME N > 3 segfaults
    TEST_F(XtensorNDTest, TestRead) {
        //for(size_t dim = 1; dim < 6; ++dim) {
        //for(size_t dim = 4; dim < 6; ++dim) {
        for(size_t dim = 1; dim < 4; ++dim) {
            testArrayRead(dim);
            // remove array
            fs::path path(path_);
            fs::remove_all(path);
        }
    }

    // FIXME N > 3 segfaults
    TEST_F(XtensorNDTest, TestWriteRead) {
        //for(size_t dim = 1; dim < 6; ++dim) {
        //for(size_t dim = 4; dim < 6; ++dim) {
        for(size_t dim = 1; dim < 4; ++dim) {
            testArrayWriteRead(dim);
            // remove array
            fs::path path(path_);
            fs::remove_all(path);
        }
    }
}
}
