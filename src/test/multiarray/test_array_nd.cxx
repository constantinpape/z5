#include "gtest/gtest.h"

#include <random>

#include "z5/factory.hxx"
#include "z5/multiarray/array_access.hxx"

#include "array_helper.hxx"

#define MIN_DIM 1
#define MAX_DIM 6

namespace z5 {
namespace multiarray {

    // fixture for the n-dimensional array IO test
    class ArrayNDTest : public ::testing::Test {

    protected:
        ArrayNDTest() : path_("test.n5"), size_(100), chunkSize_(10){
        }

        auto writeData(const std::size_t dim) {
            // need smaller shapes for dim > 3
            std::vector<std::size_t> shape, chunkShape;
            if(dim < 4) {
                shape = std::vector<std::size_t>(dim, size_);
                chunkShape = std::vector<size_t>(dim, chunkSize_);
            } else {
                for(int d = 0; d < dim; ++d) {
                    shape.push_back((d < 3) ? size_ : 3);
                    chunkShape.push_back((d < 3) ? chunkSize_ : 1);
                }
            }
            // create the dataset
            z5::filesystem::handle::File f(path_);
            z5::createFile(f, false);
            auto ds = createDataset(f, "data", "int32", shape, chunkShape, "raw");
            // write the data
            std::vector<int32_t> data(ds->defaultChunkSize(), 42);
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
            auto ds = writeData(dim);
            const auto & shape = ds->shape();

            // load a completely overlapping sub-array
            {
                types::ShapeType offset(dim, 0);
                types::ShapeType subShape(dim, 20);
                if(dim > 3) {
                    for(int d = 3; d < dim; ++d) {
                        subShape[d] = 2;
                    }
                }

                TestArray<int32_t> data(subShape);
                readSubarray<int32_t>(ds, data.view(), offset.begin());

                for(const auto elem : data.data) {
                    ASSERT_EQ(elem, 42);
                }
            }

            // load the complete array
            {
                types::ShapeType offset(dim, 0);
                TestArray<int32_t> data(types::ShapeType(shape.begin(), shape.end()));
                readSubarray<int32_t>(ds, data.view(), offset.begin());

                for(const auto elem : data.data) {
                    ASSERT_EQ(elem, 42);
                }
            }
        }


        void testArrayWriteRead(const std::size_t dim) {

            // need smaller shapes for dim > 3
            std::vector<std::size_t> shape, chunkShape;
            if(dim < 4) {
                shape = std::vector<std::size_t>(dim, size_);
                chunkShape = std::vector<size_t>(dim, chunkSize_);
            } else {
                for(int d = 0; d < dim; ++d) {
                    shape.push_back((d < 3) ? size_ : 3);
                    chunkShape.push_back((d < 3) ? chunkSize_ : 1);
                }
            }

            // create the dataset
            z5::filesystem::handle::File f(path_);
            z5::createFile(f, false);
            auto ds = createDataset(f, "data", "int32", shape, chunkShape, "raw");

            // random number generator
            std::uniform_int_distribution<int32_t> distr;
            std::default_random_engine gen;
            auto draw = std::bind(distr, gen);

            // write and read a completely overlapping sub-array
            {
                types::ShapeType offset(dim, 0);
                types::ShapeType subShape(dim, 20);
                if(dim > 3) {
                    for(int d = 3; d < dim; ++d) {
                        subShape[d] = 2;
                    }
                }

                TestArray<int32_t> dataIn(subShape);
                for(auto & v : dataIn.data) {
                    v = draw();
                }
                writeSubarray<int32_t>(ds, dataIn.cview(), offset.begin());

                TestArray<int32_t> dataOut(subShape);
                readSubarray<int32_t>(ds, dataOut.view(), offset.begin());
                ASSERT_EQ(dataIn.data, dataOut.data);
            }

            // write and read the complete array
            {
                types::ShapeType offset(dim, 0);

                TestArray<int32_t> dataIn(types::ShapeType(shape.begin(), shape.end()));
                for(auto & v : dataIn.data) {
                    v = draw();
                }
                writeSubarray<int32_t>(ds, dataIn.cview(), offset.begin());

                TestArray<int32_t> dataOut(types::ShapeType(shape.begin(), shape.end()));
                readSubarray<int32_t>(ds, dataOut.view(), offset.begin());
                ASSERT_EQ(dataIn.data, dataOut.data);
            }
        }

        fs::path path_;
        std::size_t size_;
        std::size_t chunkSize_;
    };

    TEST_F(ArrayNDTest, TestRead) {
        for(std::size_t dim = MIN_DIM; dim <= MAX_DIM; ++dim) {
            testArrayRead(dim);
            fs::remove_all(path_);
        }
    }

    TEST_F(ArrayNDTest, TestWriteRead) {
        for(std::size_t dim = MIN_DIM; dim <= MAX_DIM; ++dim) {
            testArrayWriteRead(dim);
            fs::remove_all(path_);
        }
    }
}
}
