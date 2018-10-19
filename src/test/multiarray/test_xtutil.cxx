#include <random>
#include "gtest/gtest.h"
#include "z5/dataset.hxx"
#include "z5/multiarray/xtensor_util.hxx"


namespace z5 {
namespace multiarray {

    TEST(XtUtilTest, TestCopyBufferToView) {

        const int minDim = 2;
        const int maxDim = 6;

        for(int dim = minDim; dim <= maxDim; ++dim) {

            std::cout << "Dimension: " << dim << std::endl;
            // shape of the array, the view into the array
            // and the view's offset
            std::vector<int64_t> shape(dim);
            std::vector<std::size_t> viewShape(dim);
            std::vector<std::size_t> viewOffset(dim);

            // we choose smaller shapes for d > 3, because 100^4 = 10^8
            // is pretty big already (and 10^10 even worse...)
            if(dim > 3) {
                for(int d = 0; d < dim; ++d) {
                    shape[d] = (d < 2) ? 3 : 100;
                    // TODO random values for view shape and offset
                    viewShape[d] = (d < 2) ? 2 : 40;
                    viewOffset[d] = (d < 2) ? 1 : 20;
                }
            }
            else {
                shape = std::vector<int64_t>(dim, 100);
                viewShape = std::vector<std::size_t>(dim, 40);
                viewOffset = std::vector<std::size_t>(dim, 20);
            }

            // make the big array and view
            xt::xarray<int> array = xt::zeros<int>(shape);
            xt::slice_vector slice;
            sliceFromRoi(slice, viewOffset, viewShape);
            auto view = xt::strided_view(array, slice);

            // make 1D buffer
            std::vector<int> buffer(view.size());
            std::iota(buffer.begin(), buffer.end(), 0);

            // copy the buffer into the array
            copyBufferToViewND(buffer, view, array.strides());

            // check the result
            std::size_t i = 0;
            for(auto viewIt = view.begin(); viewIt != view.end(); ++viewIt, ++i) {
                ASSERT_EQ(*viewIt, buffer[i]);
            }
        }

    }


    TEST(XtUtilTest, TestCopyViewToBuffer) {

        const int minDim = 2;
        const int maxDim = 6;

        for(int dim = minDim; dim <= maxDim; ++dim) {

            std::cout << "Dimension: " << dim << std::endl;
            // shape of the array, the view into the array
            // and the view's offset
            std::vector<int64_t> shape(dim);
            std::vector<std::size_t> viewShape(dim);
            std::vector<std::size_t> viewOffset(dim);
            for(int d = 0; d < dim; ++d) {
                // we choose smaller shapes for d > 3, because 100^4 = 10^8
                // is prettyp big already (and 10^10 even worse...)
                shape[d] = (d < 3) ? 100 : 3;
                // TODO random values for view shape and offset
                viewShape[d] = (d < 3) ? 40 : 2;
                viewOffset[d] = (d < 3) ? 20 : 1;
            }

            // make the big array and view
            xt::xarray<int> array = xt::zeros<int>(shape);
            xt::slice_vector slice;
            sliceFromRoi(slice, viewOffset, viewShape);
            auto view = xt::strided_view(array, slice);
            std::iota(view.begin(), view.end(), 0);

            // make 1D buffer
            std::vector<int> buffer(view.size());

            // copy the buffer into the array
            copyViewToBufferND(view, buffer, array.strides());

            // check the result
            std::size_t i = 0;
            for(auto viewIt = view.begin(); viewIt != view.end(); ++viewIt, ++i) {
                ASSERT_EQ(*viewIt, buffer[i]);
            }
        }

    }


}
}
