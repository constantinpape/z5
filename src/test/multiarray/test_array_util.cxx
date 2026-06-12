#include <numeric>
#include <vector>

#include "gtest/gtest.h"

#include "z5/types/types.hxx"
#include "z5/multiarray/array_view.hxx"
#include "z5/multiarray/array_util.hxx"


namespace z5 {
namespace multiarray {

    namespace {

        inline std::size_t prod(const types::ShapeType & s) {
            return std::accumulate(s.begin(), s.end(),
                                   static_cast<std::size_t>(1), std::multiplies<std::size_t>());
        }

        // flat offset of a multi-index given strides (in elements)
        inline std::size_t flatOffset(const types::ShapeType & idx, const types::ShapeType & strides) {
            std::size_t off = 0;
            for(std::size_t d = 0; d < idx.size(); ++d) {
                off += idx[d] * strides[d];
            }
            return off;
        }

        // increment a mixed-radix index over `shape`; returns false when wrapped around
        inline bool increment(types::ShapeType & idx, const types::ShapeType & shape) {
            for(std::ptrdiff_t d = static_cast<std::ptrdiff_t>(shape.size()) - 1; d >= 0; --d) {
                if(++idx[d] < shape[d]) {
                    return true;
                }
                idx[d] = 0;
            }
            return false;
        }
    }


    // copy a contiguous buffer into a strided sub-view of a larger array,
    // verify by independent strided indexing (replaces the old copyBufferToView test)
    TEST(ArrayUtilTest, TestCopyBufferToView) {
        const int minDim = 1;
        const int maxDim = 6;

        for(int dim = minDim; dim <= maxDim; ++dim) {
            types::ShapeType arrShape(dim), viewShape(dim), viewOffset(dim);
            if(dim > 3) {
                for(int d = 0; d < dim; ++d) {
                    arrShape[d]   = (d < 2) ? 3 : 40;
                    viewShape[d]  = (d < 2) ? 2 : 20;
                    viewOffset[d] = (d < 2) ? 1 : 10;
                }
            } else {
                for(int d = 0; d < dim; ++d) {
                    arrShape[d] = 40; viewShape[d] = 20; viewOffset[d] = 10;
                }
            }

            std::vector<int> array(prod(arrShape), 0);
            std::vector<int> buffer(prod(viewShape));
            std::iota(buffer.begin(), buffer.end(), 0);

            const auto arrView = makeView(array.data(), arrShape);
            copyView(makeView(buffer.data(), viewShape), subview(arrView, viewOffset, viewShape));

            // independent verification
            const auto arrStrides = cOrderStrides(arrShape);
            const auto bufStrides = cOrderStrides(viewShape);
            types::ShapeType idx(dim, 0);
            do {
                types::ShapeType global(dim);
                for(int d = 0; d < dim; ++d) {
                    global[d] = viewOffset[d] + idx[d];
                }
                ASSERT_EQ(array[flatOffset(global, arrStrides)], buffer[flatOffset(idx, bufStrides)]);
            } while(increment(idx, viewShape));
        }
    }


    // copy a strided sub-view into a contiguous buffer (replaces copyViewToBuffer test)
    TEST(ArrayUtilTest, TestCopyViewToBuffer) {
        const int minDim = 1;
        const int maxDim = 6;

        for(int dim = minDim; dim <= maxDim; ++dim) {
            types::ShapeType arrShape(dim), viewShape(dim), viewOffset(dim);
            for(int d = 0; d < dim; ++d) {
                arrShape[d]   = (d < 3) ? 40 : 3;
                viewShape[d]  = (d < 3) ? 20 : 2;
                viewOffset[d] = (d < 3) ? 10 : 1;
            }

            std::vector<int> array(prod(arrShape));
            std::iota(array.begin(), array.end(), 0);
            std::vector<int> buffer(prod(viewShape), -1);

            const auto arrView = makeView(array.data(), arrShape);
            copyView(subview(arrView, viewOffset, viewShape), makeView(buffer.data(), viewShape));

            const auto arrStrides = cOrderStrides(arrShape);
            const auto bufStrides = cOrderStrides(viewShape);
            types::ShapeType idx(dim, 0);
            do {
                types::ShapeType global(dim);
                for(int d = 0; d < dim; ++d) {
                    global[d] = viewOffset[d] + idx[d];
                }
                ASSERT_EQ(buffer[flatOffset(idx, bufStrides)], array[flatOffset(global, arrStrides)]);
            } while(increment(idx, viewShape));
        }
    }


    // round-trip: contiguous -> strided sub-view -> contiguous reproduces the input
    TEST(ArrayUtilTest, TestRoundTrip) {
        const types::ShapeType arrShape({5, 6, 7});
        const types::ShapeType viewShape({3, 4, 5});
        const types::ShapeType viewOffset({1, 1, 1});

        std::vector<int> in(prod(viewShape));
        std::iota(in.begin(), in.end(), 1);

        std::vector<int> array(prod(arrShape), 0);
        const auto arrView = makeView(array.data(), arrShape);
        copyView(makeView(in.data(), viewShape), subview(arrView, viewOffset, viewShape));

        std::vector<int> out(prod(viewShape), 0);
        copyView(subview(makeView(array.data(), arrShape), viewOffset, viewShape),
                 makeView(out.data(), viewShape));

        ASSERT_EQ(in, out);
    }


    // fillView only touches the requested sub-block of a larger array
    TEST(ArrayUtilTest, TestFillView) {
        const types::ShapeType arrShape({5, 6, 7});
        const types::ShapeType viewShape({2, 3, 4});
        const types::ShapeType viewOffset({1, 2, 1});

        std::vector<int> array(prod(arrShape), 0);
        const auto arrView = makeView(array.data(), arrShape);
        fillView(subview(arrView, viewOffset, viewShape), 7);

        const auto arrStrides = cOrderStrides(arrShape);
        types::ShapeType idx(3, 0);
        std::size_t nInside = 0;
        do {
            const std::size_t off = flatOffset(idx, arrStrides);
            const bool inside =
                idx[0] >= viewOffset[0] && idx[0] < viewOffset[0] + viewShape[0] &&
                idx[1] >= viewOffset[1] && idx[1] < viewOffset[1] + viewShape[1] &&
                idx[2] >= viewOffset[2] && idx[2] < viewOffset[2] + viewShape[2];
            if(inside) {
                ASSERT_EQ(array[off], 7);
                ++nInside;
            } else {
                ASSERT_EQ(array[off], 0);
            }
        } while(increment(idx, arrShape));
        ASSERT_EQ(nInside, prod(viewShape));
    }


    // subview only offsets the data pointer and keeps the parent strides
    TEST(ArrayUtilTest, TestSubview) {
        const types::ShapeType arrShape({4, 5});
        std::vector<int> array(prod(arrShape));
        std::iota(array.begin(), array.end(), 0);

        const auto arrView = makeView(array.data(), arrShape);
        const auto sub = subview(arrView, types::ShapeType({1, 2}), types::ShapeType({2, 2}));
        // strides are unchanged, data points at element (1, 2) == flat index 7
        ASSERT_EQ(sub.strides, arrView.strides);
        ASSERT_EQ(sub.data - arrView.data, 1 * 5 + 2);
        ASSERT_EQ(sub.shape, types::ShapeType({2, 2}));
    }


    // exercise the element-wise (non-unit last stride) copy path:
    // copy a column (last-axis stride != 1) of one 2d array into a column of another
    TEST(ArrayUtilTest, TestNonUnitLastStride) {
        const types::ShapeType arrShape({4, 3});
        std::vector<int> src(prod(arrShape));
        std::iota(src.begin(), src.end(), 0);
        std::vector<int> dst(prod(arrShape), -1);

        const auto srcStrides = cOrderStrides(arrShape);  // {3, 1}
        // a 1d view over column 1: 4 elements, stride 3 (== arrShape[1])
        const ConstArrayView<int> srcCol(src.data() + 1, types::ShapeType({4}), types::ShapeType({3}));
        const ArrayView<int> dstCol(dst.data() + 2, types::ShapeType({4}), types::ShapeType({3}));

        copyView(srcCol, dstCol);

        for(std::size_t i = 0; i < 4; ++i) {
            ASSERT_EQ(dst[i * 3 + 2], src[i * 3 + 1]);
        }
        // other entries untouched
        ASSERT_EQ(dst[0], -1);
        ASSERT_EQ(dst[1], -1);
    }

}
}
