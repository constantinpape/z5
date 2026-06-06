#pragma once

#include <algorithm>
#include <cstddef>

#include "z5/multiarray/array_view.hxx"

namespace z5 {
namespace multiarray {

    //
    // Generic strided copy between two views of equal shape.
    //
    // Both src and dst may be strided. We iterate over the outer (ndim - 1)
    // dimensions with a mixed-radix odometer and copy a run of shape[ndim - 1]
    // elements along the innermost axis. When both innermost strides are 1
    // (the common C-contiguous case, including sub-views of C-contiguous
    // arrays) the run is a plain std::copy; otherwise we fall back to an
    // element-wise strided copy.
    //
    template<typename T>
    inline void copyView(const ConstArrayView<T> & src, const ArrayView<T> & dst) {
        const std::size_t dim = dst.shape.size();

        // scalar / 0-d
        if(dim == 0) {
            dst.data[0] = src.data[0];
            return;
        }

        const std::size_t lastDim = dst.shape[dim - 1];
        const std::size_t srcLastStride = src.strides[dim - 1];
        const std::size_t dstLastStride = dst.strides[dim - 1];
        const bool contiguousRun = (srcLastStride == 1 && dstLastStride == 1);

        // number of runs = product of the outer dimensions (1 in the 1d case)
        std::size_t nRuns = 1;
        for(std::size_t d = 0; d + 1 < dim; ++d) {
            nRuns *= dst.shape[d];
        }

        types::ShapeType idx(dim - 1, 0);
        for(std::size_t run = 0; run < nRuns; ++run) {
            // flat offset of this run's start on each side (recomputed per run,
            // which is cheap because ndim is small and avoids carry corrections)
            std::size_t srcOff = 0, dstOff = 0;
            for(std::size_t d = 0; d + 1 < dim; ++d) {
                srcOff += idx[d] * src.strides[d];
                dstOff += idx[d] * dst.strides[d];
            }

            if(contiguousRun) {
                std::copy(src.data + srcOff, src.data + srcOff + lastDim, dst.data + dstOff);
            } else {
                for(std::size_t k = 0; k < lastDim; ++k) {
                    dst.data[dstOff + k * dstLastStride] = src.data[srcOff + k * srcLastStride];
                }
            }

            // increment the mixed-radix odometer over the outer dimensions
            for(std::ptrdiff_t d = static_cast<std::ptrdiff_t>(dim) - 2; d >= 0; --d) {
                if(++idx[d] < dst.shape[d]) {
                    break;
                }
                idx[d] = 0;
            }
        }
    }


    // convenience overload for a mutable source view
    template<typename T>
    inline void copyView(const ArrayView<T> & src, const ArrayView<T> & dst) {
        copyView(ConstArrayView<T>(src), dst);
    }


    // fill a (possibly strided) view with a scalar value
    template<typename T>
    inline void fillView(const ArrayView<T> & dst, const T val) {
        const std::size_t dim = dst.shape.size();

        if(dim == 0) {
            dst.data[0] = val;
            return;
        }

        const std::size_t lastDim = dst.shape[dim - 1];
        const std::size_t dstLastStride = dst.strides[dim - 1];
        const bool contiguousRun = (dstLastStride == 1);

        std::size_t nRuns = 1;
        for(std::size_t d = 0; d + 1 < dim; ++d) {
            nRuns *= dst.shape[d];
        }

        types::ShapeType idx(dim - 1, 0);
        for(std::size_t run = 0; run < nRuns; ++run) {
            std::size_t dstOff = 0;
            for(std::size_t d = 0; d + 1 < dim; ++d) {
                dstOff += idx[d] * dst.strides[d];
            }

            if(contiguousRun) {
                std::fill(dst.data + dstOff, dst.data + dstOff + lastDim, val);
            } else {
                for(std::size_t k = 0; k < lastDim; ++k) {
                    dst.data[dstOff + k * dstLastStride] = val;
                }
            }

            for(std::ptrdiff_t d = static_cast<std::ptrdiff_t>(dim) - 2; d >= 0; --d) {
                if(++idx[d] < dst.shape[d]) {
                    break;
                }
                idx[d] = 0;
            }
        }
    }

}
}
