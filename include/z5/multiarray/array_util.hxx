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
    // Copy between views whose unit-stride axes differ: `src` is contiguous along
    // axis `a` and `dst` along the innermost axis (or vice versa), as when one side
    // is a transposed chunk buffer. A plain run along the innermost axis would touch
    // a new cache line per element on the other side, so copy (a, innermost) in
    // square tiles that keep both sides' cache lines in use.
    template<typename T>
    inline void copyViewTiled(const ConstArrayView<T> & src, const ArrayView<T> & dst,
                              const std::size_t a) {
        constexpr std::size_t tile = 16;
        const std::size_t dim = dst.shape.size();
        const std::size_t last = dim - 1;
        const std::size_t nA = dst.shape[a], nLast = dst.shape[last];
        const std::size_t srcA = src.strides[a], dstA = dst.strides[a];
        const std::size_t srcL = src.strides[last], dstL = dst.strides[last];

        // Copy the (a, innermost) plane once for every combination of indices on
        // the remaining axes. Their number depends on the dimension, so instead of
        // nested loops `idx` counts through them like an odometer: the last counted
        // axis advances fastest and carries into the one before it. idx[a] and
        // idx[last] stay 0, so srcOff / dstOff below are where each plane starts.
        std::size_t nOuter = 1;
        for(std::size_t d = 0; d < last; ++d) {
            if(d != a) {
                nOuter *= dst.shape[d];
            }
        }
        types::ShapeType idx(dim, 0);
        for(std::size_t o = 0; o < nOuter; ++o) {
            std::size_t srcOff = 0, dstOff = 0;
            for(std::size_t d = 0; d < last; ++d) {
                srcOff += idx[d] * src.strides[d];
                dstOff += idx[d] * dst.strides[d];
            }
            for(std::size_t i0 = 0; i0 < nA; i0 += tile) {
                const std::size_t i1 = std::min(i0 + tile, nA);
                for(std::size_t k0 = 0; k0 < nLast; k0 += tile) {
                    const std::size_t k1 = std::min(k0 + tile, nLast);
                    // the inner loop runs along dst's unit-stride axis: scattered
                    // stores (often a power-of-two stride apart, i.e. one cache
                    // set) cost far more than scattered loads
                    if(dstL == 1) {
                        for(std::size_t i = i0; i < i1; ++i) {
                            const T * s = src.data + srcOff + i * srcA;
                            T * t = dst.data + dstOff + i * dstA;
                            for(std::size_t k = k0; k < k1; ++k) {
                                t[k] = s[k * srcL];
                            }
                        }
                    } else {
                        for(std::size_t k = k0; k < k1; ++k) {
                            const T * s = src.data + srcOff + k * srcL;
                            T * t = dst.data + dstOff + k * dstL;
                            for(std::size_t i = i0; i < i1; ++i) {
                                t[i] = s[i * srcA];
                            }
                        }
                    }
                }
            }
            for(std::ptrdiff_t d = static_cast<std::ptrdiff_t>(last) - 1; d >= 0; --d) {
                if(static_cast<std::size_t>(d) == a) {
                    continue;
                }
                if(++idx[d] < dst.shape[d]) {
                    break;
                }
                idx[d] = 0;
            }
        }
    }


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

        // one side is contiguous along the innermost axis and the other along an
        // outer one (a transposed chunk buffer): copy in tiles
        if(!contiguousRun && (srcLastStride == 1 || dstLastStride == 1)) {
            const auto & other = srcLastStride == 1 ? dst.strides : src.strides;
            for(std::size_t d = 0; d + 1 < dim; ++d) {
                if(other[d] == 1 && dst.shape[d] > 1) {
                    copyViewTiled(src, dst, d);
                    return;
                }
            }
        }

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
