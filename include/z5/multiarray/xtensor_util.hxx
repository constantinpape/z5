#pragma once

#include "xtensor/xarray.hpp"
#include "xtensor/xadapt.hpp"
#include "xtensor/xstrided_view.hpp"

// FIXME this does not work as expected either...
// copied from xtensor-blas:
// https://github.com/QuantStack/xtensor-blas/blob/master/include/xtensor-blas/xblas_utils.hpp#L17-L42
namespace xt {

    template <layout_type L = layout_type::row_major, class T>
    inline auto view_eval(T&& t)
        -> std::enable_if_t<has_raw_data_interface<T>::value && std::decay_t<T>::static_layout == L, T&&>
    {
        return std::forward<T>(t);
    }

    template <layout_type L = layout_type::row_major, class T, class I = std::decay_t<T>>
    inline auto view_eval(T&& t)
        -> std::enable_if_t<(!has_raw_data_interface<T>::value || I::static_layout != L)
                                && detail::is_array<typename I::shape_type>::value,
                            xtensor<typename I::value_type, std::tuple_size<typename I::shape_type>::value, L>>
    {
        xtensor<typename I::value_type, std::tuple_size<typename I::shape_type>::value, L> ret = t;
        return ret;
    }

    template <layout_type L = layout_type::row_major, class T, class I = std::decay_t<T>>
    inline auto view_eval(T&& t)
        -> std::enable_if_t<(!has_raw_data_interface<T>::value || I::static_layout != L) &&
                                !detail::is_array<typename I::shape_type>::value,
                            xarray<typename I::value_type, L>>
    {
        xarray<typename I::value_type, L> ret = t;
        return ret;
	}
}


namespace z5 {
namespace multiarray {


    // small helper function to convert a ROI given by (offset, shape) into
    // a proper xtensor sliceing
    inline void sliceFromRoi(xt::slice_vector & roiSlice,
                             const types::ShapeType & offset,
                             const types::ShapeType & shape) {
        for(int d = 0; d < offset.size(); ++d) {
            roiSlice.push_back(xt::range(offset[d], offset[d] + shape[d]));
        }
    }


    inline size_t offsetAndStridesFromRoi(const types::ShapeType & outStrides,
                                          const types::ShapeType & requestShape,
                                          const types::ShapeType & offsetInRequest,
                                          types::ShapeType & requestStrides) {
        // first, calculate the flat offset
        // -> sum( coordinate_offset * out_strides )
        size_t flatOffset = 1;
        for(int d = 0; d < outStrides.size(); ++d) {
            flatOffset += (outStrides[d] * offsetInRequest[d]);
        }

        // TODO this depends on the laout type.....
        // next, calculate the new strides
        requestStrides.resize(requestShape.size());
        for(int d = 0; d < requestShape.size(); ++d) {
            requestStrides[d] = std::accumulate(requestShape.rbegin(), requestShape.rbegin() + d, 1, std::multiplies<size_t>());
        }
        std::reverse(requestStrides.begin(), requestStrides.end());

        return flatOffset;
    }

    // FIXME N > 3 is segfaulting
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyBufferToViewND(const std::vector<T> & buffer,
                                   xt::xexpression<VIEW> & viewExperession,
                                   const SHAPE_TYPE & arrayStrides) {
        auto & view = viewExperession.derived_cast();
        const size_t dim = view.dimension();
        size_t bufferOffset = 0;
        size_t viewOffset = 0;
        types::ShapeType dimPositions(dim);
        const size_t bufSize = buffer.size();
        const auto & viewShape = view.shape();
        // THIS ASSUMES C-ORDER
        // -> memory is consecutive along the last axis
        const size_t memLen = viewShape[dim - 1];

        size_t covered = 0;

        // we copy data to consecutive pieces of memory in the view
        // until we have exhausted the buffer

        // we start out loop at the second from last dimension
        // (last dimension is the fastest moving and consecutive in memory)
        for(int d = dim - 2; d >= 0;) {
            // copy the peace of buffer that is consectuve to our view
            std::copy(buffer.begin() + bufferOffset,
                      buffer.begin() + bufferOffset + memLen,
                      &view(0) + viewOffset);

            // increase the buffer offset by what we have just written to the view
            bufferOffset += memLen;
            // increase the view offsets by the strides along the second from last dimension
            viewOffset += arrayStrides[dim - 2];

            // check if we need to decrease the dimension
            for(d = dim - 2; d >= 0; --d) {
                // increase the position in the current dimension
                dimPositions[d] += 1;

                // if we are smaller than the shape in this dimension, stay in this dimension
                // (i.e. break and go back to the copy loop)
                if(dimPositions[d] < viewShape[d]) {
                    break;
                // otherwise, decrease the dimension
                } else {
                    // reset the position in this dimension
                    dimPositions[d] = 0;

                    // increase the viewOffset to jump to the next point in memory
                    // -> we will decrease the axis, so we need to
                    // correct for the positions we have changed in a lower dimension
                    covered = (viewOffset - covered);
                    viewOffset -= covered;
                    if(d > 0) {
                        viewOffset += arrayStrides[d - 1];
                    }
                    covered = viewOffset;
                }
            }
        }
    }


    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyBufferToView(const std::vector<T> & buffer,
                                 xt::xexpression<VIEW> & viewExperession,
                                 const SHAPE_TYPE & arrayStrides) {
        auto & view = viewExperession.derived_cast();
        // ND impl doesn't work for 1D
        // Also, N > 3 is not working yet, so we default to std::copy
        // FIXME fix ND copy for N > 3
        if(view.dimension() == 1 || view.dimension() > 3) {
            // std::copy(buffer.begin(), buffer.end(), view.begin());
            const auto bufferView = xt::adapt(buffer, view.shape());
            view = bufferView;
        } else {
            copyBufferToViewND(buffer, viewExperession, arrayStrides);
        }
    }


    // FIXME N > 3 is segfaulting
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyViewToBufferND(const xt::xexpression<VIEW> & viewExperession,
                                  std::vector<T> & buffer,
                                  const SHAPE_TYPE & arrayStrides) {
        auto & view = viewExperession.derived_cast();
        const size_t dim = view.dimension();
        size_t bufferOffset = 0;
        size_t viewOffset = 0;
        types::ShapeType dimPositions(dim);
        const size_t bufSize = buffer.size();
        const auto & viewShape = view.shape();
        // THIS ASSUMES C-ORDER
        // -> memory is consecutive along the last axis
        const size_t memLen = viewShape[dim - 1];

        size_t covered = 0;

        // we copy data to consecutive pieces of memory in the view
        // until we have exhausted the buffer

        // we start out loop at the second from last dimension
        // (last dimension is the fastest moving and consecutive in memory)
        for(int d = dim - 2; d >= 0;) {
            // copy the piece of buffer that is consectuve to our view
            std::copy(&view(0) + viewOffset,
                      &view(0) + viewOffset + memLen,
                      buffer.begin() + bufferOffset);

            // increase the buffer offset by what we have just written to the view
            bufferOffset += memLen;
            // increase the view offsets by the strides along the second from last dimension
            viewOffset += arrayStrides[dim - 2];

            // check if we need to decrease the dimension
            for(d = dim - 2; d >= 0; --d) {
                // increase the position in the current dimension
                dimPositions[d] += 1;

                // if we are smaller than the shape in this dimension, stay in this dimension
                // (i.e. break and go back to the copy loop)
                if(dimPositions[d] < viewShape[d]) {
                    break;
                // otherwise, decrease the dimension
                } else {
                    // reset the position in this dimension
                    dimPositions[d] = 0;

                    // increase the viewOffset to jump to the next point in memory
                    // -> we will decrease the axis, so we need to
                    // correct for the positions we have changed in a lower dimension
                    covered = (viewOffset - covered);
                    viewOffset -= covered;
                    if(d > 0) {
                        viewOffset += arrayStrides[d - 1];
                    }
                    covered = viewOffset;
                }
            }
        }
    }


    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyViewToBuffer(const xt::xexpression<VIEW> & viewExperession,
                                 std::vector<T> & buffer,
                                 const SHAPE_TYPE & arrayStrides) {
        const auto & view = viewExperession.derived_cast();
        // can't use the ND implementation in 1d, hence we resort to std::copy,
        // which should be fine in 1D

        // Also, N > 3 is not working yet, so we default to std::copy
        // FIXME fix ND copy for N > 3
        if(view.dimension() == 1 || view.dimension() > 3) {
            // std::copy(view.begin(), view.end(), buffer.begin());
            auto bufferView = xt::adapt(buffer, view.shape());
            bufferView = view;
        } else {
            copyViewToBufferND(viewExperession, buffer, arrayStrides);
        }
    }


}
}
