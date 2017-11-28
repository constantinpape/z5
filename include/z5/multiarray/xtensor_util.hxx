#pragma once

#include "xtensor/xarray.hpp"
#include "xtensor/xadapt.hpp"
#include "xtensor/xstridedview.hpp"


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

    // 2D copy buffer to view
    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyBufferToView2D(const std::vector<T> & buffer,
                                   xt::xexpression<VIEW> & viewExperession,
                                   const SHAPE_TYPE & arrayShape) {
        auto & view = viewExperession.derived_cast();
        // we copy data to consecutive pieces of memory in the view
        // until we have exhausted the buffer
        size_t bufferPos = 0;
        size_t viewPos = 0;
        const size_t bufSize = buffer.size();
        const auto & viewShape = view.shape();
        // we start with the last dimension (which is consecutive in memory)
        // and copy consecutve blocks from the buffer into the view
        for(size_t y = 0; y < viewShape[1]; ++y) {
            std::copy(buffer.begin() + bufferPos, buffer.begin() + bufferPos + viewShape[1], &view(0) + viewPos);
            bufferPos += viewShape[1];
            if(bufferPos >= bufSize) {
                break;
            }
            // move the view position by our shape along the last axis
            viewPos += arrayShape[1];
        }
    }


    // 3D copy buffer to view
    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyBufferToView3D(const std::vector<T> & buffer,
                                   xt::xexpression<VIEW> & viewExperession,
                                   const SHAPE_TYPE & arrayShape) {
        auto & view = viewExperession.derived_cast();
        // we copy data to consecutive pieces of memory in the view
        // until we have exhausted the buffer
        size_t bufferPos = 0;
        size_t viewPos = 0;
        const size_t bufSize = buffer.size();
        const auto & viewShape = view.shape();
        // we start with the last dimension (which is consecutive in memory)
        // and copy consecutve blocks from the buffer into the view
        for(size_t z = 0; z < viewShape[0]; ++z) {
            for(size_t y = 0; y < viewShape[1]; ++y) {
                std::copy(buffer.begin() + bufferPos, buffer.begin() + bufferPos + viewShape[2], &view(0) + viewPos);
                bufferPos += viewShape[2];
                if(bufferPos >= bufSize) {
                    break;
                }
                // move the view position by our shape along the last axis
                viewPos += arrayShape[2];
            }
            if(bufferPos >= bufSize) {
                break;
            }
            // move the view position along the second axis, correcting for
            // our movements along third axis
            viewPos += arrayShape[1] * (arrayShape[2] - viewShape[2]);
        }
    }


    // copy the data from the buffer into the view
    // via xadapt and assignment operator
    // TODO implement ND buffer copy, following this idea:
    // https://github.com/saalfeldlab/n5-imglib2/blob/master/src/main/java/org/janelia/saalfeldlab/n5/imglib2/N5Utils.java#L501-L529
    template<typename T, typename VIEW>
    inline void copyBufferToViewND(const std::vector<T> & buffer,
                                   xt::xexpression<VIEW> & viewExperession) {
        auto & view = viewExperession.derived_cast();
        auto buffView = xt::xadapt(buffer, view.shape());
        view = buffView;
    }


    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyBufferToView(const std::vector<T> & buffer,
                                 xt::xexpression<VIEW> & viewExperession,
                                 const SHAPE_TYPE & arrayShape) {
        auto & view = viewExperession.derived_cast();
        switch(view.dimension()) {
            case 1: std::copy(buffer.begin(), buffer.end(), view.end()); break;
            case 2: copyBufferToView2D(buffer, viewExperession, arrayShape); break;
            case 3: copyBufferToView3D(buffer, viewExperession, arrayShape); break;
            // TODO implement 4D and 5D
            // TODO implement true ND instead (see link above)
            //case 4: copyBufferToView4D(buffer, viewExperession, arrayShape); break;
            //case 5: copyBufferToView5D(buffer, viewExperession, arrayShape); break;
            // use general (slow) view semantic for ND support
            default: copyBufferToViewND(buffer, viewExperession); break;
        }
    }


    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyViewToBuffer2D(const xt::xexpression<VIEW> & viewExperession,
                                   std::vector<T> & buffer,
                                   const SHAPE_TYPE & arrayShape) {
        const auto & view = viewExperession.derived_cast();
        // we copy data to consecutive pieces of memory in the view
        // until we have exhausted the buffer
        size_t bufferPos = 0;
        size_t viewPos = 0;
        const size_t bufSize = buffer.size();
        const auto & viewShape = view.shape();
        // we start with the last dimension (which is consecutive in memory)
        // and copy consecutve blocks from the buffer into the view
        for(size_t y = 0; y < viewShape[1]; ++y) {
            std::copy(&view(0) + viewPos, &view(0) + viewPos + viewShape[2], buffer.begin() + bufferPos);
            bufferPos += viewShape[1];
            if(bufferPos >= bufSize) {
                break;
            }
            // move the view position by our shape along the last axis
            viewPos += arrayShape[1];
        }
    }


    // 3D copy buffer to view
    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyViewToBuffer3D(const xt::xexpression<VIEW> & viewExperession,
                                   std::vector<T> & buffer,
                                   const SHAPE_TYPE & arrayShape) {
        const auto & view = viewExperession.derived_cast();
        // we copy data to consecutive pieces of memory in the view
        // until we have exhausted the buffer
        size_t bufferPos = 0;
        size_t viewPos = 0;
        const size_t bufSize = buffer.size();
        const auto & viewShape = view.shape();
        // we start with the last dimension (which is consecutive in memory)
        // and copy consecutve blocks from the buffer into the view
        for(size_t z = 0; z < viewShape[0]; ++z) {
            for(size_t y = 0; y < viewShape[1]; ++y) {
                std::copy(&view(0) + viewPos, &view(0) + viewPos + viewShape[2], buffer.begin() + bufferPos);
                bufferPos += viewShape[2];
                if(bufferPos >= bufSize) {
                    break;
                }
                // move the view position by our shape along the last axis
                viewPos += arrayShape[2];
            }
            if(bufferPos >= bufSize) {
                break;
            }
            // move the view position along the second axis, correcting for
            // our movements along third axis
            viewPos += arrayShape[1] * (arrayShape[2] - viewShape[2]);
        }
    }


    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyViewToBuffer(const xt::xexpression<VIEW> & viewExperession,
                                 std::vector<T> & buffer,
                                 const SHAPE_TYPE & arrayShape) {
        const auto & view = viewExperession.derived_cast();
        switch(view.dimension()) {
            case 2: copyViewToBuffer2D(viewExperession, buffer, arrayShape); break;
            case 3: copyViewToBuffer3D(viewExperession, buffer, arrayShape); break;
            // TODO implement 4D and 5D
            // TODO implement true ND instead (see link above)
            //case 4: copyViewToBuffer4D(viewExperession, buffer, arrayShape); break;
            //case 5: copyViewToBuffer5D(viewExperession, buffer, arrayShape); break;
            // use general (slow) copy for ND support (also used in 1d, where this is fine)
            default: std::copy(view.begin(), view.end(), buffer.begin()); break;
        }
    }


}
}
