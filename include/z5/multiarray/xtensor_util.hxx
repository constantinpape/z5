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
                                   const SHAPE_TYPE & arrayStrides) {
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
            // move the view position along the first axis
            // FIXME TODO do we need to correct here
            viewPos += arrayStrides[0];
        }
    }


    // 3D copy buffer to view
    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyBufferToView3D(const std::vector<T> & buffer,
                                   xt::xexpression<VIEW> & viewExperession,
                                   const SHAPE_TYPE & arrayStrides) {
        auto & view = viewExperession.derived_cast();
        // we copy data to consecutive pieces of memory in the view
        // until we have exhausted the buffer
        size_t bufferPos = 0;
        size_t viewPos = 0;
        const size_t bufSize = buffer.size();
        const auto & viewShape = view.shape();

        size_t covered;

        // we start with the last dimension (which is consecutive in memory)
        // and copy consecutve blocks from the buffer into the view
        for(size_t z = 0; z < viewShape[0]; ++z) {
            covered = viewPos;
            for(size_t y = 0; y < viewShape[1]; ++y) {

                if(bufferPos >= bufSize) {
                    // this should not happen
                    std::cout << "Inner break at z = " << z << " y = " << y << std::endl;
                    break;
                }

                std::copy(buffer.begin() + bufferPos, buffer.begin() + bufferPos + viewShape[2], &view(0) + viewPos);

                bufferPos += viewShape[2];
                // move the view position by our shape along the last axis
                viewPos += arrayStrides[1];
            }
            //if(bufferPos >= bufSize) {
            //    break;
            //}
            covered = (viewPos - covered);
            viewPos += (arrayStrides[0] - covered);
            //std::cout << "Incrementing by " << (arrayStrides[0] - covered) << std::endl;
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


    /*
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
        const size_t memLen = viewShape[dim - 1];

        size_t covered = 0;

        // we copy data to consecutive pieces of memory in the view
        // until we have exhausted the buffer

        // we start out loop at the second from last dimension
        // (last dimension is the fastest moving and consecutive in memory)
        for(int d = dim - 2; d >= 0;) {
            std::cout << "Write from buffer " << bufferOffset << " to " << bufferOffset + memLen << std::endl;
            std::cout << "Write from view " << viewOffset << " to " << viewOffset + memLen << std::endl;
            // copy the peace of buffer that is consectuve to our view
            std::copy(buffer.begin() + bufferOffset,
                      buffer.begin() + bufferOffset + memLen,
                      &view(0) + viewOffset);

            // increase the buffer offset and the view offset by the amount we have just
            // written to memory
            bufferOffset += memLen;
            viewOffset += arrayStrides[d];

            // check if we need to decrease the dimension
            for(d = dim - 2; d >= 0; --d) {
                // increase the position in the current dimension
                dimPositions[d] += 1;
                std::cout << "Dim: " << d << " pos " << dimPositions[d] << " / " << viewShape[d] << std::endl;

                // if we are smaller than the shape in this dimension, stay in this dimension
                // (i.e. break and go back to the copy loop)
                if(dimPositions[d] < viewShape[d]) {
                    break;
                // otherwise, decrease the dimension
                } else {
                    //std::cout << "Increasing view-offset due to dim jump " << d << std::endl;
                    //// measure how much we covered since the last big jump
                    //covered = (viewOffset - covered);
                    //// increase the view offset corresponding to the dimension jump
                    //// i.e. increase by the stride in this dimension, corrected for
                    //// the ground we covered in the previous dimension
                    //viewOffset += (arrayStrides[] - covered);
                    //std::cout << "Increase by " << (arrayStrides[] - covered) << " (stride) " << arrayStrides[] << " (covered) " << covered << std::endl;
                    //// reset the covered to the current offset
                    //covered = viewOffset;

                    // reset the position in this dimension
                    dimPositions[d] = 0;
                }
            }
        }
    }
    */


    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyBufferToView(const std::vector<T> & buffer,
                                 xt::xexpression<VIEW> & viewExperession,
                                 const SHAPE_TYPE & arrayStrides) {
        auto & view = viewExperession.derived_cast();
        switch(view.dimension()) {
            case 1: std::copy(buffer.begin(), buffer.end(), view.end()); break;
            case 2: copyBufferToView2D(buffer, viewExperession, arrayStrides); break;
            case 3: copyBufferToView3D(buffer, viewExperession, arrayStrides); break;
            // TODO implement 4D and 5D
            // TODO implement true ND instead (see link above)
            //case 4: copyBufferToView4D(buffer, viewExperession, arrayStrides); break;
            //case 5: copyBufferToView5D(buffer, viewExperession, arrayStrides); break;
            // use general (slow) view semantic for ND support
            default: copyBufferToViewND(buffer, viewExperession); break;
            //default: copyBufferToViewND(buffer, viewExperession, arrayStrides); break;
        }
    }


    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyViewToBuffer2D(const xt::xexpression<VIEW> & viewExperession,
                                   std::vector<T> & buffer,
                                   const SHAPE_TYPE & arrayStrides) {
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
            viewPos += arrayStrides[0];
        }
    }


    // 3D copy buffer to view
    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyViewToBuffer3D(const xt::xexpression<VIEW> & viewExperession,
                                   std::vector<T> & buffer,
                                   const SHAPE_TYPE & arrayStrides) {
        const auto & view = viewExperession.derived_cast();
        // we copy data to consecutive pieces of memory in the view
        // until we have exhausted the buffer
        size_t bufferPos = 0;
        size_t viewPos = 0;
        const size_t bufSize = buffer.size();
        const auto & viewShape = view.shape();
        size_t covered;
        // we start with the last dimension (which is consecutive in memory)
        // and copy consecutve blocks from the buffer into the view
        for(size_t z = 0; z < viewShape[0]; ++z) {
            covered = viewPos;
            for(size_t y = 0; y < viewShape[1]; ++y) {
                std::copy(&view(0) + viewPos, &view(0) + viewPos + viewShape[2], buffer.begin() + bufferPos);
                bufferPos += viewShape[2];
                if(bufferPos >= bufSize) {
                    break;
                }
                // move the view position by our shape along the last axis
                viewPos += arrayStrides[1];
            }
            if(bufferPos >= bufSize) {
                break;
            }
            // move the view position along the first axis, correcting for
            // our movements along second axis
            covered = (viewPos - covered);
            viewPos += (arrayStrides[0] - covered);
        }
    }


    // FIXME this only works for row-major (C) memory layout
    template<typename T, typename VIEW, typename SHAPE_TYPE>
    inline void copyViewToBuffer(const xt::xexpression<VIEW> & viewExperession,
                                 std::vector<T> & buffer,
                                 const SHAPE_TYPE & arrayStrides) {
        const auto & view = viewExperession.derived_cast();
        switch(view.dimension()) {
            case 2: copyViewToBuffer2D(viewExperession, buffer, arrayStrides); break;
            case 3: copyViewToBuffer3D(viewExperession, buffer, arrayStrides); break;
            // TODO implement 4D and 5D
            // TODO implement true ND instead (see link above)
            //case 4: copyViewToBuffer4D(viewExperession, buffer, arrayStrides); break;
            //case 5: copyViewToBuffer5D(viewExperession, buffer, arrayStrides); break;
            // use general (slow) copy for ND support (also used in 1d, where this is fine)
            default: std::copy(view.begin(), view.end(), buffer.begin()); break;
        }
    }


}
}
