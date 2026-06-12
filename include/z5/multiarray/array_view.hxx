#pragma once

#include <vector>
#include <cstddef>
#include <numeric>
#include <functional>
#include <utility>

#include "z5/types/types.hxx"

namespace z5 {
namespace multiarray {

    //
    // Lightweight, non-owning strided views into numpy-compatible buffers.
    //
    // The strides are given in ELEMENTS (not bytes), matching the conventions
    // used by numpy and nanobind's ndarray. The views are row-major (C-order)
    // by construction via cOrderStrides, but copyView / fillView work for any
    // strided layout.
    //

    template<typename T>
    struct ArrayView {
        T * data = nullptr;
        types::ShapeType shape;
        types::ShapeType strides;

        ArrayView() = default;
        ArrayView(T * data, types::ShapeType shape, types::ShapeType strides)
            : data(data), shape(std::move(shape)), strides(std::move(strides)) {}

        inline std::size_t ndim() const { return shape.size(); }
        inline std::size_t size() const {
            return std::accumulate(shape.begin(), shape.end(),
                                   static_cast<std::size_t>(1), std::multiplies<std::size_t>());
        }
    };


    template<typename T>
    struct ConstArrayView {
        const T * data = nullptr;
        types::ShapeType shape;
        types::ShapeType strides;

        ConstArrayView() = default;
        ConstArrayView(const T * data, types::ShapeType shape, types::ShapeType strides)
            : data(data), shape(std::move(shape)), strides(std::move(strides)) {}
        // allow implicit construction from a mutable view
        ConstArrayView(const ArrayView<T> & v) : data(v.data), shape(v.shape), strides(v.strides) {}

        inline std::size_t ndim() const { return shape.size(); }
        inline std::size_t size() const {
            return std::accumulate(shape.begin(), shape.end(),
                                   static_cast<std::size_t>(1), std::multiplies<std::size_t>());
        }
    };


    // row-major (C-order) element strides for the given shape (innermost axis has stride 1)
    inline types::ShapeType cOrderStrides(const types::ShapeType & shape) {
        const std::size_t dim = shape.size();
        types::ShapeType strides(dim, 1);
        for(std::ptrdiff_t d = static_cast<std::ptrdiff_t>(dim) - 2; d >= 0; --d) {
            strides[d] = strides[d + 1] * shape[d + 1];
        }
        return strides;
    }


    // build a C-contiguous view over a raw buffer
    template<typename T>
    inline ArrayView<T> makeView(T * data, const types::ShapeType & shape) {
        return ArrayView<T>(data, shape, cOrderStrides(shape));
    }

    template<typename T>
    inline ConstArrayView<T> makeView(const T * data, const types::ShapeType & shape) {
        return ConstArrayView<T>(data, shape, cOrderStrides(shape));
    }


    // sub-view at the given offset with the given shape; keeps the parent strides
    // and only advances the data pointer by the flat offset (in elements).
    template<typename T>
    inline ArrayView<T> subview(const ArrayView<T> & view,
                                const types::ShapeType & offset,
                                const types::ShapeType & shape) {
        std::size_t flatOffset = 0;
        for(std::size_t d = 0; d < offset.size(); ++d) {
            flatOffset += offset[d] * view.strides[d];
        }
        return ArrayView<T>(view.data + flatOffset, shape, view.strides);
    }

    template<typename T>
    inline ConstArrayView<T> subview(const ConstArrayView<T> & view,
                                     const types::ShapeType & offset,
                                     const types::ShapeType & shape) {
        std::size_t flatOffset = 0;
        for(std::size_t d = 0; d < offset.size(); ++d) {
            flatOffset += offset[d] * view.strides[d];
        }
        return ConstArrayView<T>(view.data + flatOffset, shape, view.strides);
    }

}
}
