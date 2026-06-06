#pragma once

#include <vector>
#include <numeric>
#include <functional>

#include "z5/types/types.hxx"
#include "z5/multiarray/array_view.hxx"

namespace z5 {
namespace multiarray {

    // small owning, C-contiguous array used in the tests; exposes
    // ArrayView / ConstArrayView over its buffer (replaces xt::xarray)
    template<class T>
    struct TestArray {
        types::ShapeType shape;
        std::vector<T> data;

        explicit TestArray(const types::ShapeType & s)
            : shape(s),
              data(std::accumulate(s.begin(), s.end(),
                                   static_cast<std::size_t>(1),
                                   std::multiplies<std::size_t>()), T(0)) {}

        std::size_t size() const { return data.size(); }

        ArrayView<T> view() { return makeView(data.data(), shape); }
        ConstArrayView<T> cview() const {
            return ConstArrayView<T>(data.data(), shape, cOrderStrides(shape));
        }

        // C-order element access for the 3d tests
        T & operator()(std::size_t i, std::size_t j, std::size_t k) {
            return data[(i * shape[1] + j) * shape[2] + k];
        }
        const T & operator()(std::size_t i, std::size_t j, std::size_t k) const {
            return data[(i * shape[1] + j) * shape[2] + k];
        }
    };

}
}
