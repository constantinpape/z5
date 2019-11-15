#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <boost/variant.hpp>

// see https://pybind11.readthedocs.io/en/stable/advanced/cast/stl.html#c-17-library-containers
namespace pybind11 {
namespace detail {

    // this is what's officially recommended by pybind11, but MSVC 14 doesn't like it,
    // so for now we just specify for the types we need
    template <typename... Ts>
    struct type_caster<boost::variant<Ts...>> : variant_caster<boost::variant<Ts...>> {};

    // template<>
    // struct type_caster<boost::variant<bool>> : variant_caster<boost::variant<bool>>{};
    // template<>
    // struct type_caster<boost::variant<int>> : variant_caster<boost::variant<int>>{};
    // template<>
    // struct type_caster<boost::variant<std::string>> : variant_caster<boost::variant<std::string>>{};

    // Specifies the function used to visit the variant -- `apply_visitor` instead of `visit`
    template <>
    struct visit_helper<boost::variant> {
        template <typename... Args>
        static auto call(Args &&...args) -> decltype(boost::apply_visitor(args...)) {
            return boost::apply_visitor(args...);
        }
    };

}
}
