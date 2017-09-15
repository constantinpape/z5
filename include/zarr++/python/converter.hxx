#pragma once
#include <cctype>
#include <type_traits>
#include <initializer_list>
#include <iostream>

#include <pybind11/pybind11.h>
#include <pybind11/cast.h>
#include <pybind11/numpy.h>

#include "andres/marray.hxx"

namespace py = pybind11;

// forward declerations
namespace pybind11{
namespace detail{
    template <typename Type, size_t DIM, bool AUTO_CAST_TYPE>
    struct pymarray_caster;
}
}


namespace zarr {
namespace python {

    template <typename VALUE_TYPE, size_t DIM = 0, bool AUTO_CAST_TYPE=true>
    class PyView : public andres::View<VALUE_TYPE, false>
    {
        friend struct pybind11::detail::pymarray_caster<VALUE_TYPE,DIM, AUTO_CAST_TYPE>;

      private:

        typename std::conditional<AUTO_CAST_TYPE,
                pybind11::array_t<VALUE_TYPE, py::array::forcecast>,
                pybind11::array_t<VALUE_TYPE, py::array::c_style >
        >::type py_array;

      public:

        typedef VALUE_TYPE DataType;

        template <class ShapeIterator>
        PyView(pybind11::array_t<VALUE_TYPE> array, VALUE_TYPE *data, ShapeIterator begin, ShapeIterator end)
            : View<VALUE_TYPE, false>(begin, end, data, FirstMajorOrder, FirstMajorOrder), py_array(array)
        {
            auto info = py_array.request();
            VALUE_TYPE *ptr = (VALUE_TYPE *)info.ptr;

            std::vector<size_t> strides(info.strides.begin(),info.strides.end());
            for(size_t i=0; i<strides.size(); ++i){
                strides[i] /= sizeof(VALUE_TYPE);
            }
            this->assign( info.shape.begin(), info.shape.end(), strides.begin(), ptr, FirstMajorOrder);

        }

        PyView()
        {
            //std::cout<<"EEE---\n";
        }
        const VALUE_TYPE & operator[](const uint64_t index)const{
            return this->operator()(index);
        }

        VALUE_TYPE & operator[](const uint64_t index){
            return this->operator()(index);
        }

        template <class ShapeIterator>
        PyView(ShapeIterator begin, ShapeIterator end) {
            this->assignFromShape(begin, end);
        }

        template <class ShapeIterator>
        void reshapeIfEmpty(ShapeIterator begin, ShapeIterator end){
            if(this->size() == 0){
                this->assignFromShape(begin, end);
            }
            else{
                auto c = 0;
                while(begin!=end){
                    if(this->shape(c)!=*begin){
                        throw std::runtime_error("given numpy array has an unusable shape");
                    }
                    ++begin;
                    ++c;
                }
            }
        }


        template<class T_INIT>
        PyView(std::initializer_list<T_INIT> shape) : PyView(shape.begin(), shape.end())
        {
        }

        template<class T_INIT>
        void reshapeIfEmpty(std::initializer_list<T_INIT> shape) {
            this->reshapeIfEmpty(shape.begin(), shape.end());
        }

    private:

        template <class ShapeIterator>
        void assignFromShape(ShapeIterator begin, ShapeIterator end)
        {
            std::vector<size_t> shape(begin,end);
            std::vector<size_t> strides(shape.size());

            strides.resize(shape.size());
            strides.back() = sizeof(VALUE_TYPE);
            for(int64_t i = strides.size()-2; i>=0; --i){
                strides[i] = strides[i+1] * shape[i+1];
            }

            VALUE_TYPE * ptr = nullptr;
            {
                py_array = pybind11::array(pybind11::buffer_info(
                    nullptr, sizeof(VALUE_TYPE), pybind11::format_descriptor<VALUE_TYPE>::format(), shape.size(), shape, strides));
                pybind11::buffer_info info = py_array.request();
                ptr = (VALUE_TYPE *)info.ptr;
            }
            for (size_t i = 0; i < shape.size(); ++i) {
                strides[i] /= sizeof(VALUE_TYPE);
            }
            this->assign(begin, end, strides.begin(), ptr, FirstMajorOrder);
        }

    public:

        void createViewFrom(const andres::marray::View<VALUE_TYPE> & view)
        {
            std::vector<size_t> shape(view.shapeBegin(), view.shapeEnd());
            std::vector<size_t> strides(shape.size());

            strides.resize(shape.size());
            strides.back() = sizeof(VALUE_TYPE);
            for(int64_t i = 0; i<shape.size(); ++i){
                strides[i] = view.strides(i)*sizeof(VALUE_TYPE);
            }


            py_array = pybind11::array(pybind11::buffer_info(
                &view(0), sizeof(VALUE_TYPE), pybind11::format_descriptor<VALUE_TYPE>::value, shape.size(), shape, strides));
            pybind11::buffer_info info = py_array.request();
            VALUE_TYPE *ptr = (VALUE_TYPE *)info.ptr;

            for (size_t i = 0; i < shape.size(); ++i) {
                strides[i] /= sizeof(VALUE_TYPE);
            }
            this->assign(shape.begin(), shape.end(), view.stridesBegin(), ptr, FirstMajorOrder);
        }
    };

} // end namespace python


template <typename VALUE_TYPE, size_t DIM,  bool AUTO_CAST_TYPE>
std::ostream& operator<<(
    std::ostream& os,
    const andres::marray::PyView<VALUE_TYPE, DIM, AUTO_CAST_TYPE> & obj
)
{
    os<<"PyViewArray[..]\n";
    return os;
}

} // end namespace zarr


namespace pybind11 {
namespace detail {

    template <typename Type, size_t DIM, bool AUTO_CAST_TYPE>
    struct pymarray_caster {
        typedef typename andres::marray::PyView<Type, DIM, AUTO_CAST_TYPE> ViewType;
        typedef type_caster<typename intrinsic_type<Type>::type> value_conv;
    
        //typedef typename pybind11::array_t<Type, py::array::c_style > pyarray_type;
    
        typedef typename std::conditional<AUTO_CAST_TYPE,
            pybind11::array_t<Type, py::array::forcecast>,
            pybind11::array_t<Type, py::array::c_style >
        >::type pyarray_type;
    
        typedef type_caster<pyarray_type> pyarray_conv;
    
        bool load(handle src, bool convert)
        {
            // convert numpy array to py::array_t
            pyarray_conv conv;
            if (!conv.load(src, convert)){
                return false;
            }
            auto pyarray = (pyarray_type)conv;
    
            auto info = pyarray.request();
    
            //if(!AUTO_CAST_TYPE){
            //    auto pyFormat = info.format;
            //    auto itemsize = info.itemsize;
            //    auto cppFormat =  py::detail::npy_format_descriptor<Type>::value;
            //    //std::cout<<"pyFormat  "<<pyFormat<<" size "<<itemsize<<"\n";
            //    //std::cout<<"cppFormat "<<cppFormat<<"\n";
            //    return false;
            //    //if(pyFormat != cppFormat){
            //    //    return false;
            //    //}
            //}
    
    
            if(DIM != 0 && DIM != info.shape.size()){
                ////std::cout<<"not matching\n";
                return false;
            }
            Type *ptr = (Type *)info.ptr;
    
            ViewType result(pyarray, ptr, info.shape.begin(), info.shape.end());
            value = result;
            return true;
        }
    
        static handle cast(ViewType src, return_value_policy policy, handle parent)
        {
            pyarray_conv conv;
            return conv.cast(src.py_array, policy, parent);
        }
    
        PYBIND11_TYPE_CASTER(ViewType, _("array<") + value_conv::name() + _(">"));
    };
    
    template <typename Type, size_t DIM, bool AUTO_CAST_TYPE>
    struct type_caster<andres::marray::PyView<Type, DIM, AUTO_CAST_TYPE> >
        : pymarray_caster<Type,DIM, AUTO_CAST_TYPE> {
    };

} // end namespace detail
} // end namespace pybind11
